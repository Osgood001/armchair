import contextlib
import json
import io
import os
from pathlib import Path
import plistlib
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from cryo_jobs import runtime, store
from cryo_jobs.__main__ import service
from cryo_jobs.__main__ import daemon_command

ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests" / "fake_agent.py"


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="cryo-jobs-test-")
        self.base = Path(self.tmp.name)
        self.home = self.base / "state"
        self.inbox = self.base / "inbox"
        store.initialize(self.home, [sys.executable, str(FAKE)], inbox=self.inbox, owners=["owner"])
        cfg = store.config(self.home)
        cfg.update(poll_seconds=0.03, launch_grace_seconds=0.7,
                   interrupt_grace_seconds=0.15, term_grace_seconds=0.15)
        store.atomic_json(self.home / "config.json", cfg)
        self.daemons = []

    def tearDown(self):
        # Stop further claims before cancellation; only touch this temporary namespace.
        for daemon in self.daemons:
            if daemon.poll() is None:
                daemon.terminate()
                daemon.wait(timeout=5)
        with contextlib.closing(store.connect(self.home)) as db:
            ids = [r["id"] for r in db.execute("SELECT id FROM jobs")]
            attempts = [dict(r) for r in db.execute("SELECT * FROM attempts")]
        for job_id in ids:
            store.interrupt(self.home, job_id)
        for attempt in attempts:
            pgid = attempt["agent_pid"]
            if pgid:
                runtime.signal_group(pgid, signal.SIGKILL)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            locked = False
            for attempt in attempts:
                with store.file_lock(store.guardian_lock(self.home, attempt["job_id"], attempt["number"])) as lock:
                    locked |= lock is None
            if not locked:
                break
            time.sleep(0.05)
        self.tmp.cleanup()

    def start(self):
        daemon = runtime.launch(self.home, "daemon", "run")
        self.daemons.append(daemon)
        self.until(lambda: self.locked(self.home / "daemon.lock"))
        return daemon

    def locked(self, path):
        with store.file_lock(path) as lock:
            return lock is None

    def until(self, condition, timeout=8):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            value = condition()
            if value:
                return value
            time.sleep(0.025)
        self.fail("condition timed out; " + (self.home / "guardians.log").read_text()[-3000:] if (self.home / "guardians.log").exists() else "condition timed out")

    def job(self, job_id):
        with contextlib.closing(store.connect(self.home)) as db:
            return store.get(db, job_id)

    def state(self, job_id, expected):
        return self.until(lambda: self.job(job_id) if self.job(job_id)["state"] in expected else None)

    def submit(self, **spec):
        return store.submit(self.home, json.dumps(spec))["id"]

    def test_parallel_bounded_resource_and_idempotent_registration(self):
        prompt = json.dumps({"sleep": 0.5})
        first = store.submit(self.home, prompt, resource="repo", request_key="msg-1")["id"]
        self.assertEqual(first, store.submit(self.home, prompt, request_key="msg-1")["id"])
        second = store.submit(self.home, prompt, resource="repo")["id"]
        third = self.submit(sleep=0.5)
        fourth = self.submit(sleep=0.2)
        self.start()
        self.state(first, ["running"])
        self.state(third, ["running"])
        self.assertEqual(self.job(second)["state"], "queued")
        self.assertEqual(self.job(fourth)["state"], "queued")
        for job_id in (first, second, third, fourth):
            self.state(job_id, ["succeeded"])
        with contextlib.closing(store.connect(self.home)) as db:
            rows = [dict(r) for r in db.execute("SELECT * FROM attempts")]
        ticks = sorted([(r["started"], 1) for r in rows] + [(r["ended"], -1) for r in rows])
        active = 0
        for _, delta in ticks:
            active += delta
            self.assertLessEqual(active, 2)

    def test_daemon_sigkill_adopts_guardian_no_replay_and_delivers(self):
        marker = self.base / "effects"
        job_id = self.submit(sleep=1, marker=str(marker))
        daemon = self.start()
        self.state(job_id, ["running"])
        daemon.kill()
        daemon.wait(timeout=3)
        self.state(job_id, ["succeeded"])
        self.start()
        self.until(lambda: list(self.inbox.glob("*.md")))
        self.assertEqual(marker.read_text().splitlines(), [job_id])
        self.assertEqual(self.job(job_id)["attempt"], 1)
        files = list(self.inbox.glob("*.md"))
        self.assertEqual(len(files), 1)
        text = files[0].read_text()
        self.assertIn("from: system", text)
        self.assertNotIn("source: feishu", text)

    def test_restart_while_task_running_adopts_same_attempt(self):
        job_id = self.submit(sleep=1)
        daemon = self.start()
        self.state(job_id, ["running"])
        with contextlib.closing(store.connect(self.home)) as db:
            pid = db.execute("SELECT agent_pid FROM attempts WHERE job_id=?", (job_id,)).fetchone()[0]
        daemon.kill()
        daemon.wait(timeout=3)
        self.start()
        self.assertEqual(self.job(job_id)["attempt"], 1)
        self.state(job_id, ["succeeded"])
        with contextlib.closing(store.connect(self.home)) as db:
            self.assertEqual(db.execute("SELECT agent_pid FROM attempts WHERE job_id=?", (job_id,)).fetchone()[0], pid)

    def test_interrupt_without_daemon_kills_group_and_stays_cancelled(self):
        escaped = self.base / "escaped"
        ready = self.base / "ready"
        job_id = self.submit(sleep=20, ignore_signals=True, grandchild=str(escaped), ready=str(ready))
        daemon = self.start()
        self.until(ready.exists)
        daemon.kill()
        daemon.wait(timeout=3)
        started = time.monotonic()
        store.interrupt(self.home, job_id)
        self.state(job_id, ["interrupted"])
        latency = time.monotonic() - started
        self.assertLess(latency, 2)
        print(f"interrupt including SIGKILL fallback: {latency:.3f}s", flush=True)
        self.start()
        time.sleep(3.1)
        self.assertFalse(escaped.exists())
        self.assertEqual(self.job(job_id)["attempt"], 1)
        self.assertEqual(self.job(job_id)["cancel"], 1)

    def test_queued_cancel_never_starts(self):
        job_id = self.submit()
        store.interrupt(self.home, job_id)
        self.start()
        time.sleep(0.2)
        self.assertEqual(self.job(job_id)["state"], "interrupted")
        self.assertEqual(self.job(job_id)["attempt"], 0)

    def test_explicit_checkpoint_resume_keeps_attempt_evidence(self):
        job_id = self.submit(sleep=20, checkpoint=True, resume=True)
        self.start()
        self.until(lambda: self.job(job_id)["checkpoint"])
        store.interrupt(self.home, job_id)
        self.state(job_id, ["interrupted"])
        store.resume(self.home, job_id)
        job = self.state(job_id, ["succeeded"])
        self.assertEqual(job["attempt"], 2)
        self.assertTrue(json.loads(job["result"])["resumed"])
        self.assertTrue((self.home / "jobs" / job_id / "1" / "stdout.log").exists())
        self.assertTrue((self.home / "jobs" / job_id / "2" / "stdout.log").exists())

    def test_guardian_loss_holds_capacity_and_never_replays(self):
        cfg = store.config(self.home)
        cfg["concurrency"] = 1
        store.atomic_json(self.home / "config.json", cfg)
        job_id = self.submit(sleep=20)
        other = self.submit()
        self.start()
        self.state(job_id, ["running"])
        with contextlib.closing(store.connect(self.home)) as db:
            attempt = dict(db.execute("SELECT * FROM attempts WHERE job_id=?", (job_id,)).fetchone())
        os.kill(attempt["guardian_pid"], signal.SIGKILL)
        self.state(job_id, ["lost"])
        self.assertEqual(self.job(other)["state"], "queued")
        with self.assertRaises(ValueError):
            store.resume(self.home, job_id)
        self.assertEqual(self.job(job_id)["attempt"], 1)
        self.assertEqual(store.interrupt(self.home, job_id)["state"], "lost")
        runtime.signal_group(attempt["agent_pid"], signal.SIGKILL)
        self.until(lambda: not runtime.group_alive(attempt["agent_pid"]))
        store.resolve_lost(self.home, job_id, "test killed recorded process group and confirmed stopped")
        self.state(other, ["succeeded"])

    def test_timeout_is_terminal_and_no_automatic_retry(self):
        job_id = store.submit(self.home, json.dumps({"sleep": 20}), timeout=0.2)["id"]
        self.start()
        self.state(job_id, ["timed_out"])
        time.sleep(0.2)
        self.assertEqual(self.job(job_id)["attempt"], 1)

    def test_failed_process_not_success(self):
        job_id = self.submit(exit=7)
        self.start()
        self.state(job_id, ["failed"])
        with self.assertRaises(ValueError):
            store.resume(self.home, job_id)

    def test_callback_owner_validation_and_replay_after_resume(self):
        job_id = self.submit()
        callback = dict(header=dict(event_id="evt"), event=dict(operator=dict(open_id="other"),
                        action=dict(value=dict(action="interrupt", job_id=job_id))))
        with self.assertRaises(ValueError):
            runtime.control(self.home, callback)
        self.assertEqual(self.job(job_id)["state"], "queued")
        callback["event"]["operator"]["open_id"] = "owner"
        first = runtime.control(self.home, callback)
        self.assertTrue(first["stopped"])
        store.resume(self.home, job_id, acknowledge=True)
        self.assertEqual(runtime.control(self.home, callback), first)
        self.assertEqual(self.job(job_id)["state"], "queued")

    def test_control_stream_recovers_from_invalid_events(self):
        job_id = self.submit()
        callback = dict(type="card.action.trigger", event_id="cli-event", operator_id="owner",
                        action_value=json.dumps(dict(action="interrupt", job_id=job_id)))
        output = io.StringIO()
        runtime.consume_controls(self.home, io.StringIO("not json\n[]\n" + json.dumps(callback) + "\n"), output)
        lines = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertFalse(lines[0]["accepted"])
        self.assertFalse(lines[1]["accepted"])
        self.assertTrue(lines[2]["stopped"])

    def test_token_addressed_daemon_stop_leaves_tasks_alive(self):
        job_id = self.submit(sleep=0.6)
        daemon = self.start()
        self.state(job_id, ["running"])
        result = daemon_command(self.home, "stop")
        self.assertTrue(result["tasks_continuing"])
        daemon.wait(timeout=3)
        self.state(job_id, ["succeeded"])
        self.start()
        self.assertTrue(daemon_command(self.home, "status")["running"])

    def test_low_disk_stops_new_claims(self):
        cfg = store.config(self.home)
        cfg["min_free_bytes"] = 10**30
        store.atomic_json(self.home / "config.json", cfg)
        job_id = self.submit()
        self.start()
        time.sleep(0.2)
        self.assertEqual(self.job(job_id)["state"], "queued")

    def test_crash_between_claim_and_spawn_never_blindly_replays(self):
        marker = self.base / "must-not-run"
        job_id = self.submit(marker=str(marker))
        with contextlib.closing(store.connect(self.home)) as db:
            self.assertEqual(runtime.claim(self.home, db, store.config(self.home)), (job_id, 1))
        # Simulate coordinator death after durable claim, before spawning guardian.
        self.start()
        self.state(job_id, ["lost"])
        self.assertFalse(marker.exists())
        self.assertEqual(self.job(job_id)["attempt"], 1)

    def test_missing_backend_reports_failure_with_evidence(self):
        cfg = store.config(self.home)
        cfg["command"] = [str(self.base / "nonexistent-agent")]
        store.atomic_json(self.home / "config.json", cfg)
        job_id = self.submit()
        self.start()
        self.state(job_id, ["failed"])
        with contextlib.closing(store.connect(self.home)) as db:
            row = db.execute("SELECT error FROM attempts WHERE job_id=?", (job_id,)).fetchone()
            payload = json.loads(db.execute("SELECT payload FROM events WHERE job_id=? AND kind='failed'", (job_id,)).fetchone()[0])
        self.assertIn("No such file", row["error"])
        self.assertIn("evidence_directory", payload)

    def test_stale_worker_cannot_overwrite_checkpoint(self):
        from unittest.mock import patch
        job_id = self.submit()
        with patch.dict(os.environ, dict(CRYO_JOB_ID=job_id, CRYO_JOB_ATTEMPT="99")):
            with self.assertRaises(ValueError):
                store.checkpoint(self.home, job_id, dict(stale=True))

    def test_archive_prevents_delivery_replay_after_write_crash(self):
        job_id = self.submit()
        store.interrupt(self.home, job_id)
        cfg = store.config(self.home)
        with contextlib.closing(store.connect(self.home)) as db:
            runtime.deliver(self.home, db, cfg)
            file = next(self.inbox.glob("*.md"))
            (self.inbox / "archive").mkdir()
            file.rename(self.inbox / "archive" / file.name)
            db.execute("UPDATE events SET delivered=0")
            runtime.deliver(self.home, db, cfg)
        self.assertFalse(list(self.inbox.glob("*.md")))

    def test_adapter_stdin_and_literal_argument_no_shell(self):
        cfg = store.config(self.home)
        cfg["prompt_mode"] = "stdin"
        store.atomic_json(self.home / "config.json", cfg)
        marker = self.base / "must-not-exist"
        job_id = self.submit(text=f'$(touch {marker}); `touch {marker}`')
        self.start()
        self.state(job_id, ["succeeded"])
        self.assertFalse(marker.exists())

    def test_service_templates(self):
        output = self.base / "service.plist"
        service(self.home, "launchd", output)
        payload = plistlib.loads(output.read_bytes())
        self.assertEqual(payload["ProgramArguments"][-2:], ["daemon", "run"])
        service(self.home, "systemd", output)
        self.assertIn("KillMode=process", output.read_text())

    def test_cli_submitter_exit_does_not_kill_worker(self):
        self.start()
        proc = subprocess.run([sys.executable, "-m", "cryo_jobs", "--home", str(self.home),
                               "submit", "--prompt", json.dumps(dict(sleep=0.3))],
                              env=runtime.child_env(self.home), capture_output=True, text=True, check=True)
        job_id = json.loads(proc.stdout)["id"]
        self.state(job_id, ["succeeded"])

    def test_installer_with_spaces_and_no_dependencies(self):
        prefix = self.base / "install with spaces"
        env = os.environ.copy()
        env.update(CRYO_INSTALL_PREFIX=str(prefix), CRYO_PYTHON=sys.executable)
        subprocess.run(["sh", str(ROOT / "install.sh")], env=env, check=True, capture_output=True)
        version = subprocess.check_output([str(prefix / "bin" / "cryo-jobs"), "--version"], text=True)
        self.assertEqual(version.strip(), "0.1.0")


if __name__ == "__main__":
    unittest.main(verbosity=2)
