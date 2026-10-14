"""Independent supervisor and per-attempt guardians, with no model dependency."""
import contextlib
from datetime import datetime
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import time
import traceback
import uuid

from .store import (ACTIVE, TERMINAL, atomic_json, config, connect, event, file_lock,
                    get, guardian_lock, sync_directory, transaction)


def child_env(home):
    env = os.environ.copy()
    env["CRYO_JOBS_HOME"] = str(home)
    # Enables source checkout and installed package equally, including arbitrary cwd.
    root = str(Path(__file__).resolve().parent.parent)
    env["PYTHONPATH"] = root + os.pathsep + env.get("PYTHONPATH", "")
    return env


def launch(home, *arguments, log_name="daemon.log"):
    with (home / log_name).open("ab", buffering=0) as log:
        return subprocess.Popen([sys.executable, "-m", "cryo_jobs", "--home", str(home), *arguments],
                                stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                env=child_env(home), start_new_session=True, close_fds=True)


def finish(db, job_id, number, state, exit_code=None, error=None, result=None):
    with transaction(db):
        job = get(db, job_id)
        if job["attempt"] != number or job["state"] not in ACTIVE:
            return
        if job["cancel"] and state != "lost":
            state = "interrupted"
        db.execute("UPDATE attempts SET state=?,ended=?,exit_code=?,error=? WHERE job_id=? AND number=?",
                   (state, time.time(), exit_code, error, job_id, number))
        db.execute("UPDATE jobs SET state=?,updated=?,result=? WHERE id=?",
                   (state, time.time(), json.dumps(result, ensure_ascii=False) if result is not None else None, job_id))
        attempt = db.execute("SELECT directory FROM attempts WHERE job_id=? AND number=?", (job_id, number)).fetchone()
        event(db, job_id, state, attempt=number, exit_code=exit_code, error=error, result=result,
              evidence_directory=attempt["directory"], checkpoint_available=bool(job["checkpoint"]))


def reconcile(home, db, cfg):
    for row in db.execute("SELECT * FROM jobs WHERE state IN ('starting','running','interrupting')").fetchall():
        attempt = db.execute("SELECT * FROM attempts WHERE job_id=? AND number=?", (row["id"], row["attempt"])).fetchone()
        with file_lock(guardian_lock(home, row["id"], row["attempt"])) as available:
            if available is not None and time.time() - attempt["started"] > cfg["launch_grace_seconds"]:
                finish(db, row["id"], row["attempt"], "lost", error=
                       "guardian missing; execution and external effects unknown; no automatic replay")


def claim(home, db, cfg):
    if shutil.disk_usage(home).free < cfg.get("min_free_bytes", 268435456):
        return None
    with transaction(db):
        # Lost attempts retain capacity and resource reservations until explicit reconciliation.
        held = db.execute("SELECT resource FROM jobs WHERE state IN ('starting','running','interrupting','lost')").fetchall()
        if len(held) >= cfg["concurrency"]:
            return None
        resources = {r["resource"] for r in held if r["resource"]}
        for candidate in db.execute("SELECT * FROM jobs WHERE state='queued' AND cancel=0 ORDER BY created,id"):
            job = dict(candidate)
            if job["resource"] and job["resource"] in resources:
                continue
            number = job["attempt"] + 1
            directory = home / "jobs" / job["id"] / str(number)
            directory.mkdir(parents=True, exist_ok=True)
            db.execute("UPDATE jobs SET state='starting',attempt=?,updated=? WHERE id=?",
                       (number, time.time(), job["id"]))
            db.execute("INSERT INTO attempts (job_id,number,state,started,directory) VALUES (?,?,'starting',?,?)",
                       (job["id"], number, time.time(), str(directory)))
            event(db, job["id"], "starting", attempt=number)
            return job["id"], number
    return None


def deliver(home, db, cfg):
    if not cfg.get("inbox"):
        return
    inbox = Path(cfg["inbox"])
    inbox.mkdir(parents=True, exist_ok=True)
    # Only terminal events wake the coordinator. Progress remains queryable in SQLite.
    rows = db.execute("SELECT * FROM events WHERE delivered=0 AND kind IN ('succeeded','failed','interrupted','timed_out','lost') ORDER BY created").fetchall()
    for row in rows:
        name = "cryo-job-" + row["id"] + ".md"
        target = inbox / name
        if not target.exists() and not (inbox / "archive" / name).exists():
            timestamp = datetime.fromtimestamp(row["created"]).strftime("%Y-%m-%dT%H:%M:%S")
            body = (f"---\nfrom: system\nsubject: job {row['id']} {row['kind']}\n"
                    f"timestamp: {timestamp}\nsource: cryo-jobs\nevent_id: {row['id']}\n"
                    f"job_id: {row['job_id']}\n---\n\n"
                    f"Task {row['job_id']}: {row['kind']}\n\n{row['payload']}\n\n"
                    "Integrate this execution evidence into the owning task/context. "
                    "An exit code of zero is execution success, not business acceptance. "
                    "Do not auto-resume interrupted/lost work or send operator status to a source group.\n")
            temp = inbox / ("." + name + "." + uuid.uuid4().hex)
            try:
                with temp.open("x", encoding="utf-8") as f:
                    f.write(body)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temp, target)
                sync_directory(inbox)
            finally:
                temp.unlink(missing_ok=True)
        db.execute("UPDATE events SET delivered=1 WHERE id=?", (row["id"],))


def daemon(home):
    cfg = config(home)
    if cfg["concurrency"] < 1 or cfg["poll_seconds"] <= 0:
        raise ValueError("invalid concurrency/poll_seconds")
    stop = False

    def stopping(_sig, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, stopping)
    signal.signal(signal.SIGINT, stopping)
    with file_lock(home / "daemon.lock") as lock:
        if lock is None:
            raise ValueError("daemon already running")
        lock.seek(0)
        lock.truncate()
        instance = uuid.uuid4().hex
        lock.write(json.dumps(dict(pid=os.getpid(), instance=instance)))
        lock.flush()
        children = []
        with contextlib.closing(connect(home)) as db:
            while not stop:
                stop_request = home / "daemon-stop.json"
                if stop_request.exists():
                    try:
                        if json.loads(stop_request.read_text()).get("instance") == instance:
                            break
                    except (OSError, ValueError):
                        pass
                reconcile(home, db, cfg)
                job = claim(home, db, cfg)
                if job:
                    try:
                        children.append(launch(home, "_guardian", job[0], str(job[1]), log_name="guardians.log"))
                    except OSError as exc:
                        finish(db, job[0], job[1], "failed", error=str(exc))
                try:
                    deliver(home, db, cfg)
                except OSError as exc:
                    # A bad inbox must not stop cancellation, execution, or durable events.
                    print(f"inbox delivery deferred: {exc}", file=sys.stderr, flush=True)
                children = [p for p in children if p.poll() is None]
                time.sleep(cfg["poll_seconds"])


def group_alive(pgid):
    # killpg(0) also sees zombies; ps is present on macOS/Linux/WSL.
    result = subprocess.run(["ps", "-eo", "pgid=,stat="], capture_output=True, text=True, check=True)
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0] == str(pgid) and not parts[1].startswith("Z"):
            return True
    return False


def signal_group(pgid, sig):
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass


def stop_group(proc, cfg, initial=signal.SIGINT):
    for sig, grace in ((initial, cfg["interrupt_grace_seconds"]),
                       (signal.SIGTERM, cfg["term_grace_seconds"]), (signal.SIGKILL, 1)):
        signal_group(proc.pid, sig)
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            proc.poll()
            if not group_alive(proc.pid):
                return True
            time.sleep(0.03)
    return not group_alive(proc.pid)


def guardian(home, job_id, number):
    cfg = config(home)
    proc = None
    stopping = False

    def request_stop(_sig, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    with file_lock(guardian_lock(home, job_id, number)) as lock:
        if lock is None:
            return
        with contextlib.closing(connect(home)) as db:
            try:
                job = get(db, job_id)
                if job["attempt"] != number or job["state"] not in ACTIVE:
                    return
                if job["cancel"]:
                    finish(db, job_id, number, "interrupted")
                    return
                directory = home / "jobs" / job_id / str(number)
                (directory / "prompt.txt").write_text(job["prompt"], encoding="utf-8")
                checkpoint_file = directory / "checkpoint-input.json"
                atomic_json(checkpoint_file, json.loads(job["checkpoint"]) if job["checkpoint"] else None)
                env = child_env(home)
                env.update(CRYO_JOB_ID=job_id, CRYO_JOB_ATTEMPT=str(number),
                           CRYO_JOB_DIR=str(directory), CRYO_JOB_PROMPT_FILE=str(directory / "prompt.txt"),
                           CRYO_JOB_CHECKPOINT=str(checkpoint_file), CRYO_JOB_RESULT=str(directory / "result.json"))
                argv = json.loads(job["command"])
                mode = job["prompt_mode"]
                if mode == "argument":
                    argv = argv + [job["prompt"]]
                elif mode not in ("stdin", "file"):
                    raise ValueError("prompt_mode must be argument, stdin or file")
                # File-backed stdin avoids deadlock on agents that don't consume a large prompt.
                with (directory / "stdout.log").open("ab", buffering=0) as out, \
                     (directory / "stderr.log").open("ab", buffering=0) as err, \
                     (directory / "prompt.txt").open("rb") as prompt_in:
                    with transaction(db):
                        current = get(db, job_id)
                        if current["cancel"]:
                            event(db, job_id, "launch_cancelled", attempt=number)
                        else:
                            proc = subprocess.Popen(argv, cwd=job["cwd"] or directory, env=env,
                                                    stdin=prompt_in if mode == "stdin" else subprocess.DEVNULL,
                                                    stdout=out, stderr=err, start_new_session=True, close_fds=True)
                            db.execute("UPDATE jobs SET state='running',updated=? WHERE id=?", (time.time(), job_id))
                            db.execute("UPDATE attempts SET state='running',guardian_pid=?,agent_pid=? WHERE job_id=? AND number=?",
                                       (os.getpid(), proc.pid, job_id, number))
                            event(db, job_id, "running", attempt=number, agent_pid=proc.pid)
                    if proc is None:
                        finish(db, job_id, number, "interrupted")
                        return
                    started = time.monotonic()
                    state = None
                    error = None
                    while proc.poll() is None:
                        cancelled = get(db, job_id)["cancel"] or stopping
                        timed_out = job["timeout"] is not None and time.monotonic() - started >= job["timeout"]
                        if cancelled or timed_out:
                            state = "interrupted" if cancelled else "timed_out"
                            if not stop_group(proc, cfg):
                                state, error = "lost", "process group did not confirm stopped"
                            break
                        time.sleep(0.05)
                    if state is None:
                        state = "succeeded" if proc.returncode == 0 else "failed"
                        # Background grandchildren do not escape terminal lifecycle accidentally.
                        if group_alive(proc.pid) and not stop_group(proc, cfg, signal.SIGTERM):
                            state, error = "lost", "background processes still alive"
                    result = None
                    result_file = directory / "result.json"
                    if result_file.exists():
                        try:
                            if result_file.stat().st_size > 1024 * 1024:
                                raise ValueError("result exceeds 1 MiB; store large artifacts separately")
                            result = json.loads(result_file.read_text())
                        except (ValueError, OSError) as exc:
                            error = "invalid result.json: " + str(exc)
                            if state == "succeeded":
                                state = "failed"
                    finish(db, job_id, number, state, proc.poll(), error, result)
            except BaseException as exc:
                stopped = proc is None or stop_group(proc, cfg)
                finish(db, job_id, number, "failed" if stopped else "lost", error=str(exc))
                traceback.print_exc()


def control(home, payload):
    """Accept a trusted local bridge event, never an unauthenticated HTTP body."""
    from .store import cancel_in_transaction
    cfg = config(home)
    if not isinstance(payload, dict):
        raise ValueError("callback must be an object")
    # Real Feishu card.action.trigger shape or normalized local adapter shape.
    envelope = payload.get("event", payload)
    operator = envelope.get("operator", {})
    operator_id = payload.get("operator_id") or operator.get("open_id")
    if operator_id not in cfg.get("owners", []):
        raise ValueError("operator is not in configured owners")
    event_id = payload.get("event_id") or payload.get("header", {}).get("event_id")
    action = envelope.get("action", {}).get("value", payload.get("action_value", {}))
    if isinstance(action, str):
        action = json.loads(action)
    if not isinstance(action, dict) or not isinstance(event_id, str) or not event_id or action.get("action") != "interrupt":
        raise ValueError("event_id and interrupt action are required")
    with contextlib.closing(connect(home)) as db, transaction(db):
        previous = db.execute("SELECT response FROM controls WHERE id=?", (event_id,)).fetchone()
        if previous:
            return json.loads(previous["response"])
        job = cancel_in_transaction(db, action["job_id"])
        response = dict(job_id=job["id"], state=job["state"], accepted=True,
                        stopped=job["state"] == "interrupted")
        db.execute("INSERT INTO controls VALUES (?,?)", (event_id, json.dumps(response)))
        return response


def consume_controls(home, source, output):
    """Line-by-line local bridge; malformed/foreign events never halt subsequent controls."""
    for line in source:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
            if isinstance(payload, dict) and payload.get("type") not in (None, "card.action.trigger"):
                continue
            response = control(home, payload)
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            response = dict(accepted=False, error=str(exc))
        output.write(json.dumps(response, ensure_ascii=False) + "\n")
        output.flush()
