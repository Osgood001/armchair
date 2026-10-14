import argparse
import contextlib
import json
import os
from pathlib import Path
import plistlib
import shutil
import signal
import sys
import time

from . import __version__
from . import runtime, store


def emit(value):
    print(json.dumps(value, ensure_ascii=False, indent=2))


def service(home, kind, output):
    executable = str(Path(sys.executable).absolute())
    package_root = str(Path(__file__).resolve().parent.parent)
    argv = [executable, "-m", "cryo_jobs", "--home", str(home), "daemon", "run"]
    if kind == "launchd":
        data = plistlib.dumps(dict(Label="local.cryo-jobs", ProgramArguments=argv, RunAtLoad=True,
                                  KeepAlive=True, EnvironmentVariables=dict(PYTHONPATH=package_root,
                                  PATH=os.environ.get("PATH", "/usr/bin:/bin")),
                                  StandardOutPath=str(home / "service.log"),
                                  StandardErrorPath=str(home / "service.log")))
    else:
        # Quote systemd syntax, escaping percent specifiers and literal dollars.
        def quote(value):
            return '"' + value.replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%').replace('$', '$$') + '"'
        content = ("[Unit]\nDescription=Cryo Jobs persistent task scheduler\n\n[Service]\nType=simple\n"
                   "ExecStart=" + " ".join(quote(a) for a in argv) + "\n"
                   "Environment=" + quote("PYTHONPATH=" + package_root) + "\n"
                   "Environment=" + quote("PATH=" + os.environ.get("PATH", "/usr/bin:/bin")) + "\n"
                   "Restart=on-failure\nRestartSec=2\nKillMode=process\n"
                   "# Independent guardians must survive coordinator restart. Interrupt jobs explicitly.\n"
                   "\n[Install]\nWantedBy=default.target\n")
        data = content.encode()
    Path(output).expanduser().write_bytes(data)
    return dict(written=str(output), installed=False)


def daemon_command(home, action):
    if action == "run":
        runtime.daemon(home)
        return None
    with store.file_lock(home / "daemon.lock") as lock:
        running = lock is None
    if action == "status":
        return dict(running=running)
    if action == "start":
        if running:
            return dict(running=True, already_running=True)
        proc = runtime.launch(home, "daemon", "run")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with store.file_lock(home / "daemon.lock") as lock:
                if lock is None:
                    return dict(running=True, pid=proc.pid)
            if proc.poll() is not None:
                raise ValueError("daemon failed; inspect " + str(home / "daemon.log"))
            time.sleep(0.05)
        raise ValueError("daemon startup timed out; inspect daemon.log")
    if running:
        # Token-addressed request avoids PID-reuse races when a daemon exits concurrently.
        instance = json.loads((home / "daemon.lock").read_text())["instance"]
        store.atomic_json(home / "daemon-stop.json", dict(instance=instance))
        return dict(stop_requested=True, tasks_continuing=True)
    return dict(stop_requested=False, running=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Durable command tasks, independent of any coding agent")
    parser.add_argument("--home", help="private state directory (or CRYO_JOBS_HOME)")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="verb", required=True)
    p = sub.add_parser("init")
    p.add_argument("--command-json", required=True, help='argv array, e.g. ["/path/run-agent.sh"]')
    p.add_argument("--concurrency", type=int, default=2)
    p.add_argument("--inbox", help="Cryochamber messages/inbox directory; optional")
    p.add_argument("--owner", action="append", default=[], help="Feishu owner open_id; repeatable")
    p = sub.add_parser("submit")
    p.add_argument("--prompt", help="task text; alternatively --file or stdin")
    p.add_argument("--file")
    p.add_argument("--title", default="task")
    p.add_argument("--resource", help="serialize tasks sharing this resource key")
    p.add_argument("--cwd", help="explicit workspace; implies resource key unless overridden")
    p.add_argument("--timeout", type=float)
    p.add_argument("--request-key", help="idempotency key for registration/retry")
    sub.add_parser("list")
    p = sub.add_parser("show")
    p.add_argument("job_id")
    p = sub.add_parser("events")
    p.add_argument("--job-id")
    p = sub.add_parser("interrupt")
    p.add_argument("job_id")
    p.add_argument("--wait", type=float, default=5, help="seconds to await stop confirmation; 0 returns immediately")
    p = sub.add_parser("resume")
    p.add_argument("job_id")
    p.add_argument("--ack-uncertain", action="store_true", help="explicitly acknowledge replay/unknown effects")
    p = sub.add_parser("resolve", help="release lost execution after manual process/effect reconciliation")
    p.add_argument("job_id")
    p.add_argument("--ack-stopped", action="store_true", required=True)
    p.add_argument("--evidence", required=True)
    p = sub.add_parser("checkpoint")
    p.add_argument("job_id")
    p.add_argument("--json", required=True)
    p = sub.add_parser("daemon")
    p.add_argument("action", choices=["start", "run", "stop", "status"])
    p = sub.add_parser("service")
    p.add_argument("kind", choices=["launchd", "systemd"])
    p.add_argument("--output", required=True)
    sub.add_parser("doctor")
    sub.add_parser("control", help="read trusted local Feishu callback JSON from stdin")
    sub.add_parser("control-stream", help="consume trusted local callback JSONL continuously")
    p = sub.add_parser("_guardian", help=argparse.SUPPRESS)
    p.add_argument("job_id")
    p.add_argument("number", type=int)
    args = parser.parse_args(argv)
    home = store.home_path(args.home)
    try:
        if args.verb == "init":
            store.initialize(home, json.loads(args.command_json), args.concurrency, args.inbox, args.owner)
            emit(dict(home=str(home), initialized=True))
        elif args.verb == "submit":
            if args.prompt is not None and args.file:
                raise ValueError("choose --prompt or --file")
            prompt = args.prompt if args.prompt is not None else Path(args.file).read_text() if args.file else sys.stdin.read()
            if not prompt.strip():
                raise ValueError("prompt cannot be empty")
            emit(store.submit(home, prompt, args.title, args.resource, args.timeout, args.cwd, args.request_key))
        elif args.verb in ("list", "show", "events"):
            with contextlib.closing(store.connect(home)) as db:
                if args.verb == "show":
                    result = store.get(db, args.job_id)
                    result["attempts"] = [dict(r) for r in db.execute("SELECT * FROM attempts WHERE job_id=? ORDER BY number", (args.job_id,))]
                elif args.verb == "list":
                    result = [dict(r) for r in db.execute("SELECT id,title,state,attempt,resource,updated FROM jobs ORDER BY created")]
                else:
                    result = [dict(r) for r in db.execute("SELECT * FROM events WHERE (? IS NULL OR job_id=?) ORDER BY created", (args.job_id, args.job_id))]
                emit(result)
        elif args.verb == "interrupt":
            job = store.interrupt(home, args.job_id)
            deadline = time.monotonic() + args.wait
            while job["state"] in store.ACTIVE and time.monotonic() < deadline:
                time.sleep(0.05)
                with contextlib.closing(store.connect(home)) as db:
                    job = store.get(db, args.job_id)
            emit(dict(job_id=job["id"], state=job["state"], stopped=job["state"] == "interrupted"))
            if job["state"] in store.ACTIVE or job["state"] == "lost":
                return 2
        elif args.verb == "checkpoint":
            store.checkpoint(home, args.job_id, json.loads(args.json))
            emit(dict(saved=True))
        elif args.verb == "resume":
            emit(store.resume(home, args.job_id, args.ack_uncertain))
        elif args.verb == "resolve":
            emit(store.resolve_lost(home, args.job_id, args.evidence))
        elif args.verb == "daemon":
            result = daemon_command(home, args.action)
            if result is not None:
                emit(result)
        elif args.verb == "_guardian":
            runtime.guardian(home, args.job_id, args.number)
        elif args.verb == "service":
            emit(service(home, args.kind, args.output))
        elif args.verb == "control":
            emit(runtime.control(home, json.load(sys.stdin)))
        elif args.verb == "control-stream":
            runtime.consume_controls(home, sys.stdin, sys.stdout)
        elif args.verb == "doctor":
            cfg = store.config(home)
            executable = cfg["command"][0]
            found = shutil.which(executable) is not None
            with contextlib.closing(store.connect(home)) as db:
                integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
            emit(dict(python=sys.version.split()[0], platform=sys.platform, home=str(home),
                      command_found=found, sqlite=integrity, disk_free_bytes=shutil.disk_usage(home).free,
                      ps_found=shutil.which("ps") is not None,
                      feishu_cli_found=shutil.which("lark-cli") is not None,
                      bohr_cli_found=shutil.which("bohr") is not None or shutil.which("bohr-cli") is not None))
            return 0 if found and integrity == "ok" and shutil.which("ps") else 1
        return 0
    except (ValueError, OSError, KeyError) as exc:
        print(json.dumps(dict(error=str(exc)), ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
