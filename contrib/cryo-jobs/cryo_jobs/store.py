"""SQLite is the authority; filesystem locks identify live guardians."""
import contextlib
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

ACTIVE = ("starting", "running", "interrupting")
TERMINAL = ("succeeded", "failed", "interrupted", "timed_out", "lost")


def home_path(value=None):
    return Path(value or os.environ.get("CRYO_JOBS_HOME", "~/.local/share/cryo-jobs")).expanduser().resolve()


def connect(home):
    db = sqlite3.connect(home / "jobs.sqlite3", timeout=10, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=10000")
    db.execute("PRAGMA foreign_keys=ON")
    return db


@contextlib.contextmanager
def transaction(db):
    db.execute("BEGIN IMMEDIATE")
    try:
        yield db
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise


def initialize(home, command, concurrency=2, inbox=None, owners=()):
    if not command or not all(isinstance(x, str) for x in command):
        raise ValueError("command must be a nonempty argv array")
    if concurrency < 1:
        raise ValueError("concurrency must be positive")
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = home / "config.json"
    if path.exists():
        raise ValueError("already initialized; edit config.json explicitly")
    config = dict(command=command, resume_command=None, prompt_mode="argument",
                  concurrency=concurrency, inbox=str(Path(inbox).expanduser().resolve()) if inbox else None,
                  owners=list(owners), poll_seconds=0.1, launch_grace_seconds=5,
                  interrupt_grace_seconds=1, term_grace_seconds=1, min_free_bytes=268435456)
    atomic_json(path, config)
    with contextlib.closing(connect(home)) as db:
        db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE jobs (
          id TEXT PRIMARY KEY, title TEXT NOT NULL, prompt TEXT NOT NULL,
          state TEXT NOT NULL, created REAL NOT NULL, updated REAL NOT NULL,
          attempt INTEGER NOT NULL DEFAULT 0, cancel INTEGER NOT NULL DEFAULT 0,
          resource TEXT, timeout REAL, checkpoint TEXT, result TEXT,
          command TEXT NOT NULL, prompt_mode TEXT NOT NULL, cwd TEXT,
          request_key TEXT UNIQUE);
        CREATE TABLE attempts (
          job_id TEXT NOT NULL REFERENCES jobs(id), number INTEGER NOT NULL,
          state TEXT NOT NULL, started REAL NOT NULL, ended REAL,
          guardian_pid INTEGER, agent_pid INTEGER, exit_code INTEGER,
          directory TEXT NOT NULL, error TEXT,
          PRIMARY KEY(job_id, number));
        CREATE TABLE events (
          id TEXT PRIMARY KEY, job_id TEXT NOT NULL, kind TEXT NOT NULL,
          created REAL NOT NULL, payload TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE controls (id TEXT PRIMARY KEY, response TEXT NOT NULL);
        """)


def config(home):
    with (home / "config.json").open() as f:
        return json.load(f)


def atomic_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name("." + path.name + "." + uuid.uuid4().hex)
    try:
        with temp.open("x", encoding="utf-8") as f:
            os.chmod(temp, 0o600)
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
        sync_directory(path.parent)
    finally:
        temp.unlink(missing_ok=True)


def sync_directory(path):
    """Persist renames before acknowledging them to the SQLite event ledger."""
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def event(db, job_id, kind, **payload):
    db.execute("INSERT INTO events VALUES (?,?,?,?,?,0)",
               (uuid.uuid4().hex, job_id, kind, time.time(), json.dumps(payload, ensure_ascii=False)))


def get(db, job_id):
    row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if row is None:
        raise ValueError("unknown job: " + job_id)
    return dict(row)


def submit(home, prompt, title="task", resource=None, timeout=None, cwd=None, request_key=None):
    cfg = config(home)
    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be positive")
    if cwd:
        cwd = str(Path(cwd).expanduser().resolve(strict=True))
        if not Path(cwd).is_dir():
            raise ValueError("cwd must be a directory")
        resource = resource or cwd
    job_id = uuid.uuid4().hex
    with contextlib.closing(connect(home)) as db, transaction(db):
        if request_key:
            previous = db.execute("SELECT id FROM jobs WHERE request_key=?", (request_key,)).fetchone()
            if previous:
                return get(db, previous["id"])
        now = time.time()
        db.execute("""INSERT INTO jobs
          (id,title,prompt,state,created,updated,resource,timeout,command,prompt_mode,cwd,request_key)
          VALUES (?,?,?,'queued',?,?,?,?,?,?,?,?)""",
                   (job_id, title, prompt, now, now, resource, timeout,
                    json.dumps(cfg["command"]), cfg.get("prompt_mode", "argument"), cwd, request_key))
        event(db, job_id, "queued", title=title)
        return get(db, job_id)


def cancel_in_transaction(db, job_id):
    job = get(db, job_id)
    if job["state"] == "queued":
        db.execute("UPDATE jobs SET cancel=1,state='interrupted',updated=? WHERE id=?", (time.time(), job_id))
        event(db, job_id, "interrupted", reason="cancelled before execution")
    elif job["state"] in ACTIVE:
        db.execute("UPDATE jobs SET cancel=1,state='interrupting',updated=? WHERE id=?", (time.time(), job_id))
        if not job["cancel"]:
            event(db, job_id, "interrupt_requested")
    elif job["state"] == "lost":
        # Unknown descendants cannot be declared stopped just from missing guardian.
        db.execute("UPDATE jobs SET cancel=1 WHERE id=?", (job_id,))
    return get(db, job_id)


def interrupt(home, job_id):
    with contextlib.closing(connect(home)) as db, transaction(db):
        return cancel_in_transaction(db, job_id)


def checkpoint(home, job_id, value):
    with contextlib.closing(connect(home)) as db, transaction(db):
        job = get(db, job_id)
        if os.environ.get("CRYO_JOB_ID") == job_id:
            if int(os.environ.get("CRYO_JOB_ATTEMPT", "0")) != job["attempt"] or job["state"] not in ACTIVE:
                raise ValueError("stale execution cannot update the checkpoint")
        db.execute("UPDATE jobs SET checkpoint=?,updated=? WHERE id=?",
                   (json.dumps(value, ensure_ascii=False), time.time(), job_id))
        event(db, job_id, "checkpoint", value=value)


def resume(home, job_id, acknowledge=False):
    cfg = config(home)
    with contextlib.closing(connect(home)) as db, transaction(db):
        job = get(db, job_id)
        if job["state"] not in ("interrupted", "failed", "timed_out", "lost"):
            raise ValueError("only interrupted/failed/timed_out/lost jobs can resume")
        if job["state"] == "lost" and not acknowledge:
            raise ValueError("lost execution: inspect external effects/processes, then use --ack-uncertain")
        if not job["checkpoint"] and not acknowledge:
            raise ValueError("no checkpoint: use --ack-uncertain to explicitly rerun")
        command = cfg.get("resume_command") or json.loads(job["command"])
        db.execute("UPDATE jobs SET state='queued',cancel=0,result=NULL,command=?,updated=? WHERE id=?",
                   (json.dumps(command), time.time(), job_id))
        event(db, job_id, "resume_requested", previous_attempt=job["attempt"],
              checkpoint=bool(job["checkpoint"]), acknowledged_uncertainty=acknowledge)
        return get(db, job_id)


def resolve_lost(home, job_id, evidence):
    if not evidence.strip():
        raise ValueError("reconciliation evidence is required")
    with contextlib.closing(connect(home)) as db, transaction(db):
        job = get(db, job_id)
        if job["state"] != "lost":
            raise ValueError("only lost executions need explicit reconciliation")
        db.execute("UPDATE jobs SET state='interrupted',cancel=1,updated=? WHERE id=?", (time.time(), job_id))
        event(db, job_id, "interrupted", reason="operator confirmed stopped", evidence=evidence,
              previous_attempt=job["attempt"])
        return get(db, job_id)


@contextlib.contextmanager
def file_lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    f = path.open("a+")
    try:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield None
            return
        yield f
    finally:
        f.close()


def guardian_lock(home, job_id, attempt):
    return home / "jobs" / job_id / str(attempt) / "guardian.lock"
