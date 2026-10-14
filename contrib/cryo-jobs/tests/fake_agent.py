"""A real, controllable process for scheduler fault injection (no paid model)."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from cryo_jobs import store

spec = json.loads(sys.argv[-1]) if len(sys.argv) > 1 else json.load(sys.stdin)
job_id = os.environ["CRYO_JOB_ID"]
attempt = int(os.environ["CRYO_JOB_ATTEMPT"])
if spec.get("ignore_signals"):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
if spec.get("grandchild"):
    subprocess.Popen([sys.executable, "-c",
        "import time,pathlib,sys,signal; signal.signal(signal.SIGINT,signal.SIG_IGN); "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(3); pathlib.Path(sys.argv[1]).write_text('escaped')",
        spec["grandchild"]])
if spec.get("checkpoint"):
    store.checkpoint(store.home_path(), job_id, {"completed_step": 1, "session_id": "fake-session"})
if spec.get("ready"):
    Path(spec["ready"]).write_text(str(os.getpid()))
if spec.get("resume") and attempt > 1:
    value = json.loads(Path(os.environ["CRYO_JOB_CHECKPOINT"]).read_text())
    assert value["completed_step"] == 1
    result = {"resumed": True, "checkpoint": value}
else:
    time.sleep(spec.get("sleep", 0.1))
    result = {"ok": True, "attempt": attempt}
if spec.get("marker"):
    with open(spec["marker"], "a") as f:
        f.write(job_id + "\n")
Path(os.environ["CRYO_JOB_RESULT"]).write_text(json.dumps(result))
sys.exit(spec.get("exit", 0))
