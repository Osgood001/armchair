"""End-to-end install/CLI run, retaining a small auditable evidence directory."""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

source = Path(__file__).resolve().parents[1]
output = Path(sys.argv[1]).resolve()
output.mkdir(parents=True, exist_ok=False)
prefix = output / "installed"
home = output / "state"
env = os.environ.copy()
env.update(CRYO_INSTALL_PREFIX=str(prefix), CRYO_PYTHON=sys.executable, CRYO_JOBS_HOME=str(home))
subprocess.run(["sh", str(source / "install.sh")], check=True, env=env, capture_output=True)
binary = prefix / "bin" / "cryo-jobs"
records = []


def cli(*args):
    proc = subprocess.run([str(binary), *args], env=env, text=True, capture_output=True)
    result = json.loads(proc.stdout) if proc.stdout.strip() else None
    records.append(dict(argv=list(args), exit_code=proc.returncode, result=result, stderr=proc.stderr))
    if proc.returncode:
        raise RuntimeError(proc.stderr)
    return result


def wait_for(job_id, states):
    end = time.monotonic() + 10
    while time.monotonic() < end:
        job = cli("show", job_id)
        if job["state"] in states:
            return job
        time.sleep(0.1)
    raise RuntimeError("job did not reach " + str(states))


try:
    cli("init", "--command-json", json.dumps(["/bin/sh", str(source / "examples" / "run-agent.sh")]),
        "--inbox", str(output / "inbox"), "--owner", "test-owner")
    doctor = cli("doctor")
    cli("daemon", "start")
    job = cli("submit", "--prompt", "A literal prompt with spaces, 中文, $(not-shell) and `not-shell`", "--request-key", "acceptance-demo")
    result = wait_for(job["id"], ["succeeded", "failed"])
    assert result["state"] == "succeeded"
    assert "Adapter ran successfully" in result["result"]
    cli("service", "launchd", "--output", str(output / "service.plist"))
    cli("service", "systemd", "--output", str(output / "service.service"))
    deadline = time.monotonic() + 3
    while not list((output / "inbox").glob("*.md")) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert len(list((output / "inbox").glob("*.md"))) == 1
    cli("daemon", "stop")
    deadline = time.monotonic() + 3
    while cli("daemon", "status")["running"] and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not cli("daemon", "status")["running"]
    summary = dict(passed=True, platform=platform.platform(), python=sys.version,
                   job_id=job["id"], attempt=result["attempt"], doctor=doctor,
                   installed_binary=str(binary), source_sha256={
                       str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest()
                       for p in sorted((source / "cryo_jobs").glob("*.py"))})
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
finally:
    if (home / "config.json").exists():
        subprocess.run([str(binary), "daemon", "stop"], env=env, capture_output=True)
    (output / "commands.json").write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n")
