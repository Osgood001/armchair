#!/bin/sh
set -eu
# Replace this demo body with `exec your-agent ... "$1"`.
# Do not background the agent or put it into another process session.
# Prompt is the final argument, matching Cryochamber's custom-command contract.
printf 'job=%s attempt=%s\n%s\n' "$CRYO_JOB_ID" "$CRYO_JOB_ATTEMPT" "$1"
python3 - <<'PY'
import json, os
with open(os.environ['CRYO_JOB_RESULT'], 'w') as f:
    json.dump({'summary': 'Adapter ran successfully', 'artifacts': []}, f)
PY
