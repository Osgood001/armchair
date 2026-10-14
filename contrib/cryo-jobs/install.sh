#!/bin/sh
set -eu
# No network, pip, root, or mutations to existing Cryochamber services.
SOURCE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CRYO_INSTALL_PREFIX=${CRYO_INSTALL_PREFIX:-"$HOME/.local"}
CRYO_PYTHON=${CRYO_PYTHON:-python3}
"$CRYO_PYTHON" - "$SOURCE_DIR" "$CRYO_INSTALL_PREFIX" <<'PY'
import os, pathlib, shlex, shutil, sys
if sys.version_info < (3, 10):
    raise SystemExit('Python >=3.10 required; set CRYO_PYTHON=/path/to/python3')
source, prefix = map(lambda x: pathlib.Path(x).expanduser().resolve(), sys.argv[1:])
version = '0.1.0'
target = prefix / 'lib' / 'cryo-jobs' / version
target.mkdir(parents=True, exist_ok=True)
shutil.copytree(source / 'cryo_jobs', target / 'cryo_jobs', dirs_exist_ok=True,
                ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
binary = prefix / 'bin' / 'cryo-jobs'
binary.parent.mkdir(parents=True, exist_ok=True)
binary.write_text('#!/bin/sh\nexport PYTHONPATH=' + shlex.quote(str(target)) +
                  '${PYTHONPATH:+:$PYTHONPATH}\nexec ' + shlex.quote(sys.executable) +
                  ' -m cryo_jobs "$@"\n')
binary.chmod(0o755)
print(str(binary))
PY
