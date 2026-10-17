#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/logs "$HOME/Library/LaunchAgents"
target="$HOME/Library/LaunchAgents/com.crypto-pipeline.archive.plist"
.venv/bin/python - "$PWD" "$target" <<'PY'
import plistlib
import sys
from pathlib import Path
from xml.sax.saxutils import escape

root, target = sys.argv[1:]
template = Path("ops/launchd/com.crypto-pipeline.archive.plist").read_text()
rendered = template.replace("__PROJECT_ROOT__", escape(root))
plistlib.loads(rendered.encode())
Path(target).write_text(rendered)
PY
launchctl bootstrap "gui/$(id -u)" "$target"
