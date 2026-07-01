#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
export CENACE_ROOT="$SCRIPT_DIR"
export CENACE_OPEN_BROWSER=0
export MPLCONFIGDIR="$SCRIPT_DIR/.cache/matplotlib"
mkdir -p "$MPLCONFIGDIR"
exec "$SCRIPT_DIR/venv/bin/python3" "$SCRIPT_DIR/download.py"




