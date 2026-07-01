#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CENACE_ROOT="$SCRIPT_DIR"
export CENACE_OPEN_BROWSER=0
export MPLCONFIGDIR="$SCRIPT_DIR/.cache/matplotlib"
export PLAYWRIGHT_BROWSERS_PATH="$SCRIPT_DIR/.playwright"
mkdir -p "$MPLCONFIGDIR"

exec "$SCRIPT_DIR/venv/bin/python3" \
    "$SCRIPT_DIR/generar_boletin_hidroelectricas.py" \
    --generate-deterministic-analysis \
    --insert-deterministic-analysis
