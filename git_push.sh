#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE="${CENACE_GIT_REMOTE:-origin}"
BRANCH="${CENACE_GIT_BRANCH:-main}"
cd "$SCRIPT_DIR"

git add -- index.html TAB_1 TAB_2 TAB_3 TAB_4 \
    mapa_diario_hidro.html mapa_horario_hidro.html \
    promedio_diario_todas_estaciones.csv horario_todas_estaciones.csv

if git diff --cached --quiet; then
    echo "No hay cambios para publicar."
    exit 0
fi

git commit -m "Auto actualización dashboard: $(date +'%Y-%m-%d %H:%M')"
git push "$REMOTE" "HEAD:$BRANCH"
echo "Cambios publicados correctamente en $REMOTE/$BRANCH."
