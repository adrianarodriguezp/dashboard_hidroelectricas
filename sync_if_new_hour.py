#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pandas as pd
import psycopg2

from common_runtime import load_config, project_path


SCRIPT_DIR = project_path()
VENV_PYTHON = project_path("venv", "Scripts", "python.exe")
if not VENV_PYTHON.exists():
    VENV_PYTHON = project_path("venv", "bin", "python3")

HORARIO_SCRIPT = project_path("2_Datos_horarios_Hidroelectricas.py")
STATE_FILE = project_path("last_processed_ts.txt")
REPO_DIR = project_path()

PATHS_TO_COMMIT = [
    "TAB_3",
    "mapa_horario_hidro.html",
    "horario_todas_estaciones.csv",
]

EXPECTED_OUTPUTS = [
    project_path("TAB_3"),
    project_path("mapa_horario_hidro.html"),
    project_path("horario_todas_estaciones.csv"),
]

SQL_LAST_VALID_GLOBAL = """
SELECT MAX(fecha_toma_dato) AS last_ts
FROM temporales.caudales
WHERE valor_1h IS NOT NULL
  AND TRIM(valor_1h::text) NOT ILIKE 'nan'
  AND (valor_1h::double precision) > 0;
"""


def get_conn():
    params = load_config(section="Postgres")
    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        user=params["user"],
        password=params["password"],
        dbname=params["dbname"],
    )


def read_last_processed_ts() -> str | None:
    if not STATE_FILE.exists():
        return None
    value = STATE_FILE.read_text(encoding="utf-8").strip()
    return value or None


def write_last_processed_ts(ts_iso: str) -> None:
    STATE_FILE.write_text(ts_iso, encoding="utf-8")


def run(cmd: list[str], label: str, cwd: Path | None = None) -> None:
    print(f"\n{label}")
    print("   " + " ".join(str(part) for part in cmd))
    subprocess.run(cmd, check=True, cwd=str(cwd) if cwd else None)


def detect_last_ts_iso() -> str | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_LAST_VALID_GLOBAL)
            last_ts = cur.fetchone()[0]
    finally:
        conn.close()

    if last_ts is None:
        return None
    return pd.Timestamp(last_ts).isoformat()


def validate_outputs() -> None:
    missing = [path for path in EXPECTED_OUTPUTS if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Faltan salidas esperadas: {missing}")


def git_push_horario(commit_msg: str) -> bool:
    run(["git", "add", *PATHS_TO_COMMIT], "git add (horario)", cwd=REPO_DIR)

    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=str(REPO_DIR),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    if not status:
        print("No hay cambios para publicar.")
        return False

    run(["git", "commit", "-m", commit_msg], "git commit (horario)", cwd=REPO_DIR)
    remote = os.getenv("CENACE_GIT_REMOTE", "origin")
    branch = os.getenv("CENACE_GIT_BRANCH", "main")
    run(["git", "push", remote, f"HEAD:{branch}"], "git push (horario)", cwd=REPO_DIR)
    return True


def main() -> None:
    print("Watcher horario: inicia")

    last_ts_iso = detect_last_ts_iso()
    if last_ts_iso is None:
        print("No se encontró un timestamp válido en la base de datos.")
        return

    print(f"Último timestamp válido en BD: {last_ts_iso}")
    previous_ts = read_last_processed_ts()
    if previous_ts == last_ts_iso:
        print("No hay dato nuevo. Finaliza sin cambios.")
        return

    run([str(VENV_PYTHON), str(HORARIO_SCRIPT)], "Generación horaria", cwd=SCRIPT_DIR)
    validate_outputs()
    published = git_push_horario(commit_msg=f"Auto actualización horaria: {last_ts_iso}")
    write_last_processed_ts(last_ts_iso)

    if published:
        print("Watcher horario: publicación completada.")
    else:
        print("Watcher horario: no hubo cambios nuevos para publicar, pero el estado quedó sincronizado.")


if __name__ == "__main__":
    main()
