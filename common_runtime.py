from __future__ import annotations

import os
from configparser import ConfigParser
from pathlib import Path
from typing import Iterable

import pandas as pd


STATION_COORDS: dict[str, tuple[float, float]] = {
    "Agoyan": (-1.3977000, -78.3829000),
    "Amaluza": (-2.5859180, -78.5583440),
    "Amaluza_Laterales": (-2.5859180, -78.5583440),
    "Amaluza_Total": (-2.5859180, -78.5583440),
    "Coca Codo Sinclair": (-0.1990, -77.6827),
    "Daule_Peripa": (-0.9269476, -79.7482363),
    "Delsitanisagua": (-3.9803410, -79.0169830),
    "M_S_Francisco": (-3.3150547, -79.4821319),
    "Mazar": (-2.5972000, -78.6221000),
    "Pisayambo": (-1.0744510, -78.3968000),
}


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def discover_project_root(start: Path | None = None) -> Path:
    env_root = os.getenv("CENACE_ROOT")
    if env_root:
        path = Path(env_root).expanduser().resolve()
        if path.exists():
            return path

    current = (start or Path(__file__).resolve().parent).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "config.ini").exists() or (candidate / ".git").exists():
            return candidate
    return current


PROJECT_ROOT = discover_project_root()


def project_path(*parts: str) -> Path:
    return PROJECT_ROOT.joinpath(*parts)


def load_config(filename: str = "config.ini", section: str = "Postgres", base_dir: Path | None = None) -> dict[str, str]:
    config_path = (base_dir or PROJECT_ROOT) / filename
    parser = ConfigParser()
    read_files = parser.read(config_path, encoding="utf-8")
    if not read_files:
        raise FileNotFoundError(f"No se pudo leer el archivo de configuración: {config_path}")
    if not parser.has_section(section):
        raise KeyError(f"No existe la sección [{section}] en {config_path}")
    return {k: v for k, v in parser.items(section)}


def normalize_station_name(name: str) -> str:
    return str(name).strip()


def station_coordinates() -> dict[str, tuple[float, float]]:
    return dict(STATION_COORDS)


def safe_name(value: str) -> str:
    cleaned = []
    for char in str(value).strip():
        if char.isalnum() or char in {"_", "-", "."}:
            cleaned.append(char)
        elif char in {" ", "/", "\\"}:
            cleaned.append("_")
    return "".join(cleaned)


def apply_station_coordinates(
    df: pd.DataFrame,
    station_col: str = "nombre_estacion",
    lat_col: str = "latitud",
    lon_col: str = "longitud",
) -> pd.DataFrame:
    if df.empty or station_col not in df.columns:
        return df

    out = df.copy()
    for station, (lat, lon) in STATION_COORDS.items():
        mask = out[station_col].astype(str).str.strip() == station
        if mask.any():
            out.loc[mask, lat_col] = lat
            out.loc[mask, lon_col] = lon
    return out


def should_open_browser() -> bool:
    return _env_flag("CENACE_OPEN_BROWSER", default=False)


def analysis_years(reference_year: int | None = None, history_years: int = 2) -> list[int]:
    current_year = reference_year or pd.Timestamp.now().year
    start_year = current_year - history_years
    return list(range(start_year, current_year + 1))


def month_note(reference: pd.Timestamp | None = None) -> str:
    ts = reference or pd.Timestamp.now()
    month_names = {
        1: "enero",
        2: "febrero",
        3: "marzo",
        4: "abril",
        5: "mayo",
        6: "junio",
        7: "julio",
        8: "agosto",
        9: "septiembre",
        10: "octubre",
        11: "noviembre",
        12: "diciembre",
    }
    return (
        f"El valor de {month_names[ts.month]} de {ts.year} corresponde al mes en curso "
        "y representa un promedio parcial acumulado a la fecha."
    )


def ensure_directories(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
