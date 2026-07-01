#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a semi-automated hydropower DOCX report draft.

The script captures dashboard/forecast charts as PNG files, copies a DOCX
template, and replaces the nine body figure images while preserving the
template layout, headers, footers, and paragraph structure.
"""

from __future__ import annotations

import argparse
import base64
import copy
import csv
import json
import math
import os
import re
import shutil
import struct
import sys
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = PROJECT_DIR / "boletin_hidroelectricas_config.json"
REPORT_COPY_DIR = Path(
    "/mnt/carpeta-compartida-DPA/Carpetas_personales/CRISTINA OJEDA/Hidroelectricas_INFORME"
)
NS = {
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
}

DISCLAIMER_TEXT = (
    "Nota de descargo: La información presentada fue proporcionada por CENACE y se utiliza únicamente como referencia "
    "para el monitoreo hidrológico. Los datos se encuentran sujetos a revisión y validación técnica por parte de las "
    "entidades generadoras, por lo que no deben considerarse como información oficial confirmada. Para análisis "
    "operativos o de mayor detalle, se recomienda solicitar los registros validados a las unidades de negocio "
    "correspondientes de CELEC."
)


MONTHS_ES = {
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

MONTHS_TITLE = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}


@dataclass(frozen=True)
class FigureSpec:
    number: int
    source_type: str
    source: str
    selector: str
    media_name: str
    caption: str
    viewport: dict[str, int]
    wait_ms: int


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo de configuracion: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Fecha del reporte en formato YYYY-MM-DD. Por defecto usa hoy.",
    )
    parser.add_argument(
        "--skip-screenshots",
        action="store_true",
        help="No captura imagenes; reutiliza PNG existentes en la carpeta de salida.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida configuracion, fuentes y template sin crear el DOCX final.",
    )
    parser.add_argument(
        "--generate-analysis",
        action="store_true",
        help="Genera borradores de analisis en espanol para cada figura usando un LLM.",
    )
    parser.add_argument(
        "--generate-deterministic-analysis",
        action="store_true",
        help="Genera analisis en espanol desde datos y plantillas, sin usar LLM.",
    )
    parser.add_argument(
        "--insert-deterministic-analysis",
        action="store_true",
        help="Inserta los textos deterministas en el DOCX reemplazando la narrativa vieja.",
    )
    parser.add_argument(
        "--analysis-only",
        action="store_true",
        help="Genera solo textos de analisis usando datos/PNG existentes; no crea DOCX.",
    )
    parser.add_argument(
        "--llm-model",
        type=str,
        default=None,
        help="Modelo OpenAI para analisis visual. Por defecto usa config.llm.model.",
    )
    return parser.parse_args()


def report_date_from_args(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def resolve_project_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_DIR / path
    return path


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def copy_report_to_shared_folder(output_docx: Path) -> Path:
    REPORT_COPY_DIR.mkdir(parents=True, exist_ok=True)
    copy_path = REPORT_COPY_DIR / output_docx.name
    shutil.copy2(output_docx, copy_path)
    return copy_path


def safe_replace_text(text: str, replacements: dict[str, str]) -> str:
    out = text
    for old, new in replacements.items():
        out = out.replace(old, new)
    return out


def build_replacements(report_day: date, config: dict[str, Any]) -> dict[str, str]:
    report_month_title = MONTHS_TITLE[report_day.month].upper()
    year = str(report_day.year)
    elaboration = config["report"].get("elaboration", "A. R.")

    return {
        "MAYO – 2026": f"{report_month_title} - {year}",
        "MAYO - 2026": f"{report_month_title} - {year}",
        "MAYO– 2026": f"{report_month_title} - {year}",
        "Elaboración: A. R.": f"Elaboración: {elaboration}",
    }


def html_or_url(spec: FigureSpec) -> str:
    if spec.source_type == "url":
        return spec.source
    if spec.source_type != "html":
        raise ValueError(f"source_type no soportado para figura {spec.number}: {spec.source_type}")
    path = resolve_project_path(spec.source)
    if not path.exists():
        raise FileNotFoundError(f"No existe la fuente HTML de figura {spec.number}: {path}")
    return path.as_uri()


def render_plotly_api_html(spec: FigureSpec, out_png: Path, report_day: date) -> str:
    api_url = spec.source.format(date=report_day.isoformat())
    with urllib.request.urlopen(api_url, timeout=90) as response:
        payload = json.loads(response.read().decode("utf-8"))

    figure = payload.get("fp") or payload.get("figure") or payload
    if not isinstance(figure, dict) or "data" not in figure:
        raise RuntimeError(f"La API de figura {spec.number} no devolvio un objeto Plotly valido: {api_url}")

    html_path = out_png.with_suffix(".html")
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
  <style>
    html, body {{ margin: 0; padding: 0; background: rgb(58,64,74); }}
    #plot {{ width: {spec.viewport["width"]}px; height: {spec.viewport["height"]}px; }}
  </style>
</head>
<body>
  <div id="plot"></div>
  <script>
    const fig = {json.dumps(figure, ensure_ascii=False)};
    Plotly.newPlot('plot', fig.data || [], fig.layout || {{}}, {{displayModeBar: false, responsive: false}});
  </script>
</body>
</html>
"""
    html_path.write_text(html, encoding="utf-8")
    return html_path.as_uri()


def hourly_filter_strings(report_day: date, window_days: int = 10) -> tuple[str, str]:
    start_dt = datetime.combine(report_day - timedelta(days=window_days), time.min)
    end_dt = datetime.combine(report_day, time(23, 59))
    return start_dt.strftime("%Y-%m-%d %H:%M"), end_dt.strftime("%Y-%m-%d %H:%M")


async def apply_hourly_figure_filter(page: Any, report_day: date) -> None:
    start_text, end_text = hourly_filter_strings(report_day)
    applied = await page.evaluate(
        """
        ([inicio, fin]) => {
            const ini = document.querySelector('#fecha-inicial');
            const finInput = document.querySelector('#fecha-final');
            if (!ini || !finInput || typeof filtrar !== 'function') {
                return false;
            }
            ini.value = inicio;
            finInput.value = fin;
            filtrar();
            return true;
        }
        """,
        [start_text, end_text],
    )
    if not applied:
        raise RuntimeError("No se pudo aplicar el filtro horario de 10 dias en la figura.")


async def capture_one(page: Any, spec: FigureSpec, out_png: Path, report_day: date) -> None:
    if spec.source_type == "manual_png":
        source = resolve_project_path(spec.source)
        if not source.exists():
            raise FileNotFoundError(f"No existe PNG manual de figura {spec.number}: {source}")
        ensure_parent(out_png)
        shutil.copyfile(source, out_png)
        return

    if spec.source_type == "plotly_api":
        url = render_plotly_api_html(spec, out_png, report_day)
        selector = "#plot"
    else:
        url = html_or_url(spec)
        selector = spec.selector
    await page.set_viewport_size(spec.viewport)
    await page.goto(url, wait_until="networkidle", timeout=90000)
    await page.wait_for_timeout(spec.wait_ms)
    if spec.number in {3, 8, 12}:
        await apply_hourly_figure_filter(page, report_day)
        await page.wait_for_timeout(spec.wait_ms)
    locator = page.locator(selector).first
    await locator.wait_for(state="visible", timeout=60000)
    ensure_parent(out_png)
    await locator.screenshot(path=str(out_png))


def capture_screenshots(figures: list[FigureSpec], fig_dir: Path, report_day: date) -> None:
    try:
        from playwright.async_api import async_playwright
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Falta instalar Playwright en el entorno. Ejecuta: "
            "venv/bin/pip install playwright && venv/bin/python -m playwright install chromium"
        ) from exc

    async def runner() -> None:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                for spec in figures:
                    out_png = fig_dir / f"figura_{spec.number:02d}.png"
                    print(f"Capturando figura {spec.number}: {spec.source}")
                    await capture_one(page, spec, out_png, report_day)
            finally:
                await browser.close()

    import asyncio

    asyncio.run(runner())


def validate_pngs(figures: list[FigureSpec], fig_dir: Path) -> None:
    missing = []
    invalid = []
    for spec in figures:
        path = fig_dir / f"figura_{spec.number:02d}.png"
        if not path.exists():
            missing.append(str(path))
            continue
        with path.open("rb") as f:
            sig = f.read(8)
            ihdr_len = f.read(4)
            ihdr_type = f.read(4)
            ihdr_data = f.read(8)
        if sig != b"\x89PNG\r\n\x1a\n" or ihdr_type != b"IHDR" or path.stat().st_size < 1000:
            invalid.append(str(path))
            continue
        width, height = struct.unpack(">II", ihdr_data)
        if width < 100 or height < 100:
            invalid.append(str(path))
    if missing or invalid:
        parts = []
        if missing:
            parts.append("faltan PNG: " + "; ".join(missing))
        if invalid:
            parts.append("PNG invalidos: " + "; ".join(invalid))
        raise RuntimeError("Validacion de figuras fallida: " + " | ".join(parts))


MASTER_ANALYSIS_PROMPT = """Eres un hidrologo tecnico que redacta boletines institucionales en espanol para monitoreo de caudales en hidroelectricas de Ecuador.

Analiza la imagen adjunta correspondiente a Figura {number}: {caption}.

Redacta un texto tecnico, claro y prudente, de 1 a 3 parrafos, para insertarlo debajo de la figura en un informe Word.

Instrucciones:
- Describe unicamente lo que se observa en la grafica.
- No inventes datos numericos exactos si no son claramente legibles.
- Menciona tendencias, incrementos, descensos, maximos relativos, minimos relativos y cambios importantes.
- Si la figura compara anos, compara 2026 contra 2024, 2025 y/o historico segun corresponda.
- Si la figura es de pronostico GEOGLOWS, no escribas valores numericos de caudal pronosticado. Describe solo tendencias, ascensos, descensos, estabilidad relativa, rango de incertidumbre y fechas aproximadas de picos o minimos relativos.
- Usa lenguaje institucional: "se observa", "presenta", "muestra", "se recomienda mantener el monitoreo".
- No uses vinetas.
- No incluyas titulo ni caption.
- No digas "la imagen muestra"; di "En la Figura {number}, se observa...".
- Si hay incertidumbre o datos sujetos a validacion, indicalo con cautela.
- Redacta en espanol tecnico, con tono sobrio.
"""


FIGURE_CONTEXT = {
    1: "La figura compara el caudal mensual de Coca Codo Sinclair contra el historico, percentiles y anos recientes. Enfocate en el comportamiento del mes en curso frente a la media historica, maximos/minimos historicos y anos 2024-2026.",
    2: "La figura compara el caudal diario de Coca Codo Sinclair para el mismo periodo calendario en 2024, 2025 y 2026. Describe la evolucion diaria de 2026 y comparala con 2024 y 2025.",
    3: "La figura muestra caudal horario de Coca Codo Sinclair. Describe variabilidad intra-diaria, ascensos o descensos abruptos, estabilidad relativa y eventos destacados.",
    4: "La figura corresponde al pronostico hidrologico GEOGLOWS para Coca Codo Sinclair. Por tratarse de un pronostico no calibrado, evita escribir valores de caudal pronosticado; describe la tendencia esperada, fechas aproximadas de picos o minimos relativos, banda percentil 25%-75% e incertidumbre.",
    5: "La figura corresponde al pronostico hidrologico GEOGLOWS Hydroviewer para el rio Quijos, COMID 620905703. Por tratarse de un pronostico no calibrado, evita escribir valores de caudal pronosticado; describe condiciones antecedentes de forma cualitativa, tendencia esperada, fechas aproximadas de picos o minimos relativos, rango percentilico y periodos de retorno si aparecen visibles.",
    6: "La figura compara el caudal mensual de Mazar contra el historico, percentiles y anos recientes. Enfocate en el mes en curso frente a la media historica y anos 2024-2026.",
    7: "La figura compara el caudal diario de Mazar para el mismo periodo calendario en 2024, 2025 y 2026. Describe la evolucion diaria de 2026 y comparala con 2024 y 2025.",
    8: "La figura muestra caudal horario de Mazar. Describe variabilidad horaria, ascensos, descensos, maximos relativos y persistencia de caudales altos o bajos.",
    9: "La figura corresponde al pronostico hidrologico GEOGLOWS para Mazar. Por tratarse de un pronostico no calibrado, evita escribir valores de caudal pronosticado; describe condiciones recientes de forma cualitativa, tendencia esperada, fechas aproximadas de picos o minimos relativos, banda percentil 25%-75% e incertidumbre.",
    10: "La figura compara el caudal mensual de Daule Peripa contra el historico, percentiles y anos recientes. Enfocate en el mes en curso frente a la media historica y anos 2024-2026.",
    11: "La figura compara el caudal diario de Daule Peripa para el mismo periodo calendario en 2024, 2025 y 2026. Describe la evolucion diaria de 2026 y comparala con 2024 y 2025.",
    12: "La figura muestra caudal horario de Daule Peripa. Describe variabilidad horaria, ascensos, descensos, maximos relativos y persistencia de caudales altos o bajos.",
    13: "La figura corresponde al pronostico hidrologico GEOGLOWS para Daule Peripa. Por tratarse de un pronostico no calibrado, evita escribir valores numericos de caudal pronosticado; describe condiciones recientes de forma cualitativa, tendencia esperada, fechas aproximadas de picos o minimos relativos, banda percentil 25%-75% e incertidumbre.",
}


def prompt_for_figure(spec: FigureSpec, report_day: date) -> str:
    context = FIGURE_CONTEXT.get(spec.number, "")
    return (
        MASTER_ANALYSIS_PROMPT.format(number=spec.number, caption=spec.caption)
        + "\nContexto adicional:\n"
        + context
        + f"\n\nFecha del reporte: {report_day.isoformat()}."
    )


def extract_response_text(payload: dict[str, Any]) -> str:
    if isinstance(payload.get("output_text"), str):
        return payload["output_text"].strip()
    pieces: list[str] = []
    for item in payload.get("output", []):
        for content in item.get("content", []):
            if content.get("type") in {"output_text", "text"} and content.get("text"):
                pieces.append(content["text"])
    return "\n".join(pieces).strip()


def call_openai_vision(model: str, prompt: str, image_path: Path) -> str:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Falta OPENAI_API_KEY en el entorno para generar analisis con LLM.")

    encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
    request_payload = {
        "model": model,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": f"data:image/png;base64,{encoded}"},
                ],
            }
        ],
    }
    data = json.dumps(request_payload).encode("utf-8")
    request = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"OpenAI API error {exc.code}: {detail}") from exc

    text = extract_response_text(response_payload)
    if not text:
        raise RuntimeError(f"La respuesta del LLM no incluyo texto para {image_path.name}.")
    return text


def generate_analysis_texts(
    figures: list[FigureSpec],
    fig_dir: Path,
    analysis_dir: Path,
    report_day: date,
    config: dict[str, Any],
    model_override: str | None,
) -> None:
    model = model_override or config.get("llm", {}).get("model", "gpt-4.1")
    analysis_dir.mkdir(parents=True, exist_ok=True)
    prompts: list[str] = []
    for spec in figures:
        prompt = prompt_for_figure(spec, report_day)
        prompt_path = analysis_dir / f"prompt_figura_{spec.number:02d}.txt"
        prompt_path.write_text(prompt, encoding="utf-8")
        prompts.append(f"## Prompt Figura {spec.number}\n\n{prompt}\n")
    (analysis_dir / "prompts_figuras.md").write_text("\n".join(prompts), encoding="utf-8")

    combined: list[str] = []
    for spec in figures:
        image_path = fig_dir / f"figura_{spec.number:02d}.png"
        prompt = prompt_for_figure(spec, report_day)
        output_path = analysis_dir / f"analisis_figura_{spec.number:02d}.txt"
        print(f"Generando analisis Figura {spec.number} con {model}...")
        text = call_openai_vision(model, prompt, image_path)
        output_path.write_text(text + "\n", encoding="utf-8")
        combined.append(f"## Figura {spec.number}\n\n{text}\n")
    (analysis_dir / "analisis_figuras.md").write_text("\n".join(combined), encoding="utf-8")


def fmt_value(value: float | int | None) -> str:
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return "sin dato"
    return f"{float(value):.2f}"


def fmt_m3s(value: float | int | None) -> str:
    return f"{fmt_value(value)} m³/s" if value is not None else "sin dato"


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "").replace(".000", ""))


def date_es(value: datetime | date | str) -> str:
    if isinstance(value, str):
        value = parse_dt(value)
    if isinstance(value, datetime):
        d = value.date()
    else:
        d = value
    return f"{d.day} de {MONTHS_ES[d.month]} de {d.year}"


def datetime_es(value: datetime | str) -> str:
    if isinstance(value, str):
        value = parse_dt(value)
    return f"{value.day} de {MONTHS_ES[value.month]} de {value.year} a las {value.hour:02d}:{value.minute:02d}"


def safe_pct(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return (numerator - denominator) / abs(denominator) * 100.0


def classify_trend(first: float, last: float, values: list[float]) -> str:
    if not values:
        return "sin tendencia definida"
    change = safe_pct(last, first)
    mean = sum(values) / len(values)
    spread = (max(values) - min(values)) / mean * 100.0 if mean else 0.0
    if spread > 80:
        return "variable"
    if abs(change) < 5 and spread < 20:
        return "estable"
    if change >= 10:
        return "ascendente"
    if change <= -10:
        return "descendente"
    return "variable"


def extract_js_array(html_path: Path, name: str) -> list[dict[str, Any]]:
    text = html_path.read_text(encoding="utf-8")
    match = re.search(rf"const\s+{re.escape(name)}\s*=\s*(\[.*?\]);", text, re.S)
    if not match:
        raise RuntimeError(f"No se encontro const {name} en {html_path}")
    return json.loads(match.group(1))


def extract_js_string(html_path: Path, name: str) -> str | None:
    text = html_path.read_text(encoding="utf-8")
    match = re.search(rf"const\s+{re.escape(name)}\s*=\s*\"([^\"]+)\";", text)
    return match.group(1) if match else None


def csv_monthly_metrics(station_file_stem: str, station_label: str, report_day: date) -> dict[str, Any]:
    path = PROJECT_DIR / "Analisis_Q_outliers" / "08_mensualizado_corregido" / f"{station_file_stem}_mensualizado_corregido.csv"
    if not path.exists():
        raise FileNotFoundError(f"No existe CSV mensual: {path}")
    month_key = {
        1: "ENE", 2: "FEB", 3: "MAR", 4: "ABR", 5: "MAY", 6: "JUN",
        7: "JUL", 8: "AGO", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DIC",
    }[report_day.month]
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    by_year: dict[int, float] = {}
    for row in rows:
        try:
            year = int(float(row["Year"]))
            raw = row.get(month_key, "")
            if raw not in {"", "nan", "NaN", None}:
                by_year[year] = float(raw)
        except (ValueError, TypeError):
            continue
    historical_values = [v for y, v in by_year.items() if y <= 2023]
    value_2026 = by_year.get(report_day.year)
    mean_hist = sum(historical_values) / len(historical_values) if historical_values else None
    min_hist = min(historical_values) if historical_values else None
    max_hist = max(historical_values) if historical_values else None
    return {
        "source": str(path),
        "station": station_label,
        "month": MONTHS_ES[report_day.month],
        "year": report_day.year,
        "period_end": date_es(report_day),
        "is_month_end": (report_day + timedelta(days=1)).month != report_day.month,
        "value_2026": value_2026,
        "value_2025": by_year.get(report_day.year - 1),
        "value_2024": by_year.get(report_day.year - 2),
        "historical_mean": mean_hist,
        "historical_min": min_hist,
        "historical_max": max_hist,
        "pct_vs_historical_mean": safe_pct(value_2026, mean_hist) if value_2026 is not None and mean_hist is not None else None,
    }


def monthly_text(fig: int, metrics: dict[str, Any]) -> str:
    direction = "por encima" if (metrics.get("pct_vs_historical_mean") or 0) >= 0 else "por debajo"
    pct = abs(metrics.get("pct_vs_historical_mean") or 0)
    comp_2025 = "superior" if (metrics["value_2026"] or 0) >= (metrics.get("value_2025") or 0) else "inferior"
    monthly_label = "caudal mensual" if metrics.get("is_month_end") else "caudal mensual parcial"
    return (
        f"En la Figura {fig}, se observa que el {monthly_label} de {metrics['station']} para "
        f"{metrics['month']} de {metrics['year']} alcanza {fmt_m3s(metrics['value_2026'])}. "
        f"Este valor se ubica {direction} de la media histórica mensual ({fmt_m3s(metrics['historical_mean'])}), "
        f"con una diferencia aproximada de {fmt_value(pct)} %. En el contexto histórico, el rango observado para "
        f"este mes se encuentra entre {fmt_m3s(metrics['historical_min'])} y {fmt_m3s(metrics['historical_max'])}."
        f"\n\nRespecto a los años recientes, el valor de {metrics['year']} es {comp_2025} al registrado en "
        f"{metrics['year'] - 1} ({fmt_m3s(metrics.get('value_2025'))}) y se compara con {fmt_m3s(metrics.get('value_2024'))} "
        f"en {metrics['year'] - 2}."
    )


def daily_metrics(html_rel_path: str, station_label: str) -> dict[str, Any]:
    path = resolve_project_path(html_rel_path)
    records = extract_js_array(path, "registros")
    default_ini = extract_js_string(path, "defaultIni")
    default_fin = extract_js_string(path, "defaultFin")
    if not default_ini or not default_fin:
        raise RuntimeError(f"No se encontraron defaultIni/defaultFin en {path}")
    start = datetime.strptime(default_ini, "%Y-%m-%d").date()
    end = datetime.strptime(default_fin, "%Y-%m-%d").date()
    current_year = end.year
    rows = []
    for r in records:
        d = datetime.strptime(r["Fecha"], "%Y-%m-%d").date()
        if r.get("Anio") == current_year and start <= d <= end:
            rows.append((d, float(r["Caudal Diario Promedio"]), r["Mes_Dia"]))
    rows.sort(key=lambda x: x[0])
    if not rows:
        raise RuntimeError(f"No hay datos diarios para {station_label} en {start} - {end}")
    values = [r[1] for r in rows]
    max_row = max(rows, key=lambda x: x[1])
    min_row = min(rows, key=lambda x: x[1])
    comp_counts = {"2024": {"above": 0, "total": 0}, "2025": {"above": 0, "total": 0}}
    by_key = {(int(r["Anio"]), r["Mes_Dia"]): float(r["Caudal Diario Promedio"]) for r in records}
    for _, value, md in rows:
        for year in [current_year - 2, current_year - 1]:
            other = by_key.get((year, md))
            if other is not None:
                key = str(year)
                comp_counts[key]["total"] += 1
                if value >= other:
                    comp_counts[key]["above"] += 1
    return {
        "source": str(path),
        "station": station_label,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "first_value": rows[0][1],
        "last_value": rows[-1][1],
        "max_date": max_row[0].isoformat(),
        "max_value": max_row[1],
        "min_date": min_row[0].isoformat(),
        "min_value": min_row[1],
        "trend": classify_trend(rows[0][1], rows[-1][1], values),
        "net_change_pct": safe_pct(rows[-1][1], rows[0][1]),
        "comparison_counts": comp_counts,
    }


def comparison_phrase(counts: dict[str, dict[str, int]]) -> str:
    phrases = []
    for year in ["2024", "2025"]:
        total = counts[year]["total"]
        above = counts[year]["above"]
        if total:
            pct = above / total * 100.0
            if pct >= 60:
                phrases.append(f"por encima de {year} en {above} de {total} días comparables")
            elif pct <= 40:
                phrases.append(f"por debajo de {year} en la mayor parte del periodo ({above} de {total} días por encima)")
            else:
                phrases.append(f"en un rango similar al de {year} ({above} de {total} días por encima)")
    return " y ".join(phrases) if phrases else "sin comparaciones suficientes con años previos"


def daily_text(fig: int, metrics: dict[str, Any]) -> str:
    start = date_es(metrics["start"])
    end = date_es(metrics["end"])
    return (
        f"En la Figura {fig}, durante el periodo comprendido entre el {start} y el {end}, el caudal diario de "
        f"{metrics['station']} presentó un comportamiento {metrics['trend']}. El valor máximo del periodo se registró "
        f"el {date_es(metrics['max_date'])}, con {fmt_m3s(metrics['max_value'])}, mientras que el valor mínimo se observó "
        f"el {date_es(metrics['min_date'])}, con {fmt_m3s(metrics['min_value'])}."
        f"\n\nEntre el inicio y el final del periodo, el caudal pasó de {fmt_m3s(metrics['first_value'])} a "
        f"{fmt_m3s(metrics['last_value'])}, equivalente a una variación aproximada de {fmt_value(metrics['net_change_pct'])} %. "
        f"Frente a los años recientes, la serie de 2026 se ubicó {comparison_phrase(metrics['comparison_counts'])}. "
        "Por tanto, se recomienda mantener el monitoreo continuo de la evolución diaria del caudal."
    )


def unavailable_text(fig: int, station_label: str, data_kind: str) -> str:
    return (
        f"En la Figura {fig}, la seccion de {data_kind} de {station_label} queda incorporada al boletin. "
        "Al momento de la generacion no se dispone de datos suficientes en la fuente configurada para calcular metricas "
        "y redactar una interpretacion cuantitativa. Cuando se actualicen los registros, el analisis se generara automaticamente "
        "a partir de la informacion disponible."
    )


def hourly_metrics(html_rel_path: str, station_label: str, report_day: date, window_days: int = 10) -> dict[str, Any]:
    path = resolve_project_path(html_rel_path)
    records = extract_js_array(path, "registros")
    parsed = [(parse_dt(r["FechaHora"]), float(r["valor_1h"])) for r in records if r.get("valor_1h") is not None]
    start_text, end_text = hourly_filter_strings(report_day, window_days)
    start_cut = datetime.strptime(start_text, "%Y-%m-%d %H:%M")
    end_cut = datetime.strptime(end_text, "%Y-%m-%d %H:%M")
    parsed = [(dt, v) for dt, v in parsed if start_cut <= dt <= end_cut]
    parsed.sort(key=lambda x: x[0])
    if not parsed:
        raise RuntimeError(f"No hay datos horarios para {station_label} entre {start_text} y {end_text}")
    rows = parsed
    values = [r[1] for r in rows]
    max_row = max(rows, key=lambda x: x[1])
    min_row = min(rows, key=lambda x: x[1])
    return {
        "source": str(path),
        "station": station_label,
        "start": rows[0][0].isoformat(timespec="minutes"),
        "end": rows[-1][0].isoformat(timespec="minutes"),
        "first_value": rows[0][1],
        "last_value": rows[-1][1],
        "max_datetime": max_row[0].isoformat(timespec="minutes"),
        "max_value": max_row[1],
        "min_datetime": min_row[0].isoformat(timespec="minutes"),
        "min_value": min_row[1],
        "trend": classify_trend(rows[0][1], rows[-1][1], values),
        "net_change_pct": safe_pct(rows[-1][1], rows[0][1]),
    }


def hourly_text(fig: int, metrics: dict[str, Any]) -> str:
    return (
        f"En la Figura {fig}, el caudal horario de {metrics['station']} entre el {datetime_es(metrics['start'])} y el "
        f"{datetime_es(metrics['end'])} mostró un comportamiento {metrics['trend']}. En la ventana analizada, el valor máximo "
        f"se registró el {datetime_es(metrics['max_datetime'])}, con {fmt_m3s(metrics['max_value'])}, mientras que el mínimo "
        f"se presentó el {datetime_es(metrics['min_datetime'])}, con {fmt_m3s(metrics['min_value'])}."
        f"\n\nEl caudal pasó de {fmt_m3s(metrics['first_value'])} al inicio de la ventana a {fmt_m3s(metrics['last_value'])} "
        f"al final, con una variación aproximada de {fmt_value(metrics['net_change_pct'])} %. Esta variabilidad horaria debe "
        "interpretarse como una señal de seguimiento operativo, por lo que se recomienda mantener la revisión de los registros recientes."
    )


def fetch_plotly_payload(source: str, report_day: date) -> tuple[str, dict[str, Any]]:
    url = source.format(date=report_day.isoformat())
    with urllib.request.urlopen(url, timeout=120) as response:
        payload = json.loads(response.read().decode("utf-8"))
    figure = payload.get("fp") or payload.get("figure") or payload
    if "data" not in figure:
        raise RuntimeError(f"La fuente no contiene figura Plotly: {url}")
    return url, figure


def trace_by_name(figure: dict[str, Any], names: list[str]) -> dict[str, Any] | None:
    for trace in figure.get("data", []):
        name = str(trace.get("name", "")).lower()
        if any(n.lower() in name for n in names):
            return trace
    return None


def numeric_pairs(trace: dict[str, Any] | None) -> list[tuple[datetime, float]]:
    if not trace:
        return []
    xvals = trace.get("x") or []
    yvals = trace.get("y") or []
    out = []
    for x, y in zip(xvals, yvals):
        if y is None:
            continue
        try:
            out.append((parse_dt(x), float(y)))
        except (ValueError, TypeError):
            continue
    return out


def forecast_metrics(source: str, label: str, report_day: date, hydroviewer: bool = False) -> dict[str, Any]:
    url, figure = fetch_plotly_payload(source, report_day)
    mean_trace = trace_by_name(figure, ["media", "promedio del ensamble", "promedio pronosticado"])
    observed_trace = trace_by_name(figure, ["observado", "condiciones antecedentes"])
    high_res_trace = trace_by_name(figure, ["alta resolución"])
    mean_pairs = numeric_pairs(mean_trace)
    observed_pairs = numeric_pairs(observed_trace)
    high_res_pairs = numeric_pairs(high_res_trace)
    forecast_pairs = mean_pairs or high_res_pairs
    if not forecast_pairs:
        raise RuntimeError(f"No hay serie media/alta resolucion para pronostico {label}")
    values = [v for _, v in forecast_pairs]
    max_row = max(forecast_pairs, key=lambda x: x[1])
    min_row = min(forecast_pairs, key=lambda x: x[1])
    rp_names = [str(t.get("name", "")) for t in figure.get("data", []) if "años" in str(t.get("name", "")).lower()]
    return {
        "source": url,
        "label": label,
        "start": forecast_pairs[0][0].isoformat(timespec="minutes"),
        "end": forecast_pairs[-1][0].isoformat(timespec="minutes"),
        "first_forecast": forecast_pairs[0][1],
        "last_forecast": forecast_pairs[-1][1],
        "max_datetime": max_row[0].isoformat(timespec="minutes"),
        "max_value": max_row[1],
        "min_datetime": min_row[0].isoformat(timespec="minutes"),
        "min_value": min_row[1],
        "trend": classify_trend(forecast_pairs[0][1], forecast_pairs[-1][1], values),
        "observed_last": observed_pairs[-1][1] if observed_pairs else None,
        "observed_start": observed_pairs[0][1] if observed_pairs else None,
        "high_res_last": high_res_pairs[-1][1] if high_res_pairs else None,
        "return_periods_visible": rp_names[:8],
        "hydroviewer": hydroviewer,
    }


def forecast_text(fig: int, metrics: dict[str, Any]) -> str:
    rp = ""
    if metrics.get("return_periods_visible"):
        rp = " La figura incluye referencias de periodos de retorno visibles, que deben interpretarse como umbrales operativos de referencia y no como una validacion cuantitativa del pronostico."
    observed = ""
    if metrics.get("observed_last") is not None:
        observed = (
            " Las condiciones observadas o antecedentes sirven como referencia inicial para interpretar la evolucion prevista."
        )
    high_res = ""
    if metrics.get("high_res_last") is not None:
        high_res = " El pronostico de alta resolucion se considera como apoyo cualitativo para revisar la direccion del cambio esperado."
    extended_horizon = ""
    if metrics.get("hydroviewer"):
        extended_horizon = (
            "\n\nA partir del quinto dia, el pronostico presenta mayor incertidumbre por la dispersion del ensamble, "
            "por lo que el horizonte extendido debe usarse solo como referencia de tendencia."
        )
    return (
        f"En la Figura {fig}, el pronostico GEOGLOWS para {metrics['label']}, entre el {date_es(metrics['start'])} y el "
        f"{date_es(metrics['end'])}, muestra una tendencia media {metrics['trend']} respecto a las condiciones observadas o antecedentes. "
        f"El pico relativo del horizonte se identifica alrededor del {date_es(metrics['max_datetime'])}, "
        f"mientras que el minimo relativo se ubica alrededor del {date_es(metrics['min_datetime'])}."
        f"{extended_horizon}\n\nLa interpretacion debe ser cualitativa, considerando las condiciones antecedentes, la banda de incertidumbre "
        f"y los cambios entre corridas del modelo; los valores puntuales no deben asumirse como caudales operativos confirmados."
        f"{observed}{high_res}{rp} Se recomienda mantener el monitoreo permanente de las condiciones hidrometeorologicas en la cuenca de aporte "
        "y actualizar el analisis con cada nueva corrida del modelo."
    )
    return (
        f"En la Figura {fig}, el pronóstico hidrológico para {metrics['label']} entre el {datetime_es(metrics['start'])} y el "
        f"{datetime_es(metrics['end'])} presenta una tendencia {metrics['trend']} en la serie media del pronóstico. "
        f"El pico relativo del horizonte se identifica alrededor del {datetime_es(metrics['max_datetime'])}, "
        f"mientras que el minimo relativo se ubica alrededor del {datetime_es(metrics['min_datetime'])}."
        f"\n\nLa evolucion esperada debe interpretarse de forma cualitativa, considerando la dispersion de la banda de incertidumbre "
        f"y los cambios entre corridas del modelo.{observed}{high_res}{rp} Dado que se trata de un pronostico GEOGLOWS no calibrado, "
        "se recomienda mantener el seguimiento de la tendencia y actualizar el analisis con cada nueva corrida, sin tomar los valores puntuales como caudales operativos confirmados."
    )


def short_date_es(value: datetime | date | str) -> str:
    if isinstance(value, str):
        value = parse_dt(value)
    if isinstance(value, datetime):
        value = value.date()
    return f"{value.day} de {MONTHS_ES[value.month]}"


def caption_date_range(start: str, end: str, include_time: bool = False) -> str:
    start_dt = parse_dt(start)
    end_dt = parse_dt(end)
    if include_time:
        return f"{datetime_es(start_dt)} al {datetime_es(end_dt)}"
    if start_dt.year == end_dt.year:
        return f"{short_date_es(start_dt)} al {date_es(end_dt)}"
    return f"{date_es(start_dt)} al {date_es(end_dt)}"


def build_caption_overrides(metrics_by_fig: dict[int, dict[str, Any]]) -> dict[int, str]:
    captions: dict[int, str] = {}
    daily_coca_end = metrics_by_fig.get(2, {}).get("end")
    daily_mazar_end = metrics_by_fig.get(7, {}).get("end")
    daily_daule_end = metrics_by_fig.get(11, {}).get("end")

    if 1 in metrics_by_fig:
        end_label = date_es(daily_coca_end) if daily_coca_end else metrics_by_fig[1].get("period_end", "")
        captions[1] = f"Figura 1. Comparación mensual de caudales en Coca Codo S. (hasta {end_label})"
    if 2 in metrics_by_fig:
        m = metrics_by_fig[2]
        captions[2] = (
            "Figura 2. Comparación de caudal diario "
            f"del {caption_date_range(m['start'], m['end'])} en Coca Codo Sinclair. Periodo 2024 - 2026"
        )
    if 3 in metrics_by_fig:
        m = metrics_by_fig[3]
        captions[3] = (
            "Figura 3. Caudal horario "
            f"entre el {datetime_es(m['start'])} y el {datetime_es(m['end'])} en Coca Codo Sinclair"
        )
    if 4 in metrics_by_fig:
        captions[4] = f"Figura 4. Pronóstico en Coca Codo Sinclair, hasta el {date_es(metrics_by_fig[4]['end'])}"
    if 5 in metrics_by_fig:
        captions[5] = f"Figura 5. Pronóstico en río Quijos, hasta el {date_es(metrics_by_fig[5]['end'])}"
    if 6 in metrics_by_fig:
        end_label = date_es(daily_mazar_end) if daily_mazar_end else metrics_by_fig[6].get("period_end", "")
        captions[6] = f"Figura 6. Comparación mensual de caudales en Mazar (hasta {end_label})"
    if 7 in metrics_by_fig:
        m = metrics_by_fig[7]
        captions[7] = (
            "Figura 7. Comparación de caudal diario "
            f"del {caption_date_range(m['start'], m['end'])} en Mazar. Periodo 2024 - 2026"
        )
    if 8 in metrics_by_fig:
        m = metrics_by_fig[8]
        captions[8] = f"Figura 8. Caudal horario del {caption_date_range(m['start'], m['end'], include_time=True)} en Mazar"
    if 9 in metrics_by_fig:
        captions[9] = f"Figura 9. Pronóstico en Mazar, hasta el {date_es(metrics_by_fig[9]['end'])}"
    if 10 in metrics_by_fig:
        end_label = date_es(daily_daule_end) if daily_daule_end else metrics_by_fig[10].get("period_end", "")
        captions[10] = f"Figura 10. ComparaciÃ³n mensual de caudales en Daule Peripa (hasta {end_label})"
    if 11 in metrics_by_fig:
        m = metrics_by_fig[11]
        captions[11] = (
            "Figura 11. ComparaciÃ³n de caudal diario "
            f"del {caption_date_range(m['start'], m['end'])} en Daule Peripa. Periodo 2024 - 2026"
        )
    if 12 in metrics_by_fig:
        m = metrics_by_fig[12]
        captions[12] = f"Figura 12. Caudal horario entre el {datetime_es(m['start'])} y el {datetime_es(m['end'])} en Daule Peripa"
    if 13 in metrics_by_fig:
        captions[13] = f"Figura 13. PronÃ³stico en Daule Peripa, hasta el {date_es(metrics_by_fig[13]['end'])}"
    if 10 in metrics_by_fig:
        end_label = date_es(daily_daule_end) if daily_daule_end else metrics_by_fig[10].get("period_end", "")
        captions[10] = f"Figura 10. Comparacion mensual de caudales en Daule Peripa (hasta {end_label})"
    if 11 in metrics_by_fig:
        m = metrics_by_fig[11]
        if m.get("unavailable"):
            captions[11] = "Figura 11. Comparacion de caudal diario en Daule Peripa."
        else:
            captions[11] = (
                "Figura 11. Comparacion de caudal diario "
                f"del {caption_date_range(m['start'], m['end'])} en Daule Peripa. Periodo 2024 - 2026"
            )
    if 12 in metrics_by_fig:
        m = metrics_by_fig[12]
        if m.get("unavailable"):
            captions[12] = "Figura 12. Caudal horario en Daule Peripa."
        else:
            captions[12] = f"Figura 12. Caudal horario entre el {datetime_es(m['start'])} y el {datetime_es(m['end'])} en Daule Peripa"
    if 13 in metrics_by_fig:
        captions[13] = f"Figura 13. Pronostico en Daule Peripa, hasta el {date_es(metrics_by_fig[13]['end'])}"
    return captions


def load_metrics_by_figure(analysis_dir: Path) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for path in sorted(analysis_dir.glob("metricas_figura_*.json")):
        match = re.search(r"metricas_figura_(\d+)\.json$", path.name)
        if match:
            out[int(match.group(1))] = json.loads(path.read_text(encoding="utf-8"))
    return out


def deterministic_analysis_for_figure(spec: FigureSpec, report_day: date) -> tuple[dict[str, Any], str]:
    if spec.number == 1:
        metrics = csv_monthly_metrics("Coca_Codo_Sinclair", "Coca Codo Sinclair", report_day)
        return metrics, monthly_text(spec.number, metrics)
    if spec.number == 6:
        metrics = csv_monthly_metrics("Mazar", "Mazar", report_day)
        return metrics, monthly_text(spec.number, metrics)
    if spec.number == 2:
        metrics = daily_metrics("TAB_4/grafico_Coca_Codo_Sinclair.html", "Coca Codo Sinclair")
        return metrics, daily_text(spec.number, metrics)
    if spec.number == 7:
        metrics = daily_metrics("TAB_4/grafico_Mazar.html", "Mazar")
        return metrics, daily_text(spec.number, metrics)
    if spec.number == 3:
        metrics = hourly_metrics("TAB_3/grafico_Coca_Codo_Sinclair.html", "Coca Codo Sinclair", report_day)
        return metrics, hourly_text(spec.number, metrics)
    if spec.number == 8:
        metrics = hourly_metrics("TAB_3/grafico_Mazar.html", "Mazar", report_day)
        return metrics, hourly_text(spec.number, metrics)
    if spec.number == 4:
        metrics = forecast_metrics(spec.source, "Coca Codo Sinclair", report_day)
        return metrics, forecast_text(spec.number, metrics)
    if spec.number == 5:
        metrics = forecast_metrics(spec.source, "río Quijos (COMID 620905703)", report_day, hydroviewer=True)
        return metrics, forecast_text(spec.number, metrics)
    if spec.number == 9:
        metrics = forecast_metrics(spec.source, "Mazar", report_day)
        return metrics, forecast_text(spec.number, metrics)
    if spec.number == 10:
        metrics = csv_monthly_metrics("Daule_Peripa", "Daule Peripa", report_day)
        return metrics, monthly_text(spec.number, metrics)
    if spec.number == 11:
        try:
            metrics = daily_metrics("TAB_4/grafico_Daule_Peripa.html", "Daule Peripa")
            return metrics, daily_text(spec.number, metrics)
        except RuntimeError as exc:
            metrics = {
                "source": str(resolve_project_path("TAB_4/grafico_Daule_Peripa.html")),
                "station": "Daule Peripa",
                "start": report_day.isoformat(),
                "end": report_day.isoformat(),
                "unavailable": True,
                "reason": str(exc),
            }
            return metrics, unavailable_text(spec.number, "Daule Peripa", "caudal diario")
    if spec.number == 12:
        try:
            metrics = hourly_metrics("TAB_3/grafico_Daule_Peripa.html", "Daule Peripa", report_day)
            return metrics, hourly_text(spec.number, metrics)
        except RuntimeError as exc:
            metrics = {
                "source": str(resolve_project_path("TAB_3/grafico_Daule_Peripa.html")),
                "station": "Daule Peripa",
                "start": datetime.combine(report_day, time.min).isoformat(timespec="minutes"),
                "end": datetime.combine(report_day, time.min).isoformat(timespec="minutes"),
                "unavailable": True,
                "reason": str(exc),
            }
            return metrics, unavailable_text(spec.number, "Daule Peripa", "caudal horario")
    if spec.number == 13:
        metrics = forecast_metrics(spec.source, "Daule Peripa", report_day)
        return metrics, forecast_text(spec.number, metrics)
    raise RuntimeError(f"No hay analisis deterministico definido para figura {spec.number}")


def generate_deterministic_analysis(figures: list[FigureSpec], run_dir: Path, report_day: date) -> None:
    analysis_dir = run_dir / "analisis_deterministico"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    generated: list[tuple[FigureSpec, dict[str, Any], str]] = []
    metrics_by_fig: dict[int, dict[str, Any]] = {}
    for spec in figures:
        print(f"Generando analisis deterministico Figura {spec.number}...")
        metrics, text = deterministic_analysis_for_figure(spec, report_day)
        metrics["figure"] = spec.number
        metrics_by_fig[spec.number] = metrics
        generated.append((spec, metrics, text))

    captions = build_caption_overrides(metrics_by_fig)
    combined: list[str] = []
    for spec, metrics, text in generated:
        metrics["caption"] = captions.get(spec.number, spec.caption)
        (analysis_dir / f"metricas_figura_{spec.number:02d}.json").write_text(
            json.dumps(metrics, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (analysis_dir / f"analisis_figura_{spec.number:02d}.txt").write_text(text + "\n", encoding="utf-8")
        combined.append(f"## Figura {spec.number}\n\n{text}\n")
    (analysis_dir / "analisis_figuras.md").write_text("\n".join(combined), encoding="utf-8")
    print(f"Analisis deterministico generado en: {analysis_dir}")


def read_relationship_targets(zf: zipfile.ZipFile) -> dict[str, str]:
    rels = ET.fromstring(zf.read("word/_rels/document.xml.rels"))
    relmap: dict[str, str] = {}
    for rel in rels:
        rel_id = rel.attrib.get("Id")
        target = rel.attrib.get("Target")
        rel_type = rel.attrib.get("Type", "")
        if rel_id and target and rel_type.endswith("/image"):
            relmap[rel_id] = target
    return relmap


def body_image_targets(template_docx: Path) -> list[str]:
    with zipfile.ZipFile(template_docx, "r") as zf:
        relmap = read_relationship_targets(zf)
        root = ET.fromstring(zf.read("word/document.xml"))
    targets: list[str] = []
    for blip in root.findall(".//a:blip", NS):
        rid = blip.attrib.get(f"{{{NS['r']}}}embed")
        target = relmap.get(rid or "")
        if target and target not in targets:
            targets.append(target)
    return targets


def paragraph_text(el: ET.Element) -> str:
    return "".join(t.text or "" for t in el.findall(".//w:t", NS))


def paragraph_has_image(el: ET.Element) -> bool:
    return el.find(".//a:blip", NS) is not None


def replace_paragraph_text(el: ET.Element, text: str) -> None:
    text_nodes = el.findall(".//w:t", NS)
    if not text_nodes:
        return
    text_nodes[0].text = text
    text_nodes[0].set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
    for node in text_nodes[1:]:
        node.text = ""


def set_times_new_roman_12(r_pr: ET.Element) -> bool:
    changed = False
    r_fonts = r_pr.find("w:rFonts", NS)
    if r_fonts is None:
        r_fonts = ET.SubElement(r_pr, f"{{{NS['w']}}}rFonts")
        changed = True
    for attr in ("ascii", "hAnsi", "cs", "eastAsia"):
        key = f"{{{NS['w']}}}{attr}"
        if r_fonts.get(key) != "Times New Roman":
            r_fonts.set(key, "Times New Roman")
            changed = True

    for tag in ("sz", "szCs"):
        size_el = r_pr.find(f"w:{tag}", NS)
        if size_el is None:
            size_el = ET.SubElement(r_pr, f"{{{NS['w']}}}{tag}")
            changed = True
        key = f"{{{NS['w']}}}val"
        if size_el.get(key) != "24":
            size_el.set(key, "24")
            changed = True
    return changed


def apply_global_font(root: ET.Element) -> bool:
    changed = False
    for run in root.findall(".//w:r", NS):
        r_pr = run.find("w:rPr", NS)
        if r_pr is None:
            r_pr = ET.Element(f"{{{NS['w']}}}rPr")
            run.insert(0, r_pr)
            changed = True
        if set_times_new_roman_12(r_pr):
            changed = True
    return changed


def apply_caption_font_size(root: ET.Element) -> bool:
    changed = False
    for para in root.findall(".//w:p", NS):
        if not re.match(r"^Figura\s+\d+\.", paragraph_text(para).strip()):
            continue
        for run in para.findall(".//w:r", NS):
            r_pr = run.find("w:rPr", NS)
            if r_pr is None:
                r_pr = ET.Element(f"{{{NS['w']}}}rPr")
                run.insert(0, r_pr)
                changed = True
            for tag in ("sz", "szCs"):
                size_el = r_pr.find(f"w:{tag}", NS)
                if size_el is None:
                    size_el = ET.SubElement(r_pr, f"{{{NS['w']}}}{tag}")
                    changed = True
                key = f"{{{NS['w']}}}val"
                if size_el.get(key) != "20":
                    size_el.set(key, "20")
                    changed = True
    return changed


def make_text_paragraph(text: str) -> ET.Element:
    p = ET.Element(f"{{{NS['w']}}}p")
    p_pr = ET.SubElement(p, f"{{{NS['w']}}}pPr")
    jc = ET.SubElement(p_pr, f"{{{NS['w']}}}jc")
    jc.set(f"{{{NS['w']}}}val", "both")
    spacing = ET.SubElement(p_pr, f"{{{NS['w']}}}spacing")
    spacing.set(f"{{{NS['w']}}}after", "160")
    r = ET.SubElement(p, f"{{{NS['w']}}}r")
    r_pr = ET.SubElement(r, f"{{{NS['w']}}}rPr")
    set_times_new_roman_12(r_pr)
    lang = ET.SubElement(r_pr, f"{{{NS['w']}}}lang")
    lang.set(f"{{{NS['w']}}}val", "es-EC")
    t = ET.SubElement(r, f"{{{NS['w']}}}t")
    t.text = text
    return p


def load_deterministic_analysis(analysis_dir: Path) -> dict[int, list[str]]:
    out: dict[int, list[str]] = {}
    for path in sorted(analysis_dir.glob("analisis_figura_*.txt")):
        match = re.search(r"analisis_figura_(\d+)\.txt$", path.name)
        if not match:
            continue
        fig = int(match.group(1))
        text = path.read_text(encoding="utf-8").strip()
        paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
        out[fig] = paragraphs
    if not out:
        raise FileNotFoundError(f"No existen analisis deterministicos en: {analysis_dir}")
    return out


def apply_deterministic_analysis(root: ET.Element, analysis: dict[int, list[str]]) -> None:
    body = root.find("w:body", NS)
    if body is None:
        raise RuntimeError("No se encontro word/body en document.xml")
    children = list(body)
    new_children: list[ET.Element] = []
    i = 0
    while i < len(children):
        child = children[i]
        txt = paragraph_text(child).strip() if child.tag.endswith("}p") else ""
        match = re.match(r"^Figura\s+(\d+)\.", txt)
        if not match:
            new_children.append(child)
            i += 1
            continue

        fig_num = int(match.group(1))
        new_children.append(child)
        i += 1

        # Keep blank/image paragraphs immediately after the caption.
        while i < len(children):
            next_child = children[i]
            next_txt = paragraph_text(next_child).strip() if next_child.tag.endswith("}p") else ""
            if paragraph_has_image(next_child) or next_txt == "":
                new_children.append(next_child)
                i += 1
                continue
            break

        # Figure 2 has a legend line that belongs with the figure.
        if fig_num == 2 and i < len(children):
            next_txt = paragraph_text(children[i]).strip()
            if next_txt.startswith("* líneas:") or next_txt.startswith("* lineas:"):
                new_children.append(children[i])
                i += 1

        for para in analysis.get(fig_num, []):
            new_children.append(make_text_paragraph(para))

        while i < len(children):
            next_child = children[i]
            next_txt = paragraph_text(next_child).strip() if next_child.tag.endswith("}p") else ""
            if re.match(r"^Figura\s+\d+\.", next_txt):
                break
            if next_txt in {"HIDROELÉCTRICA MAZAR", "Elaboración: A. R."}:
                break
            if next_txt.startswith("Nota de descargo:") or next_txt.startswith("Se recomienda que,"):
                break
            if next_child.tag.endswith("}sectPr"):
                break
            i += 1

    body[:] = new_children


def apply_caption_overrides(root: ET.Element, captions: dict[int, str]) -> bool:
    changed = False
    for para in root.findall(".//w:p", NS):
        txt = paragraph_text(para).strip()
        match = re.match(r"^Figura\s+(\d+)\.", txt)
        if not match:
            continue
        fig_num = int(match.group(1))
        caption = captions.get(fig_num)
        if caption and txt != caption:
            replace_paragraph_text(para, caption)
            changed = True
    return changed


def apply_disclaimer_override(root: ET.Element) -> bool:
    body = root.find("w:body", NS)
    if body is None:
        return False
    changed = False
    children = list(body)
    new_children: list[ET.Element] = []
    skip_next_recommendation = False
    for child in children:
        txt = paragraph_text(child).strip() if child.tag.endswith("}p") else ""
        if txt.startswith("Nota de descargo:"):
            replace_paragraph_text(child, DISCLAIMER_TEXT)
            new_children.append(child)
            changed = True
            skip_next_recommendation = True
            continue
        if skip_next_recommendation and txt.startswith("Se recomienda que,"):
            changed = True
            continue
        if txt:
            skip_next_recommendation = False
        new_children.append(child)
    if changed:
        body[:] = new_children
    return changed


def update_text_nodes(
    xml_bytes: bytes,
    replacements: dict[str, str],
    deterministic_analysis: dict[int, list[str]] | None = None,
    caption_overrides: dict[int, str] | None = None,
    extra_figures: list[FigureSpec] | None = None,
    extra_rel_ids: dict[int, str] | None = None,
) -> bytes:
    root = ET.fromstring(xml_bytes)
    changed = False
    for para in root.findall(".//w:p", NS):
        current_text = paragraph_text(para)
        if not current_text:
            continue
        new_text = safe_replace_text(current_text, replacements)
        if new_text != current_text:
            replace_paragraph_text(para, new_text)
            changed = True
    for node in root.findall(".//w:t", NS):
        if node.text:
            new_text = safe_replace_text(node.text, replacements)
            if new_text != node.text:
                node.text = new_text
                changed = True
    if caption_overrides and apply_caption_overrides(root, caption_overrides):
        changed = True
    if deterministic_analysis:
        apply_deterministic_analysis(root, deterministic_analysis)
        changed = True
    if extra_figures and extra_rel_ids:
        insert_extra_figures(root, extra_figures, extra_rel_ids, deterministic_analysis, caption_overrides)
        changed = True
    if apply_disclaimer_override(root):
        changed = True
    if apply_global_font(root):
        changed = True
    if apply_caption_font_size(root):
        changed = True
    if not changed:
        return xml_bytes
    ET.register_namespace("w", NS["w"])
    ET.register_namespace("r", NS["r"])
    ET.register_namespace("a", NS["a"])
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


IMAGE_REL_TYPE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/image"
RELS_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
EXTRA_FIGURE_CLONE_SOURCE = {
    10: 6,
    11: 7,
    12: 8,
    13: 9,
}


def next_relationship_ids(rels_xml: bytes, count: int) -> list[str]:
    root = ET.fromstring(rels_xml)
    used = {rel.attrib.get("Id", "") for rel in root}
    max_num = 0
    for rel_id in used:
        match = re.match(r"rId(\d+)$", rel_id)
        if match:
            max_num = max(max_num, int(match.group(1)))

    ids: list[str] = []
    candidate = max_num + 1
    while len(ids) < count:
        rel_id = f"rId{candidate}"
        if rel_id not in used:
            ids.append(rel_id)
            used.add(rel_id)
        candidate += 1
    return ids


def add_image_relationships(
    rels_xml: bytes,
    extra_rel_ids: dict[int, str],
    extra_media_targets: dict[int, str],
) -> bytes:
    if not extra_rel_ids:
        return rels_xml
    root = ET.fromstring(rels_xml)
    for fig_num, rel_id in extra_rel_ids.items():
        rel = ET.SubElement(root, f"{{{RELS_NS}}}Relationship")
        rel.set("Id", rel_id)
        rel.set("Type", IMAGE_REL_TYPE)
        rel.set("Target", extra_media_targets[fig_num])
    ET.register_namespace("", RELS_NS)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def find_extra_figure_insert_index(children: list[ET.Element]) -> int:
    for idx, child in enumerate(children):
        if not child.tag.endswith("}p"):
            continue
        txt = paragraph_text(child).strip()
        if txt.startswith("Elaboraci") or txt.startswith("Nota de descargo:") or txt.startswith("Se recomienda que,"):
            return idx
    for idx, child in enumerate(children):
        if child.tag.endswith("}sectPr"):
            return idx
    return len(children)


def find_figure_templates(body: ET.Element) -> tuple[ET.Element | None, dict[int, tuple[ET.Element, ET.Element]]]:
    children = list(body)
    heading_template = None
    templates: dict[int, tuple[ET.Element, ET.Element]] = {}

    for child in children:
        if child.tag.endswith("}p") and "HIDROEL" in paragraph_text(child) and "MAZAR" in paragraph_text(child):
            heading_template = child
            break

    for idx, child in enumerate(children):
        if not child.tag.endswith("}p"):
            continue
        txt = paragraph_text(child).strip()
        match = re.match(r"^Figura\s+([1-9])\.", txt)
        if not match:
            continue
        fig_num = int(match.group(1))
        for next_child in children[idx + 1 :]:
            if next_child.tag.endswith("}p") and paragraph_has_image(next_child):
                templates[fig_num] = (child, next_child)
                break
            next_txt = paragraph_text(next_child).strip() if next_child.tag.endswith("}p") else ""
            if re.match(r"^Figura\s+[1-9]\.", next_txt):
                break
    return heading_template, templates


def set_image_relationship(paragraph: ET.Element, rel_id: str) -> None:
    blip = paragraph.find(".//a:blip", NS)
    if blip is None:
        raise RuntimeError("No se encontro a:blip en el parrafo de imagen clonado.")
    blip.set(f"{{{NS['r']}}}embed", rel_id)


def insert_extra_figures(
    root: ET.Element,
    extra_figures: list[FigureSpec],
    extra_rel_ids: dict[int, str],
    deterministic_analysis: dict[int, list[str]] | None,
    caption_overrides: dict[int, str] | None,
) -> None:
    body = root.find("w:body", NS)
    if body is None:
        raise RuntimeError("No se encontro word/body en document.xml")

    heading_template, templates = find_figure_templates(body)
    if not templates:
        raise RuntimeError("No se encontraron parrafos de figura en el template DOCX.")

    new_children: list[ET.Element] = []
    if heading_template is not None:
        heading = copy.deepcopy(heading_template)
        replace_paragraph_text(heading, "HIDROELÉCTRICA DAULE PERIPA")
        new_children.append(heading)

    fallback_template = next(iter(templates.values()))
    for spec in extra_figures:
        template = templates.get(EXTRA_FIGURE_CLONE_SOURCE.get(spec.number), fallback_template)
        caption_para = copy.deepcopy(template[0])
        image_para = copy.deepcopy(template[1])
        caption = (caption_overrides or {}).get(spec.number, spec.caption)
        replace_paragraph_text(caption_para, caption)
        set_image_relationship(image_para, extra_rel_ids[spec.number])
        new_children.extend([caption_para, image_para])
        for para in (deterministic_analysis or {}).get(spec.number, []):
            new_children.append(make_text_paragraph(para))

    children = list(body)
    insert_idx = find_extra_figure_insert_index(children)
    body[:] = children[:insert_idx] + new_children + children[insert_idx:]


def build_docx(
    template_docx: Path,
    output_docx: Path,
    figures: list[FigureSpec],
    fig_dir: Path,
    replacements: dict[str, str],
    deterministic_analysis: dict[int, list[str]] | None = None,
    caption_overrides: dict[int, str] | None = None,
) -> None:
    if not template_docx.exists():
        raise FileNotFoundError(f"No existe el template DOCX: {template_docx}")

    targets = body_image_targets(template_docx)
    if len(targets) == 0:
        raise RuntimeError(
            f"El template contiene {len(targets)} imagenes de cuerpo; "
            "se necesita al menos una imagen para clonar el formato."
        )

    target_by_name = {Path(target).name: target for target in targets}
    figure_by_media = {}
    extra_figures: list[FigureSpec] = []
    for spec in figures:
        target = target_by_name.get(spec.media_name)
        if not target:
            if spec.number <= len(targets):
                target = targets[spec.number - 1]
            else:
                extra_figures.append(spec)
                continue
        figure_by_media[f"word/{target}"] = fig_dir / f"figura_{spec.number:02d}.png"

    extra_media_targets = {
        spec.number: f"media/generated_figura_{spec.number:02d}.png"
        for spec in extra_figures
    }
    extra_rel_ids: dict[int, str] = {}
    if extra_figures:
        with zipfile.ZipFile(template_docx, "r") as zin:
            rel_ids = next_relationship_ids(zin.read("word/_rels/document.xml.rels"), len(extra_figures))
        extra_rel_ids = {
            spec.number: rel_id
            for spec, rel_id in zip(extra_figures, rel_ids)
        }

    ensure_parent(output_docx)
    tmp = output_docx.with_suffix(".tmp.docx")
    with zipfile.ZipFile(template_docx, "r") as zin:
        with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename in figure_by_media:
                    data = figure_by_media[item.filename].read_bytes()
                elif item.filename == "word/_rels/document.xml.rels":
                    data = add_image_relationships(data, extra_rel_ids, extra_media_targets)
                elif item.filename in {
                    "word/document.xml",
                    "docProps/core.xml",
                    "docProps/app.xml",
                } or re.match(r"^word/(header|footer)\d*\.xml$", item.filename):
                    if item.filename == "word/document.xml":
                        data = update_text_nodes(
                            data,
                            replacements,
                            deterministic_analysis,
                            caption_overrides,
                            extra_figures,
                            extra_rel_ids,
                        )
                    elif re.match(r"^word/(header|footer)\d*\.xml$", item.filename):
                        data = update_text_nodes(data, {})
                zout.writestr(item, data)
            for spec in extra_figures:
                image_path = fig_dir / f"figura_{spec.number:02d}.png"
                zout.writestr(f"word/{extra_media_targets[spec.number]}", image_path.read_bytes())
    tmp.replace(output_docx)


def validate_docx_captions(output_docx: Path, caption_overrides: dict[int, str] | None) -> None:
    if not caption_overrides:
        return
    with zipfile.ZipFile(output_docx, "r") as zf:
        root = ET.fromstring(zf.read("word/document.xml"))
    paragraphs = [paragraph_text(p).strip() for p in root.findall(".//w:p", NS)]
    missing = [caption for caption in caption_overrides.values() if caption not in paragraphs]
    if missing:
        raise RuntimeError("El DOCX no contiene los captions esperados: " + " | ".join(missing))


def figure_specs(config: dict[str, Any]) -> list[FigureSpec]:
    figures = []
    for raw in config["figures"]:
        viewport = raw.get("viewport") or config.get("default_viewport") or {"width": 1400, "height": 900}
        figures.append(
            FigureSpec(
                number=int(raw["number"]),
                source_type=raw["source_type"],
                source=raw["source"],
                selector=raw.get("selector", ".plotly-graph-div"),
                media_name=raw.get("media_name", f"image{raw['number']}.png"),
                caption=raw.get("caption", ""),
                viewport={"width": int(viewport["width"]), "height": int(viewport["height"])},
                wait_ms=int(raw.get("wait_ms", config.get("wait_ms", 2500))),
            )
        )
    return sorted(figures, key=lambda x: x.number)


def report_paths(config: dict[str, Any], report_day: date) -> tuple[Path, Path, Path]:
    output_root = resolve_project_path(config["paths"]["output_root"])
    run_dir = output_root / report_day.isoformat()
    fig_dir = run_dir / "figuras"
    filename = f"Caudal_{MONTHS_TITLE[report_day.month]}_{report_day.day:02d}_{report_day.year}.docx"
    return run_dir, fig_dir, run_dir / filename


def preflight(config: dict[str, Any], figures: list[FigureSpec], template_docx: Path) -> None:
    if not template_docx.exists():
        raise FileNotFoundError(f"No existe el template DOCX: {template_docx}")
    for spec in figures:
        if spec.source_type == "html":
            source = resolve_project_path(spec.source)
            if not source.exists():
                raise FileNotFoundError(f"No existe HTML de figura {spec.number}: {source}")
        elif spec.source_type == "url":
            if not re.match(r"^https?://", spec.source):
                raise ValueError(f"URL invalida para figura {spec.number}: {spec.source}")
            if "REEMPLAZAR-CON-URL" in spec.source:
                raise ValueError(
                    f"La figura {spec.number} todavia tiene una URL placeholder "
                    "en boletin_hidroelectricas_config.json"
                )
        elif spec.source_type == "plotly_api":
            source = spec.source.format(date="2000-01-01")
            if not re.match(r"^https?://", source):
                raise ValueError(f"URL de API invalida para figura {spec.number}: {spec.source}")
        elif spec.source_type == "manual_png":
            source = resolve_project_path(spec.source)
            if not source.exists():
                raise FileNotFoundError(f"No existe PNG manual de figura {spec.number}: {source}")
        else:
            raise ValueError(f"source_type invalido para figura {spec.number}: {spec.source_type}")


def main() -> int:
    args = parse_args()
    config = load_config(args.config)
    report_day = report_date_from_args(args.date)
    figures = figure_specs(config)
    template_docx = resolve_project_path(config["paths"]["template_docx"])
    run_dir, fig_dir, output_docx = report_paths(config, report_day)

    preflight(config, figures, template_docx)
    print(f"Directorio de salida: {run_dir}")
    if args.dry_run:
        print("Dry-run OK: configuracion, fuentes locales y template validados.")
        return 0

    if not args.skip_screenshots and not args.analysis_only:
        capture_screenshots(figures, fig_dir, report_day)

    validate_pngs(figures, fig_dir)

    if args.insert_deterministic_analysis:
        args.generate_deterministic_analysis = True

    if args.generate_analysis or args.analysis_only:
        if args.generate_deterministic_analysis or (args.analysis_only and not args.generate_analysis):
            generate_deterministic_analysis(figures, run_dir, report_day)
        else:
            generate_analysis_texts(
                figures=figures,
                fig_dir=fig_dir,
                analysis_dir=run_dir / "analisis",
                report_day=report_day,
                config=config,
                model_override=args.llm_model,
            )
            if args.analysis_only:
                print(f"Analisis generados en: {run_dir / 'analisis'}")
                return 0

    if args.generate_deterministic_analysis and not args.generate_analysis and not args.analysis_only:
        generate_deterministic_analysis(figures, run_dir, report_day)

    if args.analysis_only:
        return 0

    replacements = build_replacements(report_day, config)
    deterministic_analysis = None
    caption_overrides = None
    if args.insert_deterministic_analysis:
        analysis_dir = run_dir / "analisis_deterministico"
        deterministic_analysis = load_deterministic_analysis(analysis_dir)
        metrics_by_fig = load_metrics_by_figure(analysis_dir)
        caption_overrides = build_caption_overrides(metrics_by_fig)
    build_docx(template_docx, output_docx, figures, fig_dir, replacements, deterministic_analysis, caption_overrides)
    validate_docx_captions(output_docx, caption_overrides)
    print(f"DOCX generado: {output_docx}")
    copied_docx = copy_report_to_shared_folder(output_docx)
    print(f"Copia DOCX generada: {copied_docx}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
