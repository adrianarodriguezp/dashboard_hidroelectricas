#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import sys
import re
import numpy as np
import pandas as pd
import plotly.graph_objects as go

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from common_runtime import analysis_years, month_note


# =========================================================
# 1) CONFIGURACIÓN GENERAL
# =========================================================
BASE_DIR = Path(__file__).parent
CENACE_DIR = BASE_DIR.parent
ANALISIS_DIR = CENACE_DIR / "Analisis_Q_outliers"

INPUT_MONTHLY_DIR = ANALISIS_DIR / "08_mensualizado_corregido"
INPUT_DAILY_DIR = ANALISIS_DIR / "07_diarios_corregidos_por_hidroelectrica"

OUTPUT_INDEX = BASE_DIR / "index.html"

MONTH_MAP_NUM2TXT = {
    1: "ENE",  2: "FEB",  3: "MAR",  4: "ABR",
    5: "MAY",  6: "JUN",  7: "JUL",  8: "AGO",
    9: "SEP", 10: "OCT", 11: "NOV", 12: "DIC"
}
MONTH_ORDER = [MONTH_MAP_NUM2TXT[m] for m in range(1, 13)]
MONTH_TXT2NUM = {v: k for k, v in MONTH_MAP_NUM2TXT.items()}
MESES_TICKS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
RECENT_YEARS = analysis_years(history_years=2)


# =========================================================
# 2) UTILIDADES
# =========================================================
def safe_name(texto: str) -> str:
    texto = str(texto).strip().replace(" ", "_").replace("/", "_")
    return re.sub(r"[^A-Za-z0-9_\-]", "", texto)


def pretty_name_from_file(stem: str) -> str:
    return stem.replace("_mensualizado_corregido", "").replace("_", " ")


def require_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"ERROR: No existe {label}: {path}")


def year_curve_monthly(table: pd.DataFrame, year: int):
    df_year = table[table["Year"] == year].copy()
    if df_year.empty:
        return None

    long = df_year.melt(id_vars=["Year"], var_name="Mes", value_name="value")
    long["MesNum"] = long["Mes"].map(MONTH_TXT2NUM)
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["MesNum"])

    long_valid = long.dropna(subset=["value"]).copy()
    if long_valid.empty:
        return None

    last_month = int(long_valid["MesNum"].max())
    long_valid = long_valid[long_valid["MesNum"] <= last_month].sort_values("MesNum")
    return long_valid


def get_current_month_note():
    return month_note(pd.Timestamp.today())


# =========================================================
# 3) RESUMEN POR ESTACIÓN
# =========================================================
def get_station_summary(st_name: str):
    result = {
        "last_daily_date": "No disponible",
        "last_daily_value": "No disponible"
    }

    daily_path = INPUT_DAILY_DIR / f"{safe_name(st_name)}_diario_corregido.csv"
    if daily_path.exists():
        df_daily = pd.read_csv(daily_path, encoding="utf-8")

        if not df_daily.empty:
            df_daily["Fecha"] = pd.to_datetime(df_daily["Fecha"], errors="coerce")
            df_daily["Caudal Diario Promedio"] = pd.to_numeric(
                df_daily["Caudal Diario Promedio"], errors="coerce"
            )
            df_daily = df_daily.dropna(
                subset=["Fecha", "Caudal Diario Promedio"]
            ).sort_values("Fecha")

            if not df_daily.empty:
                last_daily = df_daily.iloc[-1]
                result["last_daily_date"] = last_daily["Fecha"].strftime("%Y-%m-%d")
                result["last_daily_value"] = f'{last_daily["Caudal Diario Promedio"]:.2f} m³/s'

    return result


# =========================================================
# 4) CARGA DE TABLAS MENSUALES
# =========================================================
def load_monthly_tables():
    require_exists(INPUT_MONTHLY_DIR, "la carpeta de mensualizados corregidos")

    files = sorted(INPUT_MONTHLY_DIR.glob("*_mensualizado_corregido.csv"))
    if not files:
        raise FileNotFoundError(f"ERROR: No se encontraron CSV en {INPUT_MONTHLY_DIR}")

    station_tables = {}

    for f in files:
        st_name = pretty_name_from_file(f.stem)
        table = pd.read_csv(f, encoding="utf-8")

        table["Year"] = pd.to_numeric(table["Year"], errors="coerce")
        table = table.dropna(subset=["Year"])
        table["Year"] = table["Year"].astype(int)

        for col in MONTH_ORDER:
            if col not in table.columns:
                table[col] = np.nan

        table = table[["Year"] + MONTH_ORDER].sort_values("Year")
        station_tables[st_name] = table

    return station_tables


# =========================================================
# 5) GRÁFICO INTERACTIVO PLOTLY
# =========================================================
def build_plotly_figure(st_name: str, table: pd.DataFrame) -> go.Figure:
    hist = table[table["Year"] <= 2023].copy()

    hist_long = hist.melt(id_vars=["Year"], var_name="Mes", value_name="value")
    hist_long["MesNum"] = hist_long["Mes"].map(MONTH_TXT2NUM)
    hist_long["value"] = pd.to_numeric(hist_long["value"], errors="coerce")
    hist_long = hist_long.dropna(subset=["MesNum", "value"])

    fig = go.Figure()
    start_year = None
    end_year = None

    if not hist_long.empty:
        start_year = int(hist["Year"].min())
        end_year = int(hist["Year"].max())

        for yr in sorted(hist_long["Year"].unique()):
            d = hist_long[hist_long["Year"] == yr].sort_values("MesNum")
            fig.add_trace(
                go.Scatter(
                    x=d["MesNum"],
                    y=d["value"],
                    mode="lines",
                    line=dict(color="rgba(130,130,130,0.22)", width=1),
                    name=f"Histórico {yr}",
                    hovertemplate="Año %{text}<br>Mes %{x}<br>%{y:.2f} m³/s<extra></extra>",
                    text=[yr] * len(d),
                    showlegend=False
                )
            )

        stats = hist_long.groupby("MesNum")["value"]
        max_hist = stats.max()
        min_hist = stats.min()
        mean_hist = stats.mean()
        p10 = stats.quantile(0.10)
        p90 = stats.quantile(0.90)

        fig.add_trace(
            go.Scatter(
                x=list(p10.index) + list(p90.index[::-1]),
                y=list(p10.values) + list(p90.values[::-1]),
                fill="toself",
                fillcolor="rgba(111,168,220,0.35)",
                line=dict(color="rgba(255,255,255,0)"),
                hoverinfo="skip",
                name="Percentil 10-90%"
            )
        )

        fig.add_trace(
            go.Scatter(
                x=max_hist.index,
                y=max_hist.values,
                mode="lines",
                line=dict(color="green", dash="dash", width=1.5),
                name="Máximo Histórico",
                hovertemplate="Mes %{x}<br>%{y:.2f} m³/s<extra></extra>"
            )
        )

        fig.add_trace(
            go.Scatter(
                x=min_hist.index,
                y=min_hist.values,
                mode="lines",
                line=dict(color="magenta", dash="dash", width=1.5),
                name="Mínimo Histórico",
                hovertemplate="Mes %{x}<br>%{y:.2f} m³/s<extra></extra>"
            )
        )

        fig.add_trace(
            go.Scatter(
                x=mean_hist.index,
                y=mean_hist.values,
                mode="lines",
                line=dict(color="blue", dash="dash", width=2),
                name="Promedio Histórico",
                hovertemplate="Mes %{x}<br>%{y:.2f} m³/s<extra></extra>"
            )
        )

        fig.add_trace(
            go.Scatter(
                x=[None], y=[None],
                mode="lines",
                line=dict(color="gray", width=2),
                name=f"Históricos ({start_year}-2023)"
            )
        )

    recent_curves = {year: year_curve_monthly(table, year) for year in RECENT_YEARS}
    colors = ["red", "green", "black", "orange", "purple"]
    for idx, year in enumerate(RECENT_YEARS):
        curve = recent_curves[year]
        if curve is None:
            continue
        latest_year = year == RECENT_YEARS[-1]
        fig.add_trace(
            go.Scatter(
                x=curve["MesNum"],
                y=curve["value"],
                mode="lines+markers" if latest_year else "lines",
                line=dict(color=colors[idx % len(colors)], width=3),
                marker=dict(size=8, color=colors[idx % len(colors)]) if latest_year else None,
                name=f"{year}{' (mes en curso)' if latest_year else ''}",
                hovertemplate=f"{year}<br>Mes %{{x}}<br>%{{y:.2f}} m³/s<extra></extra>"
            )
        )

    title = (
        f"Caudal histórico {start_year}-{end_year} para {st_name}<br>"
        f"Promedio mensual + banda p10-p90 y años recientes ({RECENT_YEARS[0]}-{RECENT_YEARS[-1]})"
        if start_year and end_year
        else f"Caudal mensual para {st_name}<br>Años recientes ({RECENT_YEARS[0]}-{RECENT_YEARS[-1]})"
    )

    fig.update_layout(
        title=title,
        template="plotly_white",
        height=720,
        hovermode="x unified",
        margin=dict(l=60, r=240, t=90, b=70),
        legend=dict(
            orientation="v",
            x=1.02,
            y=1,
            xanchor="left",
            yanchor="top"
        ),
        annotations=[
            dict(
                text=get_current_month_note(),
                x=0.5,
                y=-0.18,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=12, color="#444"),
                align="center"
            )
        ]
    )

    fig.update_xaxes(
        title="Meses",
        tickmode="array",
        tickvals=list(range(1, 13)),
        ticktext=MESES_TICKS,
        range=[0.8, 12.2],
        showspikes=True
    )

    fig.update_yaxes(
        title="Caudal promedio (m³/s)",
        tickformat=".2f",
        showspikes=True
    )

    return fig


# =========================================================
# 6) HTML POR ESTACIÓN
# =========================================================
def write_station_html(st_name: str, table: pd.DataFrame):
    fig = build_plotly_figure(st_name, table)
    plot_div = fig.to_html(full_html=False, include_plotlyjs="cdn")

    summary = get_station_summary(st_name)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>TAB_2 - {st_name}</title>
    <style>
        body {{
            font-family: "Segoe UI", Roboto, Arial, sans-serif;
            margin: 0;
            background: #f4f7fb;
            color: #1f2937;
        }}
        .container {{
            max-width: 1450px;
            margin: 0 auto;
            padding: 24px;
        }}
        .topbar {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 12px;
            margin-bottom: 18px;
        }}
        .btn {{
            display: inline-block;
            background: #0b72d9;
            color: white;
            text-decoration: none;
            padding: 10px 16px;
            border-radius: 10px;
            font-weight: 600;
        }}
        .card {{
            background: white;
            border-radius: 16px;
            padding: 18px;
            box-shadow: 0 6px 18px rgba(0,0,0,0.08);
            margin-bottom: 18px;
        }}
        .meta {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 12px;
            margin-bottom: 18px;
        }}
        .meta-item {{
            background: #eef4fb;
            border-radius: 12px;
            padding: 14px;
        }}
        .meta-item b {{
            display: block;
            margin-bottom: 4px;
            color: #0b4b84;
        }}
        .notice {{
            background: #fff7e6;
            border-left: 5px solid #f0ad4e;
            padding: 14px;
            border-radius: 10px;
            margin-bottom: 18px;
            color: #6c4b00;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="topbar">
            <div>
                <h1 style="margin:0;">TAB_2 - {st_name}</h1>
                <p style="margin:6px 0 0 0;">Comparación mensual de caudales con históricos y serie corregida por outliers</p>
            </div>
            <a class="btn" href="index.html">⬅ Volver a TAB_2</a>
        </div>

        <div class="notice">
            <b>Nota de monitoreo:</b> {get_current_month_note()}
        </div>

        <div class="card">
            <div class="meta">
                <div class="meta-item">
                    <b>Hidroeléctrica</b>
                    {st_name}
                </div>
                <div class="meta-item">
                    <b>Último día incluido en el promedio diario</b>
                    {summary["last_daily_date"]}
                </div>
                <div class="meta-item">
                    <b>Último caudal diario promedio</b>
                    {summary["last_daily_value"]}
                </div>
            </div>
            {plot_div}
        </div>
    </div>
</body>
</html>
"""
    out_html = BASE_DIR / f"grafico_{safe_name(st_name)}.html"
    out_html.write_text(html, encoding="utf-8")
    print(f"OK: HTML generado: {out_html.name}")


# =========================================================
# 7) INDEX DE TAB_2
# =========================================================
def write_index_html(stations: list[str]):
    cards = []

    for st in stations:
        summary = get_station_summary(st)
        href = f"grafico_{safe_name(st)}.html"

        cards.append(f"""
        <a class="card-link" href="{href}">
            <div class="card">
                <h3>{st}</h3>
                <p><b>Último día diario:</b> {summary["last_daily_date"]}</p>
                <p><b>Último caudal diario:</b> {summary["last_daily_value"]}</p>
                <div class="btn-mini">📈 Ver gráfico</div>
            </div>
        </a>
        """)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>TAB_2 - Caudales Mensuales</title>
    <style>
        body {{
            font-family: "Segoe UI", Roboto, Arial, sans-serif;
            margin: 0;
            background: #f4f7fb;
            color: #1f2937;
        }}
        .container {{
            max-width: 1280px;
            margin: 0 auto;
            padding: 28px;
        }}
        .header {{
            text-align: center;
            margin-bottom: 26px;
        }}
        .header h1 {{
            margin-bottom: 8px;
            color: #0b4b84;
        }}
        .header p {{
            margin-top: 0;
            color: #5b6b7c;
            font-size: 18px;
        }}
        .back {{
            display: inline-block;
            margin-bottom: 22px;
            text-decoration: none;
            background: #0b72d9;
            color: white;
            padding: 10px 16px;
            border-radius: 10px;
            font-weight: 600;
        }}
        .notice {{
            background: #fff7e6;
            border-left: 5px solid #f0ad4e;
            padding: 14px;
            border-radius: 10px;
            margin-bottom: 24px;
            color: #6c4b00;
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
            gap: 18px;
        }}
        .card-link {{
            text-decoration: none;
            color: inherit;
        }}
        .card {{
            background: white;
            border-radius: 18px;
            padding: 20px;
            box-shadow: 0 8px 18px rgba(0,0,0,0.08);
            transition: transform 0.15s ease, box-shadow 0.15s ease;
            height: 100%;
        }}
        .card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 14px 24px rgba(0,0,0,0.12);
        }}
        .card h3 {{
            margin-top: 0;
            color: #0b4b84;
        }}
        .card p {{
            margin: 8px 0;
        }}
        .btn-mini {{
            display: inline-block;
            margin-top: 10px;
            background: #0b72d9;
            color: white;
            padding: 10px 14px;
            border-radius: 10px;
            font-weight: 600;
        }}
    </style>
</head>
<body>
    <div class="container">
        <a class="back" href="../index.html">⬅ Volver al dashboard</a>

        <div class="header">
            <h1>TAB_2 - Comparación mensual de caudales</h1>
            <p>Series mensualizadas corregidas por outliers con históricos, percentiles y años recientes</p>
        </div>

        <div class="notice">
            <b>Nota de monitoreo:</b> {get_current_month_note()}
        </div>

        <div class="grid">
            {''.join(cards)}
        </div>
    </div>
</body>
</html>
"""
    OUTPUT_INDEX.write_text(html, encoding="utf-8")
    print(f"OK: Index generado: {OUTPUT_INDEX}")


# =========================================================
# 8) PROCESO PRINCIPAL
# =========================================================
def main():
    print("Iniciando lectura de mensualizados corregidos...")
    station_tables = load_monthly_tables()

    stations = sorted(station_tables.keys())
    print(f"OK: Estaciones encontradas para TAB_2: {len(stations)}")

    for st in stations:
        write_station_html(st, station_tables[st])

    write_index_html(stations)
    print("OK: TAB_2 HTML generado correctamente.")


if __name__ == "__main__":
    main()
