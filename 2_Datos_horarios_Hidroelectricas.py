import os
import webbrowser
from pathlib import Path

import folium
import pandas as pd
import plotly.express as px
import psycopg2
from folium.plugins import MarkerCluster

from common_runtime import (
    apply_station_coordinates,
    load_config,
    project_path,
    should_open_browser,
)


BASE_DIR = project_path()


def get_conn():
    params = load_config(section="Postgres")
    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        user=params["user"],
        password=params["password"],
        dbname=params["dbname"],
    )


def get_data_horario():
    conn = get_conn()
    query = """
    SELECT nombre_estacion, latitud, longitud, fecha_toma_dato, valor_1h
    FROM temporales.caudales
    WHERE fecha_toma_dato >= '2009-01-01'
      AND valor_1h IS NOT NULL
      AND TRIM(valor_1h::text) NOT ILIKE 'nan'
      AND valor_1h > 0
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df["fecha_toma_dato"] = pd.to_datetime(df["fecha_toma_dato"], errors="coerce", utc=False)
    df["valor_1h"] = pd.to_numeric(df["valor_1h"], errors="coerce")
    df = df.dropna(subset=["valor_1h", "fecha_toma_dato"]).copy()
    df["nombre_estacion"] = df["nombre_estacion"].astype(str).str.strip()
    df = apply_station_coordinates(df)
    df["FechaHora"] = df["fecha_toma_dato"] - pd.to_timedelta("30min")
    df = df[["nombre_estacion", "latitud", "longitud", "FechaHora", "valor_1h"]].sort_values(
        ["nombre_estacion", "FechaHora"]
    )
    return df


def generar_grafico_estacion_horario(df_est, nombre_estacion, output_dir):
    nombre_clean = nombre_estacion.replace(" ", "_")
    file_path = output_dir / f"grafico_{nombre_clean}.html"

    fig = px.line(
        df_est,
        x="FechaHora",
        y="valor_1h",
        title=f"Caudal Horario - {nombre_estacion}",
        labels={"FechaHora": "Fecha-Hora", "valor_1h": "Q 1h (m³/s)"},
    )
    fig.update_traces(mode="lines+markers")
    fig.update_layout(height=600)
    data_json = df_est.to_json(orient="records", date_format="iso")

    html_content = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8" />
    <title>Caudal Horario - {nombre_estacion}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css">
    <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
</head>
<body style="font-family: Segoe UI, Roboto, Arial, sans-serif; margin: 16px;">
    <h2 style="margin-top:0;">Caudal Horario - {nombre_estacion}</h2>
    <div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap;">
        <label for="fecha-inicial"><b>Fecha-Hora Inicial:</b></label>
        <input type="text" id="fecha-inicial" placeholder="YYYY-MM-DD HH:mm" />
        <label for="fecha-final"><b>Fecha-Hora Final:</b></label>
        <input type="text" id="fecha-final" placeholder="YYYY-MM-DD HH:mm" />
        <button onclick="filtrar()">Filtrar</button>
        <button onclick="resetear()">Reset</button>
    </div>
    <div id="grafico" style="width:100%; height:600px; margin-top:12px;"></div>

    <script>
    flatpickr("#fecha-inicial", {{
        enableTime: true,
        time_24hr: true,
        dateFormat: "Y-m-d H:i"
    }});
    flatpickr("#fecha-final", {{
        enableTime: true,
        time_24hr: true,
        dateFormat: "Y-m-d H:i"
    }});

    const registros = {data_json};

    function dibujar(datos) {{
        const trace = {{
            x: datos.map(r => r.FechaHora),
            y: datos.map(r => r["valor_1h"]),
            mode: 'lines+markers',
            type: 'scatter',
            name: 'Q 1h (m³/s)',
            hovertemplate: '%{{y:.2f}} m³/s<br>%{{x|%Y-%m-%d %H:%M}}<extra></extra>'
        }};
        const layout = {{
            xaxis: {{ title: 'Fecha-Hora' }},
            yaxis: {{ title: 'Q 1h (m³/s)', tickformat: '.2f' }},
            height: 600
        }};
        Plotly.newPlot('grafico', [trace], layout);
    }}

    function filtrar() {{
        const inicioStr = document.getElementById('fecha-inicial').value;
        const finStr = document.getElementById('fecha-final').value;
        if (!inicioStr || !finStr) {{
            dibujar(registros);
            return;
        }}
        const inicio = new Date(inicioStr.replace(' ', 'T'));
        const fin = new Date(finStr.replace(' ', 'T'));
        const dataFiltrada = registros.filter(r => {{
            const f = new Date(r.FechaHora);
            return f >= inicio && f <= fin;
        }});
        dibujar(dataFiltrada);
    }}

    function resetear() {{
        document.getElementById('fecha-inicial').value = '';
        document.getElementById('fecha-final').value = '';
        dibujar(registros);
    }}

    window.onload = () => dibujar(registros);
    </script>
</body>
</html>
"""
    file_path.write_text(html_content, encoding="utf-8")
    print(f"Gráfico horario generado: {file_path.name}")


def crear_mapa_horario(df_horario):
    m = folium.Map(location=[-1.5, -78.0], zoom_start=6)
    marker_cluster = MarkerCluster().add_to(m)

    estaciones = df_horario[["nombre_estacion", "latitud", "longitud"]].drop_duplicates()
    for _, row in estaciones.iterrows():
        nombre = row["nombre_estacion"]
        lat, lon = row["latitud"], row["longitud"]
        archivo_html = f"TAB_3/grafico_{nombre.replace(' ', '_')}.html"

        popup_html = f"""
        <div style='font-family: Segoe UI; font-size: 15px;'>
            <strong style='font-size:16px'>{nombre}</strong><br>
            Lat: {lat:.4f}<br>
            Lon: {lon:.4f}<br>
            <a href="{archivo_html}" target="_blank">Ver gráfico horario</a>
        </div>
        """
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=nombre,
            icon=folium.Icon(color="green", icon="time", prefix="fa"),
        ).add_to(marker_cluster)

    mapa_path = BASE_DIR / "mapa_horario_hidro.html"
    m.save(str(mapa_path))
    print(f"Mapa horario generado: {mapa_path}")
    if should_open_browser():
        webbrowser.open("file://" + os.path.realpath(mapa_path))


if __name__ == "__main__":
    print("Conectando a la base de datos (horario)...")
    df_h = get_data_horario()

    csv_path = BASE_DIR / "horario_todas_estaciones.csv"
    df_h.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"CSV horario generado: {csv_path.name}")

    output_dir = BASE_DIR / "TAB_3"
    output_dir.mkdir(exist_ok=True)

    for nombre_estacion in df_h["nombre_estacion"].unique():
        df_est = df_h[df_h["nombre_estacion"] == nombre_estacion]
        generar_grafico_estacion_horario(df_est, nombre_estacion, output_dir)

    crear_mapa_horario(df_h)
    print("Proceso horario finalizado.")
