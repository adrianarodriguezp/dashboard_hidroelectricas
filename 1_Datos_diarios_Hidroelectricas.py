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


def get_data():
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

    df["fecha_toma_dato"] = pd.to_datetime(df["fecha_toma_dato"], errors="coerce")
    df["valor_1h"] = pd.to_numeric(df["valor_1h"], errors="coerce")
    df = df.dropna(subset=["fecha_toma_dato", "valor_1h"]).copy()
    df["nombre_estacion"] = df["nombre_estacion"].astype(str).str.strip()
    df = apply_station_coordinates(df)
    df["fecha_real"] = df["fecha_toma_dato"] - pd.to_timedelta("30min")
    df["fecha_dia"] = df["fecha_real"].dt.date

    hoy = pd.Timestamp.now().date()
    df = df[df["fecha_dia"] < hoy].copy()

    return df


def create_diario(df):
    return (
        df.groupby(["nombre_estacion", "latitud", "longitud", "fecha_dia"])["valor_1h"]
        .mean()
        .reset_index()
        .rename(columns={"fecha_dia": "Fecha", "valor_1h": "Caudal Diario Promedio"})
    )


def generar_grafico_estacion(df_est, nombre_estacion, output_dir):
    nombre_clean = nombre_estacion.replace(" ", "_")
    file_path = output_dir / f"grafico_{nombre_clean}.html"

    fig = px.line(
        df_est,
        x="Fecha",
        y="Caudal Diario Promedio",
        title=f"Caudal Diario Promedio - {nombre_estacion}",
        labels={"Fecha": "Fecha", "Caudal Diario Promedio": "Q Diario (m³/s)"},
    )
    fig.update_traces(mode="lines+markers")
    fig.update_layout(height=600)
    data_json = df_est.to_json(orient="records", date_format="iso")

    html_content = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Caudal Diario Promedio - {nombre_estacion}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css">
    <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
</head>
<body>
    <h2>Caudal Diario Promedio - {nombre_estacion}</h2>
    <label for="fecha-inicial">Fecha Inicial:</label>
    <input type="text" id="fecha-inicial" placeholder="Selecciona fecha inicial">
    <label for="fecha-final">Fecha Final:</label>
    <input type="text" id="fecha-final" placeholder="Selecciona fecha final">
    <button onclick="filtrar()">Filtrar</button>
    <button onclick="resetear()">Reset</button>
    <div id="grafico" style="width:100%; height:600px;"></div>
    <script>
    flatpickr("#fecha-inicial", {{dateFormat: "Y-m-d"}});
    flatpickr("#fecha-final", {{dateFormat: "Y-m-d"}});
    const registros = {data_json};

    function dibujar(datos) {{
        const trace = {{
            x: datos.map(r => r.Fecha),
            y: datos.map(r => r["Caudal Diario Promedio"]),
            mode: 'lines+markers',
            type: 'scatter',
            name: 'Caudal Diario Promedio',
            line: {{color: 'royalblue'}}
        }};
        const layout = {{
            xaxis: {{ title: 'Fecha' }},
            yaxis: {{ title: 'Q Diario (m³/s)' }},
            height: 600
        }};
        Plotly.newPlot('grafico', [trace], layout);
    }}

    function filtrar() {{
        const inicio = document.getElementById('fecha-inicial').value;
        const fin = document.getElementById('fecha-final').value;
        if (!inicio || !fin) {{
            dibujar(registros);
            return;
        }}
        const dataFiltrada = registros.filter(r => {{
            const f = new Date(r.Fecha);
            return f >= new Date(inicio) && f <= new Date(fin);
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
    print(f"Gráfico generado: {file_path.name}")


def crear_mapa(df_diario):
    m = folium.Map(location=[-1.5, -78.0], zoom_start=6)
    marker_cluster = MarkerCluster().add_to(m)
    estaciones = df_diario[["nombre_estacion", "latitud", "longitud"]].drop_duplicates()
    for _, row in estaciones.iterrows():
        nombre = row["nombre_estacion"]
        lat, lon = row["latitud"], row["longitud"]
        archivo_html = f"TAB_1/grafico_{nombre.replace(' ', '_')}.html"
        popup_html = f"""
        <div style='font-family: Segoe UI; font-size: 15px;'>
            <strong style='font-size:16px'>{nombre}</strong><br>
            Lat: {lat:.4f}<br>
            Lon: {lon:.4f}<br>
            <a href="{archivo_html}" target="_blank">Ver gráfico</a>
        </div>
        """
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=nombre,
            icon=folium.Icon(color="blue", icon="tint", prefix="fa"),
        ).add_to(marker_cluster)

    mapa_path = BASE_DIR / "mapa_diario_hidro.html"
    m.save(str(mapa_path))
    print(f"Mapa interactivo generado: {mapa_path}")
    if should_open_browser():
        webbrowser.open("file://" + os.path.realpath(mapa_path))


if __name__ == "__main__":
    print("Conectando a la base de datos...")
    df = get_data()
    print("Calculando promedio diario por estación...")
    df_diario = create_diario(df)

    csv_path = BASE_DIR / "promedio_diario_todas_estaciones.csv"
    df_diario.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"CSV generado con datos diarios: {csv_path.name}")

    output_dir = BASE_DIR / "TAB_1"
    output_dir.mkdir(exist_ok=True)
    for nombre_estacion in df_diario["nombre_estacion"].unique():
        df_est = df_diario[df_diario["nombre_estacion"] == nombre_estacion]
        generar_grafico_estacion(df_est, nombre_estacion, output_dir)

    crear_mapa(df_diario)
