import pandas as pd
import psycopg2
import folium
from folium.plugins import MarkerCluster
from configparser import ConfigParser
import plotly.express as px
import webbrowser
import os
from pathlib import Path
import subprocess

# 1) Config: lee credenciales desde config.ini junto al script

def config(filename='config.ini', section='Postgres'):
    cfg_path = Path(__file__).parent / filename
    parser = ConfigParser()
    read_files = parser.read(cfg_path, encoding="utf-8")
    if not read_files:
        raise FileNotFoundError(f"❌ No se pudo leer el archivo de configuración: {cfg_path}")
    if not parser.has_section(section):
        raise Exception(f"❌ Error: la sección [{section}] no se encuentra en {cfg_path}")
    return {k: v for k, v in parser.items(section)}

# 2) Ingesta: datos HORARIOS

def get_data_horario():
    params = config()
    conn = psycopg2.connect(
        host=params['host'],
        port=params['port'],
        user=params['user'],
        password=params['password'],
        database=params['dbname']
    )
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

    # Parseo y limpieza
    df['fecha_toma_dato'] = pd.to_datetime(df['fecha_toma_dato'], errors='coerce', utc=False)
    df = df.dropna(subset=["valor_1h", "fecha_toma_dato"])

    # Ajuste de media hora para alinear al fin de intervalo (igual que en diario)
    df['FechaHora'] = df['fecha_toma_dato'] - pd.to_timedelta("30min")
    # Reordenamos columnas para comodidad
    df = df[['nombre_estacion', 'latitud', 'longitud', 'FechaHora', 'valor_1h']].sort_values(['nombre_estacion','FechaHora'])
    return df

# 3) Gráfico por estación (HTML estático con Flatpickr date-time)

def generar_grafico_estacion_horario(df_est, nombre_estacion, output_dir):
    nombre_clean = nombre_estacion.replace(" ", "_")
    file_path = output_dir / f"grafico_{nombre_clean}.html"

    # Plotly base (no es imprescindible renderizar aquí, pero útil si abres local)
    fig = px.line(
        df_est, x='FechaHora', y='valor_1h',
        title=f'Caudal Horario - {nombre_estacion}',
        labels={'FechaHora': 'Fecha-Hora', 'valor_1h': 'Q 1h (m³/s)'}
    )
    fig.update_traces(mode='lines+markers')
    fig.update_layout(height=600)

    # Datos a JSON para filtrado en el navegador (ISO a milisegundos seguro)
    data_json = df_est.to_json(orient='records', date_format='iso')

    # HTML autónomo: Plotly + Flatpickr con hora habilitada
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
    // Inicializa Flatpickr con selección de hora
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
            // ✅ 2 decimales en el tooltip y fecha legible
            hovertemplate: '%{{y:.2f}} m³/s<br>%{{x|%Y-%m-%d %H:%M}}<extra></extra>'
        }};
        const layout = {{
            xaxis: {{ title: 'Fecha-Hora' }},
            // ✅ 2 decimales en los ticks del eje Y
            yaxis: {{ title: 'Q 1h (m³/s)', tickformat: '.2f' }},
            height: 600
        }};
        Plotly.newPlot('grafico', [trace], layout);
    }}

    function filtrar() {{
        const inicioStr = document.getElementById('fecha-inicial').value;
        const finStr = document.getElementById('fecha-final').value;
        if (!inicioStr || !finStr) {{
            alert("Por favor selecciona ambas fechas-horas.");
            return;
        }}
        const inicio = new Date(inicioStr.replace(' ', 'T'));
        const fin = new Date(finStr.replace(' ', 'T'));
        const data_filtrada = registros.filter(r => {{
            const f = new Date(r.FechaHora);
            return f >= inicio && f <= fin;
        }});
        dibujar(data_filtrada);
    }}

    function resetear() {{
        document.getElementById('fecha-inicial').value = '';
        document.getElementById('fecha-final').value = '';
        dibujar(registros);
    }}

    // Render inicial con todos los datos
    window.onload = () => dibujar(registros);
    </script>
</body>
</html>
"""
    file_path.write_text(html_content, encoding='utf-8')
    print(f"📈 (Horario) Gráfico generado: {file_path.name}")

# 4) Mapa Folium que enlaza a TAB_3/*

def crear_mapa_horario(df_horario):
    m = folium.Map(location=[-1.5, -78.0], zoom_start=6)
    marker_cluster = MarkerCluster().add_to(m)

    estaciones = df_horario[['nombre_estacion', 'latitud', 'longitud']].drop_duplicates()
    for _, row in estaciones.iterrows():
        nombre = row['nombre_estacion']
        lat, lon = row['latitud'], row['longitud']
        nombre_clean = nombre.replace(" ", "_")
        # Importante: TAB_3 en el href
        archivo_html = f"TAB_3/grafico_{nombre_clean}.html"

        popup_html = f"""
        <div style='font-family: Segoe UI; font-size: 15px;'>
            <strong style='font-size:16px'>{nombre}</strong><br>
            📍 Lat: {lat:.4f}<br>
            📍 Lon: {lon:.4f}<br>
            <a href="{archivo_html}" target="_blank">📈 Ver gráfico horario</a>
        </div>
        """
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=nombre,
            icon=folium.Icon(color="green", icon="time", prefix="fa")
        ).add_to(marker_cluster)

    mapa_path = Path(__file__).parent / "mapa_horario_hidro.html"
    m.save(str(mapa_path))
    print(f"🌍 (Horario) Mapa interactivo generado: {mapa_path}")
    # Opcional local
    try:
        webbrowser.open('file://' + os.path.realpath(mapa_path))
    except Exception:
        pass

# MAIN

if __name__ == "__main__":
    print("🔄 Conectando a la base de datos (horario)...")
    df_h = get_data_horario()

    # CSV con TODO horario
    csv_path = Path(__file__).parent / "horario_todas_estaciones.csv"
    df_h.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"📝 CSV horario generado: {csv_path.name}")

    # Carpeta de salida TAB_3
    output_dir = Path(__file__).parent / "TAB_3"
    output_dir.mkdir(exist_ok=True)

    print("📈 Generando gráficos horarios por estación...")
    for nombre_estacion in df_h['nombre_estacion'].unique():
        df_est = df_h[df_h['nombre_estacion'] == nombre_estacion]
        generar_grafico_estacion_horario(df_est, nombre_estacion, output_dir)

    print("🌍 Generando mapa horario...")
    crear_mapa_horario(df_h)

    print("✅ Proceso horario finalizado.")