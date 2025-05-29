import pandas as pd
import psycopg2
import folium
from folium.plugins import MarkerCluster
from configparser import ConfigParser
import plotly.express as px
import webbrowser
import os
from pathlib import Path

# ---------- PASO 1: Leer configuración -----------------
def config(filename='config.ini', section='Postgres'):
    parser = ConfigParser()
    parser.read(filename)

    if not parser.has_section(section):
        raise Exception(f'❌ Error: la sección [{section}] no se encuentra en {filename}')
    
    return {param[0]: param[1] for param in parser.items(section)}

# ---------- PASO 2: Obtener datos desde PostgreSQL ----------
def get_data():
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
    WHERE fecha_toma_dato >= '2020-01-01'
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df['fecha_toma_dato'] = pd.to_datetime(df['fecha_toma_dato'])
    df['fecha_real'] = df['fecha_toma_dato'] - pd.to_timedelta("30min")
    df['fecha_dia'] = df['fecha_real'].dt.date

    return df

# ---------- PASO 3: Crear promedio diario ----------
def create_diario(df):
    return df.groupby(['nombre_estacion', 'latitud', 'longitud', 'fecha_dia'])['valor_1h'].mean().reset_index().rename(
        columns={'fecha_dia': 'Fecha', 'valor_1h': 'Caudal Diario Promedio'}
    )

# ---------- PASO 4: Generar gráfico por estación ----------
def generar_grafico_estacion(df_est, nombre_estacion, output_dir):
    import json

    nombre_clean = nombre_estacion.replace(" ", "_")
    file_path = output_dir / f"grafico_{nombre_clean}.html"

    fig = px.line(
        df_est, x='Fecha', y='Caudal Diario Promedio',
        title=f'Caudal Diario Promedio - {nombre_estacion}',
        labels={'Fecha': 'Fecha', 'Caudal Diario Promedio': 'Q Diario (m³/s)'}
    )
    fig.update_traces(mode='lines+markers')
    fig.update_layout(height=600)

    # Guardar los datos como JSON
    data_json = df_est.to_json(orient='records', date_format='iso')

    # HTML template
    html_content = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Caudal Diario Promedio - {nombre_estacion}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
    <h2>Caudal Diario Promedio - {nombre_estacion}</h2>

    <label for="fecha-inicial">Fecha Inicial:</label>
    <input type="date" id="fecha-inicial" name="fecha-inicial">

    <label for="fecha-final">Fecha Final:</label>
    <input type="date" id="fecha-final" name="fecha-final">

    <button onclick="filtrar()">Filtrar</button>

    <div id="grafico" style="width:100%; height:600px;"></div>

    <script>
    const registros = {data_json};

    function filtrar() {{
        const inicio = document.getElementById('fecha-inicial').value;
        const fin = document.getElementById('fecha-final').value;

        if (!inicio || !fin) {{
            alert("Por favor selecciona ambas fechas.");
            return;
        }}

        const data_filtrada = registros.filter(r => {{
            const f = new Date(r.Fecha);
            return f >= new Date(inicio) && f <= new Date(fin);
        }});

        const trace = {{
            x: data_filtrada.map(r => r.Fecha),
            y: data_filtrada.map(r => r["Caudal Diario Promedio"]),
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

    // Mostrar todos al cargar
    window.onload = filtrar;
    </script>
</body>
</html>
"""

    # Escribir el archivo
    with open(file_path, "w", encoding='utf-8') as f:
        f.write(html_content)

    print(f"📈 Gráfico interactivo con calendario generado: {file_path.name}")

# ---------- PASO 5: Crear mapa con links a gráficos ----------
def crear_mapa(df_diario, output_dir):
    m = folium.Map(location=[-1.5, -78.0], zoom_start=6)
    marker_cluster = MarkerCluster().add_to(m)

    estaciones = df_diario[['nombre_estacion', 'latitud', 'longitud']].drop_duplicates()

    for _, row in estaciones.iterrows():
        nombre = row['nombre_estacion']
        lat, lon = row['latitud'], row['longitud']
        nombre_clean = nombre.replace(" ", "_")
        archivo_html = f"TAB_1/grafico_{nombre_clean}.html"

        popup_html = f"""
        <b>{nombre}</b><br>
        <a href="{archivo_html}" target="_blank">Ver gráfico</a>
        """
        popup = folium.Popup(popup_html, max_width=250)

        folium.Marker(
            location=[lat, lon],
            popup=popup,
            tooltip=nombre,
            icon=folium.Icon(color="blue", icon="tint", prefix="fa")
        ).add_to(marker_cluster)

    mapa_path = Path(__file__).parent / "mapa_diario_hidro.html"
    m.save(str(mapa_path))
    print(f"🗺️  Mapa interactivo generado: {mapa_path}")
    webbrowser.open('file://' + os.path.realpath(mapa_path))

# ---------- MAIN ----------
if __name__ == "__main__":
    print("🔄 Conectando a la base de datos...")
    df = get_data()

    print("📊 Calculando promedio diario por estación...")
    df_diario = create_diario(df)

    print("📈 Generando gráficos para todas las estaciones...")
    output_dir = Path(__file__).parent / "TAB_1"
    output_dir.mkdir(exist_ok=True)

    for nombre_estacion in df_diario['nombre_estacion'].unique():
        df_est = df_diario[df_diario['nombre_estacion'] == nombre_estacion]
        generar_grafico_estacion(df_est, nombre_estacion, output_dir)

    print("🌍 Generando mapa interactivo...")
    crear_mapa(df_diario, output_dir)