import pandas as pd
import psycopg2
import plotly.express as px
from configparser import ConfigParser

# -------------------------------
# Paso 1: Leer configuración desde .ini
# -------------------------------
def config(filename='config.ini', section='Postgres'):
    parser = ConfigParser()
    parser.read(filename)
    
    db_params = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_params[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in {filename}')
    return db_params

# -------------------------------
# Paso 2: Conectar a la base de datos y extraer los datos
# -------------------------------
def get_hourly_data():
    params = config()
    conn = psycopg2.connect(**params)
    
    query = """
    SELECT nombre_estacion, fecha_toma_dato, valor_1h
    FROM temporales.caudales
    WHERE fecha_toma_dato >= '2020-01-01'
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df

# -------------------------------
# Paso 3: Procesar los datos a formato diario PROMEDIO
# -------------------------------
def process_to_daily(df):
    df['fecha_toma_dato'] = pd.to_datetime(df['fecha_toma_dato'])
    df['fecha_dia'] = df['fecha_toma_dato'].dt.date

    # Agrupar por estación y fecha para calcular el promedio de valores horarios por día
    daily_df = df.groupby(['nombre_estacion', 'fecha_dia'])['valor_1h'].mean().reset_index()
    daily_df.rename(columns={'fecha_dia': 'Fecha', 'valor_1h': 'Caudal Diario Promedio'}, inplace=True)
    
    return daily_df

# -------------------------------
# Paso 4: Graficar con Plotly
# -------------------------------
def plot_daily(daily_df):
    estaciones = daily_df['nombre_estacion'].unique()
    
    for est in estaciones:
        df_est = daily_df[daily_df['nombre_estacion'] == est]
        fig = px.line(df_est, x='Fecha', y='Caudal Diario Promedio', title=f'Promedio Diario - {est}')
        fig.update_traces(mode='lines+markers')
        fig.update_layout(
            xaxis_title='Fecha',
            yaxis_title='Caudal Diario Promedio (m³/s)'
        )
        fig.write_html(f'promedio_diario_{est}.html')
        print(f"Gráfico guardado para {est}")

# -------------------------------
# Ejecutar todo
# -------------------------------
if __name__ == '__main__':
    df = get_hourly_data()
    print("Registros horarios recuperados:", len(df))
    print(df.head())

    df_diario = process_to_daily(df)
    print("Registros diarios procesados:", len(df_diario))
    print(df_diario.head())

    # Guardar como CSV
    df_diario.to_csv('promedio_diario_todas_estaciones.csv', index=False)
    print("CSV guardado como promedio_diario_todas_estaciones.csv")

    # Graficar
    plot_daily(df_diario)