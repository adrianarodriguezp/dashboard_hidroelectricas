import os
import json
import configparser
import psycopg2
from ftplib import FTP
from datetime import datetime
import io
import re

# Leer archivo de configuración
config = configparser.ConfigParser()
config.read('config.ini')

# Configuración de PostgreSQL
pg_host = config['Postgres']['host']
pg_port = config['Postgres']['port']
pg_user = config['Postgres']['user']
pg_password = config['Postgres']['password']
pg_dbname = config['Postgres']['dbname']

# Configuración FTP
ftp_url = config['CENACE']['url']
ftp_port = int(config['CENACE']['port'])
ftp_username = config['CENACE']['username']
ftp_password = config['CENACE']['password']

# Diccionario para almacenar coordenadas de estaciones
estaciones = {}

def obtener_ultima_fecha(conn, tabla):
    """Obtiene la fecha más reciente en la tabla especificada"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(fecha_toma_dato) FROM temporales.{tabla}")
        fecha = cursor.fetchone()[0]
        cursor.close()
        return fecha
    except Exception as e:
        print(f"Error al obtener la última fecha de {tabla}: {e}")
        return None

def cargar_coordenadas_manual():
    """Carga las coordenadas manualmente para todas las estaciones conocidas"""
    # Mapeo directo de todas las estaciones conocidas
    coordenadas_estaciones = {
        "Agoyan": [-1.3977000, -78.3829000],
        "Coca Codo Sinclair": [-0.1989618, -77.6827073],
        "Delsitanisagua": [-3.9803410, -79.0169830],
        "Mazar": [-2.5972000, -78.6221000],
        "M_S_Francisco": [-3.3150547, -79.4821319],
        "Pisayambo": [-1.0744510, -78.3968000],
        "Daule_Peripa": [-0.9269476, -79.7482363],
        "Amaluza": [-2.5859180, -78.5583440],
        "Amaluza_Total": [-2.5859180, -78.5583440],
        "Amaluza_Laterales": [-2.5859180, -78.5583440]
    }
    
    # Cargar coordenadas al diccionario
    for nombre, coords in coordenadas_estaciones.items():
        estaciones[nombre] = (coords[0], coords[1])
    
    print(f"Cargadas {len(estaciones)} estaciones con coordenadas manualmente")
    print("Estaciones disponibles:", ", ".join(estaciones.keys()))

def conectar_bd():
    """Establece conexión con la base de datos PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            user=pg_user,
            password=pg_password,
            dbname=pg_dbname
        )
        return conn
    except Exception as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return None

def modificar_tablas(conn):
    """Modifica las tablas para usar tipos de datos más grandes para latitud/longitud"""
    try:
        cursor = conn.cursor()
        
        # Verificar si las columnas ya son de tipo FLOAT
        cursor.execute("""
            SELECT data_type FROM information_schema.columns 
            WHERE table_schema = 'temporales' AND table_name = 'caudales' AND column_name = 'latitud'
        """)
        data_type = cursor.fetchone()[0]
        
        if data_type.lower() != 'double precision':
            # Modificar tabla caudales
            cursor.execute("""
                ALTER TABLE temporales.caudales 
                ALTER COLUMN Latitud TYPE DOUBLE PRECISION,
                ALTER COLUMN Longitud TYPE DOUBLE PRECISION;
            """)
            
            # Modificar tabla nivel
            cursor.execute("""
                ALTER TABLE temporales.nivel 
                ALTER COLUMN Latitud TYPE DOUBLE PRECISION,
                ALTER COLUMN Longitud TYPE DOUBLE PRECISION;
            """)
            
            conn.commit()
            print("Tablas modificadas correctamente para soportar valores de coordenadas más grandes")
        else:
            print("Las tablas ya están usando el tipo de dato correcto (DOUBLE PRECISION)")
        
        cursor.close()
        return True
    except Exception as e:
        print(f"Error al modificar tablas: {e}")
        conn.rollback()
        return False

def insertar_datos(conn, tipo, datos):
    """Inserta los datos en la tabla correspondiente"""
    if not conn:
        print("No hay conexión a la base de datos")
        return
    
    tabla = f"temporales.{tipo}"
    cursor = conn.cursor()
    
    registros_insertados = 0
    registros_fallidos = 0
    
    # Procesar cada dato en una transacción separada
    for dato in datos:
        try:
            # Crear un nuevo cursor para cada operación
            cursor_individual = conn.cursor()
            
            fecha = dato.get('Date')
            nombre_estacion = dato.get('N_Common')
            
            # Verificar si hay valor antes de convertir
            valor_texto = dato.get('Value')
            if valor_texto is None:
                print(f"Advertencia: Valor nulo para {nombre_estacion} en {fecha}")
                continue
                
            try:
                valor = round(float(valor_texto), 3)
            except (ValueError, TypeError) as e:
                print(f"Error al convertir valor '{valor_texto}' para {nombre_estacion}: {e}")
                continue
            
            # Obtener coordenadas
            if nombre_estacion in estaciones:
                latitud, longitud = estaciones[nombre_estacion]
            else:
                print(f"Advertencia: No se encontraron coordenadas para {nombre_estacion}, usando valores por defecto")
                latitud, longitud = 0.0, 0.0
            
            # Intentar insertar el dato
            try:
                cursor_individual.execute(
                    f"INSERT INTO {tabla} (Latitud, Longitud, nombre_estacion, fecha_toma_dato, valor_1h) "
                    f"VALUES (%s, %s, %s, %s, %s)",
                    (latitud, longitud, nombre_estacion, fecha, valor)
                )
                conn.commit()
                registros_insertados += 1
            except Exception as e:
                conn.rollback()
                print(f"Error al insertar datos para {nombre_estacion}: {e}")
                registros_fallidos += 1
            
            # Cerrar el cursor individual
            cursor_individual.close()
                
        except Exception as e:
            print(f"Error procesando dato: {e}")
            registros_fallidos += 1
    
    cursor.close()
    
    if registros_insertados > 0:
        print(f"Se insertaron {registros_insertados} registros en la tabla {tabla}")
    else:
        print(f"No se insertaron registros en la tabla {tabla}")
    
    if registros_fallidos > 0:
        print(f"Fallaron {registros_fallidos} registros en la tabla {tabla}")

def procesar_archivo_json(tipo, contenido):
    """Procesa el contenido JSON de un archivo"""
    try:
        datos = json.loads(contenido)
        if not isinstance(datos, list):
            print(f"Error: El formato del archivo no es una lista JSON")
            return []
        return datos
    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
        return []

def listar_directorio_ftp(ftp, path='.'):
    """Lista el contenido de un directorio FTP"""
    print(f"Contenido del directorio '{path}':")
    try:
        archivos = []
        ftp.dir(archivos.append)
        for archivo in archivos:
            print(f"  {archivo}")
        return archivos
    except Exception as e:
        print(f"Error al listar directorio {path}: {e}")
        return []

def navegar_ftp(ftp, path):
    """Navega a un directorio en el FTP"""
    try:
        ftp.cwd(path)
        print(f"Navegado a directorio '{path}'")
        return True
    except Exception as e:
        print(f"Error al navegar a directorio '{path}': {e}")
        return False

def es_directorio_excluido(nombre):
    """Determina si un directorio debe ser excluido de la exploración"""
    # Excluir SOLO directorios 'historica'
    if nombre.lower() == 'historica':
        return True
    
    return False

def es_archivo_reciente(nombre_archivo, ultima_fecha):
    """Determina si un archivo contiene datos más recientes que la última fecha"""
    if not ultima_fecha:
        return True
    
    # Intentar extraer la fecha del nombre del archivo
    try:
        # Buscar patrón de fecha en el nombre
        if "_" in nombre_archivo:
            partes = nombre_archivo.split('_')
            for parte in partes:
                if parte.startswith('202'):  # Buscar años que comienzan con 202x
                    fecha_str = parte
                    fecha_archivo = datetime.strptime(fecha_str, '%Y%m%d')
                    return fecha_archivo.date() >= ultima_fecha.date()
        
        # Si no podemos determinar la fecha del archivo, procesamos por si acaso
        return True
    except Exception:
        # Si hay error al parsear la fecha, procesamos por si acaso
        return True

def procesar_archivo_ftp(ftp, nombre_archivo, tipo, conn, ultima_fecha):
    """Procesa un archivo JSON desde el FTP si es más reciente que la última fecha"""
    if not es_archivo_reciente(nombre_archivo, ultima_fecha):
        print(f"Omitiendo archivo {nombre_archivo} (anterior a {ultima_fecha})")
        return
    
    try:
        print(f"Procesando archivo: {nombre_archivo}")
        
        # Descargar el contenido del archivo
        contenido = io.BytesIO()
        ftp.retrbinary(f'RETR {nombre_archivo}', contenido.write)
        contenido.seek(0)
        
        # Procesar el archivo JSON
        datos = procesar_archivo_json(tipo, contenido.getvalue().decode('utf-8'))
        if datos:
            # Filtrar datos por fecha
            if ultima_fecha:
                datos_filtrados = [
                    dato for dato in datos 
                    if 'Date' in dato and datetime.strptime(dato['Date'], '%Y-%m-%d %H:%M:%S') > ultima_fecha
                ]
                if len(datos_filtrados) < len(datos):
                    print(f"Filtrados {len(datos) - len(datos_filtrados)} registros anteriores a {ultima_fecha}")
                datos = datos_filtrados
            
            insertar_datos(conn, tipo, datos)
    except Exception as e:
        print(f"Error al procesar archivo {nombre_archivo}: {e}")

def explorar_directorio(ftp, tipo, conn, ultima_fecha, profundidad=0, max_profundidad=5):
    """Explora un directorio buscando archivos JSON evitando subdirectorios excluidos"""
    if profundidad > max_profundidad:
        print(f"Alcanzada profundidad máxima ({max_profundidad}) en {ftp.pwd()}")
        return
    
    print(f"Explorando {tipo} en directorio actual: {ftp.pwd()} (profundidad: {profundidad})")
    
    # Listar archivos en el directorio actual
    archivos = []
    ftp.dir(archivos.append)
    
    archivos_encontrados = 0
    
    # Procesar cada archivo/directorio
    for linea in archivos:
        partes = linea.split()
        if len(partes) < 9:
            continue
        
        nombre = ' '.join(partes[8:])
        es_directorio = linea.startswith('d')
        
        if es_directorio:
            # Es un directorio
            if not es_directorio_excluido(nombre):
                print(f"Explorando subdirectorio: {nombre}")
                # Guardar directorio actual
                dir_actual = ftp.pwd()
                
                # Navegar al subdirectorio
                if navegar_ftp(ftp, nombre):
                    # Explorar recursivamente
                    explorar_directorio(ftp, tipo, conn, ultima_fecha, profundidad + 1, max_profundidad)
                    
                    # Volver al directorio anterior
                    ftp.cwd(dir_actual)
            else:
                print(f"Excluyendo directorio: {nombre}")
        elif nombre.endswith('.json'):
            # Es un archivo JSON
            archivos_encontrados += 1
            procesar_archivo_ftp(ftp, nombre, tipo, conn, ultima_fecha)
    
    if archivos_encontrados == 0:
        print(f"No se encontraron archivos JSON en {ftp.pwd()}")

def main():
    # Cargar coordenadas manualmente
    cargar_coordenadas_manual()
    
    # Conectar a la base de datos
    conn = conectar_bd()
    if not conn:
        return
    
    try:
        # Modificar tablas para soportar valores de coordenadas más grandes
        if not modificar_tablas(conn):
            print("No se pudieron modificar las tablas. Continuando con la estructura actual...")
        
        # Obtener últimas fechas
        ultima_fecha_caudales = obtener_ultima_fecha(conn, 'caudales')
        ultima_fecha_nivel = obtener_ultima_fecha(conn, 'nivel')
        
        print(f"Última fecha en caudales: {ultima_fecha_caudales}")
        print(f"Última fecha en nivel: {ultima_fecha_nivel}")
        
        # Conectar al FTP
        ftp = FTP()
        ftp.connect(ftp_url, ftp_port)
        ftp.login(ftp_username, ftp_password)
        print("Conexión al FTP exitosa")
        
        # Navegar a cenace/caudales
        if navegar_ftp(ftp, 'cenace'):
            # Primero explorar carpeta de caudales
            if navegar_ftp(ftp, 'caudales'):
                print("\n--- PROCESANDO CAUDALES ---")
                listar_directorio_ftp(ftp)
                
                # Explorar carpeta 2026 (la más reciente)
                if navegar_ftp(ftp, '2026'):
                    listar_directorio_ftp(ftp)
                    explorar_directorio(ftp, 'caudales', conn, ultima_fecha_caudales)
                    # Volver a carpeta caudales
                    ftp.cwd('..')
                
                # Volver a carpeta cenace
                ftp.cwd('..')
            
            # Luego explorar carpeta de niveles
            if navegar_ftp(ftp, 'niveles'):
                print("\n--- PROCESANDO NIVELES ---")
                listar_directorio_ftp(ftp)
                
                # Explorar carpeta 2026 (la más reciente)
                if navegar_ftp(ftp, '2026'):
                    listar_directorio_ftp(ftp)
                    explorar_directorio(ftp, 'nivel', conn, ultima_fecha_nivel)
                    # Volver a carpeta niveles
                    ftp.cwd('..')
                
                # Volver a carpeta cenace
                ftp.cwd('..')
        
        # Cerrar conexión FTP
        ftp.quit()
        
    except Exception as e:
        print(f"Error general: {e}")
    finally:
        # Cerrar conexión a la base de datos
        if conn:
            conn.close()
            print("Conexión a la base de datos cerrada")

if __name__ == "__main__":
    main()
