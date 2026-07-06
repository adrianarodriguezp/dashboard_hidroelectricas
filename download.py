import os
import json
import configparser
import psycopg2
from ftplib import FTP
from datetime import datetime, timedelta
import io
from pathlib import Path


config = configparser.ConfigParser()
project_root = Path(os.environ.get("CENACE_ROOT", Path(__file__).resolve().parent)).resolve()
config_path = project_root / "config.ini"

if not config.read(config_path, encoding="utf-8"):
    raise FileNotFoundError(f"No se pudo leer la configuración: {config_path}")

pg_host = config["Postgres"]["host"]
pg_port = config["Postgres"]["port"]
pg_user = config["Postgres"]["user"]
pg_password = config["Postgres"]["password"]
pg_dbname = config["Postgres"]["dbname"]

ftp_url = config["CENACE"]["url"]
ftp_port = int(config["CENACE"]["port"])
ftp_username = config["CENACE"]["username"]
ftp_password = config["CENACE"]["password"]

LOOKBACK_DAYS = 12

estaciones = {
    "Agoyan": (-1.3977000, -78.3829000),
    "Coca Codo Sinclair": (-0.1989618, -77.6827073),
    "Delsitanisagua": (-3.9803410, -79.0169830),
    "Mazar": (-2.5972000, -78.6221000),
    "M_S_Francisco": (-3.3150547, -79.4821319),
    "Pisayambo": (-1.0744510, -78.3968000),
    "Daule_Peripa": (-0.9269476, -79.7482363),
    "Amaluza": (-2.5859180, -78.5583440),
    "Amaluza_Total": (-2.5859180, -78.5583440),
    "Amaluza_Laterales": (-2.5859180, -78.5583440),
}


def conectar_bd():
    try:
        return psycopg2.connect(
            host=pg_host,
            port=pg_port,
            user=pg_user,
            password=pg_password,
            dbname=pg_dbname,
        )
    except Exception as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return None


def obtener_ultima_fecha(conn, tabla):
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(fecha_toma_dato) FROM temporales.{tabla}")
            return cur.fetchone()[0]
    except Exception as e:
        print(f"Error al obtener la última fecha de {tabla}: {e}")
        return None


def modificar_tablas(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = 'temporales'
                  AND table_name = 'caudales'
                  AND column_name = 'latitud'
            """)
            data_type = cur.fetchone()[0]

            if data_type.lower() != "double precision":
                cur.execute("""
                    ALTER TABLE temporales.caudales
                    ALTER COLUMN Latitud TYPE DOUBLE PRECISION,
                    ALTER COLUMN Longitud TYPE DOUBLE PRECISION
                """)
                cur.execute("""
                    ALTER TABLE temporales.nivel
                    ALTER COLUMN Latitud TYPE DOUBLE PRECISION,
                    ALTER COLUMN Longitud TYPE DOUBLE PRECISION
                """)
                conn.commit()
                print("Tablas modificadas correctamente.")
            else:
                print("Las tablas ya usan DOUBLE PRECISION.")

        return True
    except Exception as e:
        conn.rollback()
        print(f"Error al modificar tablas: {e}")
        return False


def procesar_archivo_json(contenido):
    try:
        datos = json.loads(contenido)
        if not isinstance(datos, list):
            print("Error: el JSON no contiene una lista.")
            return []
        return datos
    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
        return []


def insertar_o_actualizar_datos(conn, tabla, datos, fecha_corte):
    registros_insertados = 0
    registros_actualizados = 0
    registros_omitidos = 0
    registros_fallidos = 0

    for dato in datos:
        try:
            fecha_txt = dato.get("Date")
            nombre_estacion = dato.get("N_Common")
            valor_txt = dato.get("Value")

            if not fecha_txt or not nombre_estacion or valor_txt is None:
                registros_fallidos += 1
                continue

            fecha = datetime.strptime(fecha_txt, "%Y-%m-%d %H:%M:%S")

            if fecha_corte and fecha < fecha_corte:
                registros_omitidos += 1
                continue

            valor = round(float(valor_txt), 3)
            latitud, longitud = estaciones.get(nombre_estacion, (0.0, 0.0))

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE temporales.{tabla}
                    SET Latitud = %s,
                        Longitud = %s,
                        valor_1h = %s
                    WHERE nombre_estacion = %s
                      AND fecha_toma_dato = %s
                      AND valor_1h IS DISTINCT FROM %s
                    RETURNING 1
                    """,
                    (latitud, longitud, valor, nombre_estacion, fecha, valor),
                )

                if cur.fetchone():
                    conn.commit()
                    registros_actualizados += 1
                    continue

                cur.execute(
                    f"""
                    SELECT 1
                    FROM temporales.{tabla}
                    WHERE nombre_estacion = %s
                      AND fecha_toma_dato = %s
                    LIMIT 1
                    """,
                    (nombre_estacion, fecha),
                )

                if cur.fetchone():
                    registros_omitidos += 1
                    continue

                cur.execute(
                    f"""
                    INSERT INTO temporales.{tabla}
                        (Latitud, Longitud, nombre_estacion, fecha_toma_dato, valor_1h)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (latitud, longitud, nombre_estacion, fecha, valor),
                )

                conn.commit()
                registros_insertados += 1

        except Exception as e:
            conn.rollback()
            registros_fallidos += 1
            print(f"Error procesando dato: {e}")

    print(f"Tabla temporales.{tabla}: insertados={registros_insertados}, actualizados={registros_actualizados}, omitidos={registros_omitidos}, fallidos={registros_fallidos}")


def listar_fechas(fecha_inicio, fecha_fin):
    fecha = fecha_inicio.date()
    fin = fecha_fin.date()

    while fecha <= fin:
        yield fecha
        fecha += timedelta(days=1)


def procesar_rango_ftp(ftp, conn, carpeta_ftp, tabla, fecha_corte, fecha_fin):
    print(f"\n--- PROCESANDO {tabla.upper()} DESDE {fecha_corte} ---")

    for fecha in listar_fechas(fecha_corte, fecha_fin):
        for hora in range(24):
            ruta = f"/home/cenace/cenace/{carpeta_ftp}/{fecha:%Y/%m/%d}/{hora:02d}:00"

            try:
                ftp.cwd(ruta)
            except Exception:
                continue

            try:
                archivos = [a for a in ftp.nlst() if a.endswith(".json")]
            except Exception as e:
                print(f"No se pudo listar {ruta}: {e}")
                continue

            for archivo in archivos:
                try:
                    print(f"Procesando {ruta}/{archivo}")
                    contenido = io.BytesIO()
                    ftp.retrbinary(f"RETR {archivo}", contenido.write)
                    contenido.seek(0)

                    datos = procesar_archivo_json(contenido.getvalue().decode("utf-8"))
                    if datos:
                        insertar_o_actualizar_datos(conn, tabla, datos, fecha_corte)

                except Exception as e:
                    print(f"Error al procesar {ruta}/{archivo}: {e}")


def main():
    print(f"Estaciones configuradas: {', '.join(estaciones.keys())}")
    print(f"Ventana retrospectiva activa: {LOOKBACK_DAYS} días")

    conn = conectar_bd()
    if not conn:
        return 1

    ftp = None

    try:
        modificar_tablas(conn)

        ultima_fecha_caudales = obtener_ultima_fecha(conn, "caudales")
        ultima_fecha_nivel = obtener_ultima_fecha(conn, "nivel")

        ahora = datetime.now()

        fecha_corte_caudales = (
            ultima_fecha_caudales - timedelta(days=LOOKBACK_DAYS)
            if ultima_fecha_caudales else ahora - timedelta(days=LOOKBACK_DAYS)
        )

        fecha_corte_nivel = (
            ultima_fecha_nivel - timedelta(days=LOOKBACK_DAYS)
            if ultima_fecha_nivel else ahora - timedelta(days=LOOKBACK_DAYS)
        )

        print(f"Última fecha en caudales: {ultima_fecha_caudales}")
        print(f"Última fecha en nivel: {ultima_fecha_nivel}")
        print(f"Revisando caudales desde: {fecha_corte_caudales}")
        print(f"Revisando nivel desde: {fecha_corte_nivel}")

        ftp = FTP()
        ftp.connect(ftp_url, ftp_port)
        ftp.login(ftp_username, ftp_password)
        print("Conexión al FTP exitosa")

        procesar_rango_ftp(ftp, conn, "caudales", "caudales", fecha_corte_caudales, ahora)
        procesar_rango_ftp(ftp, conn, "niveles", "nivel", fecha_corte_nivel, ahora)

    except Exception as e:
        print(f"Error general: {e}")
        return 1

    finally:
        if ftp:
            try:
                ftp.quit()
            except Exception:
                pass

        conn.close()
        print("Conexión a la base de datos cerrada")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())