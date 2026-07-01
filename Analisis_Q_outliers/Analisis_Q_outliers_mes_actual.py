#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from configparser import ConfigParser
from zoneinfo import ZoneInfo
import re
import numpy as np
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from common_runtime import analysis_years, load_config


# =========================================================
# 1) CONFIGURACIÓN GENERAL
# =========================================================
BASE_DIR = Path(__file__).parent
RECENT_YEARS = analysis_years(history_years=2)

OUT_ORIG_DIR = BASE_DIR / "01_series_originales"
OUT_CLEAN_DIR = BASE_DIR / "02_series_limpias"
OUT_OUTLIERS_DIR = BASE_DIR / "03_outliers_detectados"
OUT_PLOTS_DIR = BASE_DIR / "04_graficas_revision"
OUT_SUMMARY_DIR = BASE_DIR / "05_resumen_estaciones"

OUT_DAILY_ALL_DIR = BASE_DIR / "06_diarios_corregidos"
OUT_DAILY_STATIONS_DIR = BASE_DIR / "07_diarios_corregidos_por_hidroelectrica"
OUT_MONTHLY_DIR = BASE_DIR / "08_mensualizado_corregido"
OUT_MONTHLY_PLOTS_DIR = BASE_DIR / "09_graficas_mensuales_corregidas"

for folder in [
    OUT_ORIG_DIR, OUT_CLEAN_DIR, OUT_OUTLIERS_DIR, OUT_PLOTS_DIR, OUT_SUMMARY_DIR,
    OUT_DAILY_ALL_DIR, OUT_DAILY_STATIONS_DIR, OUT_MONTHLY_DIR, OUT_MONTHLY_PLOTS_DIR
]:
    folder.mkdir(parents=True, exist_ok=True)


# =========================================================
# 2) CONFIGURACIÓN DE MESES
# =========================================================
MONTH_MAP_NUM2TXT = {
    1: "ENE",  2: "FEB",  3: "MAR",  4: "ABR",
    5: "MAY",  6: "JUN",  7: "JUL",  8: "AGO",
    9: "SEP", 10: "OCT", 11: "NOV", 12: "DIC"
}
MONTH_ORDER = [MONTH_MAP_NUM2TXT[m] for m in range(1, 13)]
MONTH_TXT2NUM = {v: k for k, v in MONTH_MAP_NUM2TXT.items()}
MESES_TICKS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]


# =========================================================
# 3) LECTURA DE CONFIGURACIÓN
# =========================================================
def config(filename="config.ini", section="Postgres"):
    posibles_rutas = [
        Path(__file__).parent / filename,
        Path(__file__).resolve().parent.parent / filename,
    ]

    for cfg_path in posibles_rutas:
        if cfg_path.exists():
            print(f"OK: Configuracion leida desde: {cfg_path}")
            return load_config(filename=filename, section=section, base_dir=cfg_path.parent)

    rutas_txt = "\n".join(str(p) for p in posibles_rutas)
    raise FileNotFoundError(f"ERROR: No se pudo leer el archivo de configuracion. Se busco en:\n{rutas_txt}")


# =========================================================
# 4) CONSULTA A LA BASE DE DATOS - SOLO MES ACTUAL
# =========================================================
def get_data_horario_mes_actual():
    """
    Consulta el mes que contiene al último día completo, únicamente hasta el
    inicio del día actual en hora local de Ecuador (America/Guayaquil).
    En el primer día de un mes procesa el mes anterior completo; el resto de
    días procesa el mes actual hasta ayer.
    """
    params = config()

    conn = psycopg2.connect(
        host=params["host"],
        port=params["port"],
        user=params["user"],
        password=params["password"],
        database=params["dbname"]
    )

    tz_ec = ZoneInfo("America/Guayaquil")
    ahora_ec = pd.Timestamp.now(tz=tz_ec)
    hoy_ec = ahora_ec.normalize()  # 00:00 del día actual en Ecuador
    ultimo_dia_completo_ec = hoy_ec - pd.Timedelta(days=1)
    inicio_mes_ec = ultimo_dia_completo_ec.replace(day=1)

    # Quitamos zona horaria para comparar contra timestamp de la BD
    inicio_mes = inicio_mes_ec.tz_localize(None)
    fin_consulta = hoy_ec.tz_localize(None)

    query = f"""
    SELECT
        nombre_estacion,
        latitud,
        longitud,
        fecha_toma_dato,
        valor_1h
    FROM temporales.caudales
    WHERE fecha_toma_dato >= '{inicio_mes.strftime("%Y-%m-%d %H:%M:%S")}'
      AND fecha_toma_dato < '{fin_consulta.strftime("%Y-%m-%d %H:%M:%S")}'
      AND valor_1h IS NOT NULL
      AND TRIM(valor_1h::text) NOT ILIKE 'nan'
      AND valor_1h > 0
    ORDER BY nombre_estacion, fecha_toma_dato
    """

    df = pd.read_sql(query, conn)
    conn.close()

    df["fecha_toma_dato"] = pd.to_datetime(df["fecha_toma_dato"], errors="coerce", utc=False)
    df["valor_1h"] = pd.to_numeric(df["valor_1h"], errors="coerce")

    df = df.dropna(subset=["nombre_estacion", "fecha_toma_dato", "valor_1h"]).copy()
    df["FechaHora"] = df["fecha_toma_dato"] - pd.to_timedelta("30min")

    df = df[
        ["nombre_estacion", "latitud", "longitud", "FechaHora", "valor_1h"]
    ].sort_values(["nombre_estacion", "FechaHora"]).reset_index(drop=True)

    return df

# =========================================================
# 5) PARÁMETROS DE DETECCIÓN
# =========================================================
WINDOW_HOURS = 3
MAD_FACTOR = 6
JUMP_FACTOR = 5
ISOLATION_RATIO = 0.5
MIN_VALID_VALUE = 0


# =========================================================
# 6) FUNCIONES AUXILIARES
# =========================================================
def safe_name(texto: str) -> str:
    texto = str(texto).strip().replace(" ", "_").replace("/", "_")
    return re.sub(r"[^A-Za-z0-9_\-]", "", texto)


def require_cols(df: pd.DataFrame, required: set):
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")


def mad(arr):
    arr = np.asarray(arr, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) == 0:
        return np.nan
    med = np.median(arr)
    return np.median(np.abs(arr - med))


# =========================================================
# 7) DETECCIÓN Y CORRECCIÓN DE OUTLIERS
# =========================================================
def detectar_outliers_locales(df_est, col_fecha="FechaHora", col_q="valor_1h"):
    df = df_est.copy().sort_values(col_fecha).reset_index(drop=True)
    q = df[col_q].astype(float)

    df["q_prev"] = q.shift(1)
    df["q_next"] = q.shift(-1)

    local_median, local_mad, diff_prev, diff_next = [], [], [], []

    for i in range(len(df)):
        i0 = max(0, i - WINDOW_HOURS)
        i1 = min(len(df), i + WINDOW_HOURS + 1)

        ventana = q.iloc[i0:i1].copy()
        ventana = ventana.drop(index=i, errors="ignore")

        med = np.nanmedian(ventana) if len(ventana.dropna()) > 0 else np.nan
        m = mad(ventana)

        local_median.append(med)
        local_mad.append(m)

        cur_val = df.loc[i, col_q]
        prev_val = df.loc[i, "q_prev"]
        next_val = df.loc[i, "q_next"]

        diff_prev.append(np.nan if pd.isna(prev_val) else abs(cur_val - prev_val))
        diff_next.append(np.nan if pd.isna(next_val) else abs(cur_val - next_val))

    df["mediana_local"] = local_median
    df["mad_local"] = local_mad
    df["diff_prev"] = diff_prev
    df["diff_next"] = diff_next

    df["umbral_robusto"] = df["mediana_local"] + MAD_FACTOR * df["mad_local"]

    cambios = q.diff().abs().dropna()
    salto_umbral = np.nan
    if len(cambios) > 0:
        salto_ref = np.nanpercentile(cambios, 90)
        salto_umbral = salto_ref * JUMP_FACTOR

    criterio_extremo = (
        (df[col_q] > df["umbral_robusto"]) &
        (df[col_q] > MIN_VALID_VALUE)
    )

    criterio_salto = (
        (df["diff_prev"] > salto_umbral) &
        (df["diff_next"] > salto_umbral)
    )

    criterio_aislado = (
        (df["q_prev"] < df[col_q] * ISOLATION_RATIO) &
        (df["q_next"] < df[col_q] * ISOLATION_RATIO)
    )

    df["es_outlier"] = (
        criterio_extremo.fillna(False) &
        criterio_salto.fillna(False) &
        criterio_aislado.fillna(False)
    )

    return df


def corregir_outliers(df_est, col_fecha="FechaHora", col_q="valor_1h"):
    df = df_est.copy().sort_values(col_fecha).reset_index(drop=True)

    df["Q_original"] = df[col_q].astype(float)
    df["Q_corregido"] = df["Q_original"].copy()
    df["metodo_relleno"] = ""
    df["fue_corregido"] = False

    mask = df["es_outlier"] == True
    df.loc[mask, "Q_corregido"] = np.nan
    df.loc[mask, "metodo_relleno"] = "interpolacion_temporal"
    df.loc[mask, "fue_corregido"] = True

    df = df.set_index(col_fecha)
    df["Q_corregido"] = df["Q_corregido"].interpolate(method="time", limit_direction="both")
    df = df.reset_index()

    return df


def generar_resumen_estacion(df_corregido, station_name):
    total = len(df_corregido)
    n_outliers = int(df_corregido["es_outlier"].sum())
    pct_outliers = (n_outliers / total * 100) if total > 0 else 0

    return {
        "nombre_estacion": station_name,
        "total_registros": total,
        "outliers_detectados": n_outliers,
        "porcentaje_outliers": round(pct_outliers, 4),
        "fecha_inicio": df_corregido["FechaHora"].min(),
        "fecha_fin": df_corregido["FechaHora"].max(),
        "caudal_min_original": round(df_corregido["Q_original"].min(), 3),
        "caudal_max_original": round(df_corregido["Q_original"].max(), 3),
        "caudal_min_corregido": round(df_corregido["Q_corregido"].min(), 3),
        "caudal_max_corregido": round(df_corregido["Q_corregido"].max(), 3),
    }


def guardar_resultados_horarios(df_corregido, station_name):
    nombre = safe_name(station_name)

    out_orig = OUT_ORIG_DIR / f"{nombre}_original.csv"
    out_clean = OUT_CLEAN_DIR / f"{nombre}_limpio.csv"
    out_outliers = OUT_OUTLIERS_DIR / f"{nombre}_outliers.csv"
    out_plot = OUT_PLOTS_DIR / f"{nombre}_revision.png"

    df_corregido.to_csv(out_orig, index=False, encoding="utf-8")

    columnas_limpias = [
        "nombre_estacion", "latitud", "longitud", "FechaHora",
        "Q_original", "Q_corregido", "es_outlier", "fue_corregido", "metodo_relleno"
    ]
    df_corregido[columnas_limpias].to_csv(out_clean, index=False, encoding="utf-8")
    df_corregido[df_corregido["es_outlier"] == True].to_csv(out_outliers, index=False, encoding="utf-8")

    plt.figure(figsize=(16, 5))
    plt.plot(df_corregido["FechaHora"], df_corregido["Q_original"], label="Original", linewidth=1.1)
    plt.plot(df_corregido["FechaHora"], df_corregido["Q_corregido"], label="Corregido", linewidth=1.1)

    outliers = df_corregido[df_corregido["es_outlier"] == True]
    if not outliers.empty:
        plt.scatter(outliers["FechaHora"], outliers["Q_original"], s=18, label="Outliers detectados")

    plt.xlabel("Fecha-Hora")
    plt.ylabel("Q 1h (m³/s)")
    plt.title(f"Serie horaria original vs corregida - {station_name}")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_plot, dpi=180)
    plt.close()


# =========================================================
# 8) DIARIO CORREGIDO
# =========================================================
def calcular_diario_corregido(df_corregido):
    df = df_corregido.copy()
    df["Fecha"] = pd.to_datetime(df["FechaHora"]).dt.floor("D")

    diario = (
        df.groupby(["nombre_estacion", "latitud", "longitud", "Fecha"], as_index=False)["Q_corregido"]
        .mean()
        .rename(columns={"Q_corregido": "Caudal Diario Promedio"})
    )

    diario = diario.sort_values(["nombre_estacion", "Fecha"]).reset_index(drop=True)
    return diario


def guardar_diarios_corregidos(df_diario):
    out_all = OUT_DAILY_ALL_DIR / "promedio_diario_todas_estaciones_corregido.csv"
    df_diario.to_csv(out_all, index=False, encoding="utf-8")

    estaciones = sorted(df_diario["nombre_estacion"].dropna().unique())
    for st in estaciones:
        st_df = df_diario[df_diario["nombre_estacion"] == st].copy()
        out_csv = OUT_DAILY_STATIONS_DIR / f"{safe_name(st)}_diario_corregido.csv"
        st_df.to_csv(out_csv, index=False, encoding="utf-8")

    return out_all


# =========================================================
# 9) MENSUALIZADO CORREGIDO
# =========================================================
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


def plot_station_monthly(st_name: str, table: pd.DataFrame, out_png: Path) -> None:
    table = table.copy()
    table["Year"] = pd.to_numeric(table["Year"], errors="coerce")
    table = table.dropna(subset=["Year"])
    table["Year"] = table["Year"].astype(int)

    hist = table[table["Year"] <= 2023].copy()

    hist_long = hist.melt(id_vars=["Year"], var_name="Mes", value_name="value")
    hist_long["MesNum"] = hist_long["Mes"].map(MONTH_TXT2NUM)
    hist_long["value"] = pd.to_numeric(hist_long["value"], errors="coerce")
    hist_long = hist_long.dropna(subset=["MesNum", "value"])

    if not hist_long.empty:
        max_hist = hist_long.groupby("MesNum")["value"].max()
        min_hist = hist_long.groupby("MesNum")["value"].min()
        mean_hist = hist_long.groupby("MesNum")["value"].mean()
        p10 = hist_long.groupby("MesNum")["value"].quantile(0.10)
        p90 = hist_long.groupby("MesNum")["value"].quantile(0.90)
        start_year = int(hist["Year"].min())
        end_year = int(hist["Year"].max())
    else:
        max_hist = min_hist = mean_hist = p10 = p90 = None
        start_year = end_year = None

    recent_curves = {year: year_curve_monthly(table, year) for year in RECENT_YEARS}

    if hist_long.empty and all(curve is None for curve in recent_curves.values()):
        print(f"WARNING: {st_name}: sin datos suficientes para graficar.")
        return

    plt.figure(figsize=(10, 6))

    if not hist_long.empty:
        for yr in sorted(hist_long["Year"].unique()):
            d = hist_long[hist_long["Year"] == yr].sort_values("MesNum")
            plt.plot(d["MesNum"], d["value"], color="gray", alpha=0.2, label="_nolegend_")

        plt.plot(max_hist.index, max_hist.values, "g--", linewidth=1, label="Máximo Histórico")
        plt.plot(min_hist.index, min_hist.values, "m--", linewidth=1, label="Mínimo Histórico")
        plt.plot(mean_hist.index, mean_hist.values, "b--", linewidth=1.5, label="Promedio Histórico")
        plt.fill_between(p10.index, p10.values, p90.values, alpha=0.35, label="Percentil 10-90%")
        plt.plot([], [], color="gray", label=f"Históricos ({start_year}-2023)")

    colors = ["r", "g", "black", "orange", "purple"]
    for idx, year in enumerate(RECENT_YEARS):
        curve = recent_curves[year]
        if curve is not None:
            label = f"{year} (mes en curso)" if year == RECENT_YEARS[-1] else str(year)
            plt.plot(
                curve["MesNum"],
                curve["value"],
                color=colors[idx % len(colors)],
                linewidth=2,
                marker="o" if year == RECENT_YEARS[-1] else None,
                markersize=5 if year == RECENT_YEARS[-1] else None,
                label=label,
            )

    plt.xticks(range(1, 13), MESES_TICKS)
    plt.xlabel("Meses")
    plt.ylabel("Caudal promedio (m³/s)")
    plt.grid(False)
    plt.legend(loc="upper right")

    if start_year and end_year:
        plt.title(
            f"Caudal histórico {start_year}-{end_year} para {st_name}\n"
            f"Promedio mensual + banda p10-p90 y años recientes ({RECENT_YEARS[0]}-{RECENT_YEARS[-1]})"
        )
    else:
        plt.title(f"Caudal mensual para {st_name}\nAños recientes ({RECENT_YEARS[0]}-{RECENT_YEARS[-1]})")

    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close()


def generar_productos_mensuales_desde_diario(df_diario):
    """
    Actualiza solo la celda del mes actual en el mensualizado completo.
    Mantiene la serie histórica ya existente.
    """
    required_cols = {"nombre_estacion", "latitud", "longitud", "Fecha", "Caudal Diario Promedio"}
    require_cols(df_diario, required_cols)

    df = df_diario.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.dropna(subset=["Fecha"]).copy()

    df["Caudal Diario Promedio"] = pd.to_numeric(df["Caudal Diario Promedio"], errors="coerce")
    df = df.dropna(subset=["Caudal Diario Promedio"]).copy()

    df["Year"] = df["Fecha"].dt.year
    df["Month"] = df["Fecha"].dt.month
    df["Mes"] = df["Month"].map(MONTH_MAP_NUM2TXT)

    if df.empty:
        print("WARNING: No hay datos diarios del mes actual para actualizar.")
        return

    year_actual = int(df["Year"].max())
    mes_actual_num = int(df["Month"].max())
    mes_actual_txt = MONTH_MAP_NUM2TXT[mes_actual_num]

    estaciones = sorted(df["nombre_estacion"].dropna().unique())

    for st in estaciones:
        st_df = df[df["nombre_estacion"] == st].copy()
        if st_df.empty:
            continue

        valor_mes_actual = st_df["Caudal Diario Promedio"].mean()
        out_monthly = OUT_MONTHLY_DIR / f"{safe_name(st)}_mensualizado_corregido.csv"

        if out_monthly.exists():
            table = pd.read_csv(out_monthly, encoding="utf-8")
            table["Year"] = pd.to_numeric(table["Year"], errors="coerce")
            table = table.dropna(subset=["Year"])
            table["Year"] = table["Year"].astype(int)
        else:
            table = pd.DataFrame(columns=["Year"] + MONTH_ORDER)

        for m in MONTH_ORDER:
            if m not in table.columns:
                table[m] = np.nan

        if year_actual not in table["Year"].values:
            nueva_fila = {col: np.nan for col in ["Year"] + MONTH_ORDER}
            nueva_fila["Year"] = year_actual
            table = pd.concat([table, pd.DataFrame([nueva_fila])], ignore_index=True)

        table.loc[table["Year"] == year_actual, mes_actual_txt] = valor_mes_actual
        table = table[["Year"] + MONTH_ORDER].sort_values("Year")
        table.to_csv(out_monthly, index=False, encoding="utf-8")

        out_png = OUT_MONTHLY_PLOTS_DIR / f"{safe_name(st)}_mensual_corregido.png"
        plot_station_monthly(st, table, out_png)


# =========================================================
# 10) PROCESO PRINCIPAL
# =========================================================
def main():
    print("Conectando a la base de datos...")
    df = get_data_horario_mes_actual()

    print(f"OK: Registros del mes actual leidos: {len(df):,}")
    print(f"OK: Estaciones encontradas: {df['nombre_estacion'].nunique()}")

    if df.empty:
        print("WARNING: No hubo datos del último mes consolidado para corregir.")
        return

    all_raw_path = BASE_DIR / "horario_mes_actual_original.csv"
    df.to_csv(all_raw_path, index=False, encoding="utf-8")
    print(f"OK: CSV del mes actual guardado en: {all_raw_path}")

    resumenes = []
    lista_corregidos = []
    estaciones = sorted(df["nombre_estacion"].dropna().unique())

    for i, station in enumerate(estaciones, start=1):
        print(f"Procesando {i}/{len(estaciones)}: {station}")

        df_est = df[df["nombre_estacion"] == station].copy().sort_values("FechaHora")

        df_det = detectar_outliers_locales(df_est, col_fecha="FechaHora", col_q="valor_1h")
        df_cor = corregir_outliers(df_det, col_fecha="FechaHora", col_q="valor_1h")

        guardar_resultados_horarios(df_cor, station)
        resumenes.append(generar_resumen_estacion(df_cor, station))
        lista_corregidos.append(df_cor)

    df_resumen = pd.DataFrame(resumenes).sort_values("nombre_estacion")
    resumen_path = OUT_SUMMARY_DIR / "resumen_outliers_por_estacion.csv"
    df_resumen.to_csv(resumen_path, index=False, encoding="utf-8")
    print(f"OK: Resumen guardado en: {resumen_path}")

    df_horario_corregido = pd.concat(lista_corregidos, ignore_index=True)
    horario_corregido_path = BASE_DIR / "horario_mes_actual_corregido.csv"
    df_horario_corregido.to_csv(horario_corregido_path, index=False, encoding="utf-8")
    print(f"OK: CSV horario corregido del mes actual: {horario_corregido_path}")

    print("📆 Calculando promedios diarios corregidos del mes actual...")
    df_diario = calcular_diario_corregido(df_horario_corregido)
    diario_all_path = guardar_diarios_corregidos(df_diario)
    print(f"OK: Diario corregido consolidado: {diario_all_path}")

    print("Actualizando mensualizados y graficas mensuales corregidas...")
    generar_productos_mensuales_desde_diario(df_diario)

    print(f"OK: Graficas mensuales corregidas en: {OUT_MONTHLY_PLOTS_DIR}")
    print("OK: Proceso finalizado correctamente.")


if __name__ == "__main__":
    main()
