import json
import pandas as pd
import psycopg2
from configparser import ConfigParser
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "TAB_4"
OUTPUT_DIR.mkdir(exist_ok=True)

ANIOS = [2024, 2025, 2026]

ESTACIONES = [
    {"nombre_bd": "Agoyan", "nombre_mostrar": "Agoyan"},
    {"nombre_bd": "Amaluza_Laterales", "nombre_mostrar": "Amaluza Laterales"},
    {"nombre_bd": "Amaluza_Total", "nombre_mostrar": "Amaluza Total"},
    {"nombre_bd": "Coca Codo Sinclair", "nombre_mostrar": "Coca Codo Sinclair"},
    {"nombre_bd": "Daule_Peripa", "nombre_mostrar": "Daule Peripa"},
    {"nombre_bd": "Delsitanisagua", "nombre_mostrar": "Delsitanisagua"},
    {"nombre_bd": "M_S_Francisco", "nombre_mostrar": "M S Francisco"},
    {"nombre_bd": "Mazar", "nombre_mostrar": "Mazar"},
    {"nombre_bd": "Pisayambo", "nombre_mostrar": "Pisayambo"},
]

MESES_ES = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic",
}

def config(filename="config.ini", section="Postgres"):
    cfg_path = BASE_DIR / filename
    parser = ConfigParser()
    read_files = parser.read(cfg_path, encoding="utf-8")
    if not read_files:
        raise FileNotFoundError(f"❌ No se pudo leer el archivo de configuración: {cfg_path}")
    if not parser.has_section(section):
        raise Exception(f"❌ La sección [{section}] no se encuentra en {cfg_path}")
    return {k: v for k, v in parser.items(section)}

def safe_name(texto):
    return (
        str(texto).strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
        .replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U")
        .replace("ñ", "n").replace("Ñ", "N")
    )

def fmt_num(x):
    if pd.isna(x):
        return "Sin dato"
    return f"{float(x):.2f} m³/s"

def etiqueta_fecha(fecha):
    fecha = pd.to_datetime(fecha)
    return f"{fecha.day:02d}-{MESES_ES[fecha.month]}"

def obtener_datos():
    params = config()
    query = """
        SELECT nombre_estacion, fecha_toma_dato, valor_1h
        FROM temporales.caudales
        WHERE fecha_toma_dato >= '2024-01-01'
          AND fecha_toma_dato <  '2027-01-01'
          AND nombre_estacion = ANY(%s)
          AND valor_1h IS NOT NULL
          AND TRIM(valor_1h::text) NOT ILIKE 'nan'
          AND valor_1h > 0
        ORDER BY nombre_estacion, fecha_toma_dato
    """
    with psycopg2.connect(
        host=params["host"],
        port=params["port"],
        user=params["user"],
        password=params["password"],
        database=params["dbname"]
    ) as conn:
        nombres_bd = [estacion["nombre_bd"] for estacion in ESTACIONES]
        df = pd.read_sql(query, conn, params=(nombres_bd,))
    return df

def convertir_a_diario(df):
    if df.empty:
        return pd.DataFrame(
            columns=[
                "nombre_estacion",
                "Fecha",
                "Anio",
                "Mes_Dia",
                "Etiqueta",
                "Caudal Diario Promedio",
            ]
        )

    df = df.copy()
    df["fecha_toma_dato"] = pd.to_datetime(df["fecha_toma_dato"], errors="coerce")
    df["valor_1h"] = pd.to_numeric(df["valor_1h"], errors="coerce")

    df = df.dropna(subset=["nombre_estacion", "fecha_toma_dato", "valor_1h"])

    df["fecha_real"] = df["fecha_toma_dato"] - pd.to_timedelta("30min")
    df["Fecha"] = df["fecha_real"].dt.floor("D")

    diario = (
        df.groupby(["nombre_estacion", "Fecha"], as_index=False)["valor_1h"]
        .mean()
        .rename(columns={"valor_1h": "Caudal Diario Promedio"})
        .sort_values(["nombre_estacion", "Fecha"])
    )

    diario["Anio"] = diario["Fecha"].dt.year
    diario["Mes_Dia"] = diario["Fecha"].dt.strftime("%m-%d")
    diario["Etiqueta"] = diario["Fecha"].apply(etiqueta_fecha)

    # Excluir el día actual porque el promedio diario aún está incompleto.
    # El dashboard debe mostrar datos diarios consolidados hasta ayer.
    hoy_ecuador = pd.Timestamp.now(tz="America/Guayaquil").normalize().tz_localize(None)
    diario = diario[diario["Fecha"] < hoy_ecuador].copy()

    return diario

def resumen_estacion(df_diario, nombre_bd, nombre_mostrar):
    s = df_diario[df_diario["nombre_estacion"] == nombre_bd].copy()
    if s.empty:
        return {
            "estacion": nombre_mostrar,
            "archivo": f"grafico_{safe_name(nombre_mostrar)}.html",
            "ultimo_dia": "Sin dato",
            "caudal_2024": "Sin dato",
            "caudal_2025": "Sin dato",
            "caudal_2026": "Sin dato",
        }

    s_2026 = s[s["Anio"] == 2026]
    ultima_fecha = s_2026["Fecha"].max() if not s_2026.empty else s["Fecha"].max()
    mes_dia = pd.to_datetime(ultima_fecha).strftime("%m-%d")

    out = {
        "estacion": nombre_mostrar,
        "archivo": f"grafico_{safe_name(nombre_mostrar)}.html",
        "ultimo_dia": pd.to_datetime(ultima_fecha).strftime("%Y-%m-%d"),
    }

    for anio in ANIOS:
        dato = s[(s["Anio"] == anio) & (s["Mes_Dia"] == mes_dia)]["Caudal Diario Promedio"]
        out[f"caudal_{anio}"] = fmt_num(dato.iloc[0]) if not dato.empty else "Sin dato"

    return out

def datos_estacion_json(df_diario, nombre_bd):
    s = df_diario[df_diario["nombre_estacion"] == nombre_bd].copy()
    s["Fecha"] = s["Fecha"].dt.strftime("%Y-%m-%d")
    s["Caudal Diario Promedio"] = s["Caudal Diario Promedio"].round(3)
    return s[["Fecha", "Anio", "Mes_Dia", "Etiqueta", "Caudal Diario Promedio"]].to_dict(orient="records")

def fecha_inicio_default(df_est):
    s_2026 = df_est[df_est["Anio"] == 2026].copy()
    if s_2026.empty:
        return "2026-05-01", "2026-05-15"
    fin = pd.to_datetime(s_2026["Fecha"]).max()
    ini = fin - pd.Timedelta(days=30)
    return ini.strftime("%Y-%m-%d"), fin.strftime("%Y-%m-%d")

def generar_grafico_estacion(df_diario, nombre_bd, nombre_mostrar):
    datos = datos_estacion_json(df_diario, nombre_bd)
    df_est = df_diario[df_diario["nombre_estacion"] == nombre_bd].copy()
    default_ini, default_fin = fecha_inicio_default(df_est)
    archivo = OUTPUT_DIR / f"grafico_{safe_name(nombre_mostrar)}.html"
    datos_json = json.dumps(datos, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>TAB_4 - {nombre_mostrar}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css">
    <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
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
        .back {{
            display: inline-block;
            margin-bottom: 20px;
            text-decoration: none;
            background: #0b72d9;
            color: white;
            padding: 10px 16px;
            border-radius: 10px;
            font-weight: 600;
        }}
        .panel {{
            background: white;
            border-radius: 18px;
            padding: 20px;
            box-shadow: 0 8px 18px rgba(0,0,0,0.08);
            margin-bottom: 18px;
        }}
        h1 {{
            color: #0b4b84;
            margin-bottom: 6px;
        }}
        .subtitle {{
            color: #5b6b7c;
            font-size: 17px;
            margin-top: 0;
        }}
        .controls {{
            display: flex;
            gap: 12px;
            align-items: end;
            flex-wrap: wrap;
        }}
        .control {{
            display: flex;
            flex-direction: column;
            gap: 6px;
        }}
        label {{
            font-weight: 700;
            color: #334155;
        }}
        input {{
            padding: 10px;
            border: 1px solid #cbd5e1;
            border-radius: 10px;
            min-width: 170px;
            font-size: 15px;
        }}
        button {{
            border: none;
            border-radius: 10px;
            padding: 11px 16px;
            cursor: pointer;
            font-weight: 700;
            color: white;
            background: #0b72d9;
        }}
        button.secondary {{
            background: #64748b;
        }}
        .note {{
            margin-top: 10px;
            color: #64748b;
            font-size: 14px;
        }}
        #grafico {{
            width: 100%;
            height: 650px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <a class="back" href="index.html">⬅ Volver a TAB_4</a>

        <div class="panel">
            <h1>Comparación de caudal diario - {nombre_mostrar}</h1>
            <p class="subtitle">Comparación del mismo periodo calendario entre 2024, 2025 y 2026</p>

            <div class="controls">
                <div class="control">
                    <label for="fecha-inicial">Fecha inicial</label>
                    <input type="text" id="fecha-inicial" value="{default_ini}">
                </div>
                <div class="control">
                    <label for="fecha-final">Fecha final</label>
                    <input type="text" id="fecha-final" value="{default_fin}">
                </div>
                <button onclick="filtrar()">Actualizar gráfico</button>
                <button class="secondary" onclick="restablecer()">Últimos 30 días</button>
            </div>

            <div class="note">
                Selecciona fechas usando el año 2026 como referencia. El gráfico compara automáticamente ese mismo mes-día en 2024, 2025 y 2026.
            </div>
        </div>

        <div class="panel">
            <div id="grafico"></div>
        </div>
    </div>

<script>
const registros = {datos_json};
const defaultIni = "{default_ini}";
const defaultFin = "{default_fin}";
const colores = {{
    2024: "#d62728",
    2025: "#2ca02c",
    2026: "#1f77b4"
}};

flatpickr("#fecha-inicial", {{ dateFormat: "Y-m-d", defaultDate: defaultIni }});
flatpickr("#fecha-final", {{ dateFormat: "Y-m-d", defaultDate: defaultFin }});

function pad(n) {{
    return String(n).padStart(2, "0");
}}

function rangoMMDD(inicioStr, finStr) {{
    const ini = new Date(inicioStr + "T00:00:00");
    const fin = new Date(finStr + "T00:00:00");
    const lista = [];
    let d = new Date(ini);
    while (d <= fin) {{
        lista.push(pad(d.getMonth() + 1) + "-" + pad(d.getDate()));
        d.setDate(d.getDate() + 1);
    }}
    return lista;
}}

function etiquetaDesdeMMDD(md) {{
    const meses = {{
        "01": "ene", "02": "feb", "03": "mar", "04": "abr", "05": "may", "06": "jun",
        "07": "jul", "08": "ago", "09": "sep", "10": "oct", "11": "nov", "12": "dic"
    }};
    const [m, d] = md.split("-");
    return d + "-" + meses[m];
}}

function filtrar() {{
    const inicio = document.getElementById("fecha-inicial").value;
    const fin = document.getElementById("fecha-final").value;

    if (!inicio || !fin) {{
        alert("Selecciona fecha inicial y fecha final.");
        return;
    }}

    if (new Date(inicio + "T00:00:00") > new Date(fin + "T00:00:00")) {{
        alert("La fecha inicial no puede ser mayor que la fecha final.");
        return;
    }}

    const mdRango = rangoMMDD(inicio, fin);
    const trazas = [];

    [2024, 2025, 2026].forEach(anio => {{
        const serie = mdRango.map((md, idx) => {{
            const r = registros.find(x => Number(x.Anio) === anio && x.Mes_Dia === md);
            return {{
                x: idx + 1,
                etiqueta: etiquetaDesdeMMDD(md),
                fecha: anio + "-" + md,
                y: r ? r["Caudal Diario Promedio"] : null
            }};
        }});

        trazas.push({{
            x: serie.map(r => r.x),
            y: serie.map(r => r.y),
            customdata: serie.map(r => [r.fecha, r.etiqueta]),
            mode: "lines+markers",
            type: "scatter",
            name: String(anio),
            line: {{ color: colores[anio], width: 3, dash: anio === 2026 ? "dash" : "solid" }},
            marker: {{ color: colores[anio], size: 7 }},
            hovertemplate:
                "Año: " + anio + "<br>" +
                "Fecha: %{{customdata[0]}}<br>" +
                "Etiqueta: %{{customdata[1]}}<br>" +
                "Caudal: %{{y:.2f}} m³/s<extra></extra>"
        }});
    }});

    let paso = 1;
    if (mdRango.length > 45 && mdRango.length <= 90) paso = 5;
    else if (mdRango.length > 90 && mdRango.length <= 180) paso = 10;
    else if (mdRango.length > 180) paso = 15;

    let tickvals = [];
    let ticktext = [];
    mdRango.forEach((md, idx) => {{
        if (idx % paso === 0 || idx === mdRango.length - 1) {{
            tickvals.push(idx + 1);
            ticktext.push(etiquetaDesdeMMDD(md));
        }}
    }});

    const layout = {{
        title: "Caudal diario comparado - {nombre_mostrar}<br><sup>Periodo seleccionado: " + inicio + " al " + fin + "</sup>",
        xaxis: {{
            title: "Periodo comparado",
            tickmode: "array",
            tickvals: tickvals,
            ticktext: ticktext,
            tickangle: -45
        }},
        yaxis: {{
            title: "Caudal Diario Promedio (m³/s)",
            tickformat: ".2f"
        }},
        hovermode: "x unified",
        template: "plotly_white",
        height: 650,
        legend: {{ title: {{ text: "Año" }} }},
        margin: {{ l: 70, r: 30, t: 90, b: 90 }}
    }};

    Plotly.newPlot("grafico", trazas, layout, {{responsive: true}});
}}

function restablecer() {{
    document.getElementById("fecha-inicial").value = defaultIni;
    document.getElementById("fecha-final").value = defaultFin;
    filtrar();
}}

window.onload = filtrar;
</script>
</body>
</html>
"""
    archivo.write_text(html, encoding="utf-8")
    print(f"📈 Gráfico generado: {archivo}")

def generar_index(resumenes):
    cards = ""
    for r in resumenes:
        cards += f"""
        <a class="card-link" href="{r['archivo']}">
            <div class="card">
                <h3>{r['estacion']}</h3>
                <p><b>Último día diario:</b> {r['ultimo_dia']}</p>
                <p><b>Caudal 2024:</b> {r['caudal_2024']}</p>
                <p><b>Caudal 2025:</b> {r['caudal_2025']}</p>
                <p><b>Caudal 2026:</b> {r['caudal_2026']}</p>
                <div class="btn-mini">📈 Ver comparación diaria</div>
            </div>
        </a>
        """

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>TAB_4 - Comparación de caudal diario</title>
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
            background: #eaf4ff;
            border-left: 5px solid #0b72d9;
            padding: 14px;
            border-radius: 10px;
            margin-bottom: 24px;
            color: #16456d;
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(270px, 1fr));
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
            <h1>TAB_4 - Comparación de caudal diario (2024, 2025, 2026)</h1>
            <p>Comparación diaria interactiva por hidroeléctrica, con selección de fecha inicial y final</p>
        </div>

        <div class="notice">
            <b>Nota:</b> Las tarjetas muestran el último día diario disponible como referencia y el caudal diario de ese mismo mes-día para 2024, 2025 y 2026. Dentro de cada gráfico se puede seleccionar otro periodo.
        </div>

        <div class="grid">
            {cards}
        </div>
    </div>
</body>
</html>
"""
    index_path = OUTPUT_DIR / "index.html"
    index_path.write_text(html, encoding="utf-8")
    print(f"🏠 Index TAB_4 generado: {index_path}")

def main():
    print("🔄 Consultando datos horarios 2024-2026...")
    df = obtener_datos()

    if df.empty:
        print("⚠️ No se encontraron datos para las estaciones configuradas.")
        return

    print("📊 Calculando caudal diario promedio...")
    df_diario = convertir_a_diario(df)

    resumenes = []
    print("📈 Generando gráficos por hidroeléctrica...")
    for estacion in ESTACIONES:
        nombre_bd = estacion["nombre_bd"]
        nombre_mostrar = estacion["nombre_mostrar"]
        generar_grafico_estacion(df_diario, nombre_bd, nombre_mostrar)
        resumenes.append(resumen_estacion(df_diario, nombre_bd, nombre_mostrar))

    print("🏠 Generando índice interno TAB_4...")
    generar_index(resumenes)

    print("✅ TAB_4 finalizado correctamente.")

if __name__ == "__main__":
    main()
