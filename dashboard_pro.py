# -*- coding: utf-8 -*-
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import mysql.connector
import plotly.express as px
import io
import os
import json
import numpy as np
from datetime import datetime, timedelta

# -----------------------------------------------------------------------------
# 0. CONFIGURACIÓN DEL TEMA
# -----------------------------------------------------------------------------
if not os.path.exists(".streamlit"):
    os.makedirs(".streamlit")

config_path = ".streamlit/config.toml"
config_content = """
[theme]
base="light"
primaryColor="#2B3674"
backgroundColor="#F4F7FE"
secondaryBackgroundColor="#FFFFFF"
textColor="#2B3674"
font="sans serif"
[client]
toolbarMode = "viewer"
"""

write_config = True
if os.path.exists(config_path):
    with open(config_path, "r") as f:
        if f.read().strip() == config_content.strip():
            write_config = False

if write_config:
    with open(config_path, "w") as f:
        f.write(config_content)
    st.rerun()

# -----------------------------------------------------------------------------
# 1. CONFIGURACIÓN DE PÁGINA
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Sniper Pharma",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# =============================================================================
# 🔒 SISTEMA DE SEGURIDAD SNIPER (CON VENCIMIENTO Y ROLES)
# =============================================================================

# BASE DE DATOS DE USUARIOS (Tú controlas esto manualmente aquí)
# Formato: "usuario": {"pass": "clave", "role": "tipo", "expires": "YYYY-MM-DD"}
USERS_DB = {
    # 1. USUARIO DEMO (Para el gancho de WhatsApp - Vence pronto)
    "demo": {
        "pass": "flash2025", 
        "role": "demo", 
        "expires": "2026-01-05" # <--- FECHA DE CORTE (MAÑANA/PASADO)
    },
    # 2. USUARIO CLIENTE (Ejemplo de alguien que pagó)
    "farmacia_vip": {
        "pass": "cliente80k", 
        "role": "pro", 
        "expires": "2026-01-30" 
    },
    # 3. ADMIN (Tú - Acceso total ilimitado)
    "admin": {
        "pass": "sniper2025", 
        "role": "admin", 
        "expires": "2099-12-31" 
    }
}

def check_password():
    """Retorna True si el usuario ingresa credenciales válidas y vigentes."""
    
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    def password_entered():
        entered_user = st.session_state.get("username_input", "").strip()
        entered_pass = st.session_state.get("password_input", "").strip()
        
        if entered_user in USERS_DB:
            user_data = USERS_DB[entered_user]
            if user_data["pass"] == entered_pass:
                # Verificar si la licencia venció
                expire_date = datetime.strptime(user_data["expires"], "%Y-%m-%d")
                if datetime.now() > expire_date:
                    st.error("🚫 SU LICENCIA HA EXPIRADO. Contacte a soporte para renovar.")
                    st.session_state["password_correct"] = False
                else:
                    st.session_state["password_correct"] = True
                    st.session_state["user_role"] = user_data["role"]
                    st.session_state["user_name"] = entered_user
            else:
                st.session_state["password_correct"] = False
                st.error("❌ Clave incorrecta")
        else:
            st.session_state["password_correct"] = False
            st.error("❌ Usuario no encontrado")

    if not st.session_state["password_correct"]:
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        st.markdown("<h2 style='text-align: center; color: #2B3674;'>🦅 Sniper Pharma | Acceso Seguro</h2>", unsafe_allow_html=True)
        
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.text_input("👤 Usuario:", key="username_input")
            st.text_input("🔑 Clave:", type="password", key="password_input", on_change=password_entered)
            st.info("ℹ️ Para solicitar un DEMO, escriba al WhatsApp.")
        return False
    else:
        return True

if not check_password():
    st.stop() 
# =============================================================================


# -----------------------------------------------------------------------------
# ESTILOS CSS (Limpieza Visual)
# -----------------------------------------------------------------------------
st.markdown("""
    <style>
        /* Ocultar elementos de marca de agua si es posible */
        header {visibility: hidden;}
        footer {visibility: hidden;}
        .stDeployButton {display:none;}
        [data-testid="stDecoration"] {display:none;}
        [data-testid="stToolbar"] {display:none;}
        
        /* Ajustes Generales */
        .block-container { padding-top: 1rem; }
        @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700&display=swap');
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# ⚙️ CONEXIÓN INTELIGENTE (HÍBRIDA)
# -----------------------------------------------------------------------------
# Este bloque detecta si estamos en Railway o en Local automáticamente
def get_db_config():
    try:
        # Intento 1: Buscar en st.secrets (Local / Streamlit Cloud)
        if "mysql" in st.secrets:
            return {
                "host": st.secrets["mysql"]["host"],
                "user": st.secrets["mysql"]["user"],
                "password": st.secrets["mysql"]["password"],
                "port": st.secrets["mysql"]["port"],
                "database": "datos"
            }
    except:
        pass
    
    # Intento 2: Buscar en Variables de Entorno (Railway)
    return {
        "host": os.environ.get("mysql_host"),
        "user": os.environ.get("mysql_user"),
        "password": os.environ.get("mysql_password"),
        "port": int(os.environ.get("mysql_port", 4000)),
        "database": "datos"
    }

DB_CONFIG = get_db_config()

def init_connection():
    # Validamos que tengamos datos antes de conectar
    if not DB_CONFIG["host"]:
        st.error("⚠️ Error de Configuración: No se encontraron credenciales de base de datos (Secrets o Variables).")
        st.stop()
    return mysql.connector.connect(**DB_CONFIG)

# -----------------------------------------------------------------------------
# 2. ESTILO VISUAL DEL RESTO DE LA APP
# -----------------------------------------------------------------------------
st.markdown("""
    <style>
        :root { color-scheme: light !important; }
        [data-testid="stAppViewContainer"], .stApp { background-color: #F4F7FE !important; }
        hr { display: none !important; border: none !important; margin: 0 !important; }
        * { font-family: 'Poppins', sans-serif; }
        h1, h2, h3, h4, h5, h6, p, li, label, span, div, .stMarkdown { color: #2B3674; }

        /* WIDGETS */
        li[role="option"] {
            white-space: normal !important;
            height: auto !important;
            min-height: 45px !important;
            padding: 10px !important;
            border-bottom: 1px solid #f0f0f0;
            font-size: 12px !important;
            color: #2B3674 !important;
            background-color: #FFFFFF !important;
            display: flex !important;
            align-items: center !important;
        }
        span[data-baseweb="tag"] {
            background-color: #E0E5F2 !important;
            border: 1px solid #d1d9e6 !important;
        }
        span[data-baseweb="tag"] span {
            color: #000000 !important;
            font-weight: 600 !important;
        }
        div[data-baseweb="select"] > div {
            background-color: #FFFFFF !important;
            border: 1px solid #E0E5F2 !important;
            border-radius: 10px !important;
        }
        div[data-testid="stDataFrame"] { background-color: white !important; border: 1px solid #E0E5F2; border-radius: 10px; }
        div[data-testid="stDataFrame"] div[class*="columnHeader"] { 
            background-color: #2B3674 !important; 
            color: white !important; 
            font-weight: 900 !important;
        }
        
        /* KPIs */
        .kpi-container { display: grid; grid-template-columns: repeat(5, 1fr); gap: 15px; margin-bottom: 20px; }
        .kpi-card { display: flex; flex-direction: column; justify-content: center; align-items: center; height: 140px; border-radius: 20px; text-decoration: none !important; transition: transform 0.2s ease; cursor: pointer; box-shadow: 0 4px 10px rgba(0,0,0,0.1); text-align: center; padding: 10px; border: none; }
        .kpi-card:hover { transform: translateY(-5px); box-shadow: 0 8px 20px rgba(0,0,0,0.2); filter: brightness(1.05); }
        .kpi-title { font-size: 14px; font-weight: 600; margin-bottom: 5px; }
        .kpi-value { font-size: 26px; font-weight: 700; margin-bottom: 5px; line-height: 1.1; }
        .kpi-sub { font-size: 11px; font-weight: 500; opacity: 0.9; }
        .card-total { background: linear-gradient(135deg, #667EEA 0%, #764BA2 100%) !important; } .card-total * { color: white !important; }
        .card-caro { background: linear-gradient(135deg, #FF9A9E 0%, #FECFEF 100%) !important; } .card-caro .kpi-title, .card-caro .kpi-value, .card-caro .kpi-sub { color: #8B0000 !important; }
        .card-subir { background: linear-gradient(135deg, #43E97B 0%, #38F9D7 100%) !important; } .card-subir .kpi-title, .card-subir .kpi-value, .card-subir .kpi-sub { color: #005a3e !important; }
        .card-margen { background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%) !important; } .card-margen * { color: white !important; }
        .card-score { background: linear-gradient(135deg, #3B2667 0%, #BC78EC 100%) !important; } .card-score * { color: white !important; }

        /* BOTONES */
        div:not([data-testid="stForm"]) > div.stButton > button[kind="primary"] { background-color: #2B3674 !important; border: none !important; border-radius: 12px !important; height: 35px !important; width: 100% !important; box-shadow: 0 4px 6px rgba(0,0,0,0.1) !important; }
        div:not([data-testid="stForm"]) > div.stButton > button[kind="primary"] p { color: #FFFFFF !important; font-weight: 600 !important; font-size: 14px !important; }
        div.stDownloadButton > button { background: linear-gradient(135deg, #34a853 0%, #2e8b57 100%) !important; border: none !important; border-radius: 12px !important; height: 35px !important; width: 100% !important; box-shadow: 0 4px 6px rgba(0,0,0,0.1) !important; }
        div.stDownloadButton > button p { color: #FFFFFF !important; font-size: 14px !important; font-weight: 600 !important; }
        [data-testid="stForm"] button { background-color: #00C853 !important; border: 1px solid #009624 !important; border-radius: 12px !important; box-shadow: 0 4px 6px rgba(0,0,0,0.1) !important; }
        [data-testid="stForm"] button:hover { background-color: #009624 !important; transform: translateY(-2px); }
        [data-testid="stForm"] button p { color: #FFFFFF !important; font-weight: 700 !important; }
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 3. MOTOR DE DATOS
# -----------------------------------------------------------------------------
@st.cache_data(ttl=300)
def get_main_data():
    try:
        conn = init_connection()
        query = """
            SELECT id, NOMBRE, PROVEEDOR, CATEG2, Cost_prom_copifam, P_COPIFAMP,
            P_CRUZ_VERDE_Calculado, P_DROGUERIA_ALEMANA_Calculado,
            P_COLSUBSIDIO_Calculado, P_LAREBAJA_Calculado
            FROM productos_master WHERE vip_monitoreo = 1
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error cargando master: {e}")
        return pd.DataFrame()

def get_history_data(product_id):
    try:
        conn = init_connection()
        query = """
            SELECT fecha_actualizacion, drogueria_nombre, precio_oferta 
            FROM precios_historicos 
            WHERE producto_id = %s 
            ORDER BY fecha_actualizacion ASC
        """
        df = pd.read_sql(query, conn, params=(int(product_id),))
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error SQL: {e}")
        return pd.DataFrame()

def get_filtered_global_history(start_date, end_date):
    try:
        conn = init_connection()
        s_date = start_date.strftime('%Y-%m-%d 00:00:00')
        e_date = end_date.strftime('%Y-%m-%d 23:59:59')
        
        query = """
            SELECT p.id as COD_PRODUCTO, p.NOMBRE, h.drogueria_nombre, h.precio_oferta, h.fecha_actualizacion
            FROM precios_historicos h
            LEFT JOIN productos_master p ON h.producto_id = p.id
            WHERE h.fecha_actualizacion BETWEEN %s AND %s
            ORDER BY h.fecha_actualizacion DESC
        """
        df = pd.read_sql(query, conn, params=(s_date, e_date))
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error descargando historial global: {e}")
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# 4. LÓGICA DE NEGOCIO
# -----------------------------------------------------------------------------
def process_data(df):
    if df.empty: return df
    
    competidores = ['P_CRUZ_VERDE_Calculado', 'P_DROGUERIA_ALEMANA_Calculado', 'P_COLSUBSIDIO_Calculado', 'P_LAREBAJA_Calculado']
    cols_numericas = competidores + ['P_COPIFAMP', 'Cost_prom_copifam']

    for col in cols_numericas:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['Mercado_Min'] = df[competidores].min(axis=1) 
    df['GAP_Dinero'] = df['P_COPIFAMP'] - df['Mercado_Min']
    
    df['Dif_Porcentaje'] = df.apply(
        lambda row: ((row['P_COPIFAMP'] - row['Mercado_Min']) / row['Mercado_Min'] * 100) 
        if pd.notnull(row['Mercado_Min']) and row['Mercado_Min'] != 0 else 0, 
        axis=1
    )
    
    df['Alerta_Margen'] = (df['P_COPIFAMP'] - df['Cost_prom_copifam']) < 0
    return df

def convert_df_to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Datos')
    processed_data = output.getvalue()
    return processed_data

def color_dif_styler(val):
    if pd.isna(val): return ''
    color = '#D93025' if val > 0 else '#28B463' if val < 0 else '#2B3674'
    return f'color: {color}; font-weight: 800;'

# -----------------------------------------------------------------------------
# FUNCIONES GRAFICAS
# -----------------------------------------------------------------------------
def render_html_bars(df_row):
    competitors = {
        'SU EMPRESA': {'price': df_row['P_COPIFAMP'], 'color': 'linear-gradient(90deg, #2B3674 0%, #4A5699 100%)', 'label': 'SU EMPRESA'},
        'Droguería Alemana': {'price': df_row['P_DROGUERIA_ALEMANA_Calculado'], 'color': 'linear-gradient(90deg, #FF416C 0%, #FF4B2B 100%)', 'label': 'Droguería Alemana'},
        'La Rebaja': {'price': df_row['P_LAREBAJA_Calculado'], 'color': 'linear-gradient(90deg, #FFC312 0%, #F79F1F 100%)', 'label': 'Droguería La Rebaja'},
        'Colsubsidio': {'price': df_row['P_COLSUBSIDIO_Calculado'], 'color': 'linear-gradient(90deg, #8E2DE2 0%, #4A00E0 100%)', 'label': 'Colsubsidio'},
        'Cruz Verde': {'price': df_row['P_CRUZ_VERDE_Calculado'], 'color': 'linear-gradient(90deg, #11998e 0%, #38ef7d 100%)', 'label': 'Cruz Verde'}
    }
    
    clean_comps = {k: v for k, v in competitors.items() if pd.notnull(v['price']) and v['price'] > 0}
    if not clean_comps: return "No data"
    
    max_val = max(c['price'] for c in clean_comps.values())
    sorted_comps = sorted(clean_comps.items(), key=lambda item: item[1]['price'], reverse=True)
    
    css_anim = """<style>.bar-row{margin-bottom:12px;display:flex;align-items:center;transition:transform 0.3s cubic-bezier(0.175,0.885,0.32,1.275);cursor:default;}.bar-row:hover{transform:scale(1.02);}.bar-bg{flex-grow:1;background-color:#F4F7FE;border-radius:12px;height:32px;box-shadow:inset 0 2px 4px rgba(0,0,0,0.03);overflow:hidden;}.bar-fill{height:100%;border-radius:12px;display:flex;align-items:center;justify-content:flex-end;padding-right:15px;box-shadow:4px 0 8px rgba(0,0,0,0.15);animation:expandBar 1s ease-out forwards;min-width:40px;}.bar-label{width:140px;font-size:11px;font-weight:600;color:#707EAE;text-align:right;padding-right:15px;line-height:1.2;}.bar-row:hover .bar-label{color:#2B3674;}.bar-price{font-weight:800;font-size:12px;text-shadow:0px 1px 2px rgba(0,0,0,0.2);}@keyframes expandBar{from{width:0%;opacity:0;}to{opacity:1;}}</style>"""
    html = css_anim + '<div style="width:100%; font-family:Poppins; padding:15px; background:white; border-radius:15px; box-shadow: 0 4px 10px rgba(0,0,0,0.05);border:1px solid #E0E5F2;"><div style="color:#2B3674;font-weight:700;font-size:15px;margin-bottom:15px;text-align:center;">📊 Competencia de Precios Actual</div>'
    
    for i, (k, v) in enumerate(sorted_comps):
        width_pct = (v['price'] / max_val) * 100
        price_fmt = f"${v['price']:,.0f}".replace(",", ".")
        text_color = 'white'
        if 'Rebaja' in k: text_color = '#2B3674'
        anim_style = f"width: {width_pct}%; background: {v['color']}; animation-delay: {i*0.1}s;"
        bar_html = f'<div class="bar-row"><div class="bar-label">{v["label"]}</div><div class="bar-bg"><div class="bar-fill" style="{anim_style}"><span class="bar-price" style="color: {text_color};">{price_fmt}</span></div></div></div>'
        html += bar_html
    html += '</div>'
    return html

def render_modern_line_chart(hist_df, current_price):
    colors = { 'SU EMPRESA': '#2B3674', 'La Rebaja': '#F79F1F', 'Cruz Verde': '#38ef7d', 'Drogueria Alemana': '#FF4B2B', 'Colsubsidio': '#8E2DE2' }
    
    series_data = []
    unique_vendors = hist_df['drogueria_nombre'].unique()
    hist_df = hist_df.sort_values(by='fecha')
    
    all_dates = sorted(hist_df['fecha'].unique())
    my_data_points = []
    
    if all_dates:
        for d in all_dates:
            ts = int(d.timestamp() * 1000)
            # FIX: Convertir explícitamente a float para evitar error int64 de numpy
            val = float(current_price) if current_price else 0.0
            my_data_points.append([ts, val])
    else:
        ts = int(pd.Timestamp.now().timestamp() * 1000)
        val = float(current_price) if current_price else 0.0
        my_data_points.append([ts, val])

    series_data.append({"name": "SU EMPRESA", "data": my_data_points})
    colors_array = [colors['SU EMPRESA']]

    for vendor in unique_vendors:
        if "SU EMPRESA" in vendor.upper(): continue
        vendor_data = hist_df[hist_df['drogueria_nombre'] == vendor]
        data_points = []
        for _, row in vendor_data.iterrows():
            if pd.notnull(row['fecha']) and pd.notnull(row['precio']):
                ts = int(row['fecha'].timestamp() * 1000)
                # FIX: Convertir explícitamente a float
                price_val = float(row['precio'])
                data_points.append([ts, price_val])
        
        if data_points:
            series_data.append({"name": vendor, "data": data_points})
            c = '#707EAE'
            for key, val in colors.items():
                if key.lower() in vendor.lower():
                    c = val
                    break
            colors_array.append(c)

    series_json = json.dumps(series_data)
    colors_json = json.dumps(colors_array)

    html_code = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
      <style>
        body {{ margin: 0; padding: 0; font-family: 'Poppins', sans-serif; }}
        #chart {{ background: white; border-radius: 15px; padding: 15px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); border: 1px solid #E0E5F2; }}
        .chart-title {{ color: #2B3674; font-weight: 700; font-size: 15px; margin-bottom: 10px; text-align: center; }}
      </style>
    </head>
    <body>
      <div id="chart"><div class="chart-title">📈 Tendencia Histórica</div></div>
      <script>
        var options = {{
          series: {series_json},
          chart: {{ type: 'area', height: 280, fontFamily: 'Poppins, sans-serif', toolbar: {{ show: false }}, animations: {{ enabled: true, easing: 'easeinout', speed: 1000 }} }},
          colors: {colors_json},
          dataLabels: {{ enabled: false }},
          stroke: {{ curve: 'smooth', width: 2 }},
          fill: {{ type: 'gradient', gradient: {{ shadeIntensity: 1, opacityFrom: 0.4, opacityTo: 0.05, stops: [0, 90, 100] }} }},
          xaxis: {{ type: 'datetime', tooltip: {{ enabled: false }}, axisBorder: {{ show: false }}, axisTicks: {{ show: false }}, labels: {{ style: {{ colors: '#A3AED0', fontSize: '10px' }} }} }},
          yaxis: {{ labels: {{ style: {{ colors: '#2B3674', fontWeight: 600, fontSize: '10px' }}, 
              formatter: function (value) {{ return "$" + new Intl.NumberFormat('es-CO').format(value); }} 
          }} }},
          grid: {{ borderColor: '#F4F7FE', strokeDashArray: 4 }},
          tooltip: {{ theme: 'light', y: {{ formatter: function (val) {{ return "$" + new Intl.NumberFormat('es-CO').format(val); }} }} }},
          legend: {{ position: 'top', horizontalAlign: 'center', fontFamily: 'Poppins', fontSize: '10px', markers: {{ radius: 12 }} }}
        }};
        var chart = new ApexCharts(document.querySelector("#chart"), options);
        chart.render();
      </script>
    </body>
    </html>
    """
    components.html(html_code, height=330)

# -----------------------------------------------------------------------------
# 5. DASHBOARD PRINCIPAL
# -----------------------------------------------------------------------------
def main():
    query_params = st.query_params
    filtro_actual = query_params.get("kpi", "TODOS")

    df_raw = get_main_data()
    df = process_data(df_raw)

    # -------------------------------------------------------------
    # 🔒 LÓGICA DE CENSURA (MODO DEMO)
    # -------------------------------------------------------------
    user_role = st.session_state.get("user_role", "demo")
    
    if user_role == "demo":
        # Mensaje de Venta Constante
        st.warning("👀 ESTÁS EN MODO DEMO: Viendo precios reales de mercado (Cruz Verde, Rebaja, etc). Tus costos y márgenes aparecen en $0. 🔓 Para activar tu rentabilidad, adquiere la licencia PRO.")
        
        # Enmascaramos TUS datos (Visualmente se vuelven 0)
        # NOTA: No borramos nada de la BD, solo lo ocultamos en esta sesión.
        if not df.empty:
            df['P_COPIFAMP'] = 0        # Tu Precio -> 0
            df['Cost_prom_copifam'] = 0 # Tu Costo -> 0
            df['GAP_Dinero'] = 0        # Ganancia -> 0
            df['Dif_Porcentaje'] = 0    # Diferencia % -> 0
            df['Alerta_Margen'] = False

    if df.empty:
        st.error("❌ Sin datos. Revisa la base de datos.")
        st.stop()

    # --- HEADER ---
    st.markdown("""
        <div style='display: flex; flex-direction: column; justify-content: center; height: 100%; margin-bottom: 10px;'>
            <h3 style='margin:0; font-weight:800; color:#2B3674; line-height: 1.2;'>🦅 MONITOR DE PRECIOS</h3>
            <p style='margin:0; font-size:13px; color:#707EAE; line-height: 1.3;'>
            Inteligencia de precios para toma de decisiones estratégicas.
            </p>
        </div>
    """, unsafe_allow_html=True)

    # --- KPIS ---
    n_total = len(df)
    df_altos = df[df['GAP_Dinero'] > 0]
    n_altos = len(df_altos)
    df_baratos = df[df['GAP_Dinero'] < 0]
    plata = abs(df_baratos['GAP_Dinero'].sum()) if not df_baratos.empty else 0
    n_margen = len(df[df['Alerta_Margen']])
    score = int(((n_total - n_altos) / n_total) * 100) if n_total > 0 else 0

    html_kpis = f"""
    <div class="kpi-container">
        <a class="kpi-card card-total" href="?kpi=TODOS" target="_self">
            <div class="kpi-title">TOTAL PRODUCTOS</div><div class="kpi-value">{n_total}</div><div class="kpi-sub">Catálogo VIP</div>
        </a>
        <a class="kpi-card card-caro" href="?kpi=ALTOS" target="_self">
            <div class="kpi-title">SOBREPRECIO</div><div class="kpi-value">{n_altos}</div><div class="kpi-sub">Ajustar a Mercado</div>
        </a>
        <a class="kpi-card card-subir" href="?kpi=BARATOS" target="_self">
            <div class="kpi-title">OPORTUNIDAD</div><div class="kpi-value">${plata:,.0f}</div><div class="kpi-sub">Mejorar Rentabilidad</div>
        </a>
        <a class="kpi-card card-margen" href="?kpi=MARGEN" target="_self">
            <div class="kpi-title">MARGEN NEGATIVO</div><div class="kpi-value">{n_margen}</div><div class="kpi-sub">Costo > Precio</div>
        </a>
        <a class="kpi-card card-score" href="?kpi=SCORE" target="_self">
            <div class="kpi-title">COMPETITIVIDAD</div><div class="kpi-value">{score}/100</div><div class="kpi-sub">Puntaje Global</div>
        </a>
    </div>
    """
    st.markdown(html_kpis, unsafe_allow_html=True)

    # --- FILTRADO ---
    if filtro_actual == 'ALTOS': df_view = df_altos
    elif filtro_actual == 'BARATOS': df_view = df_baratos
    elif filtro_actual == 'MARGEN': df_view = df[df['Alerta_Margen']]
    elif filtro_actual == 'SCORE': df_view = df[df['GAP_Dinero'] <= 0]
    else: df_view = df

    # --- ORDENAMIENTO ESTRATÉGICO POR DEFECTO (ATAQUE) ---
    if not df_view.empty and 'Dif_Porcentaje' in df_view.columns:
         df_view = df_view.sort_values(by='Dif_Porcentaje', ascending=False)

    # FILTROS
    f1, f2, f3, f4 = st.columns([0.5, 2.2, 1.4, 1.25])
    with f1:
        st.markdown("<div style='height: 45px; display: flex; align-items: center;'><p style='font-weight:bold; font-size:18px; margin: 0; color: #2B3674;'>🔍 Filtros:</p></div>", unsafe_allow_html=True)
    with f2:
        product_options = sorted(df_view['NOMBRE'].dropna().unique())
        productos_sel = st.multiselect("Producto", product_options, label_visibility="collapsed", placeholder="Producto...")
        if productos_sel: df_view = df_view[df_view['NOMBRE'].isin(productos_sel)]
    with f3:
        provs_options = sorted(df_view['PROVEEDOR'].dropna().unique())
        provs = st.multiselect("Proveedor", provs_options, label_visibility="collapsed", placeholder="Proveedor...")
        if provs: df_view = df_view[df_view['PROVEEDOR'].isin(provs)]
    with f4:
        if 'CATEG2' in df_view.columns:
            cats_options = sorted(df_view['CATEG2'].dropna().unique())
            cats = st.multiselect("Categoría", cats_options, label_visibility="collapsed", placeholder="Categoría...")
            if cats: df_view = df_view[df_view['CATEG2'].isin(cats)]
    
    # ACCIONES
    st.markdown("<div style='margin-bottom: 2px;'></div>", unsafe_allow_html=True)
    b1, b2, b3 = st.columns([1.5, 1.5, 5])
    with b1:
        if st.button("🔄 Actualizar", type="primary", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    with b2:
        excel_data = convert_df_to_excel(df_view)
        st.download_button(label="⇩ Descargar Excel", data=excel_data, file_name='reporte_precios.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', use_container_width=True)

    # --- TABLA INTERACTIVA ---
    if not df_view.empty:
        # ORDEN LÓGICO: Producto -> Costo -> Mi Precio -> DIF % -> Competencia
        cols_base = ['NOMBRE', 'Cost_prom_copifam', 'P_COPIFAMP', 'Dif_Porcentaje']
        cols_competencia = ['P_DROGUERIA_ALEMANA_Calculado', 'P_CRUZ_VERDE_Calculado', 'P_LAREBAJA_Calculado', 'P_COLSUBSIDIO_Calculado']
        
        df_table = df_view[cols_base + cols_competencia].copy()
        
        # Renombrar para visualización
        df_table.columns = ['PRODUCTO', 'COSTO', 'MI PRECIO', 'DIF %', 'ALEMANA', 'CRUZ VERDE', 'LA REBAJA', 'COLSUBSIDIO']
        
        # --- FORMATEADOR PROFESIONAL ("-") ---
        def formatear_profesional(valor):
            s_val = str(valor).strip().lower()
            if pd.isna(valor) or s_val in ['none', 'nan', '', 'null', 'nat']:
                return "-" 
            try:
                num = float(valor)
                if num == 0: return "-" # Ceros también como guiones
                return f"${num:,.0f}".replace(",", ".")
            except:
                return "-"

        cols_a_formatear = ['COSTO', 'MI PRECIO', 'ALEMANA', 'CRUZ VERDE', 'LA REBAJA', 'COLSUBSIDIO']
        for col in cols_a_formatear:
            df_table[col] = df_table[col].apply(formatear_profesional)

        # --- ESTILO ---
        styler = df_table.style.set_properties(**{
            'background-color': '#FFFFFF',
            'color': '#2B3674',
            'border-color': '#E0E5F2',
            'font-size': '13px',
            'font-weight': '600'
        })
        
        styler.map(color_dif_styler, subset=['DIF %'])
        styler.format({"DIF %": "{:.1f}%"})
        
        # --- CONFIGURACIÓN DE COLUMNAS ---
        column_cfg = {
            "PRODUCTO": st.column_config.TextColumn("Producto", width="large", help="Nombre comercial."),
            "COSTO": st.column_config.TextColumn("Costo"),
            "MI PRECIO": st.column_config.TextColumn("Mi Precio"),
            "DIF %": st.column_config.TextColumn("Dif %", help="Diferencia vs Mercado"),
            "ALEMANA": st.column_config.TextColumn("Alemana"),
            "CRUZ VERDE": st.column_config.TextColumn("Cruz Verde"),
            "LA REBAJA": st.column_config.TextColumn("La Rebaja"),
            "COLSUBSIDIO": st.column_config.TextColumn("Colsubsidio"),
        }

        st.dataframe(styler, use_container_width=True, hide_index=True, column_config=column_cfg, height=500)

        # --- AQUÍ ESTÁ LA SOLUCIÓN "MANAGER-PROOF" ---
        st.markdown("""
        <div style="background-color: #fff3cd; border: 1px solid #ffeeba; color: #856404; padding: 10px; border-radius: 5px; font-size: 12px; margin-top: 10px;">
            <strong>⚠️ NOTA DEL SISTEMA:</strong> Los valores marcados con un guion <strong>(-)</strong> indican que el producto no está disponible o no fue encontrado en el catálogo del competidor en la fecha de consulta.
        </div>
        """, unsafe_allow_html=True)

    # --- SECCIÓN DETALLE ---
    st.markdown("<div style='margin-top: -60px;'></div>", unsafe_allow_html=True)
    
    st.markdown("<h3 style='color: #2B3674; text-align: left; margin-top: 0px; margin-bottom: 10px;'>🔎 Historia de Precios por producto</h3>", unsafe_allow_html=True)
    
    opciones = df_view.sort_values(by='GAP_Dinero', ascending=False)['NOMBRE'].unique()
    if len(opciones) > 0:
         sku = st.selectbox("Selecciona un producto para auditar:", opciones, label_visibility="collapsed")
    else:
         sku = None
         st.info("No hay productos disponibles.")

    if sku:
        c_title, c_btn = st.columns([3, 1], gap="small")
        with c_title:
             st.markdown(f"<h5 style='color: #707EAE; margin-top: 5px; margin-bottom: 0px;'>Analizando: <span style='color:#2B3674; font-weight:bold; font-size: 18px;'>{sku}</span></h5>", unsafe_allow_html=True)
        
        row = df_raw[df_raw['NOMBRE'] == sku].iloc[0]
        id_producto = row['id']
        hist = get_history_data(id_producto)
        
        with c_btn:
             if not hist.empty:
                 hist_export = hist.copy()
                 try:
                     hist_export['precio'] = pd.to_numeric(hist_export['precio_oferta'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
                     del hist_export['precio_oferta']
                 except: pass
                 excel_hist = convert_df_to_excel(hist_export)
                 st.download_button("⇩ Descargar Historial", data=excel_hist, file_name=f'historial_{id_producto}.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', use_container_width=True)

        st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)
        g1, g2 = st.columns([1, 1.5], gap="medium")
        
        with g1:
            html_chart = render_html_bars(row)
            st.markdown(html_chart, unsafe_allow_html=True)
        with g2:
            if not hist.empty:
                hist['precio_oferta'] = hist['precio_oferta'].astype(str).str.replace(r'[$,]', '', regex=True)
                hist['precio'] = pd.to_numeric(hist['precio_oferta'], errors='coerce')
                hist = hist.dropna(subset=['precio'])
                try:
                    hist['fecha'] = pd.to_datetime(hist['fecha_actualizacion'])
                    render_modern_line_chart(hist, row['P_COPIFAMP'])
                except Exception as e:
                    st.error(f"Error procesando fechas: {e}")
            else:
                st.info(f"Sin historial para ID {id_producto}.")

    # --- SECCIÓN: DESCARGA MASIVA FILTRADA ---
    st.markdown("---")
    st.markdown("<h5 style='color: #2B3674;'>📂 Exportar la historia de todos los productos</h5>", unsafe_allow_html=True)
    
    with st.container():
        with st.form("descarga_form"):
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1:
                fecha_ini = st.date_input("Fecha Inicio", value=datetime.now() - timedelta(days=30))
            with c2:
                fecha_fin = st.date_input("Fecha Fin", value=datetime.now())
            with c3:
                st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)
                submitted = st.form_submit_button("Generar Reporte", type="primary", use_container_width=True)
        
        if submitted:
            with st.spinner("Generando reporte masivo..."):
                global_hist_df = get_filtered_global_history(fecha_ini, fecha_fin)
                if not global_hist_df.empty:
                    st.success(f"✅ ¡Reporte generado! {len(global_hist_df)} registros encontrados.")
                    excel_global = convert_df_to_excel(global_hist_df)
                    st.download_button(
                        label="📥 Descargar BD Histórica Filtrada",
                        data=excel_global,
                        file_name=f'Historico_Global_{fecha_ini}_{fecha_fin}.xlsx',
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        key='btn_global_dl'
                    )
                else:
                    st.warning("⚠️ No se encontraron datos en el rango de fechas seleccionado.")

if __name__ == "__main__":
    main()


