## IMPORTACION DE BIBLIOTECAS
import pandas as pd
import numpy as np
import requests
import streamlit as st
import time
from datetime import datetime, date
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, Float, Integer, Text, text
from sqlalchemy.pool import NullPool

# ==========================================
# 1. CONFIGURACIÓN API IOL (SECRETS)
# ==========================================
try:
    USUARIO_IOL = st.secrets["iol"]["usuario"]
    PASSWORD_IOL = st.secrets["iol"]["password"]
except Exception:
    st.error("❌ Error: No se encontraron los Secrets 'iol' configurados.")
    st.stop()

# --- FUNCIONES DE APOYO ---
def obtener_headers():
    url_token = "https://api.invertironline.com/token"
    data_token = {'username': USUARIO_IOL, 'password': PASSWORD_IOL, 'grant_type': 'password'}
    try:
        response = requests.post(url_token, data=data_token)
        response.raise_for_status()
        return {'Authorization': f"Bearer {response.json().get('access_token')}"}
    except:
        return None

def obtener_valores_dolar():
    api_url = "https://dolarapi.com/v1/dolares"
    try:
        response = requests.get(api_url)
        data = response.json()
        df_dolar_api = pd.DataFrame(data)
        valor_oficial = df_dolar_api.loc[df_dolar_api['casa'] == 'oficial', 'venta'].values[0]
        valor_mep = df_dolar_api.loc[df_dolar_api['casa'] == 'bolsa', 'venta'].values[0]
        return valor_oficial, valor_mep
    except:
        return None, None

## FUNCIÓN PRINCIPAL DE PROCESAMIENTO
def procesar_y_guardar_en_sql(archivo_subido, db_host, db_name, db_user, db_pass):
    try:
        barra_progreso = st.progress(0, text="Iniciando procesamiento...")
        
        # 1. IMPORTACION DESDE STREAMLIT (reemplaza /content/)
        df = pd.read_excel(archivo_subido, sheet_name="resultados_por_lotes_finales")
        
        # 2. RENOMBRAR Y LIMPIAR
        df.rename(columns = {"Cantidad": "cantidad", "Descripcion": "descripcion", "Fecha": "fecha", "Fecha Lote": "fecha_descarga", "Gastos": "gastos", "Moneda": "moneda", "Operacion": "operacion", "Precio Compra": "precio_compra", "Ticker": "ticker", "Tipo": "tipo", "DolarCCL": "dolar_ccl", "DolarMEP": "dolar_mep", "DolarOficial": "dolar_oficial"}, inplace = True)
        df.drop(["dolar_ccl", "operacion"], axis=1, inplace=True, errors='ignore')

        # 3. FILTROS Y COSTOS (CEDEARs y ON)
        df_cedears = df[df.tipo == "Cedears"].copy()
        df_on = df[df.tipo == "Corporativos - Dólar"].copy()
        
        for dff in [df_cedears, df_on]:
            dff.fecha = pd.to_datetime(dff.fecha)
            dff.fecha_descarga = pd.to_datetime(dff.fecha_descarga)
            dff["costo_ars"] = (dff.cantidad * dff.precio_compra) + dff.gastos

        # 4. COTIZACIONES IOL
        headers = obtener_headers()
        if not headers: return False, "Error de autenticación en IOL."
        
        cotizaciones_raw = {}
        rutas = [
            ("CEDEARS", "https://api.invertironline.com/api/v2/Cotizaciones/Cedears/Argentina/Todos"),
            ("TIT_PUB", "https://api.invertironline.com/api/v2/Cotizaciones/TitulosPublicos/Argentina/Todos"),
            ("ONS_ESP", "https://api.invertironline.com/api/v2/Cotizaciones/ObligacionesNegociables/Argentina/Todos")
        ]
        for _, url in rutas:
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                for t in res.json().get('titulos', []):
                    cotizaciones_raw[t['simbolo'].strip()] = t['ultimoPrecio']

        # 5. NORMALIZACIÓN (Cedears 1:1, ON / 100)
        cotizaciones_actuales = {}
        for t in df_cedears.ticker.unique():
            if t in cotizaciones_raw: cotizaciones_actuales[t] = cotizaciones_raw[t]
        for t in df_on.ticker.unique():
            if t in cotizaciones_raw: cotizaciones_actuales[t] = cotizaciones_raw[t] / 100

        # 6. CÁLCULOS DE TENENCIA Y RENDIMIENTO
        dolar_oficial, dolar_mep = obtener_valores_dolar()
        min_dolar = np.minimum(dolar_oficial, dolar_mep)

        # Cálculo Cedears
        df_cedears["tenencia_ars"] = (df_cedears.cantidad * df_cedears.ticker.map(cotizaciones_actuales)) * (1-0.006)
        df_cedears["tenencia_usd"] = df_cedears.tenencia_ars / min_dolar
        df_cedears["costo_usd"] = np.where(df_cedears.fecha < pd.to_datetime("2025-04-15"), df_cedears.costo_ars / df_cedears.dolar_mep, df_cedears.costo_ars / np.minimum(df_cedears.dolar_oficial, df_cedears.dolar_mep))
        
        # Procesamiento Histórico Cedears
        df_ced_agrup = df_cedears.groupby("ticker").sum(numeric_only=True).reset_index()
        df_ced_agrup['fecha_ejecucion'] = date.today()
        df_final_listo = pd.wide_to_long(df_ced_agrup, stubnames=['costo', 'tenencia'], i=['ticker', 'fecha_ejecucion'], j='moneda', sep='_', suffix='(ars|usd)').reset_index()

        # Cálculo ON
        df_on["tenencia_ars"] = (df_on.cantidad * df_on.ticker.map(cotizaciones_actuales)) * (1-0.006)
        df_on["tenencia_usd"] = df_on.tenencia_ars / min_dolar
        df_on["costo_usd"] = np.where(df_on.fecha < pd.to_datetime("2025-04-15"), df_on.costo_ars / df_on.dolar_mep, df_on.costo_ars / np.minimum(df_on.dolar_oficial, df_on.dolar_mep))
        
        # Procesamiento Histórico ON
        df_on_agrup = df_on.groupby("ticker").sum(numeric_only=True).reset_index()
        df_on_agrup['fecha_ejecucion'] = date.today()
        df_on_final_listo = pd.wide_to_long(df_on_agrup, stubnames=['costo', 'tenencia'], i=['ticker', 'fecha_ejecucion'], j='moneda', sep='_', suffix='(ars|usd)').reset_index()

        # 7. CÁLCULO CUPONES (Sheet: resultados_por_realizado)
        df_c = pd.read_excel(archivo_subido, sheet_name="resultados_por_realizado")
        df_c_filt = df_c[(df_c["Tipo Movimiento"].isin(["Cupón", "Dividendo"])) & (df_c["Tipo"].isin(["Corporativos - Dólar", "Cedears"])) & (df_c["Moneda Venta"].isin(["Dólares C.V. 7000", "Dólares"]))]
        df_c_filt["CuponUSD"] = np.where(df_c_filt["Tipo"] == "Corporativos - Dólar", (df_c_filt["Cupones"].fillna(0) - df_c_filt["Gastos"].fillna(0)) / df_c_filt["OperacionVentaDolarOficial"], df_c_filt["Dividendos"].fillna(0))
        df_cupones_final = df_c_filt.groupby("Ticker")[["CuponUSD"]].sum().reset_index()

        # 8. CARGA A SUPABASE
        barra_progreso.progress(0.85, text="Guardando en Base de Datos...")
        conn_url = f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:5432/{db_name}?sslmode=require'
        engine = create_engine(conn_url, poolclass=NullPool)

        with engine.begin() as conn:
            # Fotos actuales
            df_cedears.to_sql('cedears', conn, if_exists='replace', index=False)
            df_on.to_sql('on_movimientos', conn, if_exists='replace', index=False)
            df_cupones_final.to_sql('dividendos_cupones_realizados', conn, if_exists='replace', index=False)
            
            # Históricos (Append)
            df_final_listo.to_sql('datos_historicos_cedears', conn, if_exists='append', index=False)
            df_on_final_listo.to_sql('datos_historicos_on', conn, if_exists='append', index=False)

            # Histórico Dólar
            if dolar_oficial and dolar_mep:
                df_h_dolar = pd.DataFrame([{'fecha': date.today(), 'tipo': 'Oficial', 'valor': dolar_oficial}, {'fecha': date.today(), 'tipo': 'MEP', 'valor': dolar_mep}])
                try: df_h_dolar.to_sql('historico_dolar', conn, if_exists='append', index=False)
                except: pass

        barra_progreso.progress(1.0, text="¡Completado!")
        return True, "✅ Datos procesados y cargados exitosamente."

    except Exception as e:
        return False, f"❌ Error: {e}"

# --- INTERFAZ STREAMLIT ---
st.set_page_config(layout="centered", page_title="Análisis Inversiones")
st.title("📊 Análisis de Inversiones (Contable)")

with st.form("upload_form"):
    uploaded_file = st.file_uploader("Sube tu archivo de Balanz", type=["xlsx"])
    st.divider()
    st.subheader("Credenciales de Supabase")
    col1, col2 = st.columns(2)
    with col1:
        db_host = st.text_input("Host")
        db_user = st.text_input("User")
    with col2:
        db_name = st.text_input("Database Name", "postgres")
        db_pass = st.text_input("Contraseña", type="password")
    
    submit = st.form_submit_button("🚀 Procesar y Cargar Datos", use_container_width=True)

if submit:
    if uploaded_file and db_host and db_pass:
        with st.spinner("Procesando..."):
            exito, mensaje = procesar_y_guardar_en_sql(uploaded_file, db_host, db_name, db_user, db_pass)
            if exito: st.success(mensaje)
            else: st.error(mensaje)
    else:
        st.warning("Completa todos los campos.")
