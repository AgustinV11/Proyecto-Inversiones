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
# 1. CONFIGURACIÓN API IOL (SEGURIDAD)
# ==========================================
try:
    USUARIO_IOL = st.secrets["iol"]["usuario"]
    PASSWORD_IOL = st.secrets["iol"]["password"]
except Exception:
    st.error("❌ Error: No se encontraron las credenciales 'iol' en los Secrets de Streamlit.")
    st.stop()

## INSTRUCTIVO PARA DESCARGAR REPORTE DE BALANZ
@st.dialog("📥 Cómo descargar el reporte de Balanz")
def mostrar_instructivo():
    st.markdown("""
    Sigue estos pasos para obtener el reporte de Balanz:
    1. Ingresa a Balanz y ve a **Reportes > Resultados del período**.
    2. Período: Selecciona desde el inicio. Informe: **COMPLETO**.
    3. Descarga el archivo `.xlsx`.
    """)

# --- FUNCIONES DE APOYO ---
def obtener_headers():
    url_token = "https://api.invertironline.com/token"
    data_token = {'username': USUARIO_IOL, 'password': PASSWORD_IOL, 'grant_type': 'password'}
    try:
        response = requests.post(url_token, data=data_token)
        response.raise_for_status()
        return {'Authorization': f"Bearer {response.json().get('access_token')}"}
    except Exception as e:
        return None

def obtener_valores_dolar():
    api_url = "https://dolarapi.com/v1/dolares"
    try:
        response = requests.get(api_url)
        data = response.json()
        df_dolar = pd.DataFrame(data)
        oficial = df_dolar.loc[df_dolar['casa'] == 'oficial', 'venta'].values[0]
        mep = df_dolar.loc[df_dolar['casa'] == 'bolsa', 'venta'].values[0]
        return oficial, mep
    except:
        return None, None

## FUNCIÓN PRINCIPAL DE PROCESAMIENTO
def procesar_y_guardar_en_sql(archivo_subido, db_host, db_name, db_user, db_pass):
    try:
        barra_progreso = st.progress(0, text="Iniciando procesamiento...")
        
        # 1. CARGA DE DATOS (Balanz)
        df = pd.read_excel(archivo_subido, sheet_name="resultados_por_lotes_finales")
        df.rename(columns = {"Cantidad": "cantidad", "Descripcion": "descripcion", "Fecha": "fecha", "Fecha Lote": "fecha_descarga", "Gastos": "gastos", "Moneda": "moneda", "Operacion": "operacion", "Precio Compra": "precio_compra", "Ticker": "ticker", "Tipo": "tipo", "DolarCCL": "dolar_ccl", "DolarMEP": "dolar_mep", "DolarOficial": "dolar_oficial"}, inplace = True)
        
        # 2. SEPARACIÓN Y FILTROS
        df_cedears = df[df.tipo == "Cedears"].copy()
        df_on = df[df.tipo == "Corporativos - Dólar"].copy()
        
        for dff in [df_cedears, df_on]:
            dff.fecha = pd.to_datetime(dff.fecha)
            dff.fecha_descarga = pd.to_datetime(dff.fecha_descarga)
            dff["costo_ars"] = (dff.cantidad * dff.precio_compra) + dff.gastos

        # 3. OBTENCIÓN DE COTIZACIONES (IOL)
        headers = obtener_headers()
        if not headers: return False, "Error de autenticación IOL. Revisa tus Secrets."
        
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

        # 4. NORMALIZACIÓN DE PRECIOS (Dividido 100 para ON)
        cotizaciones_actuales = {}
        for t in df_cedears.ticker.unique():
            if t in cotizaciones_raw: cotizaciones_actuales[t] = cotizaciones_raw[t]
        for t in df_on.ticker.unique():
            if t in cotizaciones_raw: cotizaciones_actuales[t] = cotizaciones_raw[t] / 100

        # 5. VALORES DE DÓLAR
        dolar_oficial, dolar_mep = obtener_valores_dolar()
        min_dolar = np.minimum(dolar_oficial, dolar_mep)

        # 6. PROCESAMIENTO CEDEARS (Histórico)
        df_cedears["tenencia_ars"] = (df_cedears.cantidad * df_cedears.ticker.map(cotizaciones_actuales)) * 0.994
        df_ced_agrup = df_cedears.groupby("ticker").sum(numeric_only=True).reset_index()
        df_ced_agrup['fecha_ejecucion'] = date.today()
        df_final_listo = pd.wide_to_long(df_ced_agrup, stubnames=['costo', 'tenencia'], i=['ticker', 'fecha_ejecucion'], j='moneda', sep='_', suffix='(ars|usd)').reset_index()

        # 7. PROCESAMIENTO ON (Histórico)
        df_on["tenencia_ars"] = (df_on.cantidad * df_on.ticker.map(cotizaciones_actuales)) * 0.994
        df_on_agrup = df_on.groupby("ticker").sum(numeric_only=True).reset_index()
        df_on_agrup['fecha_ejecucion'] = date.today()
        df_on_final_listo = pd.wide_to_long(df_on_agrup, stubnames=['costo', 'tenencia'], i=['ticker', 'fecha_ejecucion'], j='moneda', sep='_', suffix='(ars|usd)').reset_index()

        # 8. CÁLCULO DE CUPONES/DIVIDENDOS
        df_c = pd.read_excel(archivo_subido, sheet_name="resultados_por_realizado")
        df_c = df_c[(df_c["Tipo Movimiento"].isin(["Cupón", "Dividendo"])) & (df_c["Tipo"].isin(["Corporativos - Dólar", "Cedears"]))]
        df_c["CuponUSD"] = np.where(df_c["Tipo"] == "Corporativos - Dólar", 
                                    (df_c["Cupones"].fillna(0) - df_c["Gastos"].fillna(0)) / df_c["OperacionVentaDolarOficial"], 
                                    df_c["Dividendos"].fillna(0))
        df_cupones_final = df_c.groupby("Ticker")[["CuponUSD"]].sum().reset_index()

        # 9. CARGA A SUPABASE
        barra_progreso.progress(0.85, text="Impactando en Supabase...")
        conn_url = f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:5432/{db_name}?sslmode=require'
        engine = create_engine(conn_url, poolclass=NullPool)

        with engine.begin() as conn:
            # Fotos Actuales
            df_cedears.to_sql('cedears', conn, if_exists='replace', index=False)
            df_on.to_sql('on_movimientos', conn, if_exists='replace', index=False)
            df_cupones_final.to_sql('dividendos_cupones_realizados', conn, if_exists='replace', index=False)
            
            # Históricos (Append)
            df_final_listo.to_sql('datos_historicos_cedears', conn, if_exists='append', index=False)
            df_on_final_listo.to_sql('datos_historicos_on', conn, if_exists='append', index=False)

            # HISTORICO DEL DOLAR
            if dolar_oficial and dolar_mep:
                df_h_dolar = pd.DataFrame([
                    {'fecha': date.today(), 'tipo': 'Oficial', 'valor': dolar_oficial},
                    {'fecha': date.today(), 'tipo': 'MEP', 'valor': dolar_mep}
                ])
                try:
                    df_h_dolar.to_sql('historico_dolar', conn, if_exists='append', index=False)
                except:
                    pass # Evita error si se corre 2 veces el mismo día

        barra_progreso.progress(1.0, text="¡Completado!")
        return True, "✅ Datos actualizados correctamente en Supabase."

    except Exception as e:
        return False, f"❌ Error general: {e}"

# --- FRONTEND ---
st.set_page_config(layout="centered", page_title="Inversiones Balanz+IOL")
st.title("📊 Gestor de Inversiones (Contable)")

with st.form("form_carga"):
    uploaded_file = st.file_uploader("Sube tu Excel de Balanz", type=["xlsx"])
    st.divider()
    st.subheader("Credenciales Supabase")
    col1, col2 = st.columns(2)
    with col1:
        db_host = st.text_input("Host")
        db_user = st.text_input("User")
    with col2:
        db_name = st.text_input("DB Name", "postgres")
        db_pass = st.text_input("Password", type="password")
    
    submit = st.form_submit_button("🚀 Iniciar Procesamiento Masivo", use_container_width=True)

if submit:
    if uploaded_file and db_host and db_pass:
        exito, mensaje = procesar_y_guardar_en_sql(uploaded_file, db_host, db_name, db_user, db_pass)
        if exito: st.success(mensaje)
        else: st.error(mensaje)
