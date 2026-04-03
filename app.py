## IMPORTACION DE BIBLIOTECAS
import pandas as pd
import numpy as np
import requests
import streamlit as st
import os
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
    st.error("❌ Error: Configure los Secrets 'iol' en Streamlit Cloud.")
    st.stop()

## INSTRUCTIVO PARA DESCARGAR REPORTE DE BALANZ
@st.dialog("📥 Cómo descargar el reporte de Balanz")
def mostrar_instructivo():
    st.markdown("""
    Sigue estos pasos para obtener el reporte de Balanz:
    1. Ingresa a **Balanz > Reportes > Resultados del período**.
    2. Configura: Informe **COMPLETO** y el rango de fechas desde el inicio.
    3. Descarga el archivo `.xlsx`.
    """)
    st.image("resFolder/paso1.png", use_container_width=True)
    st.image("resFolder/paso2.png", use_container_width=True)

# --- FUNCIONES AUXILIARES ---
def obtener_headers_iol():
    url_token = "https://api.invertironline.com/token"
    data = {'username': USUARIO_IOL, 'password': PASSWORD_IOL, 'grant_type': 'password'}
    try:
        res = requests.post(url_token, data=data)
        return {'Authorization': f"Bearer {res.json().get('access_token')}"}
    except: return None

def obtener_valores_dolar():
    api_url = "https://dolarapi.com/v1/dolares"
    try:
        res = requests.get(api_url).json()
        df_d = pd.DataFrame(res)
        oficial = df_d.loc[df_d['casa'] == 'oficial', 'venta'].values[0]
        mep = df_d.loc[df_d['casa'] == 'bolsa', 'venta'].values[0]
        return oficial, mep
    except: return None, None

## FUNCIÓN PRINCIPAL DE PROCESAMIENTO
def procesar_y_guardar_en_sql(archivo_subido, db_host, db_name, db_user, db_pass):
    try:
        barra_progreso = st.progress(0, text="Iniciando...")
        
        # 1. LECTURA DE EXCEL
        df = pd.read_excel(archivo_subido, sheet_name="resultados_por_lotes_finales")
        df.rename(columns = {"Cantidad": "cantidad", "Descripcion": "descripcion", "Fecha": "fecha", "Fecha Lote": "fecha_descarga", "Gastos": "gastos", "Moneda": "moneda", "Operacion": "operacion", "Precio Compra": "precio_compra", "Ticker": "ticker", "Tipo": "tipo", "DolarCCL": "dolar_ccl", "DolarMEP": "dolar_mep", "DolarOficial": "dolar_oficial"}, inplace = True)
        
        # 2. FILTRADO POR TIPO
        df_cedears = df[df.tipo == "Cedears"].copy()
        df_on = df[df.tipo == "Corporativos - Dólar"].copy()
        
        for dff in [df_cedears, df_on]:
            dff.fecha = pd.to_datetime(dff.fecha)
            dff.fecha_descarga = pd.to_datetime(dff.fecha_descarga)

             # 4. DOLAR Y CÁLCULOS
            d_oficial, d_mep = obtener_valores_dolar()
            min_d = np.minimum(d_oficial, d_mep)

            # Definimos el Tipo de Cambio Histórico para la pesificación
            tc_historico = np.where(dff.fecha < pd.to_datetime("2025-04-15"), dff.dolar_mep, min_d)
            
            # Lógica de COSTO_ARS:
            # - Si es 'Dólares': (Cantidad * Precio_Compra * TC_Historico) + Gastos
            # - Si es 'Pesos': (Cantidad * Precio_Compra) + Gastos
            dff["costo_ars"] = np.where(
                dff["moneda"] == "Dólares",
                (dff["cantidad"] * dff["precio_compra"] * tc_historico) + dff["gastos"],
                (dff["cantidad"] * dff["precio_compra"]) + dff["gastos"]
            )

        # 3. COTIZACIONES IOL
        headers = obtener_headers_iol()
        cotizaciones_raw = {}
        paneles = ["Cedears", "TitulosPublicos", "ObligacionesNegociables"]
        for p in paneles:
            url = f"https://api.invertironline.com/api/v2/Cotizaciones/{p}/Argentina/Todos"
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                for t in res.json().get('titulos', []):
                    cotizaciones_raw[t['simbolo'].strip()] = t['ultimoPrecio']

        # Normalización de precios (Lámina 100 para ON)
        cotizacion_actual = {t: cotizaciones_raw.get(t) for t in df_cedears.ticker.unique()}
        for t in df_on.ticker.unique():
            if t in cotizaciones_raw: cotizacion_actual[t] = cotizaciones_raw[t] / 100

       
        # Lógica de costos y tenencia
        for dff in [df_cedears, df_on]:
            # 1. Tenencia actualizada (Valuación de mercado a hoy)
            dff["tenencia_ars"] = (dff.cantidad * dff.ticker.map(cotizacion_actual).fillna(0)) * 0.994
            dff["tenencia_usd"] = dff.tenencia_ars / min_d
            
            # 2. Tipo de Cambio Histórico (Según tu regla de la fecha de abril)
            tc_historico = np.where(dff.fecha < pd.to_datetime("2025-04-15"), dff.dolar_mep, min_d)
            
            # 3. Lógica de Costo USD (Diferenciando Pesos vs Dólares)
            # - Si es 'Dólares': (Cantidad * Precio_Compra) + (Gastos / TC_Historico)
            # - Si es 'Pesos': (Costo_ARS) / TC_Historico
            dff["costo_usd"] = np.where(
                dff["moneda"] == "Dólares",
                (dff["cantidad"] * dff["precio_compra"]) + (dff["gastos"] / tc_historico),
                dff["costo_ars"] / tc_historico
            )
            
            # 4. Resultados y Rendimientos
            dff["resultados_ars"] = dff.tenencia_ars - dff.costo_ars
            dff["resultados_usd"] = dff.tenencia_usd - dff.costo_usd
            dff["rendimiento_ars"] = round((dff.tenencia_ars / dff.costo_ars - 1) * 100, 2)
            dff["rendimiento_usd"] = round((dff.tenencia_usd / dff.costo_usd - 1) * 100, 2)
            
        # 5. PREPARACIÓN DE HISTÓRICOS (Wide to Long)
        def preparar_historico(df_input):
            agrup = df_input.groupby("ticker").agg({"cantidad":"sum", "costo_ars":"sum", "costo_usd":"sum", "tenencia_ars":"sum", "tenencia_usd":"sum", "resultados_ars":"sum", "resultados_usd":"sum"}).reset_index()
            agrup["rendimiento_ars"] = agrup["resultados_ars"] / agrup["costo_ars"]
            agrup["rendimiento_usd"] = agrup["resultados_usd"] / agrup["costo_usd"]
            agrup['fecha_ejecucion'] = date.today()
            return pd.wide_to_long(agrup, stubnames=['costo', 'tenencia', 'resultados', 'rendimiento'], i=['ticker', 'cantidad', 'fecha_ejecucion'], j='moneda', sep='_', suffix='(ars|usd)').reset_index()

        df_final_listo = preparar_historico(df_cedears)
        df_on_final_listo = preparar_historico(df_on)

        # 6. CUPONES (Realizado)
        df_realizado = pd.read_excel(archivo_subido, sheet_name="resultados_por_realizado")
        df_c = df_realizado[(df_realizado["Tipo Movimiento"].isin(["Cupón", "Dividendo"])) & (df_realizado["Moneda Venta"].isin(["Dólares C.V. 7000", "Dólares"]))].copy()
        df_c["CuponUSD"] = np.where(df_c["Tipo"] == "Corporativos - Dólar", (df_c["Cupones"].fillna(0) - df_c["Gastos"].fillna(0)) / df_c["OperacionVentaDolarOficial"], df_c["Dividendos"].fillna(0))
        df_cupones_final = df_c.groupby("Ticker")[["CuponUSD"]].sum().reset_index()

        # 7. GUARDADO EN SUPABASE (CON DEFINICIÓN DE TABLAS)
        connection_url = f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:5432/{db_name}?sslmode=require'
        engine = create_engine(connection_url, poolclass=NullPool)
        
        metadata = MetaData()

        # Definición de tablas históricas con Primary Key Compuesta (Tu original)
        hist_cedears = Table('datos_historicos_cedears', metadata, Column('ticker', String, primary_key=True), Column('cantidad', Float), Column('fecha_ejecucion', Date, primary_key=True), Column('moneda', String, primary_key=True), Column('costo', Float), Column('tenencia', Float), Column('resultados', Float), Column('rendimiento', Float))
        hist_on = Table('datos_historicos_on', metadata, Column('ticker', String, primary_key=True), Column('cantidad', Float), Column('fecha_ejecucion', Date, primary_key=True), Column('moneda', String, primary_key=True), Column('costo', Float), Column('tenencia', Float), Column('resultados', Float), Column('rendimiento', Float))
        hist_dolar = Table('historico_dolar', metadata, Column('fecha', Date, primary_key=True), Column('tipo', String, primary_key=True), Column('valor', Float))

        metadata.create_all(engine)

        with engine.begin() as conn:
            # Fotos actuales (Replace)
            df_cedears.to_sql('cedears', conn, if_exists='replace', index=False)
            df_on.to_sql('on_movimientos', conn, if_exists='replace', index=False)
            df_cupones_final.to_sql('df_cupones_final', conn, if_exists='replace', index=False)
            
            # Históricos (Append con control de duplicados)
            try:
                df_final_listo.to_sql('datos_historicos_cedears', conn, if_exists='append', index=False)
                df_on_final_listo.to_sql('datos_historicos_on', conn, if_exists='append', index=False)
                if d_oficial:
                    pd.DataFrame([{'fecha':date.today(),'tipo':'Oficial','valor':d_oficial},{'fecha':date.today(),'tipo':'MEP','valor':d_mep}]).to_sql('historico_dolar', conn, if_exists='append', index=False)
            except:
                st.warning("⚠️ Datos de hoy ya existentes en el histórico. Se omitió el duplicado.")

        barra_progreso.progress(1.0)
        return True, "✅ Proceso completado con éxito."
    except Exception as e: return False, f"Error: {e}"

# --- FRONTEND (ESTRUCTURA ORIGINAL) ---
st.set_page_config(layout="centered", page_title="Análisis Inversiones")
st.image("resFolder/logo.png", use_container_width=True)
st.title("💰 Análisis de inversiones")

if st.button("Ver instructivo de descarga"): mostrar_instructivo()
st.divider()

with st.form("upload_form"):
    uploaded_file = st.file_uploader("1. Sube tu archivo (Excel)", type=["xlsx"])
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        db_host = st.text_input("Host")
        db_user = st.text_input("Usuario")
    with col2:
        db_name = st.text_input("DB Name", "postgres")
        db_pass = st.text_input("Contraseña", type="password")
    submit = st.form_submit_button("🚀 Procesar y Cargar Datos", use_container_width=True)

if submit:
    if uploaded_file and db_host and db_pass:
        with st.spinner('Procesando...'):
            exito, mensaje = procesar_y_guardar_en_sql(uploaded_file, db_host, db_name, db_user, db_pass)
            if exito:
                st.success(mensaje)
                with open("resFolder/Reporte de inversiones - Power BI.pbit", "rb") as f:
                    st.download_button("📥 Descargar Informe Power BI", f.read(), file_name="Reporte_Inversiones.pbit", use_container_width=True)
            else: st.error(mensaje)
