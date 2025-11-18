##INSTALAR E IMPORTAR BIBLIOTECAS
!pip install supabase
!pip install sqlalchemy psycopg2-binary
import pandas as pd
import numpy as np
import yfinance as yf
import requests
import os
import streamlit as st
import io
from datetime import datetime
from supabase import create_client, Client
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, Float, Integer, Text, text, PrimaryKeyConstraint
from sqlalchemy.pool import NullPool

##IMPORTACION DE BASE DE DATOS
st.write(f"Leyendo archivo: {archivo_subido.name}...")
if archivo_subido.name.endswith('.csv'):
    df = pd.read_csv(archivo_subido)
elif archivo_subido.name.endswith(('.xls', '.xlsx')):
    df = pd.read_excel(archivo_subido)
else:
    return False, f"❌ Error: Formato de archivo no soportado."

##RENOMBRAR COLUMNAS
df.rename(columns = {"Cantidad": "cantidad", "Descripcion": "descripcion", "Fecha": "fecha", "Fecha Lote": "fecha_descarga", "Gastos": "gastos", "Moneda": "moneda", "Operacion": "operacion", "Precio Compra": "precio_compra", "Ticker": "ticker", "Tipo": "tipo", "DolarCCL": "dolar_ccl", "DolarMEP": "dolar_mep", "DolarOficial": "dolar_oficial"}, inplace = True)

##ELIMINACIÓN DE COLUMNAS INNECESARIAS
df.drop(["dolar_ccl", "operacion"], axis=1, inplace=True)

##FILTRO DE DATOS POR TIPO DE ACTIVO
df_cedears = df[df.tipo == "Cedears"]

##CAMBIO DE TIPO DE DATO A FECHA
df_cedears.fecha = pd.to_datetime(df_cedears.fecha)
df_cedears.fecha_descarga = pd.to_datetime(df_cedears.fecha_descarga)

##CALCULO DE COSTO EN PESOS ARGENTINOS
df_cedears["costo_ars"] = (df_cedears.cantidad * df_cedears.precio_compra)+df_cedears.gastos

##CALCULO DE COSTO EN USD (SEGÚN FECHA)
df_cedears["costo_usd"] = np.where(
    df_cedears.fecha < pd.to_datetime("2025-04-15"),
    df_cedears.costo_ars / df_cedears.dolar_mep,
    df_cedears.costo_ars / np.minimum(df_cedears.dolar_oficial, df_cedears.dolar_mep)
)

## LISTA UNICA DE ACCIONES
tickers_unicos = df_cedears.ticker.unique()

## CREACIÓN DE DICCIONARIO PARA LA COTIZACION ACTUAL
cotizacion_actual = {}

## COTIZACION ACTUALIZADA
for ticker in tickers_unicos:
  ticker_argentina = ticker + ".BA"
  try:
    ticker_obj = yf.Ticker(ticker_argentina)
    info = ticker_obj.info
    if 'regularMarketPrice' in info:
        precio = info['regularMarketPrice']
    else:
        precio = ticker_obj.fast_info["last_price"]

    cotizacion_actual[ticker] = precio

  except Exception as e:
    print(f"Error al obtener la cotización de {ticker}: {e}")

## CALCULO DE TENENCIA TOTAL ACTUALIZADA EN PESOS ARGENTINOS
df_cedears["tenencia_ars"] = (df_cedears.cantidad * df_cedears.ticker.map(cotizacion_actual))*(1-0.006)

## CREACION DE FUNCION PARA OBTENER VALOR DE DOLAR ACTUALIZADO

def obtener_valores_dolar():
    api_url = "https://dolarapi.com/v1/dolares"
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        df_dolar_api = pd.DataFrame(data)

        # Extraer los valores de venta del Dolar Oficial y Dolar MEP
        valor_oficial = df_dolar_api.loc[df_dolar_api['casa'] == 'oficial', 'venta'].values[0]
        valor_mep = df_dolar_api.loc[df_dolar_api['casa'] == 'bolsa', 'venta'].values[0]

        return valor_oficial, valor_mep

    except Exception as e:
        # MENSAJE DE ERROR EN CASO DE FALLA
        print(f"Error al cargar valores de dólar: {e}")
        return None, None

## EJECUCIÓN DE LA FUNCIÓN
dolar_oficial, dolar_mep = obtener_valores_dolar()

## CALCULO DE TENENCIA TOTAL ACTUALIZADA EN USD (utilizando el tipo de cambio mas bajo)

df_cedears["tenencia_usd"] = df_cedears.tenencia_ars / np.minimum(dolar_oficial, dolar_mep)

## CÁLCULO DE GANANCIA O PERDIDA EN PESOS ARGENTINOS

df_cedears["resultados_ars"] = df_cedears.tenencia_ars - df_cedears.costo_ars

## CÁLCULO DE GANANCIA O PERDIDA EN DOLARES

df_cedears["resultados_usd"] = df_cedears.tenencia_usd - df_cedears.costo_usd

## CÁLCULO DE RENDIMIENTO PORCENTUAL EN PESOS ARGENTINOS

df_cedears["rendimiento_ars"] = round((df_cedears.tenencia_ars / df_cedears.costo_ars - 1) * 100, 2)

## CÁLCULO DE RENDIMIENTO PORCENTUAL EN DOLARES


df_cedears["rendimiento_usd"] = round((df_cedears.tenencia_usd / df_cedears.costo_usd - 1) * 100, 2)

## AGRUPACION DE ACCIONES Y TOTALES

df_cedears_analisis = df_cedears[["ticker", "costo_ars","costo_usd","tenencia_ars",	"tenencia_usd", "resultados_ars",	"resultados_usd"]]

df_cedears_agrupado = df_cedears_analisis.groupby("ticker").sum().round(2)

df_cedears_agrupado["rendimiento_ars"] = df_cedears_agrupado["resultados_ars"] / df_cedears_agrupado["costo_ars"]
df_cedears_agrupado["rendimiento_usd"] = df_cedears_agrupado["resultados_usd"] / df_cedears_agrupado["costo_usd"]

df_cedears_agrupado.reset_index(inplace=True)

## MODIFICACION DEL DATAFRAME PARA FILTRAR POR MONEDA


# AÑADIR FECHA DE EJECUCIÓN
df_cedears_agrupado['fecha_ejecucion'] = datetime.now().date()


# MODIFICACIÓN PARA INCLUIR TIPO DE MONEDA
try:
    df_final_largo = pd.wide_to_long(
        df_cedears_agrupado,

        # PREFIJO DE COLUMNA
        stubnames=['costo', 'tenencia', 'resultados', 'rendimiento'],

        # COLUMNAS QUE NO DEBEN PIVOTARSE
        i=['ticker', 'fecha_ejecucion'],

        # CREACION DE COLUMNA POR TIPO DE MONEDA
        j='moneda',

        # CONECTOR ENTRE PREFIJO Y SUFIJO
        sep='_',

        # SUFIJO DE COLUMNA
        suffix='(ars|usd)'
    )

    # RESET DE INDICES
    df_final_listo = df_final_largo.reset_index()

    print("\nEjecución exitosa")
except Exception as e:
    print(f"\nError: {e}")

## DATOS DE CONEXION A SUPABASE (SQL)

user = db_user
password = db_pass
database = db_name
pooler_host = db_host
pooler_port = '5432'

## GUARDADO DE DF_CEDEARS CON PRIMARYKEY EN SUPABASE (SQL)

# CONEXION A SQL

connection_url = f'postgresql+psycopg2://{user}:{password}@{pooler_host}:{pooler_port}/{database}'

try:
    engine = create_engine(connection_url, poolclass=NullPool)

    with engine.begin() as connection:

        # ESTRUCTURA DE LA TABLA
        metadata = MetaData()
        table_name = 'cedears'

        cedears_table = Table(
            table_name,
            metadata,
            Column('id_operacion', Integer, primary_key=True, autoincrement=True),
            Column('cantidad', Float),
            Column('descripcion', Text),
            Column('fecha', Date),
            Column('fecha_descarga', Date),
            Column('gastos', Float),
            Column('moneda', String),
            Column('precio_compra', Float),
            Column('ticker', String),
            Column('tipo', String),
            Column('dolar_mep', Float),
            Column('dolar_oficial', Float),
            Column('costo_ars', Float),
            Column('costo_usd', Float),
            Column('tenencia_ars', Float),
            Column('tenencia_usd', Float),
            Column('resultados_ars', Float),
            Column('resultados_usd', Float),
            Column('rendimiento_ars', Float),
            Column('rendimiento_usd', Float)
        )

        metadata.create_all(engine)
        print(f"Tabla '{table_name}' creada.")

        # ELIMINACION DE DATOS
        connection.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"))

        # INSERCIÓN DE DATOS
        df_cedears.to_sql(
            table_name,
            connection,
            if_exists='append',
            index=False
        )

        print(f"Carga de datos exitosa")

except Exception as e:
    print(f"ERROR: {e}")

## GUARDADO DE DATOS HISTORICOS

# CONEXION A SQL

connection_url = f'postgresql+psycopg2://{user}:{password}@{pooler_host}:{pooler_port}/{database}'

try:
    engine = create_engine(connection_url)
    # Removed: connection = engine.connect() to allow metadata.drop_all to work correctly

    # ESTRUCTURA DE LA TABLA

    metadata = MetaData()
    table_name = 'datos_historicos_cedears'

    historico_table = Table(
        table_name,
        metadata,
        Column('ticker', String, primary_key=True),
        Column('fecha_ejecucion', Date, primary_key=True),
        Column('moneda', String, primary_key=True),
        Column('costo', Float),
        Column('tenencia', Float),
        Column('resultados', Float),
        Column('rendimiento', Float)
    )

    # Drop and recreate the table to ensure schema is updated
    metadata.drop_all(engine) # Added: Drop existing table
    metadata.create_all(engine)
    print(f"Tabla '{table_name}' recreada con el esquema actualizado")

    with engine.connect() as connection:
        # INSERCIÓN DE DATOS

        try:
            if 'df_final_listo' not in locals():
                print("No existe df_final_listo")

            else:
                df_final_listo.to_sql(
                    table_name,
                    connection,
                    if_exists='append',
                    index=False
                )
                print(f"Carga de datos exitosa")

        except Exception as ex:

          # COMPROBACIÓN DE DATOS DUPLICADOS
            if "violates unique constraint" in str(ex) or "duplicate key value" in str(ex):
                print(f"Datos ya cargados el día de hoy")
            else:
                print(f"Error: {ex}")

        finally:
            # CIERRE DE CONEXION is handled by 'with engine.connect() as connection:'
            pass

except Exception as e:
    print(f"Error: {e}")

# -----------------------------------------------------------------
# 2. LA INTERFAZ WEB (EL FRONT-END)
# -----------------------------------------------------------------
st.set_page_config(layout="centered", page_title="Cargador de Datos")
st.title("Análisis de inversiones")

st.write("Sube tu reporte de Balanz y completa los datos de tu Base de Datos de Supabase (PostgreSQL).")

# --- Formulario de Carga ---
with st.form(key="upload_form"):
    
    # A. El cargador de archivos
    uploaded_file = st.file_uploader("1. Sube tu archivo (CSV o Excel)", type=["csv", "xlsx", "xls"])
    
    st.divider()
    
    # B. Las credenciales de la DB
    st.subheader("2. Credenciales de tu Base de Datos (Supabase)")
    col1, col2 = st.columns(2)
    with col1:
        # Usamos placeholders para guiar al usuario
        db_host = st.text_input("Host (Servidor)", placeholder="db.xxxxxxxx.supabase.co")
        db_user = st.text_input("Usuario", "postgres")
    with col2:
        db_name = st.text_input("Nombre de la Base de Datos", "postgres")
        db_pass = st.text_input("Contraseña", type="password")

    st.divider()

    # C. El botón de envío
    submit_button = st.form_submit_button(label="🚀 Procesar y Cargar Datos")

# --- Lógica de Procesamiento (se ejecuta al apretar el botón) ---
if submit_button:
    
    # --- VALORES PREDEFINIDOS ---
    # Aquí definimos los valores que antes preguntábamos al usuario
    table_name_predefinido = "datos_procesados"
    upload_mode_predefinido = "replace" # 'replace' borra datos viejos y carga los nuevos
    
    # Verificamos que todos los campos estén completos
    if uploaded_file is not None and db_host and db_name and db_user and db_pass:
        
        with st.spinner('Procesando archivo y conectando a Supabase...'):
            
            # Llama a tu función de lógica con los valores predefinidos
            exito, mensaje = procesar_y_guardar_en_sql(
                uploaded_file, 
                db_host, 
                db_name, 
                db_user, 
                db_pass, 
                table_name_predefinido, 
                upload_mode_predefinido
            )
        
        # Muestra el resultado
        if exito:
            st.success(mensaje)
        else:
            st.error(mensaje)
            
    else:
        st.warning("Por favor, completa TODOS los campos y sube un archivo.")

