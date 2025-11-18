import pandas as pd
import numpy as np
import yfinance as yf
import requests
import streamlit as st
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, Float, Integer, Text, text, PrimaryKeyConstraint
from sqlalchemy.pool import NullPool

def procesar_y_guardar_en_sql(archivo_subido, db_host, db_name, db_user, db_pass):
    try:
        ##IMPORTACION DE BASE DE DATOS
        st.write(f"Leyendo archivo: {archivo_subido.name}...")
        if archivo_subido.name.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(archivo_subido, sheet_name="resultados_por_lotes_finales")
        else:
            st.error("Error: Formato de archivo no soportado.")
            return False, "Error de archivo"

        ##RENOMBRAR COLUMNAS
        df.rename(columns = {"Cantidad": "cantidad", "Descripcion": "descripcion", "Fecha": "fecha", "Fecha Lote": "fecha_descarga", "Gastos": "gastos", "Moneda": "moneda", "Operacion": "operacion", "Precio Compra": "precio_compra", "Ticker": "ticker", "Tipo": "tipo", "DolarCCL": "dolar_ccl", "DolarMEP": "dolar_mep", "DolarOficial": "dolar_oficial"}, inplace = True)

        ##ELIMINACIÓN DE COLUMNAS INNECESARIAS
        columnas_a_borrar = ["dolar_ccl", "operacion"]
        columnas_existentes = [col for col in columnas_a_borrar if col in df.columns]
        df.drop(columnas_existentes, axis=1, inplace=True)

        ##FILTRO DE DATOS POR TIPO DE ACTIVO
        df_cedears = df[df.tipo == "Cedears"].copy()

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
        st.write("Obteniendo cotizaciones...")

        ## COTIZACION ACTUALIZADA
        for ticker in tickers_unicos:
            ticker_argentina = ticker + ".BA"
            try:
                ticker_obj = yf.Ticker(ticker_argentina)
                info = ticker_obj.info
                if 'regularMarketPrice' in info and info['regularMarketPrice'] is not None:
                    precio = info['regularMarketPrice']
                else:
                    precio = ticker_obj.fast_info["last_price"]
                cotizacion_actual[ticker] = precio
            except Exception as e:
                st.warning(f"Error al obtener la cotización de {ticker}: {e}")

        ## CALCULO DE TENENCIA TOTAL ACTUALIZADA EN PESOS ARGENTINOS
        df_cedears["tenencia_ars"] = (df_cedears.cantidad * df_cedears.ticker.map(cotizacion_actual).fillna(0))*(1-0.006)

        ## CREACION DE FUNCION PARA OBTENER VALOR DE DOLAR ACTUALIZADO
        def obtener_valores_dolar():
            api_url = "https://dolarapi.com/v1/dolares"
            try:
                response = requests.get(api_url)
                response.raise_for_status()
                data = response.json()
                df_dolar_api = pd.DataFrame(data)
                valor_oficial = df_dolar_api.loc[df_dolar_api['casa'] == 'oficial', 'venta'].values[0]
                valor_mep = df_dolar_api.loc[df_dolar_api['casa'] == 'bolsa', 'venta'].values[0]
                return valor_oficial, valor_mep
            except Exception as e:
                st.error(f"Error al cargar valores de dólar: {e}")
                return None, None

        ## EJECUCIÓN DE LA FUNCIÓN
        st.write("Obteniendo valor del dólar...")
        dolar_oficial, dolar_mep = obtener_valores_dolar()
        if dolar_oficial is None or dolar_mep is None:
            raise Exception("No se pudo obtener el valor del dólar, el proceso no puede continuar.")

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
        except Exception as e:
            st.error(f"Error en wide_to_long: {e}")
            raise e

        ## DATOS DE CONEXION A SUPABASE (SQL)
        st.write("Conectando a la base de datos...")
        user = db_user
        password = db_pass
        database = db_name
        pooler_host = db_host
        pooler_port = '5432'

        ## GUARDADO DE DF_CEDEARS CON PRIMARYKEY EN SUPABASE (SQL)
        # CONEXION A SQL
        connection_url = f'postgresql+psycopg2://{user}:{password}@{pooler_host}:{pooler_port}/{database}?sslmode=require'

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
                st.write(f"Tabla '{table_name}' creada.")

                # ELIMINACION DE DATOS
                connection.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"))
                # INSERCIÓN DE DATOS
                df_cedears.to_sql(
                    table_name,
                    connection,
                    if_exists='append',
                    index=False
                )
                st.success(f"Carga de datos exitosa en '{table_name}'")
        except Exception as e:
            st.error(f"ERROR en la carga de 'cedears': {e}")
            raise e

        ## GUARDADO DE DATOS HISTORICOS
        # CONEXION A SQL
        connection_url_hist = f'postgresql+psycopg2://{user}:{password}@{pooler_host}:{pooler_port}/{database}?sslmode=require'

        try:
            engine_hist = create_engine(connection_url_hist, poolclass=NullPool)
            # ESTRUCTURA DE LA TABLA
            metadata_hist = MetaData()
            table_name_hist = 'datos_historicos_cedears'
            historico_table = Table(
                table_name_hist,
                metadata_hist,
                Column('ticker', String, primary_key=True),
                Column('fecha_ejecucion', Date, primary_key=True),
                Column('moneda', String, primary_key=True),
                Column('costo', Float),
                Column('tenencia', Float),
                Column('resultados', Float),
                Column('rendimiento', Float)
            )
            # Drop and recreate the table to ensure schema is updated
            metadata_hist.drop_all(engine_hist)
            metadata_hist.create_all(engine_hist)
            st.write(f"Tabla '{table_name_hist}' creada.")

            with engine_hist.connect() as connection:
                # INSERCIÓN DE DATOS
                try:
                    if 'df_final_listo' not in locals():
                        st.warning("No existe df_final_listo, omitiendo carga de datos históricos.")
                    else:
                        df_final_listo.to_sql(
                            table_name_hist,
                            connection,
                            if_exists='append',
                            index=False
                        )
                        st.success(f"Carga de datos exitosa en '{table_name_hist}'")
                except Exception as ex:
                    # COMPROBACIÓN DE DATOS DUPLICADOS
                    if "violates unique constraint" in str(ex) or "duplicate key value" in str(ex):
                        st.warning(f"Advertencia: Datos ya cargados el día de hoy en '{table_name_hist}'.")
                    else:
                        st.error(f"Error en carga de datos históricos: {ex}")
                        raise ex
        
        except Exception as e:
            st.error(f"ERROR en la carga de 'datos_historicos_cedears': {e}")
            raise e
        
        # Si todo salió bien
        return True, "¡Proceso completado con éxito!"

    except Exception as e:
        # Si algo falló en cualquier punto
        st.error(f"Error detallado: {e}")
        return False, f"Error general en el procesamiento: {e}"

# -----------------------------------------------------------------
# 2. LA INTERFAZ WEB (EL FRONT-END)
# -----------------------------------------------------------------
st.set_page_config(layout="centered", page_title="Análisis de inversiones")
st.title("💰 Análisis de inversiones")
st.write("Sube tu reporte de Balanz y completa los datos de tu Base de Datos de Supabase (PostgreSQL).")

# --- Formulario de Carga ---
with st.form(key="upload_form"):
    
    # A. El cargador de archivos
    uploaded_file = st.file_uploader("1. Sube tu archivo (Excel)", type=["xlsx", "xls"])
    
    st.divider()
    
    # B. Las credenciales de la DB
    st.subheader("2. Credenciales de tu Base de Datos (Supabase)")
    col1, col2 = st.columns(2)
    with col1:
        db_host = st.text_input("Host (Servidor)", placeholder="db.xxxxxxxx.supabase.co")
        db_user = st.text_input("Usuario", type="usuario")
    with col2:
        db_name = st.text_input("Nombre de la Base de Datos", "postgres")
        db_pass = st.text_input("Contraseña", type="password")

    st.divider()

    # C. El botón de envío
    submit_button = st.form_submit_button(
        label="🚀 Procesar y Cargar Datos", 
        use_container_width=True
    )

# --- Lógica de Procesamiento (se ejecuta al apretar el botón) ---
if submit_button:
    
    # Verificamos que todos los campos estén completos
    if uploaded_file is not None and db_host and db_name and db_user and db_pass:
        
        # Muestra el "spinner" mientras la función se ejecuta
        with st.spinner('Procesando archivo y conectando a Supabase... Esto puede tardar varios segundos...'):
            
            # Llama a tu función de lógica
            exito, mensaje = procesar_y_guardar_en_sql(
                uploaded_file, 
                db_host, 
                db_name, 
                db_user, 
                db_pass
            )
        
        # Muestra el resultado
        if exito:
            st.success(mensaje)
            
            # --- 3. BOTÓN DE DESCARGA DE POWER BI ---
            st.subheader("¡Tus datos están listos!")
            st.write("El siguiente paso es descargar tu plantilla de Power BI. Ábrela, introduce tus credenciales de Supabase (las mismas que usaste aquí) y haz clic en 'Actualizar'.")
            
            # Nombre de tu plantilla. DEBE estar en la misma carpeta que este script.
            template_file_name = "Plantilla_PowerBI.pbit" 
            
            # Verificamos si el archivo existe antes de mostrar el botón
            if os.path.exists(template_file_name):
                with open(template_file_name, "rb") as f:
                    file_data = f.read()
                
                st.download_button(
                    label="📥 Descargar Plantilla de Power BI (.pbit)",
                    data=file_data,
                    file_name=template_file_name,
                    mime="application/vnd.ms-powerbi.template",
                    use_container_width=True
                )
            else:
                st.error(f"Error de configuración: No se encontró el archivo '{template_file_name}' en el servidor.")
                st.warning("Asegúrate de haber subido el archivo .pbit a la carpeta de la aplicación.")
                
        else:
            # Si 'exito' es False, muestra el mensaje de error
            st.error(mensaje)
            
    else:
        # Si faltan campos
        st.warning("Por favor, completa TODOS los campos y sube un archivo.")

# Verificamos que todos los campos estén completos
if uploaded_file is not None and db_host and db_name and db_user and db_pass:
    
    # Muestra el "spinner" mientras la función se ejecuta
    with st.spinner('Procesando archivo y conectando a Supabase... Esto puede tardar varios segundos...'):
        
        # Llama a tu función de lógica
        exito, mensaje = procesar_y_guardar_en_sql(
            uploaded_file, 
            db_host, 
            db_name, 
            db_user, 
            db_pass
        )
    
    # Muestra el resultado
    if exito:
        st.success(mensaje)
        
        # --- 3. BOTÓN DE DESCARGA DE POWER BI ---
        st.subheader("¡Tus datos están listos!")
        st.write("Descarga tu plantilla de Power BI. Ábrela, introduce tus credenciales de Supabase y haz clic en 'Actualizar'.")
        
        # Nombre de tu plantilla. DEBE estar en la misma carpeta que este script.
        template_file_name = "Plantilla_PowerBI.pbit" 
        
        if os.path.exists(template_file_name):
            with open(template_file_name, "rb") as f:
                file_data = f.read()
            
            st.download_button(
                label="📥 Descargar Plantilla de Power BI (.pbit)",
                data=file_data,
                file_name=template_file_name,
                mime="application/vnd.ms-powerbi.template",
                use_container_width=True
            )
        else:
            st.error(f"Error de configuración: No se encontró '{template_file_name}'.")
            st.warning("Asegúrate de haber subido el archivo .pbit a la carpeta de la aplicación.")
            
    else:
        st.error(mensaje)
        
else:
    st.warning("Por favor, completa TODOS los campos y sube un archivo.")









