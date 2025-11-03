import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import io

# -----------------------------------------------------------------
# 1. TU LÓGICA DE PROCESAMIENTO (LA PEGAS AQUÍ)
# -----------------------------------------------------------------
# Esta función es tu script de Pandas.
# Recibe el archivo y las credenciales de SQL.

def procesar_y_guardar_en_sql(archivo_subido, db_host, db_name, db_user, db_pass):
    try:
        # --- A. PROCESAMIENTO CON PANDAS ---
        # Lee el archivo subido (CSV o Excel)
        # Tienes que saber qué formato es. Asumamos CSV.
        df = pd.read_csv(archivo_subido)

        # ... (Aquí va todo tu código de limpieza de Pandas) ...
        # ... (Aquí va tu código para obtener el dólar MEP, etc.) ...
        # Por ejemplo:
        # df['Fecha'] = pd.to_datetime(df['Fecha'])
        # df['Precio_USD'] = df['Precio_ARS'] / 350 # Reemplazar con tu lógica
        
        df_procesado = df # Reemplaza 'df' con tu dataframe final
        
        
        # --- B. CONEXIÓN Y CARGA A SQL ---
        # Construimos la "cadena de conexión" para SQL Server
        # (Si usas PostgreSQL (ej. Neon), la cadena es un poco diferente)
        
        # Para SQL Server (Azure)
        connection_string = f"mssql+pyodbc://{db_user}:{db_pass}@{db_host}:1433/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
        
        # Para PostgreSQL (Neon/Railway)
        # connection_string = f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}"

        engine = create_engine(connection_string)

        # Carga el dataframe a la base de datos
        df_procesado.to_sql(
            'nombre_de_tu_tabla',  # Elige un nombre para la tabla
            con=engine,
            if_exists='append', # 'append' = añadir. 'replace' = borrar y reemplazar
            index=False
        )

        return True, f"¡Éxito! Se cargaron {len(df_procesado)} filas en la tabla 'nombre_de_tu_tabla'."

    except Exception as e:
        # Si algo falla, devuelve el error
        return False, f"❌ Error: {e}"

# -----------------------------------------------------------------
# 2. LA INTERFAZ WEB (EL FRONT-END)
# -----------------------------------------------------------------
st.set_page_config(layout="centered", page_title="Cargador de Datos")
st.title("🤖 Cargador de Datos de Inversiones")

st.write("Sube tu reporte del broker y completa los datos de tu Base de Datos SQL en la nube.")

# --- Formulario de Carga ---
with st.form(key="upload_form"):
    
    # A. El cargador de archivos
    uploaded_file = st.file_uploader("1. Sube tu archivo (CSV o Excel)", type=["csv", "xlsx"])
    
    st.divider()
    
    # B. Las credenciales de la DB
    st.subheader("2. Credenciales de tu Base de Datos SQL")
    db_host = st.text_input("Host del Servidor (ej: mi-servidor.database.windows.net)")
    db_name = st.text_input("Nombre de la Base de Datos")
    db_user = st.text_input("Usuario")
    db_pass = st.text_input("Contraseña", type="password")
    
    st.divider()

    # C. El botón de envío
    submit_button = st.form_submit_button(label="🚀 Procesar y Cargar Datos")

# --- Lógica de Procesamiento (se ejecuta al apretar el botón) ---
if submit_button:
    if uploaded_file is not None and db_host and db_name and db_user and db_pass:
        # Si todos los campos están completos
        with st.spinner('Procesando y conectando a SQL... Esto puede tardar...'):
            
            # Llama a tu función de lógica
            exito, mensaje = procesar_y_guardar_en_sql(
                uploaded_file, db_host, db_name, db_user, db_pass
            )
        
        # Muestra el resultado
        if exito:
            st.success(mensaje)
        else:
            st.error(mensaje)
            
    else:
        st.warning("Por favor, completa TODOS los campos y sube un archivo.")
