import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import io

# -----------------------------------------------------------------
# 1. LÓGICA DE PROCESAMIENTO (LA PEGARÁS LUEGO)
# -----------------------------------------------------------------
# Esta función es tu script de Pandas.
# Recibe el archivo y las credenciales de SQL.
# Añadimos 'table_name' y 'upload_mode' como argumentos
def procesar_y_guardar_en_sql(archivo_subido, db_host, db_name, db_user, db_pass, table_name, upload_mode):
    try:
        # --- A. LECTURA INTELIGENTE DEL ARCHIVO ---
        # Leemos el archivo dependiendo de su extensión
        if archivo_subido.name.endswith('.csv'):
            df = pd.read_csv(archivo_subido)
        elif archivo_subido.name.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(archivo_subido)
        else:
            return False, f"❌ Error: Formato de archivo no soportado. Sube un .csv o .xlsx"
        
        # --- B. PROCESAMIENTO CON PANDAS ---
        # ... (Aquí pegarás tu código de limpieza de Pandas) ...
        # ... (Aquí va tu código para obtener el dólar MEP, etc.) ...
        
        # Por ahora, usamos el df original como 'procesado'
        # Reemplaza esto con tu dataframe final
        df_procesado = df 
        
        
        # --- C. CONEXIÓN Y CARGA A SUPABASE (POSTGRESQL) ---
        
        # ¡¡CRÍTICO!! Esta es la cadena de conexión para Supabase (PostgreSQL)
        # Incluye la solución ?sslmode=require que encontramos para Power BI.
        connection_string = f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}?sslmode=require"

        # (La cadena de SQL Server que tenías no aplica para Supabase)
        # connection_string = f"mssql+pyodbc://{db_user}:{db_pass}@{db_host}:1433/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"

        engine = create_engine(connection_string)

        # Carga el dataframe a la base de datos usando los parámetros
        df_procesado.to_sql(
            table_name,
            con=engine,
            if_exists=upload_mode, # 'append' o 'replace'
            index=False
        )

        return True, f"¡Éxito! Se cargaron {len(df_procesado)} filas en la tabla '{table_name}'."

    except Exception as e:
        # Si algo falla, devuelve el error
        return False, f"❌ Error: {e}"

# -----------------------------------------------------------------
# 2. LA INTERFAZ WEB (EL FRONT-END)
# -----------------------------------------------------------------
st.set_page_config(layout="centered", page_title="Cargador de Datos")
st.title("🤖 Cargador de Datos a Supabase")

st.write("Sube tu reporte del broker y completa los datos de tu Base de Datos de Supabase (PostgreSQL).")

# --- Formulario de Carga ---
with st.form(key="upload_form"):
    
    # A. El cargador de archivos
    uploaded_file = st.file_uploader("1. Sube tu archivo (CSV o Excel)", type=["csv", "xlsx", "xls"])
    
    st.divider()
    
    # B. Las credenciales de la DB
    st.subheader("2. Credenciales de tu Base de Datos (Supabase)")
    col1, col2 = st.columns(2)
    with col1:
        db_host = st.text_input("Host (Servidor)", placeholder="db.xxxxxxxx.supabase.co")
        db_user = st.text_input("Usuario", "postgres")
    with col2:
        db_name = st.text_input("Nombre de la Base de Datos", "postgres")
        db_pass = st.text_input("Contraseña", type="password")

    st.divider()

    # C. Opciones de Carga
    st.subheader("3. Opciones de Carga")
    col3, col4 = st.columns(2)
    with col3:
        table_name = st.text_input("Nombre de la Tabla en SQL", "mis_datos_procesados")
    with col4:
        # Damos a elegir al usuario para evitar duplicar datos por error
        upload_mode_label = st.selectbox("Modo de Carga", 
                                        ["Añadir (append)", "Reemplazar (replace)"])

    st.divider()

    # D. El botón de envío
    submit_button = st.form_submit_button(label="🚀 Procesar y Cargar Datos")

# --- Lógica de Procesamiento (se ejecuta al apretar el botón) ---
if submit_button:
    
    # Mapeamos la etiqueta del selectbox al valor real de pandas
    upload_mode_map = {
        "Añadir (append)": "append",
        "Reemplazar (replace)": "replace"
    }
    upload_mode = upload_mode_map[upload_mode_label]
    
    # Verificamos que todos los campos estén completos
    if uploaded_file is not None and db_host and db_name and db_user and db_pass and table_name:
        
        with st.spinner('Procesando archivo y conectando a Supabase...'):
            
            # Llama a tu función de lógica
            exito, mensaje = procesar_y_guardar_en_sql(
                uploaded_file, db_host, db_name, db_user, db_pass, table_name, upload_mode
            )
        
        # Muestra el resultado
        if exito:
            st.success(mensaje)
        else:
            st.error(mensaje)
            
    else:
        st.warning("Por favor, completa TODOS los campos y sube un archivo.")
