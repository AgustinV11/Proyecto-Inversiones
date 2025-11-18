import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import io

# -----------------------------------------------------------------
# 1. LÓGICA DE PROCESAMIENTO (TU CÓDIGO VA AQUÍ)
# -----------------------------------------------------------------
# Esta función es tu script de Pandas.
# Recibe el archivo y las credenciales de SQL.
def procesar_y_guardar_en_sql(archivo_subido, db_host, db_name, db_user, db_pass, table_name, upload_mode):
    """
    Procesa un archivo (CSV o Excel) y lo carga a una base de datos PostgreSQL.
    """
    try:
        # --- A. LECTURA INTELIGENTE DEL ARCHIVO ---
        st.write(f"Leyendo archivo: {archivo_subido.name}...")
        if archivo_subido.name.endswith('.csv'):
            df = pd.read_csv(archivo_subido)
        elif archivo_subido.name.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(archivo_subido)
        else:
            return False, f"❌ Error: Formato de archivo no soportado. Sube un .csv o .xlsx"
        
        # --- B. PROCESAMIENTO CON PANDAS ---
        # 
        #   ¡AQUÍ PEGAS TU CÓDIGO DE PYTHON!
        #
        #   Usa el dataframe 'df' que acabamos de leer.
        #   ... (Tu limpieza de datos)
        #   ... (Tu llamada a la API del dólar MEP)
        #   ... (Tus cálculos de nuevas columnas)
        #
        
        # Al final, tu dataframe final debe llamarse 'df_procesado'
        
        # ----- INICIO DE ESPACIO PARA TU CÓDIGO -----
        
        # (Ejemplo - REEMPLAZA ESTO)
        st.write("Procesando datos (lógica de ejemplo)...")
        df_procesado = df.copy() # Usamos 'copy()' para evitar advertencias
        df_procesado['procesado'] = True 
        
        # ----- FIN DE ESPACIO PARA TU CÓDIGO -----
        
        
        # --- C. CONEXIÓN Y CARGA A SUPABASE (POSTGRESQL) ---
        st.write("Conectando a la base de datos...")
        
        # ¡CRÍTICO! Cadena de conexión para Supabase (PostgreSQL)
        # Incluye la solución ?sslmode=require que encontramos para Power BI
        connection_string = f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}?sslmode=require"

        engine = create_engine(connection_string)

        st.write(f"Cargando datos en la tabla '{table_name}'...")
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
        st.error(f"Error detallado: {e}") # Añadimos más detalle al error
        return False, f"❌ Error: {e}"

# -----------------------------------------------------------------
# 2. LA INTERFAZ WEB (EL FRONT-END)
# -----------------------------------------------------------------
st.set_page_config(layout="centered", page_title="Cargador de Datos")
st.title("🤖 Cargador de Datos a Supabase")

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
