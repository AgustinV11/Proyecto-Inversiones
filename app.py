import streamlit as st
import pandas as pd
import openpyxl  # Importante para que Pandas pueda leer .xlsx

# Título de la aplicación web
st.title("📊 Analizador de Reportes")
st.write("Sube tu archivo (CSV o Excel) para que Pandas lo procese.")

# Widget para subir archivos
uploaded_file = st.file_uploader(
    "Selecciona tu archivo:",
    type=["csv", "xlsx"]  # Acepta solo estos formatos
)

# Esto solo se ejecuta si el usuario ha subido un archivo
if uploaded_file is not None:
    
    st.info(f"Archivo subido: {uploaded_file.name}")
    
    try:
        # Lógica para leer el archivo correcto
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            # openpyxl es necesario para que read_excel funcione
            df = pd.read_excel(uploaded_file, engine='openpyxl')

        # Si todo sale bien...
        st.success("¡Pandas leyó el archivo exitosamente!")
        
        st.subheader("Primeras 10 filas de tus datos:")
        st.dataframe(df.head(10)) # Muestra un DataFrame interactivo

    except Exception as e:
        # Si Pandas falla
        st.error(f"Ocurrió un error al leer el archivo: {e}")