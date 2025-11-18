

# -----------------------------------------------------------------
# 2. LA INTERFAZ WEB (EL FRONT-END)
# -----------------------------------------------------------------
st.set_page_config(layout="centered", page_title="Cargador de Datos")
st.title("Análisis de inversiones")
st.image("https://placehold.co/800x200/4B4B4B/FFFFFF?text=Analizador+de+Inversiones&font=roboto", use_column_width=True) # Banner
st.write("Sube tu reporte del broker y completa los datos de tu Base de Datos de Supabase (PostgreSQL). El proceso tomará los datos, los procesará y los cargará en tu base de datos.")

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



