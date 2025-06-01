import streamlit as st
import pandas as pd
import os
import subprocess

from datetime import datetime
from pathlib import Path
from utils.helpers import get_template_dataframe
from utils.validators import validar_semantica
from utils.connection import upload_dataframe


# Diccionario de plantillas
seed_names = {
        "DATOS_FINANCIEROS": "Contiene los registros contables por cuenta, mes y año, con su importe.",
        "CONCEPTOS_BASE": "Define los conceptos contables con reglas de inclusión y exclusión por número de cuenta.",
        "AGRUPACIONES": "Agrupa múltiples conceptos individuales bajo categorías como Revenue o Cost.",
        "CALCULADOS": "Define conceptos calculados a partir de otros conceptos base, por ejemplo Profit."
}

#Rutas
DBT_PROJECT_PATH = "../dbt"
SEEDS_PATH = os.path.join(DBT_PROJECT_PATH, "seeds")
LOG_PATH = "logs"
os.makedirs(LOG_PATH, exist_ok=True)

st.header("📥 Carga de Datos")

#Selección de plantilla
selected_seed = st.selectbox("Plantilla a revisar", list(seed_names.keys()))
st.info(f"📝 **Descripción**: {seed_names[selected_seed]}")

#Botón para descargar plantilla
template_df = get_template_dataframe(selected_seed)
st.download_button(
    "⬇️ Descargar plantilla CSV",
    data=template_df.to_csv(index=False).encode("utf-8"),
    file_name=f"{selected_seed}.csv",
    mime="text/csv"
)

st.markdown("---")

#Subida de archivos
uploaded_file = st.file_uploader(f"🔄 Subir nuevo archivo CSV para la plantilla '{selected_seed}'", type="csv")

if uploaded_file:
    try:
        df_uploaded = pd.read_csv(uploaded_file)

        #Validación de columnas
        expected_columns = template_df.columns.tolist()
        uploaded_columns = df_uploaded.columns.tolist()

        if uploaded_columns != expected_columns:
            st.error(f"❌ Las columnas del archivo no coinciden con las esperadas: {expected_columns}")
        elif df_uploaded.empty:
            st.error("❌ El archivo no contiene registros para validar.")            
        else:
            # Validación semántica
            valid_semantics = validar_semantica(selected_seed, df_uploaded)

            if valid_semantics:
                st.success(f"✅ {df_uploaded.shape[0]} registros cargados.")
                st.dataframe(df_uploaded.head(10), use_container_width=True)
                st.caption("Mostrando los primeros 10 registros.")

                #Botón para cargar a Snowflake
                if st.button("🚀 Cargar a Snowflake (Reemplaza)"):
                    try:                                             
                         # Guardar el archivo CSV en la carpeta seeds
                        file_path = os.path.join(SEEDS_PATH, f"{selected_seed}.csv")
                        os.makedirs(SEEDS_PATH, exist_ok=True)
                        df_uploaded.to_csv(file_path, index=False, encoding="utf-8")
                        st.info(f"📂 Archivo guardado como `{file_path}`.")

                        result = upload_dataframe(df_uploaded, selected_seed)
                        #st.info(f"Result`{result}`.")

                        #Registro del log
                        log_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                        log_file = os.path.join(LOG_PATH, f"seed_{selected_seed}_{log_time}.log")
                        with open(log_file, "w", encoding="utf-8") as f:
                            f.write(f"[{log_time}] ✅ Carga exitosa.\n")           

                        if result:
                            st.success("✅ Tabla actualizada en Snowflake")
                        else:
                            st.error("❌ Error al ejecutar la carga de datos")
                            st.info(f"📄 Detalles guardados en `{log_file}`.")
                    except Exception as e:
                        st.error(f"❌ Error durante la carga: {str(e)}")
            else:
                st.warning("⚠️ La validación semántica falló. Corrige los errores antes de continuar.")

                # Log de error de validación
                log_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                log_file = os.path.join(LOG_PATH, f"validacion_{selected_seed}_{log_time}.log")
                with open(log_file, "w", encoding="utf-8") as f:
                    f.write(f"Validación semántica fallida para plantilla: {selected_seed}\n")
                    f.write(f"Columnas esperadas: {expected_columns}\n")
                    f.write(f"Columnas recibidas: {uploaded_columns}\n")
                    f.write("Contenido del archivo:\n")
                    f.write(df_uploaded.to_csv(index=False))
                st.info(f"📄 Registro de validación guardado en `{log_file}`.")                    

    except Exception as e:
        st.error(f"❌ Error al leer el CSV: {str(e)}")
        log_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(LOG_PATH, f"error_{selected_seed}_{log_time}.log")
        with open(log_file, "w", encoding="utf-8") as f:
            f.write(f"Error al leer archivo CSV para plantilla: {selected_seed}\n")
            f.write(str(e))
        st.info(f"📄 Detalles del error guardados en `{log_file}`.")        
