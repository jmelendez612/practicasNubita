import streamlit as st
import pandas as pd
from utils.connection import init_connection, load_data

st.header("⬇️ Exportar Reporte")
st.markdown("Puedes descargar el reporte del mes y año seleccionados en formato CSV.")

#Conexión a DB
try:
    #df = load_data()

    #df["mes"] = df["mes"].astype(int)
    #df["anio"] = df["anio"].astype(int)
    df = pd.DataFrame(columns=["anio", "mes"])

except Exception as e:
    st.error("❌ No se pudo conectar a la base de datos o cargar los datos.")
    st.exception(e)
    df = pd.DataFrame(columns=["anio", "mes"])

#Armado de los filtros
col1, col2 = st.columns(2)
with col1:
    selected_year = st.selectbox("Año", sorted(df["anio"].unique(), reverse=True))
with col2:
    selected_month = st.selectbox("Mes", sorted(df["mes"].unique()))

#Filtro de datos
if not df.empty:
    filtered_df = df[(df["anio"] == selected_year) & (df["mes"] == selected_month)]
else:
    filtered_df = pd.DataFrame()

#Botón de descarga
if not filtered_df.empty:
    st.download_button(
        label="⬇️ Descargar en CSV",
        data=filtered_df.to_csv(index=False).encode("utf-8"),
        file_name=f"PNL_{selected_year}_{selected_month}.csv",
        mime="text/csv"
    )
else:
    st.info("No hay datos disponibles para exportar.")