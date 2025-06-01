import streamlit as st

st.set_page_config(page_title="Reporte P&L", layout="wide")
st.title("📊 Reporte de Pérdidas y Ganancias (P&L)")


st.markdown("""
    <iframe width="1000" height="600"
    src="https://app.powerbi.com/reportEmbed?reportId=24064fa2-8056-4eec-8a4a-96f3652889fc&autoAuth=true&ctid=02674ef9-5c2c-47d0-b2bc-595a784dda00&actionBarEnabled=true"
    frameborder="0" allowFullScreen="true"></iframe>            
""", unsafe_allow_html=True)



st.markdown("Selecciona una opción del menú lateral para comenzar.")