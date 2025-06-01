import streamlit as st

st.set_page_config(page_title="Reporte P&L", layout="wide")
st.title("📊 Reporte de Pérdidas y Ganancias (P&L)")

st.markdown("""
    <iframe title="nubitaPBI" width="1000" height="600" 
        src="https://app.powerbi.com/view?r=eyJrIjoiZGQ2OGU4ZGEtNmQyNS00YzY5LTk1YTYtOTNlMjU3MTU0N2M4IiwidCI6ImM0YTY2YzM0LTJiYjctNDUxZi04YmUxLWIyYzI2YTQzMDE1OCIsImMiOjR9" frameborder="0" allowFullScreen="true">
    </iframe>          
""", unsafe_allow_html=True)

st.markdown("Selecciona una opción del menú lateral para comenzar.")