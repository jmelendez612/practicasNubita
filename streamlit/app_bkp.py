import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Cargar variables del archivo .env
load_dotenv()

# Conexión a Snowflake usando SQLAlchemy
def init_connection():
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    role = os.getenv("SNOWFLAKE_ROLE")

    url = (
        f"snowflake://{user}:{password}@{account}/"
        f"{database}/{schema}?warehouse={warehouse}&role={role}"
    )
    return create_engine(url)

# Ejecutar consulta
def load_data(engine):
    query = "SELECT * FROM pnl_final"
    return pd.read_sql(query, engine)

# Streamlit UI
st.title("Reporte Pérdidas y Ganancias (P&L)")

try:
    engine = init_connection()
    df = load_data(engine)
    st.success("Datos cargados correctamente desde Snowflake.")
    st.dataframe(df)
except Exception as e:
    st.error(f"Error al cargar datos: {e}")