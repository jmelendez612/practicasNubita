import os
import pandas as pd
import snowflake.connector

from sqlalchemy import create_engine
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

def init_connection():
    engine = create_engine(
        f"snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}@"
        f"{os.getenv('SNOWFLAKE_ACCOUNT')}/"
        f"{os.getenv('SNOWFLAKE_DATABASE')}/{os.getenv('SNOWFLAKE_SCHEMA')}?"
        f"warehouse={os.getenv('SNOWFLAKE_WAREHOUSE')}&role={os.getenv('SNOWFLAKE_ROLE')}"
    )
    return engine.connect()

def init_connection_snowflake():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )
    return conn

def load_data():
    conn = init_connection()
    query = "SELECT * FROM STG_DATOS_DETALLADOS"
    return pd.read_sql(query, conn)


def upload_dataframe(df: pd.DataFrame, table_name: str, overwrite: bool = True) -> bool:
    conn = init_connection_snowflake()
    try:
        cs = conn.cursor()
        if overwrite:
            # Borra datos previos
            cs.execute(f"TRUNCATE TABLE IF EXISTS {table_name.upper()}")
        # Usa write_pandas para carga eficiente
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
        return success
    except Exception as e:
        print(f"Error al cargar DataFrame: {e}")
        return False
    finally:
        cs.close()
        conn.close()