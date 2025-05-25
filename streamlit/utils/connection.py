import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def init_connection():
    engine = create_engine(
        f"snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}@{os.getenv('SNOWFLAKE_ACCOUNT')}/"
        f"{os.getenv('SNOWFLAKE_DATABASE')}/{os.getenv('SNOWFLAKE_SCHEMA')}?warehouse={os.getenv('SNOWFLAKE_WAREHOUSE')}"
    )
    return engine.connect()

def load_data():
    conn = init_connection()
    query = "SELECT * FROM PNL_FINAL"
    return pd.read_sql(query, conn)