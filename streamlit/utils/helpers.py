from utils.connection import init_connection
import pandas as pd

def get_template_dataframe(seed_name):
    if seed_name == "DATOS_FINANCIEROS":
        return pd.DataFrame({
            "NUMERO_CUENTA": ["600001"],
            "MES": [1],
            "ANIO": [2025],
            "IMPORTE": [10000.00]
        })
    elif seed_name == "CONCEPTOS_BASE":
        return pd.DataFrame({
            "CONCEPTO": ["Sells"],
            "INCLUSION": ["600*"],
            "EXCLUSION": [""]
        })
    elif seed_name == "AGRUPACIONES":
        return pd.DataFrame({
            "AGRUPACION": ["Revenue"],
            "CONCEPTOS": ["Sells"]
        })
    elif seed_name == "CALCULADOS":
        return pd.DataFrame({
            "CONCEPTO_CALCULADO": ["Profit"],
            "TIPO_CALCULO": ["Resta"],
            "CONCEPTO_1": ["Sells"],
            "CONCEPTO_2": ["Buys;Damages"]
        })