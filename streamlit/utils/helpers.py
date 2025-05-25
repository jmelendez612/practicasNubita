from utils.connection import init_connection
import pandas as pd

def get_template_dataframe(seed_name):
    if seed_name == "datos_financieros":
        return pd.DataFrame({
            "numero_cuenta": ["600001"],
            "mes": [1],
            "anio": [2025],
            "importe": [10000.00]
        })
    elif seed_name == "conceptos_base":
        return pd.DataFrame({
            "concepto": ["Sells"],
            "inclusion": ["600*"],
            "exclusion": [""]
        })
    elif seed_name == "agrupaciones":
        return pd.DataFrame({
            "agrupacion": ["Revenue"],
            "conceptos": ["Sells"]
        })
    elif seed_name == "calculados":
        return pd.DataFrame({
            "concepto_calculado": ["Profit"],
            "tipo_calculo": ["Resta"],
            "concepto_1": ["Sells"],
            "concepto_2": ["Buys;Damages"]
        })