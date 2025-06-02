import pandas as pd
import streamlit as st

def validar_semantica(seed_name: str, df: pd.DataFrame) -> bool:
    valid = True

    if seed_name == "AGRUPACIONES": #Nota para mejorar para que no esté hardcodeado#JM
        for idx, row in df.iterrows():
            if pd.isna(row["AGRUPACION"]) or pd.isna(row["CONCEPTOS"]):
                st.error(f"❌ Fila {idx+2}: Todos los campos son obligatorios.")
                valid = False
                break

    elif seed_name == "CONCEPTOS_BASE":
        for idx, row in df.iterrows():
            if pd.isna(row["CONCEPTO"]) or pd.isna(row["INCLUSION"]):
                st.error(f"❌ Fila {idx+2}: Todos los campos son obligatorios.")
                valid = False
                break

    elif seed_name == "CALCULADOS":
        for idx, row in df.iterrows():
            if (pd.isna(row["CONCEPTO_CALCULADO"]) or pd.isna(row["TIPO_CALCULO"]) or pd.isna(row["CONCEPTO_SUMA"]) or pd.isna(row["CONCEPTO_RESTA"])):
                st.error(f"❌ Fila {idx+2}: Todos los campos son obligatorios.")
                valid = False
                break

    elif seed_name == "DATOS_FINANCIEROS":
        for idx, row in df.iterrows():
            if (pd.isna(row["NUMERO_CUENTA"]) or pd.isna(row["MES"]) or pd.isna(row["ANIO"]) or pd.isna(row["IMPORTE"])):
                st.error(f"❌ Fila {idx+2}: Todos los campos son obligatorios.")
                valid = False
                break
            try:
                int(row["MES"])
                int(row["ANIO"])
                imp = float(row["IMPORTE"])
                if not imp.is_integer():
                    st.warning(f"⚠️ Fila {idx+2}: El IMPORTE tiene decimales, lo cual está inhabilitado por ahora.")  #Nota para habilitar decimales#JM
                    valid = False
                    break
            except ValueError:
                st.error(f"❌ Fila {idx+2}: 'MES', 'ANIO' e 'IMPORTE' deben ser números enteros.")
                valid = False
                break

    return valid
