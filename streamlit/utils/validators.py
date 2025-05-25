import pandas as pd
import streamlit as st

def validar_semantica(seed_name: str, df: pd.DataFrame) -> bool:
    valid = True

    if seed_name == "agrupaciones": #Nota para mejorar para que no esté hardcodeado#JM
        for idx, row in df.iterrows():
            if pd.isna(row["agrupacion"]) or pd.isna(row["conceptos"]):
                st.error(f"❌ Fila {idx+2}: Todos los campos son obligatorios.")
                valid = False
                break

    elif seed_name == "conceptos_base":
        for idx, row in df.iterrows():
            if pd.isna(row["concepto"]) or pd.isna(row["inclusion"]):
                st.error(f"❌ Fila {idx+2}: Todos los campos son obligatorios.")
                valid = False
                break

    elif seed_name == "calculados":
        st.warning("⚠️ La validación semántica para esta plantilla está deshabilitada temporalmente.")
        valid = False

    elif seed_name == "datos_financieros":
        for idx, row in df.iterrows():
            if (pd.isna(row["numero_cuenta"]) or pd.isna(row["mes"]) or pd.isna(row["anio"]) or pd.isna(row["importe"])):
                st.error(f"❌ Fila {idx+2}: Todos los campos son obligatorios.")
                valid = False
                break
            try:
                int(row["mes"])
                int(row["anio"])
                imp = float(row["importe"])
                if not imp.is_integer():
                    st.warning(f"⚠️ Fila {idx+2}: El importe tiene decimales, lo cual está inhabilitado por ahora.")  #Nota para habilitar decimales#JM
                    valid = False
                    break
            except ValueError:
                st.error(f"❌ Fila {idx+2}: 'mes', 'anio' e 'importe' deben ser números enteros.")
                valid = False
                break

    return valid
