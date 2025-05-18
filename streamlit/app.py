import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from st_aggrid import AgGrid, GridOptionsBuilder
import os
import subprocess

# Configurar la página
st.set_page_config(page_title="Reporte P&L", layout="wide")
st.title("📊 Reporte de Pérdidas y Ganancias (P&L)")

# Cargar variables del archivo .env
# --- CARGAR VARIABLES DEL ARCHIVO .env ---
load_dotenv()

# --- CONEXIÓN A SNOWFLAKE ---
@st.cache_resource
def init_connection():
    engine = create_engine(
        f"snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}@{os.getenv('SNOWFLAKE_ACCOUNT')}/"
        f"{os.getenv('SNOWFLAKE_DATABASE')}/{os.getenv('SNOWFLAKE_SCHEMA')}?warehouse={os.getenv('SNOWFLAKE_WAREHOUSE')}"
    )
    return engine.connect()
conn = init_connection()

# --- CONSULTAR DATOS DASH ---
@st.cache_data
def load_data():
    query = "SELECT * FROM PNL_FINAL"
    return pd.read_sql(query, conn)

df = load_data()
df['mes'] = df['mes'].astype(int)
df['anio'] = df['anio'].astype(int)

#Funciones
def load_seed_table(seed_name):
    query = f"SELECT * FROM {seed_name}"
    return pd.read_sql(query, conn)

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

# --- ORGANIZACIÓN EN PESTAÑAS ---
tab1, tab2, tab3 = st.tabs(["📥 Carga de Datos", "📊 Reporte P&L", "⬇️ Exportar CSV"])

# --- PESTAÑA 1: CARGA DE DATOS --
with tab1:
    st.subheader("📥 Carga de datos")

    # Diccionario de nombres de seed y su descripción
    seed_names = {
        "datos_financieros": "Contiene los registros contables por cuenta, mes y año, con su importe.",
        "conceptos_base": "Define los conceptos contables con reglas de inclusión y exclusión por número de cuenta.",
        "agrupaciones": "Agrupa múltiples conceptos individuales bajo categorías como Revenue o Cost.",
        "calculados": "Define conceptos calculados a partir de otros conceptos base, por ejemplo Profit."
    }

    # Selección del seed
    selected_seed = st.selectbox("Selecciona la plantilla a revisar y obtén una vista previa de los datos cargados en Snowflake.", list(seed_names.keys()))
    st.info(f"📝 **Descripción**: {seed_names[selected_seed]}")

    # Botón para descargar plantilla CSV
    template_df = get_template_dataframe(selected_seed)
    csv_bytes = template_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="⬇️ Descargar plantilla CSV",
        data=csv_bytes,
        file_name=f"{selected_seed}_plantilla.csv",
        mime="text/csv"
    )

    # Cargar la tabla correspondiente desde Snowflake
    # seed_df = load_seed_table(selected_seed)
    # st.dataframe(seed_df.head(10), use_container_width=True)
    # st.caption("Mostrando los primeros 10 registros.")

    st.markdown("---")

    #Subir archivo CSV
    DBT_PATH = "../dbt"  # Ajusta si tu carpeta dbt está en otra ruta
    DBT_DATA_PATH = os.path.join(DBT_PATH, "seeds")

    uploaded_file = st.file_uploader(f"🔄 Subir nuevo archivo CSV para la plantilla '{selected_seed}'", type="csv")
    if uploaded_file:
        try:
            df_uploaded = pd.read_csv(uploaded_file)
            st.success(f"✅ Archivo cargado correctamente con {df_uploaded.shape[0]} registros. Vista previa:")

            st.dataframe(df_uploaded.head(10), use_container_width=True)
            st.caption("Mostrando los primeros 10 registros.")

            # Botón para reemplazar tabla
            if st.button("🚀 Cargar a Snowflake (reemplazar tabla existente)"):

                file_path = os.path.join(DBT_DATA_PATH, f"{selected_seed}.csv")
                df_uploaded.to_csv(file_path, index=False, encoding="utf-8")
                st.info(f"📂 Archivo guardado como `{file_path}`.")

                result = subprocess.run(
                ["dbt", "seed", "--select", selected_seed],
                cwd=DBT_PATH,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
                
                if result.returncode == 0:
                    st.success("✅ Tabla actualizada exitosamente en Snowflake con dbt seed.")
                else:
                    st.error("❌ Ocurrió un error al ejecutar `dbt seed`.")
                    st.text(result.stderr)                

            #df_uploaded.to_sql(name=selected_seed, con=conn, index=False, if_exists="replace")
            #st.success(f"✅ La tabla `{selected_seed}` fue actualizada correctamente en Snowflake.")

        except Exception as e:
            st.error(f"❌ Error al leer el archivo CSV: {str(e)}")

# --- PESTAÑA 2: VISUALIZACIÓN P&L ---
with tab2:
    st.subheader("Visualización del Reporte P&L")
    
    
    col1, col2 = st.columns(2)
    with col1:
        selected_year = st.selectbox("Selecciona el Año", sorted(df['anio'].unique(), reverse=True))
    with col2:
        selected_month = st.selectbox("Selecciona el Mes", sorted(df['mes'].unique()))

    filtered_df = df[(df['anio'] == selected_year) & (df['mes'] == selected_month)]

    st.markdown("### Resumen")
    kpi1, kpi2, kpi3 = st.columns(3)
    monto_total = filtered_df['importe'].sum()
    monto_ingresos = filtered_df[filtered_df['agrupacion'] == 'Revenue']['importe'].sum()
    monto_costos = filtered_df[filtered_df['agrupacion'] == 'Operating Cost']['importe'].sum()

    kpi1.metric("Total", f"S/. {monto_total:,.2f}")
    kpi2.metric("Ingresos", f"S/. {monto_ingresos:,.2f}")
    kpi3.metric("Costos", f"S/. {monto_costos:,.2f}")

    st.markdown("### Detalle")
    st.dataframe(filtered_df, use_container_width=True)

    st.markdown("---")
    
    df_pnl = load_data()

    if df_pnl.empty:
        st.warning("⚠️ No se encontraron datos en la tabla `PNL_FINAL`.")
    else:
        # Crear columna "periodo" en formato "Mes Año" (Ej: Ene 2024)
        df_pnl["periodo"] = pd.to_datetime(df_pnl["anio"].astype(str) + "-" + df_pnl["mes"].astype(str) + "-01")
        df_pnl["periodo"] = df_pnl["periodo"].dt.strftime("%b %Y")  

        # Agrupar por agrupación, concepto y periodo
        grouped = df_pnl.groupby(["agrupacion", "concepto", "periodo"])["importe"].sum().reset_index()

        # Pivotear: filas → agrupación/concepto, columnas → meses
        pivot_df = grouped.pivot(index=["agrupacion", "concepto"], columns="periodo", values="importe").fillna(0).reset_index()

        # Configurar tabla jerárquica
        gb = GridOptionsBuilder.from_dataframe(pivot_df)
        gb.configure_default_column(groupable=True, resizable=True)
        gb.configure_column("agrupacion", rowGroup=True, hide=True)
        gb.configure_column("concepto", rowGroup=False)
                            
        gb.configure_grid_options(domLayout='normal', groupIncludeFooter=True, groupIncludeTotalFooter=True)
        grid_options = gb.build()

        # Mostrar tabla con AgGrid
        AgGrid(
            pivot_df,
            gridOptions=grid_options,
            enable_enterprise_modules=True,
            use_container_width=True,
            fit_columns_on_grid_load=True
        )    

# --- PESTAÑA 3: EXPORTACIÓN ---
with tab3:
    st.subheader("Exportar Reporte")
    st.markdown("Puedes descargar el reporte del mes y año seleccionados en formato CSV.")
    st.download_button(
        label="⬇️ Descargar en CSV",
        data=filtered_df.to_csv(index=False).encode('utf-8'),
        file_name=f"PNL_{selected_year}_{selected_month}.csv",
        mime='text/csv'
    )