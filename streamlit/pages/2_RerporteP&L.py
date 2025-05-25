import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder
from utils.connection import init_connection, load_data

st.header("📊 Reporte P&L")

#Conexión BD
try:
    df = load_data()

    df["mes"] = df["mes"].astype(int)
    df["anio"] = df["anio"].astype(int)
    #df = pd.DataFrame(columns=["anio", "mes"])
    
except Exception as e:
    st.error("❌ No se pudo conectar a la base de datos o cargar los datos.")
    st.exception(e)
    df = pd.DataFrame(columns=["anio", "mes"])

col1, col2 = st.columns(2)
with col1:
    selected_year = st.selectbox("Año", sorted(df["anio"].unique(), reverse=True))
with col2:
    selected_month = st.selectbox("Mes", sorted(df["mes"].unique()))

filtered_df = df[(df["anio"] == selected_year) & (df["mes"] == selected_month)]

# Filtros por agrupación y concepto
agrupaciones = filtered_df["agrupacion"].dropna().unique().tolist()
conceptos = filtered_df["concepto"].dropna().unique().tolist()

col1, col2 = st.columns(2)
with col1:
    selected_agrupacion = st.selectbox("Filtrar por Agrupación", ["Todos"] + agrupaciones)
with col2:
    selected_concepto = st.selectbox("Filtrar por Concepto", ["Todos"] + conceptos)

if selected_agrupacion != "Todos":
    filtered_df = filtered_df[filtered_df["agrupacion"] == selected_agrupacion]
if selected_concepto != "Todos":
    filtered_df = filtered_df[filtered_df["concepto"] == selected_concepto]

st.markdown("### Resumen")

#KPIs
k1, k2, k3 = st.columns(3)
total = filtered_df["importe"].sum()
revenues = filtered_df[filtered_df["agrupacion"] == "Revenue"]["importe"].sum()
costs = filtered_df[filtered_df["agrupacion"] == "Operating Cost"]["importe"].sum()

k1.metric("Total", f"S/. {total:,.2f}")
k2.metric("Ingresos", f"S/. {revenues:,.2f}")
k3.metric("Costos", f"S/. {costs:,.2f}")

# Tabla detallada
st.markdown("### Detalle")
st.dataframe(
    #filtered_df,
    filtered_df.style.format({"importe": "S/. {:,.2f}"}),
    use_container_width=True
    )

st.markdown("---")
if df.empty:
    st.warning("⚠️ No se encontraron datos en la tabla `PNL_FINAL`.")
else:
    # Crear columna "periodo" en formato "Mes Año" (Ej: Ene 2024)
    df["periodo"] = pd.to_datetime(df["anio"].astype(str) + "-" + df["mes"].astype(str) + "-01")
    df["periodo"] = df["periodo"].dt.strftime("%b %Y")  

    # Agrupar por agrupación, concepto y periodo
    grouped = df.groupby(["agrupacion", "concepto", "periodo"])["importe"].sum().reset_index()

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