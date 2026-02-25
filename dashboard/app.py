from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "warehouse.db"

st.set_page_config(page_title="Hospital Emergency Response Analytics", layout="wide")
st.title("Hospital Emergency Response Analytics Dashboard")

if not DB_PATH.exists():
    st.warning("warehouse.db not found. Run: python etl/extract_transform_load.py")
    st.stop()

engine = create_engine(f"sqlite:///{DB_PATH}")

query = """
SELECT
    d.full_date,
    h.hospital_name,
    h.city,
    i.incident_type,
    f.severity,
    f.patient_count,
    f.triage_minutes,
    f.response_duration_minutes,
    f.cost_usd,
    f.is_breach_sla
FROM fact_emergency_response f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_hospital h ON f.hospital_key = h.hospital_key
JOIN dim_incident_type i ON f.incident_type_key = i.incident_type_key
"""

_df = pd.read_sql(query, engine)
_df["full_date"] = pd.to_datetime(_df["full_date"])

st.sidebar.header("Filters")
hospital_filter = st.sidebar.multiselect("Hospital", sorted(_df["hospital_name"].unique()))
incident_filter = st.sidebar.multiselect("Incident Type", sorted(_df["incident_type"].unique()))

filtered = _df.copy()
if hospital_filter:
    filtered = filtered[filtered["hospital_name"].isin(hospital_filter)]
if incident_filter:
    filtered = filtered[filtered["incident_type"].isin(incident_filter)]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Incidents", int(filtered.shape[0]))
col2.metric("Avg Response (min)", round(filtered["response_duration_minutes"].mean(), 2))
col3.metric("Avg Triage (min)", round(filtered["triage_minutes"].mean(), 2))
col4.metric("SLA Breach %", f"{round(filtered['is_breach_sla'].mean()*100, 2)}%")

trend = (
    filtered.groupby("full_date", as_index=False)
    .agg(
        incident_count=("incident_type", "count"),
        avg_response=("response_duration_minutes", "mean"),
        avg_triage=("triage_minutes", "mean"),
    )
)

fig_trend = px.line(
    trend,
    x="full_date",
    y=["incident_count", "avg_response", "avg_triage"],
    title="Daily Emergency Trends",
)
st.plotly_chart(fig_trend, use_container_width=True)

by_hospital = (
    filtered.groupby("hospital_name", as_index=False)
    .agg(total_cost=("cost_usd", "sum"), incidents=("incident_type", "count"))
    .sort_values("total_cost", ascending=False)
)
fig_cost = px.bar(
    by_hospital,
    x="hospital_name",
    y="total_cost",
    color="incidents",
    title="Cost by Hospital",
)
st.plotly_chart(fig_cost, use_container_width=True)

fig_scatter = px.scatter(
    filtered,
    x="triage_minutes",
    y="response_duration_minutes",
    color="incident_type",
    size="cost_usd",
    hover_data=["hospital_name", "severity"],
    title="Triage vs Response Duration",
)
st.plotly_chart(fig_scatter, use_container_width=True)

st.subheader("Detailed Records")
st.dataframe(filtered.sort_values("full_date", ascending=False), use_container_width=True)
