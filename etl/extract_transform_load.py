from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from config import CURATED_DIR, RAW_DIR, SLA_THRESHOLD_MINUTES, WAREHOUSE_DB


def _load_raw() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    incidents = pd.read_csv(RAW_DIR / "incidents.csv", escapechar="\\")
    for col in ["incident_ts", "response_start_ts", "response_end_ts"]:
        incidents[col] = pd.to_datetime(incidents[col], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if incidents[["incident_ts", "response_start_ts", "response_end_ts"]].isna().any().any():
        raise ValueError("Timestamp parsing failed for one or more incident records.")
    hospitals = pd.read_csv(RAW_DIR / "hospitals.csv")
    staff = pd.read_csv(RAW_DIR / "staff.csv")
    return incidents, hospitals, staff


def _build_date_dim(incidents: pd.DataFrame) -> pd.DataFrame:
    dates = pd.DataFrame({"full_date": incidents["incident_ts"].dt.date.drop_duplicates().sort_values()})
    dates["date_key"] = dates["full_date"].astype(str).str.replace("-", "").astype(int)
    dates["day_of_week"] = pd.to_datetime(dates["full_date"]).dt.day_name()
    dates["month_number"] = pd.to_datetime(dates["full_date"]).dt.month
    dates["month_name"] = pd.to_datetime(dates["full_date"]).dt.month_name()
    dates["quarter_number"] = pd.to_datetime(dates["full_date"]).dt.quarter
    dates["year_number"] = pd.to_datetime(dates["full_date"]).dt.year
    return dates[[
        "date_key", "full_date", "day_of_week", "month_number",
        "month_name", "quarter_number", "year_number"
    ]]


def _build_shift_dim() -> pd.DataFrame:
    return pd.DataFrame([
        {"shift_key": 1, "shift_name": "Night", "shift_start_hour": 0, "shift_end_hour": 7},
        {"shift_key": 2, "shift_name": "Day", "shift_start_hour": 8, "shift_end_hour": 15},
        {"shift_key": 3, "shift_name": "Evening", "shift_start_hour": 16, "shift_end_hour": 23},
    ])


def _assign_shift(hour: int) -> int:
    if 0 <= hour <= 7:
        return 1
    if 8 <= hour <= 15:
        return 2
    return 3


def _build_incident_type_dim(incidents: pd.DataFrame) -> pd.DataFrame:
    unique_types = sorted(incidents["incident_type"].unique())
    rows = []
    for i, incident_type in enumerate(unique_types, start=1):
        severity_default = int(incidents.loc[incidents["incident_type"] == incident_type, "severity"].median())
        rows.append({
            "incident_type_key": i,
            "incident_type": incident_type,
            "severity_default": severity_default,
        })
    return pd.DataFrame(rows)


def _build_hospital_dim(hospitals: pd.DataFrame) -> pd.DataFrame:
    dim = hospitals.copy()
    dim = dim.reset_index(drop=True)
    dim["hospital_key"] = dim.index + 1
    return dim[[
        "hospital_key", "hospital_id", "hospital_name", "city", "state", "region", "beds", "trauma_level"
    ]]


def _build_fact(
    incidents: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_hospital: pd.DataFrame,
    dim_incident_type: pd.DataFrame,
) -> pd.DataFrame:
    fact = incidents.copy()
    fact["date_key"] = fact["incident_ts"].dt.strftime("%Y%m%d").astype(int)
    fact["shift_key"] = fact["incident_ts"].dt.hour.apply(_assign_shift)
    fact["response_duration_minutes"] = (
        (fact["response_end_ts"] - fact["response_start_ts"]).dt.total_seconds() / 60
    ).astype(int)
    fact["is_breach_sla"] = fact["response_duration_minutes"] > SLA_THRESHOLD_MINUTES

    fact = fact.merge(
        dim_hospital[["hospital_key", "hospital_id"]],
        how="left",
        on="hospital_id",
    )
    fact = fact.merge(
        dim_incident_type[["incident_type_key", "incident_type"]],
        how="left",
        on="incident_type",
    )

    fact["response_key"] = range(1, len(fact) + 1)
    fact["metadata_json"] = fact["metadata_json"].apply(lambda v: json.dumps(json.loads(v)))

    return fact[[
        "response_key", "incident_id", "date_key", "hospital_key", "incident_type_key", "shift_key",
        "severity", "patient_count", "triage_minutes", "ambulance_eta_minutes",
        "response_duration_minutes", "cost_usd", "is_breach_sla", "metadata_json"
    ]]


def _persist_curated(
    dim_date: pd.DataFrame,
    dim_hospital: pd.DataFrame,
    dim_incident_type: pd.DataFrame,
    dim_shift: pd.DataFrame,
    fact: pd.DataFrame,
    staff: pd.DataFrame,
) -> None:
    CURATED_DIR.mkdir(parents=True, exist_ok=True)

    dim_date.to_parquet(CURATED_DIR / "dim_date.parquet", index=False)
    dim_hospital.to_parquet(CURATED_DIR / "dim_hospital.parquet", index=False)
    dim_incident_type.to_parquet(CURATED_DIR / "dim_incident_type.parquet", index=False)
    dim_shift.to_parquet(CURATED_DIR / "dim_shift.parquet", index=False)
    fact.to_parquet(CURATED_DIR / "fact_emergency_response.parquet", index=False)
    staff.to_parquet(CURATED_DIR / "staff.parquet", index=False)


def _load_warehouse(
    dim_date: pd.DataFrame,
    dim_hospital: pd.DataFrame,
    dim_incident_type: pd.DataFrame,
    dim_shift: pd.DataFrame,
    fact: pd.DataFrame,
    staff: pd.DataFrame,
) -> None:
    engine = create_engine(f"sqlite:///{WAREHOUSE_DB}")
    dim_date.to_sql("dim_date", engine, if_exists="replace", index=False)
    dim_hospital.to_sql("dim_hospital", engine, if_exists="replace", index=False)
    dim_incident_type.to_sql("dim_incident_type", engine, if_exists="replace", index=False)
    dim_shift.to_sql("dim_shift", engine, if_exists="replace", index=False)
    fact.to_sql("fact_emergency_response", engine, if_exists="replace", index=False)
    staff.to_sql("stg_staff", engine, if_exists="replace", index=False)

    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_fact_date_key ON fact_emergency_response(date_key)")
        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_fact_hospital_key ON fact_emergency_response(hospital_key)")
        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_fact_incident_type_key ON fact_emergency_response(incident_type_key)")


def main() -> None:
    incidents, hospitals, staff = _load_raw()

    dim_date = _build_date_dim(incidents)
    dim_hospital = _build_hospital_dim(hospitals)
    dim_incident_type = _build_incident_type_dim(incidents)
    dim_shift = _build_shift_dim()
    fact = _build_fact(incidents, dim_date, dim_hospital, dim_incident_type)

    _persist_curated(dim_date, dim_hospital, dim_incident_type, dim_shift, fact, staff)
    _load_warehouse(dim_date, dim_hospital, dim_incident_type, dim_shift, fact, staff)

    print(f"Curated data written to: {CURATED_DIR}")
    print(f"Local warehouse updated: {WAREHOUSE_DB}")
    print(f"Fact rows: {len(fact)}")


if __name__ == "__main__":
    main()
