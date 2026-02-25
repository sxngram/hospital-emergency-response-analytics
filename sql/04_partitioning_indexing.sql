
CREATE INDEX IF NOT EXISTS idx_fact_date_key ON fact_emergency_response(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_hospital_key ON fact_emergency_response(hospital_key);
CREATE INDEX IF NOT EXISTS idx_fact_incident_type_key ON fact_emergency_response(incident_type_key);
CREATE INDEX IF NOT EXISTS idx_fact_severity_date ON fact_emergency_response(severity, date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sla_breach ON fact_emergency_response(is_breach_sla);

CREATE TABLE IF NOT EXISTS fact_emergency_response_part (
    response_key BIGINT,
    incident_id BIGINT,
    incident_ts TIMESTAMP,
    date_key INTEGER,
    hospital_key INTEGER,
    incident_type_key INTEGER,
    shift_key INTEGER,
    severity INTEGER,
    patient_count INTEGER,
    triage_minutes INTEGER,
    ambulance_eta_minutes INTEGER,
    response_duration_minutes INTEGER,
    cost_usd NUMERIC(12,2),
    is_breach_sla BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (incident_ts);

CREATE TABLE IF NOT EXISTS fact_emergency_response_2026_01
    PARTITION OF fact_emergency_response_part
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE IF NOT EXISTS fact_emergency_response_2026_02
    PARTITION OF fact_emergency_response_part
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE INDEX IF NOT EXISTS idx_part_2026_01_hospital_ts
    ON fact_emergency_response_2026_01(hospital_key, incident_ts);
