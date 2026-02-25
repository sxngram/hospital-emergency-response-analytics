
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    month_number INTEGER,
    month_name VARCHAR(10),
    quarter_number INTEGER,
    year_number INTEGER
);

CREATE TABLE IF NOT EXISTS dim_hospital (
    hospital_key INTEGER PRIMARY KEY,
    hospital_id VARCHAR(10) UNIQUE NOT NULL,
    hospital_name VARCHAR(150),
    city VARCHAR(100),
    state VARCHAR(50),
    region VARCHAR(50),
    beds INTEGER,
    trauma_level INTEGER
);

CREATE TABLE IF NOT EXISTS dim_incident_type (
    incident_type_key INTEGER PRIMARY KEY,
    incident_type VARCHAR(100) UNIQUE NOT NULL,
    severity_default INTEGER
);

CREATE TABLE IF NOT EXISTS dim_shift (
    shift_key INTEGER PRIMARY KEY,
    shift_name VARCHAR(20),
    shift_start_hour INTEGER,
    shift_end_hour INTEGER
);

CREATE TABLE IF NOT EXISTS fact_emergency_response (
    response_key BIGINT PRIMARY KEY,
    incident_id BIGINT NOT NULL,
    date_key INTEGER NOT NULL,
    hospital_key INTEGER NOT NULL,
    incident_type_key INTEGER NOT NULL,
    shift_key INTEGER NOT NULL,
    severity INTEGER,
    patient_count INTEGER,
    triage_minutes INTEGER,
    ambulance_eta_minutes INTEGER,
    response_duration_minutes INTEGER,
    cost_usd NUMERIC(12,2),
    is_breach_sla BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (hospital_key) REFERENCES dim_hospital(hospital_key),
    FOREIGN KEY (incident_type_key) REFERENCES dim_incident_type(incident_type_key),
    FOREIGN KEY (shift_key) REFERENCES dim_shift(shift_key)
);
