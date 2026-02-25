
CREATE TABLE IF NOT EXISTS dim_region (
    region_key INTEGER PRIMARY KEY,
    region_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_state (
    state_key INTEGER PRIMARY KEY,
    state_code VARCHAR(10) UNIQUE NOT NULL,
    region_key INTEGER NOT NULL,
    FOREIGN KEY (region_key) REFERENCES dim_region(region_key)
);

CREATE TABLE IF NOT EXISTS dim_city (
    city_key INTEGER PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    state_key INTEGER NOT NULL,
    FOREIGN KEY (state_key) REFERENCES dim_state(state_key)
);

CREATE TABLE IF NOT EXISTS dim_hospital_norm (
    hospital_key INTEGER PRIMARY KEY,
    hospital_id VARCHAR(10) UNIQUE NOT NULL,
    hospital_name VARCHAR(150),
    city_key INTEGER NOT NULL,
    beds INTEGER,
    trauma_level INTEGER,
    FOREIGN KEY (city_key) REFERENCES dim_city(city_key)
);

CREATE TABLE IF NOT EXISTS dim_incident_group (
    incident_group_key INTEGER PRIMARY KEY,
    incident_group_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_incident_type_norm (
    incident_type_key INTEGER PRIMARY KEY,
    incident_type VARCHAR(100) UNIQUE NOT NULL,
    incident_group_key INTEGER,
    severity_default INTEGER,
    FOREIGN KEY (incident_group_key) REFERENCES dim_incident_group(incident_group_key)
);
