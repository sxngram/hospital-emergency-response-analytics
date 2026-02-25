
CREATE OR REPLACE WAREHOUSE ER_ANALYTICS_WH
  WITH WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE DATABASE HOSPITAL_ER_DB;
CREATE OR REPLACE SCHEMA HOSPITAL_ER_DB.RAW;
CREATE OR REPLACE SCHEMA HOSPITAL_ER_DB.CORE;
CREATE OR REPLACE SCHEMA HOSPITAL_ER_DB.MART;

USE DATABASE HOSPITAL_ER_DB;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE INCIDENTS_RAW (
  incident_id NUMBER,
  incident_ts TIMESTAMP_NTZ,
  hospital_id STRING,
  incident_type STRING,
  severity NUMBER,
  patient_count NUMBER,
  response_start_ts TIMESTAMP_NTZ,
  response_end_ts TIMESTAMP_NTZ,
  triage_minutes NUMBER,
  ambulance_eta_minutes NUMBER,
  city STRING,
  state STRING,
  cost_usd NUMBER(12,2),
  metadata VARIANT,
  ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE INCIDENT_EVENTS_JSON (
  event_id STRING,
  incident_id NUMBER,
  payload VARIANT,
  ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

USE SCHEMA CORE;

CREATE OR REPLACE TABLE FACT_EMERGENCY_RESPONSE (
  response_key NUMBER AUTOINCREMENT,
  incident_id NUMBER,
  incident_ts TIMESTAMP_NTZ,
  hospital_id STRING,
  incident_type STRING,
  severity NUMBER,
  patient_count NUMBER,
  triage_minutes NUMBER,
  ambulance_eta_minutes NUMBER,
  response_duration_minutes NUMBER,
  cost_usd NUMBER(12,2),
  sla_breach_flag BOOLEAN,
  event_date DATE,
  metadata VARIANT,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE DIM_HOSPITAL (
  hospital_id STRING,
  hospital_name STRING,
  city STRING,
  state STRING,
  region STRING,
  beds NUMBER,
  trauma_level NUMBER
);
