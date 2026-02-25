USE DATABASE HOSPITAL_ER_DB;
USE SCHEMA CORE;

ALTER TABLE FACT_EMERGENCY_RESPONSE
  CLUSTER BY (event_date, hospital_id, incident_type);

ALTER TABLE FACT_EMERGENCY_RESPONSE
  ADD SEARCH OPTIMIZATION ON EQUALITY(hospital_id, incident_type);

CREATE OR REPLACE MATERIALIZED VIEW MV_DAILY_HOSPITAL_METRICS AS
SELECT
  event_date,
  hospital_id,
  COUNT(*) AS incident_count,
  AVG(response_duration_minutes) AS avg_response_duration,
  AVG(triage_minutes) AS avg_triage_minutes,
  SUM(IFF(sla_breach_flag, 1, 0)) AS sla_breach_count
FROM FACT_EMERGENCY_RESPONSE
GROUP BY event_date, hospital_id;


CREATE OR REPLACE TABLE FACT_EMERGENCY_RESPONSE_CLONE
  CLONE FACT_EMERGENCY_RESPONSE;

SELECT
  incident_id,
  metadata:source::STRING AS source_channel,
  metadata:language::STRING AS language_code
FROM FACT_EMERGENCY_RESPONSE
WHERE metadata:source::STRING = '911';
