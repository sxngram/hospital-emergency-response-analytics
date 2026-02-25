
WITH agg AS (
    SELECT
        h.hospital_name,
        i.incident_type,
        AVG(f.triage_minutes) AS avg_triage_minutes,
        COUNT(*) AS incident_count
    FROM fact_emergency_response f
    JOIN dim_hospital h ON f.hospital_key = h.hospital_key
    JOIN dim_incident_type i ON f.incident_type_key = i.incident_type_key
    GROUP BY h.hospital_name, i.incident_type
), ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY hospital_name
            ORDER BY avg_triage_minutes DESC
        ) AS triage_rank
    FROM agg
)
SELECT *
FROM ranked
WHERE triage_rank <= 3
ORDER BY hospital_name, triage_rank;

SELECT
    d.full_date,
    h.hospital_name,
    SUM(CASE WHEN f.is_breach_sla THEN 1 ELSE 0 END) AS daily_breach_count,
    AVG(SUM(CASE WHEN f.is_breach_sla THEN 1 ELSE 0 END)) OVER (
        PARTITION BY h.hospital_name
        ORDER BY d.full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg_breach
FROM fact_emergency_response f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_hospital h ON f.hospital_key = h.hospital_key
GROUP BY d.full_date, h.hospital_name
ORDER BY d.full_date, h.hospital_name;

SELECT
    i.incident_type,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY f.response_duration_minutes) AS p50_response_min,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY f.response_duration_minutes) AS p90_response_min,
    AVG(f.response_duration_minutes) AS avg_response_min
FROM fact_emergency_response f
JOIN dim_incident_type i ON f.incident_type_key = i.incident_type_key
GROUP BY i.incident_type
ORDER BY p90_response_min DESC;

WITH daily_incidents AS (
    SELECT
        d.full_date,
        h.hospital_name,
        COUNT(*) AS incident_count
    FROM fact_emergency_response f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_hospital h ON f.hospital_key = h.hospital_key
    GROUP BY d.full_date, h.hospital_name
)
SELECT
    full_date,
    hospital_name,
    incident_count,
    LAG(incident_count) OVER (
        PARTITION BY hospital_name ORDER BY full_date
    ) AS prev_day_incident_count,
    incident_count - COALESCE(LAG(incident_count) OVER (
        PARTITION BY hospital_name ORDER BY full_date
    ), 0) AS delta_vs_prev_day
FROM daily_incidents
ORDER BY hospital_name, full_date;
