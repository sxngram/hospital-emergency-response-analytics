# Hospital Emergency Response Analytics - Presentation Notes

## 1. Goal
Create one analytics workflow for emergency operations so teams can track response speed, triage performance, and SLA risk across hospitals.

## 2. Expected impact
- Faster triage and dispatch decisions
- Clear SLA breach visibility by hospital, shift, and incident type
- Controlled data access for analyst and executive groups
- Better planning for demand spikes

## 3. What was built
- Warehouse model: star schema for reporting and snowflake schema for normalized reference domains
- Data pipelines: Python ETL for batch loads, Spark for batch and streaming aggregation
- Snowflake layer: clustered fact table, materialized view, semi-structured support
- Security layer: RBAC, masking policy, row access policy, and data tags
- BI layer: Streamlit dashboard for trend, cost, and SLA tracking

## 4. Core data model
- Fact table: `fact_emergency_response`
- Dimensions: `dim_date`, `dim_hospital`, `dim_incident_type`, `dim_shift`
- Optional normalized dimensions: `dim_region`, `dim_state`, `dim_city`, incident group tables

## 5. Analytics patterns used
- CTE chains for reusable KPI logic
- Window functions for ranking, rolling averages, and day-over-day deltas
- Percentiles (P50/P90) for response-time distribution
- Indexing and partitioning guidance for large fact volumes

## 6. Snowflake performance setup
- Cluster by `(event_date, hospital_id, incident_type)`
- Search optimization for selective equality filters
- Materialized view for daily KPI reads
- Time travel and zero-copy clone for safe investigation

## 7. Security and governance
- Roles: `ER_ADMIN`, `ER_ANALYST`, `ER_EXECUTIVE`
- Cost masking for non-admin users
- Row access filter for geography-based restrictions
- Classification tags for governance workflows

## 8. Performance approach
- Partition pruning and predicate pushdown
- Limit wide scans in dashboard-facing queries
- Incremental loading where possible
- Pre-aggregates for dashboard response time

## 9. Dashboard KPIs
- Incident volume trend
- Average triage time
- Average response duration
- SLA breach rate
- Cost by hospital and incident type

## 10. Next phase
- Add forecasting for surge periods
- Add geospatial routing inputs for ambulance operations
- Add automated data quality checks and SLA alerting
