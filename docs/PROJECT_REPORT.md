# Hospital Emergency Response Analytics - Project Report

## 1. Abstract
This project builds an end-to-end analytics platform for hospital emergency response operations. The solution integrates data warehousing, advanced SQL analytics, ETL/ELT pipelines, Spark batch/stream processing, Snowflake optimization and security, and an interactive dashboard. The goal is to provide operational visibility into response speed, triage quality, incident load, and SLA compliance.

## 2. Problem Statement
Emergency departments handle time-critical incidents where delays can directly affect outcomes. Data often exists in separate systems and is difficult to analyze in one place. The project addresses this by creating a unified analytics workflow that helps teams monitor performance and identify bottlenecks quickly.

## 3. Objectives
- Design both Star and Snowflake warehouse schemas.
- Implement advanced SQL for trend, ranking, and percentile analytics.
- Build Python ETL/ELT to transform raw records into analytics-ready models.
- Process data using Spark in batch and streaming modes.
- Prepare Snowflake scripts for storage optimization and governance.
- Enforce security using RBAC, masking, and row-level controls.
- Build an interactive dashboard for operational stakeholders.

## 4. Tools and Technologies
- Programming: Python 3
- Data Processing: pandas, PySpark
- Storage: SQLite (local warehouse), Parquet, Snowflake SQL scripts
- Dashboard: Streamlit + Plotly
- Querying: SQL (CTEs, window functions, percentile functions)
- Version Control: Git + GitHub

## 5. System Architecture
Data from hospital incident feeds and reference datasets is ingested into raw files. A Python ETL pipeline transforms and loads conformed dimensions and fact tables into local warehouse storage and curated parquet datasets. Spark batch jobs generate KPI aggregates; Spark streaming processes incoming events in time windows. Snowflake scripts define production-ready schemas, optimization settings, and security controls. A Streamlit dashboard consumes warehouse data for visualization.

## 6. Data Model Design
### 6.1 Star Schema
- Fact table: `fact_emergency_response`
- Dimension tables: `dim_date`, `dim_hospital`, `dim_incident_type`, `dim_shift`

### 6.2 Snowflake Schema
- Normalized location hierarchy: `dim_region`, `dim_state`, `dim_city`
- Normalized incident taxonomy: incident group and incident type tables

## 7. Implementation Details
### 7.1 Advanced SQL
Implemented query patterns include:
- CTE-based transformations
- Window ranking (`DENSE_RANK`) for top delay contributors
- Rolling metrics for SLA trend tracking
- Percentile analysis (P50/P90) for response-time distribution
- Partitioning/indexing strategy scripts for scale

### 7.2 ETL/ELT Pipeline
The ETL pipeline:
1. Extracts raw CSV datasets.
2. Parses timestamps and validates records.
3. Builds dimensions (date, hospital, incident type, shift).
4. Computes fact metrics (response duration, SLA breach).
5. Loads data into SQLite warehouse and parquet curated layer.

### 7.3 Spark Batch + Streaming
- Batch: computes daily KPIs and facility-level aggregates.
- Streaming: processes incident events in 15-minute windows with watermarking and checkpointing.

### 7.4 Snowflake Features
SQL scripts include:
- Warehouse/database/schema setup
- Clustering strategy
- Search optimization and materialized view
- Semi-structured data querying with `VARIANT`
- Time travel/clone usage patterns

### 7.5 Security and Governance
- Roles: `ER_ADMIN`, `ER_ANALYST`, `ER_EXECUTIVE`
- Privilege grants per role
- Dynamic data masking for cost fields
- Row access policy for controlled regional access
- Data classification tagging for governance

## 8. Performance Tuning Approach
- Indexing on frequent filters and join keys
- Time-based partitioning strategy for large facts
- Aggregate precomputation for dashboard reads
- Spark partitioning and adaptive execution patterns
- Snowflake clustering and selective search optimization

## 9. Dashboard and Insights
The dashboard provides:
- Total incidents, average response/triage time, SLA breach rate
- Daily trends
- Cost comparison by hospital
- Triage vs response correlation by incident type
- Record-level filtering for drill-down analysis

## 10. Results
The implemented pipeline successfully:
- Produces conformed warehouse tables from raw data
- Generates batch KPI outputs using Spark
- Processes streaming files in near real-time
- Serves visual insights via Streamlit dashboard
- Demonstrates production-oriented Snowflake optimization and governance design

## 11. Conclusion
The project demonstrates a complete modern data engineering and analytics workflow for emergency response operations. It is scalable from local development to cloud deployment and provides a strong foundation for future additions such as forecasting, alerting, and geospatial dispatch optimization.

## 12. Future Enhancements
- ML-based incident surge forecasting
- Real-time SLA alert notifications
- Geospatial route optimization integration
- Data quality monitoring and lineage tracking

## 13. Repository Contents (Key Files)
- `sql/01_schema_star.sql`
- `sql/02_schema_snowflake.sql`
- `sql/03_advanced_queries.sql`
- `sql/04_partitioning_indexing.sql`
- `etl/extract_transform_load.py`
- `spark/batch_job.py`
- `spark/streaming_job.py`
- `snowflake/01_setup_warehouse.sql`
- `snowflake/02_optimization.sql`
- `snowflake/03_security_governance.sql`
- `dashboard/app.py`
