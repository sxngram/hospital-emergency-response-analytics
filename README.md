# Hospital Emergency Response Analytics

This project is a working analytics setup for emergency response data. It includes modeling, ETL, Spark processing, Snowflake scripts, and a dashboard.

## What is included
- Star and snowflake warehouse schemas
- SQL examples using CTEs and window functions
- Indexing and partitioning patterns
- Python ETL pipeline
- Spark batch and streaming jobs
- Snowflake setup, optimization, and security scripts
- Streamlit dashboard
- Architecture and presentation notes

## Project layout
- `data/raw/` sample input files
- `data/curated/` ETL output files
- `sql/` warehouse and analytics SQL
- `snowflake/` Snowflake-specific SQL
- `etl/` Python ETL code
- `spark/` batch and streaming jobs
- `dashboard/` Streamlit app
- `docs/` architecture, tuning, and presentation material

## Quick start
1. Install dependencies:
```bash
pip install -r requirements.txt
```
2. Build local warehouse (`warehouse.db`) and curated parquet files:
```bash
python etl/extract_transform_load.py
```
3. Run Spark batch job:
```bash
python spark/batch_job.py
```
4. Run Spark streaming job (reads from `data/stream_input/`, writes to `data/stream_output/`):
```bash
python spark/streaming_job.py
```
5. Start dashboard:
```bash
streamlit run dashboard/app.py
```

## Snowflake setup order
1. `snowflake/01_setup_warehouse.sql`
2. `snowflake/02_optimization.sql`
3. `snowflake/03_security_governance.sql`

## Notes
- Data in this repo is synthetic.
- Scripts are designed to be easy to run locally first, then adapt for production.
