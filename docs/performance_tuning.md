# Performance Tuning Guide

## Warehouse / SQL
- Index join and filter keys used most often.
- Partition the fact table by incident month.
- Use summary tables or materialized views for dashboard-heavy queries.
- Check query plans and remove avoidable full scans.

## ETL
- Keep transforms vectorized in pandas.
- Load incrementally using `incident_ts` as a watermark.
- Write curated parquet partitioned by date when data volume grows.

## Spark
- Enable adaptive query execution.
- Repartition on `hospital_key` for heavy aggregations.
- Cache reused dimension datasets in batch jobs.
- Use checkpointing and watermarks for streaming stability.

## Snowflake
- Cluster on common time/entity filters.
- Use search optimization only for truly selective filters.
- Monitor clustering quality and warehouse spend.
- Separate ETL and BI workloads by warehouse.

## Dashboard
- Prefer pre-aggregated sources over raw fact scans.
- Push filters into SQL where possible.
- Keep chart queries narrow and predictable.
