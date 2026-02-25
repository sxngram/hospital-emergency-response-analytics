# Architecture

```mermaid
flowchart LR
    A[Hospital Systems and EMS Feeds] --> B[Raw Landing Zone CSV/JSON]
    B --> C[Python ETL/ELT Pipeline]
    C --> D[Curated Parquet + SQLite Warehouse]
    C --> E[Snowflake RAW Schema]
    E --> F[Snowflake CORE and MART]

    D --> G[Spark Batch Job]
    B --> H[Spark Structured Streaming]
    H --> I[Real-time Aggregates Parquet/Snowflake]
    G --> J[Batch KPI Tables]

    F --> K[Security and Governance RBAC/Masking/Tags]
    F --> L[Query Layer Advanced SQL]
    J --> M[Streamlit Dashboard]
    I --> M
    L --> M

    M --> N[Operations Command Center Insights]
```

## Layers
- Sources: incident feeds, hospital reference data, and staffing data.
- Processing: Python ETL for conformed tables, Spark for larger batch and streaming workloads.
- Storage: local SQLite/parquet for dev, Snowflake for production analytics.
- Serving: SQL models and aggregates consumed by dashboard users.
- Governance: role-based access, masking, row policies, and tags.
- Consumption: dashboard and presentation outputs.
