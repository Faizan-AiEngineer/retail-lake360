# Architecture

RetailLake 360 separates its sample retail source, analytical serving tables, and operational metadata into three MySQL databases. The Bronze, Silver, and Gold pipeline uses Pandas for processing and PyArrow for Parquet storage.

```mermaid
flowchart LR
    subgraph MySQL
        S[retaillake_source]
        V[retaillake_serving]
        M[retaillake_metadata]
    end
    subgraph Lakehouse
        B[Bronze]
        Si[Silver]
        G[Gold]
    end
    Sim[Retail data simulator] --> S
    S --> B --> Si --> G
    G --> V
    B -. ingestion manifests .-> M
    Si -. quality results .-> M
    G -. run and checkpoint data .-> M
    V --> D[Streamlit dashboard]
    M --> D
    A[Apache Airflow] -. orchestrates .-> S
    A -. orchestrates .-> B
    A -. orchestrates .-> G
    A -. orchestrates .-> V
```

## Components

- **Source database:** simulated retail entities and transactions in `retaillake_source`.
- **Lakehouse:** Parquet files in the Bronze, Silver, and Gold layers. Bronze retains ingested source records. Silver applies transformations and quality checks. Gold contains business aggregates.
- **Serving database:** published Gold tables and views in `retaillake_serving`. The publisher uses MySQL upserts to support repeatable publishing.
- **Metadata database:** pipeline runs, checkpoints, quality results, and ingestion metadata in `retaillake_metadata`.
- **Orchestration:** Airflow defines a daily end-to-end workflow and an hourly sync. The hourly DAG checks the recorded watermark, processes the source tables, and updates the checkpoint.
- **Dashboard:** Streamlit reads serving data and selected pipeline metadata.

## Runtime options

`scripts/run_pipeline.py` runs the main workflow directly. `scripts/setup_mysql.py` initializes databases, schemas, users, and grants against a running MySQL server. The Compose configuration provides the database, Airflow services, and dashboard for local use.
