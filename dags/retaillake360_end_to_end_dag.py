"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
End-to-End Orchestration DAG (Apache Airflow)
Data Flow:
Retail Sources (MySQL) -> Bronze Parquet -> Silver Parquet -> Gold Parquet -> MySQL Serving Layer -> Streamlit Analytics
"""

import sys
import os
from datetime import datetime, timedelta

# Ensure src and config are accessible in Python path
sys.path.append("/app")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "retaillake_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def task_check_mysql_health():
    """Verifies that MySQL 8.4 LTS is responsive and accessible."""
    from sqlalchemy import text
    from config.database import get_mysql_engine
    engine = get_mysql_engine(role="pipeline", db_name="retaillake_metadata")
    with engine.connect() as conn:
        res = conn.execute(text("SELECT VERSION();")).scalar()
        print(f"Connected successfully to MySQL version: {res}")


def task_seed_source_data():
    """Generates synthetic source retail OLTP records in retaillake_source."""
    from src.source_simulation.seed_source_db import seed_source_database
    res = seed_source_database(n_customers=150, n_orders=400)
    print(f"Source database seeded: {res}")


def task_ingest_bronze(**context):
    """Ingests raw records into Bronze Parquet layer."""
    from src.lakehouse.bronze.ingest_source_to_bronze import BronzeIngestionEngine
    run_id = context.get("run_id", f"run-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    engine = BronzeIngestionEngine(run_id=run_id)
    summary = engine.run_all()
    print(f"Bronze ingestion completed: {summary}")
    return summary


def task_transform_silver(**context):
    """Transforms raw Bronze into cleansed, validated Silver Star Schema."""
    from src.lakehouse.silver.transform_bronze_to_silver import SilverTransformationEngine
    run_id = context.get("run_id", f"run-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    engine = SilverTransformationEngine(run_id=run_id)
    summary = engine.transform_all()
    print(f"Silver transformation completed: {summary}")
    return summary


def task_aggregate_gold(**context):
    """Aggregates Silver dimensional models into business-level Gold datamarts."""
    from src.lakehouse.gold.aggregate_silver_to_gold import GoldAggregationEngine
    run_id = context.get("run_id", f"run-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    engine = GoldAggregationEngine(run_id=run_id)
    models = engine.compute_all_gold_models()
    print(f"Gold aggregations completed for {len(models)} models.")


def task_publish_serving(**context):
    """Publishes Gold Parquet datasets to MySQL retaillake_serving layer idempotently."""
    from src.serving.publisher import ServingPublisher
    run_id = context.get("run_id", f"run-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    publisher = ServingPublisher(run_id=run_id)
    counts = publisher.publish_all()
    print(f"Published to MySQL serving: {counts}")


def task_analyze_queries():
    """Analyzes MySQL 8.4 query execution plans using EXPLAIN ANALYZE."""
    from src.serving.query_analyzer import MySQLQueryAnalyzer
    analyzer = MySQLQueryAnalyzer()
    benchmarks = analyzer.run_standard_benchmarks()
    for b in benchmarks:
        print(f"Analyzed {b.get('benchmark_label')}: Status={b.get('status')}")


with DAG(
    dag_id="retaillake360_end_to_end_pipeline",
    default_args=default_args,
    description="End-to-end lakehouse pipeline with MySQL 8.4 LTS and Parquet",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "mysql84", "parquet", "retail360"],
) as dag:

    check_db = PythonOperator(
        task_id="check_mysql_health",
        python_callable=task_check_mysql_health,
    )

    seed_source = PythonOperator(
        task_id="seed_source_data",
        python_callable=task_seed_source_data,
    )

    ingest_bronze = PythonOperator(
        task_id="ingest_to_bronze_delta",
        python_callable=task_ingest_bronze,
    )

    transform_silver = PythonOperator(
        task_id="transform_bronze_to_silver_delta",
        python_callable=task_transform_silver,
    )

    aggregate_gold = PythonOperator(
        task_id="aggregate_silver_to_gold_delta",
        python_callable=task_aggregate_gold,
    )

    publish_serving = PythonOperator(
        task_id="publish_gold_to_mysql_serving",
        python_callable=task_publish_serving,
    )

    analyze_queries = PythonOperator(
        task_id="analyze_serving_queries_explain",
        python_callable=task_analyze_queries,
    )

    # Pipeline task dependency graph
    check_db >> seed_source >> ingest_bronze >> transform_silver >> aggregate_gold >> publish_serving >> analyze_queries
