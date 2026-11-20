"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Hourly Pipeline Sync DAG (Apache Airflow)
Runs the medallion pipeline and records a serving checkpoint.
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.append("/app")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "retaillake_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def task_check_watermark():
    from src.serving.incremental_loader import IncrementalCheckpointer
    checkpointer = IncrementalCheckpointer()
    cp = checkpointer.get_checkpoint("orders_incremental_watermark")
    print(f"Current orders incremental watermark: {cp}")


def task_incremental_sync(**context):
    from src.lakehouse.bronze.ingest_source_to_bronze import BronzeIngestionEngine
    from src.lakehouse.silver.transform_bronze_to_silver import SilverTransformationEngine
    from src.lakehouse.gold.aggregate_silver_to_gold import GoldAggregationEngine
    from src.serving.publisher import ServingPublisher
    from src.serving.incremental_loader import IncrementalCheckpointer

    run_id = f"inc-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    # Ingest, transform, aggregate, publish
    BronzeIngestionEngine(run_id=run_id).run_all()
    SilverTransformationEngine(run_id=run_id).transform_all()
    GoldAggregationEngine(run_id=run_id).compute_all_gold_models()
    ServingPublisher(run_id=run_id).publish_all()

    # Advance watermark
    IncrementalCheckpointer().update_checkpoint(
        checkpoint_key="orders_incremental_watermark",
        pipeline_name="retaillake360_incremental",
        layer="SERVING",
        table_name="gold_daily_sales_summary",
        last_processed_timestamp=datetime.now()
    )
    print("Incremental sync completed and watermark updated.")


with DAG(
    dag_id="retaillake360_incremental_sync",
    default_args=default_args,
    description="Hourly lakehouse refresh with a MySQL checkpoint",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["incremental", "watermark", "mysql84"],
) as dag:

    check_wm = PythonOperator(
        task_id="check_watermark",
        python_callable=task_check_watermark,
    )

    inc_sync = PythonOperator(
        task_id="incremental_sync_and_publish",
        python_callable=task_incremental_sync,
    )

    check_wm >> inc_sync
