"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Spark & Delta Lake Engine Session Factory
Configures PySpark with Delta Lake extensions, MySQL JDBC connector, and optimized execution settings.
"""

import os
import sys
import logging
from typing import Optional
from config.settings import settings

logger = logging.getLogger("retaillake.spark")


def get_spark_session(app_name: Optional[str] = None):
    """
    Initializes and returns a configured SparkSession with Delta Lake extensions.
    """
    try:
        from pyspark.sql import SparkSession
        import delta

        name = app_name or settings.SPARK_APP_NAME

        builder = (
            SparkSession.builder.appName(name)
            .master(settings.SPARK_MASTER)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", os.path.join(settings.LAKEHOUSE_STORAGE_BASE, "warehouse"))
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.driver.memory", "2g")
        )

        spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        logger.warning(f"Native Spark/Delta initialization encountered: {e}. Falling back to Pandas/PyArrow Lakehouse runtime.")
        return None
