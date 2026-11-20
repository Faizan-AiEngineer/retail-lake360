"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Data Quality & Expectation Rules Engine
Validates datasets transitioning from Bronze to Silver layer in the Lakehouse.
"""

import logging
from typing import Dict, Any, List, Tuple
import pandas as pd

from src.serving.audit_logger import LakehouseAuditLogger

logger = logging.getLogger("retaillake.quality")


class DataQualityValidator:
    def __init__(self, run_id: str, audit_logger: LakehouseAuditLogger = None):
        self.run_id = run_id
        self.audit_logger = audit_logger or LakehouseAuditLogger(run_id=run_id)

    def validate_non_null(self, df: pd.DataFrame, table_name: str, column: str) -> bool:
        """Checks that key columns have no NULL values."""
        tested = len(df)
        null_count = int(df[column].isnull().sum())
        passed = tested - null_count
        status = "PASSED" if null_count == 0 else "FAILED"

        self.audit_logger.record_quality_check(
            layer="SILVER",
            table_name=table_name,
            rule_name=f"non_null_{column}",
            rule_type="NOT_NULL",
            records_tested=tested,
            records_passed=passed,
            records_failed=null_count,
            status=status,
            details=f"Null count in {column}: {null_count} / {tested}"
        )
        return status == "PASSED"

    def validate_positive_numeric(self, df: pd.DataFrame, table_name: str, column: str, allow_zero: bool = True) -> bool:
        """Checks that numeric columns are non-negative."""
        tested = len(df)
        if allow_zero:
            invalid_count = int((df[column] < 0).sum())
        else:
            invalid_count = int((df[column] <= 0).sum())
        passed = tested - invalid_count
        status = "PASSED" if invalid_count == 0 else "WARNING"

        self.audit_logger.record_quality_check(
            layer="SILVER",
            table_name=table_name,
            rule_name=f"positive_val_{column}",
            rule_type="RANGE_CHECK",
            records_tested=tested,
            records_passed=passed,
            records_failed=invalid_count,
            status=status,
            details=f"Values violating range check on {column}: {invalid_count}"
        )
        return status in ("PASSED", "WARNING")

    def validate_referential_integrity(
        self,
        child_df: pd.DataFrame,
        child_col: str,
        parent_df: pd.DataFrame,
        parent_col: str,
        child_table: str
    ) -> bool:
        """Validates foreign key relationships."""
        tested = len(child_df)
        valid_parent_keys = set(parent_df[parent_col].dropna().unique())
        orphan_count = int((~child_df[child_col].isin(valid_parent_keys)).sum())
        passed = tested - orphan_count
        status = "PASSED" if orphan_count == 0 else "FAILED"

        self.audit_logger.record_quality_check(
            layer="SILVER",
            table_name=child_table,
            rule_name=f"fk_integrity_{child_col}",
            rule_type="REFERENTIAL_INTEGRITY",
            records_tested=tested,
            records_passed=passed,
            records_failed=orphan_count,
            status=status,
            details=f"Orphaned foreign keys on {child_col}: {orphan_count}"
        )
        return status == "PASSED"
