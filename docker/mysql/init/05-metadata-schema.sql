-- ==============================================================================
-- RetailLake 360 - Lakehouse Metadata & Audit Schema (retaillake_metadata)
-- Complete Operational Observability, Checkpointing, & Governance
-- ==============================================================================

USE `retaillake_metadata`;

DROP TABLE IF EXISTS `audit_logs`;
DROP TABLE IF EXISTS `data_quality_results`;
DROP TABLE IF EXISTS `ingestion_manifests`;
DROP TABLE IF EXISTS `pipeline_checkpoints`;
DROP TABLE IF EXISTS `pipeline_runs`;

-- 1. Pipeline Runs Table
CREATE TABLE `pipeline_runs` (
    `run_id` VARCHAR(64) PRIMARY KEY,
    `pipeline_name` VARCHAR(100) NOT NULL,
    `trigger_type` ENUM('MANUAL', 'SCHEDULED', 'AIRFLOW', 'EVENT') NOT NULL DEFAULT 'MANUAL',
    `start_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `end_time` TIMESTAMP NULL,
    `duration_seconds` DECIMAL(10, 2) NULL,
    `status` ENUM('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED') NOT NULL DEFAULT 'RUNNING',
    `records_ingested` BIGINT NOT NULL DEFAULT 0,
    `records_transformed` BIGINT NOT NULL DEFAULT 0,
    `records_published` BIGINT NOT NULL DEFAULT 0,
    `error_message` TEXT NULL,
    `execution_metadata` JSON NULL,
    INDEX `idx_pipeline_runs_time` (`start_time`),
    INDEX `idx_pipeline_runs_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 2. Pipeline Checkpoints & Watermarks Table
CREATE TABLE `pipeline_checkpoints` (
    `checkpoint_key` VARCHAR(100) PRIMARY KEY,
    `pipeline_name` VARCHAR(100) NOT NULL,
    `layer` ENUM('SOURCE', 'BRONZE', 'SILVER', 'GOLD', 'SERVING') NOT NULL,
    `table_name` VARCHAR(100) NOT NULL,
    `last_processed_timestamp` DATETIME NOT NULL,
    `last_processed_id` BIGINT NOT NULL DEFAULT 0,
    `records_count` BIGINT NOT NULL DEFAULT 0,
    `data_checksum` VARCHAR(64) NULL,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_checkpoint_layer_table` (`layer`, `table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 3. Audit Logs Table
CREATE TABLE `audit_logs` (
    `log_id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `run_id` VARCHAR(64) NOT NULL,
    `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `log_level` ENUM('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL') NOT NULL DEFAULT 'INFO',
    `component` VARCHAR(100) NOT NULL,
    `action` VARCHAR(100) NOT NULL,
    `message` TEXT NOT NULL,
    `details_json` JSON NULL,
    INDEX `idx_audit_run_id` (`run_id`),
    INDEX `idx_audit_timestamp` (`timestamp`),
    INDEX `idx_audit_component` (`component`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 4. Data Quality Results Table
CREATE TABLE `data_quality_results` (
    `check_id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `run_id` VARCHAR(64) NOT NULL,
    `layer` ENUM('BRONZE', 'SILVER', 'GOLD', 'SERVING') NOT NULL,
    `table_name` VARCHAR(100) NOT NULL,
    `rule_name` VARCHAR(100) NOT NULL,
    `rule_type` VARCHAR(50) NOT NULL,
    `records_tested` INT NOT NULL DEFAULT 0,
    `records_passed` INT NOT NULL DEFAULT 0,
    `records_failed` INT NOT NULL DEFAULT 0,
    `status` ENUM('PASSED', 'WARNING', 'FAILED') NOT NULL,
    `details` TEXT NULL,
    `checked_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_dq_run_id` (`run_id`),
    INDEX `idx_dq_status` (`status`),
    INDEX `idx_dq_layer_table` (`layer`, `table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 5. Ingestion Manifests Table
CREATE TABLE `ingestion_manifests` (
    `manifest_id` VARCHAR(64) PRIMARY KEY,
    `run_id` VARCHAR(64) NOT NULL,
    `source_table` VARCHAR(100) NOT NULL,
    `target_delta_path` VARCHAR(255) NOT NULL,
    `row_count` BIGINT NOT NULL DEFAULT 0,
    `file_format` VARCHAR(20) NOT NULL DEFAULT 'DELTA',
    `file_size_bytes` BIGINT NOT NULL DEFAULT 0,
    `partition_info` VARCHAR(100) NULL,
    `ingested_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_manifest_run_id` (`run_id`),
    INDEX `idx_manifest_source` (`source_table`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
