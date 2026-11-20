-- ==============================================================================
-- RetailLake 360 - Analytical Serving Layer Schema (retaillake_serving)
-- Published Gold Lakehouse Models Optimized for Low-Latency BI & Dashboards
-- ==============================================================================

USE `retaillake_serving`;

-- Drop existing views first
DROP VIEW IF EXISTS `vw_customer_segment_distribution`;
DROP VIEW IF EXISTS `vw_store_leaderboard`;
DROP VIEW IF EXISTS `vw_top_profitable_products`;
DROP VIEW IF EXISTS `vw_executive_kpis`;

-- Drop existing tables
DROP TABLE IF EXISTS `gold_inventory_turnover_alerts`;
DROP TABLE IF EXISTS `gold_product_category_profitability`;
DROP TABLE IF EXISTS `gold_customer_rfm_segments`;
DROP TABLE IF EXISTS `gold_store_performance_analytics`;
DROP TABLE IF EXISTS `gold_monthly_sales_summary`;
DROP TABLE IF EXISTS `gold_daily_sales_summary`;

-- 1. Daily Sales Summary (Granular Time-Series & Store Level)
CREATE TABLE `gold_daily_sales_summary` (
    `summary_id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `report_date` DATE NOT NULL,
    `store_id` INT NOT NULL,
    `store_name` VARCHAR(100) NOT NULL,
    `store_type` VARCHAR(20) NOT NULL,
    `total_orders` INT NOT NULL DEFAULT 0,
    `total_units_sold` INT NOT NULL DEFAULT 0,
    `total_revenue` DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    `total_cogs` DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    `gross_profit` DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    `gross_margin_pct` DECIMAL(6, 2) NOT NULL DEFAULT 0.00,
    `avg_order_value` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    `distinct_customers` INT NOT NULL DEFAULT 0,
    `published_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_daily_sales_date_store` (`report_date`, `store_id`),
    INDEX `idx_daily_sales_date` (`report_date`),
    INDEX `idx_daily_sales_store` (`store_id`),
    INDEX `idx_daily_sales_type` (`store_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 2. Monthly Sales Summary
CREATE TABLE `gold_monthly_sales_summary` (
    `summary_id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `year_month` VARCHAR(7) NOT NULL, -- Format: YYYY-MM
    `store_id` INT NOT NULL,
    `store_name` VARCHAR(100) NOT NULL,
    `total_orders` INT NOT NULL DEFAULT 0,
    `total_revenue` DECIMAL(16, 2) NOT NULL DEFAULT 0.00,
    `total_cogs` DECIMAL(16, 2) NOT NULL DEFAULT 0.00,
    `gross_profit` DECIMAL(16, 2) NOT NULL DEFAULT 0.00,
    `gross_margin_pct` DECIMAL(6, 2) NOT NULL DEFAULT 0.00,
    `avg_order_value` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    `published_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_monthly_sales_ym_store` (`year_month`, `store_id`),
    INDEX `idx_monthly_sales_ym` (`year_month`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 3. Customer RFM Segments & CLV Metrics
CREATE TABLE `gold_customer_rfm_segments` (
    `customer_id` INT PRIMARY KEY,
    `customer_uid` VARCHAR(36) NOT NULL,
    `full_name` VARCHAR(100) NOT NULL,
    `email` VARCHAR(100) NOT NULL,
    `city` VARCHAR(50) NOT NULL,
    `state` VARCHAR(50) NOT NULL,
    `recency_days` INT NOT NULL,
    `frequency_orders` INT NOT NULL,
    `monetary_total` DECIMAL(12, 2) NOT NULL,
    `r_score` TINYINT NOT NULL,
    `f_score` TINYINT NOT NULL,
    `m_score` TINYINT NOT NULL,
    `rfm_segment` ENUM('Champions', 'Loyal Customers', 'Potential Loyalists', 'Recent Customers', 'Promising', 'Need Attention', 'About to Sleep', 'At Risk', 'Hibernating', 'Lost') NOT NULL,
    `estimated_clv` DECIMAL(12, 2) NOT NULL,
    `churn_risk` ENUM('Low', 'Medium', 'High') NOT NULL DEFAULT 'Low',
    `first_purchase_date` DATE NOT NULL,
    `last_purchase_date` DATE NOT NULL,
    `published_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_rfm_segment` (`rfm_segment`),
    INDEX `idx_churn_risk` (`churn_risk`),
    INDEX `idx_clv` (`estimated_clv`),
    INDEX `idx_monetary` (`monetary_total`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 4. Product Category Profitability
CREATE TABLE `gold_product_category_profitability` (
    `profitability_id` INT AUTO_INCREMENT PRIMARY KEY,
    `category` VARCHAR(50) NOT NULL,
    `sub_category` VARCHAR(50) NOT NULL,
    `total_units_sold` INT NOT NULL DEFAULT 0,
    `total_revenue` DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    `total_cogs` DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    `gross_profit` DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    `gross_margin_pct` DECIMAL(6, 2) NOT NULL DEFAULT 0.00,
    `product_count` INT NOT NULL DEFAULT 0,
    `published_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_cat_subcat` (`category`, `sub_category`),
    INDEX `idx_category` (`category`),
    INDEX `idx_gross_profit` (`gross_profit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 5. Store Performance Analytics
CREATE TABLE `gold_store_performance_analytics` (
    `store_id` INT PRIMARY KEY,
    `store_code` VARCHAR(20) NOT NULL,
    `store_name` VARCHAR(100) NOT NULL,
    `store_type` VARCHAR(20) NOT NULL,
    `city` VARCHAR(50) NOT NULL,
    `state` VARCHAR(50) NOT NULL,
    `sqft_area` INT NOT NULL,
    `total_orders` INT NOT NULL DEFAULT 0,
    `total_revenue` DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    `gross_profit` DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    `revenue_per_sqft` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    `active_customers` INT NOT NULL DEFAULT 0,
    `revenue_rank` INT NOT NULL DEFAULT 1,
    `published_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_store_perf_rev` (`total_revenue`),
    INDEX `idx_store_perf_rank` (`revenue_rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 6. Inventory Turnover & Restock Alerts
CREATE TABLE `gold_inventory_turnover_alerts` (
    `alert_id` INT AUTO_INCREMENT PRIMARY KEY,
    `store_id` INT NOT NULL,
    `store_name` VARCHAR(100) NOT NULL,
    `product_id` INT NOT NULL,
    `sku` VARCHAR(30) NOT NULL,
    `product_name` VARCHAR(150) NOT NULL,
    `category` VARCHAR(50) NOT NULL,
    `stock_on_hand` INT NOT NULL,
    `reorder_threshold` INT NOT NULL,
    `daily_run_rate` DECIMAL(8, 2) NOT NULL DEFAULT 1.00,
    `estimated_days_of_stock` DECIMAL(8, 1) NOT NULL DEFAULT 0.0,
    `stock_status` ENUM('CRITICAL', 'WARNING', 'OPTIMAL', 'OVERSTOCKED') NOT NULL,
    `reorder_urgency` ENUM('IMMEDIATE', 'UPCOMING', 'NONE') NOT NULL,
    `published_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_store_product_alert` (`store_id`, `product_id`),
    INDEX `idx_stock_status` (`stock_status`),
    INDEX `idx_urgency` (`reorder_urgency`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ==============================================================================
-- Analytical Serving Views (Pre-computed Aggregations for Executive Queries)
-- ==============================================================================

-- View: Executive KPIs
CREATE VIEW `vw_executive_kpis` AS
SELECT
    COALESCE(SUM(total_orders), 0) AS lifetime_orders,
    COALESCE(SUM(total_revenue), 0.00) AS lifetime_revenue,
    COALESCE(SUM(gross_profit), 0.00) AS lifetime_gross_profit,
    CASE 
        WHEN SUM(total_revenue) > 0 THEN ROUND((SUM(gross_profit) / SUM(total_revenue)) * 100, 2)
        ELSE 0.00
    END AS overall_margin_pct,
    CASE 
        WHEN SUM(total_orders) > 0 THEN ROUND(SUM(total_revenue) / SUM(total_orders), 2)
        ELSE 0.00
    END AS avg_basket_size,
    (SELECT COUNT(DISTINCT customer_id) FROM `gold_customer_rfm_segments`) AS total_active_customers,
    (SELECT COUNT(*) FROM `gold_inventory_turnover_alerts` WHERE stock_status = 'CRITICAL') AS critical_stock_alerts
FROM `gold_daily_sales_summary`;

-- View: Top Profitable Products & Categories
CREATE VIEW `vw_top_profitable_products` AS
SELECT 
    category,
    sub_category,
    total_units_sold,
    total_revenue,
    gross_profit,
    gross_margin_pct,
    DENSE_RANK() OVER (ORDER BY gross_profit DESC) as profitability_rank
FROM `gold_product_category_profitability`;

-- View: Store Leaderboard
CREATE VIEW `vw_store_leaderboard` AS
SELECT 
    store_id,
    store_name,
    store_type,
    city,
    state,
    total_orders,
    total_revenue,
    gross_profit,
    revenue_per_sqft,
    revenue_rank
FROM `gold_store_performance_analytics`
ORDER BY revenue_rank ASC;

-- View: Customer Segment Distribution
CREATE VIEW `vw_customer_segment_distribution` AS
SELECT 
    rfm_segment,
    COUNT(*) AS customer_count,
    ROUND(SUM(monetary_total), 2) AS total_segment_spend,
    ROUND(AVG(monetary_total), 2) AS avg_segment_spend,
    ROUND(AVG(recency_days), 1) AS avg_recency_days,
    ROUND(AVG(estimated_clv), 2) AS avg_estimated_clv
FROM `gold_customer_rfm_segments`
GROUP BY rfm_segment
ORDER BY total_segment_spend DESC;
