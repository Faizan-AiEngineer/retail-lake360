-- ==============================================================================
-- RetailLake 360 - Source System Schema (retaillake_source)
-- Simulating Enterprise Retail OLTP Source System
-- ==============================================================================

USE `retaillake_source`;

-- Drop existing tables in dependency order if recreating
DROP TABLE IF EXISTS `order_items`;
DROP TABLE IF EXISTS `orders`;
DROP TABLE IF EXISTS `inventory`;
DROP TABLE IF EXISTS `products`;
DROP TABLE IF EXISTS `stores`;
DROP TABLE IF EXISTS `customers`;

-- 1. Customers Table
CREATE TABLE `customers` (
    `customer_id` INT AUTO_INCREMENT PRIMARY KEY,
    `customer_uid` VARCHAR(36) NOT NULL UNIQUE,
    `first_name` VARCHAR(50) NOT NULL,
    `last_name` VARCHAR(50) NOT NULL,
    `email` VARCHAR(100) NOT NULL UNIQUE,
    `phone` VARCHAR(25) NULL,
    `city` VARCHAR(50) NOT NULL,
    `state` VARCHAR(50) NOT NULL,
    `postal_code` VARCHAR(20) NOT NULL,
    `country` VARCHAR(50) NOT NULL DEFAULT 'USA',
    `registration_date` DATE NOT NULL,
    `customer_segment` VARCHAR(20) NOT NULL DEFAULT 'Standard',
    `is_active` BOOLEAN NOT NULL DEFAULT TRUE,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_customers_email` (`email`),
    INDEX `idx_customers_reg_date` (`registration_date`),
    INDEX `idx_customers_city_state` (`city`, `state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 2. Stores Table
CREATE TABLE `stores` (
    `store_id` INT AUTO_INCREMENT PRIMARY KEY,
    `store_code` VARCHAR(20) NOT NULL UNIQUE,
    `store_name` VARCHAR(100) NOT NULL,
    `store_type` ENUM('Flagship', 'Outlet', 'Express', 'Online') NOT NULL DEFAULT 'Flagship',
    `city` VARCHAR(50) NOT NULL,
    `state` VARCHAR(50) NOT NULL,
    `country` VARCHAR(50) NOT NULL DEFAULT 'USA',
    `sqft_area` INT NOT NULL DEFAULT 5000,
    `opening_date` DATE NOT NULL,
    `is_active` BOOLEAN NOT NULL DEFAULT TRUE,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_stores_type` (`store_type`),
    INDEX `idx_stores_state` (`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 3. Products Table
CREATE TABLE `products` (
    `product_id` INT AUTO_INCREMENT PRIMARY KEY,
    `sku` VARCHAR(30) NOT NULL UNIQUE,
    `product_name` VARCHAR(150) NOT NULL,
    `category` VARCHAR(50) NOT NULL,
    `sub_category` VARCHAR(50) NOT NULL,
    `brand` VARCHAR(50) NOT NULL,
    `unit_cost` DECIMAL(10, 2) NOT NULL,
    `unit_price` DECIMAL(10, 2) NOT NULL,
    `weight_kg` DECIMAL(6, 2) NULL,
    `is_active` BOOLEAN NOT NULL DEFAULT TRUE,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_products_category` (`category`, `sub_category`),
    INDEX `idx_products_price` (`unit_price`),
    INDEX `idx_products_sku` (`sku`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 4. Orders Table
CREATE TABLE `orders` (
    `order_id` INT AUTO_INCREMENT PRIMARY KEY,
    `order_number` VARCHAR(36) NOT NULL UNIQUE,
    `customer_id` INT NOT NULL,
    `store_id` INT NOT NULL,
    `order_date` DATETIME NOT NULL,
    `order_status` ENUM('COMPLETED', 'PENDING', 'PROCESSING', 'SHIPPED', 'CANCELLED', 'REFUNDED') NOT NULL DEFAULT 'COMPLETED',
    `payment_method` ENUM('Credit Card', 'Debit Card', 'PayPal', 'Gift Card', 'Apple Pay', 'Cash') NOT NULL,
    `subtotal_amount` DECIMAL(12, 2) NOT NULL,
    `tax_amount` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    `shipping_fee` DECIMAL(8, 2) NOT NULL DEFAULT 0.00,
    `discount_amount` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    `total_amount` DECIMAL(12, 2) NOT NULL,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT `fk_orders_customer` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`customer_id`) ON DELETE RESTRICT,
    CONSTRAINT `fk_orders_store` FOREIGN KEY (`store_id`) REFERENCES `stores` (`store_id`) ON DELETE RESTRICT,
    INDEX `idx_orders_customer` (`customer_id`),
    INDEX `idx_orders_store` (`store_id`),
    INDEX `idx_orders_date` (`order_date`),
    INDEX `idx_orders_status` (`order_status`),
    INDEX `idx_orders_composite_filter` (`order_date`, `order_status`, `store_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 5. Order Items Table
CREATE TABLE `order_items` (
    `order_item_id` INT AUTO_INCREMENT PRIMARY KEY,
    `order_id` INT NOT NULL,
    `product_id` INT NOT NULL,
    `quantity` INT NOT NULL DEFAULT 1,
    `unit_price` DECIMAL(10, 2) NOT NULL,
    `unit_cost` DECIMAL(10, 2) NOT NULL,
    `discount_amount` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    `total_price` DECIMAL(12, 2) NOT NULL,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT `fk_items_order` FOREIGN KEY (`order_id`) REFERENCES `orders` (`order_id`) ON DELETE CASCADE,
    CONSTRAINT `fk_items_product` FOREIGN KEY (`product_id`) REFERENCES `products` (`product_id`) ON DELETE RESTRICT,
    INDEX `idx_items_order` (`order_id`),
    INDEX `idx_items_product` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 6. Inventory Table
CREATE TABLE `inventory` (
    `inventory_id` INT AUTO_INCREMENT PRIMARY KEY,
    `store_id` INT NOT NULL,
    `product_id` INT NOT NULL,
    `stock_on_hand` INT NOT NULL DEFAULT 0,
    `reorder_threshold` INT NOT NULL DEFAULT 20,
    `reorder_quantity` INT NOT NULL DEFAULT 100,
    `last_restock_date` DATE NULL,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT `fk_inventory_store` FOREIGN KEY (`store_id`) REFERENCES `stores` (`store_id`) ON DELETE RESTRICT,
    CONSTRAINT `fk_inventory_product` FOREIGN KEY (`product_id`) REFERENCES `products` (`product_id`) ON DELETE RESTRICT,
    UNIQUE KEY `uk_store_product_inventory` (`store_id`, `product_id`),
    INDEX `idx_inventory_stock` (`stock_on_hand`, `reorder_threshold`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
