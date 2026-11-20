-- ==============================================================================
-- RetailLake 360 - Database Namespace Initialization (MySQL 8.4 LTS)
-- Three separate logical databases for Source, Serving, and Metadata
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS `retaillake_source`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `retaillake_serving`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `retaillake_metadata`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;
