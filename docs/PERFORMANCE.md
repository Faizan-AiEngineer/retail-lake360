# Performance configuration

This document records configuration choices in the repository. It does not report benchmark results; actual performance depends on the host, dataset, and workload.

## MySQL

`docker/mysql/my.cnf` configures the MySQL server, including its InnoDB buffer pool, transaction-log flushing behavior, connection limit, and SQL mode. Review these values against the memory, durability, and concurrency requirements of the deployment before changing them.

## SQLAlchemy

`config/database.py` creates pooled SQLAlchemy engines and configures connection health checks and recycling. Pool size, overflow, timeout, and recycling settings can be adjusted through the variables in `.env.example`.

## Query analysis

`src/serving/query_analyzer.py` uses MySQL `EXPLAIN ANALYZE` for selected serving queries. Run the pipeline or its query-analysis step against the target database to inspect its actual execution plans. Results should be measured in the target environment rather than inferred from configuration.
