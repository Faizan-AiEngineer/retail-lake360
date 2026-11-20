#!/usr/bin/env bash
# ==============================================================================
# RetailLake 360 - Quickstart Script (Linux/macOS)
# ==============================================================================
set -e

echo "==========================================================================="
echo "   Starting RetailLake 360 Enterprise Data Lakehouse Platform"
echo "   Database Engine: MySQL 8.4 LTS | Lakehouse Storage: Parquet"
echo "==========================================================================="

# Check for .env file
if [ ! -f .env ]; then
    echo "Creating .env from .env.example. Set unique passwords, then run this script again."
    cp .env.example .env
    exit 1
fi

# Start Docker containers
echo "Starting MySQL 8.4 LTS, Airflow, and Streamlit Dashboard..."
docker compose up -d

echo "Waiting for MySQL 8.4 service to become healthy..."
until [ "`docker inspect -f {{.State.Health.Status}} retaillake-mysql-84`"=="healthy" ]; do
    sleep 3;
done

echo "Running platform acceptance verification..."
python scripts/verify_platform.py

echo ""
echo "🎉 RetailLake 360 Platform is up and running!"
echo "   - Streamlit Analytics Dashboard: http://localhost:8501"
echo "   - Apache Airflow Webserver:     http://localhost:8080 (sign in with the credentials in .env)"
echo "   - MySQL 8.4 Database:           localhost:3306"
echo "==========================================================================="
