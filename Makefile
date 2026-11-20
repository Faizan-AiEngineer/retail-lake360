# ==============================================================================
# RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
# MySQL 8.4 LTS Build & Test Automation Makefile
# ==============================================================================

.PHONY: help install test verify up down pipeline dashboard clean

help:
	@echo "RetailLake 360 - Lakehouse & Analytics Commands:"
	@echo "  make install     Install local Python dependencies"
	@echo "  make test        Run complete pytest integration test suite"
	@echo "  make verify      Run automated acceptance verification suite"
	@echo "  make up          Start all Docker services (MySQL 8.4, Airflow, Dashboard)"
	@echo "  make down        Stop all Docker services"
	@echo "  make pipeline    Execute end-to-end Lakehouse pipeline"
	@echo "  make dashboard   Launch Streamlit Analytics Dashboard locally"
	@echo "  make clean       Clean temp cache files"

install:
	pip install -r requirements.txt

test:
	python -m pytest tests/ -v

verify:
	python scripts/verify_platform.py

up:
	docker compose up -d

down:
	docker compose down

pipeline:
	python scripts/run_pipeline.py

dashboard:
	streamlit run src/dashboard/app.py

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
