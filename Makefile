# Kaspero ETL System - Makefile

.PHONY: help build up down logs test clean lint format install dev etl shell db-shell migrate

# Default target
help:
	@echo "Kaspero ETL System - Available Commands:"
	@echo ""
	@echo "  make build        - Build Docker images"
	@echo "  make up           - Start all services"
	@echo "  make down         - Stop all services"
	@echo "  make logs         - View logs from all services"
	@echo "  make test         - Run test suite"
	@echo "  make clean        - Remove containers, volumes, and images"
	@echo ""
	@echo "Development Commands:"
	@echo "  make install      - Install Python dependencies locally"
	@echo "  make dev          - Run API in development mode"
	@echo "  make etl          - Run ETL pipeline manually"
	@echo "  make lint         - Run linting"
	@echo "  make format       - Format code"
	@echo ""
	@echo "Database Commands:"
	@echo "  make shell        - Open shell in API container"
	@echo "  make db-shell     - Open PostgreSQL shell"
	@echo "  make migrate      - Run database migrations"

# ============== Docker Commands ==============

build:
	docker-compose build

up:
	docker-compose up -d
	@echo "Services starting..."
	@echo "API available at: http://localhost:8000"
	@echo "API Docs at: http://localhost:8000/docs"
	@echo "Health check: http://localhost:8000/health"

down:
	docker-compose down

logs:
	docker-compose logs -f

logs-api:
	docker-compose logs -f api

logs-etl:
	docker-compose logs -f etl-worker

# ============== Test Commands ==============

test:
	docker-compose run --rm api pytest tests/ -v --tb=short

test-coverage:
	docker-compose run --rm api pytest tests/ -v --cov=. --cov-report=html --cov-report=term

test-local:
	pytest tests/ -v --tb=short

# ============== Clean Commands ==============

clean:
	docker-compose down -v --rmi local
	rm -rf __pycache__ .pytest_cache .coverage htmlcov
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

clean-db:
	docker-compose down -v
	docker volume rm kaspero_postgres_data 2>/dev/null || true

# ============== Development Commands ==============

install:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

dev:
	uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

etl:
	python -m scripts.run_etl

etl-once:
	docker-compose run --rm etl-worker python -m scripts.run_etl --once

lint:
	flake8 . --max-line-length=100 --exclude=.venv,__pycache__
	mypy . --ignore-missing-imports

format:
	black . --line-length=100
	isort .

# ============== Shell Commands ==============

shell:
	docker-compose exec api /bin/bash

db-shell:
	docker-compose exec db psql -U kaspero -d kaspero

migrate:
	docker-compose exec api python -c "from core.database import init_db; init_db()"

# ============== Health & Status ==============

health:
	curl -s http://localhost:8000/health | python -m json.tool

stats:
	curl -s http://localhost:8000/stats | python -m json.tool

metrics:
	curl -s http://localhost:8000/metrics

# ============== Production Commands ==============

prod-build:
	docker build -t kaspero-etl:latest .

prod-push:
	docker push kaspero-etl:latest

# ============== Quick Start ==============

quick-start: build up
	@echo ""
	@echo "Kaspero ETL System is starting..."
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@make health
