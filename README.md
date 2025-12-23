# 🚀 Kaspero - Cryptocurrency ETL System

[![CI/CD Pipeline](https://github.com/AdityaAneNenu/AdityaAneNenu-kasparro-backend-venkata_siva_lalitaaditya-duggi/actions/workflows/ci.yml/badge.svg)](https://github.com/AdityaAneNenu/AdityaAneNenu-kasparro-backend-venkata_siva_lalitaaditya-duggi/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-green)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production-grade ETL (Extract, Transform, Load) system for cryptocurrency market data, with a RESTful API backend, built with Python, FastAPI, PostgreSQL, and Docker.

## 📋 Table of Contents

- [Features](#-features)
- [Data Sources](#-data-sources)
- [Architecture](#-architecture)
- [Quick Start](#-quick-start)
- [API Documentation](#-api-documentation)
- [Configuration](#-configuration)
- [Development](#-development)
- [Testing](#-testing)
- [Deployment](#-deployment)
- [Design Decisions](#-design-decisions)

---

## ✨ Features

### P0 - Foundation Layer ✅

- **Multi-source Data Ingestion**

  - **CoinPaprika API** - Cryptocurrency prices, market caps, and rankings
  - **CoinGecko API** - Real-time market data for 10,000+ coins
  - **CSV file source** - Historical data import
  - Raw data stored in `raw_*` tables
  - Unified schema normalization
  - Pydantic validation with type cleaning

- **Backend API Service**

  - `GET /data` - Paginated, filtered crypto data with metadata
  - `GET /health` - DB connectivity & ETL status
  - Request tracking with `request_id` and `api_latency_ms`

- **Dockerized System**
  - Single command startup: `make up`
  - Automatic ETL service and API startup
  - PostgreSQL database included

### P1 - Growth Layer ✅

- **Dual API Data Sources** - CoinPaprika + CoinGecko
- **Incremental Ingestion**
  - Checkpoint table for resume-on-failure
  - Idempotent writes (upserts)
  - No reprocessing of old data
- **`/stats` Endpoint** - ETL summaries, run metadata
- **Clean Architecture** - Separated concerns

### P2 - Differentiator Layer ✅

- **Schema Drift Detection**
  - Fuzzy matching for renamed fields
  - Confidence scoring
  - Warning logs and tracking
- **Failure Injection & Recovery**
  - Controlled mid-run failures
  - Clean resume from checkpoint
  - Duplicate avoidance
- **Rate Limiting & Backoff**
  - Per-source rate limits
  - Exponential backoff with retry
- **Observability**
  - `/metrics` endpoint (Prometheus format)
  - Structured JSON logging
  - ETL metadata tracking
- **DevOps**
  - GitHub Actions CI pipeline
  - Docker health checks
  - Cron-based scheduling
- **Run Comparison**
  - `/runs` and `/compare-runs` endpoints
  - Anomaly detection between runs

---

## 📊 Data Sources

### CoinPaprika API (Primary)

- **URL**: https://api.coinpaprika.com/
- **Data**: 2500+ cryptocurrencies with real-time prices
- **Features**: Market cap, 24h volume, price changes, rankings
- **Rate Limit**: Free tier available, API key for higher limits

### CoinGecko API (Secondary)

- **URL**: https://api.coingecko.com/
- **Data**: 10,000+ coins with comprehensive market data
- **Features**: Detailed price history, market stats, sparklines
- **Rate Limit**: 10-30 calls/minute (no API key required)

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                              │
├─────────────┬─────────────┬─────────────────────────────────────┤
│     API     │     CSV     │              RSS Feed                │
│  (External) │   (Files)   │               (Feeds)                │
└──────┬──────┴──────┬──────┴──────────────┬──────────────────────┘
       │             │                      │
       ▼             ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                        ETL Pipeline                              │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐           │
│  │ Extract  │───▶│  Transform   │───▶│    Load      │           │
│  └──────────┘    └──────────────┘    └──────────────┘           │
│       │                │                    │                    │
│       ▼                ▼                    ▼                    │
│   Rate Limit     Schema Drift          Checkpoint                │
│   + Backoff       Detection             Manager                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL                                │
│  ┌───────────┐  ┌───────────┐  ┌────────────┐  ┌─────────────┐  │
│  │  raw_api  │  │  raw_csv  │  │  raw_rss   │  │unified_data │  │
│  └───────────┘  └───────────┘  └────────────┘  └─────────────┘  │
│  ┌───────────┐  ┌───────────┐  ┌────────────┐                   │
│  │ etl_runs  │  │checkpoints│  │schema_drift│                   │
│  └───────────┘  └───────────┘  └────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      FastAPI Backend                             │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐  ┌─────────┐   │
│  │ /data  │  │/health │  │ /stats │  │ /runs  │  │/metrics │   │
│  └────────┘  └────────┘  └────────┘  └────────┘  └─────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Project Structure

```
kaspero/
├── api/                    # FastAPI application
│   ├── main.py             # App entry point
│   ├── middleware.py       # Request tracking
│   ├── dependencies.py     # FastAPI dependencies
│   └── routes/             # API endpoints
│       ├── data.py         # /data endpoint
│       ├── health.py       # /health, /ready, /live
│       ├── stats.py        # /stats, /runs, /compare-runs
│       └── metrics.py      # /metrics (Prometheus)
├── core/                   # Core utilities
│   ├── config.py           # Settings management
│   ├── database.py         # DB connection
│   ├── models.py           # SQLAlchemy models
│   ├── exceptions.py       # Custom exceptions
│   └── logging_config.py   # Structured logging
├── ingestion/              # ETL pipeline
│   ├── base.py             # Base extractor class
│   ├── api_extractor.py    # API data extraction
│   ├── csv_extractor.py    # CSV data extraction
│   ├── rss_extractor.py    # RSS feed extraction
│   └── orchestrator.py     # ETL orchestration
├── services/               # Business logic
│   ├── checkpoint.py       # Checkpoint management
│   ├── rate_limiter.py     # Rate limiting
│   ├── schema_drift.py     # Drift detection
│   └── etl_tracker.py      # Run tracking
├── schemas/                # Pydantic models
│   └── data_schemas.py     # Request/Response schemas
├── tests/                  # Test suite
│   ├── conftest.py         # Fixtures
│   ├── test_etl_transformation.py
│   ├── test_api_endpoints.py
│   ├── test_incremental_ingestion.py
│   ├── test_failure_scenarios.py
│   ├── test_schema_drift.py
│   ├── test_rate_limiting.py
│   ├── test_stress.py      # Stress tests
│   ├── test_security.py    # Security tests
│   ├── test_performance.py # Performance tests
│   └── test_integration.py # Integration tests
├── scripts/                # Utility scripts
│   └── run_etl.py          # ETL runner
├── data/                   # Sample data files
├── .github/workflows/      # CI/CD pipeline
│   └── ci.yml              # GitHub Actions
├── docker-compose.yml      # Docker services
├── Dockerfile              # API image
├── Dockerfile.scheduler    # Scheduler image
├── Makefile                # Development commands
└── README.md               # This file
```

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Make (optional, for convenience commands)

### 1. Clone and Configure

```bash
git clone https://github.com/AdityaAneNenu/AdityaAneNenu-kasparro-backend-venkata_siva_lalitaaditya-duggi.git
cd kaspero

# Copy environment template
cp .env.example .env

# Edit .env with your API key
nano .env  # or use your preferred editor
```

### 2. Start the System

```bash
# Using Make
make up

# Or using Docker Compose directly
docker-compose up -d
```

### 3. Verify

```bash
# Check health
curl http://localhost:8000/health

# View API docs
open http://localhost:8000/docs
```

### 4. Stop the System

```bash
make down
# or
docker-compose down
```

---

## 📖 API Documentation

### Endpoints

| Endpoint         | Method | Description                       |
| ---------------- | ------ | --------------------------------- |
| `/health`        | GET    | Health check with DB & ETL status |
| `/ready`         | GET    | Kubernetes readiness probe        |
| `/live`          | GET    | Kubernetes liveness probe         |
| `/data`          | GET    | Paginated, filtered data          |
| `/data/{id}`     | GET    | Single record by ID               |
| `/stats`         | GET    | ETL statistics & summaries        |
| `/runs`          | GET    | List ETL runs                     |
| `/runs/{run_id}` | GET    | Single run details                |
| `/compare-runs`  | GET    | Compare two runs                  |
| `/checkpoints`   | GET    | Current ingestion checkpoints     |
| `/schema-drifts` | GET    | Detected schema drifts            |
| `/metrics`       | GET    | Prometheus metrics                |

### GET /data

Query parameters:

- `page` (int): Page number (default: 1)
- `page_size` (int): Items per page (default: 20, max: 100)
- `source_type` (str): Filter by source (api, csv, rss)
- `category` (str): Filter by category
- `author` (str): Filter by author
- `search` (str): Search in title/description
- `start_date` (datetime): Filter by date range
- `end_date` (datetime): Filter by date range
- `sort_by` (str): Sort field (default: created_at)
- `sort_order` (str): asc or desc

Response includes:

```json
{
  "data": [...],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 100,
    "total_pages": 5
  },
  "meta": {
    "request_id": "uuid",
    "api_latency_ms": 12.5,
    "timestamp": "2024-01-15T10:00:00Z"
  }
}
```

### GET /health

Response:

```json
{
  "status": "healthy",
  "database": true,
  "etl_last_run": "2024-01-15T10:00:00Z",
  "etl_last_status": "success",
  "version": "1.0.0"
}
```

---

## ⚙️ Configuration

### Environment Variables

| Variable                         | Description                     | Default                                        |
| -------------------------------- | ------------------------------- | ---------------------------------------------- |
| `DATABASE_URL`                   | PostgreSQL connection string    | `postgresql://kaspero:kaspero@db:5432/kaspero` |
| `API_KEY`                        | External API authentication key | Required                                       |
| `API_SOURCE_URL`                 | External API URL                | -                                              |
| `RSS_SOURCE_URL`                 | RSS feed URL                    | -                                              |
| `CSV_SOURCE_PATH`                | Path to CSV file                | `/app/data/source.csv`                         |
| `ETL_SCHEDULE_MINUTES`           | ETL run interval                | `5`                                            |
| `RATE_LIMIT_REQUESTS_PER_MINUTE` | Rate limit                      | `60`                                           |
| `LOG_LEVEL`                      | Logging level                   | `INFO`                                         |
| `LOG_FORMAT`                     | Log format (json/text)          | `json`                                         |

---

## 💻 Development

### Local Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Start PostgreSQL (using Docker)
docker-compose up -d db

# Run API locally
make dev
# or
uvicorn api.main:app --reload

# Run ETL manually
make etl
# or
python -m scripts.run_etl --once
```

### Code Quality

```bash
# Format code
make format

# Lint
make lint

# Type check
mypy . --ignore-missing-imports
```

---

## 🧪 Testing

The project includes a comprehensive test suite with **161 tests** covering unit, integration, performance, security, and stress testing.

### Run All Tests

```bash
# Using Docker
make test

# Local
make test-local
# or
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=. --cov-report=html
```

### Test Categories

| Category                  | Description                                     | Tests |
| ------------------------- | ----------------------------------------------- | ----- |
| **Unit Tests**            | ETL transformation, rate limiting, schema drift | 41    |
| **API Endpoints**         | HTTP response validation, pagination            | 21    |
| **Integration Tests**     | End-to-end service integration                  | 14    |
| **Performance Tests**     | Response time, throughput benchmarks            | 18    |
| **Security Tests**        | Input validation, injection prevention          | 24    |
| **Stress Tests**          | Load handling, resource limits                  | 18    |
| **Incremental Ingestion** | Checkpoint and resume                           | 12    |
| **Failure Scenarios**     | Error handling and recovery                     | 13    |

### CI/CD Pipeline

The project uses GitHub Actions with a comprehensive CI/CD pipeline:

- **Code Quality**: Black, isort, Flake8, MyPy
- **Security Scanning**: Bandit, Safety, pip-audit
- **Matrix Testing**: Python 3.10, 3.11, 3.12
- **Integration Tests**: With PostgreSQL service
- **Performance Tests**: Benchmarks and stress tests
- **Docker Build**: Multi-arch (amd64, arm64)
- **Container Scanning**: Trivy vulnerability scan

---

## 🌐 Deployment

### Quick Start with Docker

```bash
# Clone the repository
git clone https://github.com/AdityaAneNenu/AdityaAneNenu-kasparro-backend-venkata_siva_lalitaaditya-duggi.git
cd kaspero

# Start services
docker compose up -d

# Verify
curl http://localhost:8000/health
```

### Docker Image

```bash
# Build
docker build -t kaspero-etl:latest .

# Push to registry
docker push your-registry/kaspero-etl:latest
```

### Cloud Deployment (AWS/GCP/Azure)

1. **Set up managed PostgreSQL** (RDS, Cloud SQL, Azure Database)
2. **Deploy containers** (ECS, Cloud Run, AKS)
3. **Configure environment variables** in cloud console
4. **Set up cron/scheduler** (EventBridge, Cloud Scheduler, Logic Apps)
5. **Enable logging** (CloudWatch, Cloud Logging, Azure Monitor)

### Kubernetes

```yaml
# Example deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kaspero-api
spec:
  replicas: 2
  template:
    spec:
      containers:
        - name: api
          image: kaspero-etl:latest
          ports:
            - containerPort: 8000
          livenessProbe:
            httpGet:
              path: /live
              port: 8000
          readinessProbe:
            httpGet:
              path: /ready
              port: 8000
```

---

## 🎨 Design Decisions

### 1. Unified Schema

All data sources are normalized to a common schema in `unified_data`, enabling consistent querying regardless of source.

### 2. Idempotent Ingestion

Upserts ensure running ETL multiple times doesn't create duplicates. Checkpoints enable resume-on-failure.

### 3. Schema Drift Detection

Fuzzy matching identifies renamed fields. Confidence scores help prioritize issues.

### 4. Rate Limiting

Per-source rate limits with exponential backoff prevent API throttling and handle transient failures.

### 5. Observability

Prometheus metrics, structured JSON logs, and comprehensive run metadata enable monitoring and debugging.

### 6. Separation of Concerns

- `core/` - Infrastructure
- `ingestion/` - ETL logic
- `services/` - Business logic
- `api/` - HTTP interface
- `schemas/` - Data contracts

---

## 📄 License

MIT License

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Submit a pull request

---

Built with ❤️ for the Kasparro assignment.
