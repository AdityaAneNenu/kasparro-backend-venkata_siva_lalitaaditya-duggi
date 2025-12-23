"""Main FastAPI application."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.middleware import RequestTrackingMiddleware
from api.routes import data, health, metrics, stats
from core.config import get_settings
from core.database import init_db
from core.logging_config import setup_logging

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    setup_logging()
    logger.info("Starting Kaspero API service")

    # Initialize database
    init_db()
    logger.info("Database initialized")

    yield

    # Shutdown
    logger.info("Shutting down Kaspero API service")


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    app = FastAPI(
        title="Kaspero ETL API",
        description="Backend API for Kaspero ETL system",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add request tracking middleware
    app.add_middleware(RequestTrackingMiddleware)

    # Include routers
    app.include_router(health.router, tags=["Health"])
    app.include_router(data.router, tags=["Data"])
    app.include_router(stats.router, tags=["Statistics"])
    app.include_router(metrics.router, tags=["Metrics"])

    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=True,
    )
