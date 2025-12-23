"""ETL Runner Script - Entry point for scheduled ETL."""

import argparse
import logging
import signal
import sys
import time
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, "/app")

from core.config import get_settings
from core.database import check_db_connection, init_db
from core.logging_config import setup_logging
from ingestion.orchestrator import ETLOrchestrator

logger = logging.getLogger(__name__)
settings = get_settings()

# Flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True


def run_once():
    """Run ETL pipeline once."""
    logger.info("Starting single ETL run")

    orchestrator = ETLOrchestrator(parallel=False)
    results = orchestrator.run_all()

    # Log summary
    successful = sum(1 for r in results if r.get("status") == "success")
    failed = sum(1 for r in results if r.get("status") == "failed")

    logger.info(
        f"ETL run completed: {successful} successful, {failed} failed", extra={"results": results}
    )

    return results


def run_scheduled(interval_minutes: int = None):
    """Run ETL pipeline on a schedule."""
    interval = interval_minutes or settings.ETL_SCHEDULE_MINUTES

    logger.info(f"Starting ETL scheduler (interval: {interval} minutes)")

    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    while not shutdown_requested:
        try:
            run_once()
        except Exception as e:
            logger.error(f"ETL run failed with error: {e}", exc_info=True)

        # Wait for next run
        wait_seconds = interval * 60
        logger.info(f"Next ETL run in {interval} minutes")

        # Sleep in small increments to allow for graceful shutdown
        for _ in range(wait_seconds):
            if shutdown_requested:
                break
            time.sleep(1)

    logger.info("ETL scheduler shutting down")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Kaspero ETL Runner")
    parser.add_argument("--once", action="store_true", help="Run ETL once and exit")
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Interval in minutes between runs (default: from settings)",
    )

    args = parser.parse_args()

    # Setup
    setup_logging()

    # Wait for database to be ready
    max_retries = 30
    for i in range(max_retries):
        if check_db_connection():
            logger.info("Database connection established")
            break
        logger.warning(f"Database not ready, retrying ({i+1}/{max_retries})...")
        time.sleep(2)
    else:
        logger.error("Could not connect to database after maximum retries")
        sys.exit(1)

    # Initialize database tables
    init_db()

    # Run ETL
    if args.once:
        results = run_once()
        # Exit with error code if any failed
        if any(r.get("status") == "failed" for r in results):
            sys.exit(1)
    else:
        run_scheduled(args.interval)


if __name__ == "__main__":
    main()
