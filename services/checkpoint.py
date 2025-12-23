"""Checkpoint management for incremental ingestion."""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.exceptions import CheckpointError
from core.models import ETLCheckpoint, SourceType

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manages ETL checkpoints for incremental ingestion."""

    def __init__(self, db: Session):
        self.db = db

    def get_checkpoint(self, source_type: SourceType) -> Optional[ETLCheckpoint]:
        """Get the current checkpoint for a source type."""
        try:
            checkpoint = (
                self.db.query(ETLCheckpoint)
                .filter(ETLCheckpoint.source_type == source_type)
                .first()
            )
            return checkpoint
        except Exception as e:
            logger.error(f"Error getting checkpoint for {source_type}: {e}")
            raise CheckpointError(f"Failed to get checkpoint: {e}")

    def get_last_source_id(self, source_type: SourceType) -> Optional[str]:
        """Get the last processed source ID for a source type."""
        checkpoint = self.get_checkpoint(source_type)
        return checkpoint.last_source_id if checkpoint else None

    def get_last_offset(self, source_type: SourceType) -> int:
        """Get the last processed offset for a source type."""
        checkpoint = self.get_checkpoint(source_type)
        return checkpoint.last_offset if checkpoint else 0

    def update_checkpoint(
        self,
        source_type: SourceType,
        last_source_id: Optional[str] = None,
        last_offset: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ETLCheckpoint:
        """Update or create a checkpoint for a source type."""
        try:
            checkpoint = self.get_checkpoint(source_type)

            if checkpoint is None:
                # Create new checkpoint
                checkpoint = ETLCheckpoint(
                    source_type=source_type,
                    last_source_id=last_source_id,
                    last_offset=last_offset or 0,
                    last_processed_at=datetime.utcnow(),
                    checkpoint_metadata=metadata,
                )
                self.db.add(checkpoint)
            else:
                # Update existing checkpoint
                if last_source_id is not None:
                    checkpoint.last_source_id = last_source_id
                if last_offset is not None:
                    checkpoint.last_offset = last_offset
                if metadata is not None:
                    checkpoint.checkpoint_metadata = metadata
                checkpoint.last_processed_at = datetime.utcnow()

            self.db.commit()
            self.db.refresh(checkpoint)

            logger.info(
                f"Checkpoint updated for {source_type.value}: "
                f"source_id={last_source_id}, offset={last_offset}"
            )

            return checkpoint

        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating checkpoint for {source_type}: {e}")
            raise CheckpointError(f"Failed to update checkpoint: {e}")

    def reset_checkpoint(self, source_type: SourceType) -> None:
        """Reset checkpoint for a source type (for reprocessing)."""
        try:
            checkpoint = self.get_checkpoint(source_type)
            if checkpoint:
                checkpoint.last_source_id = None
                checkpoint.last_offset = 0
                checkpoint.last_processed_at = None
                checkpoint.checkpoint_metadata = None
                self.db.commit()
                logger.info(f"Checkpoint reset for {source_type.value}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error resetting checkpoint for {source_type}: {e}")
            raise CheckpointError(f"Failed to reset checkpoint: {e}")

    def get_all_checkpoints(self) -> Dict[str, Dict[str, Any]]:
        """Get all checkpoints as a dictionary."""
        try:
            checkpoints = self.db.query(ETLCheckpoint).all()
            return {
                cp.source_type.value: {
                    "last_source_id": cp.last_source_id,
                    "last_offset": cp.last_offset,
                    "last_processed_at": (
                        cp.last_processed_at.isoformat() if cp.last_processed_at else None
                    ),
                    "metadata": cp.checkpoint_metadata,
                    "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
                }
                for cp in checkpoints
            }
        except Exception as e:
            logger.error(f"Error getting all checkpoints: {e}")
            raise CheckpointError(f"Failed to get checkpoints: {e}")
