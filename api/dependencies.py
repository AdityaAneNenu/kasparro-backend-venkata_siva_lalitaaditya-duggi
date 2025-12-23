"""FastAPI dependencies."""

import time
import uuid
from typing import Generator

from fastapi import Depends, Request
from sqlalchemy.orm import Session

from core.database import get_db


def get_request_id() -> str:
    """Generate a unique request ID."""
    return str(uuid.uuid4())


class RequestContext:
    """Context for tracking request metadata."""

    def __init__(self, request: Request):
        self.request_id = (
            request.state.request_id if hasattr(request.state, "request_id") else get_request_id()
        )
        self.start_time = (
            request.state.start_time if hasattr(request.state, "start_time") else time.time()
        )

    @property
    def elapsed_ms(self) -> float:
        """Get elapsed time in milliseconds."""
        return (time.time() - self.start_time) * 1000


def get_request_context(request: Request) -> RequestContext:
    """Get request context dependency."""
    return RequestContext(request)
