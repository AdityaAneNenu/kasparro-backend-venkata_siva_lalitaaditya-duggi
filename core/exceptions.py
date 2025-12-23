"""Custom exceptions for the application."""


class KasperoException(Exception):
    """Base exception for all application errors."""

    pass


class ETLException(KasperoException):
    """Base exception for ETL-related errors."""

    pass


class ExtractionError(ETLException):
    """Error during data extraction."""

    pass


class TransformationError(ETLException):
    """Error during data transformation."""

    pass


class LoadError(ETLException):
    """Error during data loading."""

    pass


class SchemaValidationError(ETLException):
    """Error during schema validation."""

    pass


class SchemaDriftError(ETLException):
    """Schema drift detected."""

    def __init__(self, message: str, drift_details: dict = None) -> None:  # type: ignore[assignment]
        super().__init__(message)
        self.drift_details = drift_details or {}


class RateLimitError(ETLException):
    """Rate limit exceeded."""

    def __init__(self, message: str, retry_after: float = None) -> None:  # type: ignore[assignment]
        super().__init__(message)
        self.retry_after = retry_after


class CheckpointError(ETLException):
    """Error with checkpoint operations."""

    pass


class APIError(KasperoException):
    """API-related errors."""

    pass


class AuthenticationError(APIError):
    """Authentication failed."""

    pass


class DatabaseError(KasperoException):
    """Database-related errors."""

    pass
