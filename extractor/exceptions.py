"""Custom exception hierarchy for extraction flows."""


class ExtractionError(Exception):
    """Base class for all extraction errors."""


class ConfigurationError(ExtractionError):
    """Raised when required configuration is invalid or missing."""


class ApiAuthError(ExtractionError):
    """Raised when Jira API authentication or authorization fails."""


class ApiTransientError(ExtractionError):
    """Raised for retriable Jira API failures."""


class ApiSchemaError(ExtractionError):
    """Raised when Jira API response payload shape is invalid."""


class FallbackExecutionError(ExtractionError):
    """Raised when Playwright fallback fails."""


class ValidationError(ExtractionError):
    """Raised when transformed data does not satisfy quality checks."""
