"""Custom exceptions for the YAML validation framework."""


class ValidationError(Exception):
    """Raised when YAML validation fails against config rules."""
    
    def __init__(self, field: str, message: str, value=None):
        self.field = field
        self.message = message
        self.value = value
        super().__init__(f"Validation failed for '{field}': {message}")


class ConfigurationError(Exception):
    """Raised when there's an issue with configuration loading or parsing."""
    
    def __init__(self, message: str, config_key: str = None):
        self.message = message
        self.config_key = config_key
        error_msg = f"Configuration error: {message}"
        if config_key:
            error_msg += f" (key: {config_key})"
        super().__init__(error_msg)


class DynamoDBError(Exception):
    """Raised when there's an issue with DynamoDB operations."""
    
    def __init__(self, message: str, operation: str = None):
        self.message = message
        self.operation = operation
        error_msg = f"DynamoDB error: {message}"
        if operation:
            error_msg += f" (operation: {operation})"
        super().__init__(error_msg)
