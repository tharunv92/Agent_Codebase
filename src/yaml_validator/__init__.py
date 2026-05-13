"""
Metadata-driven YAML validation framework.

This package provides a configurable validation framework that validates
YAML inputs against configuration values stored in DynamoDB.
"""

from .validator import YAMLValidator
from .config_loader import ConfigLoader
from .dynamodb_client import DynamoDBClient
from .exceptions import ValidationError, ConfigurationError

__all__ = [
    "YAMLValidator",
    "ConfigLoader",
    "DynamoDBClient",
    "ValidationError",
    "ConfigurationError",
]

__version__ = "0.1.0"
