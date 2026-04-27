"""
datavalidation: pip-installable Python library for DB2-to-Azure and Azure-to-Azure migration validation.

Usage:
    pip install datavalidation

    from datavalidation import ValidationClient

    client = ValidationClient(
        source={"type": "db2", "host": "...", "database": "MYDB", "username": "u", "password": "p"},
        target={"type": "azure_sql", "host": "server.database.windows.net", "database": "MyDB", "username": "u", "password": "p"}
    )
    result = client.validate_row_counts(schemas=("dbo", "dbo"))
    print(result.summary)
    result.to_csv("report.csv")
"""
from datavalidation.client import ValidationClient
from datavalidation.results import ValidationResult, ValidationReport
from datavalidation.config import (
    ConnectionConfig,
    ValidationOptions,
    DATA_VALIDATION_PHASE_KEYS,
    resolve_data_validation_phases,
)

__version__ = "2.0.0"
__all__ = [
    "ValidationClient",
    "ValidationResult",
    "ValidationReport",
    "ConnectionConfig",
    "ValidationOptions",
    "DATA_VALIDATION_PHASE_KEYS",
    "resolve_data_validation_phases",
]
