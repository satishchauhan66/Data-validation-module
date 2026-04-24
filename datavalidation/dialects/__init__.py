"""SQL dialects per database engine."""
from datavalidation.dialects.base import SQLDialect
from datavalidation.dialects.db2 import DB2Dialect
from datavalidation.dialects.azure_sql import AzureSQLDialect


def get_dialect(engine_type: str) -> SQLDialect:
    if engine_type == "db2":
        return DB2Dialect()
    if engine_type == "azure_sql":
        return AzureSQLDialect()
    raise ValueError(f"Unknown dialect: {engine_type}")


__all__ = ["SQLDialect", "DB2Dialect", "AzureSQLDialect", "get_dialect"]
