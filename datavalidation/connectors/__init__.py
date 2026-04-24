"""Database connection adapters."""
from datavalidation.connectors.base import ConnectionAdapter
from datavalidation.connectors.azure_sql import AzureSQLAdapter
from datavalidation.connectors.db2 import DB2Adapter


def get_adapter(config) -> ConnectionAdapter:
    """Return the appropriate adapter for the given ConnectionConfig."""
    from datavalidation.config import ConnectionConfig
    if config.type == "azure_sql":
        return AzureSQLAdapter(config)
    if config.type == "db2":
        return DB2Adapter(config)
    raise ValueError(f"Unknown connection type: {config.type}")


__all__ = ["ConnectionAdapter", "AzureSQLAdapter", "DB2Adapter", "get_adapter"]
