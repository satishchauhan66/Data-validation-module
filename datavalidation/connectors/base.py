"""
Abstract base for database connection adapters.
"""
from abc import ABC, abstractmethod
from typing import Any

from datavalidation.config import ConnectionConfig


class ConnectionAdapter(ABC):
    """Abstract adapter for executing SQL against a database."""

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._engine = None
        self._connection = None

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the database."""
        pass

    @abstractmethod
    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> list[dict[str, Any]]:
        """Execute SQL and return rows as list of dicts.

        ``timeout_seconds`` is a best-effort per-query timeout (driver-dependent). For DB2 JDBC it
        maps to ``Statement.setQueryTimeout``; for pyodbc it maps to ``Connection.timeout``. When a
        backend can't honour it, the kwarg is ignored.
        """
        pass

    def execute_df(self, sql: str, params: dict[str, Any] | None = None, timeout_seconds: int | None = None):
        """Execute SQL and return a pandas DataFrame. Optional dependency."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for execute_df(). pip install pandas")
        rows = self.execute(sql, params, timeout_seconds=timeout_seconds)
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    @abstractmethod
    def test_connection(self) -> bool:
        """Return True if connection works."""
        pass

    def close(self) -> None:
        """Release connection resources."""
        self._connection = None
        self._engine = None

    def __enter__(self) -> "ConnectionAdapter":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
