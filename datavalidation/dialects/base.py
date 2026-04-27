"""
Abstract base for SQL dialect (engine-specific queries).
"""
from abc import ABC, abstractmethod
from typing import Any


class SQLDialect(ABC):
    """Abstract dialect: provides catalog and validation SQL per engine."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Engine name: 'db2' or 'azure_sql'."""
        pass

    @abstractmethod
    def catalog_tables_query(self, schema: str | None, object_types: list[str]) -> str:
        """SQL to list tables/views in a schema."""
        pass

    @abstractmethod
    def catalog_columns_query(self, schema: str | None, table_name: str | None) -> str:
        """SQL to list columns (optionally for one table)."""
        pass

    @abstractmethod
    def row_count_query(self, schema: str, table_name: str, dirty_read: bool = False) -> str:
        """SQL to get exact row count for a table (COUNT(*)).

        ``dirty_read=True`` should append the dialect's uncommitted-read hint
        (DB2 ``WITH UR``, SQL Server ``WITH (NOLOCK)``) so the count doesn't wait on row locks.
        """
        pass

    def row_count_estimate_query(self, schema: str, table_name: str) -> str | None:
        """SQL returning estimated row count for one table (column ``cnt``).

        Override per dialect to use catalog statistics (DB2 ``SYSCAT.TABLES.CARD``,
        Azure SQL ``sys.dm_db_partition_stats``). Return ``None`` to disable estimates.
        """
        return None

    def table_stats_query(self, schema: str | None) -> str | None:
        """SQL returning ``schema_name``, ``table_name``, ``row_estimate``, ``bytes_estimate`` for all
        tables under ``schema`` in one round-trip. Used to decide which tables exceed the size threshold
        in 'auto' row-count mode. Return ``None`` to skip per-table sizing.
        """
        return None

    def catalog_objects_query(self, schema: str | None, object_types: list[str]) -> str | None:
        """Optional: SQL returning schema_name, table_name, object_type for TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER. Return None to use catalog_tables_query only."""
        return None

    def catalog_presence_sequences_query(self, schema: str | None) -> str | None:
        """Optional: SQL returning schema_name, object_name (sequence name), object_type='SEQUENCE' for presence check. Return None to skip."""
        return None

    def catalog_presence_indexes_query(self, schema: str | None) -> str | None:
        """Optional: SQL returning schema_name, object_name (TableName.IndexName), object_type='INDEX'. Return None to skip."""
        return None

    def catalog_presence_constraints_query(self, schema: str | None) -> str | None:
        """Optional: SQL returning schema_name, object_name (TableName.ConstraintName), object_type='CONSTRAINT'. Return None to skip."""
        return None

    def catalog_indexes_query(self, schema: str | None) -> str:
        """SQL to list indexes. Override in subclass."""
        raise NotImplementedError

    def catalog_index_columns_query(self, schema: str | None) -> str | None:
        """SQL returning index key columns (for legacy index comparison). Override in subclass."""
        return None

    def catalog_fk_query(self, schema: str | None) -> str:
        """SQL to list foreign keys. Override in subclass."""
        raise NotImplementedError

    def catalog_fk_columns_query(self, schema: str | None) -> str | None:
        """Optional: FK column mapping rows: fk_name, schema_name, table_name, col_seq, fk_column, pk_column."""
        return None

    def catalog_check_constraints_query(self, schema: str | None) -> str:
        """SQL to list check constraints. Override in subclass."""
        raise NotImplementedError

    def checksum_query(self, schema: str, table_name: str, columns: list[str]) -> str:
        """SQL for checksum/hash of table data. Override in subclass."""
        raise NotImplementedError

    def checksum_row_fingerprint_query(
        self,
        schema: str,
        table_name: str,
        key_columns: list[str],
        value_columns: list[str],
    ) -> str | None:
        """Optional per-row ``KeySig`` + ``RowHash`` for row-level checksum. Return ``None`` to skip."""
        return None
