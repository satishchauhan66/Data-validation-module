"""
Base validator: shared logic for running dialect queries and comparing results.
"""
from abc import ABC
from typing import Any

from datavalidation.config import ConnectionConfig, ValidationOptions
from datavalidation.connectors.base import ConnectionAdapter
from datavalidation.connectors import get_adapter
from datavalidation.dialects import get_dialect
from datavalidation.results import ValidationResult


class BaseValidator(ABC):
    """Base class for schema, data, and behavior validators."""

    def __init__(
        self,
        source_config: ConnectionConfig,
        target_config: ConnectionConfig,
        options: ValidationOptions | None = None,
        source_adapter: ConnectionAdapter | None = None,
        target_adapter: ConnectionAdapter | None = None,
    ):
        self.source_config = source_config
        self.target_config = target_config
        self.options = options or ValidationOptions()
        self._source_adapter = source_adapter
        self._target_adapter = target_adapter
        self._source_dialect = get_dialect(source_config.type)
        self._target_dialect = get_dialect(target_config.type)

    def _resolve_source_schema(self, source_schema: str | None) -> str | None:
        """For DB2, resolve 'USERID' to the actual connection username for catalog queries. Return as-is otherwise."""
        if not source_schema or str(source_schema).strip() == "":
            return source_schema
        if str(source_schema).strip().upper() == "USERID" and self.source_config.type == "db2":
            return (self.source_config.username or "").strip() or source_schema
        return source_schema

    def _get_source_adapter(self) -> ConnectionAdapter:
        if self._source_adapter is None:
            self._source_adapter = get_adapter(self.source_config)
            self._source_adapter.connect()
        return self._source_adapter

    def _get_target_adapter(self) -> ConnectionAdapter:
        if self._target_adapter is None:
            self._target_adapter = get_adapter(self.target_config)
            self._target_adapter.connect()
        return self._target_adapter

    def _source_execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> list[dict]:
        adapter = self._get_source_adapter()
        try:
            return adapter.execute(sql, params, timeout_seconds=timeout_seconds)
        except TypeError:
            # Adapter on this codebase predates the timeout kwarg
            return adapter.execute(sql, params)

    def _target_execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> list[dict]:
        adapter = self._get_target_adapter()
        try:
            return adapter.execute(sql, params, timeout_seconds=timeout_seconds)
        except TypeError:
            return adapter.execute(sql, params)

    def close(self) -> None:
        if self._source_adapter:
            self._source_adapter.close()
        if self._target_adapter:
            self._target_adapter.close()
