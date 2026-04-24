"""
ValidationClient: main entry point for the datavalidation library.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from datavalidation.config import (
    DEFAULT_SCHEMA_OBJECT_TYPES,
    ConnectionConfig,
    ValidationOptions,
    connection_from_env,
    load_config_from_file,
)
from datavalidation.results import ValidationResult, ValidationReport
from datavalidation.connectors import get_adapter
from datavalidation.connectors.base import ConnectionAdapter
from datavalidation.validators import SchemaValidator, DataValidator, BehaviorValidator


class ValidationClient:
    """
    Client for running DB2-to-Azure or Azure-to-Azure migration validations.
    Use validate_* methods and get ValidationResult or ValidationReport.
    """

    def __init__(
        self,
        source: dict[str, Any] | ConnectionConfig,
        target: dict[str, Any] | ConnectionConfig,
        options: ValidationOptions | dict[str, Any] | None = None,
    ):
        if isinstance(source, dict):
            self._source_config = ConnectionConfig.from_dict(source)
        else:
            self._source_config = source
        if isinstance(target, dict):
            self._target_config = ConnectionConfig.from_dict(target)
        else:
            self._target_config = target
        if options is None:
            self._options = ValidationOptions()
        elif isinstance(options, dict):
            self._options = ValidationOptions(
                parallel_workers=options.get("parallel_workers", 4),
                datatype_leniency=options.get("datatype_leniency", False),
                output_dir=options.get("output_dir"),
                object_types=options.get("object_types", list(DEFAULT_SCHEMA_OBJECT_TYPES)),
                include_definitions=options.get("include_definitions", False),
            )
        else:
            self._options = options
        self._schema_validator: SchemaValidator | None = None
        self._data_validator: DataValidator | None = None
        self._behavior_validator: BehaviorValidator | None = None
        self._source_adapter: ConnectionAdapter | None = None
        self._target_adapter: ConnectionAdapter | None = None

    def _get_adapters(self) -> tuple[ConnectionAdapter, ConnectionAdapter]:
        """Create source and target adapters once and reuse so Azure AD auth (browser) runs only once."""
        if self._source_adapter is None:
            self._source_adapter = get_adapter(self._source_config)
            self._target_adapter = get_adapter(self._target_config)
        return self._source_adapter, self._target_adapter

    @classmethod
    def from_file(cls, path: str | Path) -> ValidationClient:
        """Build client from a YAML or JSON config file."""
        source, target, options = load_config_from_file(path)
        return cls(source=source, target=target, options=options)

    @classmethod
    def from_env(cls) -> ValidationClient:
        """Build client from DV_SOURCE_* and DV_TARGET_* environment variables."""
        source = connection_from_env("DV_SOURCE")
        target = connection_from_env("DV_TARGET")
        if not source or not target:
            raise ValueError("Set DV_SOURCE_* and DV_TARGET_* environment variables")
        return cls(source=source, target=target)

    def _schema(self) -> SchemaValidator:
        if self._schema_validator is None:
            src_adapter, tgt_adapter = self._get_adapters()
            self._schema_validator = SchemaValidator(
                self._source_config, self._target_config, self._options,
                source_adapter=src_adapter, target_adapter=tgt_adapter,
            )
        return self._schema_validator

    def _data(self) -> DataValidator:
        if self._data_validator is None:
            src_adapter, tgt_adapter = self._get_adapters()
            self._data_validator = DataValidator(
                self._source_config, self._target_config, self._options,
                source_adapter=src_adapter, target_adapter=tgt_adapter,
            )
        return self._data_validator

    def _behavior(self) -> BehaviorValidator:
        if self._behavior_validator is None:
            src_adapter, tgt_adapter = self._get_adapters()
            self._behavior_validator = BehaviorValidator(
                self._source_config, self._target_config, self._options,
                source_adapter=src_adapter, target_adapter=tgt_adapter,
            )
        return self._behavior_validator

    # --- Schema validations ---
    def validate_table_presence(
        self,
        schemas: tuple[str | None, str | None] | None = None,
        object_types: list[str] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._schema().validate_table_presence(source_schema, target_schema, object_types)

    def validate_column_counts(
        self,
        schemas: tuple[str | None, str | None] | None = None,
        object_types: list[str] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._schema().validate_column_counts(source_schema, target_schema, object_types)

    def validate_datatype_mapping(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._schema().validate_datatype_mapping(source_schema, target_schema)

    def validate_nullable(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._schema().validate_nullable(source_schema, target_schema)

    def validate_default_values(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._schema().validate_default_values(source_schema, target_schema)

    def validate_indexes(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._schema().validate_indexes(source_schema, target_schema)

    def validate_foreign_keys(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._schema().validate_foreign_keys(source_schema, target_schema)

    def validate_check_constraints(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._schema().validate_check_constraints(source_schema, target_schema)

    def validate_schema(
        self,
        schemas: tuple[str | None, str | None] | None = None,
        object_types: list[str] | None = None,
    ) -> ValidationReport:
        """Run all schema validations."""
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        results = self._schema().run_all(source_schema, target_schema, object_types)
        return ValidationReport(results=results)

    # --- Data validations ---
    def validate_row_counts(
        self,
        schemas: tuple[str, str],
        object_types: list[str] | None = None,
    ) -> ValidationResult:
        if not schemas or len(schemas) < 2:
            raise ValueError("schemas must be (source_schema, target_schema)")
        return self._data().validate_row_counts(schemas[0], schemas[1], object_types)

    def validate_column_nulls(self, schemas: tuple[str, str]) -> ValidationResult:
        return self._data().validate_column_nulls(schemas[0], schemas[1])

    def validate_distinct_keys(self, schemas: tuple[str, str]) -> ValidationResult:
        return self._data().validate_distinct_keys(schemas[0], schemas[1])

    def validate_checksum(self, schemas: tuple[str, str]) -> ValidationResult:
        return self._data().validate_checksum(schemas[0], schemas[1])

    def validate_referential_integrity(self, schemas: tuple[str, str]) -> ValidationResult:
        return self._data().validate_referential_integrity(schemas[0], schemas[1])

    def validate_constraint_integrity(self, schemas: tuple[str, str]) -> ValidationResult:
        return self._data().validate_constraint_integrity(schemas[0], schemas[1])

    def validate_data(
        self,
        schemas: tuple[str, str],
        object_types: list[str] | None = None,
    ) -> ValidationReport:
        """Run all data validations."""
        results = self._data().run_all(schemas[0], schemas[1], object_types)
        return ValidationReport(results=results)

    # --- Behavior validations ---
    def validate_identity_sequence(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._behavior().validate_identity_sequence(source_schema, target_schema)

    def validate_identity_collision(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._behavior().validate_identity_collision(source_schema, target_schema)

    def validate_collation(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._behavior().validate_collation(source_schema, target_schema)

    def validate_triggers(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._behavior().validate_triggers(source_schema, target_schema)

    def validate_routines(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._behavior().validate_routines(source_schema, target_schema)

    def validate_extended_properties(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationResult:
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        return self._behavior().validate_extended_properties(source_schema, target_schema)

    def validate_behavior(
        self,
        schemas: tuple[str | None, str | None] | None = None,
    ) -> ValidationReport:
        """Run all behavior validations."""
        source_schema, target_schema = (schemas or (None, None))[0], (schemas or (None, None))[1]
        results = self._behavior().run_all(source_schema, target_schema)
        return ValidationReport(results=results)

    # --- Run all ---
    def validate_all(
        self,
        schemas: tuple[str, str] | tuple[str | None, str | None],
        object_types: list[str] | None = None,
    ) -> ValidationReport:
        """Run schema, data, and behavior validations."""
        src, tgt = schemas[0], schemas[1]
        results = {}
        results.update(self._schema().run_all(src, tgt, object_types))
        if src and tgt:
            results.update(self._data().run_all(src, tgt, object_types))
        results.update(self._behavior().run_all(src, tgt))
        return ValidationReport(results=results)

    def close(self) -> None:
        """Release connection resources."""
        for v in (self._schema_validator, self._data_validator, self._behavior_validator):
            if v is not None:
                v.close()
        self._schema_validator = None
        self._data_validator = None
        self._behavior_validator = None
        self._source_adapter = None
        self._target_adapter = None

    def __enter__(self) -> "ValidationClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
