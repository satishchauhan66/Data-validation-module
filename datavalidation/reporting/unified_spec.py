"""
Canonical CSV column order from the original backend:
``app/schemas/common.py`` → ``get_unified_columns(include_change_type=False)``.
"""
from __future__ import annotations

# Same order as original; used by ``ValidationReport.to_legacy_csv``.
UNIFIED_REPORT_COLUMNS: list[str] = [
    "ValidationType",
    "Status",
    "ObjectType",
    "SourceObjectName",
    "SourceSchemaName",
    "DestinationObjectName",
    "DestinationSchemaName",
    "ElementPath",
    "ErrorCode",
    "ErrorDescription",
    "DetailsJson",
]

# Internal validation_name → CSV ValidationType (subset; rest pass through)
LEGACY_VALIDATION_TYPE_MAP: dict[str, str] = {
    "table_presence": "presence",
    "nullable": "nullable_constraints",
}

# Row order in legacy schema CSV (matches typical old-tool export: presence first, then column/datatype/nullable/defaults, then indexes/FK/checks).
LEGACY_CSV_VALIDATION_TYPE_ORDER: list[str] = [
    "presence",
    "column_counts",
    "datatype_mapping",
    "nullable_constraints",
    "default_values",
    "indexes",
    "foreign_keys",
    "check_constraints",
    "row_counts",
]
