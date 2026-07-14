"""
DB2 to Azure SQL data type mapping rules (CDC / script conversion utility).

Compatible remaps (different names, allowed conversion) → INFO.
Incompatible remaps → ERROR.
"""
from __future__ import annotations

from typing import Literal

# DB2 base type → allowed Azure SQL base types (precision/length ignored for presence of remap).
# Aligns with conversion utility: INTEGER→INT, CLOB→VARCHAR(MAX), BLOB→VARBINARY(MAX), etc.
DB2_TO_AZURE_TYPE_MAP: dict[str, list[str]] = {
    "INTEGER": ["int"],
    "INT": ["int"],
    "SMALLINT": ["smallint"],
    "BIGINT": ["bigint"],
    "VARCHAR": ["varchar"],
    "LONG VARCHAR": ["varchar", "text"],
    "CHAR": ["char"],
    "CHARACTER": ["char"],
    "CLOB": ["varchar"],  # VARCHAR(MAX) catalogs as varchar
    "DBCLOB": ["nvarchar"],  # NVARCHAR(MAX)
    "GRAPHIC": ["nchar"],
    "VARGRAPHIC": ["nvarchar"],
    "TIMESTAMP": ["datetime2", "datetime"],  # DATETIME2(7)
    "DATE": ["date"],
    "TIME": ["time"],
    "DECIMAL": ["decimal"],
    "NUMERIC": ["numeric", "decimal"],
    "DOUBLE": ["float"],
    "FLOAT": ["float"],
    "REAL": ["real"],
    "BLOB": ["varbinary"],  # VARBINARY(MAX)
    "BINARY": ["binary"],
    "VARBINARY": ["varbinary"],
    "XML": ["xml"],
    "BOOLEAN": ["bit"],
    "DECFLOAT": ["float"],
    # VARCHAR/CHAR FOR BIT DATA → VARBINARY (see normalize + for_bit_data)
    "VARCHAR FOR BIT DATA": ["varbinary"],
    "CHAR FOR BIT DATA": ["varbinary"],
    "CHARACTER FOR BIT DATA": ["varbinary"],
}


def _base_type(type_name: str) -> str:
    """Strip length/precision: VARCHAR(100) → VARCHAR, DATETIME2(7) → DATETIME2."""
    s = (type_name or "").strip().upper()
    if not s:
        return ""
    # Collapse FOR BIT DATA forms used by some catalogs
    if "FOR BIT DATA" in s:
        if s.startswith("VAR") or "VARCHAR" in s:
            return "VARCHAR FOR BIT DATA"
        return "CHAR FOR BIT DATA"
    return s.split("(", 1)[0].strip()


def normalize_azure_base(azure_type: str) -> str:
    """Normalize Azure type for comparison (sys.types.name is usually bare: varchar, int)."""
    return _base_type(azure_type).lower()


def normalize_db2_base(db2_type: str) -> str:
    return _base_type(db2_type)


def get_expected_azure_types(db2_type: str) -> list[str]:
    """Return list of acceptable Azure SQL base types for a given DB2 type."""
    base = normalize_db2_base(db2_type)
    return list(DB2_TO_AZURE_TYPE_MAP.get(base, []))


def is_compatible_type(db2_type: str, azure_type: str) -> bool:
    """Return True if azure_type is an accepted conversion for db2_type."""
    return classify_type_mapping(db2_type, azure_type) != "mismatch"


def classify_type_mapping(
    db2_type: str,
    azure_type: str,
) -> Literal["exact", "mapped", "mismatch"]:
    """
    Classify a DB2 → Azure type pair.

    - exact: same logical type (e.g. VARCHAR→varchar, DATE→date)
    - mapped: conversion-utility rename (e.g. CLOB→varchar, INTEGER→int, TIMESTAMP→datetime2)
    - mismatch: not in the conversion map
    """
    db2_base = normalize_db2_base(db2_type)
    az_base = normalize_azure_base(azure_type)
    if not db2_base or not az_base:
        return "exact"

    allowed = get_expected_azure_types(db2_base)
    if not allowed:
        # Unknown DB2 type: don't fail the run
        return "exact" if db2_base.lower() == az_base else "mismatch"

    if az_base not in allowed and not any(az_base.startswith(a) for a in allowed):
        return "mismatch"

    # Same spelling after normalize (CHAR/char, VARCHAR/varchar)
    if db2_base.lower() == az_base:
        return "exact"
    # DB2 synonym that is the same Azure target as itself
    if db2_base in ("INT", "INTEGER") and az_base == "int":
        # INTEGER→int is a remap; INT→int is exact-ish
        return "mapped" if db2_base == "INTEGER" else "exact"
    if db2_base == "CHARACTER" and az_base == "char":
        return "mapped"

    return "mapped"
