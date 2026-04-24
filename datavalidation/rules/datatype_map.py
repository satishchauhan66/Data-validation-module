"""
DB2 to Azure SQL data type mapping rules.
Used by schema validator to classify type differences as match, warning, or error.
"""
from typing import Any

# Canonical mappings: DB2 type -> allowed Azure SQL equivalents (or pattern).
# If target type is in the list or matches pattern, consider it OK.
DB2_TO_AZURE_TYPE_MAP: dict[str, list[str]] = {
    "CHAR": ["char", "nchar"],
    "VARCHAR": ["varchar", "nvarchar"],
    "LONG VARCHAR": ["varchar", "nvarchar", "text"],
    "CLOB": ["varchar", "nvarchar", "nvarchar(max)", "varchar(max)", "text"],
    "GRAPHIC": ["nchar"],
    "VARGRAPHIC": ["nvarchar"],
    "DBCLOB": ["nvarchar(max)", "nvarchar"],
    "SMALLINT": ["smallint"],
    "INTEGER": ["int"],
    "INT": ["int"],
    "BIGINT": ["bigint"],
    "DECIMAL": ["decimal", "numeric"],
    "NUMERIC": ["numeric", "decimal"],
    "REAL": ["real"],
    "FLOAT": ["float", "real"],
    "DOUBLE": ["float"],
    "DATE": ["date", "datetime", "datetime2"],
    "TIME": ["time", "datetime2"],
    "TIMESTAMP": ["datetime", "datetime2", "datetimeoffset"],
    "BLOB": ["varbinary", "varbinary(max)", "image"],
    "CLOB": ["varchar(max)", "nvarchar(max)", "text"],
    "BINARY": ["binary"],
    "VARBINARY": ["varbinary"],
    "XML": ["xml"],
}


def get_expected_azure_types(db2_type: str) -> list[str]:
    """Return list of acceptable Azure SQL types for a given DB2 type."""
    # Normalize: uppercase, strip size in parens for lookup
    base = (db2_type or "").split("(")[0].strip().upper()
    return list(DB2_TO_AZURE_TYPE_MAP.get(base, []))


def is_compatible_type(db2_type: str, azure_type: str) -> bool:
    """Return True if azure_type is an accepted mapping for db2_type."""
    allowed = get_expected_azure_types(db2_type)
    if not allowed:
        return True  # Unknown DB2 type: don't fail
    at = (azure_type or "").strip().lower()
    return any(at.startswith(a) or a in at for a in allowed)
