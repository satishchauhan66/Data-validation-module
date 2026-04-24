"""Output formatting and element path helpers."""


def element_path(schema: str, table: str, column: str | None = None, object_type: str = "TABLE") -> str:
    """Build a canonical element path for reporting (e.g. Schema.Table or Schema.Table.Column)."""
    base = f"{schema}.{table}"
    if column:
        return f"{base}.{column}"
    return base
