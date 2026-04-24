# Behavior: Collation / Encoding

## Purpose

Compare **database-level** and **column-level** collation (and encoding where applicable) between source and target. Report when database default collation differs, or when column collation differs for matched columns.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: TABLE (for column collation).

## Comparison logic

1. **Database collation**: Get default collation of source DB and target DB. If different → DATABASE_COLLATION_MISMATCH (one row per DB or one row for the pair).
2. **Column collation**: For each character column in matched tables, get collation from source and target catalogs. If different → COLLATION_MISMATCH per column.

## Error codes

| Code | Meaning |
|------|--------|
| DATABASE_COLLATION_MISMATCH | Database default collation differs. |
| COLLATION_MISMATCH | Column collation differs. |

## Output (unified row)

- **ValidationType**: `collation_encoding` or `CollationEncoding`
- **Status**: `error`
- **ObjectType**: Database or TABLE
- **ElementPath**: e.g. database name or `Schema.Table.Column`
- **DetailsJson**: JSON with source/destination collation names (and encoding if applicable).

## DB-specific notes

- **DB2**: Collation from catalog (e.g. COLINFO or column attributes); encoding from DB/code page.
- **Azure SQL**: `DATABASEPROPERTYEX(db, 'Collation')`; column collation from `sys.columns` + `sys.types` or collation_name in catalog.
