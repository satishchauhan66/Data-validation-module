# Schema: Table Column Count

## Purpose

For each **table** that exists in both source and target (after schema/name mapping), compare the **number of columns**. Report tables where the counts differ.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically `["TABLE"]`.

## Comparison logic

1. **Table pairs**: Determine matched tables (same logical table in source and target, using schema/name mapping if applicable).
2. **Source column count**: Per table, count columns in source catalog.
3. **Target column count**: Per table, count columns in target catalog.
4. **Compare**: Emit a row when `source_column_count != target_column_count`.

## Error codes

| Code | Meaning |
|------|--------|
| COLUMN_COUNT_MISMATCH | Number of columns differs between source and target for this table. |

## Output (unified row)

- **ValidationType**: `column_counts`
- **Status**: `error`
- **ObjectType**: TABLE
- **ElementPath**: e.g. `SourceSchema.SourceTable`
- **DetailsJson**: JSON object with:
  - `source_column_count`: number  
  - `destination_column_count`: number  

## DB-specific notes

- **DB2**: Count columns from SYSCAT.COLUMNS (or equivalent) filtered by TABSCHEMA, TABNAME.
- **Azure SQL**: Count from `sys.columns` joined to `sys.tables` / `sys.schemas`, filtered by schema and table name.
