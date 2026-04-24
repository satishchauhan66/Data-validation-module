# Data: Row Count

## Purpose

For each **table** (or view) that exists in both source and target, compare the **total row count**. Report tables where counts differ, or where the table exists only on one side (source missing or target missing).

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically `["TABLE", "VIEW"]`.

## Comparison logic

1. **Table pairs**: Determine matched tables/views (same logical name in both DBs).
2. **Source count**: `SELECT COUNT(*) FROM source_schema.source_table` (per table).
3. **Target count**: `SELECT COUNT(*) FROM target_schema.target_table` (per table).
4. **Compare**: Emit a row when counts differ, or when one side has no rows (e.g. table missing on one side → "Source missing" or "Destination missing" with one count null/zero).

## Error codes

| Code | Meaning |
|------|--------|
| ROW_COUNT_MISMATCH | Row count differs between source and target for this table. |

## Output (unified row)

- **ValidationType**: `row_counts`
- **Status**: `error`
- **ObjectType**: TABLE (or VIEW)
- **ElementPath**: e.g. `Schema.TableName`
- **ErrorDescription**: e.g. "Found mismatch in row-count validation", "Source missing", "Destination missing".
- **DetailsJson**: JSON object with:
  - `source_row_count`: number | null  
  - `destination_row_count`: number | null  

## DB-specific notes

- Use standard `COUNT(*)`; ensure no filter (e.g. by schema/table name) is applied except for the intended table. Schema/table identifiers may need quoting (e.g. Azure `[schema].[table]`, DB2 `"SCHEMA"."TABLE"`).
