# Data: Column Null Check

## Purpose

For each **column** in matched tables, compare the number of **NULL** values and the number of **empty string** values (or equivalent) between source and target. Report columns where these counts differ.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically TABLE (and optionally VIEW).
- **only_when_rowcount_matches** (optional): If true, run only for tables where row count already matches; otherwise run for all matched tables.

## Comparison logic

1. **Table pairs**: Matched tables (and optionally views).
2. **Per column** (for columns present in both):  
   - Source: `COUNT(*)` where column IS NULL; `COUNT(*)` where column = '' (or TRIM(column) = '' for string type).  
   - Target: Same.  
   (Exact definition of "empty" may depend on type: e.g. empty string for char types.)
3. **Compare**: Emit a row when source null count != target null count, or source empty count != target empty count.

## Error codes

| Code | Meaning |
|------|--------|
| NULL_OR_EMPTY_MISMATCH | NULL count or empty-string count differs for this column. |

## Output (unified row)

- **ValidationType**: `column_nulls` (or `column_null_check`)
- **Status**: `error`
- **ObjectType**: TABLE
- **ElementPath**: e.g. `Schema.Table.Column`
- **DetailsJson**: JSON object with counts, e.g.:
  - `SourceNullCount`, `DestinationNullCount`  
  - `SourceEmptyCount`, `DestinationEmptyCount`  

(Exact key names may vary; document the shape in your implementation.)

## DB-specific notes

- **Empty string**: DB2 and Azure both support ''; for VARCHAR/NVARCHAR use consistent definition (e.g. NULL vs '' vs whitespace-only). Optional: treat trailing spaces differently per DB.
