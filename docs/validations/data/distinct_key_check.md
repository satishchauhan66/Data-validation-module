# Data: Distinct Key Check

## Purpose

For each matched table, use a **key** (primary key columns, or a configured key) to:
1. Count **distinct keys** in source and in target.
2. Compare **total row count** vs **distinct key count** to detect **duplicates** (rows > distinct keys).
3. Report: no key found, count failed, duplicates in source, duplicates in target, or distinct count mismatch between source and target.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically TABLE.
- **Key**: Primary key from catalog, or configured list of columns. If no PK and no config, report KEY_NOT_FOUND.

## Comparison logic

1. **Table pairs**: Matched tables.
2. **Resolve key**: Get primary key columns for the table (source or target); if none, emit KEY_NOT_FOUND and stop for that table.
3. **Source**: `SELECT COUNT(*) AS row_count, COUNT(DISTINCT key_col1, key_col2, ...) AS distinct_count` (syntax varies by DB).
4. **Target**: Same.
5. **Compare**:
   - If source row_count > source distinct_count → DUPLICATES_IN_SOURCE.
   - If target row_count > target distinct_count → DUPLICATES_IN_TARGET.
   - If source distinct_count != target distinct_count → DISTINCT_COUNT_MISMATCH.
6. **Errors**: If count query fails (e.g. timeout), emit COUNT_FAILED with error message.

## Error codes

| Code | Meaning |
|------|--------|
| KEY_NOT_FOUND | No primary key (or configured key) for this table. |
| COUNT_FAILED | Count query failed (e.g. exception). |
| DUPLICATES_IN_SOURCE | Source has duplicate keys. |
| DUPLICATES_IN_TARGET | Target has duplicate keys. |
| DISTINCT_COUNT_MISMATCH | Distinct key count differs between source and target. |

## Output (unified row)

- **ValidationType**: `distinct_key`
- **Status**: `error`
- **ObjectType**: TABLE
- **ElementPath**: e.g. `Schema.Table`
- **DetailsJson**: JSON object with:
  - `key_columns`: array of column names  
  - `source_row_count`, `source_distinct_key_count` (when applicable)  
  - `target_row_count`, `target_distinct_key_count` (when applicable)  

## DB-specific notes

- **Distinct key**: DB2 and Azure support `COUNT(DISTINCT col1, col2, ...)` or equivalent (e.g. subquery with DISTINCT). Use consistent column order.
