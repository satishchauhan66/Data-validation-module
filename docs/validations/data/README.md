# Data Validations

Data validations compare **actual data** (row counts, values, hashes, integrity) between source and target. They require read access to tables and may run heavy queries.

## Categories

| Category | Purpose |
|----------|--------|
| [row_count](row_count.md) | Compare total row count per table/view. |
| [column_null_check](column_null_check.md) | Compare NULL and empty-string counts per column. |
| [distinct_key_check](distinct_key_check.md) | Compare distinct key count and detect duplicates on primary (or configured) key. |
| [checksum_hash](checksum_hash.md) | Compare per-row hashes by key; optionally unordered set hash. |
| [reference_integrity](reference_integrity.md) | Find child rows that have no matching parent (FK violation). |
| [constraint_integrity](constraint_integrity.md) | Find rows violating NOT NULL, length, check, date format, or numeric constraints. |

## Inputs (common)

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically TABLE (and optionally VIEW) for row_count; TABLE for others.
- **Table pairs**: Only tables present in both source and target (after schema/name mapping) are validated.

## Output

Each category produces rows conforming to the [unified output schema](../README.md#unified-output-schema-per-row). **DetailsJson** is category-specific.

## Performance notes

- Row count and column null checks are relatively cheap (aggregates).
- Distinct key and checksum/hash can be expensive (full scan, sort/hash).
- Reference and constraint integrity run per-table queries (e.g. anti-join for orphans; validation queries). Consider batching and timeouts when reimplementing.
