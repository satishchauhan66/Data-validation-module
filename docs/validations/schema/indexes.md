# Schema: Indexes

## Purpose

Compare **index** definitions between source and target for matched tables:
- Presence (index in source but not target, or vice versa).
- **Column list** and order.
- **Uniqueness** (unique vs non-unique).
- **Primary key** is treated as a special index; report if PK exists on one side but not the other, or if both lack a PK (informational).

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically `["TABLE"]`.

## Comparison logic

1. **Table pairs**: Matched tables.
2. **Source indexes**: For each table, list indexes with name, column list (ordered), uniqueness, and whether it is the primary key.
3. **Target indexes**: Same for target.
4. **Match indexes**: Match by normalized index name (or by “same” definition if names differ).
5. **Report**:
   - Index in source but not in target → INDEX_MISSING_IN_TARGET.
   - Index in target but not in source → INDEX_MISSING_IN_SOURCE.
   - Same index but column list differs → INDEX_COLUMNS_MISMATCH.
   - Same index but uniqueness differs → INDEX_UNIQUENESS_MISMATCH.
   - Primary key missing on one side → use ErrorDescription like "Primary key missing in target/source".
6. **Optional**: Tables with very high column count can be reported as informational (TableSize / info).

## Error codes

| Code | Meaning |
|------|--------|
| INDEX_MISSING_IN_SOURCE | Index exists in target but not in source. |
| INDEX_MISSING_IN_TARGET | Index exists in source but not in target. |
| INDEX_COLUMNS_MISMATCH | Index columns or order differ. |
| INDEX_UNIQUENESS_MISMATCH | Uniqueness differs. |
| INDEX_MISMATCH | Generic index mismatch. |

## Output (unified row)

- **ValidationType**: `indexes`
- **Status**: `error` (or `warning`/`info` for PK-absent-both or table size).
- **ObjectType**: TABLE
- **ElementPath**: e.g. `SourceSchema.SourceTable.IndexName` or table-level for PK.
- **DetailsJson**: JSON object with:
  - `index_name`: string  
  - `source_columns`: string (e.g. comma-separated list) or null  
  - `destination_columns`: string or null  
  - `source_unique`: boolean | null  
  - `destination_unique`: boolean | null  

## DB-specific notes

- **DB2**: Index definitions from SYSCAT.INDEXES, index columns from SYSCAT.INDEXCOLUSE; PK from SYSCAT.TABCONST / key columns.
- **Azure SQL**: `sys.indexes`, `sys.index_columns`; PK from `sys.key_constraints` (type = 'PK'). Column order by index_column_id.
