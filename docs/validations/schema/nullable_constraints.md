# Schema: Nullable Constraints

## Purpose

For each **column** that exists in both source and target for matched tables, compare **nullable** vs **NOT NULL**. Report columns that are missing in source or target, or where nullability differs.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically `["TABLE"]`.

## Comparison logic

1. **Table pairs**: Matched tables (same logical table in both DBs).
2. **Column lists**: For each table, get list of columns with nullability (nullable = true/false) from both catalogs.
3. **Match columns**: Match by normalized column name (e.g. uppercase).
4. **Report**:
   - Column in source but not in target → missing in target.
   - Column in target but not in source → missing in source.
   - Both present but nullable differs → nullability mismatch.

## Error codes

| Code | Meaning |
|------|--------|
| NULLABILITY_MISMATCH | Column exists in both but nullable (YES/NO) differs. |

(Descriptions for “missing in source” / “missing in target” are free-form; ErrorCode can remain NULLABILITY_MISMATCH or be generalized.)

## Output (unified row)

- **ValidationType**: `nullable_constraints`
- **Status**: `error`
- **ObjectType**: TABLE
- **ElementPath**: e.g. `SourceSchema.SourceTable.ColumnName`
- **ErrorDescription**: e.g. "Column missing in source", "Column missing in target", "Nullable constraint mismatch".
- **DetailsJson**: JSON object with:
  - `column_name`: string  
  - `source_nullable`: boolean  
  - `destination_nullable`: boolean  

## DB-specific notes

- **DB2**: SYSCAT.COLUMNS or similar; nullability flag (e.g. NULLS = 'Y'/'N').
- **Azure SQL**: `sys.columns.is_nullable` (0/1) joined to tables/schemas.
