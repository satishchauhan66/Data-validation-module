# Behavior: Extended Properties

## Purpose

Compare **extended properties** (e.g. **MS_Description**) on tables and columns between source and target. Typically used for table and column descriptions (comments). Report when property is missing on one side or value differs.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: TABLE (for table and column properties).

## Comparison logic

1. **Table pairs**: Matched tables.
2. **Table description**: Get extended property "MS_Description" (or equivalent) for each table in source and target. Compare; report mismatch or missing.
3. **Column description**: For each column in matched tables, get same property for the column. Compare; report mismatch or missing.
4. **Normalize**: Optional trim/whitespace normalization.

## Error codes

| Code | Meaning |
|------|--------|
| TABLE_DESCRIPTION_MISMATCH | Table-level description (extended property) missing or differs. |
| COLUMN_DESCRIPTION_MISMATCH | Column-level description (extended property) missing or differs. |

## Output (unified row)

- **ValidationType**: `extended_properties` or `ExtendedProperty`
- **Status**: `error`
- **ObjectType**: TABLE
- **ElementPath**: e.g. `Schema.Table` (table description) or `Schema.Table.Column` (column description)
- **DetailsJson**: JSON with object type (table/column), property name, source value, destination value.

## DB-specific notes

- **DB2**: Comments may be in SYSCAT.TABLES (REMARKS) and SYSCAT.COLUMNS (REMARKS) or equivalent.
- **Azure SQL**: `sys.extended_properties` (major_id, minor_id; class=1 for table, class=2 for column); name = 'MS_Description'. Map object_id and column_id to tables/columns.
