# Data: Reference Integrity

## Purpose

For each **foreign key** defined in the target (or source) catalog, check that there are **no child rows without a matching parent** (i.e. no orphaned child rows). Run an anti-join or NOT EXISTS check: child rows for which no parent row exists. Report tables/FKs and count of violating rows.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically TABLE.
- **FK list**: From catalog: child table, parent table, child columns, parent columns (and schema names).

## Comparison logic

1. **Table pairs**: Matched tables.
2. **Enumerate FKs**: Get FK definitions (child table/schema, parent table/schema, child columns, parent columns) from source and/or target catalog. Typically use target catalog so we validate target DB integrity; or both.
3. **Per FK**:  
   - **Source side**: `SELECT COUNT(*) FROM child WHERE NOT EXISTS (SELECT 1 FROM parent WHERE parent.col1 = child.col1 AND ...)` (child columns not matching any parent row).  
   - **Target side**: Same query against target DB.  
4. **Report**: If broken_count > 0, emit one row per FK with ErrorCode indicating which side (source or target) has the violation.

## Error codes

| Code | Meaning |
|------|--------|
| REF_INTEGRITY_IN_SOURCE | Child rows without matching parent in **source** DB. |
| REF_INTEGRITY_IN_TARGET | Child rows without matching parent in **target** DB. |

## Output (unified row)

- **ValidationType**: `reference_integrity`
- **Status**: `error`
- **ObjectType**: TABLE
- **ElementPath**: e.g. `ChildSchema.ChildTable.FkName`
- **ErrorDescription**: e.g. "Child rows without matching parent: N"
- **DetailsJson**: JSON object with e.g. FK name, child/parent table, broken count (implementation-specific).

## DB-specific notes

- Use standard SQL: NOT EXISTS or LEFT JOIN parent ... WHERE parent.key IS NULL. Ensure join uses correct columns and schema/table names. Pushdown to DB is preferred for large tables.
