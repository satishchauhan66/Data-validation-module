# Schema: Check Constraints

## Purpose

Compare **check constraint** definitions between source and target for matched tables: presence and **definition expression**. Report constraints missing in source or target, or with different definition text.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically `["TABLE"]`.

## Comparison logic

1. **Table pairs**: Matched tables.
2. **Source checks**: For each table, list check constraints: name, definition expression (text).
3. **Target checks**: Same for target.
4. **Normalize definition**: Optionally canonicalize (e.g. uppercase, remove whitespace, normalize brackets/parens) so equivalent expressions compare equal.
5. **Match**: Match by normalized constraint name (or by normalized definition if names differ).
6. **Report**:
   - Check in source but not in target → missing in target.
   - Check in target but not in source → missing in source.
   - Same name but definition differs → check constraint definition mismatch.

## Error codes

| Code | Meaning |
|------|--------|
| CHECK_MISMATCH | Check constraint missing or definition differs. |

(ErrorDescription: "Check constraint missing in source", "Check constraint missing in target", "Check constraint definition mismatch".)

## Output (unified row)

- **ValidationType**: `check_constraints`
- **Status**: `error`
- **ObjectType**: TABLE
- **ElementPath**: e.g. `SourceSchema.SourceTable.ConstraintName`
- **DetailsJson**: JSON object with:
  - `constraint_name`: string  
  - `source_definition`: string (expression text)  
  - `destination_definition`: string (expression text)  

## DB-specific notes

- **DB2**: Check definitions from SYSCAT.CHECKS or equivalent (text expression).
- **Azure SQL**: `sys.check_constraints`; definition from `OBJECT_DEFINITION(object_id)` or `sys.sql_expression_dependencies`. Normalize for comparison (e.g. schema-qualified names may differ).
