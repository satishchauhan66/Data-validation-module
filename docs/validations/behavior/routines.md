# Behavior: Routines (Procedures / Functions)

## Purpose

Compare **stored procedures** and **user-defined functions** between source and target: presence and **definition body** (creation script or body text). Report routines missing in source or target, or with different definition.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: PROCEDURE, FUNCTION (list of routine types to compare).

## Comparison logic

1. **Routine list**: Enumerate procedures and functions in source and target (by schema and name).
2. **Match**: Match by schema + routine name (and type: procedure vs function).
3. **Definition**: For each routine, get definition text (body) from catalog.
4. **Normalize**: Optionally canonicalize (whitespace, comments, schema qualifiers) for comparison.
5. **Report**: Routine in source but not in target → missing in target; in target but not in source → missing in source; both present but definition differs → ROUTINE_MISMATCH.

## Error codes

| Code | Meaning |
|------|--------|
| ROUTINE_MISMATCH | Routine missing or definition differs. |

(ErrorDescription can distinguish "missing in source", "missing in target", "definition mismatch".)

## Output (unified row)

- **ValidationType**: `routines` or `Routine`
- **Status**: `error`
- **ObjectType**: PROCEDURE or FUNCTION
- **ElementPath**: e.g. `Schema.RoutineName`
- **DetailsJson**: JSON with routine name, type, source_definition, destination_definition (or equivalent).

## DB-specific notes

- **DB2**: Routine text from SYSCAT.ROUTINES, SYSCAT.PROCEDURES, SYSCAT.FUNCTIONS or equivalent (TEXT column or separate view).
- **Azure SQL**: `sys.procedures`, `sys.sql_expression_dependencies`; definition from `OBJECT_DEFINITION(object_id)` or `sys.sql_modules`.
