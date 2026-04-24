# Behavior: Triggers

## Purpose

Compare **trigger** definitions between source and target: presence and **definition body** (creation script or body text). Report triggers missing in source or target, or with different definition.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: TABLE (triggers are on tables).

## Comparison logic

1. **Table pairs**: Matched tables.
2. **Source triggers**: For each table, list triggers with name and definition (body text).
3. **Target triggers**: Same for target.
4. **Normalize**: Optionally canonicalize definition (e.g. trim, remove whitespace, normalize line endings) for comparison.
5. **Match**: Match by trigger name (or table+trigger name).
6. **Report**: Missing in source/target or definition mismatch.

## Error codes

(Use a single code with description; e.g. TRIGGER_MISMATCH with "Trigger missing in target", "Trigger missing in source", "Trigger definition mismatch".)

| Code | Meaning |
|------|--------|
| TRIGGER_MISMATCH | Trigger missing or definition differs. |

## Output (unified row)

- **ValidationType**: `triggers` or `Trigger`
- **Status**: `error`
- **ObjectType**: TABLE (or TRIGGER)
- **ElementPath**: e.g. `Schema.Table.TriggerName`
- **DetailsJson**: JSON with trigger name, source_definition, destination_definition (or equivalent).

## DB-specific notes

- **DB2**: Trigger body from SYSCAT.TRIGGERS or catalog view that stores text.
- **Azure SQL**: `sys.triggers`, definition from `OBJECT_DEFINITION(object_id)` or `sys.sql_modules`.
