# Schema: Default Values

## Purpose

For each **column** in matched tables, compare **default value expressions** between source and target. Report differences, with optional rule-based equivalence (e.g. bracket vs no bracket, function equivalence).

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically `["TABLE"]`.
- **Rules** (optional): e.g. "bracket_equivalent" (treat `(value)` and `value` as same), "function_equivalent", "missing_vs_numeric" — with match type **ignore** or **warning**.

## Comparison logic

1. **Table/column pairs**: For each matched table, pair columns by normalized name.
2. **Source default**: Get default expression string from source catalog.
3. **Target default**: Get default expression string from target catalog.
4. **Normalize**: Optionally canonicalize (e.g. trim, uppercase, remove outer parens) for comparison.
5. **Compare**:
   - Exact match → no row.
   - If rules apply (e.g. bracket_equivalent, function_equivalent), may treat as **warning** or **ignore**.
   - Otherwise → **error**.
6. **Status**: `error` or `warning` depending on rules.

## Error codes

| Code | Meaning |
|------|--------|
| DEFAULT_MISMATCH | Default value expression differs (and not downgraded to warning by rules). |

## Output (unified row)

- **ValidationType**: `default_values`
- **Status**: `error` or `warning`
- **ObjectType**: TABLE
- **ElementPath**: e.g. `SourceSchema.SourceTable.ColumnName`
- **ErrorDescription**: e.g. "Default value mismatch", "Default value difference (treated as warning)".
- **DetailsJson**: JSON object with:
  - `column_name`: string  
  - `source_default`: string (expression)  
  - `destination_default`: string (expression)  

## DB-specific notes

- **DB2**: Default from SYSCAT.COLUMNS or catalog view that stores default text.
- **Azure SQL**: `sys.default_constraints` + definition; or default in `sys.columns` / generation; expression may be stored in different form. Normalize for comparison.
