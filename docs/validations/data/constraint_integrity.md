# Data: Constraint Integrity

## Purpose

Run **data-level** checks to find rows that **violate** schema constraints or expected conversion rules:

- **NOT NULL**: Rows where a NOT NULL column is NULL (source or target).
- **Length**: Rows where string length exceeds target column max length (source) or where target has values exceeding source max (target).
- **Check constraint**: Rows that violate a check constraint expression (source or target).
- **Date format**: Rows where a string column (mapped to date) has non-ISO or invalid date format (source or target).
- **Numeric overflow/scale**: Rows where numeric value would overflow target precision, or require rounding to fit target scale (source or target); and string columns that cannot convert to target decimal (source or target).

Each check is per table/column pair and reports **count** of violating rows.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically TABLE.
- **Table/column metadata**: Types, precision, scale, length, nullability, check expressions (from schema validation or catalog).

## Comparison logic (summary)

1. **NOT NULL**: For columns that are NOT NULL in target, count rows where column IS NULL (in source or target as applicable). Emit NOT_NULL_VIOLATION_IN_SOURCE / NOT_NULL_VIOLATION_IN_TARGET.
2. **Length**: For string columns, count rows where LEN(column) > max_length (or equivalent). Emit LENGTH_EXCEEDED_IN_SOURCE / LENGTH_EXCEEDED_IN_TARGET.
3. **Check**: For each check constraint, count rows where the check expression evaluates to false. Emit CHECK_VIOLATION_IN_SOURCE / CHECK_VIOLATION_IN_TARGET.
4. **Date format**: For columns mapped to date/datetime, count rows that do not match expected format (e.g. YYYY-MM-DD) or fail TRY_CONVERT. Emit INVALID_DATE_FORMAT_IN_SOURCE / INVALID_DATE_FORMAT_IN_TARGET.
5. **Numeric overflow**: Count rows where integer part exceeds target precision (e.g. ABS(value) >= 10^integer_digits). Emit NUMERIC_OVERFLOW_IN_SOURCE / NUMERIC_OVERFLOW_IN_TARGET.
6. **Numeric scale rounding**: Count rows where value <> ROUND(value, scale). Emit NUMERIC_SCALE_ROUNDING_IN_SOURCE / NUMERIC_SCALE_ROUNDING_IN_TARGET.
7. **Numeric string conversion**: For string columns mapped to numeric, count rows where TRY_CONVERT to decimal(precision, scale) fails. Emit NUMERIC_STRING_CONVERSION_FAILED_IN_SOURCE / NUMERIC_STRING_CONVERSION_FAILED_IN_TARGET.

(Exact logic may depend on which side you are validating; implement per-column rules from metadata.)

## Error codes

| Code | Meaning |
|------|--------|
| NOT_NULL_VIOLATION_IN_SOURCE | NULL in NOT NULL column (source). |
| NOT_NULL_VIOLATION_IN_TARGET | NULL in NOT NULL column (target). |
| LENGTH_EXCEEDED_IN_SOURCE | String length exceeds limit (source). |
| LENGTH_EXCEEDED_IN_TARGET | String length exceeds limit (target). |
| CHECK_VIOLATION_IN_SOURCE | Check constraint violated (source). |
| CHECK_VIOLATION_IN_TARGET | Check constraint violated (target). |
| INVALID_DATE_FORMAT_IN_SOURCE | Invalid/non-ISO date (source). |
| INVALID_DATE_FORMAT_IN_TARGET | Invalid/non-ISO date (target). |
| NUMERIC_OVERFLOW_IN_SOURCE | Numeric overflow vs target precision (source). |
| NUMERIC_SCALE_ROUNDING_IN_SOURCE | Requires rounding to fit target scale (source). |
| NUMERIC_OVERFLOW_IN_TARGET | Numeric overflow (target). |
| NUMERIC_SCALE_ROUNDING_IN_TARGET | Requires rounding to fit scale (target). |
| NUMERIC_STRING_CONVERSION_FAILED_IN_SOURCE | String not convertible to decimal (source). |
| NUMERIC_STRING_CONVERSION_FAILED_IN_TARGET | String not convertible to decimal (target). |

## Output (unified row)

- **ValidationType**: `constraint_integrity`
- **Status**: `error`
- **ObjectType**: TABLE
- **ElementPath**: e.g. `Schema.Table.Column` or `Schema.Table.ConstraintName`
- **DetailsJson**: JSON object with e.g. `column_name`, `source_precision`, `source_scale`, `target_precision`, `target_scale`, `expected_format`, `max_length`, `constraint_name`, `expression` as applicable.

## DB-specific notes

- **DB2**: Use REGEXP_LIKE for date format; scalar functions for length/precision. Check expressions may be in catalog.
- **Azure SQL**: Use TRY_CONVERT, LEN, CHECK constraints via catalog. Build dynamic SQL for each check type if needed.
