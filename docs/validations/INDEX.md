# Master Index: All Validations and Error Codes

Quick reference for every validation category and the error codes each can produce. Use this to ensure parity when reimplementing in another language/stack.

---

## Schema validations

| Category | Error codes | Description |
|----------|-------------|-------------|
| [presence](schema/presence.md) | `PRESENCE_MISSING_IN_TARGET`, `PRESENCE_MISSING_IN_SOURCE`, `PRESENCE_DIFFERENCE` | Objects in source/target by type (TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER, INDEX, CONSTRAINT, SEQUENCE, SYNONYM). |
| [table_column_count](schema/table_column_count.md) | `COLUMN_COUNT_MISMATCH` | Column count per table differs between source and target. |
| [nullable_constraints](schema/nullable_constraints.md) | `NULLABILITY_MISMATCH` | Nullable vs NOT NULL differs for a column. |
| [datatype_mapping](schema/datatype_mapping.md) | `DATATYPE_NAME_MISMATCH`, `DATATYPE_SIZE_MISMATCH`, `DATATYPE_MISMATCH` | Column type or size (precision/scale/length) differs from expected mapping. |
| [default_values](schema/default_values.md) | `DEFAULT_MISMATCH` | Default value expression differs. |
| [indexes](schema/indexes.md) | `INDEX_MISSING_IN_SOURCE`, `INDEX_MISSING_IN_TARGET`, `INDEX_COLUMNS_MISMATCH`, `INDEX_UNIQUENESS_MISMATCH`, `INDEX_MISMATCH` | Index presence, columns, or uniqueness differs. |
| [foreign_keys](schema/foreign_keys.md) | `FK_MISMATCH` | FK presence, referenced table, column pairs, or ON DELETE/UPDATE action differs. |
| [check_constraints](schema/check_constraints.md) | `CHECK_MISMATCH` | Check constraint presence or definition differs. |

---

## Data validations

| Category | Error codes | Description |
|----------|-------------|-------------|
| [row_count](data/row_count.md) | `ROW_COUNT_MISMATCH` | Row count per table/view differs. |
| [column_null_check](data/column_null_check.md) | `NULL_OR_EMPTY_MISMATCH` | NULL or empty-string counts per column differ. |
| [distinct_key_check](data/distinct_key_check.md) | `KEY_NOT_FOUND`, `COUNT_FAILED`, `DUPLICATES_IN_SOURCE`, `DUPLICATES_IN_TARGET`, `DISTINCT_COUNT_MISMATCH` | Distinct key count / duplicate detection using primary or configurable key. |
| [checksum_hash](data/checksum_hash.md) | `READ_FAILED`, `UNORDERED_HASH_MISMATCH`, `NO_KEY_NO_UNORDERED`, `MISSING_IN_TARGET`, `MISSING_IN_SOURCE`, `ROW_HASH_MISMATCH` | Per-key row hash comparison; optional unordered set hash. |
| [reference_integrity](data/reference_integrity.md) | `REF_INTEGRITY_IN_SOURCE`, `REF_INTEGRITY_IN_TARGET` | Child rows without matching parent (FK violation). |
| [constraint_integrity](data/constraint_integrity.md) | `NOT_NULL_VIOLATION_IN_SOURCE`, `NOT_NULL_VIOLATION_IN_TARGET`, `LENGTH_EXCEEDED_IN_SOURCE`, `LENGTH_EXCEEDED_IN_TARGET`, `CHECK_VIOLATION_IN_SOURCE`, `CHECK_VIOLATION_IN_TARGET`, `INVALID_DATE_FORMAT_IN_SOURCE`, `INVALID_DATE_FORMAT_IN_TARGET`, `NUMERIC_OVERFLOW_IN_SOURCE`, `NUMERIC_SCALE_ROUNDING_IN_SOURCE`, `NUMERIC_OVERFLOW_IN_TARGET`, `NUMERIC_SCALE_ROUNDING_IN_TARGET`, `NUMERIC_STRING_CONVERSION_FAILED_IN_SOURCE`, `NUMERIC_STRING_CONVERSION_FAILED_IN_TARGET` | Data-level violations: NOT NULL, length, check, date format, numeric overflow/scale, string-to-numeric conversion. |

---

## Behavior validations

| Category | Error codes | Description |
|----------|-------------|-------------|
| [identity_sequence](behavior/identity_sequence.md) | (varies by match/mismatch) | Identity column and sequence definitions (seed, increment, current value). |
| [collation_encoding](behavior/collation_encoding.md) | `DATABASE_COLLATION_MISMATCH`, `COLLATION_MISMATCH` | Database/collation and column collation comparison. |
| [triggers](behavior/triggers.md) | `TRIGGER_MISMATCH` | Trigger presence and definition. |
| [routines](behavior/routines.md) | `ROUTINE_MISMATCH` | Stored procedure/function presence and definition. |
| [extended_properties](behavior/extended_properties.md) | `TABLE_DESCRIPTION_MISMATCH`, `COLUMN_DESCRIPTION_MISMATCH` | MS_Description extended properties (table/column). |
| [identity_collision_check](behavior/identity_collision_check.md) | `IDENTITY_COLLISION_RISK` | Risk of identity/sequence collision after migration. |

---

## Object types (for schema presence / scoping)

Supported object types when scoping validations: `TABLE`, `VIEW`, `PROCEDURE`, `FUNCTION`, `TRIGGER`, `INDEX`, `CONSTRAINT`, `SEQUENCE`, `SYNONYM`. (INDEX and CONSTRAINT are used for schema presence in DB2→Azure.)
