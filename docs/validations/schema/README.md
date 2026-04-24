# Schema Validations

Schema validations compare **metadata** between source and target databases: which objects exist, column counts, data types, defaults, indexes, foreign keys, check constraints, and nullability. They do **not** read or compare row data.

## Categories

| Category | Purpose |
|----------|--------|
| [presence](presence.md) | List objects by type (TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER, INDEX, CONSTRAINT, SEQUENCE, SYNONYM); report missing in source or target. |
| [table_column_count](table_column_count.md) | For each matched table, compare column count. |
| [nullable_constraints](nullable_constraints.md) | For each matched table/column, compare nullable vs NOT NULL. |
| [datatype_mapping](datatype_mapping.md) | For each column, compare source type to expected target type and size (precision/scale/length). |
| [default_values](default_values.md) | Compare default value expressions (with optional rule-based equivalence). |
| [indexes](indexes.md) | Compare index presence, columns, and uniqueness (including primary key). |
| [foreign_keys](foreign_keys.md) | Compare FK presence, referenced table/columns, ON DELETE/UPDATE actions. |
| [check_constraints](check_constraints.md) | Compare check constraint presence and definition. |

## Inputs (common)

- **source_schema** (optional): Filter source by schema name.
- **target_schema** (optional): Filter target by schema name. If one is provided, both should be.
- **object_types** (optional): Restrict to given types (e.g. TABLE only). For presence, default often includes TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER, INDEX, CONSTRAINT, SEQUENCE, SYNONYM.

## Output

Each category produces rows conforming to the [unified output schema](../README.md#unified-output-schema-per-row). **DetailsJson** is category-specific; see each validation doc.
