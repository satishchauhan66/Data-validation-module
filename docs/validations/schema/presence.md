# Schema: Presence

## Purpose

Compare which **objects** exist in source vs target, by object type. Report objects that exist in source but not in target, and objects that exist in target but not in source.

## Object types

- TABLE  
- VIEW  
- PROCEDURE  
- FUNCTION  
- TRIGGER  
- INDEX  
- CONSTRAINT  
- SEQUENCE  
- SYNONYM  

(INDEX and CONSTRAINT are typically compared for DB2→Azure presence; target catalog may expose them differently.)

## Inputs

- **source_schema**, **target_schema** (optional): Scope to these schemas.
- **object_types**: List of types to compare. If omitted, use a sensible default (e.g. all of the above).

## Comparison logic

1. **Enumerate source**: For each object type, query source catalog to list `(schema_name, object_name, object_type)`.
2. **Enumerate target**: Same for target catalog.
3. **Normalize names**: Use consistent normalization (e.g. uppercase) for schema and object names when matching.
4. **Left-only**: Objects in source not in target → **MISSING_IN_TARGET**.
5. **Right-only**: Objects in target not in source → **MISSING_IN_SOURCE**.

## Error codes

| Code | Meaning |
|------|--------|
| PRESENCE_MISSING_IN_TARGET | Object exists in source but not in target. |
| PRESENCE_MISSING_IN_SOURCE | Object exists in target but not in source. |
| PRESENCE_DIFFERENCE | Generic presence difference. |

## Output (unified row)

- **ValidationType**: `presence`
- **Status**: `error`
- **ObjectType**: e.g. TABLE, VIEW, PROCEDURE, …
- **SourceSchemaName**, **SourceObjectName**: Filled for source-only; empty for target-only.
- **DestinationSchemaName**, **DestinationObjectName**: Filled for target-only; empty for source-only.
- **ElementPath**: e.g. `SCHEMA.OBJECT_NAME` or `SCHEMA.OBJECT_NAME` for target-only.
- **ErrorCode**, **ErrorDescription**: As above.
- **DetailsJson**: JSON object with:
  - `object_type`: string  
  - `change_type`: `MISSING_IN_TARGET` | `MISSING_IN_SOURCE`  
  - `source_schema_name`: string (optional)  
  - `source_object_name`: string (optional)  
  - `destination_schema_name`: string (optional)  
  - `destination_object_name`: string (optional)  

## DB-specific notes

- **DB2**: Catalog views per type (e.g. SYSCAT.TABLES, SYSCAT.VIEWS, SYSCAT.PROCEDURES, SYSCAT.FUNCTIONS, SYSCAT.TRIGGERS, indexes/constraints from catalog). Filter by schema (e.g. TABSCHEMA).
- **Azure SQL**: `sys.objects` (and type filters), `sys.schemas`; indexes/constraints via `sys.indexes`, `sys.key_constraints`, `sys.foreign_keys`, etc. Filter by `schema_id` / `SCHEMA_NAME(schema_id)`.
