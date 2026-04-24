# Database Migration Validations — Specification

This folder defines **all database validations** implemented by this project in a **technology-agnostic** way so they can be reimplemented in any language or tech stack (e.g. Java, C#, Go, Node, Python with different runtimes).

## Purpose

- **Source of truth** for what validations exist and what each checks.
- **Portability**: implement the same checks against source and target DBs using any SQL client / runtime.
- **Unified report shape**: every validation produces rows that conform to the same output schema (below).

## Validation Types (Top Level)

| Type       | Description |
|------------|-------------|
| **schema** | Compare object and column metadata (presence, counts, types, defaults, indexes, FKs, checks, nullability). |
| **data**   | Compare row counts, null/empty counts, distinct keys, checksums/hashes, referential and constraint integrity on data. |
| **behavior** | Compare identity/sequence definitions, collation/encoding, triggers, routines, extended properties, identity collision risk. |

## Unified Output Schema (Per Row)

Every validation emits rows with these columns (order and names are canonical):

| Column | Type | Description |
|--------|------|-------------|
| ValidationType | string | Category name (e.g. `presence`, `row_counts`, `identity_sequence`). |
| Status | string | `error`, `warning`, or `info` (or equivalent). |
| ObjectType | string | e.g. `TABLE`, `VIEW`, `PROCEDURE`, `FUNCTION`, `INDEX`, `CONSTRAINT`. |
| SourceObjectName | string | Object name in source DB. |
| SourceSchemaName | string | Schema in source DB. |
| DestinationObjectName | string | Object name in target DB. |
| DestinationSchemaName | string | Schema in target DB. |
| ElementPath | string | Canonical path for the element (e.g. `SCHEMA.TABLE.COLUMN` or `SCHEMA.TABLE`). |
| ErrorCode | string | Machine-readable code (e.g. `ROW_COUNT_MISMATCH`, `PRESENCE_MISSING_IN_TARGET`). |
| ErrorDescription | string | Human-readable message. |
| DetailsJson | string | JSON object with category-specific detail (see each validation doc). |

Implementations in other languages should produce CSV or in-memory rows that match this schema so results can be merged and consumed uniformly.

## Scope (Source → Target)

- **Supported pairs**: Source = DB2 or Azure SQL; Target = Azure SQL.
- **Optional scope**: Validations can be scoped by `source_schema` / `target_schema` and by `object_types` (e.g. TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER, INDEX, CONSTRAINT, SEQUENCE, SYNONYM where applicable).

## Folder Layout

```
validations/
├── README.md                 # This file
├── INDEX.md                  # Master list of all categories and error codes
├── schema/                   # Schema validations
│   ├── README.md
│   ├── presence.md
│   ├── table_column_count.md
│   ├── nullable_constraints.md
│   ├── datatype_mapping.md
│   ├── default_values.md
│   ├── indexes.md
│   ├── foreign_keys.md
│   └── check_constraints.md
├── data/                     # Data validations
│   ├── README.md
│   ├── row_count.md
│   ├── column_null_check.md
│   ├── distinct_key_check.md
│   ├── checksum_hash.md
│   ├── reference_integrity.md
│   └── constraint_integrity.md
└── behavior/                 # Behavior validations
    ├── README.md
    ├── identity_sequence.md
    ├── collation_encoding.md
    ├── triggers.md
    ├── routines.md
    ├── extended_properties.md
    └── identity_collision_check.md
```

## How to Use This Spec

1. Read **INDEX.md** for the full list of validations and error codes.
2. For each category you want to implement, open the corresponding file under `schema/`, `data/`, or `behavior/`.
3. Each doc describes: **purpose**, **inputs**, **comparison logic**, **error codes**, **DetailsJson** shape, and any DB-specific notes (e.g. catalog queries for DB2 vs Azure).
