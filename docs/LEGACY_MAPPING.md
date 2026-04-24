# Legacy Codebase Mapping

Reference: path to your clone of the legacy FastAPI + PySpark app (e.g. `db2-to-azure-migration-validation/app`).

This document maps the legacy FastAPI + PySpark validation app to the new **datavalidation** pip library so behavior and report format stay aligned.

---

## Unified report columns (same in both)

From legacy `app/schemas/common.py` → `datavalidation/results.py` (`to_legacy_csv`):

| Column | Legacy | New library |
|--------|--------|-------------|
| ValidationType | presence, row_counts, column_counts, … | Same; `table_presence` → **presence** |
| Status | error / ok | Same |
| ObjectType | TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER, INDEX, … | Same |
| SourceObjectName | object name (or empty if TARGET_ONLY) | Same |
| SourceSchemaName | schema (or empty if TARGET_ONLY) | Same |
| DestinationObjectName | object name (or empty if SOURCE_ONLY) | Same |
| DestinationSchemaName | schema (or empty if SOURCE_ONLY) | Same |
| ElementPath | e.g. USERID.TABLENAME | Same |
| ErrorCode | PRESENCE_MISSING_IN_TARGET, ROW_COUNT_MISMATCH, … | Same |
| ErrorDescription | "Object exists in source but not in target", … | Same |
| DetailsJson | {} for presence; `{"source_row_count", "destination_row_count"}` for row_counts | Same |

---

## Presence validation

| Legacy | New library |
|--------|-------------|
| `app/services/schema_validation_service.py` → `compare_schema_presence()` | `datavalidation/validators/schema.py` → `validate_table_presence()` |
| Matches by (object_name, object_type) when source_schema & target_schema set | Same (cross-schema by table/object name + type) |
| DB2: SYSIBM.SYSTABLES, SYSCAT.ROUTINES (P/F), SYSCAT.TRIGGERS, SYSCAT.INDEXES, … | `datavalidation/dialects/db2.py` → `catalog_objects_query()` (TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER) |
| Azure: sys.objects (U,V,P,FN,IF,TF), sys.triggers, sys.indexes, … | `datavalidation/dialects/azure_sql.py` → `catalog_objects_query()` |
| SOURCE_ONLY → empty Destination*; TARGET_ONLY → empty Source* | Same in `to_legacy_csv()` |

**Object types:** Legacy also supports INDEX, CONSTRAINT, SEQUENCE, SYNONYM. The new library currently supports TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER. INDEX/CONSTRAINT/SEQUENCE/SYNONYM can be added to `catalog_objects_query()` in both dialects if needed.

---

## Row count validation

| Legacy | New library |
|--------|-------------|
| `app/services/data_validation_service.py` → `compare_row_counts()` | `datavalidation/validators/data.py` → `validate_row_counts()` |
| Pairs by object_norm when source_schema & target_schema set | Same (common table names across schemas) |
| **Data report CSV:** only rows where row count differs (MISMATCH); SOURCE_ONLY/TARGET_ONLY are presence and not in data report | Same: `to_legacy_csv()` outputs only `row_counts` details with status MISMATCH |
| ValidationType = "row_counts", DetailsJson = source_row_count, destination_row_count | Same in `to_legacy_csv()` |
| ErrorCode = ROW_COUNT_MISMATCH, ErrorDescription = "Found mismatch in row-count validation" | Same |

---

## Config and entrypoint

| Legacy | New library |
|--------|-------------|
| `database_config.json` (db2, azure_sql), env vars, Bearer token | `ConnectionConfig` from dict / `from_file()` / `from_env()`; Azure token via auth=interactive or password |
| Routers call services, return JSON + CSV path | No server; `ValidationClient.validate_schema()` / `validate_data()` return `ValidationReport`; `report.to_legacy_csv(path)` writes CSV |

---

## File layout

| Legacy | New library |
|--------|-------------|
| `app/services/pyspark_schema_comparison.py` (Spark, JDBC) | `datavalidation/connectors/` + `datavalidation/dialects/` (SQLAlchemy, pyodbc, ibm_db) |
| `app/services/schema_validation_service.py` | `datavalidation/validators/schema.py` |
| `app/services/data_validation_service.py` | `datavalidation/validators/data.py` |
| `app/schemas/common.py` (get_unified_columns, ensure_all_columns_as_strings) | `datavalidation/results.py` (`ValidationReport.to_legacy_csv()`) |

---

## Adding INDEX / CONSTRAINT / SEQUENCE / SYNONYM (optional)

To match legacy presence for INDEX, CONSTRAINT, SEQUENCE, SYNONYM:

1. **DB2** (`dialects/db2.py`): Already have SYSCAT.INDEXES; add branches in `catalog_objects_query()` for INDEX (object_name = TABNAME.INDNAME), CONSTRAINT (SYSCAT.TABCONST / checks / defaults), SEQUENCE (SYSCAT.SEQUENCES), SYNONYM (if supported).
2. **Azure** (`dialects/azure_sql.py`): Add INDEX (sys.indexes → schema + table.name + index.name), CONSTRAINT (key_constraints, check_constraints, default_constraints), SEQUENCE (sys.sequences), SYNONYM (sys.synonyms) to `catalog_objects_query()`.
3. **Validator:** No change; it already uses whatever object types are returned by `catalog_objects_query()`.

This keeps the new library aligned with the legacy report format and behavior while using the simpler pip-install + function-call flow.
