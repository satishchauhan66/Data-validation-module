# Schema report comparison: new module vs old report

## Reference (old codebase report)
- **File:** e.g. `schema_validate_all_<dbname>_<timestamp>.csv` from the legacy tool
- **Lines:** varies by run (example: ~100+ data rows)

## New run (this module)
- **File:** e.g. `schema_validate_all_<dbname>_<timestamp>.csv` in `examples/` after a run
- **Lines:** varies by run and database state

---

## Format alignment

| Aspect | Old report | New report | Match |
|--------|------------|------------|--------|
| **Header** | ValidationType, Status, ObjectType, SourceObjectName, SourceSchemaName, DestinationObjectName, DestinationSchemaName, ElementPath, ErrorCode, ErrorDescription, DetailsJson | Same | Yes |
| **Presence DetailsJson** | `{"object_type":"SEQUENCE","change_type":"MISSING_IN_TARGET","source_schema_name":"USERID",...}` | Same structure | Yes |
| **ErrorDescription (TARGET_ONLY)** | "Object exists in Azure SQL but not in DB2" | Same | Yes |
| **ErrorCode** | PRESENCE_MISSING_IN_TARGET / PRESENCE_MISSING_IN_SOURCE | Same | Yes |
| **ObjectType** | SEQUENCE, INDEX, CONSTRAINT, TABLE | Same | Yes |
| **ElementPath** | e.g. `USERID.SQL190719113604520`, `dbo.SURVEY_RESPONSE_EVENT.SURVEY_RESPONSE_EVENT_SURVEYID_IDX` | Same pattern (schema.object_name) | Yes |

---

## Content differences (expected)

- **Row count:** Different runs and DB state produce different numbers of differences. Old report typically mixes presence, indexes, foreign keys, datatype_mapping, default_values, etc. New report includes presence + indexes + foreign_keys; `default_values` is a stub and not implemented, so row counts differ from a full legacy run.
- **Presence order/content:** Old report lists SEQUENCE (source only) first, then INDEX/CONSTRAINT (target only). New report may list TABLE (target only) first if no SEQUENCE under schema `USERID` in current DB2, or if table names don’t match (e.g. schema/catalog differences).
- **default_values:** Old report has many default_values rows; new module has a stub (no real default comparison yet), so no default_values rows in the new CSV.
- **datatype_mapping / indexes / foreign_keys:** Logic matches the old tool; row count and exact rows depend on current DB state and matching (e.g. case-insensitive object name matching is used for presence).

---

## Changes made for alignment

1. **Presence:** Uses literal `source_schema` (e.g. `USERID`) for presence queries so DB2 returns objects under that schema.
2. **Presence object types:** SEQUENCE, INDEX, CONSTRAINT included via `catalog_presence_sequences_query`, `catalog_presence_indexes_query`, `catalog_presence_constraints_query` (object_name format: `TableName.IndexName`, `TableName.ConstraintName`).
3. **Case-insensitive matching:** Presence compares by `(object_name.upper(), object_type)` so DB2 (uppercase) and Azure (mixed case) match.
4. **Legacy CSV:** DetailsJson for presence uses the same structure as the old report; ErrorCode/ErrorDescription and column order match.

To regenerate and compare: run `examples/quickstart.py` (with env vars set) and diff the new schema CSV against a legacy export from the same databases.
