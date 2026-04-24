# Legacy reporting & comparison port

This library mirrors the **unified CSV shape** and parts of the **comparison logic** from the original FastAPI / PySpark service:

**Repository:** `db2-to-azure-migration-validation` (e.g. `milkyjain/db2-to-azure-migration-validation`)

| Original | This package |
|----------|----------------|
| `app/schemas/common.py` → `get_unified_columns()` | `datavalidation/reporting/unified_spec.py` → `UNIFIED_REPORT_COLUMNS` |
| `app/services/pyspark_schema_comparison.py` → `_build_table_pairs` | `datavalidation/reporting/cross_schema.py` → `build_table_pairs_from_catalog_rows` |
| `app/services/pyspark_schema_comparison.py` → `compare_index_definitions` | `datavalidation/reporting/index_comparison.py` → `compare_indexes_legacy` |
| JDBC `_fetch_db2_index_cols` / `_fetch_sql_index_cols` | `catalog_index_columns_query()` on DB2 / Azure SQL dialects |

## Index comparison

- Full outer join on `(schema_norm, table_norm, index_name, Kind)` with **column signatures** (ordered `colseq`, `COL A`/`COL D`).
- **Signature masking**: when the same definition exists under a different index name (e.g. DB2 `SQL…` vs Azure `PK_*`), rows are suppressed like the Spark `sig_pairs` logic.
- **PK presence** and **high column count** info rows (threshold `DV_MANY_COLUMNS_THRESHOLD`, default `120`).
- Optional rules via `DV_INDEX_RULES` JSON (same idea as `DV_INDEX_RULES` in the original), e.g. `[{"rule_type":"column_order_insensitive","match_type":"warning"}]`.

## Row counts / presence / other validators

Presence joins and USERID↔dbo behavior follow the same rules as `schema_validation_service.compare_schema_presence` (object name + type when both schemas are set). Other categories (datatype mapping, defaults, FKs) still use the lightweight validators; further parity can be added by porting the corresponding Spark methods into `datavalidation/reporting/`.
