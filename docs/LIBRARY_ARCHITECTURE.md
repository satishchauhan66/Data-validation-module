# Library Architecture: `datavalidation`

## A pip-installable Python module for DB2-to-Azure and Azure-to-Azure migration validation

**Version:** 2.0  
**Last Updated:** March 2026

---

## 1. Vision

Replace the current FastAPI + PySpark server with a **lightweight, pip-installable Python library** that users import and call directly:

```python
pip install datavalidation
```

```python
from datavalidation import ValidationClient

client = ValidationClient(
    source={"type": "db2", "host": "...", "port": 50000, "database": "MYDB", "username": "user", "password": "pass"},
    target={"type": "azure_sql", "host": "myserver.database.windows.net", "database": "MyDB", "username": "user", "password": "pass"}
)

# Run a single validation
result = client.validate_row_counts(schemas=("SRC_SCHEMA", "TGT_SCHEMA"))
print(result.summary)       # Quick text summary
print(result.to_dict())     # Structured dict for your app
print(result.to_dataframe()) # Pandas DataFrame
result.to_csv("row_counts.csv")  # Export

# Run all validations in a category
results = client.validate_schema()  # all schema validations
results = client.validate_data()    # all data validations
results = client.validate_all()     # everything
```

**No web server. No Java. No Spark. No JDBC JARs. No setup scripts.**

---

## 2. What Changes (Old vs New)

| Aspect | Old (FastAPI + PySpark) | New (pip library) |
|--------|------------------------|-------------------|
| **Interface** | REST API (HTTP calls) | Python function calls |
| **Engine** | PySpark over JDBC | SQLAlchemy + pyodbc/ibm_db (direct SQL) |
| **Java dependency** | Required (JDK 11/17) | Not needed |
| **Installation** | Clone repo + run setup scripts + configure 3 files | `pip install datavalidation` |
| **Configuration** | .env + database_config.json + azure_database_config.json | Python dict/dataclass or YAML/JSON file |
| **Output** | JSON HTTP response + CSV files on server | Python objects (dict, DataFrame, CSV export) |
| **Authentication** | Bearer token per HTTP request | Connection-level (password, AAD token, managed identity) |
| **Deployment** | Must run as a server process | Import in any Python script, notebook, or app |
| **Size** | ~300MB+ (PySpark alone) | ~20-30MB (SQLAlchemy + drivers) |
| **Scalability** | Spark distributed processing | Single-node but with connection pooling + threading |
| **Setup time** | 30-60 min (Java, Hadoop, drivers, config) | 2-5 min (pip install + configure) |

---

## 3. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    User's Application / Script                    │
│                                                                  │
│   from datavalidation import ValidationClient                    │
│   client = ValidationClient(source={...}, target={...})          │
│   result = client.validate_row_counts(schemas=("S", "T"))        │
│   result.to_dataframe()                                          │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                      Public API Layer                             │
│                                                                  │
│   ValidationClient          - Main entry point                   │
│   ├── validate_schema()     - All schema validations             │
│   ├── validate_data()       - All data validations               │
│   ├── validate_behavior()   - All behavior validations           │
│   ├── validate_all()        - Everything                         │
│   └── Individual methods:                                        │
│       ├── validate_table_presence()                              │
│       ├── validate_column_counts()                               │
│       ├── validate_datatype_mapping()                            │
│       ├── validate_nullable()                                    │
│       ├── validate_default_values()                              │
│       ├── validate_indexes()                                     │
│       ├── validate_foreign_keys()                                │
│       ├── validate_check_constraints()                           │
│       ├── validate_row_counts()                                  │
│       ├── validate_column_nulls()                                │
│       ├── validate_distinct_keys()                               │
│       ├── validate_checksum()                                    │
│       ├── validate_referential_integrity()                       │
│       ├── validate_constraint_integrity()                        │
│       ├── validate_identity_sequence()                           │
│       ├── validate_identity_collision()                          │
│       ├── validate_collation()                                   │
│       ├── validate_triggers()                                    │
│       ├── validate_routines()                                    │
│       └── validate_extended_properties()                         │
│                                                                  │
│   ValidationResult          - Returned by every method           │
│   ├── .passed (bool)                                             │
│   ├── .summary (str)                                             │
│   ├── .details (list[dict])                                      │
│   ├── .warnings (list[str])                                      │
│   ├── .errors (list[str])                                        │
│   ├── .stats (dict)         - counts, timings                    │
│   ├── .to_dict()                                                 │
│   ├── .to_dataframe()       - pandas DataFrame                   │
│   └── .to_csv(path)                                              │
│                                                                  │
│   ValidationReport          - Returned by category/all methods   │
│   ├── .results (dict[str, ValidationResult])                     │
│   ├── .all_passed (bool)                                         │
│   ├── .summary (str)                                             │
│   ├── .to_dict()                                                 │
│   ├── .to_dataframe()                                            │
│   └── .to_csv(path)                                              │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                      Validation Engine                            │
│                                                                  │
│   SchemaValidator           - Schema comparison logic            │
│   DataValidator             - Data comparison logic              │
│   BehaviorValidator         - Behavior comparison logic          │
│                                                                  │
│   Each validator:                                                │
│   ├── Generates engine-specific SQL (DB2 vs Azure SQL)           │
│   ├── Executes via connection adapters                           │
│   ├── Compares source vs target results                          │
│   └── Produces ValidationResult objects                          │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Connection Layer                               │
│                                                                  │
│   ConnectionAdapter (ABC)                                        │
│   ├── DB2Adapter            - ibm_db_sa / ibm_db + SQLAlchemy    │
│   ├── AzureSQLAdapter       - pyodbc + SQLAlchemy                │
│   └── (future adapters: PostgreSQL, MySQL, Oracle, etc.)         │
│                                                                  │
│   Each adapter provides:                                         │
│   ├── .execute(sql) -> list[dict]                                │
│   ├── .execute_df(sql) -> pd.DataFrame                           │
│   ├── .get_catalog_query(query_type) -> str                      │
│   ├── .test_connection() -> bool                                 │
│   └── .close()                                                   │
│                                                                  │
│   Auth strategies:                                               │
│   ├── Password (default)                                         │
│   ├── Azure AD token (azure-identity)                            │
│   └── Managed Identity                                           │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                    SQL Dialect Layer                              │
│                                                                  │
│   DB2Dialect                                                     │
│   ├── catalog_tables_query()     - SYSCAT.TABLES                 │
│   ├── catalog_columns_query()    - SYSCAT.COLUMNS                │
│   ├── catalog_indexes_query()    - SYSCAT.INDEXES                │
│   ├── catalog_fk_query()         - SYSCAT.REFERENCES             │
│   ├── row_count_query()                                          │
│   ├── checksum_query()                                           │
│   └── ...                                                        │
│                                                                  │
│   AzureSQLDialect                                                │
│   ├── catalog_tables_query()     - sys.objects + INFORMATION_SCHEMA│
│   ├── catalog_columns_query()    - sys.columns                   │
│   ├── catalog_indexes_query()    - sys.indexes                   │
│   ├── catalog_fk_query()         - sys.foreign_keys              │
│   ├── row_count_query()                                          │
│   ├── checksum_query()                                           │
│   └── ...                                                        │
└──────────────────────────────────────────────────────────────────┘
```

---

## 4. Package Structure

```
datavalidation/
├── __init__.py                    # Public exports: ValidationClient, ValidationResult, etc.
├── client.py                      # ValidationClient - main entry point
├── config.py                      # ConnectionConfig, ValidationOptions (dataclasses)
├── results.py                     # ValidationResult, ValidationReport
│
├── validators/
│   ├── __init__.py
│   ├── base.py                    # BaseValidator (shared logic)
│   ├── schema.py                  # SchemaValidator (all schema validations)
│   ├── data.py                    # DataValidator (all data validations)
│   └── behavior.py               # BehaviorValidator (all behavior validations)
│
├── connectors/
│   ├── __init__.py
│   ├── base.py                    # ConnectionAdapter ABC
│   ├── db2.py                     # DB2Adapter
│   └── azure_sql.py               # AzureSQLAdapter
│
├── dialects/
│   ├── __init__.py
│   ├── base.py                    # SQLDialect ABC
│   ├── db2.py                     # DB2-specific catalog/validation queries
│   └── azure_sql.py               # Azure SQL-specific catalog/validation queries
│
├── rules/
│   ├── __init__.py
│   ├── engine.py                  # Rule engine (warning/ignore/error classification)
│   ├── datatype_map.py            # DB2 -> Azure SQL type mapping rules
│   └── defaults.py                # Default rule sets
│
└── utils/
    ├── __init__.py
    ├── comparison.py              # DataFrame comparison helpers
    └── formatting.py              # Output formatting, element paths
```

**Total: ~15 files** (vs. 25+ files + 7 scripts in the old architecture)

---

## 5. Configuration (Simple)

### Option A: Python dict (inline)

```python
from datavalidation import ValidationClient

client = ValidationClient(
    source={
        "type": "db2",
        "host": "db2server.example.com",
        "port": 50000,
        "database": "PRODDB",
        "username": "admin",
        "password": "secret"
    },
    target={
        "type": "azure_sql",
        "host": "myserver.database.windows.net",
        "database": "MigratedDB",
        "username": "admin",
        "password": "secret"
    }
)
```

### Option B: From environment variables

```python
client = ValidationClient.from_env()  # reads DV_SOURCE_*, DV_TARGET_* env vars
```

---

## 6. Usage Examples

### 6.1 Basic: Run one validation

```python
from datavalidation import ValidationClient

client = ValidationClient(source={...}, target={...})

result = client.validate_row_counts(schemas=("MYSCHEMA", "dbo"))

if result.passed:
    print("All row counts match!")
else:
    print(f"Mismatches found: {result.stats['mismatch_count']}")
    for row in result.details:
        if row["status"] == "MISMATCH":
            print(f"  {row['table']}: source={row['source_count']}, target={row['target_count']}")
```

### 6.2 Run all schema validations

```python
report = client.validate_schema(schemas=("MYSCHEMA", "dbo"))

print(report.summary)
# Schema Validation: 6/8 passed, 2 failed
#   ✓ Table Presence
#   ✓ Column Counts
#   ✗ Datatype Mapping (3 mismatches)
#   ✓ Nullable Constraints
#   ✗ Default Values (1 mismatch)
#   ✓ Indexes
#   ✓ Foreign Keys
#   ✓ Check Constraints

report.to_csv("schema_validation_report.csv")
```

### 6.3 Run everything and get a DataFrame

```python
report = client.validate_all(schemas=("MYSCHEMA", "dbo"))
df = report.to_dataframe()

# Filter to failures only
failures = df[df["status"] == "FAIL"]
print(failures)
```

### 6.4 Use in a Jupyter Notebook

```python
from datavalidation import ValidationClient

client = ValidationClient.from_file("config.yaml")
report = client.validate_all(schemas=("SRC", "TGT"))

# Rich display in notebook
report.to_dataframe()  # renders as a nice table in Jupyter
```

### 6.5 Integration with a web app (Flask/Django/Streamlit)

```python
# In a Streamlit app
import streamlit as st
from datavalidation import ValidationClient

client = ValidationClient(source={...}, target={...})
result = client.validate_row_counts(schemas=("SRC", "TGT"))

st.metric("Tables Validated", result.stats["total_tables"])
st.metric("Mismatches", result.stats["mismatch_count"])
st.dataframe(result.to_dataframe())
```

### 6.6 Azure AD / Managed Identity

```python
client = ValidationClient(
    source={"type": "db2", ...},
    target={
        "type": "azure_sql",
        "host": "myserver.database.windows.net",
        "database": "MyDB",
        "auth": "managed_identity",        # no username/password needed
        "client_id": "xxxxxxxx-xxxx-..."   # optional for user-assigned MI
    }
)
```

---

## 7. Dependency Comparison

### Old (requirements.txt ~20 packages, ~400MB+)

```
pyspark==3.5.x          (~300MB, requires JDK)
fastapi
uvicorn
pydantic
sqlalchemy
pyodbc
ibm_db
ibm_db_sa
azure-identity
python-dotenv
pandas
... + JDBC JARs (DB2 JCC, MSSQL JDBC)
... + Java JDK 11/17
... + Hadoop winutils (Windows)
```

### New (requirements ~8 packages, ~25MB)

```
sqlalchemy>=2.0
pyodbc                  # Azure SQL connectivity
ibm_db                  # DB2 connectivity (optional extra)
ibm_db_sa               # DB2 SQLAlchemy dialect (optional extra)
azure-identity          # AAD auth (optional extra)
pandas                  # DataFrame output
pydantic>=2.0           # Config validation
pyyaml                  # YAML config support (optional)
```

Install with extras:

```bash
pip install datavalidation                      # Azure SQL only
pip install datavalidation[db2]                 # + DB2 support
pip install datavalidation[azure-auth]          # + AAD / managed identity
pip install datavalidation[all]                 # everything
```

---

## 8. Validation Coverage (Same as before, no loss)

All 19 validations from the old architecture are preserved:

### Schema (8 validations)
| # | Method | Description |
|---|--------|-------------|
| 1 | `validate_table_presence()` | Tables/views exist on both sides |
| 2 | `validate_column_counts()` | Column counts match per table |
| 3 | `validate_datatype_mapping()` | DB2 types correctly mapped to Azure SQL |
| 4 | `validate_nullable()` | Nullability consistency |
| 5 | `validate_default_values()` | Default constraints match |
| 6 | `validate_indexes()` | Index definitions match |
| 7 | `validate_foreign_keys()` | FK references correct tables/columns |
| 8 | `validate_check_constraints()` | CHECK constraint definitions match |

### Data (6 validations)
| # | Method | Description |
|---|--------|-------------|
| 9 | `validate_row_counts()` | Row counts per table |
| 10 | `validate_column_nulls()` | Null/empty counts per column |
| 11 | `validate_distinct_keys()` | Distinct key consistency |
| 12 | `validate_checksum()` | Hash comparison for data drift |
| 13 | `validate_referential_integrity()` | FK values exist in parent tables |
| 14 | `validate_constraint_integrity()` | Data satisfies NOT NULL, CHECK, length |

### Behavior (6 validations)
| # | Method | Description |
|---|--------|-------------|
| 15 | `validate_identity_sequence()` | Identity columns and sequences |
| 16 | `validate_identity_collision()` | Next identity won't collide with child data |
| 17 | `validate_collation()` | Collation/encoding differences |
| 18 | `validate_triggers()` | Trigger presence and definitions |
| 19 | `validate_routines()` | Stored procedures and functions |
| 20 | `validate_extended_properties()` | Table/column descriptions |

---

## 9. Scalability: PySpark vs Direct SQL

| Scenario | PySpark (Old) | SQLAlchemy + Threading (New) |
|----------|---------------|------------------------------|
| **< 500 tables** | Overkill; Spark startup ~10s overhead | Fast, no overhead |
| **500-2000 tables** | Works but JDBC round-trips dominate | Threaded queries, comparable speed |
| **2000+ tables** | Spark parallelism helps | ThreadPoolExecutor with configurable workers |
| **Checksum on 100M+ rows** | Spark distributes hash computation | SQL-side HASHBYTES/CHECKSUM, no data movement |
| **Memory** | Spark JVM + Python (1GB+ baseline) | ~50-100MB typical |

Key insight: The heavy computations (row counts, checksums, aggregations) are **pushed down to the database engine via SQL** in both architectures. PySpark was essentially a SQL executor over JDBC -- the new approach does the same thing with lighter tooling.

---

## 10. Migration Path (Old -> New)

For teams currently using the REST API, provide a thin compatibility layer:

```python
# Optional: run the library as a REST API (for backward compatibility)
from datavalidation.server import create_app

app = create_app()  # Returns a FastAPI app with the same endpoints
# uvicorn datavalidation.server:create_app --factory
```

This is an **optional extra** (`pip install datavalidation[server]`), not the primary interface.

---

## 11. Technology Stack

| Area | Technology |
|------|------------|
| Language | Python 3.10+ |
| DB connectivity | SQLAlchemy 2.0 + pyodbc (Azure SQL) + ibm_db (DB2) |
| Auth | azure-identity (AAD, managed identity) |
| Config | pydantic dataclasses + optional YAML |
| Data handling | pandas (output), native dicts |
| Parallelism | concurrent.futures.ThreadPoolExecutor |
| Packaging | pyproject.toml, setuptools/hatch |
| Testing | pytest + pytest-mock |

---

## 12. Summary of Complexity Reduction

| Metric | Old | New | Reduction |
|--------|-----|-----|-----------|
| External runtime dependencies | 4 (Python, Java, Hadoop, ODBC) | 1 (Python + ODBC) | 75% |
| Setup time | 30-60 min | 2-5 min | 90% |
| Install size | ~400MB+ | ~25MB | 94% |
| Config files needed | 3 (.env, 2x JSON) | 0-1 (optional YAML) | 67-100% |
| Setup scripts | 7 | 0 | 100% |
| Lines of code to use | ~20 (HTTP client + parse response) | ~5 (import + call) | 75% |
| Files in package | 25+ source + 7 scripts | ~15 source | 50% |
| Server process required | Yes (uvicorn) | No | 100% |

---

*This document defines the target architecture for the `datavalidation` pip-installable Python library.*
