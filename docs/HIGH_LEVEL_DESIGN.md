# High-Level Design Document

## DB2 to Azure Migration Validation

**Version:** 1.0  
**Last Updated:** February 2025

---

## 1. Executive Summary

This project is a **migration validation platform** that compares and validates database schemas, data, and behavior between **IBM DB2** (source) and **Microsoft Azure SQL** (target), and optionally between **two Azure SQL databases** (source vs. target). It exposes a **FastAPI** REST API and uses **Apache PySpark** over JDBC to run comparisons at scale, producing CSV reports and structured JSON responses.

### 1.1 Purpose

- **Pre-migration (schema & structure):** Validate that all tables, columns, and objects exist on both source and target; confirm data type mapping, nullability, defaults, primary/foreign keys, and indexes before data or application usage.
- **Post-migration (data):** Validate row counts, checksums, and null/empty distributions to ensure data integrity.
- **Behavior / readiness:** Compare identity columns, sequences, triggers, routines, collation, and extended properties.
- **Operational:** Source DB utilities (e.g., stored procedure inventory), Azure SQL token generation, and managed-identity permission grants.

### 1.2 Schema & Structure Validations (Use Case)

Validations in this category detect **missing or structurally incorrect objects** before data migration or application usage:

| Validation | Description |
|------------|-------------|
| **Table & Column Count** | Verifies all tables and columns exist on both source and target |
| **Object Compare** | Presence and count of Tables, Views, Stored Procedures, Functions, Triggers, Indexes, Constraints |
| **Data Type Mapping** | Ensures DB2 data types are correctly mapped to Azure SQL equivalents |
| **Nullable / Not-Null** | Validates column nullability consistency |
| **Default Values** | Checks that default constraints exist and match logically |
| **Index Validation** | Compares index definitions (PK, unique, non-unique) |
| **Foreign Key Relationships** | Ensures FKs reference the correct target tables and columns |
| **Check Constraints** | Compares CHECK constraint definitions |
| **Run all schema validations** | Runs presence, column counts, nullable, datatype mapping, default values, indexes, foreign keys, check constraints |

**Outcome:** Detects missing or structurally incorrect objects before data migration or application usage.

### 1.3 Data Validations (Use Case)

Validations in this category verify **data integrity** between source and target (row counts, content, and integrity rules):

| Validation | Description |
|------------|-------------|
| **Row Count** | Compares row counts per table/view between source and target |
| **Column Null Check** | Compares null and empty-string counts per column for matching tables |
| **Distinct Key Check** | Validates distinct key (e.g. primary key) consistency between source and target |
| **Checksum Hash** | Per-table checksum/hash comparison to detect data drift |
| **Reference Integrity** | Verifies that FK values in child tables exist in the referenced parent tables on both sides |
| **Constraint Integrity** | Verifies that data satisfies NOT NULL, CHECK constraints, and length rules on both sides |
| **Run all data validations** | Runs row counts, column nulls, distinct key, checksum, reference integrity, constraint integrity |

### 1.4 Behavior Validations (Use Case)

Validations in this category verify **behavior and readiness** (identity, sequences, triggers, routines, collation, extended properties):

| Validation | Description |
|------------|-------------|
| **Identity & Sequence** | Validates identity column properties and standalone sequence definitions |
| **Identity Collision Check** | On DB2 (source): flags when child max(FK) > parent max(identity). On Azure (target): also checks IDENT_CURRENT and flags when identity is NULL or &lt; max(child). Detects Wave Job Detail–style duplicate key / orphan risk (pre- and post-migration). |
| **Collation / Encoding** | Validates database and column collation/encoding differences |
| **Triggers** | Validates trigger presence and definitions |
| **Routines** | Validates stored procedure and function definitions |
| **Extended Properties** | Validates table/column descriptions and extended properties |
| **Run all behavior validations** | Runs identity-sequence, identity-collision-check, collation, triggers, routines, extended-properties |

**Reference integrity vs identity collision (Wave Job Detail–style issues):**

- **Reference integrity** (data validation) answers: *Do existing child rows have a matching parent?* It runs a per-FK check: LEFT JOIN child to parent, count rows where parent key IS NULL. So if the child table has FK values (e.g. 664M) that do not exist in the parent (e.g. parent max 360M), reference integrity **should** report a non-zero broken count and fail. Run **reference integrity** (e.g. as part of “run all data validations”) to catch orphaned child rows.
- **Identity collision check** (behavior validation) answers: *Will the next identity value collide with existing child data?* It uses MAX(parent), MAX(child FK), and IDENT_CURRENT(parent). It flags when identity is NULL or when the next insert could duplicate an existing child FK.
- Both use the same FK discovery. A built-in fallback ensures the pair `USERID.WAVE_JOB_DETAIL_MSG` → `USERID.WAVE_JOB_DETAIL` is always included for both checks when USERID is in scope, so neither validation misses this pair due to metadata or schema filters.

---

## 2. System Overview

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Clients (HTTP / Swagger)                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FastAPI Application (app/main.py)                     │
│  • CORS, dotenv, router registration                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
          ┌─────────────────────────────┼─────────────────────────────┐
          ▼                             ▼                             ▼
┌──────────────────┐         ┌──────────────────┐         ┌──────────────────┐
│  API Routers      │         │  Dependencies    │         │  Core Config     │
│  (HTTP layer)     │         │  (DI, Auth)      │         │  (settings)      │
└────────┬──────────┘         └────────┬─────────┘         └──────────────────┘
         │                             │
         │    ┌────────────────────────┘
         ▼    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Service Layer                                       │
│  • Schema Validation (DB2→Azure, Azure→Azure)                                │
│  • Data Validation (row counts, checksums, nulls)                             │
│  • Behavior Validation (identity, sequences, triggers, routines, etc.)        │
└─────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              PySpark Schema Comparison (pyspark_schema_comparison.py)         │
│  • SparkSession, JDBC URLs, config loading                                    │
│  • Engine-specific queries (DB2 vs Azure SQL)                                 │
│  • Shared helpers: _read_tables, _build_jdbc_urls, AAD token                  │
└─────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Data Sources (JDBC / optional SQLAlchemy)                  │
│  • DB2 (ibm_db / DB2 JDBC driver)                                            │
│  • Azure SQL (ODBC 18 / SQL Server JDBC, AAD or password)                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Design Principles

- **REST API–first:** All comparison and validation flows are triggered via HTTP; no long-running batch CLI as the primary interface.
- **PySpark as engine:** Heavy lifting (metadata reads, row counts, checksums) is done in Spark over JDBC for scalability and consistent SQL dialect handling.
- **Dual mode:** Same concepts (schema, data, behavior) are implemented for **DB2 → Azure** and **Azure → Azure**, with shared response shapes (unified columns, CSV, JSON).
- **Configuration externalized:** DB connections come from JSON config files (`database_config.json`, `azure_database_config.json`) or environment variables; auth can be password or Azure AD (including MFA / device code).
- **Stateless services:** Each request gets a service instance (with optional bearer token); no server-side session store for comparison state.

---

## 3. Component Overview

### 3.1 Directory Structure

```
project root/
├── app/
│   ├── main.py                    # FastAPI app, router includes, load_dotenv
│   ├── core/
│   │   └── config.py             # Pydantic settings (Azure SQL env vars)
│   ├── db/
│   │   ├── db2_session.py        # DB2 SQLAlchemy engine/session (optional)
│   │   └── schema_comparison_session.py  # Azure SQL async engine (optional)
│   ├── routers/                   # HTTP API layer
│   │   ├── dependencies.py       # DI: get_schema_service, get_data_service, get_behavior_service, AAD token
│   │   ├── source_db.py          # Source DB utilities (e.g. stored procedures)
│   │   ├── unified_validation.py # Unified validation endpoint (source/target + validation type)
│   │   ├── db2_schema_comparison.py
│   │   ├── db2_data_validation.py
│   │   ├── behavior_validation.py
│   │   ├── azure_schema_validation.py
│   │   ├── azure_data_validation.py
│   │   ├── azure_behavior_validation.py
│   │   └── azure_permissions.py  # Managed identity grants
│   ├── services/                  # Business logic
│   │   ├── pyspark_schema_comparison.py  # Base Spark + JDBC + engine-specific queries
│   │   ├── schema_validation_service.py  # Schema checks (presence, definitions, column counts)
│   │   ├── data_validation_service.py    # Row counts, checksums, null checks
│   │   ├── behavior_validation_service.py # Identity, sequences, triggers, routines, collation
│   │   └── db2_schema_comparison.py      # Legacy/alternate schema comparison (if used)
│   ├── schemas/                   # Pydantic request/response + shared column definitions
│   │   ├── common.py             # get_unified_columns, ensure_all_columns_as_strings
│   │   ├── unified_validation.py # Unified request/response models
│   │   └── db2_schema_comparison.py
│   └── utils/
│       └── element_path.py       # Formatting for element paths in reports
├── scripts/                       # Setup and run (cross-platform)
│   ├── setup.sh                  # Entry point: detects OS, runs setup-linux or setup-macos
│   ├── setup-linux.sh            # Linux: Java, ODBC, venv, PySpark, .env/config templates
│   ├── setup-macos.sh            # macOS: Homebrew Java/ODBC, venv, PySpark, config templates
│   ├── setup.ps1                 # Windows: winget Java/ODBC/VC++ Redist, venv, PySpark, config templates
│   ├── run.sh                    # Linux/macOS: set JAVA_HOME, activate venv, run uvicorn
│   ├── run.ps1                   # Windows: load .env (JAVA_HOME, HADOOP_HOME, JDBC JARs), activate venv, uvicorn
│   ├── verify-setup.sh           # Linux/macOS: check Python, Java, PySpark, .env, config files, ODBC
│   └── verify-setup.ps1         # Windows: same checks + winutils/hadoop.dll, SparkSession test, MSSQL_JDBC_JAR
├── docs/
│   └── HIGH_LEVEL_DESIGN.md      # This document
├── env.example                   # Template for .env (copy to .env); Azure, DB2, JDBC, tuning vars
├── database_config.json.example  # Template for DB2→Azure connections (copy to database_config.json)
├── azure_database_config.json.example  # Template for Azure→Azure (copy to azure_database_config.json)
├── requirements.txt              # Python dependencies (PySpark, FastAPI, JDBC libs, etc.)
├── .env                          # Local env (gitignored); created from env.example by setup scripts
├── database_config.json         # DB2 + Azure SQL connection details (gitignored)
├── azure_database_config.json   # Source/target Azure SQL for Azure→Azure (gitignored)
├── inputs/                       # Optional input artifacts (gitignored)
└── outputs/                      # Generated CSV reports (gitignored)
```

### 3.2 Layer Responsibilities

| Layer | Responsibility |
|-------|----------------|
| **Routers** | Parse request body (e.g. `SchemaComparisonRequest`), call service, map results to HTTP (JSON/CSV/FileResponse), handle 4xx/5xx. |
| **Dependencies** | Resolve `PySparkSchemaValidationService`, `PySparkDataValidationService`, `PySparkBehaviorValidationService` (and Azure variants); optional Bearer token for AAD. |
| **Services** | Implement comparison logic: call base `pyspark_schema_comparison` for JDBC and engine detection, run presence/definition/row/checksum/behavior queries, return Spark DataFrames. |
| **PySpark base** | Load config, build JDBC URLs (DB2 + Azure SQL), create SparkSession, implement `_read_tables`, `_build_jdbc_urls`, `_side_engine`, Azure token acquisition (password / managed identity / AAD MFA). |
| **Schemas** | Pydantic models for API; `common` defines unified CSV column names and string casting for consistent report output. |

---

## 4. API Surface

### 4.1 Router Prefixes and Tags

| Prefix | Tag | Description |
|--------|-----|-------------|
| `/api` | Default | Token generation (`/api/azure/sql/token`), dependencies |
| `/api/unified-validation` | Unified Validation | Single endpoint by source/target type and validation type (schema/data/behavior) |
| `/api/source-db` | Source DB Utilities | Stored procedures inventory, etc. |
| `/api/db2-schema-comparison` | DB2 Schema Comparison | Table/column count, object compare, datatype, nullable, defaults, indexes, FKs, check constraints (DB2→Azure) |
| `/api/db2-data-comparison` | DB2 Data Validation | Row counts, checksums, null checks (DB2→Azure) |
| `/api/db2-behavior-validation` | DB2 Behavior Validation | Identity, sequences, triggers, routines, collation (DB2→Azure) |
| `/api/azure-schema-validation` | Azure Schema Validation | Same schema validations (Azure→Azure) |
| `/api/azure-data-validation` | Azure Data Validation | Row counts, null checks, etc. (Azure→Azure) |
| `/api/azure-behavior-validation` | Azure Behavior Validation | Identity, triggers, routines, extended properties (Azure→Azure) |
| `/api` (azure_permissions) | Azure Permissions | Grant managed identity to Azure SQL databases |

### 4.2 Common Request Model

Many endpoints accept **SchemaComparisonRequest** (or variants):

- `source_schema` / `target_schema`: Optional; if one is set, both must be set (schema-level comparison).
- `object_types`: Optional list (e.g. TABLE, VIEW, PROCEDURE); default varies by endpoint.
- `include_definitions`: Optional (schema comparison).

### 4.3 Response Patterns

- **JSON:** Summary stats, file path of generated CSV, list of diff rows.
- **CSV download:** Unified columns; filename often includes timestamp and validation type (e.g. `db2_vs_azure_comparison_20250204.csv`).
- **Token endpoint:** `{ "access_token": "...", "expires_on": ... }`.

### 4.4 Authentication

- **Password:** Default; credentials from config or environment.
- **Azure AD (AAD):** When `AZURE_SQL_AUTH` is set to `aad_mfa` (or similar), callers must send `Authorization: Bearer <token>`. Token can be obtained from `GET /api/azure/sql/token` (optional device-code flow).
- **Dependencies:** `get_schema_service`, `get_data_service`, `get_behavior_service` (and Azure variants) resolve the appropriate service and optionally pass `access_token_override` into the service constructor.

---

## 5. Configuration and Security

### 5.1 Configuration Sources

- **database_config.json** (project root): Used for **DB2 → Azure** flows. Sections: `azure_sql`, `db2` (host, port, database, username, password, schema, driver, encrypt, trust_server_certificate, port).
- **azure_database_config.json**: Used for **Azure → Azure** flows. Sections: `source_azure_sql`, `target_azure_sql`.
- **Environment variables:** Override or supply Azure SQL settings (e.g. `AZURE_SQL_DRIVER`, `AZURE_SQL_ENCRYPT`, `AZURE_SQL_TENANT_ID`, `AZURE_SQL_AUTH_MODE`, `JAVA_HOME`, `JDBC_FETCHSIZE`, `SPARK_MASTER`). See `app/core/config.py` and `env.example`.

### 5.2 Security Considerations

- **Secrets:** Passwords and tokens should not be committed; use env vars or secure vaults in production.
- **TLS:** Azure SQL uses Encrypt=yes by default; `TrustServerCertificate` can be used for dev/test.
- **Managed identity:** Azure Permissions router can grant a managed identity access to Azure SQL databases (e.g. for app hosting in Azure).
- **Bearer token:** When using AAD, the token is passed per-request; no server-side token storage.

### 5.3 Rule-based and environment-driven behavior

Validation behavior and tuning are controlled by environment variables. Rules are parsed from comma-separated entries in the form `RULE_TYPE:PATTERN:MATCH_TYPE`; `MATCH_TYPE` can be `warning` (report as warning instead of error) or `ignore` (skip).

**Validation rule env vars (schema / comparison):**

| Env var | Used by | Purpose / rule types |
|---------|---------|----------------------|
| `DV_DTYPE_RULES` | Data type mapping | e.g. `varchar_to_varbinary:warning` — treat type differences as warning |
| `DV_DEFAULT_VALUE_RULES` | Default values | e.g. `bracket_equivalent::warning`, `function_equivalent::warning`, `missing_vs_numeric::warning` |
| `DV_INDEX_RULES` | Index validation | e.g. `column_order_insensitive:warning`, `missing_sysname:warning` |
| `DV_FK_RULES` | Foreign keys | e.g. `action_equivalent:warning` — FK action differences as warning |
| `DV_FK_ACTION_NORMALIZE` | FK comparison | `1`/`true` to normalize FK action names before comparing |
| `TYPE_LENIENCY_TS_SCALE_LTE_7` | Data type mapping | `1`/`true` to allow timestamp scale ≤7 as match |
| `DV_MANY_COLUMNS_THRESHOLD` | Index / column logic | Threshold (default 120) for “many columns” handling |

**Data validation and runtime tuning:**

| Env var | Purpose |
|---------|---------|
| `DV_REFINT_DISABLE_PUSHDOWN` | `1` disables reference-integrity pushdown to DB |
| `DV_CI_DISABLE_PUSHDOWN` | `1` disables constraint-integrity pushdown |
| `DV_NULLCHECK_*` | Null-check: timeout warn, skip rowcount filter, verbose, skip columns list, skip when metadata match |
| `DV_COL_AGG_*` | Column aggregation: workers, batch size (DB2/AZ), mode (per_table/per_column), timeout skip, error budget, cols per query |
| `DV_CI_WORKERS`, `DV_CI_ERROR_BUDGET` | Constraint integrity: parallelism and error budget |
| `DV_BROKEN_FK_WORKERS` | Reference integrity: worker count |
| `DV_DISTINCT_KEY_WORKERS`, `DV_CHECKSUM_WORKERS` | Distinct-key and checksum: worker count |
| `DV_CHECKSUM_MAX_MISMATCHES`, `DV_CHECKSUM_SAMPLE_LIMIT` | Checksum: cap on mismatches and sample limit |
| `DV_ALL_STOP_ON_ERROR`, `DV_ALL_ERROR_BUDGET`, `DV_ALL_PARALLEL`, `DV_ALL_PARALLEL_WORKERS` | Data validate-all: stop on first error, error budget, parallel run and workers |
| `DV_WORKER_ERROR_BUDGET` | Global worker error budget for data validations |
| `DV_DEBUG_SQL` | `1` to log SQL for debugging |

**JDBC / Spark (examples):** `JDBC_FETCHSIZE`, `JDBC_QUERY_TIMEOUT`, `MSSQL_JDBC_JAR`, `DB2_JDBC_JAR`, `SPARK_MASTER`, `SPARK_SHUFFLE_PARTITIONS`, `SPARK_NETWORK_TIMEOUT`, `SPARK_DEBUG_MAX_TOSTRING`.

**Azure auth:** `AZURE_SQL_AUTH` (`password` \| `managed_identity` \| `aad_mfa`), `AZURE_SQL_MI_CLIENT_ID`, `AZURE_SQL_TENANT_ID`, `AZURE_SQL_CLIENT_ID`, `AZURE_SQL_MFA_DEVICE_CODE`, `AZURE_SQL_AUTH_TIMEOUT`.

**Source DB (e.g. stored procedures):** `DB2_PORT`, `DB2_USERNAME`, `DB2_PASSWORD`, `SD_PROC_WORKERS`, `SD_PROC_ERROR_BUDGET`.

See `env.example` for sample values and comments.

---

## 6. Technology Stack

| Area | Technology |
|------|------------|
| API | FastAPI, Uvicorn |
| Validation engine | Apache PySpark 3.5.x |
| DB access | JDBC (DB2 JCC, Microsoft SQL Server JDBC), optional SQLAlchemy + aioodbc/pyodbc |
| Auth | Azure Identity (managed identity, device code, etc.), Bearer token |
| Config | JSON files, pydantic-settings, python-dotenv |
| Data handling | Pandas (optional), Pydantic for request/response |

### 6.1 Runtime Requirements

- **Python** 3.10+
- **JDK** 11 or 17 with `JAVA_HOME` set (required by PySpark).
- **Windows only:** `HADOOP_HOME` with `winutils.exe` and `hadoop.dll` in `%HADOOP_HOME%\bin` (e.g. `C:\hadoop\bin`); **Visual C++ Redistributable** 2015–2022 (x64).
- **ODBC Driver 18** for SQL Server (Azure SQL).
- **Microsoft SQL Server JDBC Driver** (optional): set `MSSQL_JDBC_JAR` in `.env` to the full path of the JAR; otherwise Spark pulls it via Maven.
- **DB2:** `ibm_db` / `ibm_db_sa` for SQLAlchemy path; DB2 JDBC driver for Spark path (when using DB2→Azure).

### 6.2 Setup (script-based)

Setup is automated per platform. From the **project root**:

**Linux or macOS**

```bash
chmod +x scripts/*.sh
./scripts/setup.sh
```

- **setup.sh** detects the OS and runs **setup-linux.sh** or **setup-macos.sh**. These install Java (apt/dnf or Homebrew), ODBC Driver 18, create a Python venv, run `pip install -r requirements.txt` (includes PySpark), and copy `env.example` → `.env` and config examples → `database_config.json` / `azure_database_config.json` if missing.
- Then edit `.env` and the config JSON files with your credentials. Run **./scripts/verify-setup.sh** to check the environment, then **./scripts/run.sh** to start the app.

**Windows (PowerShell)**

```powershell
.\scripts\setup.ps1
```

- If you see an execution policy error: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`.
- **setup.ps1** uses **winget** to install OpenJDK 11/17, ODBC Driver 18, and Visual C++ Redistributable 2015–2022 x64 if missing; creates a venv; installs dependencies; copies `env.example` → `.env` and config examples if missing. It sets `JAVA_HOME` (and `HADOOP_HOME` if `C:\hadoop\bin\winutils.exe` exists) for the session.
- Put `JAVA_HOME`, `HADOOP_HOME`, and `MSSQL_JDBC_JAR` (and optionally `DB2_JDBC_JAR`) in **.env** so **run.ps1** and the app pick them up; **run.ps1** loads these from `.env` before starting the app.
- Then run **.\scripts\verify-setup.ps1** to verify, and **.\scripts\run.ps1** to start the app.

**Config and env**

- **.env** — Copy from `env.example`; set Azure/DB2 and tuning variables. On Windows, set `JAVA_HOME`, `HADOOP_HOME`, `MSSQL_JDBC_JAR` here so run.ps1 and the app use them.
- **database_config.json** — Copy from `database_config.json.example`; fill in `azure_sql` and `db2` connection details for DB2→Azure.
- **azure_database_config.json** — Copy from `azure_database_config.json.example` for Azure→Azure flows.

**Run the app**

- **Linux/macOS:** `./scripts/run.sh` (or `source venv/bin/activate` then `python -m uvicorn app.main:app --reload`).
- **Windows:** `.\scripts\run.ps1`. Use `$env:HOST='0.0.0.0'; .\scripts\run.ps1` to listen on all interfaces.

See **README.md** for full quick-setup and troubleshooting.

---

## 7. Data Flow (Typical Request)

1. **Client** sends `POST /api/db2-schema-comparison/compare-schemas` with `SchemaComparisonRequest`.
2. **Router** validates input (e.g. source_schema/target_schema pairing), calls `get_schema_service()` (which may use Bearer token), then calls `svc.compare_schema_presence(...)` and related methods.
3. **Schema validation service** loads config (DB2 + Azure), builds JDBC URLs, uses Spark JDBC to run engine-specific catalog queries (e.g. `sys.objects` on Azure, `SYSCAT.TABLES` on DB2), unions and compares results, adds `ValidationType`, `ErrorCode`, etc.
4. **Result** DataFrame is cast to unified string columns, written to CSV under `outputs/` (or similar), and summary JSON is returned (e.g. total differences, file path).
5. **Client** can download the CSV or parse the JSON for integration with CI/CD or reporting.

---

## 8. Deployment and Operations

- **Run locally:** Use `./scripts/run.sh` (Linux/macOS) or `.\scripts\run.ps1` (Windows), or after activating venv run `python -m uvicorn app.main:app --reload`.
- **Docs:** Swagger UI at `/docs`, ReDoc at `/redoc`.
- **Outputs:** CSV and optional artifacts under a configurable output directory (e.g. `outputs/`).
- **Logging:** Print-based timing and ad-hoc logs; can be replaced with structured logging (e.g. `logging` + JSON) for production.

---

## 9. Future Considerations

- **Structured logging:** Replace print with a logging framework and optional correlation IDs.
- **Async Spark:** Current Spark usage is synchronous per request; for very large catalogs, consider background jobs or chunked execution.
- **Caching:** Optional caching of catalog metadata (with TTL) to reduce repeated JDBC round-trips for the same schema.
- **Production config:** Move from file-based config to environment or secret manager for credentials; restrict file system write paths for CSV output.
- **Health checks:** Add `/health` or `/ready` that verifies Spark and optionally DB connectivity.
- **Add test cases:** Introduce unit and integration tests (e.g. for services, routers, and validation logic) with Pytest.

---

*This document describes the high-level design of the DB2 to Azure Migration Validation project. For API details, see the OpenAPI schema at `/docs` or the router and schema modules in the codebase.*
