# datavalidation

Lightweight Python library for **DB2-to-Azure** and **Azure-to-Azure** migration validation. No Java, no Spark, no REST server — install with pip and call functions from your app.

## Install

One install works for **DB2** and **Azure SQL** (including Azure AD / MFA):

```bash
pip install datavalidation
```

Optional: YAML config file support:

```bash
pip install datavalidation[yaml]
```

## Quick start

```python
from datavalidation import ValidationClient

client = ValidationClient(
    source={
        "type": "azure_sql",
        "host": "source-server.database.windows.net",
        "database": "SourceDB",
        "username": "user",
        "password": "secret",
    },
    target={
        "type": "azure_sql",
        "host": "target-server.database.windows.net",
        "database": "TargetDB",
        "username": "user",
        "password": "secret",
    },
)

# Single validation
result = client.validate_row_counts(schemas=("dbo", "dbo"))
print(result.summary)
print(result.passed)
df = result.to_dataframe()
result.to_csv("row_counts.csv")

# All schema validations
report = client.validate_schema(schemas=("dbo", "dbo"))
print(report.summary)
print(report.all_passed)
report.to_csv("schema_report.csv")

# Everything
report = client.validate_all(schemas=("dbo", "dbo"))
```

## Configuration

**Inline (dict):**

```python
client = ValidationClient(
    source={"type": "db2", "host": "...", "port": 50000, "database": "MYDB", "username": "u", "password": "p"},
    target={"type": "azure_sql", "host": "x.database.windows.net", "database": "MyDB", "username": "u", "password": "p"},
)
```

**Azure SQL with no password (MSAL interactive / MFA):**  
Omit `password` for Azure SQL to use Azure AD interactive (browser or device code).

```python
client = ValidationClient(
    source={
        "type": "azure_sql",
        "host": "myserver.database.windows.net",
        "database": "MyDB",
        "username": "you@company.com",
        # no "password" -> opens browser for Azure AD / Entra ID sign-in (MFA)
    },
    target={...},
)
# Or set explicitly: "auth": "interactive"
```

**From a YAML or JSON file:**

```python
client = ValidationClient.from_file("validation_config.yaml")
```

Example `validation_config.yaml`:

```yaml
source:
  type: azure_sql
  host: source.database.windows.net
  database: SourceDB
  username: user
  password: secret

target:
  type: azure_sql
  host: target.database.windows.net
  database: TargetDB
  username: user
  password: secret

options:
  parallel_workers: 4
  object_types: ["TABLE", "VIEW"]
```

**From environment variables:**

Set `DV_SOURCE_*` and `DV_TARGET_*` (e.g. `DV_SOURCE_TYPE`, `DV_SOURCE_HOST`, `DV_SOURCE_DATABASE`, `DV_SOURCE_USERNAME`, `DV_SOURCE_PASSWORD`, and same for `DV_TARGET_*`), then:

```python
client = ValidationClient.from_env()
```

## Validations

- **Schema:** table presence, column counts, datatype mapping, nullable, default values, indexes, foreign keys, check constraints.
- **Data:** row counts, column nulls, distinct keys, checksum, referential integrity, constraint integrity.
- **Behavior:** identity/sequence, identity collision, collation, triggers, routines, extended properties.

Some validations are implemented as stubs and can be extended later. Row counts, table presence, column counts, datatype mapping, nullable, indexes, foreign keys, and check constraints are implemented.

## Result API

- `result.passed` — bool  
- `result.summary` — str  
- `result.details` — list of dicts  
- `result.stats` — dict  
- `result.to_dict()` — dict  
- `result.to_dataframe()` — pandas DataFrame (requires `pandas`)  
- `result.to_csv(path)` — write CSV  

For reports (e.g. `validate_schema()`): `report.results`, `report.all_passed`, `report.summary`, `report.to_dict()`, `report.to_dataframe()`, `report.to_csv(path)`.

## Requirements

- Python 3.10+
- **Azure SQL:** ODBC Driver 18 for SQL Server (install from [Microsoft](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) if needed).
- **DB2:** The library uses native driver first; if unavailable (e.g. Windows), it falls back to JDBC and auto-downloads the driver. For the JDBC path, Java (JRE) must be installed.

See `docs/LIBRARY_ARCHITECTURE.md` for full architecture.
