"""
Configuration models for datavalidation library.
Supports dict, YAML file, and environment variable sources.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

# Object kinds included in schema presence by default (matches legacy DB2→Azure schema report).
DEFAULT_SCHEMA_OBJECT_TYPES: list[str] = [
    "TABLE",
    "VIEW",
    "PROCEDURE",
    "FUNCTION",
    "TRIGGER",
    "INDEX",
    "CONSTRAINT",
    "SEQUENCE",
]


@dataclass
class ConnectionConfig:
    """Connection configuration for a single database (source or target)."""
    type: str  # "db2" | "azure_sql"
    host: str
    database: str
    username: str = ""
    password: str = ""
    port: int | None = None
    schema: str = ""
    auth: str = "password"  # "password" | "aad" | "managed_identity" | "interactive"
    client_id: str | None = None  # for user-assigned managed identity
    driver: str | None = None
    encrypt: bool = True
    trust_server_certificate: bool = False
    connect_timeout_seconds: int | None = None  # JDBC loginTimeout / pyodbc timeout

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ConnectionConfig:
        """Build from a plain dict (e.g. user config). For Azure SQL, if password is omitted, auth becomes 'interactive' (MSAL interactive MFA)."""
        type_ = str(d.get("type", "azure_sql")).lower()
        password = str(d.get("password", "")).strip()
        auth = str(d.get("auth", "password")).strip().lower()
        if type_ == "azure_sql" and not password and auth == "password":
            auth = "interactive"
        return cls(
            type=type_,
            host=str(d["host"]),
            database=str(d["database"]),
            username=str(d.get("username", "")),
            password=password,
            port=int(d["port"]) if d.get("port") is not None else None,
            schema=str(d.get("schema", "")),
            auth=auth,
            client_id=d.get("client_id"),
            driver=d.get("driver"),
            encrypt=bool(d.get("encrypt", True)),
            trust_server_certificate=bool(d.get("trust_server_certificate", False)),
            connect_timeout_seconds=(int(d["connect_timeout_seconds"]) if d.get("connect_timeout_seconds") else None),
        )


@dataclass
class ValidationOptions:
    """Options for validation runs.

    Row-count tuning (for very large tables):
        row_count_mode: 'exact' = always COUNT(*); 'estimate' = catalog stats (DB2 SYSCAT.TABLES.CARD,
            Azure sys.dm_db_partition_stats); 'auto' = exact for small tables, estimate for tables larger
            than ``large_table_threshold_bytes`` and whenever exact fails; 'skip' = never run row counts.
        large_table_threshold_bytes: in 'auto' mode, tables whose size on either side exceeds this are
            counted with estimates instead of COUNT(*). Default 50 GB.
        count_with_dirty_read: append ``WITH UR`` (DB2) / ``WITH (NOLOCK)`` (Azure SQL) to row counts so
            they don't wait on row locks taken by writers. Read-only and recommended for big tables. ON
            by default; set to False for strict isolation reads.
        row_count_timeout_seconds: best-effort per-table query timeout (JDBC ``setQueryTimeout`` and
            pyodbc ``connection.timeout``). When exact times out in ``auto`` mode, the validator falls
            back to the catalog estimate so the run completes.
        exclude_tables: case-insensitive table names to skip (e.g. archive/staging tables).
        estimate_tables: case-insensitive table names that always use the estimate path.
        estimate_tolerance_pct: when comparing two estimates, treat |s-t|/max(s,t) <= pct/100 as match.
    """
    parallel_workers: int = 4
    datatype_leniency: bool = False
    output_dir: str | Path | None = None
    object_types: list[str] = field(default_factory=lambda: list(DEFAULT_SCHEMA_OBJECT_TYPES))
    include_definitions: bool = False
    row_count_mode: str = "auto"  # 'exact' | 'estimate' | 'auto' | 'skip'
    large_table_threshold_bytes: int = 50 * 1024 ** 3
    count_with_dirty_read: bool = True
    row_count_timeout_seconds: int | None = None
    exclude_tables: list[str] = field(default_factory=list)
    estimate_tables: list[str] = field(default_factory=list)
    estimate_tolerance_pct: float = 1.0


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def connection_from_env(prefix: str) -> ConnectionConfig | None:
    """
    Build ConnectionConfig from environment variables.
    Prefix is 'DV_SOURCE' or 'DV_TARGET'.
    Required: DV_SOURCE_TYPE, DV_SOURCE_HOST, DV_SOURCE_DATABASE, etc.
    """
    t = _env(f"{prefix}_TYPE")
    if not t:
        return None
    return ConnectionConfig(
        type=t.lower(),
        host=_env(f"{prefix}_HOST"),
        database=_env(f"{prefix}_DATABASE"),
        username=_env(f"{prefix}_USERNAME"),
        password=_env(f"{prefix}_PASSWORD"),
        port=int(_env(f"{prefix}_PORT")) if _env(f"{prefix}_PORT") else None,
        schema=_env(f"{prefix}_SCHEMA"),
        auth=_env(f"{prefix}_AUTH") or "password",
        client_id=_env(f"{prefix}_CLIENT_ID") or None,
    )


def load_config_from_file(path: str | Path) -> tuple[ConnectionConfig, ConnectionConfig, ValidationOptions]:
    """
    Load source, target, and options from a YAML or JSON file.
    Returns (source_config, target_config, options).
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, encoding="utf-8") as f:
        if path.suffix.lower() in (".yaml", ".yml"):
            if not _HAS_YAML:
                raise ImportError("PyYAML is required for YAML config. pip install pyyaml")
            data = yaml.safe_load(f)
        else:
            import json
            data = json.load(f)

    source = ConnectionConfig.from_dict(data["source"])
    target = ConnectionConfig.from_dict(data["target"])
    opts_data = data.get("options", {})
    options = ValidationOptions(
        parallel_workers=int(opts_data.get("parallel_workers", 4)),
        datatype_leniency=bool(opts_data.get("datatype_leniency", False)),
        output_dir=opts_data.get("output_dir"),
        object_types=opts_data.get("object_types", list(DEFAULT_SCHEMA_OBJECT_TYPES)),
        include_definitions=bool(opts_data.get("include_definitions", False)),
        row_count_mode=str(opts_data.get("row_count_mode", "auto")).strip().lower() or "auto",
        large_table_threshold_bytes=int(opts_data.get("large_table_threshold_bytes", 50 * 1024 ** 3)),
        count_with_dirty_read=bool(opts_data.get("count_with_dirty_read", True)),
        row_count_timeout_seconds=(int(opts_data["row_count_timeout_seconds"]) if opts_data.get("row_count_timeout_seconds") else None),
        exclude_tables=list(opts_data.get("exclude_tables", []) or []),
        estimate_tables=list(opts_data.get("estimate_tables", []) or []),
        estimate_tolerance_pct=float(opts_data.get("estimate_tolerance_pct", 1.0)),
    )
    return source, target, options
