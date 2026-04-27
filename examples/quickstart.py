"""
Quick start: schema, data, and behavior validations; writes legacy CSVs under
``outputs/<org>/<instance>/{schema-validation,data-validation,behavior-validation}/``
(relative to repo root).

**Configuration:** set environment variables (see repo root ``.env.example``).
Recommended: copy ``.env.example`` to ``.env`` at the repo root and run
``pip install python-dotenv`` so this script loads ``.env`` automatically.

Data validation defaults to **row counts only** (fast). Enable more checks with
``DV_DATA_VALIDATIONS`` (e.g. ``all`` or ``row_counts,distinct_keys,checksum``).

Override output layout with ``DV_OUTPUT_ORG`` / ``DV_OUTPUT_INSTANCE``.
"""
from __future__ import annotations

import os
import secrets
import sys
from datetime import datetime
from pathlib import Path

# Ensure repo root is on path when running: python quickstart.py from examples/
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from datavalidation import ValidationClient, ValidationOptions

OUTPUT_ORG = os.environ.get("DV_OUTPUT_ORG", "INFOQFIT")
OUTPUT_INSTANCE = os.environ.get("DV_OUTPUT_INSTANCE", "SS-DEV-DB21D")


def _load_dotenv_files() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore[import-untyped]
    except ImportError:
        return
    load_dotenv(_ROOT / ".env")
    load_dotenv(Path(__file__).resolve().parent / ".env")


def _env_required(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(
            f"Missing required environment variable {name!r}. "
            f"Copy {_ROOT / '.env.example'} to {_ROOT / '.env'}, fill in values, "
            "and install python-dotenv (`pip install python-dotenv`) to load .env automatically."
        )
    return v


def _env_optional(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()


def _build_options() -> ValidationOptions:
    """Pull row-count tuning from env vars so DB2 multi-TB tables don't time out."""
    mode = os.environ.get("DV_ROW_COUNT_MODE", "auto").strip().lower() or "auto"
    try:
        threshold_gb = float(os.environ.get("DV_LARGE_TABLE_THRESHOLD_GB", "50") or "50")
    except ValueError:
        threshold_gb = 50.0
    try:
        tol = float(os.environ.get("DV_ESTIMATE_TOLERANCE_PCT", "1.0") or "1.0")
    except ValueError:
        tol = 1.0
    dirty = (os.environ.get("DV_DIRTY_READ_COUNT", "true").strip().lower() not in ("0", "false", "no", "off"))
    try:
        q_timeout = int(os.environ.get("DV_ROW_COUNT_TIMEOUT_SEC", "0") or "0")
    except ValueError:
        q_timeout = 0

    # Cap DISTINCT/CHECKSUM/FK/null SQL so a single huge table cannot block completion (0 = no cap).
    try:
        dq_raw = os.environ.get("DV_DATA_QUERY_TIMEOUT_SEC", "").strip()
        if dq_raw == "":
            dq_timeout = 600
        else:
            dq_iv = int(dq_raw)
            dq_timeout = None if dq_iv <= 0 else dq_iv
    except ValueError:
        dq_timeout = 600

    def _csv(name: str) -> list[str]:
        raw = os.environ.get(name, "") or ""
        return [s.strip() for s in raw.split(",") if s.strip()]

    return ValidationOptions(
        row_count_mode=mode,
        large_table_threshold_bytes=int(threshold_gb * 1024**3),
        count_with_dirty_read=dirty,
        row_count_timeout_seconds=(q_timeout or None),
        data_query_timeout_seconds=dq_timeout,
        exclude_tables=_csv("DV_EXCLUDE_TABLES"),
        estimate_tables=_csv("DV_ESTIMATE_TABLES"),
        estimate_tolerance_pct=tol,
    )


def _connect_timeout() -> int | None:
    try:
        v = int(os.environ.get("DV_CONNECT_TIMEOUT_SEC", "0") or "0")
    except ValueError:
        return None
    return v or None


def _source_dict() -> dict:
    port_s = _env_optional("DV_SOURCE_PORT")
    d: dict = {
        "type": _env_optional("DV_SOURCE_TYPE", "db2") or "db2",
        "host": _env_required("DV_SOURCE_HOST"),
        "database": _env_required("DV_SOURCE_DATABASE"),
        "username": _env_required("DV_SOURCE_USERNAME"),
        "password": _env_required("DV_SOURCE_PASSWORD"),
    }
    if port_s:
        try:
            d["port"] = int(port_s)
        except ValueError:
            raise SystemExit(f"DV_SOURCE_PORT must be an integer, got {port_s!r}") from None
    ct = _connect_timeout()
    if ct is not None:
        d["connect_timeout_seconds"] = ct
    return d


def _target_dict() -> dict:
    port_s = _env_optional("DV_TARGET_PORT")
    d: dict = {
        "type": _env_optional("DV_TARGET_TYPE", "azure_sql") or "azure_sql",
        "host": _env_required("DV_TARGET_HOST"),
        "database": _env_required("DV_TARGET_DATABASE"),
        "username": _env_optional("DV_TARGET_USERNAME"),
        "password": _env_optional("DV_TARGET_PASSWORD"),
    }
    if port_s:
        try:
            d["port"] = int(port_s)
        except ValueError:
            raise SystemExit(f"DV_TARGET_PORT must be an integer, got {port_s!r}") from None
    auth = _env_optional("DV_TARGET_AUTH")
    if auth:
        d["auth"] = auth
    if _env_optional("DV_TARGET_TRUST_SERVER_CERT", "").lower() in ("1", "true", "yes"):
        d["trust_server_certificate"] = True
    ct = _connect_timeout()
    if ct is not None:
        d["connect_timeout_seconds"] = ct
    return d


def _out_base() -> Path:
    return _ROOT / "outputs" / OUTPUT_ORG / OUTPUT_INSTANCE


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def run() -> None:
    _load_dotenv_files()

    source_schema = _env_optional("DV_SOURCE_SCHEMA", "USERID") or "USERID"
    target_schema = _env_optional("DV_TARGET_SCHEMA", "dbo") or "dbo"
    schemas = (source_schema, target_schema)

    client = ValidationClient(
        source=_source_dict(),
        target=_target_dict(),
        options=_build_options(),
    )
    db_label = client._target_config.database

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = secrets.token_hex(3)
    run_label = f"{ts}_{run_id}"

    print("Running schema validations (source vs target)...")
    try:
        schema_report = client.validate_schema(schemas=schemas)
        print(schema_report.summary)
        schema_path = _out_base() / "schema-validation" / f"schema_validate_all_{db_label}_{run_label}.csv"
        _ensure_dir(schema_path)
        schema_report.to_legacy_csv(schema_path)
        print(f"Schema report saved to: {schema_path}")
    except ImportError as e:
        if "ibm_db" in str(e) or "db2" in str(e).lower():
            print(
                "DB2 driver not available. Ensure Java (JRE) is installed for JDBC fallback; "
                "the library will auto-download the DB2 driver."
            )
        raise
    except Exception as e:
        print(f"Schema validation failed: {e}")
        raise

    print("\nRunning data validations (row counts, etc.)...")
    try:
        data_report = client.validate_data(schemas=schemas)
        print(data_report.summary)
        data_path = _out_base() / "data-validation" / f"data_validate_all_{db_label}_{run_label}.csv"
        _ensure_dir(data_path)
        data_report.to_legacy_csv(data_path)
        print(f"Data report saved to: {data_path}")
    except Exception as e:
        print(f"Data validation failed: {e}")
        raise

    client.close()
    print("\nDone.")


if __name__ == "__main__":
    run()
