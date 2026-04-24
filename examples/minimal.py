"""
Minimal example: load credentials from environment (see repo root ``.env.example``).

  pip install python-dotenv
  copy ..\\.env.example ..\\.env   # then edit .env

Or set ``DV_SOURCE_*`` / ``DV_TARGET_*`` in the shell before running.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

from datavalidation import ValidationClient


def _req(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(f"Set {name} in .env or environment (see {_ROOT / '.env.example'})")
    return v


def _opt(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()


def main() -> None:
    port_s = _opt("DV_SOURCE_PORT", "50000") or "50000"
    tgt: dict = {
        "type": _opt("DV_TARGET_TYPE", "azure_sql") or "azure_sql",
        "host": _req("DV_TARGET_HOST"),
        "database": _req("DV_TARGET_DATABASE"),
        "username": _opt("DV_TARGET_USERNAME"),
        "password": _opt("DV_TARGET_PASSWORD"),
    }
    if _opt("DV_TARGET_AUTH"):
        tgt["auth"] = _opt("DV_TARGET_AUTH")

    client = ValidationClient(
        source={
            "type": _opt("DV_SOURCE_TYPE", "db2") or "db2",
            "host": _req("DV_SOURCE_HOST"),
            "port": int(port_s),
            "database": _req("DV_SOURCE_DATABASE"),
            "username": _req("DV_SOURCE_USERNAME"),
            "password": _req("DV_SOURCE_PASSWORD"),
        },
        target=tgt,
    )
    src_s = _opt("DV_SOURCE_SCHEMA", "USERID") or "USERID"
    tgt_s = _opt("DV_TARGET_SCHEMA", "dbo") or "dbo"
    print(client.validate_data(schemas=(src_s, tgt_s)))
    client.close()


if __name__ == "__main__":
    main()
