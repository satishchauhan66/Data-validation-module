"""
Azure SQL / SQL Server connection adapter using pyodbc + SQLAlchemy.
Azure AD token is passed via pyodbc attrs_before (SQL_COPT_SS_ACCESS_TOKEN), not in the connection string.
"""
import struct
from urllib.parse import quote_plus
from typing import Any

import pyodbc
from sqlalchemy import create_engine, text

from datavalidation.config import ConnectionConfig
from datavalidation.connectors.base import ConnectionAdapter

# Pass Azure AD token to ODBC Driver 17/18 (required; connection string AccessToken is not supported)
SQL_COPT_SS_ACCESS_TOKEN = 1256


def _token_struct(token: str) -> bytes:
    """Format token for pyodbc attrs_before: each UTF-8 byte followed by null, then 4-byte length prefix."""
    exptoken = b""
    for b in token.encode("utf-8"):
        exptoken += bytes((b, 0))
    return struct.pack("=i", len(exptoken)) + exptoken


def _build_connection_string(config: ConnectionConfig, use_token: bool = False) -> str:
    """Build pyodbc connection string. When use_token is True, do not add UID/PWD (token is passed via attrs_before)."""
    driver = config.driver or "ODBC Driver 18 for SQL Server"
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={config.host}",
        f"DATABASE={config.database}",
        "Encrypt=yes" if config.encrypt else "Encrypt=no",
    ]
    if config.trust_server_certificate:
        parts.append("TrustServerCertificate=yes")
    if not use_token:
        if config.auth == "password" and config.username and config.password:
            parts.append(f"UID={config.username}")
            parts.append(f"PWD={config.password}")
        else:
            parts.append("Authentication=ActiveDirectoryPassword")
            if config.username:
                parts.append(f"UID={config.username}")
            if config.password:
                parts.append(f"PWD={config.password}")
    return ";".join(parts)


def _get_azure_token(config: ConnectionConfig) -> str | None:
    """Obtain Azure AD token. Uses MSAL public client interactive (browser MFA) when auth is 'interactive' or password not provided."""
    try:
        from azure.identity import (
            DefaultAzureCredential,
            ManagedIdentityCredential,
            InteractiveBrowserCredential,
            DeviceCodeCredential,
        )
    except ImportError:
        raise ImportError(
            "Azure AD auth requires azure-identity. Reinstall: pip install datavalidation"
        ) from None

    scope = "https://database.windows.net/.default"

    if config.auth == "managed_identity":
        cred = (
            ManagedIdentityCredential(client_id=config.client_id)
            if config.client_id
            else DefaultAzureCredential()
        )
        return cred.get_token(scope).token
    if config.auth in ("interactive", "aad_interactive"):
        # MSAL public client: interactive browser for Azure AD / Entra ID MFA
        try:
            cred = InteractiveBrowserCredential()
        except Exception:
            cred = DeviceCodeCredential()
        return cred.get_token(scope).token
    if config.auth in ("aad", "aad_mfa"):
        return DefaultAzureCredential().get_token(scope).token
    return None


class AzureSQLAdapter(ConnectionAdapter):
    """Connection adapter for Azure SQL / SQL Server using pyodbc."""

    def __init__(self, config: ConnectionConfig, access_token: str | None = None):
        if config.type != "azure_sql":
            raise ValueError("AzureSQLAdapter requires type='azure_sql'")
        super().__init__(config)
        self._access_token = access_token

    def connect(self) -> None:
        if self._engine is not None:
            return
        token = self._access_token or _get_azure_token(self.config)
        use_token = bool(token)
        conn_str = _build_connection_string(self.config, use_token=use_token)
        quoted = quote_plus(conn_str)

        connect_timeout = int(self.config.connect_timeout_seconds or 30)
        if use_token:
            token_struct = _token_struct(token)
            # pyodbc requires token via attrs_before; use a creator so each connection gets the token
            def creator():
                return pyodbc.connect(
                    conn_str,
                    attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
                    timeout=connect_timeout,
                )
            self._engine = create_engine(
                "mssql+pyodbc://",
                creator=creator,
                pool_pre_ping=True,
                pool_size=2,
                max_overflow=2,
            )
        else:
            self._engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={quoted}",
                connect_args={"timeout": connect_timeout},
                pool_pre_ping=True,
                pool_size=2,
                max_overflow=2,
            )

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> list[dict[str, Any]]:
        self.connect()
        with self._engine.connect() as conn:
            if timeout_seconds:
                # pyodbc connection timeout (seconds) — cancels the query when exceeded.
                raw = getattr(conn, "connection", None)
                inner = getattr(raw, "driver_connection", None) or raw
                try:
                    inner.timeout = int(timeout_seconds)
                except Exception:
                    pass
            try:
                result = conn.execute(text(sql), params or {})
                keys = result.keys()
                return [dict(zip(keys, row)) for row in result.fetchall()]
            finally:
                if timeout_seconds:
                    # Reset so the same pooled connection doesn't carry the timeout to the next caller.
                    raw = getattr(conn, "connection", None)
                    inner = getattr(raw, "driver_connection", None) or raw
                    try:
                        inner.timeout = 0
                    except Exception:
                        pass

    def test_connection(self) -> bool:
        try:
            rows = self.execute("SELECT 1 AS test")
            return len(rows) == 1 and rows[0].get("test") == 1
        except Exception:
            return False
