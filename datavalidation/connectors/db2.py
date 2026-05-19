"""
DB2 connection adapter using ibm_db_sa (SQLAlchemy) or JDBC (jaydebeapi) fallback.
When ibm_db is not available (e.g. no native DB2 client on Windows), uses packed
JDBC driver from datavalidation/drivers (or auto-downloaded).
"""
import os
from typing import Any

from datavalidation.config import ConnectionConfig
from datavalidation.connectors.base import ConnectionAdapter


def _is_ibm_db_native_error(exc: BaseException) -> bool:
    """True when the native ibm_db driver cannot load (missing DLL, import_dbapi, etc.)."""
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        msg = str(cur).lower()
        if isinstance(cur, (ImportError, OSError)):
            if any(k in msg for k in ("ibm_db", "dll", "load failed", "import_dbapi", "module could not be found")):
                return True
        if "can't load plugin" in msg and "ibm_db" in msg:
            return True
        cur = cur.__cause__ or cur.__context__  # type: ignore[assignment]
    return False


def _prefer_jdbc_only() -> bool:
    """Skip native ibm_db when JDBC is the reliable path (Windows, frozen exe, or explicit env)."""
    import sys

    env = os.environ.get("DV_DB2_USE_JDBC", "").strip().lower()
    if env in ("1", "true", "yes"):
        return True
    if env in ("0", "false", "no"):
        return False
    # Default: no IBM CLI install on typical Windows / packaged apps — use bundled JDBC jar.
    if getattr(sys, "frozen", False):
        return True
    if sys.platform == "win32":
        return True
    return False


def _build_db2_url(config: ConnectionConfig) -> str:
    """Build SQLAlchemy URL for DB2."""
    port = config.port or 50000
    user = config.username or ""
    password = config.password or ""
    auth = f"{user}:{password}@" if user else ""
    return f"db2+ibm_db://{auth}{config.host}:{port}/{config.database}"


class DB2Adapter(ConnectionAdapter):
    """Connection adapter for IBM DB2: ibm_db_sa first, JDBC (packed driver) fallback."""

    def __init__(self, config: ConnectionConfig):
        if config.type != "db2":
            raise ValueError("DB2Adapter requires type='db2'")
        super().__init__(config)
        self._use_jdbc = False

    def _connect_jdbc(self) -> None:
        """JDBC path: packed driver in drivers/ or auto-downloaded; requires Java (JRE)."""
        try:
            from datavalidation.connectors.db2_jdbc import connect_db2_jdbc, ensure_db2_jdbc_driver
        except ImportError:
            raise ImportError(
                "DB2 native driver failed and JDBC fallback requires jaydebeapi and jpype1. "
                "Reinstall: pip install datavalidation"
            ) from None
        jar_path = ensure_db2_jdbc_driver()
        if not jar_path:
            raise ImportError(
                "DB2 JDBC driver (db2jcc4.jar) not found. Reinstall datavalidation to restore the "
                "bundled driver, or allow auto-download when Java and network are available."
            )
        port = self.config.port or 50000
        try:
            self._connection = connect_db2_jdbc(
                host=self.config.host,
                port=port,
                database=self.config.database,
                user=self.config.username or "",
                password=self.config.password or "",
                jar_path=jar_path,
                connect_timeout_seconds=self.config.connect_timeout_seconds,
            )
        except Exception as e:
            msg = str(e).lower()
            if "refused" in msg or "4499" in str(e) or "08001" in str(e) or "connect" in msg and "timed out" in msg:
                raise RuntimeError(
                    "DB2 connection failed (network): TCP refused, unreachable, or login timeout. "
                    f"Tried host={self.config.host!r} port={port} database={self.config.database!r}. "
                    "If the hostname resolves to a private address (10.x, 172.x, 192.168.x), connect "
                    "from the corporate network or VPN. Confirm DB2 is running, "
                    "DV_SOURCE_HOST and DV_SOURCE_PORT in .env match the server, and firewalls allow this client."
                ) from e
            raise
        self._use_jdbc = True

    def connect(self) -> None:
        if self._engine is not None or self._connection is not None:
            return
        native_ok = False
        if not _prefer_jdbc_only():
            # 1) Try native ibm_db (SQLAlchemy). create_engine is lazy; probe connect() so DLL errors fall back.
            engine = None
            try:
                from sqlalchemy import create_engine
                url = _build_db2_url(self.config)
                engine = create_engine(url, pool_pre_ping=True, pool_size=2, max_overflow=2)
                with engine.connect():
                    pass
                self._engine = engine
                native_ok = True
            except Exception as e:
                if not _is_ibm_db_native_error(e):
                    raise
                if engine is not None:
                    try:
                        engine.dispose()
                    except Exception:
                        pass
        if not native_ok:
            self._connect_jdbc()

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> list[dict[str, Any]]:
        self.connect()
        if self._engine is not None:
            from sqlalchemy import text
            with self._engine.connect() as conn:
                stmt = text(sql)
                if timeout_seconds:
                    # Best-effort: SQLAlchemy/ibm_db doesn't have a portable per-query timeout;
                    # fall through silently if the dialect doesn't honor execution_options.
                    try:
                        stmt = stmt.execution_options(timeout=int(timeout_seconds))
                    except Exception:
                        pass
                result = conn.execute(stmt, params or {})
                keys = result.keys()
                # Normalize keys to lowercase so schema_name/table_name match validator (DB2 often returns uppercase)
                return [{str(k).lower(): v for k, v in dict(zip(keys, row)).items()} for row in result.fetchall()]
        # JDBC path
        conn = self._connection
        cursor = conn.cursor()
        cancel_timer = None
        if timeout_seconds:
            # jaydebeapi creates the underlying java.sql.Statement lazily during execute(); we can't
            # call setQueryTimeout on it ahead of time. Instead spin a Python timer that calls
            # Statement.cancel() once the timeout elapses — the canonical JDBC server-side cancel.
            import threading

            def _cancel() -> None:
                try:
                    stmt = getattr(cursor, "_prep", None)
                    if stmt is not None:
                        stmt.cancel()
                except Exception:
                    pass

            cancel_timer = threading.Timer(int(timeout_seconds), _cancel)
            cancel_timer.daemon = True
            cancel_timer.start()
        try:
            if params:
                # JDBC often uses ? placeholders; pass values in order (dict may not preserve order)
                cursor.execute(sql, list(params.values()))
            else:
                cursor.execute(sql)
            cols = [d[0] for d in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            # Normalize keys to lowercase so schema_name/table_name match validator lookups (JDBC often returns uppercase)
            return [{str(k).lower(): v for k, v in dict(zip(cols, row)).items()} for row in rows]
        finally:
            if cancel_timer is not None:
                cancel_timer.cancel()
            cursor.close()

    def test_connection(self) -> bool:
        try:
            rows = self.execute("SELECT 1 AS test FROM SYSIBM.SYSDUMMY1")
            if len(rows) != 1:
                return False
            row = rows[0]
            val = row.get("test") or row.get("TEST")
            if val is None and row:
                val = next(iter(row.values()), None)
            if val is None:
                return False
            try:
                return int(val) == 1 or float(val) == 1.0
            except (TypeError, ValueError):
                return False
        except Exception:
            return False

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None
        self._engine = None
        self._use_jdbc = False
