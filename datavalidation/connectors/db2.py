"""
DB2 connection adapter using ibm_db_sa (SQLAlchemy) or JDBC (jaydebeapi) fallback.
When ibm_db is not available (e.g. no native DB2 client on Windows), uses packed
JDBC driver from datavalidation/drivers (or auto-downloaded).
"""
from typing import Any

from datavalidation.config import ConnectionConfig
from datavalidation.connectors.base import ConnectionAdapter


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

    def connect(self) -> None:
        if self._engine is not None or self._connection is not None:
            return
        # 1) Try native ibm_db (SQLAlchemy)
        try:
            from sqlalchemy import create_engine
            url = _build_db2_url(self.config)
            self._engine = create_engine(url, pool_pre_ping=True, pool_size=2, max_overflow=2)
            return
        except ImportError as e:
            err = str(e).lower()
            if "ibm_db" not in err and "dll" not in err and "load" not in err:
                raise
        except OSError as e:
            if "DLL" not in str(e) and "module" not in str(e).lower():
                raise
        # 2) Fallback: JDBC (packed driver in drivers/ or auto-download)
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
                "DB2 JDBC driver (db2jcc4.jar) not found. Place it in the package 'drivers' folder "
                "or set DB2_JDBC_DRIVER_PATH. The library can also auto-download it if Java is installed."
            )
        port = self.config.port or 50000
        self._connection = connect_db2_jdbc(
            host=self.config.host,
            port=port,
            database=self.config.database,
            user=self.config.username or "",
            password=self.config.password or "",
            jar_path=jar_path,
            connect_timeout_seconds=self.config.connect_timeout_seconds,
        )
        self._use_jdbc = True

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
