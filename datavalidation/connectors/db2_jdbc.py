"""
DB2 JDBC driver resolution and connection (fallback when ibm_db is not available).
Uses jaydebeapi + jpype1; jar can be in datavalidation/drivers or auto-downloaded.
"""
from pathlib import Path
from typing import Any

# Package drivers dir (next to connectors)
_PKG_DIR = Path(__file__).resolve().parent.parent
DRIVERS_DIR = _PKG_DIR / "drivers"

JAR_NAMES = ["db2jcc4.jar", "db2jcc.jar", "jcc.jar"]
MAVEN_URL = "https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.9.0/jcc-11.5.9.0.jar"


def _search_paths() -> list[Path]:
    """Ordered search paths for DB2 JDBC jar."""
    import os
    import sys
    paths = [
        DRIVERS_DIR,
        Path.cwd() / "drivers",
        Path.home() / ".datavalidation" / "drivers",
    ]
    env = os.environ.get("DB2_JDBC_DRIVER_PATH") or os.environ.get("DV_DB2_DRIVERS")
    if env:
        paths.insert(0, Path(env))
    if sys.platform == "win32":
        paths.extend([Path(r"C:\Program Files\IBM\SQLLIB\java"), Path(r"C:\IBM\SQLLIB\java")])
    else:
        paths.append(Path("/opt/ibm/db2/java"))
    paths.append(Path.home() / ".db2" / "java")
    return paths


def find_db2_jar() -> str | None:
    """Find DB2 JDBC jar in drivers folder or known locations. Returns path or None."""
    for search_path in _search_paths():
        if not search_path.exists():
            continue
        for name in JAR_NAMES:
            candidate = search_path / name
            if candidate.is_file():
                return str(candidate)
    return None


def ensure_db2_jdbc_driver() -> str | None:
    """
    Find DB2 JDBC jar; if not found, try to download to package drivers dir or ~/.datavalidation/drivers.
    Returns jar path or None.
    """
    found = find_db2_jar()
    if found:
        return found

    # Prefer package drivers dir if writable, else user dir
    for dir_candidate in [DRIVERS_DIR, Path.home() / ".datavalidation" / "drivers"]:
        try:
            dir_candidate.mkdir(parents=True, exist_ok=True)
            jar_path = dir_candidate / "db2jcc4.jar"
        except OSError:
            continue
        try:
            import ssl
            import urllib.request
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            with urllib.request.urlopen(MAVEN_URL, context=ctx, timeout=120) as resp:
                jar_path.write_bytes(resp.read())
            if jar_path.is_file():
                return str(jar_path)
        except Exception:
            pass
    return None


def connect_db2_jdbc(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    jar_path: str | None = None,
    connect_timeout_seconds: int | None = None,
) -> Any:
    """
    Connect to DB2 via JDBC (jaydebeapi). Requires jaydebeapi and jpype1.
    Returns a DB-API 2 connection (cursor(), commit(), close()).

    ``connect_timeout_seconds`` maps to the IBM JDBC ``loginTimeout`` URL property so a hung
    DB2 server doesn't block the whole process at connect time.
    """
    import jaydebeapi
    import jpype

    path = jar_path or ensure_db2_jdbc_driver()
    if not path:
        raise RuntimeError(
            "DB2 JDBC driver (db2jcc4.jar) not found. "
            "Place it in the 'drivers' folder next to the datavalidation package, "
            "or set DB2_JDBC_DRIVER_PATH to the folder containing the jar."
        )

    url_props = []
    if connect_timeout_seconds:
        url_props.append(f"loginTimeout={int(connect_timeout_seconds)}")
    suffix = (":" + ";".join(url_props) + ";") if url_props else ""
    jdbc_url = f"jdbc:db2://{host}:{port}/{database}{suffix}"
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[path])
    conn = jaydebeapi.connect("com.ibm.db2.jcc.DB2Driver", jdbc_url, [user, password], path)
    return conn
