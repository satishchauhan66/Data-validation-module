"""
DB2 JDBC driver resolution and connection (fallback when ibm_db is not available).
Uses jaydebeapi + jpype1. The DB2 JCC jar is bundled under datavalidation/drivers/ and
installed with the package; auto-download is only a last resort.
"""
from __future__ import annotations

import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

# Package drivers dir (editable install / site-packages layout)
_PKG_DIR = Path(__file__).resolve().parent.parent
DRIVERS_DIR = _PKG_DIR / "drivers"

JAR_NAMES = ["db2jcc4.jar", "db2jcc.jar", "jcc.jar", "jcc-11.5.9.0.jar"]
BUNDLED_JAR_NAME = "db2jcc4.jar"
MAVEN_URL = "https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.9.0/jcc-11.5.9.0.jar"


def _frozen_bundle_root() -> Path | None:
    """PyInstaller one-file/one-folder extract dir."""
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass)
    return None


@lru_cache(maxsize=1)
def _bundled_jar_filesystem_path() -> str | None:
    """
    Resolve the jar shipped inside the datavalidation package (pip install / wheel).
    Uses importlib.resources so it works from site-packages and zips; copies to a temp
    file when the wheel stores the jar inside the zip.
    """
    # 1) Adjacent to this module (editable install, unpacked site-packages)
    adjacent = DRIVERS_DIR / BUNDLED_JAR_NAME
    if adjacent.is_file():
        return str(adjacent.resolve())

    # 2) PyInstaller: datavalidation/drivers next to _MEIPASS root
    frozen = _frozen_bundle_root()
    if frozen is not None:
        for base in (frozen / "datavalidation" / "drivers", frozen / "drivers"):
            candidate = base / BUNDLED_JAR_NAME
            if candidate.is_file():
                return str(candidate.resolve())

    # 3) importlib.resources (wheel / zip-safe)
    try:
        from importlib.resources import as_file, files

        ref = files("datavalidation").joinpath("drivers", BUNDLED_JAR_NAME)
        if not ref.is_file():
            return None
        with as_file(ref) as extracted:
            return str(Path(extracted).resolve())
    except Exception:
        return None


def _search_paths() -> list[Path]:
    """Ordered search paths for DB2 JDBC jar (bundled paths first)."""
    paths: list[Path] = [DRIVERS_DIR]
    frozen = _frozen_bundle_root()
    if frozen is not None:
        paths.extend([frozen / "datavalidation" / "drivers", frozen / "drivers"])
    paths.extend(
        [
            Path.cwd() / "drivers",
            Path.home() / ".datavalidation" / "drivers",
        ]
    )
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
    """Find DB2 JDBC jar. Prefers the bundled install artifact."""
    bundled = _bundled_jar_filesystem_path()
    if bundled:
        return bundled
    for search_path in _search_paths():
        if not search_path.exists():
            continue
        for name in JAR_NAMES:
            candidate = search_path / name
            if candidate.is_file():
                return str(candidate.resolve())
    return None


def ensure_db2_jdbc_driver() -> str | None:
    """
    Return path to DB2 JDBC jar (bundled with package when installed via pip).
    Downloads only if the bundled jar is missing (e.g. incomplete checkout).
    """
    found = find_db2_jar()
    if found:
        return found

    for dir_candidate in [DRIVERS_DIR, Path.home() / ".datavalidation" / "drivers"]:
        try:
            dir_candidate.mkdir(parents=True, exist_ok=True)
            jar_path = dir_candidate / BUNDLED_JAR_NAME
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
                return str(jar_path.resolve())
        except Exception:
            pass
    return None


def _resolve_jvm_path() -> str | None:
    """Locate a JVM for JPype without requiring user configuration."""
    import jpype

    try:
        return jpype.getDefaultJVMPath()
    except Exception:
        pass

    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        home = Path(java_home)
        if sys.platform == "win32":
            candidates = [
                home / "bin" / "server" / "jvm.dll",
                home / "jre" / "bin" / "server" / "jvm.dll",
            ]
        elif sys.platform == "darwin":
            candidates = [
                home / "lib" / "server" / "libjvm.dylib",
                home / "jre" / "lib" / "server" / "libjvm.dylib",
            ]
        else:
            candidates = [
                home / "lib" / "server" / "libjvm.so",
                home / "jre" / "lib" / "amd64" / "server" / "libjvm.so",
            ]
        for c in candidates:
            if c.is_file():
                return str(c)

    if sys.platform == "win32":
        for base in (
            Path(r"C:\Program Files\Java"),
            Path(r"C:\Program Files\Eclipse Adoptium"),
            Path(r"C:\Program Files\Microsoft"),
            Path(r"C:\Program Files\Amazon Corretto"),
        ):
            if not base.is_dir():
                continue
            for child in sorted(base.iterdir(), reverse=True):
                jvm = child / "bin" / "server" / "jvm.dll"
                if jvm.is_file():
                    return str(jvm)
    return None


def _start_jvm(jar_path: str) -> None:
    import jpype

    if jpype.isJVMStarted():
        return
    jvm = _resolve_jvm_path()
    if jvm:
        jpype.startJVM(jvm, classpath=[jar_path], convertStrings=True)
    else:
        jpype.startJVM(classpath=[jar_path], convertStrings=True)


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
    Connect to DB2 via JDBC (jaydebeapi). Requires jaydebeapi, jpype1, and a JRE on the machine.

    ``connect_timeout_seconds`` maps to the IBM JDBC ``loginTimeout`` URL property.
    """
    import jaydebeapi

    path = jar_path or ensure_db2_jdbc_driver()
    if not path:
        raise RuntimeError(
            "DB2 JDBC driver (db2jcc4.jar) not found. Reinstall the datavalidation package "
            "so the bundled driver is included, or ensure Java is installed for auto-download."
        )

    url_props = []
    if connect_timeout_seconds:
        url_props.append(f"loginTimeout={int(connect_timeout_seconds)}")
    suffix = (":" + ";".join(url_props) + ";") if url_props else ""
    jdbc_url = f"jdbc:db2://{host}:{port}/{database}{suffix}"
    _start_jvm(path)
    return jaydebeapi.connect("com.ibm.db2.jcc.DB2Driver", jdbc_url, [user, password], path)
