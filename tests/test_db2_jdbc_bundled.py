"""Bundled DB2 JDBC jar is discoverable after install."""

from pathlib import Path

from datavalidation.connectors import db2_jdbc


def test_bundled_jar_on_disk():
    adjacent = Path(db2_jdbc.DRIVERS_DIR) / db2_jdbc.BUNDLED_JAR_NAME
    assert adjacent.is_file(), "datavalidation/drivers/db2jcc4.jar must be present in the repo"


def test_find_db2_jar_returns_bundled():
    path = db2_jdbc.find_db2_jar()
    assert path is not None
    assert Path(path).is_file()
    assert Path(path).name in db2_jdbc.JAR_NAMES


def test_ensure_db2_jdbc_driver_returns_bundled():
    path = db2_jdbc.ensure_db2_jdbc_driver()
    assert path is not None
    assert "db2jcc" in Path(path).name.lower() or Path(path).name.endswith(".jar")
