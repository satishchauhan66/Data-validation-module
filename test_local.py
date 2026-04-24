"""
Local test script for datavalidation package.
Run from project root: python test_local.py
Uses no real DB connections; validates imports, client, results, and optional from_file.
"""
import sys
import os

# Ensure package is importable from repo root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test that public API imports work."""
    from datavalidation import (
        ValidationClient,
        ValidationResult,
        ValidationReport,
        ConnectionConfig,
        ValidationOptions,
    )
    assert ValidationClient is not None
    assert ValidationResult is not None
    assert ValidationReport is not None
    print("OK imports")

def test_client_from_dict():
    """Test ValidationClient from dict config."""
    from datavalidation import ValidationClient
    client = ValidationClient(
        source={
            "type": "azure_sql",
            "host": "source.database.windows.net",
            "database": "SourceDB",
            "username": "u",
            "password": "p",
        },
        target={
            "type": "azure_sql",
            "host": "target.database.windows.net",
            "database": "TargetDB",
            "username": "u",
            "password": "p",
        },
    )
    assert client._source_config.type == "azure_sql"
    assert client._target_config.database == "TargetDB"
    print("OK client from dict")

def test_validation_result():
    """Test ValidationResult and to_dict/to_dataframe/to_csv."""
    from datavalidation import ValidationResult
    result = ValidationResult(
        validation_name="row_counts",
        passed=True,
        summary="Row counts: 5 tables compared, 0 mismatch(es).",
        details=[
            {"schema": "dbo", "table": "T1", "status": "OK", "source_count": 100, "target_count": 100},
        ],
        stats={"tables_compared": 5, "mismatch_count": 0},
    )
    assert result.passed is True
    d = result.to_dict()
    assert d["validation_name"] == "row_counts"
    assert d["stats"]["tables_compared"] == 5
    try:
        df = result.to_dataframe()
        assert len(df) == 1
        print("OK result to_dict and to_dataframe")
    except ImportError:
        print("OK result to_dict (pandas not required for to_dict)")

def test_validation_report():
    """Test ValidationReport (multiple results)."""
    from datavalidation import ValidationResult, ValidationReport
    r1 = ValidationResult("table_presence", True, "All tables present", [], stats={})
    r2 = ValidationResult("column_counts", False, "2 mismatch(es)", [{"table": "T1", "status": "MISMATCH"}], stats={"mismatch_count": 2})
    report = ValidationReport(results={"table_presence": r1, "column_counts": r2})
    assert report.all_passed is False
    assert "table_presence" in report.results
    assert "2/2" in report.summary or "1 passed" in report.summary or "passed" in report.summary
    print("OK report")

def test_legacy_csv_format():
    """Assert legacy CSV matches old validation report format: ValidationType=presence, empty Destination* for SOURCE_ONLY, DetailsJson {} for presence."""
    import tempfile
    from datavalidation import ValidationResult, ValidationReport
    r1 = ValidationResult(
        "table_presence",
        False,
        "Diffs found",
        details=[
            {"source_schema": "USERID", "target_schema": "dbo", "schema": "USERID", "table": "MYPROC", "object_type": "PROCEDURE", "status": "SOURCE_ONLY", "element_path": "USERID.MYPROC"},
        ],
        stats={},
    )
    r2 = ValidationResult(
        "row_counts",
        False,
        "Mismatches",
        details=[
            {"source_schema": "USERID", "target_schema": "dbo", "schema": "USERID", "table": "SURV_DATA", "status": "MISMATCH", "source_count": 2, "target_count": 4, "element_path": "USERID.SURV_DATA", "object_type": "TABLE"},
        ],
        stats={},
    )
    report = ValidationReport(results={"table_presence": r1, "row_counts": r2})
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        import csv
        report.to_legacy_csv(path)
        with open(path, encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) >= 2
        # First row: presence, SOURCE_ONLY -> empty Destination*
        assert rows[0]["ValidationType"] == "presence"
        assert rows[0]["SourceObjectName"] == "MYPROC"
        assert rows[0]["SourceSchemaName"] == "USERID"
        assert rows[0]["DestinationObjectName"] == ""
        assert rows[0]["DestinationSchemaName"] == ""
        # Presence rows use structured DetailsJson (object_type, source_schema_name, etc.)
        assert "object_type" in rows[0]["DetailsJson"] and "source_schema_name" in rows[0]["DetailsJson"]
        # Second: row_counts
        assert rows[1]["ValidationType"] == "row_counts"
        assert "source_row_count" in rows[1]["DetailsJson"] and "destination_row_count" in rows[1]["DetailsJson"]
    finally:
        os.unlink(path)
    print("OK legacy CSV format")

def test_legacy_csv_foreign_keys_and_indexes():
    """FK rows use FK_MISMATCH, descriptions, Schema.Table.FK element path, and legacy DetailsJson; indexes include source_cols."""
    import tempfile
    import csv
    import json
    from datavalidation import ValidationResult, ValidationReport
    fk = ValidationResult(
        "foreign_keys",
        False,
        "FK diffs",
        details=[
            {
                "source_schema": "USERID",
                "target_schema": "dbo",
                "schema": "USERID",
                "table": "PROV_DEFN",
                "fk_name": "FKFAC_ID",
                "status": "SOURCE_ONLY",
                "object_type": "TABLE",
                "element_path": "USERID.PROV_DEFN.FKFAC_ID",
                "error_code": "FK_MISMATCH",
                "error_description": "FK missing in target",
                "source_ref_schema": "USERID",
                "source_ref_table": "FACILITY",
                "source_delete_action": "NO_ACTION",
                "source_update_action": "NO_ACTION",
                "source_column_pairs": "FAC_ID->ID",
            },
        ],
        stats={},
    )
    ix = ValidationResult(
        "indexes",
        False,
        "Ix",
        details=[
            {
                "source_schema": "USERID",
                "target_schema": "dbo",
                "schema": "USERID",
                "table": "T1",
                "index": "IX1",
                "object_type": "TABLE",
                "status": "MISMATCH",
                "element_path": "USERID.T1.IX1",
                "error_code": "INDEX_COLUMNS_MISMATCH",
                "error_description": "Index columns mismatch",
                "source_columns": "A A",
                "destination_columns": "B A",
                "source_unique": False,
                "destination_unique": False,
            },
        ],
        stats={},
    )
    report = ValidationReport(results={"foreign_keys": fk, "indexes": ix})
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        report.to_legacy_csv(path)
        with open(path, encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 2
        rfk = next(r for r in rows if r["ValidationType"] == "foreign_keys")
        assert rfk["ErrorCode"] == "FK_MISMATCH"
        assert rfk["ErrorDescription"] == "FK missing in target"
        assert rfk["ElementPath"] == "USERID.PROV_DEFN.FKFAC_ID"
        dj = json.loads(rfk["DetailsJson"])
        assert dj["constraint_name"] == "FKFAC_ID"
        assert dj["source_ref_table"] == "FACILITY"
        rix = next(r for r in rows if r["ValidationType"] == "indexes")
        ixj = json.loads(rix["DetailsJson"])
        assert ixj["source_cols"] == "A A"
        assert ixj["destination_cols"] == "B A"
    finally:
        os.unlink(path)
    print("OK legacy CSV foreign keys and indexes")

def test_from_file_if_yaml():
    """Test ValidationClient.from_file if YAML config exists (optional)."""
    from datavalidation import ValidationClient
    import tempfile
    config_path = os.path.join(os.path.dirname(__file__), "test_config.yaml")
    if not os.path.exists(config_path):
        # Create a minimal config for this test run only
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("""
source:
  type: azure_sql
  host: s.database.windows.net
  database: D
  username: u
  password: p
target:
  type: azure_sql
  host: t.database.windows.net
  database: D2
  username: u
  password: p
""")
            config_path = f.name
    try:
        client = ValidationClient.from_file(config_path)
        assert client._source_config.database == "D"
        assert client._target_config.database == "D2"
        print("OK from_file")
    except ImportError as e:
        if "yaml" in str(e).lower() or "pyyaml" in str(e).lower():
            print("SKIP from_file (PyYAML not installed)")
        else:
            raise
    finally:
        if config_path.startswith(tempfile.gettempdir()):
            try:
                os.unlink(config_path)
            except Exception:
                pass

def _make_data_validator(
    src_tables=None,
    tgt_tables=None,
    src_stats=None,
    tgt_stats=None,
    src_counts=None,
    tgt_counts=None,
    src_estimates=None,
    tgt_estimates=None,
    options=None,
    src_count_raises=None,
):
    """Build a DataValidator with stubbed adapters/dialects for the row-count tests."""
    from datavalidation.config import ConnectionConfig, ValidationOptions
    from datavalidation.validators.data import DataValidator

    src_tables = src_tables or []
    tgt_tables = tgt_tables or []
    src_stats = src_stats or {}
    tgt_stats = tgt_stats or {}
    src_counts = src_counts or {}
    tgt_counts = tgt_counts or {}
    src_estimates = src_estimates or {}
    tgt_estimates = tgt_estimates or {}
    src_count_raises = src_count_raises or set()

    class StubAdapter:
        def __init__(self, side):
            self.side = side
            self._closed = False
        def connect(self):
            return None
        def close(self):
            self._closed = True

    class StubSrcDialect:
        def catalog_tables_query(self, schema, object_types):
            return ("CATALOG", "src", schema)
        def row_count_query(self, schema, table, dirty_read=False):
            return ("COUNT", "src", schema, table, dirty_read)
        def row_count_estimate_query(self, schema, table):
            return ("ESTIMATE", "src", schema, table)
        def table_stats_query(self, schema):
            return ("STATS", "src", schema) if schema else None

    class StubTgtDialect:
        def catalog_tables_query(self, schema, object_types):
            return ("CATALOG", "tgt", schema)
        def row_count_query(self, schema, table, dirty_read=False):
            return ("COUNT", "tgt", schema, table, dirty_read)
        def row_count_estimate_query(self, schema, table):
            return ("ESTIMATE", "tgt", schema, table)
        def table_stats_query(self, schema):
            return ("STATS", "tgt", schema) if schema else None

    src_cfg = ConnectionConfig(type="db2", host="h", database="d", username="USERID")
    tgt_cfg = ConnectionConfig(type="azure_sql", host="h", database="d", username="u", password="p")
    v = DataValidator(
        src_cfg, tgt_cfg,
        options=options or ValidationOptions(),
        source_adapter=StubAdapter("src"),
        target_adapter=StubAdapter("tgt"),
    )
    v._source_dialect = StubSrcDialect()
    v._target_dialect = StubTgtDialect()

    def _src_exec(sql, params=None, timeout_seconds=None):
        kind = sql[0]
        if kind == "CATALOG":
            return [{"schema_name": "USERID", "table_name": t} for t in src_tables]
        if kind == "STATS":
            return [{"schema_name": "USERID", "table_name": t, "row_estimate": s.get("rows"), "bytes_estimate": s.get("bytes")} for t, s in src_stats.items()]
        if kind == "COUNT":
            tbl = sql[3]
            if tbl in src_count_raises:
                raise RuntimeError("simulated COUNT(*) timeout / failure")
            return [{"cnt": src_counts.get(tbl)}]
        if kind == "ESTIMATE":
            tbl = sql[3]
            return [{"cnt": src_estimates.get(tbl)}]
        return []

    def _tgt_exec(sql, params=None, timeout_seconds=None):
        kind = sql[0]
        if kind == "CATALOG":
            return [{"schema_name": "dbo", "table_name": t} for t in tgt_tables]
        if kind == "STATS":
            return [{"schema_name": "dbo", "table_name": t, "row_estimate": s.get("rows"), "bytes_estimate": s.get("bytes")} for t, s in tgt_stats.items()]
        if kind == "COUNT":
            tbl = sql[3]
            return [{"cnt": tgt_counts.get(tbl)}]
        if kind == "ESTIMATE":
            tbl = sql[3]
            return [{"cnt": tgt_estimates.get(tbl)}]
        return []

    v._source_execute = _src_exec
    v._target_execute = _tgt_exec
    return v


def test_validation_options_defaults():
    """Defaults should be safe for huge tables (auto mode, 50 GB threshold)."""
    from datavalidation import ValidationOptions
    o = ValidationOptions()
    assert o.row_count_mode == "auto"
    assert o.large_table_threshold_bytes == 50 * 1024 ** 3
    assert o.estimate_tolerance_pct == 1.0
    assert o.exclude_tables == [] and o.estimate_tables == []
    print("OK ValidationOptions defaults")


def test_dialect_estimate_queries():
    """Both dialects must produce non-empty estimate + table_stats SQL with the schema embedded."""
    from datavalidation.dialects import get_dialect
    db2 = get_dialect("db2")
    az = get_dialect("azure_sql")
    s_db2 = db2.row_count_estimate_query("userid", "MY_TBL")
    assert "SYSCAT.TABLES" in s_db2 and "USERID" in s_db2 and "MY_TBL" in s_db2
    t_db2 = db2.table_stats_query("userid")
    assert t_db2 and "row_estimate" in t_db2 and "bytes_estimate" in t_db2
    s_az = az.row_count_estimate_query("dbo", "MyTable")
    assert "dm_db_partition_stats" in s_az and "dbo" in s_az and "MyTable" in s_az
    t_az = az.table_stats_query("dbo")
    assert t_az and "row_estimate" in t_az and "bytes_estimate" in t_az
    print("OK dialect estimate + stats queries")


def test_dialect_dirty_read_clauses():
    """DB2 -> WITH UR; Azure SQL -> WITH (NOLOCK); off by default."""
    from datavalidation.dialects import get_dialect
    db2 = get_dialect("db2")
    az = get_dialect("azure_sql")
    assert "WITH UR" not in db2.row_count_query("u", "t")
    assert db2.row_count_query("u", "t", dirty_read=True).rstrip().endswith("WITH UR")
    assert "NOLOCK" not in az.row_count_query("dbo", "T")
    assert "WITH (NOLOCK)" in az.row_count_query("dbo", "T", dirty_read=True)
    print("OK dirty-read clauses")


def test_row_counts_exact_match():
    """All counts equal -> passed=True, method=exact for every call."""
    from datavalidation import ValidationOptions
    v = _make_data_validator(
        src_tables=["T1", "T2"],
        tgt_tables=["T1", "T2"],
        src_counts={"T1": 100, "T2": 50},
        tgt_counts={"T1": 100, "T2": 50},
        options=ValidationOptions(row_count_mode="exact"),
    )
    res = v.validate_row_counts("USERID", "dbo")
    assert res.passed is True, res.summary
    assert res.stats["mismatch_count"] == 0
    assert res.stats["methods"].get("exact") == 4  # 2 tables x 2 sides
    print("OK row_counts exact match")


def test_row_counts_auto_uses_estimate_for_huge_table():
    """In 'auto' mode, a 900 GB table should be counted via estimates instead of COUNT(*)."""
    big = 900 * 1024 ** 3
    v = _make_data_validator(
        src_tables=["BIG", "SMALL"],
        tgt_tables=["BIG", "SMALL"],
        src_stats={"BIG": {"rows": 100_000_000, "bytes": big}, "SMALL": {"rows": 10, "bytes": 4096}},
        tgt_stats={"BIG": {"rows": 100_000_000, "bytes": big}, "SMALL": {"rows": 10, "bytes": 4096}},
        src_counts={"SMALL": 10},  # BIG must NOT be counted exactly
        tgt_counts={"SMALL": 10},
        src_estimates={"BIG": 100_000_000},
        tgt_estimates={"BIG": 100_000_000},
    )
    res = v.validate_row_counts("USERID", "dbo")
    assert res.passed is True, res.summary
    methods = res.stats["methods"]
    assert methods.get("estimate", 0) == 2, f"expected BIG counted via estimate on both sides, got {methods}"
    assert methods.get("exact", 0) == 2, f"expected SMALL via exact on both sides, got {methods}"
    print("OK row_counts auto routes huge tables to estimates")


def test_row_counts_auto_falls_back_when_exact_fails():
    """Auto mode should fall back to estimate when COUNT(*) raises (e.g. driver timeout)."""
    v = _make_data_validator(
        src_tables=["T"],
        tgt_tables=["T"],
        src_stats={"T": {"rows": 9, "bytes": 1024}},  # below threshold -> picks exact first
        tgt_stats={"T": {"rows": 9, "bytes": 1024}},
        src_counts={"T": 9},  # source COUNT(*) will raise (see below)
        tgt_counts={"T": 9},
        src_estimates={"T": 9},
        tgt_estimates={"T": 9},
        src_count_raises={"T"},
    )
    res = v.validate_row_counts("USERID", "dbo")
    assert res.passed is True, res.summary
    methods = res.stats["methods"]
    assert methods.get("estimate_fallback", 0) == 1, methods
    assert methods.get("exact", 0) == 1, methods  # target side still went exact
    print("OK row_counts auto falls back to estimate on exact failure")


def test_row_counts_estimate_tolerance():
    """Estimate-vs-estimate within tolerance pct must be considered a match (not a MISMATCH)."""
    from datavalidation import ValidationOptions
    v = _make_data_validator(
        src_tables=["T"],
        tgt_tables=["T"],
        src_estimates={"T": 10_000_000},
        tgt_estimates={"T": 10_050_000},  # 0.5% drift
        options=ValidationOptions(row_count_mode="estimate", estimate_tolerance_pct=1.0),
    )
    res = v.validate_row_counts("USERID", "dbo")
    assert res.passed is True, res.summary
    assert res.stats["mismatch_count"] == 0
    print("OK row_counts estimate tolerance")


def test_row_counts_exclude_and_skip_modes():
    """exclude_tables marks tables SKIPPED; mode='skip' skips everything."""
    from datavalidation import ValidationOptions
    v = _make_data_validator(
        src_tables=["KEEP", "DROP_ME"],
        tgt_tables=["KEEP", "DROP_ME"],
        src_counts={"KEEP": 1, "DROP_ME": 1},
        tgt_counts={"KEEP": 1, "DROP_ME": 1},
        options=ValidationOptions(row_count_mode="exact", exclude_tables=["drop_me"]),  # case-insensitive
    )
    res = v.validate_row_counts("USERID", "dbo")
    skipped = [d for d in res.details if d["status"] == "SKIPPED"]
    assert len(skipped) == 1 and skipped[0]["table"] == "DROP_ME", res.details
    assert res.stats["skipped_count"] == 1

    v2 = _make_data_validator(
        src_tables=["A", "B"],
        tgt_tables=["A", "B"],
        src_counts={"A": 1, "B": 2},
        tgt_counts={"A": 1, "B": 2},
        options=ValidationOptions(row_count_mode="skip"),
    )
    res2 = v2.validate_row_counts("USERID", "dbo")
    assert res2.stats["skipped_count"] == 2, res2.stats
    assert all(d.get("count_method") == "skip" for d in res2.details), res2.details
    print("OK row_counts exclude_tables + mode='skip'")


def test_row_counts_propagates_dirty_read_and_timeout():
    """Counter must request WITH UR and the configured timeout when running exact counts."""
    from datavalidation import ValidationOptions
    captured: list[dict] = []

    v = _make_data_validator(
        src_tables=["T"],
        tgt_tables=["T"],
        src_counts={"T": 7},
        tgt_counts={"T": 7},
        options=ValidationOptions(
            row_count_mode="exact",
            count_with_dirty_read=True,
            row_count_timeout_seconds=120,
        ),
    )

    def _exec(sql, params=None, timeout_seconds=None):
        captured.append({"sql": sql, "timeout_seconds": timeout_seconds})
        kind = sql[0] if isinstance(sql, tuple) else None
        if kind == "CATALOG":
            return [{"schema_name": "USERID", "table_name": "T"}]
        if kind == "STATS":
            return []
        return [{"cnt": 7}]

    v._source_execute = _exec
    v._target_execute = _exec

    # Re-stub dialects so row_count_query observes dirty_read kwarg
    class DRDialect:
        def catalog_tables_query(self, schema, object_types): return ("CATALOG", schema)
        def table_stats_query(self, schema): return ("STATS", schema) if schema else None
        def row_count_query(self, schema, table, dirty_read=False):
            return ("COUNT", schema, table, "WITH UR" if dirty_read else "")
        def row_count_estimate_query(self, schema, table): return ("ESTIMATE", schema, table)

    v._source_dialect = DRDialect()
    v._target_dialect = DRDialect()

    v.validate_row_counts("USERID", "dbo")

    count_calls = [c for c in captured if isinstance(c["sql"], tuple) and c["sql"][0] == "COUNT"]
    assert count_calls, "Expected at least one COUNT call"
    assert all(c["sql"][3] == "WITH UR" for c in count_calls), count_calls
    assert all(c["timeout_seconds"] == 120 for c in count_calls), count_calls
    print("OK row_counts propagates dirty_read + timeout")


def test_row_counts_estimate_tables_force_path():
    """estimate_tables should force the estimate path even for small tables in exact mode."""
    from datavalidation import ValidationOptions
    v = _make_data_validator(
        src_tables=["FORCED"],
        tgt_tables=["FORCED"],
        src_stats={"FORCED": {"rows": 5, "bytes": 1024}},
        tgt_stats={"FORCED": {"rows": 5, "bytes": 1024}},
        src_estimates={"FORCED": 5},
        tgt_estimates={"FORCED": 5},
        options=ValidationOptions(row_count_mode="exact", estimate_tables=["FORCED"]),
    )
    res = v.validate_row_counts("USERID", "dbo")
    assert res.stats["methods"].get("estimate") == 2, res.stats["methods"]
    print("OK row_counts estimate_tables forces estimate path")


def main():
    print("Testing datavalidation package...")
    test_imports()
    test_client_from_dict()
    test_validation_result()
    test_validation_report()
    test_legacy_csv_format()
    test_legacy_csv_foreign_keys_and_indexes()
    test_from_file_if_yaml()
    test_validation_options_defaults()
    test_dialect_estimate_queries()
    test_dialect_dirty_read_clauses()
    test_row_counts_exact_match()
    test_row_counts_auto_uses_estimate_for_huge_table()
    test_row_counts_auto_falls_back_when_exact_fails()
    test_row_counts_estimate_tolerance()
    test_row_counts_exclude_and_skip_modes()
    test_row_counts_propagates_dirty_read_and_timeout()
    test_row_counts_estimate_tables_force_path()
    print("All tests passed.")

if __name__ == "__main__":
    main()
