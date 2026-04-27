"""
Unit tests for data validators (mocked execute; no real DB).

Run from repo root::

    python -m unittest tests.test_data_validation_unit
"""
import os
import unittest
from unittest.mock import MagicMock

from datavalidation.config import ConnectionConfig, ValidationOptions
from datavalidation.validators.data import DataValidator


def _cfg(db_type: str, schema: str = "S") -> ConnectionConfig:
    return ConnectionConfig(type=db_type, host="h", database="d", username="u", password="p", schema=schema)


class TestDataValidators(unittest.TestCase):
    def setUp(self):
        self.src = _cfg("db2", "X")
        self.tgt = _cfg("azure_sql", "dbo")
        self.opts = ValidationOptions(parallel_workers=2)

    def test_constraint_integrity_respects_disable_env(self):
        os.environ["DV_CI_DISABLE_PUSHDOWN"] = "1"
        try:
            v = DataValidator(self.src, self.tgt, self.opts)
            r = v.validate_constraint_integrity("X", "dbo")
            self.assertTrue(r.passed)
            self.assertIn("disabled", r.summary.lower())
        finally:
            del os.environ["DV_CI_DISABLE_PUSHDOWN"]

    def test_distinct_keys_no_pk(self):
        """When no PK metadata, emit KEY_NOT_FOUND for common tables."""

        def src_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "SYSIBM.SYSTABLES" in u:
                return [{"schema_name": "X", "table_name": "T1", "object_type": "T"}]
            if "SYSCAT.INDEXES" in u or "SYSCAT.INDEXCOLUSE" in u:
                return []
            return []

        def tgt_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "SYS.OBJECTS" in u and "SYS.SCHEMAS" in u:
                return [{"schema_name": "dbo", "table_name": "T1", "object_type": "U"}]
            if "SYS.INDEX_COLUMNS" in u or "SYS.INDEXES" in u:
                return []
            return []

        v = DataValidator(self.src, self.tgt, self.opts)
        src_ad = MagicMock()
        tgt_ad = MagicMock()
        src_ad.execute.side_effect = src_exec
        tgt_ad.execute.side_effect = tgt_exec
        v._source_adapter = src_ad
        v._target_adapter = tgt_ad

        r = v.validate_distinct_keys("X", "dbo")
        self.assertFalse(r.passed)
        self.assertTrue(any(d.get("error_code") == "KEY_NOT_FOUND" for d in r.details))

    def test_column_nulls_metadata_mismatch(self):
        """Nullable Y vs N on same column produces METADATA_MISMATCH row."""

        def src_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "SYSIBM.SYSTABLES" in u:
                return [{"schema_name": "X", "table_name": "T1", "object_type": "T"}]
            if "SYSCAT.COLUMNS" in u or "FROM SYSCAT.COLUMNS" in u:
                return [
                    {
                        "schema_name": "X",
                        "table_name": "T1",
                        "column_name": "C1",
                        "data_type": "VARCHAR",
                        "is_nullable": "Y",
                    }
                ]
            return []

        def tgt_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "SYS.OBJECTS" in u and "SYS.SCHEMAS" in u:
                return [{"schema_name": "dbo", "table_name": "T1", "object_type": "U"}]
            if "SYS.COLUMNS" in u and "SYS.TYPES" in u:
                return [
                    {
                        "schema_name": "dbo",
                        "table_name": "T1",
                        "column_name": "C1",
                        "data_type": "varchar",
                        "is_nullable": False,
                    }
                ]
            return []

        v = DataValidator(self.src, self.tgt, self.opts)
        v._source_adapter = MagicMock()
        v._target_adapter = MagicMock()
        v._source_adapter.execute.side_effect = src_exec
        v._target_adapter.execute.side_effect = tgt_exec

        r = v.validate_column_nulls("X", "dbo")
        self.assertFalse(r.passed)
        self.assertTrue(any(d.get("status") == "METADATA_MISMATCH" for d in r.details))

    def test_referential_integrity_no_groups(self):
        """No FK metadata -> zero issues."""
        def empty_src(sql, params=None, timeout_seconds=None):
            if "REFERENCES" in sql.upper() or "REFKEYCOLUSE" in sql.upper():
                return []
            return []

        v = DataValidator(self.src, self.tgt, self.opts)
        v._source_adapter = MagicMock()
        v._target_adapter = MagicMock()
        v._source_adapter.execute.side_effect = empty_src
        v._target_adapter.execute.side_effect = empty_src

        r = v.validate_referential_integrity("X", "dbo")
        self.assertTrue(r.passed)
        self.assertEqual(len(r.details), 0)

    def test_checksum_row_hash_missing_key(self):
        """row_hash mode reports MISSING_IN_TARGET when source has extra key."""
        opts = ValidationOptions(checksum_mode="row_hash", checksum_row_cap=100)
        v = DataValidator(self.src, self.tgt, opts)

        def src_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "SYSIBM.SYSTABLES" in u:
                return [{"schema_name": "X", "table_name": "T1", "object_type": "T"}]
            if "SYSCAT.COLUMNS" in u:
                return [
                    {"schema_name": "X", "table_name": "T1", "column_name": "ID", "data_type": "INTEGER", "is_nullable": "N"},
                    {"schema_name": "X", "table_name": "T1", "column_name": "N1", "data_type": "VARCHAR", "is_nullable": "Y"},
                ]
            if "SYSCAT.INDEXES" in u or "SYSCAT.INDEXCOLUSE" in u:
                return [
                    {
                        "schema_name": "X",
                        "table_name": "T1",
                        "idx_name": "PK1",
                        "unique_rule": "P",
                        "colseq": 1,
                        "col_name": "ID",
                    }
                ]
            if "FETCH FIRST" in u or "KEYSIG" in u:
                return [{"KeySig": "1", "RowHash": "aaa"}]
            return []

        def tgt_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "SYS.OBJECTS" in u and "SYS.SCHEMAS" in u:
                return [{"schema_name": "dbo", "table_name": "T1", "object_type": "U"}]
            if "SYS.COLUMNS" in u and "JOIN SYS.TYPES" in u:
                return [
                    {"schema_name": "dbo", "table_name": "T1", "column_name": "ID", "data_type": "int", "is_nullable": False},
                    {"schema_name": "dbo", "table_name": "T1", "column_name": "N1", "data_type": "varchar", "is_nullable": True},
                ]
            if "SYS.INDEX_COLUMNS" in u:
                return [
                    {
                        "schema_name": "dbo",
                        "table_name": "T1",
                        "idx_name": "PK1",
                        "is_unique": 0,
                        "is_primary_key": 1,
                        "colseq": 1,
                        "col_name": "ID",
                    }
                ]
            if "TOP (" in u or "keysig" in u.lower():
                return [{"KeySig": "2", "RowHash": "bbb"}]
            return []

        v._source_adapter = MagicMock()
        v._target_adapter = MagicMock()
        v._source_adapter.execute.side_effect = src_exec
        v._target_adapter.execute.side_effect = tgt_exec

        r = v.validate_checksum("X", "dbo")
        self.assertFalse(r.passed)
        self.assertEqual(r.stats.get("checksum_mode"), "row_hash")
        self.assertTrue(any(d.get("error_code") == "MISSING_IN_TARGET" for d in r.details))


if __name__ == "__main__":
    unittest.main()
