"""
Foreign key pairing across DB2/Azure when system constraint names differ (mocked execute).
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from datavalidation.config import ConnectionConfig, ValidationOptions
from datavalidation.validators.schema import SchemaValidator


def _cfg(db_type: str, schema: str = "S") -> ConnectionConfig:
    return ConnectionConfig(type=db_type, host="h", database="d", username="u", password="p", schema=schema)


class TestSchemaFkPairing(unittest.TestCase):
    def setUp(self):
        self.src = _cfg("db2", "X")
        self.tgt = _cfg("azure_sql", "dbo")
        self.opts = ValidationOptions()

    def test_different_system_fk_names_same_definition_no_false_missing(self):
        """Same logical FK with different constraint names pairs by signature; no SOURCE_ONLY/TARGET_ONLY."""

        src_header = [
            {
                "fk_name": "SQL730",
                "schema_name": "X",
                "table_name": "CHILD",
                "ref_schema": "X",
                "ref_table": "PARENT",
                "delete_action": "A",
                "update_action": "A",
            }
        ]
        src_cols = [
            {
                "fk_name": "SQL730",
                "schema_name": "X",
                "table_name": "CHILD",
                "col_seq": 1,
                "fk_column": "CID",
                "pk_column": "PID",
            }
        ]
        tgt_header = [
            {
                "fk_name": "SQL750",
                "schema_name": "dbo",
                "table_name": "CHILD",
                "ref_schema": "dbo",
                "ref_table": "PARENT",
                "delete_action": 0,
                "update_action": 0,
            }
        ]
        tgt_cols = [
            {
                "fk_name": "SQL750",
                "schema_name": "dbo",
                "table_name": "CHILD",
                "col_seq": 1,
                "fk_column": "CID",
                "pk_column": "PID",
            }
        ]

        def src_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "KEYCOLUSE" in u:
                return list(src_cols)
            if "SYSCAT.REFERENCES" in u:
                return list(src_header)
            return []

        def tgt_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "SYS.FOREIGN_KEY_COLUMNS" in u:
                return list(tgt_cols)
            if "SYS.FOREIGN_KEYS" in u:
                return list(tgt_header)
            return []

        v = SchemaValidator(self.src, self.tgt, self.opts)
        v._source_adapter = MagicMock()
        v._target_adapter = MagicMock()
        v._source_adapter.execute.side_effect = src_exec
        v._target_adapter.execute.side_effect = tgt_exec

        r = v.validate_foreign_keys("X", "dbo")
        self.assertTrue(r.passed)
        statuses = {d.get("status") for d in r.details}
        self.assertNotIn("SOURCE_ONLY", statuses)
        self.assertNotIn("TARGET_ONLY", statuses)

    def test_main_row_constname_only_matches_column_fk_name(self):
        """Header row exposes only constname; column rows use fk_name; pair string still resolves."""

        src_header = [
            {
                "constname": "SQL730",
                "schema_name": "X",
                "table_name": "CHILD",
                "ref_schema": "X",
                "ref_table": "PARENT",
                "delete_action": "A",
                "update_action": "A",
            }
        ]
        src_cols = [
            {
                "fk_name": "SQL730",
                "schema_name": "X",
                "table_name": "CHILD",
                "col_seq": 1,
                "fk_column": "CID",
                "pk_column": "PID",
            }
        ]
        tgt_header = [
            {
                "fk_name": "SQL750",
                "schema_name": "dbo",
                "table_name": "CHILD",
                "ref_schema": "dbo",
                "ref_table": "PARENT",
                "delete_action": 0,
                "update_action": 0,
            }
        ]
        tgt_cols = [
            {
                "fk_name": "SQL750",
                "schema_name": "dbo",
                "table_name": "CHILD",
                "COL_SEQ": 1,
                "fk_column": "CID",
                "pk_column": "PID",
            }
        ]

        def src_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "KEYCOLUSE" in u:
                return list(src_cols)
            if "SYSCAT.REFERENCES" in u:
                return list(src_header)
            return []

        def tgt_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "SYS.FOREIGN_KEY_COLUMNS" in u:
                return list(tgt_cols)
            if "SYS.FOREIGN_KEYS" in u:
                return list(tgt_header)
            return []

        v = SchemaValidator(self.src, self.tgt, self.opts)
        v._source_adapter = MagicMock()
        v._target_adapter = MagicMock()
        v._source_adapter.execute.side_effect = src_exec
        v._target_adapter.execute.side_effect = tgt_exec

        r = v.validate_foreign_keys("X", "dbo")
        self.assertTrue(r.passed)
        self.assertEqual(
            sum(1 for d in r.details if d.get("status") in ("SOURCE_ONLY", "TARGET_ONLY")),
            0,
        )

    def test_different_referenced_column_names_still_pair_orphans(self):
        """Same child FK columns and ref table; DB2 vs Azure PK column labels differ; pair, do not emit missing."""

        src_header = [
            {
                "fk_name": "TK_IF_PK",
                "schema_name": "X",
                "table_name": "TK_INPUT_FILE_RECS",
                "ref_schema": "X",
                "ref_table": "TK_INPUT_FILES",
                "delete_action": "A",
                "update_action": "A",
            }
        ]
        src_cols = [
            {
                "fk_name": "TK_IF_PK",
                "schema_name": "X",
                "table_name": "TK_INPUT_FILE_RECS",
                "col_seq": 1,
                "fk_column": "TK_INPUT_FILES_ID",
                "pk_column": "LEGACY_PK_COL",
            }
        ]
        tgt_header = [
            {
                "fk_name": "TK_IFR_FK1",
                "schema_name": "dbo",
                "table_name": "TK_INPUT_FILE_RECS",
                "ref_schema": "dbo",
                "ref_table": "TK_INPUT_FILES",
                "delete_action": 0,
                "update_action": 0,
            }
        ]
        tgt_cols = [
            {
                "fk_name": "TK_IFR_FK1",
                "schema_name": "dbo",
                "table_name": "TK_INPUT_FILE_RECS",
                "col_seq": 1,
                "fk_column": "TK_INPUT_FILES_ID",
                "pk_column": "TK_INPUT_FILES_ID",
            }
        ]

        def src_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "KEYCOLUSE" in u:
                return list(src_cols)
            if "SYSCAT.REFERENCES" in u:
                return list(src_header)
            return []

        def tgt_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "SYS.FOREIGN_KEY_COLUMNS" in u:
                return list(tgt_cols)
            if "SYS.FOREIGN_KEYS" in u:
                return list(tgt_header)
            return []

        v = SchemaValidator(self.src, self.tgt, self.opts)
        v._source_adapter = MagicMock()
        v._target_adapter = MagicMock()
        v._source_adapter.execute.side_effect = src_exec
        v._target_adapter.execute.side_effect = tgt_exec

        r = v.validate_foreign_keys("X", "dbo")
        self.assertEqual(
            sum(1 for d in r.details if d.get("status") in ("SOURCE_ONLY", "TARGET_ONLY")),
            0,
        )
        self.assertTrue(any(d.get("status") == "MISMATCH" for d in r.details))

    def test_relaxed_fk_column_table_name_pairs_different_system_names(self):
        """Column rows use a single alternate FKTABNAME; still resolve keys so ...730 pairs ...750."""

        src_header = [
            {
                "fk_name": "SQL150521162547730",
                "schema_name": "X",
                "table_name": "ADVISE_INDEX",
                "ref_schema": "X",
                "ref_table": "ADVISE_INSTANCE",
                "delete_action": "C",
                "update_action": "A",
            }
        ]
        src_cols = [
            {
                "fk_name": "SQL150521162547730",
                "schema_name": "X",
                "table_name": "ALT_CHILD_TAB",
                "col_seq": 1,
                "fk_column": "RUN_ID",
                "pk_column": "START_TIME",
            }
        ]
        tgt_header = [
            {
                "fk_name": "SQL150521162547750",
                "schema_name": "dbo",
                "table_name": "ADVISE_INDEX",
                "ref_schema": "dbo",
                "ref_table": "ADVISE_INSTANCE",
                "delete_action": 1,
                "update_action": 0,
            }
        ]
        tgt_cols = [
            {
                "fk_name": "SQL150521162547750",
                "schema_name": "dbo",
                "table_name": "ADVISE_INDEX",
                "col_seq": 1,
                "fk_column": "RUN_ID",
                "pk_column": "START_TIME",
            }
        ]

        def src_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "KEYCOLUSE" in u:
                return list(src_cols)
            if "SYSCAT.REFERENCES" in u:
                return list(src_header)
            return []

        def tgt_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "SYS.FOREIGN_KEY_COLUMNS" in u:
                return list(tgt_cols)
            if "SYS.FOREIGN_KEYS" in u:
                return list(tgt_header)
            return []

        v = SchemaValidator(self.src, self.tgt, self.opts)
        v._source_adapter = MagicMock()
        v._target_adapter = MagicMock()
        v._source_adapter.execute.side_effect = src_exec
        v._target_adapter.execute.side_effect = tgt_exec

        r = v.validate_foreign_keys("X", "dbo")
        self.assertEqual(
            sum(1 for d in r.details if d.get("status") in ("SOURCE_ONLY", "TARGET_ONLY")),
            0,
        )

    def test_greedy_prefers_exact_name_when_two_source_one_target(self):
        """Same signature bucket: two DB2 names, one Azure FK — Azure pairs to same-named DB2; other DB2 stays orphan."""

        src_header = [
            {
                "fk_name": "SQL_AAA",
                "schema_name": "X",
                "table_name": "T1",
                "ref_schema": "X",
                "ref_table": "P1",
                "delete_action": "A",
                "update_action": "A",
            },
            {
                "fk_name": "SQL_BBB",
                "schema_name": "X",
                "table_name": "T1",
                "ref_schema": "X",
                "ref_table": "P1",
                "delete_action": "A",
                "update_action": "A",
            },
        ]
        src_cols = [
            {"fk_name": "SQL_AAA", "schema_name": "X", "table_name": "T1", "col_seq": 1, "fk_column": "C1", "pk_column": "K1"},
            {"fk_name": "SQL_BBB", "schema_name": "X", "table_name": "T1", "col_seq": 1, "fk_column": "C1", "pk_column": "K1"},
        ]
        tgt_header = [
            {
                "fk_name": "SQL_AAA",
                "schema_name": "dbo",
                "table_name": "T1",
                "ref_schema": "dbo",
                "ref_table": "P1",
                "delete_action": 0,
                "update_action": 0,
            },
        ]
        tgt_cols = [
            {"fk_name": "SQL_AAA", "schema_name": "dbo", "table_name": "T1", "col_seq": 1, "fk_column": "C1", "pk_column": "K1"},
        ]

        def src_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "KEYCOLUSE" in u:
                return list(src_cols)
            if "SYSCAT.REFERENCES" in u:
                return list(src_header)
            return []

        def tgt_exec(sql, params=None, timeout_seconds=None):
            u = sql.upper()
            if "SYS.FOREIGN_KEY_COLUMNS" in u:
                return list(tgt_cols)
            if "SYS.FOREIGN_KEYS" in u:
                return list(tgt_header)
            return []

        v = SchemaValidator(self.src, self.tgt, self.opts)
        v._source_adapter = MagicMock()
        v._target_adapter = MagicMock()
        v._source_adapter.execute.side_effect = src_exec
        v._target_adapter.execute.side_effect = tgt_exec

        r = v.validate_foreign_keys("X", "dbo")
        self.assertEqual(sum(1 for d in r.details if d.get("status") == "SOURCE_ONLY"), 1)
        self.assertEqual(sum(1 for d in r.details if d.get("status") == "TARGET_ONLY"), 0)


if __name__ == "__main__":
    unittest.main()
