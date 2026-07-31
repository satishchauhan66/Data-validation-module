"""Datatype mapping: app-team column type overrides reported as INFO (acknowledged)."""
from __future__ import annotations

import os
import unittest

from datavalidation.config import ConnectionConfig, ValidationOptions, normalize_column_type_overrides
from datavalidation.validators.schema import SchemaValidator


class TestNormalizeOverrides(unittest.TestCase):
    def test_dict_is_upper_cased(self):
        out = normalize_column_type_overrides({"user_name": "VARCHAR(150)", "UID": "varchar(150)"})
        self.assertEqual(out, {"USER_NAME": "VARCHAR(150)", "UID": "varchar(150)"})

    def test_json_string_parsed(self):
        out = normalize_column_type_overrides('{"USER_ID": "VARCHAR(150)"}')
        self.assertEqual(out, {"USER_ID": "VARCHAR(150)"})

    def test_bad_input_returns_empty(self):
        self.assertEqual(normalize_column_type_overrides("not json"), {})
        self.assertEqual(normalize_column_type_overrides(None), {})


class TestColumnOverrideMatch(unittest.TestCase):
    def test_matches_base_type_ignoring_length(self):
        self.assertTrue(SchemaValidator._azure_type_matches_override("varchar", "VARCHAR(150)"))
        self.assertTrue(SchemaValidator._azure_type_matches_override("varchar(150)", "VARCHAR(150)"))
        self.assertFalse(SchemaValidator._azure_type_matches_override("nvarchar", "VARCHAR(150)"))

    def test_resolve_prefers_table_column(self):
        ov = {"UPD_LOG.USER_NAME": "VARCHAR(200)", "USER_NAME": "VARCHAR(150)"}
        self.assertEqual(SchemaValidator._resolve_column_override(ov, "UPD_LOG", "USER_NAME"), "VARCHAR(200)")
        self.assertEqual(SchemaValidator._resolve_column_override(ov, "OTHER", "USER_NAME"), "VARCHAR(150)")


class TestDatatypeOverrideAcknowledged(unittest.TestCase):
    def _validator(self, overrides):
        src = ConnectionConfig(type="db2", host="h", database="d", username="u", password="x")
        tgt = ConnectionConfig(type="azure_sql", host="h", database="d", username="u", password="x")
        return SchemaValidator(src, tgt, ValidationOptions(column_type_overrides=overrides))

    def test_character_to_varchar_override_is_info(self):
        v = self._validator({"USER_NAME": "VARCHAR(150)"})

        def src_exec(sql, *a, **k):
            return [{"schema_name": "USERID", "table_name": "UPD_LOG", "column_name": "USER_NAME", "data_type": "CHARACTER"}]

        def tgt_exec(sql, *a, **k):
            return [{"schema_name": "dbo", "table_name": "UPD_LOG", "column_name": "USER_NAME", "data_type": "varchar"}]

        v._source_execute = src_exec  # type: ignore[method-assign]
        v._target_execute = tgt_exec  # type: ignore[method-assign]
        v._resolve_source_schema = lambda s: "USERID"  # type: ignore[method-assign]
        v._table_kind_map = lambda schema, source=True: {"UPD_LOG": "TABLE"}  # type: ignore[method-assign]

        result = v.validate_datatype_mapping(source_schema="USERID", target_schema="dbo")
        rows = [d for d in result.details if d.get("column") == "USER_NAME"]
        self.assertEqual(len(rows), 1, result.details)
        self.assertEqual(rows[0]["status"], "INFO")
        self.assertEqual(rows[0]["error_code"], "DATATYPE_OVERRIDE_ACKNOWLEDGED")
        self.assertIn("App team", rows[0]["error_description"])
        self.assertEqual(rows[0]["mapping"]["override"]["requested_type"], "VARCHAR(150)")
        self.assertEqual(result.stats["mismatch_count"], 0)
        self.assertTrue(result.passed)

    def test_without_override_stays_error(self):
        v = self._validator({})

        def src_exec(sql, *a, **k):
            return [{"schema_name": "USERID", "table_name": "UPD_LOG", "column_name": "USER_NAME", "data_type": "CHARACTER"}]

        def tgt_exec(sql, *a, **k):
            return [{"schema_name": "dbo", "table_name": "UPD_LOG", "column_name": "USER_NAME", "data_type": "varchar"}]

        v._source_execute = src_exec  # type: ignore[method-assign]
        v._target_execute = tgt_exec  # type: ignore[method-assign]
        v._resolve_source_schema = lambda s: "USERID"  # type: ignore[method-assign]
        v._table_kind_map = lambda schema, source=True: {"UPD_LOG": "TABLE"}  # type: ignore[method-assign]

        result = v.validate_datatype_mapping(source_schema="USERID", target_schema="dbo")
        rows = [d for d in result.details if d.get("column") == "USER_NAME"]
        self.assertEqual(rows[0]["status"], "MISMATCH")
        self.assertEqual(rows[0]["error_code"], "DATATYPE_NAME_MISMATCH")
        self.assertEqual(result.stats["mismatch_count"], 1)

    def test_env_var_override(self):
        v = self._validator({})
        os.environ["DV_COLUMN_TYPE_OVERRIDES"] = '{"USER_NAME": "VARCHAR(150)"}'
        try:
            def src_exec(sql, *a, **k):
                return [{"schema_name": "USERID", "table_name": "UPD_LOG", "column_name": "USER_NAME", "data_type": "CHARACTER"}]

            def tgt_exec(sql, *a, **k):
                return [{"schema_name": "dbo", "table_name": "UPD_LOG", "column_name": "USER_NAME", "data_type": "varchar"}]

            v._source_execute = src_exec  # type: ignore[method-assign]
            v._target_execute = tgt_exec  # type: ignore[method-assign]
            v._resolve_source_schema = lambda s: "USERID"  # type: ignore[method-assign]
            v._table_kind_map = lambda schema, source=True: {"UPD_LOG": "TABLE"}  # type: ignore[method-assign]

            result = v.validate_datatype_mapping(source_schema="USERID", target_schema="dbo")
            rows = [d for d in result.details if d.get("column") == "USER_NAME"]
            self.assertEqual(rows[0]["error_code"], "DATATYPE_OVERRIDE_ACKNOWLEDGED")
        finally:
            del os.environ["DV_COLUMN_TYPE_OVERRIDES"]


if __name__ == "__main__":
    unittest.main()
