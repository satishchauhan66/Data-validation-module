"""Presence: DB2 index owned in another schema on a USERID table → INFO (not MISSING_IN_SOURCE)."""
from __future__ import annotations

import unittest
from datavalidation.config import ConnectionConfig, ValidationOptions
from datavalidation.dialects.db2 import DB2Dialect
from datavalidation.validators.schema import SchemaValidator


class TestDb2CrossSchemaIndexPresenceQuery(unittest.TestCase):
    def test_presence_indexes_filter_by_tabschema_and_return_index_schema(self):
        q = DB2Dialect().catalog_presence_indexes_query("USERID")
        ql = q.lower()
        self.assertIn("rtrim(tabschema)", ql)
        self.assertIn("as index_schema", ql)
        self.assertIn("rtrim(indschema)", ql)
        # Presence must key off table schema, not index owner
        self.assertIn("upper(rtrim(tabschema)) = upper('userid')", ql)
        self.assertNotIn("upper(rtrim(indschema)) = upper('userid')", ql)


class TestCrossSchemaIndexPresenceRemap(unittest.TestCase):
    def _validator(self) -> SchemaValidator:
        src = ConnectionConfig(type="db2", host="h", database="d", username="INFOQFIT", password="x")
        tgt = ConnectionConfig(type="azure_sql", host="h", database="d", username="u", password="x")
        v = SchemaValidator(src, tgt, ValidationOptions())
        return v

    def test_matched_cross_schema_index_is_info_not_target_only(self):
        v = self._validator()
        obj = "SURVEY_RESPONSE_EVENT.SURVEY_RESPONSE_EVENT_SURVEYID_IDX"

        def src_exec(sql: str, *a, **k):
            s = sql.lower()
            if "syscat.indexes" in s and "indname" in s and "tabschema" in s and "object_name" in s:
                return [
                    {
                        "schema_name": "USERID",
                        "object_name": obj,
                        "object_type": "INDEX",
                        "index_schema": "CARVAR",
                    }
                ]
            if "syscat.indexes" in s and "lookup" in s or (
                "indname" in s and "index_schema" in s and "table_schema" in s and "object_name" not in s
            ):
                return []
            # base objects / sequences / constraints
            return []

        def tgt_exec(sql: str, *a, **k):
            s = sql.lower()
            if "sys.indexes" in s:
                return [
                    {
                        "schema_name": "dbo",
                        "object_name": obj,
                        "object_type": "INDEX",
                    }
                ]
            if "is_identity" in s:
                return []
            return []

        v._source_execute = src_exec  # type: ignore[method-assign]
        v._target_execute = tgt_exec  # type: ignore[method-assign]
        v._resolve_source_schema = lambda s: "USERID"  # type: ignore[method-assign]

        result = v.validate_table_presence(
            source_schema="USERID",
            target_schema="dbo",
            object_types=["INDEX"],
        )
        info = [d for d in result.details if d.get("error_code") == "INDEX_CROSS_SCHEMA_REMAPPED"]
        self.assertEqual(len(info), 1, result.details)
        self.assertEqual(info[0]["status"], "INFO")
        self.assertEqual(info[0]["mapping"]["source_of_truth"]["index_schema"], "CARVAR")
        hard = [d for d in result.details if d.get("status") in ("TARGET_ONLY", "SOURCE_ONLY")]
        self.assertEqual(hard, [])

    def test_target_only_resolved_by_name_lookup(self):
        """Azure has index; presence missed it on source → lookup by name finds CARVAR owner."""
        v = self._validator()
        obj = "SURVEY_RESPONSE_EVENT.SURVEY_RESPONSE_EVENT_SURVEYID_IDX"

        def src_exec(sql: str, *a, **k):
            s = sql.lower()
            # Presence by TABSCHEMA returns nothing (simulates miss); lookup finds it
            if "upper(rtrim(indname))" in s and "table_schema" in s:
                return [
                    {
                        "index_schema": "CARVAR",
                        "table_schema": "USERID",
                        "table_name": "SURVEY_RESPONSE_EVENT",
                        "index_name": "SURVEY_RESPONSE_EVENT_SURVEYID_IDX",
                        "unique_rule": "D",
                    }
                ]
            return []

        def tgt_exec(sql: str, *a, **k):
            s = sql.lower()
            if "sys.indexes" in s:
                return [{"schema_name": "dbo", "object_name": obj, "object_type": "INDEX"}]
            return []

        v._source_execute = src_exec  # type: ignore[method-assign]
        v._target_execute = tgt_exec  # type: ignore[method-assign]
        v._resolve_source_schema = lambda s: "USERID"  # type: ignore[method-assign]

        result = v.validate_table_presence(
            source_schema="USERID",
            target_schema="dbo",
            object_types=["INDEX"],
        )
        self.assertFalse(any(d.get("status") == "TARGET_ONLY" for d in result.details), result.details)
        info = [d for d in result.details if d.get("error_code") == "INDEX_CROSS_SCHEMA_REMAPPED"]
        self.assertEqual(len(info), 1, result.details)


if __name__ == "__main__":
    unittest.main()
