"""Presence: client-confirmed out-of-scope objects reported as INFO."""
from __future__ import annotations

import unittest

from datavalidation.config import ConnectionConfig, ValidationOptions
from datavalidation.validators.schema import SchemaValidator, _presence_out_of_scope_note


class TestPresenceOutOfScope(unittest.TestCase):
    def test_explain_get_msgs_is_allowlisted(self):
        note = _presence_out_of_scope_note("EXPLAIN_GET_MSGS", "FUNCTION")
        self.assertIsNotNone(note)
        self.assertIn("client confirmed", note.lower())
        self.assertIn("out of scope", note.lower())

    def test_other_functions_not_allowlisted(self):
        self.assertIsNone(_presence_out_of_scope_note("SOME_OTHER_FN", "FUNCTION"))

    def test_cdc_objects_allowlisted_target_side_only(self):
        for name, typ in [
            ("RELOAD_LOG", "TABLE"),
            ("TABLES_CONFIG", "TABLE"),
            ("USP_DELETEINSERTRELOAD", "PROCEDURE"),
            ("USP_PROCESSALLSTAGINGTABLES", "PROCEDURE"),
        ]:
            note = _presence_out_of_scope_note(name, typ, side="target")
            self.assertIsNotNone(note, f"{name} should be out-of-scope on target side")
            self.assertIn("cdc", note.lower())
            # These are Azure-only; must NOT be suppressed if they appeared source-only
            self.assertIsNone(_presence_out_of_scope_note(name, typ, side="source"))

    def test_explain_get_msgs_only_source_side(self):
        self.assertIsNotNone(_presence_out_of_scope_note("EXPLAIN_GET_MSGS", "FUNCTION", side="source"))
        self.assertIsNone(_presence_out_of_scope_note("EXPLAIN_GET_MSGS", "FUNCTION", side="target"))

    def test_source_only_explain_get_msgs_is_info(self):
        src = ConnectionConfig(type="db2", host="h", database="d", username="u", password="x")
        tgt = ConnectionConfig(type="azure_sql", host="h", database="d", username="u", password="x")
        v = SchemaValidator(src, tgt, ValidationOptions())

        def src_exec(sql: str, *a, **k):
            s = sql.lower()
            if "syscat.routines" in s or "sysroutines" in s or "routines" in s:
                return [
                    {
                        "schema_name": "USERID",
                        "object_name": "EXPLAIN_GET_MSGS",
                        "table_name": "EXPLAIN_GET_MSGS",
                        "object_type": "FUNCTION",
                    }
                ]
            # catalog_objects_query path
            if "function" in s or "routine" in s:
                return [
                    {
                        "schema_name": "USERID",
                        "object_name": "EXPLAIN_GET_MSGS",
                        "table_name": "EXPLAIN_GET_MSGS",
                        "object_type": "FUNCTION",
                    }
                ]
            return []

        def tgt_exec(sql: str, *a, **k):
            return []

        v._source_execute = src_exec  # type: ignore[method-assign]
        v._target_execute = tgt_exec  # type: ignore[method-assign]
        v._resolve_source_schema = lambda s: "USERID"  # type: ignore[method-assign]

        # Force known dialect query path via catalog_objects_query by stubbing dialect methods
        class _SrcD:
            name = "db2"

            def catalog_objects_query(self, schema, object_types):
                return "SELECT * FROM SYSCAT.ROUTINES -- function"

            def catalog_tables_query(self, schema, object_types):
                return None

            def catalog_presence_sequences_query(self, schema):
                return None

            def catalog_presence_indexes_query(self, schema):
                return None

            def catalog_presence_constraints_query(self, schema):
                return None

            def catalog_identity_columns_query(self, schema):
                return None

            def catalog_index_lookup_by_name_query(self, index_name, table_name=None):
                return None

        class _TgtD:
            name = "azure_sql"

            def catalog_objects_query(self, schema, object_types):
                return "SELECT * FROM sys.objects -- function"

            def catalog_tables_query(self, schema, object_types):
                return None

            def catalog_presence_sequences_query(self, schema):
                return None

            def catalog_presence_indexes_query(self, schema):
                return None

            def catalog_presence_constraints_query(self, schema):
                return None

            def catalog_identity_columns_query(self, schema):
                return None

        v._source_dialect = _SrcD()  # type: ignore[assignment]
        v._target_dialect = _TgtD()  # type: ignore[assignment]

        result = v.validate_table_presence(
            source_schema="USERID",
            target_schema="dbo",
            object_types=["FUNCTION"],
        )
        rows = [d for d in result.details if str(d.get("object_name", "")).upper() == "EXPLAIN_GET_MSGS"]
        self.assertEqual(len(rows), 1, result.details)
        self.assertEqual(rows[0]["status"], "INFO")
        self.assertEqual(rows[0]["error_code"], "PRESENCE_OUT_OF_SCOPE")
        self.assertIn("client confirmed", rows[0]["error_description"].lower())
        self.assertEqual(result.stats.get("diff_count"), 0)


if __name__ == "__main__":
    unittest.main()
