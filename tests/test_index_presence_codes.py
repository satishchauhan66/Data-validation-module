"""Index presence error codes: target-only vs source-only use status error (correct ErrorCode)."""
from __future__ import annotations

import unittest

from datavalidation.reporting.index_comparison import compare_indexes_legacy


class TestIndexPresenceCodes(unittest.TestCase):
    def test_index_only_on_target_is_missing_in_source_error(self):
        pairs = [
            {
                "SourceSchemaName": "USERID",
                "SourceObjectName": "ADVISE_INSTANCE",
                "DestinationSchemaName": "dbo",
                "DestinationObjectName": "ADVISE_INSTANCE",
                "s_schema_norm": "USERID",
                "s_object_norm": "ADVISE_INSTANCE",
                "r_schema_norm": "DBO",
                "r_object_norm": "ADVISE_INSTANCE",
            }
        ]
        tgt_ix = [
            {
                "schema_name": "dbo",
                "table_name": "ADVISE_INSTANCE",
                "idx_name": "PK_ADVISE_INSTANCE",
                "is_primary_key": 1,
                "is_unique": 0,
                "colseq": 1,
                "col_name": "START_TIME",
                "is_descending_key": 0,
            }
        ]
        out = compare_indexes_legacy(
            pairs,
            [],
            tgt_ix,
            source_schema="USERID",
            target_schema="dbo",
            src_col_counts={("USERID", "ADVISE_INSTANCE"): 2},
            tgt_col_counts={("DBO", "ADVISE_INSTANCE"): 2},
        )
        row = next(d for d in out if d.get("index") == "PK_ADVISE_INSTANCE")
        self.assertEqual(row["error_code"], "INDEX_MISSING_IN_SOURCE")
        self.assertEqual(row["status"], "error")
        self.assertIsNone(row.get("source_columns"))
        self.assertEqual(row.get("destination_columns"), "START_TIME A")

    def test_index_only_on_source_is_missing_in_target_error(self):
        pairs = [
            {
                "SourceSchemaName": "S",
                "SourceObjectName": "T1",
                "DestinationSchemaName": "dbo",
                "DestinationObjectName": "T1",
                "s_schema_norm": "S",
                "s_object_norm": "T1",
                "r_schema_norm": "DBO",
                "r_object_norm": "T1",
            }
        ]
        src_ix = [
            {
                "schema_name": "S",
                "table_name": "T1",
                "idx_name": "IX_SRC_ONLY",
                "unique_rule": "U",
                "colseq": 1,
                "col_name": "ID",
                "colorder": "A",
            }
        ]
        out = compare_indexes_legacy(
            pairs,
            src_ix,
            [],
            source_schema="S",
            target_schema="dbo",
            src_col_counts={("S", "T1"): 1},
            tgt_col_counts={("DBO", "T1"): 1},
        )
        row = next(d for d in out if d.get("index") == "IX_SRC_ONLY")
        self.assertEqual(row["error_code"], "INDEX_MISSING_IN_TARGET")
        self.assertEqual(row["status"], "error")

    def test_column_mismatch_stays_error(self):
        pairs = [
            {
                "SourceSchemaName": "S",
                "SourceObjectName": "T1",
                "DestinationSchemaName": "dbo",
                "DestinationObjectName": "T1",
                "s_schema_norm": "S",
                "s_object_norm": "T1",
                "r_schema_norm": "DBO",
                "r_object_norm": "T1",
            }
        ]
        src_ix = [
            {
                "schema_name": "S",
                "table_name": "T1",
                "idx_name": "IX1",
                "unique_rule": "D",
                "colseq": 1,
                "col_name": "A",
                "colorder": "A",
            }
        ]
        tgt_ix = [
            {
                "schema_name": "dbo",
                "table_name": "T1",
                "idx_name": "IX1",
                "is_primary_key": 0,
                "is_unique": 0,
                "colseq": 1,
                "col_name": "B",
                "is_descending_key": 0,
            }
        ]
        out = compare_indexes_legacy(
            pairs,
            src_ix,
            tgt_ix,
            source_schema="S",
            target_schema="dbo",
            src_col_counts={("S", "T1"): 2},
            tgt_col_counts={("DBO", "T1"): 2},
        )
        row = next(d for d in out if d.get("index") == "IX1")
        self.assertEqual(row["error_code"], "INDEX_COLUMNS_MISMATCH")
        self.assertEqual(row["status"], "error")


if __name__ == "__main__":
    unittest.main()
