"""Legacy CSV: table-scoped validations (FK, index, check) keep both sides' table + schema populated."""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from datavalidation.results import ValidationReport, ValidationResult


class TestLegacyCsvTableScoped(unittest.TestCase):
    def test_foreign_keys_target_only_fills_source_columns(self):
        rep = ValidationReport(
            results={
                "foreign_keys": ValidationResult(
                    validation_name="foreign_keys",
                    passed=False,
                    summary="n",
                    details=[
                        {
                            "source_schema": "FILES_DB_TEST",
                            "target_schema": "dbo",
                            "schema": "dbo",
                            "table": "TK_INPUT_FILE_RECS",
                            "fk_name": "TK_IFR_FK1",
                            "status": "TARGET_ONLY",
                            "object_type": "TABLE",
                            "element_path": "dbo.TK_INPUT_FILE_RECS.TK_IFR_FK1",
                            "error_code": "FK_MISMATCH",
                            "error_description": "FK missing in source",
                            "destination_ref_schema": "dbo",
                            "destination_ref_table": "TK_INPUT_FILES",
                            "destination_column_pairs": "TK_INPUT_FILES_ID->TK_INPUT_FILES_ID",
                        }
                    ],
                )
            }
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "out.csv"
            rep.to_legacy_csv(path)
            with open(path, newline="", encoding="utf-8") as f:
                row = next(csv.DictReader(f))
        self.assertEqual(row["SourceObjectName"], "TK_INPUT_FILE_RECS")
        self.assertEqual(row["SourceSchemaName"], "FILES_DB_TEST")
        self.assertEqual(row["DestinationObjectName"], "TK_INPUT_FILE_RECS")
        self.assertEqual(row["DestinationSchemaName"], "dbo")

    def test_foreign_keys_source_only_fills_destination_columns(self):
        rep = ValidationReport(
            results={
                "foreign_keys": ValidationResult(
                    validation_name="foreign_keys",
                    passed=False,
                    summary="n",
                    details=[
                        {
                            "source_schema": "FILES_DB_TEST",
                            "target_schema": "dbo",
                            "schema": "FILES_DB_TEST",
                            "table": "CHILD_T",
                            "fk_name": "SQL0001",
                            "status": "SOURCE_ONLY",
                            "object_type": "TABLE",
                            "element_path": "FILES_DB_TEST.CHILD_T.SQL0001",
                            "error_code": "FK_MISMATCH",
                            "error_description": "FK missing in target",
                            "source_ref_schema": "FILES_DB_TEST",
                            "source_ref_table": "PARENT_T",
                        }
                    ],
                )
            }
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "out.csv"
            rep.to_legacy_csv(path)
            with open(path, newline="", encoding="utf-8") as f:
                row = next(csv.DictReader(f))
        self.assertEqual(row["SourceObjectName"], "CHILD_T")
        self.assertEqual(row["SourceSchemaName"], "FILES_DB_TEST")
        self.assertEqual(row["DestinationObjectName"], "CHILD_T")
        self.assertEqual(row["DestinationSchemaName"], "dbo")

    def test_indexes_target_only_fills_source_columns(self):
        rep = ValidationReport(
            results={
                "indexes": ValidationResult(
                    validation_name="indexes",
                    passed=False,
                    summary="n",
                    details=[
                        {
                            "source_schema": "S",
                            "target_schema": "dbo",
                            "schema": "dbo",
                            "table": "T1",
                            "index": "IX_T1_A",
                            "status": "TARGET_ONLY",
                            "element_path": "dbo.T1.IX_T1_A",
                        }
                    ],
                )
            }
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "out.csv"
            rep.to_legacy_csv(path)
            with open(path, newline="", encoding="utf-8") as f:
                row = next(csv.DictReader(f))
        self.assertEqual(row["SourceObjectName"], "T1")
        self.assertEqual(row["SourceSchemaName"], "S")
        self.assertEqual(row["DestinationObjectName"], "T1")
        self.assertEqual(row["DestinationSchemaName"], "dbo")

    def test_table_presence_target_only_still_clears_source(self):
        """Presence rows describe a whole object missing on one side; do not apply FK-style fill."""
        rep = ValidationReport(
            results={
                "table_presence": ValidationResult(
                    validation_name="table_presence",
                    passed=False,
                    summary="n",
                    details=[
                        {
                            "source_schema": "S",
                            "target_schema": "dbo",
                            "schema": "dbo",
                            "table": "ONLY_TGT",
                            "object_name": "ONLY_TGT",
                            "object_type": "TABLE",
                            "status": "TARGET_ONLY",
                            "element_path": "dbo.ONLY_TGT",
                        }
                    ],
                )
            }
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "out.csv"
            rep.to_legacy_csv(path)
            with open(path, newline="", encoding="utf-8") as f:
                row = next(csv.DictReader(f))
        self.assertEqual(row["SourceObjectName"], "")
        self.assertEqual(row["SourceSchemaName"], "")
        self.assertEqual(row["DestinationObjectName"], "ONLY_TGT")


if __name__ == "__main__":
    unittest.main()
