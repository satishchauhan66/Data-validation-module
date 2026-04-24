"""
Result types returned by validation methods.
ValidationResult for single validation; ValidationReport for category/all.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False

from datavalidation.reporting.unified_spec import (
    LEGACY_CSV_VALIDATION_TYPE_ORDER,
    LEGACY_VALIDATION_TYPE_MAP,
    UNIFIED_REPORT_COLUMNS,
)


def _legacy_json_dumps(obj: Any) -> str:
    """JSON for legacy CSV DetailsJson. JDBC/JPype rows may use ``java.lang.String`` etc.; ``default=str`` coerces them."""
    import json

    return json.dumps(obj, separators=(",", ":"), default=str)


@dataclass
class ValidationResult:
    """Result of a single validation (e.g. row counts, table presence)."""
    validation_name: str
    passed: bool
    summary: str
    details: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "validation_name": self.validation_name,
            "passed": self.passed,
            "summary": self.summary,
            "details": self.details,
            "warnings": self.warnings,
            "errors": self.errors,
            "stats": self.stats,
        }

    def to_dataframe(self):
        """Return details as a pandas DataFrame if available."""
        if not _HAS_PANDAS:
            raise ImportError("pandas is required for to_dataframe(). pip install pandas")
        if not self.details:
            return pd.DataFrame()
        return pd.DataFrame(self.details)

    def to_csv(self, path: str | Path) -> None:
        """Write details to a CSV file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df = self.to_dataframe()
        df.to_csv(path, index=False)


@dataclass
class ValidationReport:
    """Report from running multiple validations (e.g. validate_schema, validate_all)."""
    results: dict[str, ValidationResult] = field(default_factory=dict)

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results.values())

    @property
    def summary(self) -> str:
        if not self.results:
            return "No validations run."
        passed = sum(1 for r in self.results.values() if r.passed)
        total = len(self.results)
        lines = [f"Validation Report: {passed}/{total} passed"]
        for name, result in self.results.items():
            icon = "[OK]" if result.passed else "[FAIL]"
            lines.append(f"  {icon} {name}: {result.summary}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "all_passed": self.all_passed,
            "results": {k: v.to_dict() for k, v in self.results.items()},
        }

    def to_dataframe(self):
        """Single DataFrame with a 'validation' column and all detail rows."""
        if not _HAS_PANDAS:
            raise ImportError("pandas is required for to_dataframe(). pip install pandas")
        import pandas as pd
        rows = []
        for name, result in self.results.items():
            for d in result.details:
                row = {"validation": name, **d}
                rows.append(row)
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def to_csv(self, path: str | Path) -> None:
        """Write combined report to CSV."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.to_dataframe().to_csv(path, index=False)

    def to_legacy_csv(self, path: str | Path) -> None:
        """Write report in legacy format: ValidationType, Status, ObjectType, SourceObjectName, SourceSchemaName, DestinationObjectName, DestinationSchemaName, ElementPath, ErrorCode, ErrorDescription, DetailsJson. Matches old validation report exactly."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = []
        for val_name, result in self.results.items():
            validation_type = LEGACY_VALIDATION_TYPE_MAP.get(val_name, val_name)
            for d in result.details:
                # Match old backend: data report only includes row count MISMATCH (not SOURCE_ONLY/TARGET_ONLY)
                if val_name == "row_counts" and d.get("status") != "MISMATCH":
                    continue
                # Presence: use object_name (or table); skip only if both empty
                obj_name = (d.get("object_name") or d.get("table") or "").strip()
                if val_name == "table_presence" and not obj_name:
                    continue
                # Skip index rows with empty name unless informational (e.g. high column count note)
                if val_name == "indexes":
                    idx_s = (d.get("index") or "").strip()
                    if not idx_s and str(d.get("status") or "").upper() != "INFO":
                        continue
                tbl = (d.get("table") or "").strip()
                raw_st = str(d.get("status") or "").strip().upper()
                # Index/FK validators use lowercase "error"; JDBC may return non-Python str — normalize via str().
                if raw_st in ("SOURCE_ONLY", "TARGET_ONLY", "MISMATCH", "ERROR"):
                    status = "error"
                elif raw_st == "WARNING":
                    status = "warning"
                elif raw_st == "INFO":
                    status = "info"
                else:
                    status = "ok"
                raw_type = str(d.get("object_type", "TABLE")).upper()
                obj_type = {"T": "TABLE", "U": "TABLE", "V": "VIEW"}.get(raw_type, raw_type if raw_type else "TABLE")
                # Use logical source/target schema (USERID, dbo) for report; validators must set source_schema/target_schema in details
                src_schema = (d.get("source_schema") or d.get("schema") or "").strip()
                tgt_schema = (d.get("target_schema") or "").strip()
                elem = (d.get("element_path") or (f"{src_schema}.{obj_name}" if src_schema or obj_name else "") or (f"{tgt_schema}.{obj_name}" if tgt_schema or obj_name else "")).strip()
                is_source_only = d.get("status") == "SOURCE_ONLY"
                is_target_only = d.get("status") == "TARGET_ONLY"
                src_obj = obj_name if not is_target_only else ""
                src_sch = src_schema if not is_target_only else ""
                dest_obj = obj_name if not is_source_only else ""
                dest_sch = tgt_schema if not is_source_only else ""
                if val_name == "row_counts":
                    err_code = "ROW_COUNT_MISMATCH"
                    err_desc = "Found mismatch in row-count validation"
                    details_json = _legacy_json_dumps(
                        {"source_row_count": d.get("source_count"), "destination_row_count": d.get("target_count")}
                    )
                elif val_name == "table_presence":
                    err_code = "PRESENCE_MISSING_IN_TARGET" if is_source_only else ("PRESENCE_MISSING_IN_SOURCE" if is_target_only else "PRESENCE_DIFFERENCE")
                    err_desc = "Object exists in source but not in target" if is_source_only else ("Object exists in Azure SQL but not in DB2" if is_target_only else d.get("error_description", ""))
                    details_json = _legacy_json_dumps({
                        "object_type": obj_type,
                        "change_type": "MISSING_IN_TARGET" if is_source_only else ("MISSING_IN_SOURCE" if is_target_only else "DIFFERENCE"),
                        "source_schema_name": src_sch,
                        "source_object_name": src_obj,
                        "destination_schema_name": dest_sch,
                        "destination_object_name": dest_obj,
                    })
                else:
                    err_code = d.get("error_code")
                    err_desc = d.get("error_description", "")
                    details_json = None
                    if validation_type == "default_values":
                        if status != "warning":
                            status = "warning"
                        err_code = err_code or "DEFAULT_MISMATCH"
                        err_desc = err_desc or "Default value difference (treated as warning)"
                        details_json = _legacy_json_dumps({
                            "column_name": d.get("column"),
                            "source_default": d.get("source_default"),
                            "destination_default": d.get("target_default"),
                        })
                    elif validation_type == "foreign_keys":
                        err_code = err_code or "FK_MISMATCH"
                        err_desc = err_desc or ""
                        fk_body = {
                            "constraint_name": d.get("fk_name"),
                            "source_ref_schema": d.get("source_ref_schema"),
                            "source_ref_table": d.get("source_ref_table"),
                            "destination_ref_schema": d.get("destination_ref_schema"),
                            "destination_ref_table": d.get("destination_ref_table"),
                            "source_delete_action": d.get("source_delete_action"),
                            "destination_delete_action": d.get("destination_delete_action"),
                            "source_update_action": d.get("source_update_action"),
                            "destination_update_action": d.get("destination_update_action"),
                            "source_column_pairs": d.get("source_column_pairs"),
                            "destination_column_pairs": d.get("destination_column_pairs"),
                        }
                        details_json = _legacy_json_dumps({k: v for k, v in fk_body.items() if v is not None})
                    elif validation_type == "check_constraints":
                        err_code = err_code or "CHECK_CONSTRAINT_MISMATCH"
                        err_desc = err_desc or ""
                        ck_body = {
                            "constraint_name": d.get("constraint_name"),
                            "source_check_clause": d.get("source_check_clause"),
                            "destination_check_clause": d.get("destination_check_clause"),
                        }
                        details_json = _legacy_json_dumps({k: v for k, v in ck_body.items() if v is not None})
                    elif validation_type == "column_counts":
                        err_code = err_code or "COLUMN_COUNT_MISMATCH"
                        err_desc = err_desc or "Column count mismatch between source and target"
                        details_json = _legacy_json_dumps({
                            "source_column_count": d.get("source_column_count"),
                            "destination_column_count": d.get("destination_column_count")
                            or d.get("target_column_count"),
                        })
                    elif validation_type == "nullable_constraints":
                        err_code = err_code or "NULLABILITY_MISMATCH"
                        err_desc = err_desc or "Nullable constraint mismatch"
                        details_json = _legacy_json_dumps({
                            "column_name": d.get("column"),
                            "source_nullable": d.get("source_nullable"),
                            "destination_nullable": d.get("target_nullable"),
                        })
                    elif validation_type == "datatype_mapping":
                        err_code = err_code or "DATATYPE_NAME_MISMATCH"
                        err_desc = err_desc or "Data type name mismatch"
                        details_json = _legacy_json_dumps({
                            "column_name": d.get("column"),
                            "source_type": d.get("source_type"),
                            "destination_type": d.get("target_type"),
                            "source_data_type": d.get("source_type"),
                            "destination_data_type": d.get("target_type"),
                        })
                    elif validation_type == "indexes":
                        err_code = d.get("error_code") or (
                            "INDEX_MISSING_IN_TARGET"
                            if is_source_only
                            else ("INDEX_MISSING_IN_SOURCE" if is_target_only else "INDEX_MISMATCH")
                        )
                        err_desc = d.get("error_description") or (
                            "Index missing in target"
                            if is_source_only
                            else ("Index missing in source" if is_target_only else "Index columns mismatch")
                        )
                        sc = d.get("source_columns")
                        dc = d.get("destination_columns")
                        details_json = _legacy_json_dumps({
                            "index_name": d.get("index"),
                            "source_columns": sc,
                            "destination_columns": dc,
                            "source_cols": sc,
                            "destination_cols": dc,
                            "source_unique": d.get("source_unique"),
                            "destination_unique": d.get("destination_unique"),
                        })
                    else:
                        err_code = err_code or "MISMATCH"
                    if details_json is None:
                        skip_detail_keys = (
                            "source_schema",
                            "target_schema",
                            "schema",
                            "table",
                            "object_name",
                            "object_type",
                            "status",
                            "element_path",
                            "error_code",
                            "error_description",
                            "index",
                            "source_columns",
                            "destination_columns",
                            "source_unique",
                            "destination_unique",
                            "index_kind",
                            "fk_name",
                            "constraint_name",
                            "column",
                            "source_type",
                            "target_type",
                            "source_nullable",
                            "target_nullable",
                            "source_default",
                            "target_default",
                            "source_column_count",
                            "target_column_count",
                            "destination_column_count",
                            "source_ref_schema",
                            "source_ref_table",
                            "destination_ref_schema",
                            "destination_ref_table",
                            "source_delete_action",
                            "destination_delete_action",
                            "source_update_action",
                            "destination_update_action",
                            "source_column_pairs",
                            "destination_column_pairs",
                            "source_check_clause",
                            "destination_check_clause",
                        )
                        details_json = _legacy_json_dumps(
                            {k: v for k, v in d.items() if k not in skip_detail_keys and v is not None}
                        )
                rows.append({
                    "ValidationType": validation_type,
                    "Status": status,
                    "ObjectType": obj_type,
                    "SourceObjectName": src_obj,
                    "SourceSchemaName": src_sch,
                    "DestinationObjectName": dest_obj,
                    "DestinationSchemaName": dest_sch,
                    "ElementPath": elem,
                    "ErrorCode": err_code,
                    "ErrorDescription": err_desc,
                    "DetailsJson": details_json,
                })

        def _legacy_csv_row_order(row: dict[str, Any]) -> tuple[int, str, str]:
            vt = row.get("ValidationType") or ""
            try:
                idx = LEGACY_CSV_VALIDATION_TYPE_ORDER.index(vt)
            except ValueError:
                idx = len(LEGACY_CSV_VALIDATION_TYPE_ORDER)
            return (idx, row.get("ElementPath") or "", row.get("ErrorCode") or "")

        rows.sort(key=_legacy_csv_row_order)

        legacy_columns = list(UNIFIED_REPORT_COLUMNS)
        if not _HAS_PANDAS:
            import csv
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=legacy_columns)
                w.writeheader()
                if rows:
                    w.writerows(rows)
            return
        df = pd.DataFrame(rows, columns=legacy_columns)
        df.to_csv(path, index=False)
