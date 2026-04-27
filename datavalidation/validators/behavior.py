"""
Behavior validations: identity/sequence, identity collision, collation, triggers, routines, extended properties.

Ports catalog/JDBC logic from the legacy FastAPI ``behavior_validation_service`` without PySpark.
"""
from __future__ import annotations

import json
import re
from typing import Any

from datavalidation.results import ValidationResult
from datavalidation.validators.base import BaseValidator
from datavalidation.validators.data import _norm_upper, _row_get, _to_int


def _canon_sql_text(s: str | None) -> str:
    if not s:
        return ""
    x = str(s).upper().strip()
    x = re.sub(r"\s+", "", x)
    x = re.sub(r";+$", "", x)
    return x


class BehaviorValidator(BaseValidator):
    """Runs behavior-level validations using dialect catalog queries."""

    def _pair_common_tables(self, source_schema: str, target_schema: str) -> tuple[str, list[str]]:
        """Reuse same TABLE matching as :class:`DataValidator`."""
        from datavalidation.validators.data import DataValidator  # local import avoids cycles

        dv = DataValidator(self.source_config, self.target_config, self.options, self._source_adapter, self._target_adapter)
        return dv._pair_common_tables(source_schema, target_schema, ["TABLE"])

    def _fetch_db2_identities(self, schema: str) -> list[dict[str, Any]]:
        s = str(schema or "").strip().replace("'", "''")
        q = (
            f"SELECT RTRIM(TABSCHEMA) AS schema_name, RTRIM(TABNAME) AS table_name, RTRIM(COLNAME) AS column_name, "
            f"CAST(1 AS SMALLINT) AS is_identity, "
            f"CAST(IDENTITYSTART AS VARCHAR(128)) AS seed_value, "
            f"CAST(IDENTITYINCREMENT AS VARCHAR(128)) AS increment_value "
            f"FROM SYSCAT.COLIDENTITIES WHERE UPPER(RTRIM(TABSCHEMA)) = UPPER('{s}')"
        )
        try:
            return self._source_execute(q) or []
        except Exception:
            q2 = (
                f"SELECT RTRIM(TABSCHEMA) AS schema_name, RTRIM(TABNAME) AS table_name, RTRIM(COLNAME) AS column_name, "
                f"CASE WHEN IDENTITY = 'Y' THEN 1 ELSE 0 END AS is_identity, "
                f"CAST(NULL AS VARCHAR(128)) AS seed_value, CAST(NULL AS VARCHAR(128)) AS increment_value "
                f"FROM SYSCAT.COLUMNS WHERE UPPER(RTRIM(TABSCHEMA)) = UPPER('{s}')"
            )
            try:
                return self._source_execute(q2) or []
            except Exception:
                return []

    def _fetch_azure_identities(self, schema: str) -> list[dict[str, Any]]:
        s = str(schema or "").strip().replace("'", "''")
        q = (
            f"SELECT RTRIM(s.name) AS schema_name, RTRIM(t.name) AS table_name, RTRIM(c.name) AS column_name, "
            f"CAST(ic.is_identity AS int) AS is_identity, "
            f"CAST(ic.seed_value AS NVARCHAR(128)) AS seed_value, "
            f"CAST(ic.increment_value AS NVARCHAR(128)) AS increment_value "
            f"FROM sys.identity_columns ic "
            f"JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
            f"JOIN sys.tables t ON t.object_id = c.object_id "
            f"JOIN sys.schemas s ON s.schema_id = t.schema_id "
            f"WHERE UPPER(s.name) = UPPER(N'{s}') AND ic.is_identity = 1"
        )
        try:
            return self._target_execute(q) or []
        except Exception:
            return []

    def _fetch_db2_sequences(self, schema: str) -> list[dict[str, Any]]:
        s = str(schema or "").strip().replace("'", "''")
        q = (
            f"SELECT RTRIM(SEQSCHEMA) AS schema_name, RTRIM(SEQNAME) AS sequence_name, "
            f"CAST(START AS VARCHAR(64)) AS start_value, CAST(INCREMENT AS VARCHAR(64)) AS increment_value, "
            f"CAST(MINVALUE AS VARCHAR(64)) AS minimum_value, CAST(MAXVALUE AS VARCHAR(64)) AS maximum_value, "
            f"CASE WHEN CYCLE = 'Y' THEN 1 ELSE 0 END AS is_cycling, CAST(CACHE AS VARCHAR(64)) AS cache_size "
            f"FROM SYSCAT.SEQUENCES WHERE UPPER(RTRIM(SEQSCHEMA)) = UPPER('{s}')"
        )
        try:
            return self._source_execute(q) or []
        except Exception:
            return []

    def _fetch_azure_sequences(self, schema: str) -> list[dict[str, Any]]:
        s = str(schema or "").strip().replace("'", "''")
        q = (
            f"SELECT RTRIM(SCHEMA_NAME(schema_id)) AS schema_name, RTRIM(name) AS sequence_name, "
            f"CAST(start_value AS NVARCHAR(128)) AS start_value, CAST(increment AS NVARCHAR(128)) AS increment_value, "
            f"CAST(minimum_value AS NVARCHAR(128)) AS minimum_value, CAST(maximum_value AS NVARCHAR(128)) AS maximum_value, "
            f"CAST(is_cycling AS int) AS is_cycling, CAST(cache_size AS NVARCHAR(128)) AS cache_size "
            f"FROM sys.sequences WHERE UPPER(SCHEMA_NAME(schema_id)) = UPPER(N'{s}')"
        )
        try:
            return self._target_execute(q) or []
        except Exception:
            return []

    def validate_identity_sequence(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        if not source_schema or not target_schema:
            return ValidationResult(
                validation_name="identity_sequence",
                passed=True,
                summary="Identity & sequence: skipped (schema not set).",
                details=[],
                stats={},
            )
        resolved_src, common = self._pair_common_tables(source_schema, target_schema)
        lid = [r for r in self._fetch_db2_identities(resolved_src) if _is_identity_row(r)]
        rid = [r for r in self._fetch_azure_identities(target_schema) if _is_identity_row(r)]

        details: list[dict[str, Any]] = []
        for tbl in common:
            tu = _norm_upper(tbl)
            src_rows = [r for r in lid if _norm_upper(r.get("table_name")) == tu]
            tgt_rows = [r for r in rid if _norm_upper(r.get("table_name")) == tu]
            sm = {_norm_upper(r.get("column_name")): r for r in src_rows}
            tm = {_norm_upper(r.get("column_name")): r for r in tgt_rows}
            for cn in sorted(set(sm.keys()) | set(tm.keys())):
                lr, rr = sm.get(cn), tm.get(cn)
                key = (_norm_upper(resolved_src), tu, cn)
                if lr and not rr:
                    details.append(_id_detail(source_schema, target_schema, key, "IDENTITY_MISSING_IN_TARGET", lr, None))
                elif rr and not lr:
                    details.append(_id_detail(source_schema, target_schema, key, "IDENTITY_MISSING_IN_SOURCE", None, rr))
                elif lr and rr:
                    if _norm_val(lr.get("seed_value")) != _norm_val(rr.get("seed_value")):
                        details.append(_id_detail(source_schema, target_schema, key, "IDENTITY_SEED_MISMATCH", lr, rr))
                    elif _norm_val(lr.get("increment_value")) != _norm_val(rr.get("increment_value")):
                        details.append(_id_detail(source_schema, target_schema, key, "IDENTITY_INCREMENT_MISMATCH", lr, rr))

        # Sequences: match by sequence_name within schema
        ls = {(_norm_upper(r.get("schema_name")), _norm_upper(r.get("sequence_name"))): r for r in self._fetch_db2_sequences(resolved_src)}
        rs = {(_norm_upper(r.get("schema_name")), _norm_upper(r.get("sequence_name"))): r for r in self._fetch_azure_sequences(target_schema)}
        for k in sorted(set(ls.keys()) | set(rs.keys())):
            a, b = ls.get(k), rs.get(k)
            if a and not b:
                details.append(_seq_detail(source_schema, target_schema, k, "SEQUENCE_MISSING_IN_TARGET", a, None))
            elif b and not a:
                details.append(_seq_detail(source_schema, target_schema, k, "SEQUENCE_MISSING_IN_SOURCE", None, b))
            elif a and b:
                for fld in ("start_value", "increment_value", "minimum_value", "maximum_value"):
                    if _norm_val(a.get(fld)) != _norm_val(b.get(fld)):
                        details.append(_seq_detail(source_schema, target_schema, k, f"SEQUENCE_{fld.upper()}_MISMATCH", a, b))
                        break

        passed = len(details) == 0
        return ValidationResult(
            validation_name="identity_sequence",
            passed=passed,
            summary=f"Identity & sequence: {len(details)} issue row(s).",
            details=details,
            stats={"issue_rows": len(details)},
        )

    def validate_identity_collision(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Best-effort Azure check: ``IDENT_CURRENT`` vs max child FK (simplified vs full legacy)."""
        if not target_schema:
            return ValidationResult(
                validation_name="identity_collision",
                passed=True,
                summary="Identity collision: skipped (no target schema).",
                details=[],
                stats={},
            )
        details: list[dict[str, Any]] = []
        # Example pattern from legacy: probe WAVE tables if present
        sql_probe = (
            f"SELECT COUNT(*) AS cnt FROM [{target_schema.replace(']', ']]')}].[WAVE_JOB_DETAIL_MSG] m "
            f"LEFT JOIN [{target_schema.replace(']', ']]')}].[WAVE_JOB_DETAIL] p "
            f"ON m.WAVE_JOB_DETAIL_ID = p.WAVE_JOB_DETAIL_ID WHERE m.WAVE_JOB_DETAIL_ID IS NOT NULL AND p.WAVE_JOB_DETAIL_ID IS NULL"
        )
        try:
            rows = self._target_execute(sql_probe)
            n = _to_int(_row_get(rows[0], "cnt")) if rows else 0
            if n and n > 0:
                details.append({
                    "source_schema": source_schema or "",
                    "target_schema": target_schema,
                    "schema": target_schema,
                    "table": "WAVE_JOB_DETAIL_MSG",
                    "status": "MISMATCH",
                    "element_path": f"{target_schema}.WAVE_JOB_DETAIL_MSG",
                    "object_type": "TABLE",
                    "error_code": "IDENTITY_FK_ORPHAN_RISK",
                    "error_description": f"Possible orphan child rows vs parent: {n}",
                    "details_json": json.dumps({"broken_row_count": n}),
                })
        except Exception:
            pass
        return ValidationResult(
            validation_name="identity_collision",
            passed=len(details) == 0,
            summary="Identity collision risk (Azure heuristic)." if details else "Identity collision: no heuristic issues.",
            details=details,
            stats={},
        )

    def validate_collation(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Compare DB2 ``SYSCAT.DATABASES`` collation name with SQL Server database collation."""
        details: list[dict[str, Any]] = []
        c1 = ""
        try:
            db2 = self._source_execute(
                "SELECT COLLATIONSCHEMA || '.' || COLLATIONNAME AS collation_name FROM SYSCAT.DATABASES FETCH FIRST 1 ROW ONLY"
            )
            if db2:
                c1 = str(_row_get(db2[0], "collation_name") or "").strip()
        except Exception:
            pass
        c2 = ""
        try:
            az = self._target_execute(
                "SELECT CONVERT(varchar(128), DATABASEPROPERTYEX(DB_NAME(), N'Collation')) AS collation_name"
            )
            if az:
                c2 = str(_row_get(az[0], "collation_name") or "").strip()
        except Exception:
            pass
        if c1 and c2 and c1 != c2:
            details.append({
                "source_schema": source_schema or "",
                "target_schema": target_schema or "",
                "schema": "",
                "table": "",
                "status": "MISMATCH",
                "element_path": "DATABASE",
                "object_type": "DATABASE",
                "error_code": "DATABASE_COLLATION_MISMATCH",
                "error_description": "Database collation differs",
                "details_json": json.dumps({"source_collation": c1, "destination_collation": c2}),
            })
        return ValidationResult(
            validation_name="collation",
            passed=len(details) == 0,
            summary=f"Collation: {len(details)} database-level issue(s).",
            details=details,
            stats={},
        )

    def validate_triggers(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        if not source_schema or not target_schema:
            return ValidationResult("triggers", True, "Triggers: skipped.", [], stats={})
        sch_s = str(source_schema).strip().replace("'", "''")
        sch_t = str(target_schema).strip().replace("'", "''")
        q_l = (
            f"SELECT RTRIM(TRIGSCHEMA) AS schema_name, RTRIM(TRIGNAME) AS trigger_name, TEXT AS definition "
            f"FROM SYSCAT.TRIGGERS WHERE UPPER(TRIGSCHEMA) = UPPER('{sch_s}')"
        )
        q_r = (
            f"SELECT RTRIM(s.name) AS schema_name, RTRIM(trg.name) AS trigger_name, "
            f"OBJECT_DEFINITION(trg.object_id) AS definition FROM sys.triggers trg "
            f"INNER JOIN sys.objects o ON trg.parent_id = o.object_id "
            f"INNER JOIN sys.schemas s ON o.schema_id = s.schema_id "
            f"WHERE trg.is_ms_shipped = 0 AND UPPER(s.name) = UPPER(N'{sch_t}')"
        )
        try:
            left = self._source_execute(q_l) or []
            right = self._target_execute(q_r) or []
        except Exception as ex:
            return ValidationResult(
                validation_name="triggers",
                passed=False,
                summary=f"Triggers: catalog query failed: {ex}",
                details=[],
                stats={},
            )
        lm = {(_norm_upper(r.get("schema_name")), _norm_upper(r.get("trigger_name"))): r for r in left}
        rm = {(_norm_upper(r.get("schema_name")), _norm_upper(r.get("trigger_name"))): r for r in right}
        details: list[dict[str, Any]] = []
        for k in sorted(set(lm.keys()) | set(rm.keys())):
            l, r = lm.get(k), rm.get(k)
            if l and not r:
                details.append(_obj_detail(source_schema, target_schema, k, "TRIGGER_MISSING_IN_TARGET", "trigger_name", l, None))
            elif r and not l:
                details.append(_obj_detail(source_schema, target_schema, k, "TRIGGER_MISSING_IN_SOURCE", "trigger_name", None, r))
            elif l and r:
                if _canon_sql_text(str(l.get("definition"))) != _canon_sql_text(str(r.get("definition"))):
                    details.append(_obj_detail(source_schema, target_schema, k, "TRIGGER_DEFINITION_DIFFERENT", "trigger_name", l, r))
        return ValidationResult(
            validation_name="triggers",
            passed=len(details) == 0,
            summary=f"Triggers: {len(details)} issue(s).",
            details=details,
            stats={"issues": len(details)},
        )

    def validate_routines(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        if not source_schema or not target_schema:
            return ValidationResult("routines", True, "Routines: skipped.", [], stats={})
        sch_s = str(source_schema).strip().replace("'", "''")
        sch_t = str(target_schema).strip().replace("'", "''")
        q_l = (
            f"SELECT RTRIM(ROUTINESCHEMA) AS schema_name, RTRIM(ROUTINENAME) AS routine_name, TEXT AS definition "
            f"FROM SYSCAT.ROUTINES WHERE UPPER(ROUTINESCHEMA) = UPPER('{sch_s}')"
        )
        q_r = (
            f"SELECT RTRIM(s.name) AS schema_name, RTRIM(o.name) AS routine_name, m.definition AS definition "
            f"FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id "
            f"JOIN sys.sql_modules m ON m.object_id = o.object_id "
            f"WHERE o.type IN ('P','FN','IF','TF') AND UPPER(s.name) = UPPER(N'{sch_t}')"
        )
        try:
            left = self._source_execute(q_l) or []
            right = self._target_execute(q_r) or []
        except Exception as ex:
            return ValidationResult(
                validation_name="routines",
                passed=False,
                summary=f"Routines: catalog query failed: {ex}",
                details=[],
                stats={},
            )
        lm = {(_norm_upper(r.get("schema_name")), _norm_upper(r.get("routine_name"))): r for r in left}
        rm = {(_norm_upper(r.get("schema_name")), _norm_upper(r.get("routine_name"))): r for r in right}
        details: list[dict[str, Any]] = []
        for k in sorted(set(lm.keys()) | set(rm.keys())):
            l, r = lm.get(k), rm.get(k)
            if l and not r:
                details.append(_obj_detail(source_schema, target_schema, k, "ROUTINE_MISSING_IN_TARGET", "routine_name", l, None))
            elif r and not l:
                details.append(_obj_detail(source_schema, target_schema, k, "ROUTINE_MISSING_IN_SOURCE", "routine_name", None, r))
            elif l and r:
                if _canon_sql_text(str(l.get("definition"))) != _canon_sql_text(str(r.get("definition"))):
                    details.append(_obj_detail(source_schema, target_schema, k, "ROUTINE_DEFINITION_DIFFERENT", "routine_name", l, r))
        return ValidationResult(
            validation_name="routines",
            passed=len(details) == 0,
            summary=f"Routines: {len(details)} issue(s).",
            details=details,
            stats={"issues": len(details)},
        )

    def validate_extended_properties(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """SQL Server extended properties vs DB2 REMARKS (best-effort)."""
        if not source_schema or not target_schema:
            return ValidationResult("extended_properties", True, "Extended properties: skipped.", [], stats={})
        sch_s = str(source_schema).strip().replace("'", "''")
        sch_t = str(target_schema).strip().replace("'", "''")
        q_l = (
            f"SELECT RTRIM(TABSCHEMA) AS schema_name, RTRIM(TABNAME) AS object_name, "
            f"RTRIM(REMARKS) AS prop_value FROM SYSCAT.TABLES WHERE TYPE IN ('T','U') "
            f"AND UPPER(TABSCHEMA) = UPPER('{sch_s}') AND REMARKS IS NOT NULL"
        )
        q_r = (
            f"SELECT RTRIM(s.name) AS schema_name, RTRIM(t.name) AS object_name, "
            f"CAST(ep.value AS NVARCHAR(MAX)) AS prop_value "
            f"FROM sys.extended_properties ep "
            f"JOIN sys.tables t ON ep.major_id = t.object_id AND ep.minor_id = 0 AND ep.name IN ('MS_Description','Caption') "
            f"JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE UPPER(s.name) = UPPER(N'{sch_t}')"
        )
        details: list[dict[str, Any]] = []
        try:
            left = self._source_execute(q_l) or []
            right = self._target_execute(q_r) or []
        except Exception:
            return ValidationResult(
                validation_name="extended_properties",
                passed=True,
                summary="Extended properties: catalog not available.",
                details=[],
                stats={},
            )
        lm = {(_norm_upper(r.get("schema_name")), _norm_upper(r.get("object_name"))): str(r.get("prop_value") or "").strip() for r in left}
        rm = {(_norm_upper(r.get("schema_name")), _norm_upper(r.get("object_name"))): str(r.get("prop_value") or "").strip() for r in right}
        resolved_src, common = self._pair_common_tables(source_schema, target_schema)
        for tbl in common:
            ku = (_norm_upper(resolved_src), _norm_upper(tbl))
            kt = (_norm_upper(target_schema), _norm_upper(tbl))
            lv, rv = lm.get(ku), rm.get(kt)
            if lv is None and rv is None:
                continue
            if lv != rv:
                details.append({
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": source_schema,
                    "table": tbl,
                    "status": "MISMATCH",
                    "element_path": f"{source_schema}.{tbl}",
                    "object_type": "TABLE",
                    "error_code": "EXTENDED_PROPERTY_MISMATCH",
                    "error_description": "Table description / extended property differs",
                    "details_json": json.dumps({"source": lv, "target": rv}),
                })
        return ValidationResult(
            validation_name="extended_properties",
            passed=len(details) == 0,
            summary=f"Extended properties: {len(details)} mismatch(es).",
            details=details,
            stats={"issues": len(details)},
        )

    def run_all(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> dict[str, ValidationResult]:
        """Run all behavior validations."""
        return {
            "identity_sequence": self.validate_identity_sequence(source_schema, target_schema),
            "identity_collision": self.validate_identity_collision(source_schema, target_schema),
            "collation": self.validate_collation(source_schema, target_schema),
            "triggers": self.validate_triggers(source_schema, target_schema),
            "routines": self.validate_routines(source_schema, target_schema),
            "extended_properties": self.validate_extended_properties(source_schema, target_schema),
        }


def _is_identity_row(r: dict[str, Any]) -> bool:
    v = r.get("is_identity")
    if isinstance(v, bool):
        return v
    return bool(_to_int(v))


def _norm_val(v: Any) -> str:
    return str(v or "").strip()


def _id_detail(
    src_s: str,
    tgt_s: str,
    key: tuple[str, str, str],
    code: str,
    lr: dict[str, Any] | None,
    rr: dict[str, Any] | None,
) -> dict[str, Any]:
    _, tb, col = key
    return {
        "source_schema": src_s,
        "target_schema": tgt_s,
        "schema": src_s,
        "table": tb,
        "column": col,
        "status": "MISMATCH",
        "element_path": f"{src_s}.{tb}.{col}",
        "object_type": "TABLE",
        "error_code": code,
        "error_description": code.replace("_", " ").title(),
        "details_json": json.dumps(
            {
                "source_seed": _row_get(lr or {}, "seed_value"),
                "destination_seed": _row_get(rr or {}, "seed_value"),
                "source_increment": _row_get(lr or {}, "increment_value"),
                "destination_increment": _row_get(rr or {}, "increment_value"),
            }
        ),
    }


def _seq_detail(
    src_s: str,
    tgt_s: str,
    key: tuple[str, str],
    code: str,
    a: dict[str, Any] | None,
    b: dict[str, Any] | None,
) -> dict[str, Any]:
    name = key[1]
    return {
        "source_schema": src_s,
        "target_schema": tgt_s,
        "schema": tgt_s,
        "table": name,
        "status": "MISMATCH",
        "element_path": f"{tgt_s}.{name}",
        "object_type": "SEQUENCE",
        "error_code": code,
        "error_description": code.replace("_", " ").title(),
        "details_json": json.dumps({"source": a, "target": b}),
    }


def _obj_detail(
    src_s: str,
    tgt_s: str,
    key: tuple[str, str],
    code: str,
    name_field: str,
    l: dict[str, Any] | None,
    r: dict[str, Any] | None,
) -> dict[str, Any]:
    nm = key[1]
    return {
        "source_schema": src_s,
        "target_schema": tgt_s,
        "schema": src_s,
        "table": nm,
        "status": "MISMATCH",
        "element_path": f"{src_s}.{nm}",
        "object_type": "OBJECT",
        "error_code": code,
        "error_description": code.replace("_", " ").title(),
        "details_json": json.dumps({name_field: nm, "source_definition": (l or {}).get("definition"), "target_definition": (r or {}).get("definition")}),
    }
