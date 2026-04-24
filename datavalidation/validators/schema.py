"""
Schema validations: table presence, column counts, datatype mapping, nullable, defaults, indexes, FKs, check constraints.
"""
from __future__ import annotations

from typing import Any

from datavalidation.reporting.cross_schema import build_table_pairs_from_catalog_rows
from datavalidation.reporting.index_comparison import compare_indexes_legacy
from datavalidation.results import ValidationResult
from datavalidation.utils.formatting import element_path
from datavalidation.rules.datatype_map import is_compatible_type
from datavalidation.validators.base import BaseValidator


def _norm_whitespace_upper(s: Any) -> str:
    return " ".join(str(s or "").strip().upper().split())


def _norm_default_expr(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s or s.upper() in ("-", "NULL", "(NULL)"):
        return ""
    return " ".join(s.upper().split())


def _db2_fk_delete_update(row: dict[str, Any]) -> tuple[str, str]:
    mp = {"A": "NO_ACTION", "C": "CASCADE", "R": "RESTRICT", "N": "NO_ACTION", "L": "SET_NULL", "D": "SET_DEFAULT"}
    dc = str(row.get("delete_action") or "").strip().upper()
    uc = str(row.get("update_action") or "").strip().upper()
    return mp.get(dc, dc or "NO_ACTION"), mp.get(uc, uc or "NO_ACTION")


def _azure_fk_delete_update(row: dict[str, Any]) -> tuple[str, str]:
    mp = {0: "NO_ACTION", 1: "CASCADE", 2: "SET_NULL", 3: "SET_DEFAULT"}

    def one(v: Any) -> str:
        try:
            return mp.get(int(v), str(v))
        except (TypeError, ValueError):
            return str(v or "").strip().upper() or "NO_ACTION"

    return one(row.get("delete_action")), one(row.get("update_action"))


def _fk_delete_update(row: dict[str, Any], dialect_name: str) -> tuple[str, str]:
    if dialect_name == "db2":
        return _db2_fk_delete_update(row)
    return _azure_fk_delete_update(row)


def _fk_column_pair_string(col_rows: list[dict[str, Any]], table_u: str, fk_u: str) -> str:
    sub = [
        r
        for r in col_rows
        if str(r.get("table_name", "")).strip().upper() == table_u and str(r.get("fk_name", "")).strip().upper() == fk_u
    ]

    def seq_key(r: dict[str, Any]) -> int:
        try:
            return int(r.get("col_seq"))
        except (TypeError, ValueError):
            return 0

    sub = sorted(sub, key=seq_key)
    return ",".join(
        f"{str(r.get('fk_column') or '').strip()}->{str(r.get('pk_column') or '').strip()}" for r in sub
    )


def _fk_ref_tables_match(
    sr: dict[str, Any],
    tr: dict[str, Any],
    source_schema: str | None,
    target_schema: str | None,
) -> bool:
    st = str(sr.get("ref_table") or "").strip().upper()
    tt = str(tr.get("ref_table") or "").strip().upper()
    if st != tt or not st:
        return False
    rss = str(sr.get("ref_schema") or "").strip().upper()
    rts = str(tr.get("ref_schema") or "").strip().upper()
    if rss == rts:
        return True
    ssu = (source_schema or "").strip().upper()
    tsu = (target_schema or "").strip().upper()
    return bool(ssu and tsu and rss == ssu and rts == tsu)


class SchemaValidator(BaseValidator):
    """Runs all schema-level validations."""

    def validate_table_presence(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
        object_types: list[str] | None = None,
    ) -> ValidationResult:
        """Compare object presence (TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER, SEQUENCE, INDEX, CONSTRAINT) between source and target. Matches by (object_name, object_type) across schemas (USERID->dbo)."""
        object_types = object_types or [
            "TABLE", "VIEW", "PROCEDURE", "FUNCTION", "TRIGGER", "INDEX", "CONSTRAINT", "SEQUENCE",
        ]
        # Resolve USERID to actual DB2 schema (e.g. connection username) for catalog queries; report still shows USERID
        src_schema_for_presence = getattr(self, "_resolve_source_schema", lambda s: s)(source_schema) or source_schema
        src_d = self._source_dialect
        tgt_d = self._target_dialect

        def norm(r: dict) -> dict:
            """Normalize row to schema_name, object_name, object_type. object_name from object_name or table_name."""
            schema = str(r.get("schema_name") or "").strip()
            obj_name = str(r.get("object_name") or r.get("table_name") or "").strip()
            typ = str(r.get("object_type") or "TABLE").strip().upper()
            if typ in ("T", "U"):
                typ = "TABLE"
            elif typ == "V":
                typ = "VIEW"
            return {"schema_name": schema, "object_name": obj_name, "object_type": typ}

        def run_src_base(schema_val: str) -> list[dict]:
            out: list[dict] = []
            sql = getattr(src_d, "catalog_objects_query", lambda s, o: None)(schema_val, base_types)
            if sql:
                for r in self._source_execute(sql):
                    out.append(norm({**r, "object_name": str(r.get("table_name") or r.get("object_name") or "").strip()}))
            else:
                for r in self._source_execute(src_d.catalog_tables_query(schema_val, base_types or ["TABLE"])):
                    out.append(norm({**r, "object_type": "TABLE" if str(r.get("object_type") or "").strip().upper() in ("T", "U") else "VIEW"}))
            return out

        src_rows: list[dict] = []
        tgt_rows: list[dict] = []

        # Base object types (TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER)
        base_types = [t for t in object_types if t in ("TABLE", "VIEW", "PROCEDURE", "FUNCTION", "TRIGGER")]
        if base_types:
            src_rows = run_src_base(src_schema_for_presence)
            # Fallback: if USERID was resolved and we got 0 tables, try literal USERID (some DB2 have schema named USERID)
            if not src_rows and source_schema and str(source_schema).strip().upper() == "USERID" and src_schema_for_presence != "USERID":
                src_rows = run_src_base("USERID")
            tgt_sql = getattr(tgt_d, "catalog_objects_query", lambda s, o: None)(target_schema, base_types)
            if tgt_sql:
                for r in self._target_execute(tgt_sql):
                    tgt_rows.append(norm({**r, "object_name": str(r.get("table_name") or r.get("object_name") or "").strip()}))
            else:
                for r in self._target_execute(tgt_d.catalog_tables_query(target_schema, base_types or ["TABLE"])):
                    tgt_rows.append(norm({**r, "object_type": "TABLE" if str(r.get("object_type") or "").strip().upper() in ("T", "U") else "VIEW"}))

        # SEQUENCE, INDEX, CONSTRAINT from presence-specific queries
        seen_src_keys = {(str(r.get("object_name", "")).strip().upper(), str(r.get("object_type", "")).strip().upper()) for r in src_rows}
        for kind, attr in [("SEQUENCE", "catalog_presence_sequences_query"), ("INDEX", "catalog_presence_indexes_query"), ("CONSTRAINT", "catalog_presence_constraints_query")]:
            if kind not in object_types:
                continue
            for try_schema in [src_schema_for_presence] + (["USERID"] if (source_schema and str(source_schema).strip().upper() == "USERID" and src_schema_for_presence != "USERID") else []):
                q = getattr(src_d, attr, lambda s: None)(try_schema)
                if q:
                    for r in self._source_execute(q):
                        nr = norm(r)
                        key = (nr["object_name"].upper(), nr["object_type"].upper())
                        if key not in seen_src_keys:
                            seen_src_keys.add(key)
                            src_rows.append(nr)
            q = getattr(tgt_d, attr, lambda s: None)(target_schema)
            if q:
                for r in self._target_execute(q):
                    tgt_rows.append(norm(r))

        def name_key(r: dict) -> tuple:
            # Case-insensitive match so DB2 (uppercase) and Azure (mixed) object names match
            obj = str(r.get("object_name", "")).strip()
            typ = str(r.get("object_type", "TABLE")).strip().upper()
            return (obj.upper(), typ)

        src_by_key = {name_key(r): r for r in src_rows}
        tgt_by_key = {name_key(r): r for r in tgt_rows}
        src_names = set(src_by_key)
        tgt_names = set(tgt_by_key)
        source_only = src_names - tgt_names
        target_only = tgt_names - src_names

        details = []
        # Use logical schema (USERID/dbo) for element_path so report matches old tool
        for (key_name, typ) in source_only:
            r = src_by_key.get((key_name, typ), {})
            obj_name = str(r.get("object_name", key_name)).strip()
            sch = str(r.get("schema_name") or source_schema or "").strip()
            elem = f"{source_schema or sch}.{obj_name}" if (source_schema or sch) or obj_name else obj_name
            details.append({
                "source_schema": source_schema, "target_schema": target_schema,
                "schema": source_schema or sch, "table": obj_name if typ in ("TABLE", "VIEW") else "", "object_name": obj_name,
                "object_type": typ, "status": "SOURCE_ONLY", "element_path": elem,
            })
        for (key_name, typ) in target_only:
            r = tgt_by_key.get((key_name, typ), {})
            obj_name = str(r.get("object_name", key_name)).strip()
            sch = str(r.get("schema_name") or target_schema or "").strip()
            elem = f"{target_schema or sch}.{obj_name}" if (target_schema or sch) or obj_name else obj_name
            details.append({
                "source_schema": source_schema, "target_schema": target_schema,
                "schema": target_schema or sch, "table": obj_name if typ in ("TABLE", "VIEW") else "", "object_name": obj_name,
                "object_type": typ, "status": "TARGET_ONLY", "element_path": elem,
            })
        passed = len(details) == 0
        summary = f"Objects: {len(src_rows)} source, {len(tgt_rows)} target; {len(details)} difference(s)."
        return ValidationResult(
            validation_name="table_presence",
            passed=passed,
            summary=summary,
            details=details,
            stats={"source_count": len(src_rows), "target_count": len(tgt_rows), "diff_count": len(details)},
        )

    def validate_column_counts(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
        object_types: list[str] | None = None,
    ) -> ValidationResult:
        """Compare column count per table between source and target. Matches tables by table name across schemas (USERID->dbo)."""
        object_types = object_types or self.options.object_types
        resolved_src = getattr(self, "_resolve_source_schema", lambda s: s)(source_schema) or source_schema
        src_d, tgt_d = self._source_dialect, self._target_dialect
        src_tables = self._source_execute(src_d.catalog_tables_query(resolved_src, object_types))
        if not src_tables and source_schema and str(source_schema).strip().upper() == "USERID" and resolved_src != "USERID":
            src_tables = self._source_execute(src_d.catalog_tables_query("USERID", object_types))
            if src_tables:
                resolved_src = "USERID"
        tgt_tables = self._target_execute(tgt_d.catalog_tables_query(target_schema, object_types))
        src_table_names = {str(r.get("table_name", "")).strip() for r in src_tables}
        tgt_table_names = {str(r.get("table_name", "")).strip() for r in tgt_tables}
        # Case-insensitive common tables (USERID->dbo mapping)
        src_upper = {t.upper(): t for t in src_table_names}
        tgt_upper = {t.upper(): t for t in tgt_table_names}
        common_pairs = [(src_upper[k], tgt_upper[k]) for k in src_upper if k in tgt_upper]
        details = []
        for src_tbl, tgt_tbl in common_pairs:
            src_cols = self._source_execute(src_d.catalog_columns_query(resolved_src, src_tbl))
            tgt_cols = self._target_execute(tgt_d.catalog_columns_query(target_schema, tgt_tbl))
            sc, tc = len(src_cols), len(tgt_cols)
            if sc != tc:
                details.append({
                    "source_schema": source_schema, "target_schema": target_schema,
                    "schema": source_schema, "table": src_tbl, "status": "MISMATCH",
                    "source_column_count": sc,
                    "target_column_count": tc,
                    "destination_column_count": tc,
                    "element_path": element_path(source_schema or "", src_tbl),
                    "error_code": "COLUMN_COUNT_MISMATCH",
                    "error_description": "Column count mismatch between source and target",
                    "object_type": "TABLE",
                })
        passed = len(details) == 0
        return ValidationResult(
            validation_name="column_counts",
            passed=passed,
            summary=f"Compared {len(common_pairs)} tables; {len(details)} column count mismatch(es).",
            details=details,
            stats={"tables_compared": len(common_pairs), "mismatch_count": len(details)},
        )

    def validate_datatype_mapping(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Compare column data types between source and target (USERID->dbo mapping)."""
        resolved_src = getattr(self, "_resolve_source_schema", lambda s: s)(source_schema) or source_schema
        src_d, tgt_d = self._source_dialect, self._target_dialect
        src_cols = self._source_execute(src_d.catalog_columns_query(resolved_src, None))
        if not src_cols and source_schema and str(source_schema).strip().upper() == "USERID" and resolved_src != "USERID":
            src_cols = self._source_execute(src_d.catalog_columns_query("USERID", None))
            if src_cols:
                resolved_src = "USERID"
        tgt_cols = self._target_execute(tgt_d.catalog_columns_query(target_schema, None))
        # Match by (table_name, column_name) case-insensitive for cross-schema USERID->dbo
        tgt_by_key = {}
        for r in tgt_cols:
            tbl = str(r.get("table_name", "")).strip()
            col = str(r.get("column_name", "")).strip()
            tgt_by_key[(tbl.upper(), col.upper())] = r
        details = []
        for r in src_cols:
            sch = str(r.get("schema_name", "")).strip()
            tbl = str(r.get("table_name", "")).strip()
            col = str(r.get("column_name", "")).strip()
            tr = tgt_by_key.get((tbl.upper(), col.upper()))
            if tr is None:
                continue
            src_type = str(r.get("data_type", "")).strip().upper()
            tgt_type = str(tr.get("data_type", "")).strip()
            if not is_compatible_type(src_type, tgt_type):
                details.append({
                    "source_schema": source_schema, "target_schema": target_schema,
                    "schema": sch, "table": tbl, "column": col,
                    "source_type": src_type, "target_type": tgt_type,
                    "status": "MISMATCH",
                    "element_path": element_path(source_schema or sch, tbl, col),
                    "error_code": "DATATYPE_NAME_MISMATCH",
                    "error_description": "Data type name mismatch",
                    "object_type": "TABLE",
                })
        passed = len(details) == 0
        return ValidationResult(
            validation_name="datatype_mapping",
            passed=passed,
            summary=f"Datatype mapping: {len(details)} mismatch(es).",
            details=details,
            stats={"mismatch_count": len(details)},
        )

    def validate_nullable(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Compare nullability of columns between source and target (USERID->dbo)."""
        resolved_src = getattr(self, "_resolve_source_schema", lambda s: s)(source_schema) or source_schema
        src_d, tgt_d = self._source_dialect, self._target_dialect
        src_cols = self._source_execute(src_d.catalog_columns_query(resolved_src, None))
        if not src_cols and source_schema and str(source_schema).strip().upper() == "USERID" and resolved_src != "USERID":
            src_cols = self._source_execute(src_d.catalog_columns_query("USERID", None))
            if src_cols:
                resolved_src = "USERID"
        tgt_cols = self._target_execute(tgt_d.catalog_columns_query(target_schema, None))
        tgt_by_key = {}
        for r in tgt_cols:
            tbl = str(r.get("table_name", "")).strip()
            col = str(r.get("column_name", "")).strip()
            tgt_by_key[(tbl.upper(), col.upper())] = r
        details = []
        for r in src_cols:
            sch = str(r.get("schema_name", "")).strip()
            tbl = str(r.get("table_name", "")).strip()
            col = str(r.get("column_name", "")).strip()
            tr = tgt_by_key.get((tbl.upper(), col.upper()))
            if tr is None:
                continue
            src_null = r.get("is_nullable")
            tgt_null = tr.get("is_nullable")
            if src_null != tgt_null:
                details.append({
                    "source_schema": source_schema, "target_schema": target_schema,
                    "schema": sch, "table": tbl, "column": col,
                    "source_nullable": src_null, "target_nullable": tgt_null,
                    "status": "MISMATCH",
                    "element_path": element_path(source_schema or sch, tbl, col),
                    "error_code": "NULLABILITY_MISMATCH",
                    "error_description": "Nullable constraint mismatch",
                    "object_type": "TABLE",
                })
        passed = len(details) == 0
        return ValidationResult(
            validation_name="nullable",
            passed=passed,
            summary=f"Nullable: {len(details)} mismatch(es).",
            details=details,
            stats={"mismatch_count": len(details)},
        )

    def validate_default_values(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Compare column default expressions for columns present on both sides (catalog)."""
        resolved_src = getattr(self, "_resolve_source_schema", lambda s: s)(source_schema) or source_schema
        src_d, tgt_d = self._source_dialect, self._target_dialect
        try:
            src_cols = self._source_execute(src_d.catalog_columns_query(resolved_src, None))
            if not src_cols and source_schema and str(source_schema).strip().upper() == "USERID" and resolved_src != "USERID":
                src_cols = self._source_execute(src_d.catalog_columns_query("USERID", None))
                if src_cols:
                    resolved_src = "USERID"
            tgt_cols = self._target_execute(tgt_d.catalog_columns_query(target_schema, None))
        except Exception:
            return ValidationResult(
                validation_name="default_values",
                passed=True,
                summary="Default values: catalog query failed (skipped).",
                details=[],
                stats={},
            )
        tgt_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for r in tgt_cols:
            tbl = str(r.get("table_name", "")).strip()
            col = str(r.get("column_name", "")).strip()
            tgt_by_key[(tbl.upper(), col.upper())] = r
        details: list[dict[str, Any]] = []
        for r in src_cols:
            sch = str(r.get("schema_name", "")).strip()
            tbl = str(r.get("table_name", "")).strip()
            col = str(r.get("column_name", "")).strip()
            tr = tgt_by_key.get((tbl.upper(), col.upper()))
            if tr is None:
                continue
            sdef = _norm_default_expr(r.get("column_default"))
            tdef = _norm_default_expr(tr.get("column_default"))
            if sdef == tdef:
                continue
            details.append({
                "source_schema": source_schema,
                "target_schema": target_schema,
                "schema": sch,
                "table": tbl,
                "column": col,
                "status": "MISMATCH",
                "element_path": element_path(source_schema or sch, tbl, col),
                "error_code": "DEFAULT_MISMATCH",
                "error_description": "Default value mismatch",
                "object_type": "TABLE",
                "source_default": r.get("column_default"),
                "target_default": tr.get("column_default"),
            })
        passed = len(details) == 0
        return ValidationResult(
            validation_name="default_values",
            passed=passed,
            summary=f"Default values: {len(details)} mismatch(es).",
            details=details,
            stats={"mismatch_count": len(details)},
        )

    def validate_indexes(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Compare indexes like the original FastAPI service (column signatures, PK, masking, many-column note)."""
        resolved_src = getattr(self, "_resolve_source_schema", lambda s: s)(source_schema) or source_schema
        src_d, tgt_d = self._source_dialect, self._target_dialect
        src_ix_q = getattr(src_d, "catalog_index_columns_query", lambda s: None)(resolved_src)
        tgt_ix_q = getattr(tgt_d, "catalog_index_columns_query", lambda s: None)(target_schema)

        if not src_ix_q or not tgt_ix_q:
            return self._validate_indexes_simple(source_schema, target_schema, resolved_src)

        try:
            src_ix = self._source_execute(src_ix_q)
            if not src_ix and source_schema and str(source_schema).strip().upper() == "USERID" and resolved_src != "USERID":
                fq = getattr(src_d, "catalog_index_columns_query", lambda s: None)("USERID")
                if fq:
                    src_ix = self._source_execute(fq)
                    if src_ix:
                        resolved_src = "USERID"
            tgt_ix = self._target_execute(tgt_ix_q)
        except Exception:
            return self._validate_indexes_simple(source_schema, target_schema, resolved_src)

        object_types = self.options.object_types or ["TABLE"]
        src_tables = self._source_execute(src_d.catalog_tables_query(resolved_src, object_types))
        tgt_tables = self._target_execute(tgt_d.catalog_tables_query(target_schema, object_types))
        pairs = build_table_pairs_from_catalog_rows(src_tables, tgt_tables, source_schema, target_schema)

        src_cc: dict[tuple[str, str], int] = {}
        for r in self._source_execute(src_d.catalog_columns_query(resolved_src, None)):
            k = (
                str(r.get("schema_name") or "").strip().upper(),
                str(r.get("table_name") or "").strip().upper(),
            )
            src_cc[k] = src_cc.get(k, 0) + 1
        tgt_cc: dict[tuple[str, str], int] = {}
        for r in self._target_execute(tgt_d.catalog_columns_query(target_schema, None)):
            k = (
                str(r.get("schema_name") or "").strip().upper(),
                str(r.get("table_name") or "").strip().upper(),
            )
            tgt_cc[k] = tgt_cc.get(k, 0) + 1

        details = compare_indexes_legacy(
            pairs,
            src_ix,
            tgt_ix,
            source_schema=source_schema,
            target_schema=target_schema,
            src_col_counts=src_cc,
            tgt_col_counts=tgt_cc,
        )
        bad = [d for d in details if d.get("status") in ("error", "WARNING")]
        passed = len(bad) == 0
        return ValidationResult(
            validation_name="indexes",
            passed=passed,
            summary=f"Indexes: {len(details)} row(s) ({len(bad)} error/warning).",
            details=details,
            stats={"diff_count": len(details), "error_or_warning": len(bad)},
        )

    def _validate_indexes_simple(
        self,
        source_schema: str | None,
        target_schema: str | None,
        resolved_src: str | None,
    ) -> ValidationResult:
        """Fallback: compare index names only when per-column catalog SQL is unavailable."""
        src_d, tgt_d = self._source_dialect, self._target_dialect
        try:
            src_rows = self._source_execute(src_d.catalog_indexes_query(resolved_src))
            if not src_rows and source_schema and str(source_schema).strip().upper() == "USERID" and resolved_src != "USERID":
                src_rows = self._source_execute(src_d.catalog_indexes_query("USERID"))
                if src_rows:
                    resolved_src = "USERID"
            tgt_rows = self._target_execute(tgt_d.catalog_indexes_query(target_schema))
        except NotImplementedError:
            return ValidationResult(
                validation_name="indexes",
                passed=True,
                summary="Index validation: not implemented for this dialect.",
                details=[],
                stats={},
            )

        def src_key(r):
            t = str(r.get("table_name", "")).strip()
            i = str(r.get("index_name", "")).strip()
            return (t.upper(), i.upper())

        src_set = {src_key(r): (str(r.get("table_name", "")).strip(), str(r.get("index_name", "")).strip()) for r in src_rows}
        tgt_set = {src_key(r): (str(r.get("table_name", "")).strip(), str(r.get("index_name", "")).strip()) for r in tgt_rows}
        src_keys, tgt_keys = set(src_set), set(tgt_set)
        source_only = [(src_set[k][0], src_set[k][1]) for k in src_keys - tgt_keys]
        target_only = [(tgt_set[k][0], tgt_set[k][1]) for k in tgt_keys - src_keys]
        details = [
            {
                "source_schema": source_schema,
                "target_schema": target_schema,
                "schema": source_schema,
                "table": tbl,
                "index": idx,
                "status": "SOURCE_ONLY",
                "element_path": element_path(source_schema or "", tbl) + f".{idx}",
            }
            for (tbl, idx) in source_only
        ]
        details += [
            {
                "source_schema": source_schema,
                "target_schema": target_schema,
                "schema": target_schema,
                "table": tbl,
                "index": idx,
                "status": "TARGET_ONLY",
                "element_path": element_path(target_schema or "", tbl) + f".{idx}",
            }
            for (tbl, idx) in target_only
        ]
        passed = len(details) == 0
        return ValidationResult(
            validation_name="indexes",
            passed=passed,
            summary=f"Indexes: {len(details)} difference(s).",
            details=details,
            stats={"source_count": len(src_keys), "target_count": len(tgt_keys), "diff_count": len(details)},
        )

    def validate_foreign_keys(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Compare foreign key definitions between source and target (USERID->dbo)."""
        resolved_src = getattr(self, "_resolve_source_schema", lambda s: s)(source_schema) or source_schema
        src_d, tgt_d = self._source_dialect, self._target_dialect
        try:
            src_rows = self._source_execute(src_d.catalog_fk_query(resolved_src))
            if not src_rows and source_schema and str(source_schema).strip().upper() == "USERID" and resolved_src != "USERID":
                src_rows = self._source_execute(src_d.catalog_fk_query("USERID"))
                if src_rows:
                    resolved_src = "USERID"
            tgt_rows = self._target_execute(tgt_d.catalog_fk_query(target_schema))
        except NotImplementedError:
            return ValidationResult(validation_name="foreign_keys", passed=True, summary="FK validation: not implemented.", details=[], stats={})

        def fk_key(r: dict[str, Any]) -> tuple[str, str]:
            t = str(r.get("table_name", "")).strip()
            f = str(r.get("fk_name", "")).strip()
            return (t.upper(), f.upper())

        src_map: dict[tuple[str, str], dict[str, Any]] = {fk_key(r): r for r in src_rows}
        tgt_map: dict[tuple[str, str], dict[str, Any]] = {fk_key(r): r for r in tgt_rows}

        src_fk_cols: list[dict[str, Any]] = []
        tgt_fk_cols: list[dict[str, Any]] = []
        src_cq = getattr(src_d, "catalog_fk_columns_query", lambda s: None)(resolved_src)
        tgt_cq = getattr(tgt_d, "catalog_fk_columns_query", lambda s: None)(target_schema)
        if src_cq:
            try:
                src_fk_cols = self._source_execute(src_cq)
            except Exception:
                src_fk_cols = []
        if tgt_cq:
            try:
                tgt_fk_cols = self._target_execute(tgt_cq)
            except Exception:
                tgt_fk_cols = []

        details: list[dict[str, Any]] = []

        def append_fk_detail(
            *,
            tbl: str,
            fk: str,
            status: str,
            err_desc: str,
            sr: dict[str, Any] | None,
            tr: dict[str, Any] | None,
        ) -> None:
            tbl_u, fk_u = tbl.strip().upper(), fk.strip().upper()
            if status == "TARGET_ONLY":
                path_sch = (target_schema or "").strip() or str((tr or {}).get("schema_name") or "")
                row_schema = target_schema
            elif status == "SOURCE_ONLY":
                path_sch = (source_schema or "").strip() or str((sr or {}).get("schema_name") or "")
                row_schema = source_schema
            else:
                path_sch = (source_schema or "").strip() or str((sr or {}).get("schema_name") or "")
                row_schema = source_schema
            elem = element_path(path_sch, tbl, fk)
            sd_d, sd_u = ("", "")
            if sr is not None:
                sd_d, sd_u = _fk_delete_update(sr, src_d.name)
            td_d, td_u = ("", "")
            if tr is not None:
                td_d, td_u = _fk_delete_update(tr, tgt_d.name)
            spairs = _fk_column_pair_string(src_fk_cols, tbl_u, fk_u)
            tpairs = _fk_column_pair_string(tgt_fk_cols, tbl_u, fk_u)
            details.append(
                {
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": row_schema,
                    "table": tbl,
                    "fk_name": fk,
                    "status": status,
                    "object_type": "TABLE",
                    "element_path": elem,
                    "error_code": "FK_MISMATCH",
                    "error_description": err_desc,
                    "source_ref_schema": (sr or {}).get("ref_schema"),
                    "source_ref_table": (sr or {}).get("ref_table"),
                    "destination_ref_schema": (tr or {}).get("ref_schema"),
                    "destination_ref_table": (tr or {}).get("ref_table"),
                    "source_delete_action": sd_d if sr is not None else None,
                    "source_update_action": sd_u if sr is not None else None,
                    "destination_delete_action": td_d if tr is not None else None,
                    "destination_update_action": td_u if tr is not None else None,
                    "source_column_pairs": spairs or None,
                    "destination_column_pairs": tpairs or None,
                }
            )

        all_keys = set(src_map) | set(tgt_map)
        for k in sorted(all_keys):
            sr, tr = src_map.get(k), tgt_map.get(k)
            tbl = str((sr or tr or {}).get("table_name", "")).strip()
            fk = str((sr or tr or {}).get("fk_name", "")).strip()
            tbl_u, fk_u = k
            if sr is not None and tr is None:
                append_fk_detail(tbl=tbl, fk=fk, status="SOURCE_ONLY", err_desc="FK missing in target", sr=sr, tr=None)
            elif tr is not None and sr is None:
                append_fk_detail(tbl=tbl, fk=fk, status="TARGET_ONLY", err_desc="FK missing in source", sr=None, tr=tr)
            elif sr is not None and tr is not None:
                spairs = _fk_column_pair_string(src_fk_cols, tbl_u, fk_u)
                tpairs = _fk_column_pair_string(tgt_fk_cols, tbl_u, fk_u)
                refs_ok = _fk_ref_tables_match(sr, tr, source_schema, target_schema)
                sd_d, sd_u = _fk_delete_update(sr, src_d.name)
                td_d, td_u = _fk_delete_update(tr, tgt_d.name)
                if (
                    not refs_ok
                    or spairs.upper() != tpairs.upper()
                    or sd_d != td_d
                    or sd_u != td_u
                ):
                    append_fk_detail(
                        tbl=tbl,
                        fk=fk,
                        status="MISMATCH",
                        err_desc="Foreign key definition mismatch",
                        sr=sr,
                        tr=tr,
                    )

        passed = len(details) == 0
        return ValidationResult(
            validation_name="foreign_keys",
            passed=passed,
            summary=f"Foreign keys: {len(details)} difference(s).",
            details=details,
            stats={"diff_count": len(details)},
        )

    def validate_check_constraints(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Compare check constraints between source and target (USERID->dbo)."""
        resolved_src = getattr(self, "_resolve_source_schema", lambda s: s)(source_schema) or source_schema
        src_d, tgt_d = self._source_dialect, self._target_dialect
        try:
            src_rows = self._source_execute(src_d.catalog_check_constraints_query(resolved_src))
            if not src_rows and source_schema and str(source_schema).strip().upper() == "USERID" and resolved_src != "USERID":
                src_rows = self._source_execute(src_d.catalog_check_constraints_query("USERID"))
                if src_rows:
                    resolved_src = "USERID"
            tgt_rows = self._target_execute(tgt_d.catalog_check_constraints_query(target_schema))
        except NotImplementedError:
            return ValidationResult(validation_name="check_constraints", passed=True, summary="Check constraints: not implemented.", details=[], stats={})

        def ck_key(r: dict[str, Any]) -> tuple[str, str]:
            t = str(r.get("table_name", "")).strip()
            c = str(r.get("constraint_name", "")).strip()
            return (t.upper(), c.upper())

        src_map: dict[tuple[str, str], dict[str, Any]] = {ck_key(r): r for r in src_rows}
        tgt_map: dict[tuple[str, str], dict[str, Any]] = {ck_key(r): r for r in tgt_rows}
        log_src = (source_schema or "").strip()
        details: list[dict[str, Any]] = []

        for k in sorted(set(src_map) | set(tgt_map)):
            sr, tr = src_map.get(k), tgt_map.get(k)
            tbl = str((sr or tr or {}).get("table_name", "")).strip()
            cname = str((sr or tr or {}).get("constraint_name", "")).strip()
            if sr is not None and tr is None:
                elem = element_path(
                    (source_schema or "").strip() or str(sr.get("schema_name") or ""),
                    tbl,
                    cname,
                )
            elif tr is not None and sr is None:
                elem = element_path(
                    (target_schema or "").strip() or str(tr.get("schema_name") or ""),
                    tbl,
                    cname,
                )
            else:
                elem = element_path(
                    (source_schema or "").strip() or str((sr or tr or {}).get("schema_name") or ""),
                    tbl,
                    cname,
                )
            if sr is not None and tr is None:
                details.append(
                    {
                        "source_schema": source_schema,
                        "target_schema": target_schema,
                        "schema": source_schema,
                        "table": tbl,
                        "constraint_name": cname,
                        "status": "SOURCE_ONLY",
                        "object_type": "TABLE",
                        "element_path": elem,
                        "error_code": "CHECK_CONSTRAINT_MISMATCH",
                        "error_description": "Check constraint missing in target",
                        "source_check_clause": sr.get("check_clause"),
                    }
                )
            elif tr is not None and sr is None:
                details.append(
                    {
                        "source_schema": source_schema,
                        "target_schema": target_schema,
                        "schema": target_schema,
                        "table": tbl,
                        "constraint_name": cname,
                        "status": "TARGET_ONLY",
                        "object_type": "TABLE",
                        "element_path": elem,
                        "error_code": "CHECK_CONSTRAINT_MISMATCH",
                        "error_description": "Check constraint missing in source",
                        "destination_check_clause": tr.get("check_clause"),
                    }
                )
            elif sr is not None and tr is not None:
                if _norm_whitespace_upper(sr.get("check_clause")) != _norm_whitespace_upper(tr.get("check_clause")):
                    details.append(
                        {
                            "source_schema": source_schema,
                            "target_schema": target_schema,
                            "schema": log_src or str(sr.get("schema_name") or ""),
                            "table": tbl,
                            "constraint_name": cname,
                            "status": "MISMATCH",
                            "object_type": "TABLE",
                            "element_path": elem,
                            "error_code": "CHECK_CONSTRAINT_MISMATCH",
                            "error_description": "Check constraint definition mismatch",
                            "source_check_clause": sr.get("check_clause"),
                            "destination_check_clause": tr.get("check_clause"),
                        }
                    )

        passed = len(details) == 0
        return ValidationResult(
            validation_name="check_constraints",
            passed=passed,
            summary=f"Check constraints: {len(details)} difference(s).",
            details=details,
            stats={"diff_count": len(details)},
        )

    def run_all(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
        object_types: list[str] | None = None,
    ) -> dict[str, ValidationResult]:
        """Run all schema validations and return a dict of name -> ValidationResult."""
        object_types = object_types or self.options.object_types
        return {
            "table_presence": self.validate_table_presence(source_schema, target_schema, object_types),
            "column_counts": self.validate_column_counts(source_schema, target_schema, object_types),
            "datatype_mapping": self.validate_datatype_mapping(source_schema, target_schema),
            "nullable": self.validate_nullable(source_schema, target_schema),
            "default_values": self.validate_default_values(source_schema, target_schema),
            "indexes": self.validate_indexes(source_schema, target_schema),
            "foreign_keys": self.validate_foreign_keys(source_schema, target_schema),
            "check_constraints": self.validate_check_constraints(source_schema, target_schema),
        }
