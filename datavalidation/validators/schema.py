"""
Schema validations: table presence, column counts, datatype mapping, nullable, defaults, indexes, FKs, check constraints.
"""
from __future__ import annotations

import re
from collections import defaultdict
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
    # R (RESTRICT) → NO_ACTION: Azure SQL has no RESTRICT; migration tools convert it to NO_ACTION.
    mp = {"A": "NO_ACTION", "C": "CASCADE", "R": "NO_ACTION", "N": "NO_ACTION", "L": "SET_NULL", "D": "SET_DEFAULT"}
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


def _catalog_object_type_label(raw: Any) -> str:
    """Normalize SYSIBM.SYSTABLES / sys.objects type codes (or names) to TABLE or VIEW for reporting."""
    u = str(raw if raw is not None else "").strip().upper()
    if u in ("T", "U", "TABLE"):
        return "TABLE"
    if u in ("V", "VIEW"):
        return "VIEW"
    return u or "TABLE"


def _normalize_fk_pairs_key(pairs: str) -> str:
    """Normalize FK fk->pk column pair list for cross-dialect signature matching."""
    p = " ".join(str(pairs or "").strip().upper().split())
    return re.sub(r"\s*->\s*", "->", p)


def _fk_row_constraint_name_u(r: dict[str, Any]) -> str:
    """FK / constraint name from a catalog row (DB2 may expose ``constname``; JDBC keys vary)."""
    v = r.get("fk_name") or r.get("constname")
    if v is None or (isinstance(v, str) and not str(v).strip()):
        v = r.get("FK_NAME") or r.get("CONSTNAME")
    return str(v or "").strip().upper()


def _fk_col_rows_for_fk(col_rows: list[dict[str, Any]], table_u: str, fk_u: str) -> list[dict[str, Any]]:
    """FK column catalog rows for ``(table_u, fk_u)``. Strict match first; then match by constraint name only when unambiguous."""
    fk_u = fk_u.strip().upper()
    tu = table_u.strip().upper()
    strict = [
        r
        for r in col_rows
        if str(r.get("table_name", "")).strip().upper() == tu and _fk_row_constraint_name_u(r) == fk_u
    ]
    if strict:
        return strict
    by_name = [r for r in col_rows if _fk_row_constraint_name_u(r) == fk_u]
    if not by_name:
        return []
    distinct_tables = {str(r.get("table_name", "")).strip().upper() for r in by_name if str(r.get("table_name", "")).strip()}
    if len(distinct_tables) <= 1:
        return by_name
    if tu in distinct_tables:
        return [r for r in by_name if str(r.get("table_name", "")).strip().upper() == tu]
    return []


def _fk_column_pair_string(col_rows: list[dict[str, Any]], table_u: str, fk_u: str) -> str:
    fk_u = fk_u.strip().upper()
    sub = _fk_col_rows_for_fk(col_rows, table_u, fk_u)

    def seq_key(r: dict[str, Any]) -> int:
        raw = r.get("col_seq")
        if raw is None:
            raw = r.get("COL_SEQ") or r.get("constraint_column_id")
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    sub = sorted(sub, key=seq_key)
    return ",".join(
        f"{str(r.get('fk_column') or '').strip()}->{str(r.get('pk_column') or '').strip()}" for r in sub
    )


def _fk_ordered_fk_columns_key_only(col_rows: list[dict[str, Any]], table_u: str, fk_u: str) -> str:
    """Child-side FK column names in key order (excludes referenced PK names) for cross-dialect orphan pairing."""
    fk_u = fk_u.strip().upper()
    sub = _fk_col_rows_for_fk(col_rows, table_u, fk_u)

    def seq_key(r: dict[str, Any]) -> int:
        raw = r.get("col_seq")
        if raw is None:
            raw = r.get("COL_SEQ") or r.get("constraint_column_id")
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    sub = sorted(sub, key=seq_key)
    return ",".join(_norm_whitespace_upper(r.get("fk_column")) for r in sub if str(r.get("fk_column") or "").strip())


def _fk_loose_bucket_key(
    r: dict[str, Any],
    fk_col_rows: list[dict[str, Any]],
) -> tuple[str, str, frozenset[str]]:
    """Match orphans when full fk->pk string differs but child table, ref table, and FK column set agree."""
    tbl_u = str(r.get("table_name", "")).strip().upper()
    ref_t = str(r.get("ref_table", "")).strip().upper()
    fk_u = _fk_row_constraint_name_u(r)
    names: list[str] = []
    for row in _fk_col_rows_for_fk(fk_col_rows, tbl_u, fk_u):
        fc = str(row.get("fk_column", "") or "").strip().upper()
        if fc:
            names.append(fc)
    return (tbl_u, ref_t, frozenset(names))


def _fk_ref_tables_match(
    sr: dict[str, Any],
    tr: dict[str, Any],
    source_schema: str | None,
    target_schema: str | None,
    resolved_source_schema: str | None = None,
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
    rsu = (resolved_source_schema or "").strip().upper()
    tsu = (target_schema or "").strip().upper()
    if not tsu:
        return False
    src_ok = rss == ssu or (bool(rsu) and rss == rsu)
    tgt_ok = rts == tsu
    return bool(src_ok and tgt_ok)


def _fk_signature(tbl_u: str, fk_u: str, fk_col_rows: list[dict[str, Any]], row: dict[str, Any]) -> tuple[str, str, str]:
    """Logical FK identity for pairing: child table, referenced table, ordered child FK columns (PK names excluded).

    DB2 and Azure often use different referenced-column names in catalogs for the same FK; matching on ``fk->pk``
    strings alone produces false orphan SOURCE_ONLY / TARGET_ONLY pairs.
    """
    ref_t = str(row.get("ref_table") or "").strip().upper()
    fk_only = _fk_ordered_fk_columns_key_only(fk_col_rows, tbl_u, fk_u)
    return (tbl_u, ref_t, _normalize_fk_pairs_key(fk_only))


def _fk_common_prefix_len(a: str, b: str) -> int:
    u, v = str(a or "").strip().upper(), str(b or "").strip().upper()
    n = 0
    for c1, c2 in zip(u, v):
        if c1 != c2:
            break
        n += 1
    return n


def _fk_orphan_pair_sort_key(
    sr: dict[str, Any],
    tr: dict[str, Any],
    tbl_u: str,
    *,
    src_fk_cols: list[dict[str, Any]],
    tgt_fk_cols: list[dict[str, Any]],
    source_schema: str | None,
    target_schema: str | None,
    resolved_src: str | None,
    src_dialect: str,
    tgt_dialect: str,
) -> tuple[int, int, int]:
    """Higher tuple is better for greedy FK orphan pairing (same child/ref/column signature, unequal counts)."""
    su, tu = _fk_row_constraint_name_u(sr), _fk_row_constraint_name_u(tr)
    spairs = _fk_column_pair_string(src_fk_cols, tbl_u, su)
    tpairs = _fk_column_pair_string(tgt_fk_cols, tbl_u, tu)
    refs_ok = _fk_ref_tables_match(sr, tr, source_schema, target_schema, resolved_src)
    pairs_match = _normalize_fk_pairs_key(spairs) == _normalize_fk_pairs_key(tpairs)
    sd_d, sd_u = _fk_delete_update(sr, src_dialect)
    td_d, td_u = _fk_delete_update(tr, tgt_dialect)
    actions_match = sd_d == td_d and sd_u == td_u
    exact_name = 1 if su == tu else 0
    score = 8 * int(refs_ok) + 4 * int(pairs_match) + 2 * int(actions_match)
    cp = _fk_common_prefix_len(su, tu)
    return (exact_name, score, cp)


def _fk_greedy_orphan_pairs(
    ls: list[dict[str, Any]],
    rs: list[dict[str, Any]],
    tbl_u: str,
    *,
    src_fk_cols: list[dict[str, Any]],
    tgt_fk_cols: list[dict[str, Any]],
    source_schema: str | None,
    target_schema: str | None,
    resolved_src: str | None,
    src_dialect: str,
    tgt_dialect: str,
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Pair orphan FK headers when bucket sizes differ: prefer exact constraint name, then ref/column/action agreement."""
    if not ls or not rs:
        return []
    ls_sorted = sorted(ls, key=_fk_row_constraint_name_u)
    rs_sorted = sorted(rs, key=_fk_row_constraint_name_u)
    if len(ls_sorted) == len(rs_sorted):
        return list(zip(ls_sorted, rs_sorted))
    edges: list[tuple[tuple[int, int, int], int, int]] = []
    for i, sr in enumerate(ls_sorted):
        for j, tr in enumerate(rs_sorted):
            k = _fk_orphan_pair_sort_key(
                sr,
                tr,
                tbl_u,
                src_fk_cols=src_fk_cols,
                tgt_fk_cols=tgt_fk_cols,
                source_schema=source_schema,
                target_schema=target_schema,
                resolved_src=resolved_src,
                src_dialect=src_dialect,
                tgt_dialect=tgt_dialect,
            )
            edges.append((k, i, j))
    edges.sort(key=lambda t: (-t[0][0], -t[0][1], -t[0][2], t[1], t[2]))
    used_i: set[int] = set()
    used_j: set[int] = set()
    out: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for _k, i, j in edges:
        if i in used_i or j in used_j:
            continue
        out.append((ls_sorted[i], rs_sorted[j]))
        used_i.add(i)
        used_j.add(j)
    return out


def _fk_mismatch_description(
    *,
    refs_ok: bool,
    ref_table_match: bool,
    pairs_match: bool,
    actions_match: bool,
) -> str:
    if not ref_table_match or not refs_ok:
        return "Referenced table mismatch"
    if not pairs_match:
        return "Foreign key column mapping mismatch"
    if not actions_match:
        return "Foreign key delete/update rule mismatch"
    return "Foreign key definition mismatch"


class SchemaValidator(BaseValidator):
    """Runs all schema-level validations."""

    def _table_kind_map(self, resolved_schema: str | None, *, source: bool) -> dict[str, str]:
        """Uppercase table/view name -> TABLE or VIEW (queries both kinds for accurate labels)."""
        dialect = self._source_dialect if source else self._target_dialect
        execute = self._source_execute if source else self._target_execute
        out: dict[str, str] = {}
        try:
            rows = execute(dialect.catalog_tables_query(resolved_schema, ["TABLE", "VIEW"])) or []
        except Exception:
            return out
        for r in rows:
            t = str(r.get("table_name", "")).strip().upper()
            if t:
                out[t] = _catalog_object_type_label(r.get("object_type"))
        return out

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
            typ = _catalog_object_type_label(r.get("object_type"))
            return {"schema_name": schema, "object_name": obj_name, "object_type": typ}

        def run_src_base(schema_val: str) -> list[dict]:
            out: list[dict] = []
            sql = getattr(src_d, "catalog_objects_query", lambda s, o: None)(schema_val, base_types)
            if sql:
                for r in self._source_execute(sql):
                    out.append(norm({**r, "object_name": str(r.get("table_name") or r.get("object_name") or "").strip()}))
            else:
                for r in self._source_execute(src_d.catalog_tables_query(schema_val, base_types or ["TABLE"])):
                    out.append(norm({**r, "object_type": _catalog_object_type_label(r.get("object_type"))}))
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
                    tgt_rows.append(norm({**r, "object_type": _catalog_object_type_label(r.get("object_type"))}))

        # SEQUENCE, INDEX, CONSTRAINT from presence-specific queries
        seen_src_keys = {(str(r.get("object_name", "")).strip().upper(), str(r.get("object_type", "")).strip().upper()) for r in src_rows}
        identity_seq_parent: dict[str, str] = {}  # seq_name_upper -> parent_table_upper
        for kind, attr in [("SEQUENCE", "catalog_presence_sequences_query"), ("INDEX", "catalog_presence_indexes_query"), ("CONSTRAINT", "catalog_presence_constraints_query")]:
            if kind not in object_types:
                continue
            for try_schema in [src_schema_for_presence] + (["USERID"] if (source_schema and str(source_schema).strip().upper() == "USERID" and src_schema_for_presence != "USERID") else []):
                q = getattr(src_d, attr, lambda s: None)(try_schema)
                if q:
                    for r in self._source_execute(q):
                        if kind == "SEQUENCE" and str(r.get("seq_type") or "").strip().upper() == "I":
                            pt = str(r.get("parent_table") or "").strip().upper()
                            if pt:
                                identity_seq_parent[str(r.get("object_name") or r.get("table_name") or "").strip().upper()] = pt
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
            obj = str(r.get("object_name", "")).strip()
            typ = str(r.get("object_type", "TABLE")).strip().upper()
            return (obj.upper(), typ)

        # Build target table set for identity sequence filtering
        tgt_table_names = {str(r.get("object_name", "")).strip().upper()
                          for r in tgt_rows
                          if str(r.get("object_type", "")).strip().upper() == "TABLE"}

        # Filter out identity sequences whose parent table exists on target (migrated as IDENTITY column)
        if identity_seq_parent:
            src_rows = [
                r for r in src_rows
                if not (str(r.get("object_type", "")).strip().upper() == "SEQUENCE"
                        and r.get("object_name", "").strip().upper() in identity_seq_parent
                        and identity_seq_parent[r.get("object_name", "").strip().upper()] in tgt_table_names)
            ]

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
        """Compare column count per table/view between source and target. Matches names across schemas (USERID->dbo)."""
        object_types = object_types or self.options.object_types
        resolved_src = getattr(self, "_resolve_source_schema", lambda s: s)(source_schema) or source_schema
        src_d, tgt_d = self._source_dialect, self._target_dialect
        src_tables = self._source_execute(src_d.catalog_tables_query(resolved_src, object_types))
        if not src_tables and source_schema and str(source_schema).strip().upper() == "USERID" and resolved_src != "USERID":
            src_tables = self._source_execute(src_d.catalog_tables_query("USERID", object_types))
            if src_tables:
                resolved_src = "USERID"
        tgt_tables = self._target_execute(tgt_d.catalog_tables_query(target_schema, object_types))
        src_table_kind = {
            str(r.get("table_name", "")).strip().upper(): _catalog_object_type_label(r.get("object_type"))
            for r in src_tables
            if str(r.get("table_name", "")).strip()
        }
        tgt_table_kind = {
            str(r.get("table_name", "")).strip().upper(): _catalog_object_type_label(r.get("object_type"))
            for r in tgt_tables
            if str(r.get("table_name", "")).strip()
        }
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
                sk = src_table_kind.get(src_tbl.strip().upper(), "TABLE")
                tk = tgt_table_kind.get(tgt_tbl.strip().upper(), "TABLE")
                ot = "VIEW" if (sk == "VIEW" or tk == "VIEW") else "TABLE"
                details.append({
                    "source_schema": source_schema, "target_schema": target_schema,
                    "schema": source_schema, "table": src_tbl, "status": "MISMATCH",
                    "source_column_count": sc,
                    "target_column_count": tc,
                    "destination_column_count": tc,
                    "element_path": element_path(source_schema or "", src_tbl),
                    "error_code": "COLUMN_COUNT_MISMATCH",
                    "error_description": "Column count mismatch between source and target",
                    "object_type": ot,
                })
        passed = len(details) == 0
        return ValidationResult(
            validation_name="column_counts",
            passed=passed,
            summary=f"Compared {len(common_pairs)} table(s)/view(s); {len(details)} column count mismatch(es).",
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
        src_kind = self._table_kind_map(resolved_src, source=True)
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
                ot = src_kind.get(tbl.upper(), "TABLE")
                details.append({
                    "source_schema": source_schema, "target_schema": target_schema,
                    "schema": sch, "table": tbl, "column": col,
                    "source_type": src_type, "target_type": tgt_type,
                    "status": "MISMATCH",
                    "element_path": element_path(source_schema or sch, tbl, col),
                    "error_code": "DATATYPE_NAME_MISMATCH",
                    "error_description": "Data type name mismatch",
                    "object_type": ot,
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
        src_kind = self._table_kind_map(resolved_src, source=True)
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
                ot = src_kind.get(tbl.upper(), "TABLE")
                details.append({
                    "source_schema": source_schema, "target_schema": target_schema,
                    "schema": sch, "table": tbl, "column": col,
                    "source_nullable": src_null, "target_nullable": tgt_null,
                    "status": "MISMATCH",
                    "element_path": element_path(source_schema or sch, tbl, col),
                    "error_code": "NULLABILITY_MISMATCH",
                    "error_description": "Nullable constraint mismatch",
                    "object_type": ot,
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
        src_kind = self._table_kind_map(resolved_src, source=True)
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
            ot = src_kind.get(tbl.upper(), "TABLE")
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
                "object_type": ot,
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
        src_table_kind = self._table_kind_map(resolved_src, source=True)

        def _build_col_counts(execute_fn, dialect, schema):
            cc: dict[tuple[str, str], int] = {}
            try:
                for r in execute_fn(dialect.catalog_columns_query(schema, None)):
                    k = (
                        str(r.get("schema_name") or "").strip().upper(),
                        str(r.get("table_name") or "").strip().upper(),
                    )
                    cc[k] = cc.get(k, 0) + 1
            except Exception:
                pass
            if not cc and schema:
                alt = getattr(dialect, "catalog_columns_query_by_creator", None)
                if alt:
                    try:
                        for r in execute_fn(alt(schema)):
                            k = (
                                str(r.get("schema_name") or "").strip().upper(),
                                str(r.get("table_name") or "").strip().upper(),
                            )
                            cc[k] = cc.get(k, 0) + 1
                    except Exception:
                        pass
            return cc

        src_cc = _build_col_counts(self._source_execute, src_d, resolved_src)
        if not src_cc and source_schema and resolved_src != source_schema:
            src_cc = _build_col_counts(self._source_execute, src_d, source_schema)
        tgt_cc = _build_col_counts(self._target_execute, tgt_d, target_schema)

        details = compare_indexes_legacy(
            pairs,
            src_ix,
            tgt_ix,
            source_schema=source_schema,
            target_schema=target_schema,
            src_col_counts=src_cc,
            tgt_col_counts=tgt_cc,
            source_table_kind=src_table_kind,
        )
        bad = [d for d in details if str(d.get("status") or "").strip().upper() in ("ERROR", "WARNING")]
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
            f = _fk_row_constraint_name_u(r)
            return (t.upper(), f)

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

        src_kind = self._table_kind_map(resolved_src, source=True)
        tgt_kind = self._table_kind_map(target_schema, source=False)

        details: list[dict[str, Any]] = []

        def append_fk_detail(
            *,
            tbl: str,
            fk: str,
            status: str,
            err_desc: str,
            sr: dict[str, Any] | None,
            tr: dict[str, Any] | None,
            element_fk: str | None = None,
        ) -> None:
            tbl_u = tbl.strip().upper()
            fk_elem = (element_fk or fk).strip()
            fk_src_u = _fk_row_constraint_name_u(sr) if sr is not None else ""
            fk_tgt_u = _fk_row_constraint_name_u(tr) if tr is not None else ""
            if status == "TARGET_ONLY":
                path_sch = (target_schema or "").strip() or str((tr or {}).get("schema_name") or "")
                row_schema = target_schema
            elif status == "SOURCE_ONLY":
                path_sch = (source_schema or "").strip() or str((sr or {}).get("schema_name") or "")
                row_schema = source_schema
            elif status in ("MISMATCH", "WARNING"):
                path_sch = (source_schema or "").strip() or str((sr or {}).get("schema_name") or "")
                row_schema = source_schema
            elem = element_path(path_sch, tbl, fk_elem)
            sd_d, sd_u = ("", "")
            if sr is not None:
                sd_d, sd_u = _fk_delete_update(sr, src_d.name)
            td_d, td_u = ("", "")
            if tr is not None:
                td_d, td_u = _fk_delete_update(tr, tgt_d.name)
            spairs = _fk_column_pair_string(src_fk_cols, tbl_u, fk_src_u) if fk_src_u else ""
            tpairs = _fk_column_pair_string(tgt_fk_cols, tbl_u, fk_tgt_u) if fk_tgt_u else ""
            if status == "TARGET_ONLY":
                ot = tgt_kind.get(tbl_u, "TABLE")
            elif status == "SOURCE_ONLY":
                ot = src_kind.get(tbl_u, "TABLE")
            else:
                ot = src_kind.get(tbl_u) or tgt_kind.get(tbl_u, "TABLE")
            details.append(
                {
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": row_schema,
                    "table": tbl,
                    "fk_name": fk,
                    "status": status,
                    "object_type": ot,
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

        consumed_src: set[tuple[str, str]] = set()
        consumed_tgt: set[tuple[str, str]] = set()

        def compare_fk_pair(
            sr: dict[str, Any],
            tr: dict[str, Any],
            tbl: str,
            tbl_u: str,
            *,
            from_signature_pair: bool = False,
        ) -> bool:
            """Return True if a mismatch or warning detail row was appended."""
            spairs = _fk_column_pair_string(src_fk_cols, tbl_u, _fk_row_constraint_name_u(sr))
            tpairs = _fk_column_pair_string(tgt_fk_cols, tbl_u, _fk_row_constraint_name_u(tr))
            refs_ok = _fk_ref_tables_match(sr, tr, source_schema, target_schema, resolved_src)
            ref_tbl_match = str(sr.get("ref_table") or "").strip().upper() == str(tr.get("ref_table") or "").strip().upper()
            pairs_match = _normalize_fk_pairs_key(spairs) == _normalize_fk_pairs_key(tpairs)
            sd_d, sd_u = _fk_delete_update(sr, src_d.name)
            td_d, td_u = _fk_delete_update(tr, tgt_d.name)
            actions_match = sd_d == td_d and sd_u == td_u
            if refs_ok and pairs_match and actions_match:
                return False
            err_desc = _fk_mismatch_description(
                refs_ok=refs_ok,
                ref_table_match=ref_tbl_match,
                pairs_match=pairs_match,
                actions_match=actions_match,
            )
            out_status = "WARNING" if (from_signature_pair and err_desc == "Referenced table mismatch") else "MISMATCH"
            display_fk = str(
                tr.get("fk_name") or tr.get("constname") or sr.get("fk_name") or sr.get("constname") or ""
            ).strip()
            append_fk_detail(
                tbl=tbl,
                fk=display_fk,
                status=out_status,
                err_desc=err_desc,
                sr=sr,
                tr=tr,
                element_fk=display_fk,
            )
            return True

        all_keys = set(src_map) | set(tgt_map)
        for k in sorted(all_keys):
            sr, tr = src_map.get(k), tgt_map.get(k)
            tbl = str((sr or tr or {}).get("table_name", "")).strip()
            tbl_u, fk_u = k
            if sr is not None and tr is not None:
                consumed_src.add(fk_key(sr))
                consumed_tgt.add(fk_key(tr))
                compare_fk_pair(sr, tr, tbl, tbl_u, from_signature_pair=False)

        orphan_src = [r for r in src_rows if fk_key(r) not in consumed_src]
        orphan_tgt = [r for r in tgt_rows if fk_key(r) not in consumed_tgt]
        sig_used_src: set[tuple[str, str]] = set()
        sig_used_tgt: set[tuple[str, str]] = set()

        def _orphan_row_sig(r: dict[str, Any], is_src: bool) -> tuple[str, str, str]:
            tbl_u = str(r.get("table_name", "")).strip().upper()
            fk_u = _fk_row_constraint_name_u(r)
            cols = src_fk_cols if is_src else tgt_fk_cols
            return _fk_signature(tbl_u, fk_u, cols, r)

        src_by_sig: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for sr in orphan_src:
            src_by_sig[_orphan_row_sig(sr, True)].append(sr)
        tgt_by_sig: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for tr in orphan_tgt:
            tgt_by_sig[_orphan_row_sig(tr, False)].append(tr)

        for sig in sorted(set(src_by_sig.keys()) & set(tgt_by_sig.keys())):
            ls = src_by_sig[sig]
            rs = tgt_by_sig[sig]
            tbl_u = sig[0]
            for sr, tr in _fk_greedy_orphan_pairs(
                ls,
                rs,
                tbl_u,
                src_fk_cols=src_fk_cols,
                tgt_fk_cols=tgt_fk_cols,
                source_schema=source_schema,
                target_schema=target_schema,
                resolved_src=resolved_src,
                src_dialect=src_d.name,
                tgt_dialect=tgt_d.name,
            ):
                sig_used_src.add(fk_key(sr))
                sig_used_tgt.add(fk_key(tr))
                tbl = str(sr.get("table_name") or tr.get("table_name") or "").strip()
                tbl_u_row = tbl.upper()
                compare_fk_pair(sr, tr, tbl, tbl_u_row, from_signature_pair=True)

        loose_used_src: set[tuple[str, str]] = set()
        loose_used_tgt: set[tuple[str, str]] = set()
        rem_src = [sr for sr in orphan_src if fk_key(sr) not in sig_used_src]
        rem_tgt = [tr for tr in orphan_tgt if fk_key(tr) not in sig_used_tgt]
        loose_src: dict[tuple[str, str, frozenset[str]], list[dict[str, Any]]] = defaultdict(list)
        for sr in rem_src:
            loose_src[_fk_loose_bucket_key(sr, src_fk_cols)].append(sr)
        loose_tgt: dict[tuple[str, str, frozenset[str]], list[dict[str, Any]]] = defaultdict(list)
        for tr in rem_tgt:
            loose_tgt[_fk_loose_bucket_key(tr, tgt_fk_cols)].append(tr)
        for lk in sorted(set(loose_src.keys()) & set(loose_tgt.keys())):
            ls = loose_src[lk]
            rs = loose_tgt[lk]
            tbl_u = lk[0]
            for sr, tr in _fk_greedy_orphan_pairs(
                ls,
                rs,
                tbl_u,
                src_fk_cols=src_fk_cols,
                tgt_fk_cols=tgt_fk_cols,
                source_schema=source_schema,
                target_schema=target_schema,
                resolved_src=resolved_src,
                src_dialect=src_d.name,
                tgt_dialect=tgt_d.name,
            ):
                loose_used_src.add(fk_key(sr))
                loose_used_tgt.add(fk_key(tr))
                tbl = str(sr.get("table_name") or tr.get("table_name") or "").strip()
                tbl_u_row = tbl.upper()
                compare_fk_pair(sr, tr, tbl, tbl_u_row, from_signature_pair=True)
        sig_used_src |= loose_used_src
        sig_used_tgt |= loose_used_tgt

        for sr in orphan_src:
            if fk_key(sr) in sig_used_src:
                continue
            tbl = str(sr.get("table_name", "")).strip()
            fk = str(sr.get("fk_name") or sr.get("constname") or "").strip()
            append_fk_detail(tbl=tbl, fk=fk, status="SOURCE_ONLY", err_desc="FK missing in target", sr=sr, tr=None)

        for tr in orphan_tgt:
            if fk_key(tr) in sig_used_tgt:
                continue
            tbl = str(tr.get("table_name", "")).strip()
            fk = str(tr.get("fk_name") or tr.get("constname") or "").strip()
            append_fk_detail(tbl=tbl, fk=fk, status="TARGET_ONLY", err_desc="FK missing in source", sr=None, tr=tr)

        hard = sum(
            1
            for d in details
            if str(d.get("status", "")).upper() in ("MISMATCH", "SOURCE_ONLY", "TARGET_ONLY", "ERROR")
        )
        warn = sum(1 for d in details if str(d.get("status", "")).upper() == "WARNING")
        passed = hard == 0
        return ValidationResult(
            validation_name="foreign_keys",
            passed=passed,
            summary=f"Foreign keys: {len(details)} row(s); {hard} error(s), {warn} warning(s).",
            details=details,
            stats={"diff_count": len(details), "error_count": hard, "warning_count": warn},
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
        src_kind = self._table_kind_map(resolved_src, source=True)
        tgt_kind = self._table_kind_map(target_schema, source=False)

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
            tbl_u = tbl.upper()
            if sr is not None and tr is None:
                details.append(
                    {
                        "source_schema": source_schema,
                        "target_schema": target_schema,
                        "schema": source_schema,
                        "table": tbl,
                        "constraint_name": cname,
                        "status": "SOURCE_ONLY",
                        "object_type": src_kind.get(tbl_u, "TABLE"),
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
                        "object_type": tgt_kind.get(tbl_u, "TABLE"),
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
                            "object_type": src_kind.get(tbl_u) or tgt_kind.get(tbl_u, "TABLE"),
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
