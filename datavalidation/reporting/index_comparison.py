"""
Index / PK / table-size comparison ported from
``PySparkSchemaComparisonService.compare_index_definitions`` in
``app/services/pyspark_schema_comparison.py`` (full_outer join, signature masking,
column-order vs set equality, optional env rules).
"""
from __future__ import annotations

import os
import re
from typing import Any


def _norm(s: Any) -> str:
    """Uppercase normalized identifier; coerces JDBC/JPype ``java.lang.String`` to Python ``str``."""
    if s is None:
        return ""
    return str(s).strip().upper()


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().lower() in ("1", "true", "y", "yes")


def _parse_index_rules() -> tuple[bool, bool]:
    raw = os.environ.get("DV_INDEX_RULES", "") or ""
    order_warn = False
    sys_warn = False
    try:
        import json

        rules = json.loads(raw) if raw.strip().startswith("[") else []
        for r in rules if isinstance(rules, list) else []:
            rt = str(r.get("rule_type", "")).lower()
            mt = str(r.get("match_type", "")).lower()
            if rt == "column_order_insensitive" and mt == "warning":
                order_warn = True
            if rt == "missing_sysname" and mt == "warning":
                sys_warn = True
    except Exception:
        pass
    return order_warn, sys_warn


def _many_col_threshold() -> int:
    try:
        return int(os.environ.get("DV_MANY_COLUMNS_THRESHOLD", "120"))
    except Exception:
        return 120


def _normalize_src_ix_row(r: dict[str, Any], pair: dict[str, Any]) -> dict[str, Any] | None:
    sn = _norm(r.get("schema_name"))
    tn = _norm(r.get("table_name"))
    if (sn, tn) != (pair["s_schema_norm"], pair["s_object_norm"]):
        return None
    rule = str(r.get("unique_rule") or "").strip().upper()
    if rule == "P":
        kind, is_u = "PK", True
    elif rule == "U":
        kind, is_u = "UNIQUE", True
    else:
        kind, is_u = "INDEX", False
    co = str(r.get("colorder") or "A").strip().upper()[:1] or "A"
    if co not in ("A", "D"):
        co = "A"
    return {
        "j_schema": pair["s_schema_norm"],
        "j_table": pair["s_object_norm"],
        "idx_norm": _norm(r.get("idx_name")),
        "Kind": kind,
        "IsUnique": is_u,
        "colseq": r.get("colseq") if r.get("colseq") is not None else 0,
        "col_name": str(r.get("col_name") or "").strip(),
        "ord": co,
        "pair": pair,
    }


def _normalize_tgt_ix_row(r: dict[str, Any], pair: dict[str, Any]) -> dict[str, Any] | None:
    sn = _norm(r.get("schema_name"))
    tn = _norm(r.get("table_name"))
    if (sn, tn) != (pair["r_schema_norm"], pair["r_object_norm"]):
        return None
    pk = _truthy(r.get("is_primary_key"))
    uq = _truthy(r.get("is_unique"))
    if pk:
        kind, is_u = "PK", True
    elif uq:
        kind, is_u = "UNIQUE", True
    else:
        kind, is_u = "INDEX", False
    desc = _truthy(r.get("is_descending_key"))
    co = "D" if desc else "A"
    return {
        "j_schema": pair["s_schema_norm"],
        "j_table": pair["s_object_norm"],
        "idx_norm": _norm(r.get("idx_name")),
        "Kind": kind,
        "IsUnique": is_u,
        "colseq": r.get("colseq") if r.get("colseq") is not None else 0,
        "col_name": str(r.get("col_name") or "").strip(),
        "ord": co,
        "pair": pair,
    }


def _aggregate_sig(rows: list[dict[str, Any]]) -> str:
    rows = sorted(rows, key=lambda x: (x.get("colseq") is None, x.get("colseq") or 0))
    return ",".join(f"{x['col_name']} {x['ord']}" for x in rows)


def _cols_set_equal(sig_a: str | None, sig_b: str | None) -> bool:
    def tokens(sig: str | None) -> list[str]:
        if not sig:
            return []
        parts = [p.strip() for p in sig.split(",") if p.strip()]
        cleaned = [re.sub(r"\s+", "", p.upper()) for p in parts]
        return sorted(cleaned)

    return tokens(sig_a) == tokens(sig_b)


def compare_indexes_legacy(
    pairs: list[dict[str, Any]],
    src_ix_rows: list[dict[str, Any]],
    tgt_ix_rows: list[dict[str, Any]],
    *,
    source_schema: str | None,
    target_schema: str | None,
    src_col_counts: dict[tuple[str, str], int],
    tgt_col_counts: dict[tuple[str, str], int],
) -> list[dict[str, Any]]:
    """
    Return ``details`` rows for ``ValidationResult`` (validation_name ``indexes``),
    aligned with the original Spark service output shape (before ``to_legacy_csv``).
    """
    order_insensitive_warn, missing_sysname_warn = _parse_index_rules()
    many_thr = _many_col_threshold()
    details: list[dict[str, Any]] = []

    src_norm: list[dict[str, Any]] = []
    tgt_norm: list[dict[str, Any]] = []
    for p in pairs:
        for r in src_ix_rows:
            nr = _normalize_src_ix_row(r, p)
            if nr:
                src_norm.append(nr)
        for r in tgt_ix_rows:
            nr = _normalize_tgt_ix_row(r, p)
            if nr:
                tgt_norm.append(nr)

    # group -> list of col rows
    def group_key(r: dict[str, Any]) -> tuple[str, str, str, str, bool]:
        return (r["j_schema"], r["j_table"], r["idx_norm"], r["Kind"], r["IsUnique"])

    src_groups: dict[tuple[str, str, str, str, bool], list[dict[str, Any]]] = {}
    for r in src_norm:
        src_groups.setdefault(group_key(r), []).append(r)
    tgt_groups: dict[tuple[str, str, str, str, bool], list[dict[str, Any]]] = {}
    for r in tgt_norm:
        tgt_groups.setdefault(group_key(r), []).append(r)

    src_sig: dict[tuple[str, str, str, str, bool], str] = {k: _aggregate_sig(v) for k, v in src_groups.items()}
    tgt_sig: dict[tuple[str, str, str, str, bool], str] = {k: _aggregate_sig(v) for k, v in tgt_groups.items()}

    all_keys = set(src_sig) | set(tgt_sig)

    def _uniq_for_key(k: tuple[str, str, str, str, bool]) -> tuple[bool | None, bool | None]:
        su = src_groups[k][0]["IsUnique"] if k in src_groups else None
        du = tgt_groups[k][0]["IsUnique"] if k in tgt_groups else None
        return su, du

    # signature-based name pairs (same j_s,j_t, Kind, IsUnique, ColsSig) — different idx names
    sig_pairs: list[tuple[str, str, str, str]] = []
    src_by_sig: dict[tuple[str, str, str, bool, str], list[str]] = {}
    for k, sig in src_sig.items():
        js, jt, idx, kind, is_u = k
        src_by_sig.setdefault((js, jt, kind, is_u, sig), []).append(idx)
    tgt_by_sig: dict[tuple[str, str, str, bool, str], list[str]] = {}
    for k, sig in tgt_sig.items():
        js, jt, idx, kind, is_u = k
        tgt_by_sig.setdefault((js, jt, kind, is_u, sig), []).append(idx)
    for sk, l_idxs in src_by_sig.items():
        r_idxs = tgt_by_sig.get(sk, [])
        js, jt, kind, is_u, _sig = sk
        for li in l_idxs:
            for ri in r_idxs:
                if li != ri:
                    sig_pairs.append((js, jt, li, ri))

    pair_by_jt: dict[tuple[str, str], dict[str, Any]] = {}
    for p in pairs:
        pair_by_jt[(p["s_schema_norm"], p["s_object_norm"])] = p

    for key in sorted(all_keys):
        js, jt, idx_norm, kind, is_u = key
        if not idx_norm:
            continue
        s_sig = src_sig.get(key)
        t_sig = tgt_sig.get(key)
        pair = pair_by_jt.get((js, jt), {})
        src_sch = source_schema or pair.get("SourceSchemaName") or ""
        src_tbl = pair.get("SourceObjectName") or ""
        dst_sch = target_schema or pair.get("DestinationSchemaName") or ""
        dst_tbl = pair.get("DestinationObjectName") or ""

        missing_src = t_sig is None
        missing_tgt = s_sig is None
        cols_match = (s_sig or "") == (t_sig or "") if s_sig is not None and t_sig is not None else False
        su, du = _uniq_for_key(key)
        uniq_match = (su == du) if su is not None and du is not None else True

        has_sig_right = missing_tgt and any(sp[0] == js and sp[1] == jt and sp[2] == idx_norm for sp in sig_pairs)
        has_sig_left = missing_src and any(sp[0] == js and sp[1] == jt and sp[3] == idx_norm for sp in sig_pairs)
        mask = (missing_tgt and has_sig_right) or (missing_src and has_sig_left)
        if mask:
            continue

        if not (missing_src or missing_tgt or not cols_match or not uniq_match):
            continue

        cols_set_eq = _cols_set_equal(s_sig, t_sig)
        idx_uc = idx_norm or ""
        warn_order = (not cols_match) and cols_set_eq and order_insensitive_warn
        warn_sys = (
            (missing_tgt or missing_src)
            and missing_sysname_warn
            and (idx_uc.startswith("SQL") or idx_uc.startswith("PK"))
        )
        st = "WARNING" if (warn_order or warn_sys) else "error"

        if missing_src:
            err_desc = "Index missing in source"
            err_code = "INDEX_MISSING_IN_SOURCE"
        elif missing_tgt:
            err_desc = "Index missing in target"
            err_code = "INDEX_MISSING_IN_TARGET"
        elif (not cols_match) and cols_set_eq:
            err_desc = "Index column order mismatch"
            err_code = "INDEX_MISMATCH"
        elif not cols_match:
            err_desc = "Index columns mismatch"
            err_code = "INDEX_COLUMNS_MISMATCH"
        elif not uniq_match:
            err_desc = "Index uniqueness mismatch"
            err_code = "INDEX_UNIQUENESS_MISMATCH"
        else:
            err_desc = "Index mismatch"
            err_code = "INDEX_MISMATCH"

        elem = f"{src_sch}.{src_tbl}.{idx_norm}".strip(".") if idx_norm else f"{src_sch}.{src_tbl}"
        details.append({
            "source_schema": src_sch,
            "target_schema": dst_sch,
            "schema": src_sch,
            "table": src_tbl,
            "index": idx_norm,
            "object_type": "TABLE",
            "status": st,
            "element_path": elem,
            "error_code": err_code,
            "error_description": err_desc,
            "source_columns": s_sig,
            "destination_columns": t_sig,
            "source_unique": is_u if s_sig is not None else None,
            "destination_unique": is_u if t_sig is not None else None,
            "index_kind": kind,
        })

    # --- PK presence (per matched table) ---
    db2_pk: set[tuple[str, str]] = set()
    for k in src_sig:
        js, jt, _idx, kind, _u = k
        if kind == "PK":
            db2_pk.add((js, jt))
    az_pk: set[tuple[str, str]] = set()
    for k in tgt_sig:
        js, jt, _idx, kind, _u = k
        if kind == "PK":
            az_pk.add((js, jt))

    for p in pairs:
        js, jt = p["s_schema_norm"], p["s_object_norm"]
        hs = (js, jt) in db2_pk
        ht = (js, jt) in az_pk
        src_sch = source_schema or p.get("SourceSchemaName") or ""
        src_tbl = p.get("SourceObjectName") or ""
        dst_sch = target_schema or p.get("DestinationSchemaName") or ""
        dst_tbl = p.get("DestinationObjectName") or ""
        elem = f"{src_sch}.{src_tbl}.PRIMARY KEY"
        if hs != ht:
            details.append({
                "source_schema": src_sch,
                "target_schema": dst_sch,
                "schema": src_sch,
                "table": src_tbl,
                "index": "PRIMARY KEY",
                "object_type": "TABLE",
                "status": "error",
                "element_path": elem,
                "error_code": "INDEX_MISMATCH",
                "error_description": "Primary key missing in target" if hs and not ht else (
                    "Primary key missing in source" if not hs and ht else "Primary key presence mismatch"
                ),
                "source_columns": "",
                "destination_columns": "",
                "source_unique": True,
                "destination_unique": True,
            })
        elif not hs and not ht:
            details.append({
                "source_schema": src_sch,
                "target_schema": dst_sch,
                "schema": src_sch,
                "table": src_tbl,
                "index": "PRIMARY KEY",
                "object_type": "TABLE",
                "status": "INFO",
                "element_path": elem,
                "error_code": "INDEX_MISMATCH",
                "error_description": "Table has no primary key on either side (note)",
                "source_columns": "",
                "destination_columns": "",
                "source_unique": None,
                "destination_unique": None,
            })

    # --- High column count note ---
    seen_many: set[tuple[str, str]] = set()
    for p in pairs:
        js, jt = p["s_schema_norm"], p["s_object_norm"]
        if (js, jt) in seen_many:
            continue
        seen_many.add((js, jt))
        sc = src_col_counts.get((js, jt), 0)
        tc = tgt_col_counts.get((p["r_schema_norm"], p["r_object_norm"]), 0)
        mx = max(sc, tc)
        if mx >= many_thr:
            src_sch = source_schema or p.get("SourceSchemaName") or ""
            src_tbl = p.get("SourceObjectName") or ""
            dst_sch = target_schema or p.get("DestinationSchemaName") or ""
            dst_tbl = p.get("DestinationObjectName") or ""
            details.append({
                "source_schema": src_sch,
                "target_schema": dst_sch,
                "schema": src_sch,
                "table": src_tbl,
                "index": "",
                "object_type": "TABLE",
                "status": "INFO",
                "element_path": f"{src_sch}.{src_tbl}",
                "error_code": "INDEX_MISMATCH",
                "error_description": f"High column count (>={many_thr})",
                "source_columns": str(sc),
                "destination_columns": str(tc),
                "source_unique": None,
                "destination_unique": None,
            })

    return details
