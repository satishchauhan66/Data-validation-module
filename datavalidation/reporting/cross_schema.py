"""
Cross-schema table pairing from ``PySparkSchemaComparisonService._build_table_pairs``:
``app/services/pyspark_schema_comparison.py`` (join on object name when both schemas set).
"""
from __future__ import annotations

from typing import Any


def _norm(s: Any) -> str:
    """Uppercase normalized identifier; coerces JDBC/JPype ``java.lang.String`` to Python ``str``."""
    if s is None:
        return ""
    return str(s).strip().upper()


def build_table_pairs_from_catalog_rows(
    src_rows: list[dict[str, Any]],
    tgt_rows: list[dict[str, Any]],
    source_schema: str | None,
    target_schema: str | None,
) -> list[dict[str, Any]]:
    """
    Match tables between source and target.

    When both ``source_schema`` and ``target_schema`` are set, join only on table name
    (case-insensitive), enabling USERID↔dbo style mapping.

    When either is missing, join on (schema_norm, table_norm) for auto-discovery style.
    """
    src_list: list[tuple[str, str, str, str]] = []  # schema_name, table_name, s_norm, t_norm
    for r in src_rows:
        sch = str(r.get("schema_name") or "").strip()
        tbl = str(r.get("table_name") or "").strip()
        ot = str(r.get("object_type") or "TABLE").strip().upper()
        if ot in ("T", "U"):
            ot = "TABLE"
        if ot != "TABLE":
            continue
        src_list.append((sch, tbl, _norm(sch), _norm(tbl)))

    tgt_list: list[tuple[str, str, str, str]] = []
    for r in tgt_rows:
        sch = str(r.get("schema_name") or "").strip()
        tbl = str(r.get("table_name") or "").strip()
        ot = str(r.get("object_type") or "TABLE").strip().upper()
        if ot in ("T", "U"):
            ot = "TABLE"
        if ot != "TABLE":
            continue
        tgt_list.append((sch, tbl, _norm(sch), _norm(tbl)))

    pairs: list[dict[str, Any]] = []
    if source_schema and target_schema:
        tgt_by_table = {}
        for sch, tbl, sn, tn in tgt_list:
            tgt_by_table.setdefault(tn, []).append((sch, tbl, sn, tn))
        for sch, tbl, sn, tn in src_list:
            for rsch, rtbl, rsn, rtn in tgt_by_table.get(tn, []):
                pairs.append({
                    "SourceSchemaName": sch,
                    "SourceObjectName": tbl,
                    "DestinationSchemaName": rsch,
                    "DestinationObjectName": rtbl,
                    "s_schema_norm": sn,
                    "s_object_norm": tn,
                    "r_schema_norm": rsn,
                    "r_object_norm": rtn,
                })
    else:
        tgt_keys = {(rsn, rtn) for _, _, rsn, rtn in tgt_list}
        for sch, tbl, sn, tn in src_list:
            if (sn, tn) not in tgt_keys:
                continue
            for rsch, rtbl, rsn, rtn in tgt_list:
                if (sn, tn) == (rsn, rtn):
                    pairs.append({
                        "SourceSchemaName": sch,
                        "SourceObjectName": tbl,
                        "DestinationSchemaName": rsch,
                        "DestinationObjectName": rtbl,
                        "s_schema_norm": sn,
                        "s_object_norm": tn,
                        "r_schema_norm": rsn,
                        "r_object_norm": rtn,
                    })
                    break
    return pairs
