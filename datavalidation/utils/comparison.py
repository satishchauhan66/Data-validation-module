"""Comparison helpers for source vs target results."""
from typing import Any


def compare_rows_by_key(
    source_rows: list[dict],
    target_rows: list[dict],
    key_columns: list[str],
    value_columns: list[str],
) -> list[dict[str, Any]]:
    """
    Compare two lists of dicts by key_columns; report differences in value_columns.
    Returns list of diff rows with status SOURCE_ONLY, TARGET_ONLY, or MISMATCH.
    """
    def key_from(row: dict) -> tuple:
        return tuple(row.get(k) for k in key_columns)

    src_map = {key_from(r): r for r in source_rows}
    tgt_map = {key_from(r): r for r in target_rows}
    all_keys = set(src_map) | set(tgt_map)
    diffs = []
    for k in all_keys:
        sr, tr = src_map.get(k), tgt_map.get(k)
        if sr is None:
            diffs.append({"status": "TARGET_ONLY", "key": k, "target": tr, "source": None})
        elif tr is None:
            diffs.append({"status": "SOURCE_ONLY", "key": k, "source": sr, "target": None})
        else:
            mismatches = []
            for col in value_columns:
                sv, tv = sr.get(col), tr.get(col)
                if sv != tv:
                    mismatches.append({"column": col, "source": sv, "target": tv})
            if mismatches:
                diffs.append({"status": "MISMATCH", "key": k, "source": sr, "target": tr, "diffs": mismatches})
    return diffs


def normalize_for_compare(val: Any) -> Any:
    """Normalize values for comparison (e.g. str strip, case)."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val
    # str, JDBC java.lang.String, etc.
    return str(val).strip().upper()
