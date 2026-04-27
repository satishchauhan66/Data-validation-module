"""
Data validations: row counts, column nulls, distinct keys, checksum, referential integrity, constraint integrity.

Ports core behaviour from the legacy FastAPI/PySpark service using dialect SQL + JDBC execution (no Spark).
"""
from __future__ import annotations

import json
import os
import threading
import time
from collections import defaultdict
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

from datavalidation.config import ConnectionConfig, ValidationOptions, resolve_data_validation_phases
from datavalidation.connectors.base import ConnectionAdapter
from datavalidation.results import ValidationResult
from datavalidation.validators.base import BaseValidator


_VALID_MODES = {"exact", "estimate", "auto", "skip"}


def _to_int(v: Any) -> int | None:
    """Coerce JDBC/SQLAlchemy numeric to plain ``int``; return ``None`` on failure."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except Exception:
            return None


def _norm_upper(s: Any) -> str:
    return str(s or "").strip().upper()


def _quote_db2_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""').upper() + '"'


def _quote_azure_ident(name: str) -> str:
    return "[" + str(name or "").replace("]", "]]") + "]"


def _row_get(row: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
        lk = k.lower()
        if lk in row and row[lk] is not None:
            return row[lk]
    return None


def _is_nullable_column(row: dict[str, Any], dialect_name: str) -> bool:
    v = _row_get(row, "is_nullable", "NULLS")
    if dialect_name == "db2":
        return str(v or "").strip().upper() == "Y"
    return bool(v) if isinstance(v, bool) else str(v).strip().lower() in ("yes", "true", "1", "y")


def _char_length_cap(row: dict[str, Any], dialect_name: str) -> int | None:
    if dialect_name == "db2":
        return _to_int(_row_get(row, "length", "LENGTH"))
    return _to_int(_row_get(row, "max_length", "length"))


class DataValidator(BaseValidator):
    """Runs all data-level validations."""

    def __init__(
        self,
        source_config: ConnectionConfig,
        target_config: ConnectionConfig,
        options: ValidationOptions | None = None,
        source_adapter: ConnectionAdapter | None = None,
        target_adapter: ConnectionAdapter | None = None,
    ):
        super().__init__(source_config, target_config, options, source_adapter, target_adapter)
        # One catalog round-trip per schema per process — avoids N× repeats when scanning many tables.
        self._columns_catalog_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._index_catalog_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._pair_tables_cache: dict[tuple[str, str, tuple[str, ...]], tuple[str, list[str]]] = {}
        self._catalog_lock = threading.Lock()

    def _data_query_timeout_seconds(self) -> int | None:
        """Cap for DISTINCT/CHECKSUM/FK/null/constraint queries so work cannot hang forever (locks, huge scans).

        ``DV_DATA_QUERY_TIMEOUT_SEC``: seconds per statement; ``0`` or negative = no limit.
        Otherwise uses :attr:`ValidationOptions.data_query_timeout_seconds`.
        """
        raw = os.environ.get("DV_DATA_QUERY_TIMEOUT_SEC", "").strip()
        if raw:
            try:
                v = int(raw)
                return None if v <= 0 else v
            except ValueError:
                pass
        opt = self.options.data_query_timeout_seconds
        if opt is not None:
            try:
                v = int(opt)
                return None if v <= 0 else v
            except (TypeError, ValueError):
                pass
        return None

    def _source_execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> list[dict[str, Any]]:
        if timeout_seconds is None:
            timeout_seconds = self._data_query_timeout_seconds()
        return super()._source_execute(sql, params, timeout_seconds=timeout_seconds)

    def _target_execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> list[dict[str, Any]]:
        if timeout_seconds is None:
            timeout_seconds = self._data_query_timeout_seconds()
        return super()._target_execute(sql, params, timeout_seconds=timeout_seconds)

    def _excluded(self, table: str) -> bool:
        ex = {str(t).strip().upper() for t in (self.options.exclude_tables or [])}
        return str(table).strip().upper() in ex

    def _force_estimate(self, table: str) -> bool:
        ex = {str(t).strip().upper() for t in (self.options.estimate_tables or [])}
        return str(table).strip().upper() in ex

    def _fetch_table_stats(self, side: str, schema: str | None) -> dict[str, dict[str, int | None]]:
        """Return ``{TABLE_UPPER: {"row_estimate": n, "bytes_estimate": n}}`` from one catalog query.

        Empty dict on any failure or if the dialect doesn't support it.
        """
        dialect = self._source_dialect if side == "source" else self._target_dialect
        sql = dialect.table_stats_query(schema)
        if not sql:
            return {}
        try:
            executor = self._source_execute if side == "source" else self._target_execute
            rows = executor(sql)
        except Exception:
            return {}
        out: dict[str, dict[str, int | None]] = {}
        for r in rows or []:
            tbl = str(r.get("table_name") or "").strip().upper()
            if not tbl:
                continue
            out[tbl] = {
                "row_estimate": _to_int(r.get("row_estimate")),
                "bytes_estimate": _to_int(r.get("bytes_estimate")),
            }
        return out

    def _decide_method(
        self,
        table: str,
        src_bytes: int | None,
        tgt_bytes: int | None,
    ) -> str:
        """Return 'exact' | 'estimate' | 'skip' for one table based on options + sizes."""
        mode = (self.options.row_count_mode or "auto").strip().lower()
        if mode not in _VALID_MODES:
            mode = "auto"
        if self._excluded(table):
            return "skip"
        if mode == "skip":
            return "skip"
        if self._force_estimate(table):
            return "estimate"
        if mode == "exact":
            return "exact"
        if mode == "estimate":
            return "estimate"
        threshold = max(0, int(self.options.large_table_threshold_bytes or 0))
        max_bytes = max(int(src_bytes or 0), int(tgt_bytes or 0))
        if threshold and max_bytes and max_bytes >= threshold:
            return "estimate"
        return "exact"

    def _count_one(
        self,
        side: str,
        schema: str,
        table: str,
        method: str,
    ) -> tuple[int | None, str, str | None]:
        """Run one row count. Returns ``(value, used_method, error_message)``.

        Honours ``count_with_dirty_read`` (DB2 ``WITH UR`` / SQL Server ``WITH (NOLOCK)``) and
        ``row_count_timeout_seconds`` so big-table exact counts don't block on locks or hang forever.
        On exact failure in ``auto`` mode, falls back to estimate so the run completes.
        """
        dialect = self._source_dialect if side == "source" else self._target_dialect
        executor = self._source_execute if side == "source" else self._target_execute
        dirty = bool(self.options.count_with_dirty_read)
        timeout = self.options.row_count_timeout_seconds

        def _run_exact(sql_provider) -> int | None:
            try:
                sql = sql_provider(schema, table, dirty_read=dirty)
            except TypeError:
                sql = sql_provider(schema, table)
            rows = executor(sql, timeout_seconds=timeout)
            return _to_int(rows[0].get("cnt")) if rows else None

        def _run_est() -> int | None:
            sql = dialect.row_count_estimate_query(schema, table)
            if not sql:
                return None
            rows = executor(sql, timeout_seconds=timeout)
            return _to_int(rows[0].get("cnt")) if rows else None

        if method == "skip":
            return None, "skip", None
        if method == "estimate":
            try:
                return _run_est(), "estimate", None
            except Exception as e:
                return None, "estimate", str(e)
        # exact
        try:
            return _run_exact(dialect.row_count_query), "exact", None
        except Exception as e:
            if (self.options.row_count_mode or "auto").strip().lower() == "auto":
                try:
                    return _run_est(), "estimate_fallback", str(e)
                except Exception as e2:
                    return None, "exact", f"{e}; estimate fallback failed: {e2}"
            return None, "exact", str(e)

    def _is_match(self, src: int | None, tgt: int | None, methods: tuple[str, str]) -> bool:
        """Compare counts; allow tolerance when either side is an estimate."""
        if src is None or tgt is None:
            return False
        if any(m.startswith("estimate") for m in methods):
            tol = max(0.0, float(self.options.estimate_tolerance_pct or 0.0)) / 100.0
            base = max(abs(int(src)), abs(int(tgt)), 1)
            return abs(int(src) - int(tgt)) <= base * tol
        return int(src) == int(tgt)

    # --- Shared pairing + SQL helpers (ported from legacy PySpark data validation) ---

    def _catalog_object_types_for_data(self) -> list[str]:
        """TABLE/VIEW list for row-level checks (matches legacy default)."""
        ot = list(self.options.object_types or [])
        wanted = {"TABLE", "VIEW"}
        picked = [x for x in ot if str(x).upper() in wanted]
        return picked or ["TABLE", "VIEW"]

    def _pair_common_tables(
        self,
        source_schema: str,
        target_schema: str,
        object_types: list[str],
    ) -> tuple[str, list[str]]:
        """Return ``(resolved_source_schema, common_table_names)`` for USERID→dbo-style matching."""
        ot_key = tuple(sorted(str(x).upper() for x in (object_types or [])))
        cache_key = ((source_schema or "").strip(), (target_schema or "").strip(), ot_key)
        with self._catalog_lock:
            hit = self._pair_tables_cache.get(cache_key)
        if hit is not None:
            return hit

        src_d, tgt_d = self._source_dialect, self._target_dialect
        resolved_src = getattr(self, "_resolve_source_schema", lambda s: s)(source_schema) or source_schema
        src_tables = self._source_execute(src_d.catalog_tables_query(resolved_src, object_types))
        if (
            not src_tables
            and source_schema
            and str(source_schema).strip().upper() == "USERID"
            and resolved_src != "USERID"
        ):
            src_tables = self._source_execute(src_d.catalog_tables_query("USERID", object_types))
            if src_tables:
                resolved_src = "USERID"
        tgt_tables = self._target_execute(tgt_d.catalog_tables_query(target_schema, object_types))

        def row_key(r: dict[str, Any]) -> tuple[str, str]:
            return (str(r.get("schema_name", "")).strip(), str(r.get("table_name", "")).strip())

        src_set = {row_key(r) for r in src_tables}
        tgt_set = {row_key(r) for r in tgt_tables}
        src_names = {tbl for (_, tbl) in src_set}
        tgt_names = {tbl for (_, tbl) in tgt_set}
        src_upper = {str(t).strip().upper(): t for t in src_names}
        tgt_upper = {str(t).strip().upper(): t for t in tgt_names}
        common = sorted({src_upper[k] for k in src_upper if k in tgt_upper})
        out = (resolved_src, common)
        with self._catalog_lock:
            self._pair_tables_cache[cache_key] = out
        return out

    def _fetch_all_columns(self, side: str, schema: str) -> list[dict[str, Any]]:
        """All columns for a schema (cached — safe to call many times per validation run)."""
        nk = ("source" if side == "source" else "target", (schema or "").strip())
        with self._catalog_lock:
            if nk in self._columns_catalog_cache:
                return self._columns_catalog_cache[nk]
            d = self._source_dialect if side == "source" else self._target_dialect
            q = d.catalog_columns_query(schema, None)
            exec_fn = self._source_execute if side == "source" else self._target_execute
            try:
                rows = exec_fn(q) or []
            except Exception:
                rows = []
            self._columns_catalog_cache[nk] = rows
            return rows

    def _get_index_catalog_rows(self, side: str, schema: str) -> list[dict[str, Any]]:
        """Index column catalog rows for a schema (cached; large query — must run once per side)."""
        nk = ("source" if side == "source" else "target", (schema or "").strip())
        with self._catalog_lock:
            if nk in self._index_catalog_cache:
                return self._index_catalog_cache[nk]
            d = self._source_dialect if side == "source" else self._target_dialect
            q = getattr(d, "catalog_index_columns_query", lambda _s: None)(schema)
            exec_fn = self._source_execute if side == "source" else self._target_execute
            if not q:
                rows = []
            else:
                try:
                    rows = exec_fn(q) or []
                except Exception:
                    rows = []
            self._index_catalog_cache[nk] = rows
            return rows

    def _pk_column_list(self, side: str, schema: str, table_upper: str) -> list[str]:
        d = self._source_dialect if side == "source" else self._target_dialect
        rows = self._get_index_catalog_rows(side, schema)
        hits: list[tuple[int, str]] = []
        tu = table_upper.strip().upper()
        for r in rows:
            if _norm_upper(_row_get(r, "table_name")) != tu:
                continue
            if d.name == "db2":
                if _norm_upper(_row_get(r, "unique_rule")) != "P":
                    continue
            else:
                if not _to_int(_row_get(r, "is_primary_key")):
                    continue
            seq = _to_int(_row_get(r, "colseq")) or 0
            cname = str(_row_get(r, "col_name") or "").strip()
            if cname:
                hits.append((seq, cname))
        hits.sort(key=lambda x: x[0])
        return [c for _, c in hits]

    def _exec_scalar_int(self, side: str, sql: str) -> int | None:
        exec_fn = self._source_execute if side == "source" else self._target_execute
        timeout = self.options.row_count_timeout_seconds
        try:
            rows = exec_fn(sql, timeout_seconds=timeout)
            if not rows:
                return 0
            row = rows[0]
            v = _row_get(row, "cnt", "CNT")
            if v is None and row:
                v = next(iter(row.values()))
            return _to_int(v)
        except Exception:
            return None

    def validate_row_counts(
        self,
        source_schema: str,
        target_schema: str,
        object_types: list[str] | None = None,
    ) -> ValidationResult:
        """Compare row count per table between source and target.

        Behavior is controlled by :class:`ValidationOptions`:

        * ``row_count_mode`` (``exact`` | ``estimate`` | ``auto`` | ``skip``)
        * ``large_table_threshold_bytes`` (auto mode threshold)
        * ``exclude_tables`` / ``estimate_tables`` lists
        * ``estimate_tolerance_pct`` for estimate-vs-estimate comparisons.

        Tables are matched by name across schemas (e.g. USERID → dbo).
        """
        object_types = object_types or self.options.object_types
        src_d, tgt_d = self._source_dialect, self._target_dialect
        # Resolve USERID to actual DB2 schema for queries; keep source_schema for report labels
        resolved_src = getattr(self, "_resolve_source_schema", lambda s: s)(source_schema) or source_schema
        src_tables = self._source_execute(src_d.catalog_tables_query(resolved_src, object_types))
        # Fallback: if 0 tables and source_schema is USERID, try literal USERID (some DB2 have schema named USERID)
        if not src_tables and source_schema and str(source_schema).strip().upper() == "USERID" and resolved_src != "USERID":
            src_tables = self._source_execute(src_d.catalog_tables_query("USERID", object_types))
            if src_tables:
                resolved_src = "USERID"
        tgt_tables = self._target_execute(tgt_d.catalog_tables_query(target_schema, object_types))

        def row_key(r):
            return (str(r.get("schema_name", "")).strip(), str(r.get("table_name", "")).strip())

        src_set = {row_key(r) for r in src_tables}
        tgt_set = {row_key(r) for r in tgt_tables}
        src_table_names = {tbl for (_, tbl) in src_set}
        tgt_table_names = {tbl for (_, tbl) in tgt_set}
        # Case-insensitive match so DB2 (uppercase) and Azure (mixed) table names match (USERID->dbo)
        src_upper = {str(t).strip().upper(): t for t in src_table_names}
        tgt_upper = {str(t).strip().upper(): t for t in tgt_table_names}
        common_table_names = {src_upper[k] for k in src_upper if k in tgt_upper}
        source_only_pairs = [(s, t) for (s, t) in src_set if t not in tgt_table_names]
        target_only_pairs = [(s, t) for (s, t) in tgt_set if t not in src_table_names]

        # Fetch table sizes once per side (used for 'auto' mode + reporting).
        src_stats = self._fetch_table_stats("source", resolved_src)
        tgt_stats = self._fetch_table_stats("target", target_schema)

        def _stats(side_map: dict[str, dict[str, int | None]], tbl: str) -> tuple[int | None, int | None]:
            entry = side_map.get(str(tbl).strip().upper()) or {}
            return entry.get("row_estimate"), entry.get("bytes_estimate")

        details: list[dict[str, Any]] = []
        method_counter: dict[str, int] = {}
        skipped_count = 0
        error_count = 0

        for (sch, tbl) in source_only_pairs:
            method = "skip" if self._excluded(tbl) else self._decide_method(tbl, _stats(src_stats, tbl)[1], None)
            cnt, used, err = self._count_one("source", resolved_src if sch == source_schema else sch, tbl, method)
            method_counter[used] = method_counter.get(used, 0) + 1
            if used == "skip":
                skipped_count += 1
            if err and cnt is None:
                error_count += 1
            details.append({
                "source_schema": source_schema, "target_schema": target_schema,
                "schema": sch, "table": tbl, "status": "SOURCE_ONLY",
                "source_count": cnt, "target_count": None,
                "element_path": f"{sch}.{tbl}",
                "object_type": "TABLE",
                "count_method": used,
                "source_bytes_estimate": _stats(src_stats, tbl)[1],
                "error": err,
            })

        for (sch, tbl) in target_only_pairs:
            method = "skip" if self._excluded(tbl) else self._decide_method(tbl, None, _stats(tgt_stats, tbl)[1])
            cnt, used, err = self._count_one("target", sch, tbl, method)
            method_counter[used] = method_counter.get(used, 0) + 1
            if used == "skip":
                skipped_count += 1
            if err and cnt is None:
                error_count += 1
            details.append({
                "source_schema": source_schema, "target_schema": target_schema,
                "schema": sch, "table": tbl, "status": "TARGET_ONLY",
                "source_count": None, "target_count": cnt,
                "element_path": f"{sch}.{tbl}",
                "object_type": "TABLE",
                "count_method": used,
                "target_bytes_estimate": _stats(tgt_stats, tbl)[1],
                "error": err,
            })

        for tbl in common_table_names:
            sb = _stats(src_stats, tbl)[1]
            tb = _stats(tgt_stats, tbl)[1]
            method = self._decide_method(tbl, sb, tb)
            if method == "skip":
                skipped_count += 1
                method_counter["skip"] = method_counter.get("skip", 0) + 1
                details.append({
                    "source_schema": source_schema, "target_schema": target_schema,
                    "schema": source_schema, "table": tbl, "status": "SKIPPED",
                    "source_count": None, "target_count": None,
                    "element_path": f"{source_schema}.{tbl}",
                    "object_type": "TABLE",
                    "count_method": "skip",
                    "source_bytes_estimate": sb, "target_bytes_estimate": tb,
                    "error": None,
                })
                continue
            src_cnt, src_used, src_err = self._count_one("source", resolved_src, tbl, method)
            tgt_cnt, tgt_used, tgt_err = self._count_one("target", target_schema, tbl, method)
            method_counter[src_used] = method_counter.get(src_used, 0) + 1
            method_counter[tgt_used] = method_counter.get(tgt_used, 0) + 1
            if src_err and src_cnt is None:
                error_count += 1
            if tgt_err and tgt_cnt is None:
                error_count += 1
            matched = self._is_match(src_cnt, tgt_cnt, (src_used, tgt_used))
            if not matched:
                details.append({
                    "source_schema": source_schema, "target_schema": target_schema,
                    "schema": source_schema, "table": tbl, "status": "MISMATCH",
                    "source_count": src_cnt, "target_count": tgt_cnt,
                    "element_path": f"{source_schema}.{tbl}",
                    "object_type": "TABLE",
                    "count_method": src_used if src_used == tgt_used else f"{src_used}/{tgt_used}",
                    "source_bytes_estimate": sb, "target_bytes_estimate": tb,
                    "error": src_err or tgt_err,
                })

        mismatch_count = sum(1 for d in details if d.get("status") == "MISMATCH")
        passed = (mismatch_count == 0) and (error_count == 0)
        method_summary = ", ".join(f"{k}:{v}" for k, v in sorted(method_counter.items())) or "n/a"
        summary = (
            f"Row counts: {len(common_table_names)} tables compared, "
            f"{mismatch_count} mismatch(es), {skipped_count} skipped, {error_count} error(s); "
            f"methods: {method_summary}."
        )
        return ValidationResult(
            validation_name="row_counts",
            passed=passed,
            summary=summary,
            details=details,
            stats={
                "tables_compared": len(common_table_names),
                "source_only": len(source_only_pairs),
                "target_only": len(target_only_pairs),
                "mismatch_count": mismatch_count,
                "skipped_count": skipped_count,
                "error_count": error_count,
                "methods": method_counter,
                "row_count_mode": (self.options.row_count_mode or "auto").strip().lower(),
                "threshold_bytes": int(self.options.large_table_threshold_bytes or 0),
            },
        )

    def _null_agg_union(self, side: str, schema: str, table: str, col_names: list[str]) -> list[dict[str, Any]]:
        """Run batched NULL aggregate queries; return rows ``schema_name, table_name, column_name, total_rows, non_nulls``."""
        dname = self._source_dialect.name if side == "source" else self._target_dialect.name
        chunk = 40 if dname == "db2" else 80
        try:
            chunk = int(os.environ.get("DV_COL_AGG_COLS_PER_QUERY_DB2" if dname == "db2" else "DV_COL_AGG_COLS_PER_QUERY_AZ", str(chunk)))
        except Exception:
            pass
        if not col_names:
            return []
        unions: list[str] = []
        exec_fn = self._source_execute if side == "source" else self._target_execute
        timeout = self.options.row_count_timeout_seconds
        out: list[dict[str, Any]] = []
        for i in range(0, len(col_names), chunk):
            part = col_names[i : i + chunk]
            if dname == "db2":
                st = str(schema or "").strip()
                tb = str(table or "").strip()
                trep = f"{_quote_db2_ident(st)}.{_quote_db2_ident(tb)}"
                exprs = ["COUNT(*) AS total_rows"]
                for j, cn in enumerate(part):
                    cref = _quote_db2_ident(cn)
                    exprs.append(f"SUM(CASE WHEN {cref} IS NOT NULL THEN 1 ELSE 0 END) AS nn_{j}")
                base = f"(SELECT {', '.join(exprs)} FROM {trep}) base"
                pieces = []
                for j, cn in enumerate(part):
                    pieces.append(
                        f"SELECT '{st.replace(chr(39), chr(39)+chr(39))}' AS schema_name, "
                        f"'{tb.replace(chr(39), chr(39)+chr(39))}' AS table_name, "
                        f"'{cn.replace(chr(39), chr(39)+chr(39))}' AS column_name, "
                        f"total_rows, nn_{j} AS non_nulls FROM {base}"
                    )
                unions.append(" UNION ALL ".join(pieces))
            else:
                st = str(schema or "").strip()
                tb = str(table or "").strip()
                ss = st.replace("]", "]]")
                tt = tb.replace("]", "]]")
                trep = f"[{ss}].[{tt}]"
                exprs = ["COUNT(*) AS total_rows"]
                for j, cn in enumerate(part):
                    cref = _quote_azure_ident(cn)
                    exprs.append(f"SUM(CASE WHEN {cref} IS NOT NULL THEN 1 ELSE 0 END) AS nn_{j}")
                base = f"(SELECT {', '.join(exprs)} FROM {trep}) base"
                pieces = []
                for j, cn in enumerate(part):
                    esc = cn.replace("'", "''")
                    pieces.append(
                        f"SELECT CAST('{st.replace(chr(39), chr(39)+chr(39))}' AS NVARCHAR(256)) AS schema_name, "
                        f"CAST('{tb.replace(chr(39), chr(39)+chr(39))}' AS NVARCHAR(256)) AS table_name, "
                        f"CAST('{esc}' AS NVARCHAR(256)) AS column_name, "
                        f"total_rows, nn_{j} AS non_nulls FROM {base}"
                    )
                unions.append(" UNION ALL ".join(pieces))
        for sql in unions:
            try:
                rows = exec_fn(sql, timeout_seconds=timeout) or []
                out.extend(rows)
            except Exception:
                continue
        return out

    def validate_column_nulls(
        self,
        source_schema: str,
        target_schema: str,
    ) -> ValidationResult:
        """Compare NULL counts per column on matched tables (same pairing as row counts).

        Uses SYSCAT/SYS catalog nullability plus batched ``COUNT`` / ``SUM(CASE…)`` queries.
        Optional: ``DV_NULLCHECK_SKIP_COLUMNS`` (comma substrings), ``DV_NULLCHECK_ONLY_WHEN_ROWCOUNT_MATCHES``.
        """
        object_types = self._catalog_object_types_for_data()
        resolved_src, common = self._pair_common_tables(source_schema, target_schema, object_types)
        src_d = self._source_dialect
        tgt_d = self._target_dialect
        l_all = self._fetch_all_columns("source", resolved_src)
        r_all = self._fetch_all_columns("target", target_schema)
        skip_sub = [s.strip().lower() for s in os.environ.get("DV_NULLCHECK_SKIP_COLUMNS", "").split(",") if s.strip()]
        only_rc = os.environ.get("DV_NULLCHECK_ONLY_WHEN_ROWCOUNT_MATCHES", "0").strip().lower() in ("1", "true", "yes")
        src_stats = self._fetch_table_stats("source", resolved_src)
        tgt_stats = self._fetch_table_stats("target", target_schema)

        details: list[dict[str, Any]] = []
        cols_checked = 0

        def _stats(side_map: dict[str, dict[str, int | None]], tbl: str) -> tuple[int | None, int | None]:
            entry = side_map.get(str(tbl).strip().upper()) or {}
            return entry.get("row_estimate"), entry.get("bytes_estimate")

        for tbl in common:
            if self._excluded(tbl):
                continue
            sb = _stats(src_stats, tbl)[1]
            tb = _stats(tgt_stats, tbl)[1]
            method = self._decide_method(tbl, sb, tb)
            if only_rc:
                sc, su, se = self._count_one("source", resolved_src, tbl, method)
                tc, tu, te = self._count_one("target", target_schema, tbl, method)
                if not self._is_match(sc, tc, (su, tu)):
                    continue
            lmap: dict[str, dict[str, Any]] = {}
            for r in l_all:
                if _norm_upper(r.get("table_name")) != _norm_upper(tbl):
                    continue
                lmap[_norm_upper(r.get("column_name"))] = r
            rmap: dict[str, dict[str, Any]] = {}
            for r in r_all:
                if _norm_upper(r.get("table_name")) != _norm_upper(tbl):
                    continue
                rmap[_norm_upper(r.get("column_name"))] = r
            keys = sorted(set(lmap.keys()) & set(rmap.keys()))
            work: list[tuple[str, str, str]] = []
            for ck in keys:
                low = ck.lower()
                if any(s in low for s in skip_sub):
                    continue
                lr, rr = lmap[ck], rmap[ck]
                ln = _is_nullable_column(lr, src_d.name)
                rn = _is_nullable_column(rr, tgt_d.name)
                if ln != rn:
                    details.append({
                        "source_schema": source_schema,
                        "target_schema": target_schema,
                        "schema": source_schema,
                        "table": tbl,
                        "column": str(_row_get(lr, "column_name")),
                        "status": "METADATA_MISMATCH",
                        "element_path": f"{source_schema}.{tbl}.{_row_get(lr, 'column_name')}",
                        "object_type": "TABLE",
                        "error_code": "NULLABILITY_METADATA_MISMATCH",
                        "error_description": "Nullability differs between catalogs",
                        "source_nullable": ln,
                        "target_nullable": rn,
                    })
                    continue
                if ln and rn:
                    work.append((ck, str(_row_get(lr, "column_name")), str(_row_get(rr, "column_name"))))
            if not work:
                continue
            l_names = [a[1] for a in work]
            r_names = [a[2] for a in work]
            l_agg = self._null_agg_union("source", resolved_src, tbl, l_names)
            r_agg = self._null_agg_union("target", target_schema, tbl, r_names)
            l_by = {_norm_upper(_row_get(r, "column_name")): r for r in l_agg}
            r_by = {_norm_upper(_row_get(r, "column_name")): r for r in r_agg}
            for ck, lcn, rcn in work:
                cols_checked += 1
                lr, rr = l_by.get(ck), r_by.get(ck)
                if not lr or not rr:
                    details.append({
                        "source_schema": source_schema,
                        "target_schema": target_schema,
                        "schema": source_schema,
                        "table": tbl,
                        "column": lcn,
                        "status": "ERROR",
                        "element_path": f"{source_schema}.{tbl}.{lcn}",
                        "object_type": "TABLE",
                        "error_code": "AGG_QUERY_FAILED",
                        "error_description": "Could not compute null counts for column",
                    })
                    continue
                tr_l = _to_int(_row_get(lr, "total_rows"))
                tr_r = _to_int(_row_get(rr, "total_rows"))
                nn_l = _to_int(_row_get(lr, "non_nulls"))
                nn_r = _to_int(_row_get(rr, "non_nulls"))
                if tr_l is None or nn_l is None or tr_r is None or nn_r is None:
                    continue
                null_l, null_r = tr_l - nn_l, tr_r - nn_r
                if null_l != null_r:
                    details.append({
                        "source_schema": source_schema,
                        "target_schema": target_schema,
                        "schema": source_schema,
                        "table": tbl,
                        "column": lcn,
                        "status": "MISMATCH",
                        "element_path": f"{source_schema}.{tbl}.{lcn}",
                        "object_type": "TABLE",
                        "error_code": "NULL_COUNT_MISMATCH",
                        "error_description": "Column-level null count differs",
                        "source_null_count": null_l,
                        "target_null_count": null_r,
                        "source_total_rows": tr_l,
                        "target_total_rows": tr_r,
                    })

        mismatch = sum(1 for d in details if d.get("status") == "MISMATCH")
        meta = sum(1 for d in details if d.get("status") == "METADATA_MISMATCH")
        err = sum(1 for d in details if d.get("status") == "ERROR")
        passed = mismatch == 0 and meta == 0 and err == 0
        summary = (
            f"Column nulls: {len(common)} table(s) scanned, {cols_checked} column pair(s) compared, "
            f"{mismatch} null mismatch(es), {meta} metadata mismatch(es), {err} error(s)."
        )
        return ValidationResult(
            validation_name="column_nulls",
            passed=passed,
            summary=summary,
            details=details,
            stats={"tables": len(common), "columns_checked": cols_checked, "mismatch": mismatch},
        )

    def _distinct_sql(self, side: str, schema: str, table: str, cols_csv: str) -> str:
        d = self._source_dialect if side == "source" else self._target_dialect
        if d.name == "db2":
            s = str(schema or "").strip().upper()
            t = str(table or "").strip().upper()
            parts = [(c or "").strip().upper() for c in cols_csv.split(",") if (c or "").strip()]
            colsql = ", ".join(parts) if parts else "1"
            return (
                f"SELECT (SELECT COUNT(*) FROM {s}.{t}) AS row_count, "
                f"(SELECT COUNT(*) FROM (SELECT DISTINCT {colsql} FROM {s}.{t}) d) AS distinct_count "
                f"FROM SYSIBM.SYSDUMMY1"
            )
        ss = str(schema or "").strip().replace("]", "]]")
        tt = str(table or "").strip().replace("]", "]]")
        parts = [f"[{c.strip()}]" for c in cols_csv.split(",") if c.strip()]
        colsql = ", ".join(parts) if parts else "1"
        return (
            f"SELECT (SELECT COUNT(*) FROM [{ss}].[{tt}]) AS row_count, "
            f"(SELECT COUNT(*) FROM (SELECT DISTINCT {colsql} FROM [{ss}].[{tt}]) d) AS distinct_count"
        )

    def validate_distinct_keys(
        self,
        source_schema: str,
        target_schema: str,
    ) -> ValidationResult:
        """Compare ``COUNT(*)`` vs ``COUNT(DISTINCT pk…)`` on primary-key columns (per legacy distinct-key check)."""
        object_types = ["TABLE"]
        resolved_src, common = self._pair_common_tables(source_schema, target_schema, object_types)
        # Load PK/index catalog once (was N queries — one per table × threads).
        self._get_index_catalog_rows("source", resolved_src)
        self._get_index_catalog_rows("target", target_schema)
        details: list[dict[str, Any]] = []
        workers = max(1, int(os.environ.get("DV_DISTINCT_KEY_WORKERS", str(max(1, self.options.parallel_workers)))))
        try:
            cap_w = int(os.environ.get("DV_MAX_PARALLEL_TABLE_WORKERS", "8"))
        except Exception:
            cap_w = 8
        workers = max(1, min(workers, max(1, cap_w)))
        lock = threading.Lock()

        def _one(tbl: str) -> None:
            if self._excluded(tbl):
                return
            pk_s = self._pk_column_list("source", resolved_src, _norm_upper(tbl))
            pk_t = self._pk_column_list("target", target_schema, _norm_upper(tbl))
            key_cols = ",".join(pk_s or pk_t)
            ep = f"{source_schema}.{tbl}"
            if not key_cols.strip():
                with lock:
                    details.append({
                        "source_schema": source_schema,
                        "target_schema": target_schema,
                        "schema": source_schema,
                        "table": tbl,
                        "status": "ERROR",
                        "element_path": ep,
                        "object_type": "TABLE",
                        "error_code": "KEY_NOT_FOUND",
                        "error_description": "No primary key detected on either side",
                        "details_json": json.dumps({"key_columns": []}),
                    })
                return
            try:
                sql_s = self._distinct_sql("source", resolved_src, tbl, key_cols)
                sql_t = self._distinct_sql("target", target_schema, tbl, key_cols)
                rs = self._source_execute(sql_s)
                rt = self._target_execute(sql_t)
                if not rs or not rt:
                    raise RuntimeError("empty result")
                s_row, t_row = rs[0], rt[0]
                s_rows = _to_int(_row_get(s_row, "row_count")) or 0
                s_dist = _to_int(_row_get(s_row, "distinct_count")) or 0
                t_rows = _to_int(_row_get(t_row, "row_count")) or 0
                t_dist = _to_int(_row_get(t_row, "distinct_count")) or 0
            except Exception as ex:
                with lock:
                    details.append({
                        "source_schema": source_schema,
                        "target_schema": target_schema,
                        "schema": source_schema,
                        "table": tbl,
                        "status": "ERROR",
                        "element_path": ep,
                        "object_type": "TABLE",
                        "error_code": "COUNT_FAILED",
                        "error_description": f"Count failed: {ex}",
                        "details_json": json.dumps({"key_columns": key_cols.split(",")}),
                    })
                return
            dj_base = {"key_columns": key_cols.split(",")}
            rows_to_add: list[dict[str, Any]] = []
            if s_dist < s_rows:
                rows_to_add.append({
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": source_schema,
                    "table": tbl,
                    "status": "MISMATCH",
                    "element_path": ep,
                    "object_type": "TABLE",
                    "error_code": "DUPLICATES_IN_SOURCE",
                    "error_description": "Duplicates detected in source on key",
                    "details_json": json.dumps({**dj_base, "source_row_count": s_rows, "source_distinct_key_count": s_dist}),
                })
            if t_dist < t_rows:
                rows_to_add.append({
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": source_schema,
                    "table": tbl,
                    "status": "MISMATCH",
                    "element_path": ep,
                    "object_type": "TABLE",
                    "error_code": "DUPLICATES_IN_TARGET",
                    "error_description": "Duplicates detected in target on key",
                    "details_json": json.dumps({**dj_base, "target_row_count": t_rows, "target_distinct_key_count": t_dist}),
                })
            if s_dist != t_dist:
                rows_to_add.append({
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": source_schema,
                    "table": tbl,
                    "status": "MISMATCH",
                    "element_path": ep,
                    "object_type": "TABLE",
                    "error_code": "DISTINCT_COUNT_MISMATCH",
                    "error_description": "Distinct key count differs between source and target",
                    "details_json": json.dumps({
                        **dj_base,
                        "source_distinct_key_count": s_dist,
                        "target_distinct_key_count": t_dist,
                        "source_row_count": s_rows,
                        "target_row_count": t_rows,
                    }),
                })
            if rows_to_add:
                with lock:
                    details.extend(rows_to_add)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(_one, common))

        bad = sum(1 for d in details if d.get("status") != "OK")
        passed = bad == 0
        return ValidationResult(
            validation_name="distinct_keys",
            passed=passed,
            summary=f"Distinct keys: {len(common)} table(s); {len(details)} issue row(s).",
            details=details,
            stats={"tables": len(common), "issue_rows": len(details)},
        )

    def _checksum_mode(self) -> str:
        e = os.environ.get("DV_CHECKSUM_MODE", "").strip().lower()
        if e in ("aggregate", "row_hash"):
            return e
        m = (self.options.checksum_mode or "aggregate").strip().lower()
        return m if m in ("aggregate", "row_hash") else "aggregate"

    def _checksum_row_cap(self) -> int:
        try:
            return int(os.environ.get("DV_CHECKSUM_ROW_CAP", str(self.options.checksum_row_cap)))
        except Exception:
            return max(1, int(self.options.checksum_row_cap))

    def _checksum_max_mismatches(self) -> int:
        try:
            return max(1, int(os.environ.get("DV_CHECKSUM_MAX_MISMATCHES", str(self.options.checksum_max_mismatches))))
        except Exception:
            return max(1, int(self.options.checksum_max_mismatches))

    def _hash_map_from_rows(self, rows: list[dict[str, Any]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for r in rows or []:
            k = _row_get(r, "KeySig", "keysig")
            h = _row_get(r, "RowHash", "rowhash")
            if k is None:
                continue
            out[str(k)] = str(h) if h is not None else ""
        return out

    def _wrap_row_checksum_sql(self, dialect_name: str, inner_sql: str, cap: int) -> str:
        if cap <= 0:
            return inner_sql
        if dialect_name == "db2":
            return f"SELECT * FROM ({inner_sql}) AS _chk FETCH FIRST {cap} ROWS ONLY"
        # azure_sql
        return f"SELECT TOP ({cap}) * FROM ({inner_sql}) AS _chk"

    def validate_checksum(
        self,
        source_schema: str,
        target_schema: str,
    ) -> ValidationResult:
        """Compare checksums: ``aggregate`` (``CHECKSUM_AGG``) or ``row_hash`` (per-key row fingerprints).

        Mode: :attr:`ValidationOptions.checksum_mode` or env ``DV_CHECKSUM_MODE`` (``aggregate`` | ``row_hash``).
        Row mode caps: ``checksum_row_cap`` / ``DV_CHECKSUM_ROW_CAP``; mismatches capped by
        ``checksum_max_mismatches`` / ``DV_CHECKSUM_MAX_MISMATCHES``.
        """
        mode = self._checksum_mode()
        if mode == "row_hash":
            return self._validate_checksum_row_hash(source_schema, target_schema)

        object_types = ["TABLE"]
        resolved_src, common = self._pair_common_tables(source_schema, target_schema, object_types)
        details: list[dict[str, Any]] = []
        skip_types = {"CLOB", "BLOB", "XML", "IMAGE", "TEXT", "NTEXT"}

        l_all = self._fetch_all_columns("source", resolved_src)
        r_all = self._fetch_all_columns("target", target_schema)
        l_by_tbl: dict[str, list[dict[str, Any]]] = defaultdict(list)
        r_by_tbl: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in l_all:
            l_by_tbl[_norm_upper(r.get("table_name"))].append(r)
        for r in r_all:
            r_by_tbl[_norm_upper(r.get("table_name"))].append(r)

        for tbl in common:
            if self._excluded(tbl):
                continue
            pk_s = set(x.upper() for x in self._pk_column_list("source", resolved_src, _norm_upper(tbl)))
            l_rows = l_by_tbl.get(_norm_upper(tbl), [])
            r_rows = r_by_tbl.get(_norm_upper(tbl), [])
            l_by = {_norm_upper(r.get("column_name")): r for r in l_rows}
            r_by = {_norm_upper(r.get("column_name")): r for r in r_rows}
            shared = sorted(set(l_by.keys()) & set(r_by.keys()))
            cols: list[str] = []
            for ck in shared:
                if ck in pk_s:
                    continue
                lt = _norm_upper(_row_get(l_by[ck], "data_type"))
                rt = _norm_upper(_row_get(r_by[ck], "data_type"))
                if any(x in lt for x in skip_types) or any(x in rt for x in skip_types):
                    continue
                cols.append(str(_row_get(l_by[ck], "column_name") or "").strip())
            cols = cols[:32]
            if not cols:
                continue
            src_d, tgt_d = self._source_dialect, self._target_dialect
            try:
                qs = src_d.checksum_query(resolved_src, tbl, cols)
                qt = tgt_d.checksum_query(target_schema, tbl, cols)
            except Exception:
                continue
            if not qs or not qt:
                continue
            try:
                vs = self._source_execute(qs)
                vt = self._target_execute(qt)
                if not vs or not vt:
                    continue
                a = next(iter(vs[0].values()))
                b = next(iter(vt[0].values()))
            except Exception as ex:
                details.append({
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": source_schema,
                    "table": tbl,
                    "status": "ERROR",
                    "element_path": f"{source_schema}.{tbl}",
                    "object_type": "TABLE",
                    "error_code": "CHECKSUM_FAILED",
                    "error_description": str(ex),
                    "details_json": json.dumps({"columns": cols}),
                })
                continue
            if str(a) != str(b):
                details.append({
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": source_schema,
                    "table": tbl,
                    "status": "MISMATCH",
                    "element_path": f"{source_schema}.{tbl}",
                    "object_type": "TABLE",
                    "error_code": "AGG_CHECKSUM_MISMATCH",
                    "error_description": "Aggregate checksum differs",
                    "details_json": json.dumps({"columns": cols, "source_checksum": str(a), "target_checksum": str(b)}),
                })

        passed = len(details) == 0
        return ValidationResult(
            validation_name="checksum",
            passed=passed,
            summary=f"Checksum (aggregate): {len(common)} table(s) scanned; {len(details)} issue(s).",
            details=details,
            stats={"tables": len(common), "checksum_mode": "aggregate"},
        )

    def _validate_checksum_row_hash(
        self,
        source_schema: str,
        target_schema: str,
    ) -> ValidationResult:
        """Per-row key + value hash comparison (legacy-style), with row cap and mismatch cap."""
        object_types = ["TABLE"]
        resolved_src, common = self._pair_common_tables(source_schema, target_schema, object_types)
        details: list[dict[str, Any]] = []
        skip_types = {"CLOB", "BLOB", "XML", "IMAGE", "TEXT", "NTEXT"}
        cap = self._checksum_row_cap()
        max_bad = self._checksum_max_mismatches()
        src_d, tgt_d = self._source_dialect, self._target_dialect

        l_all = self._fetch_all_columns("source", resolved_src)
        r_all = self._fetch_all_columns("target", target_schema)
        l_by_tbl: dict[str, list[dict[str, Any]]] = defaultdict(list)
        r_by_tbl: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in l_all:
            l_by_tbl[_norm_upper(r.get("table_name"))].append(r)
        for r in r_all:
            r_by_tbl[_norm_upper(r.get("table_name"))].append(r)

        for tbl in common:
            if self._excluded(tbl):
                continue
            pk_src = self._pk_column_list("source", resolved_src, _norm_upper(tbl))
            pk_tgt = self._pk_column_list("target", target_schema, _norm_upper(tbl))
            key_cols = pk_src or pk_tgt
            if not key_cols:
                details.append({
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": source_schema,
                    "table": tbl,
                    "status": "SKIP",
                    "element_path": f"{source_schema}.{tbl}",
                    "object_type": "TABLE",
                    "error_code": "ROW_HASH_NO_PK",
                    "error_description": "Row-hash checksum requires a primary key; skipped.",
                    "details_json": json.dumps({}),
                })
                continue

            l_rows = l_by_tbl.get(_norm_upper(tbl), [])
            r_rows = r_by_tbl.get(_norm_upper(tbl), [])
            l_by = {_norm_upper(r.get("column_name")): r for r in l_rows}
            r_by = {_norm_upper(r.get("column_name")): r for r in r_rows}
            pk_u = {_norm_upper(x) for x in key_cols}
            shared = sorted(set(l_by.keys()) & set(r_by.keys()))
            val_cols: list[str] = []
            for ck in shared:
                if ck in pk_u:
                    continue
                lt = _norm_upper(_row_get(l_by[ck], "data_type"))
                rt = _norm_upper(_row_get(r_by[ck], "data_type"))
                if any(x in lt for x in skip_types) or any(x in rt for x in skip_types):
                    continue
                val_cols.append(str(_row_get(l_by[ck], "column_name") or "").strip())
            val_cols = val_cols[:32]

            inner_s = src_d.checksum_row_fingerprint_query(resolved_src, tbl, key_cols, val_cols)
            inner_t = tgt_d.checksum_row_fingerprint_query(target_schema, tbl, key_cols, val_cols)
            if not inner_s or not inner_t:
                details.append({
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": source_schema,
                    "table": tbl,
                    "status": "ERROR",
                    "element_path": f"{source_schema}.{tbl}",
                    "object_type": "TABLE",
                    "error_code": "ROW_HASH_SQL_UNAVAILABLE",
                    "error_description": "Dialect does not support row fingerprint SQL.",
                    "details_json": json.dumps({"key_columns": key_cols}),
                })
                continue

            sql_s = self._wrap_row_checksum_sql(src_d.name, inner_s, cap)
            sql_t = self._wrap_row_checksum_sql(tgt_d.name, inner_t, cap)

            try:
                rs = self._source_execute(sql_s)
                rt = self._target_execute(sql_t)
            except Exception as ex:
                details.append({
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": source_schema,
                    "table": tbl,
                    "status": "ERROR",
                    "element_path": f"{source_schema}.{tbl}",
                    "object_type": "TABLE",
                    "error_code": "ROW_HASH_QUERY_FAILED",
                    "error_description": str(ex),
                    "details_json": json.dumps({"key_columns": key_cols, "value_columns": val_cols}),
                })
                continue

            ms = self._hash_map_from_rows(rs)
            mt = self._hash_map_from_rows(rt)
            emitted = 0

            for k in sorted(set(ms.keys()) - set(mt.keys())):
                if emitted >= max_bad:
                    break
                emitted += 1
                details.append({
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": source_schema,
                    "table": tbl,
                    "status": "MISMATCH",
                    "element_path": f"{source_schema}.{tbl}",
                    "object_type": "TABLE",
                    "error_code": "MISSING_IN_TARGET",
                    "error_description": "Key present only in source (row checksum)",
                    "details_json": json.dumps({"key_columns": key_cols, "key": k}),
                })
            for k in sorted(set(mt.keys()) - set(ms.keys())):
                if emitted >= max_bad:
                    break
                emitted += 1
                details.append({
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema": source_schema,
                    "table": tbl,
                    "status": "MISMATCH",
                    "element_path": f"{source_schema}.{tbl}",
                    "object_type": "TABLE",
                    "error_code": "MISSING_IN_SOURCE",
                    "error_description": "Key present only in target (row checksum)",
                    "details_json": json.dumps({"key_columns": key_cols, "key": k}),
                })
            for k in sorted(set(ms.keys()) & set(mt.keys())):
                if emitted >= max_bad:
                    break
                if ms[k] != mt[k]:
                    emitted += 1
                    details.append({
                        "source_schema": source_schema,
                        "target_schema": target_schema,
                        "schema": source_schema,
                        "table": tbl,
                        "status": "MISMATCH",
                        "element_path": f"{source_schema}.{tbl}",
                        "object_type": "TABLE",
                        "error_code": "ROW_HASH_MISMATCH",
                        "error_description": "Row hash mismatch",
                        "details_json": json.dumps({"key_columns": key_cols, "key": k}),
                    })

        skip_n = sum(1 for d in details if d.get("status") == "SKIP")
        passed = not any(d.get("status") in ("MISMATCH", "ERROR") for d in details)
        summary = (
            f"Checksum (row_hash): {len(common)} table(s); cap={cap}; "
            f"{len(details)} detail row(s) ({skip_n} skipped without PK)."
        )
        return ValidationResult(
            validation_name="checksum",
            passed=passed,
            summary=summary,
            details=details,
            stats={
                "tables": len(common),
                "checksum_mode": "row_hash",
                "row_cap": cap,
                "max_mismatches": max_bad,
            },
        )

    def _fk_constraint_groups(self, side: str, schema: str) -> list[dict[str, Any]]:
        d = self._source_dialect if side == "source" else self._target_dialect
        fq = d.catalog_fk_query(schema)
        cq = getattr(d, "catalog_fk_columns_query", lambda _s: None)(schema)
        exec_fn = self._source_execute if side == "source" else self._target_execute
        try:
            heads = exec_fn(fq) or []
            cols = exec_fn(cq) if cq else []
        except Exception:
            return []
        col_by_fk: dict[tuple[str, str, str], list[tuple[int, str, str]]] = defaultdict(list)
        for r in cols:
            k = (_norm_upper(r.get("schema_name")), _norm_upper(r.get("table_name")), _norm_upper(r.get("fk_name")))
            seq = _to_int(r.get("col_seq")) or 0
            fk_c = str(_row_get(r, "fk_column") or "").strip()
            pk_c = str(_row_get(r, "pk_column") or "").strip()
            col_by_fk[(k[0], k[1], k[2])].append((seq, fk_c, pk_c))
        groups: list[dict[str, Any]] = []
        for r in heads:
            sch = str(_row_get(r, "schema_name") or "").strip()
            tb = str(_row_get(r, "table_name") or "").strip()
            fk = str(_row_get(r, "fk_name") or "").strip()
            ref_s = str(_row_get(r, "ref_schema") or "").strip()
            ref_t = str(_row_get(r, "ref_table") or "").strip()
            key = (_norm_upper(sch), _norm_upper(tb), _norm_upper(fk))
            pairs = [p[1:] for p in sorted(col_by_fk.get(key, []), key=lambda x: x[0])]
            if not pairs:
                continue
            groups.append({
                "side": side,
                "child_schema": sch,
                "child_table": tb,
                "fk_name": fk,
                "parent_schema": ref_s,
                "parent_table": ref_t,
                "pairs": pairs,
            })
        return groups

    def _ref_int_sql(self, g: dict[str, Any], sample_limit: int) -> tuple[str, str]:
        """Return (count_sql, sample_sql) for one FK group on its engine."""
        side = g["side"]
        child_cols = [c for c, _ in g["pairs"]]
        parent_cols = [p for _, p in g["pairs"]]
        if side == "target":
            nn_c = [f"c.{_quote_azure_ident(cc)} IS NOT NULL" for cc in child_cols]
            non_null = " AND ".join(nn_c) if nn_c else "1=1"
        else:
            nn_c = [f"c.{(cc or '').upper()} IS NOT NULL" for cc in child_cols]
            non_null = " AND ".join(nn_c) if nn_c else "1=1"
        if side == "target":
            cs, ct = g["child_schema"], g["child_table"]
            ps, pt = g["parent_schema"], g["parent_table"]

            def qc(alias: str, c: str) -> str:
                return f"{alias}.{_quote_azure_ident(c)}"

            def norm(alias: str, c: str) -> str:
                return f"LOWER(LTRIM(RTRIM({qc(alias, c)})))"

            join_cond = " AND ".join([f"{norm('c', cc)} = {norm('p', pp)}" for cc, pp in zip(child_cols, parent_cols)]) or "1=1"
            tbl_c = f"[{str(cs).replace(']', ']]')}].[{str(ct).replace(']', ']]')}]"
            tbl_p = f"[{str(ps).replace(']', ']]')}].[{str(pt).replace(']', ']]')}]"
            parent_probe = parent_cols[0] if parent_cols else None
            if parent_probe:
                where_orphan = f"p.{_quote_azure_ident(parent_probe)} IS NULL"
            else:
                where_orphan = "1=1"
            count_sql = (
                f"SELECT COUNT(*) AS cnt FROM {tbl_c} c LEFT JOIN {tbl_p} p ON {join_cond} "
                f"WHERE ({non_null}) AND ({where_orphan})"
            )
            parts: list[str] = []
            for idx, nm in enumerate(child_cols):
                if idx:
                    parts.append("'|'")
                parts.append(norm("c", nm))
            key_expr = "CONCAT(" + ", ".join(parts) + ")" if parts else "''"
            sample_sql = (
                f"SELECT TOP {sample_limit} {key_expr} AS KeySig FROM {tbl_c} c LEFT JOIN {tbl_p} p ON {join_cond} "
                f"WHERE ({non_null}) AND ({where_orphan})"
            )
            return count_sql, sample_sql

        # DB2 source-side
        cs, ct = g["child_schema"], g["child_table"]
        ps, pt = g["parent_schema"], g["parent_table"]
        csu, ctu = str(cs).upper(), str(ct).upper()
        psu, ptu = str(ps).upper(), str(pt).upper()

        def qc(_a: str, c: str) -> str:
            return f"c.{(c or '').upper()}"

        def norm(_a: str, c: str) -> str:
            return f"LOWER(TRIM({qc('c', c)}))"

        join_cond = " AND ".join(
            [f"LOWER(TRIM(c.{cc.upper()})) = LOWER(TRIM(p.{pp.upper()}))" for cc, pp in zip(child_cols, parent_cols)]
        ) or "1=1"
        non_null_d = " AND ".join([f"c.{cc.upper()} IS NOT NULL" for cc in child_cols]) or "1=1"
        tbl_c = f"{csu}.{ctu}"
        tbl_p = f"{psu}.{ptu}"
        parent_probe = (parent_cols[0] or "").upper() if parent_cols else None
        where_orphan = f"p.{parent_probe} IS NULL" if parent_probe else "1=1"
        count_sql = (
            f"SELECT COUNT(*) AS cnt FROM {tbl_c} c LEFT JOIN {tbl_p} p ON {join_cond} "
            f"WHERE ({non_null_d}) AND ({where_orphan})"
        )
        keys = " || '|' || ".join([f"COALESCE(LOWER(TRIM(c.{cc.upper()})), '<NULL>')" for cc in child_cols]) if child_cols else "''"
        sample_sql = f"SELECT {keys} AS KeySig FROM {tbl_c} c LEFT JOIN {tbl_p} p ON {join_cond} WHERE ({non_null_d}) AND ({where_orphan}) FETCH FIRST {sample_limit} ROWS ONLY"
        return count_sql, sample_sql

    def validate_referential_integrity(
        self,
        source_schema: str,
        target_schema: str,
    ) -> ValidationResult:
        """Detect child FK rows with no matching parent (LEFT JOIN pushdown, legacy-compatible)."""
        resolved_src, _common = self._pair_common_tables(source_schema, target_schema, ["TABLE"])
        pair_lookup = {}
        for tb in _common:
            pair_lookup[(_norm_upper(resolved_src), _norm_upper(tb))] = (source_schema, tb, target_schema, tb)

        groups = self._fk_constraint_groups("source", resolved_src) + self._fk_constraint_groups("target", target_schema)
        sample_limit = max(1, int(os.environ.get("DV_REFINT_SAMPLE_LIMIT", "10")))
        workers = max(1, int(os.environ.get("DV_BROKEN_FK_WORKERS", str(max(1, self.options.parallel_workers)))))
        try:
            cap_w = int(os.environ.get("DV_MAX_PARALLEL_TABLE_WORKERS", "8"))
        except Exception:
            cap_w = 8
        workers = max(1, min(workers, max(1, cap_w)))
        details: list[dict[str, Any]] = []
        ref_lock = threading.Lock()

        def _run(g: dict[str, Any]) -> None:
            if os.environ.get("DV_REFINT_DISABLE_PUSHDOWN", "0").strip().lower() in ("1", "true", "yes"):
                return
            try:
                cnt_sql, samp_sql = self._ref_int_sql(g, sample_limit)
                side = g["side"]
                if side == "source":
                    n = self._exec_scalar_int("source", cnt_sql) or 0
                    samples = []
                    if n > 0:
                        rows = self._source_execute(samp_sql) or []
                        samples = [next(iter(r.values())) for r in rows[:sample_limit]]
                else:
                    n = self._exec_scalar_int("target", cnt_sql) or 0
                    samples = []
                    if n > 0:
                        rows = self._target_execute(samp_sql) or []
                        samples = [next(iter(r.values())) for r in rows[:sample_limit]]
                if n <= 0:
                    return
                cn, ct = g["child_schema"], g["child_table"]
                names = pair_lookup.get((_norm_upper(cn), _norm_upper(ct)))
                if names:
                    s_schema, s_table, d_schema, d_table = names
                else:
                    s_schema, s_table, d_schema, d_table = cn, ct, cn, ct
                err = "REF_INTEGRITY_IN_TARGET" if side == "target" else "REF_INTEGRITY_IN_SOURCE"
                with ref_lock:
                    details.append({
                        "source_schema": s_schema,
                        "target_schema": d_schema,
                        "schema": s_schema,
                        "table": s_table,
                        "status": "MISMATCH",
                        "element_path": f"{cn}.{ct}.{g.get('fk_name','')}",
                        "object_type": "TABLE",
                        "error_code": err,
                        "error_description": f"Child rows without matching parent: {n}",
                        "details_json": json.dumps({
                            "fk_name": g["fk_name"],
                            "child_schema": cn,
                            "child_table": ct,
                            "parent_schema": g["parent_schema"],
                            "parent_table": g["parent_table"],
                            "fk_columns": [p[0] for p in g["pairs"]],
                            "ref_columns": [p[1] for p in g["pairs"]],
                            "broken_row_count": n,
                            "sample_child_keys": samples,
                        }),
                    })
            except Exception:
                return

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_run, g) for g in groups]
            for f in as_completed(futs):
                f.result()

        passed = len(details) == 0
        return ValidationResult(
            validation_name="referential_integrity",
            passed=passed,
            summary=f"Referential integrity: {len(groups)} FK group(s) checked; {len(details)} issue(s).",
            details=details,
            stats={"fk_groups": len(groups)},
        )

    def validate_constraint_integrity(
        self,
        source_schema: str,
        target_schema: str,
    ) -> ValidationResult:
        """NOT NULL / CHECK / length catalog violations on matched tables (subset of legacy pushdown)."""
        if os.environ.get("DV_CI_DISABLE_PUSHDOWN", "0").strip().lower() in ("1", "true", "yes"):
            return ValidationResult(
                validation_name="constraint_integrity",
                passed=True,
                summary="Constraint integrity: disabled via DV_CI_DISABLE_PUSHDOWN.",
                details=[],
                stats={},
            )
        resolved_src, common = self._pair_common_tables(source_schema, target_schema, ["TABLE"])
        src_d, tgt_d = self._source_dialect, self._target_dialect
        l_all = self._fetch_all_columns("source", resolved_src)
        r_all = self._fetch_all_columns("target", target_schema)

        def checks_for(schema: str, side: str) -> list[dict[str, Any]]:
            d = self._source_dialect if side == "source" else self._target_dialect
            q = d.catalog_check_constraints_query(schema)
            exec_fn = self._source_execute if side == "source" else self._target_execute
            try:
                return exec_fn(q) or []
            except Exception:
                return []

        details: list[dict[str, Any]] = []
        ci_lock = threading.Lock()

        def process_table(tbl: str) -> None:
            if self._excluded(tbl):
                return
            lmap = { _norm_upper(r.get("column_name")): r for r in l_all if _norm_upper(r.get("table_name")) == _norm_upper(tbl)}
            rmap = { _norm_upper(r.get("column_name")): r for r in r_all if _norm_upper(r.get("table_name")) == _norm_upper(tbl)}
            # NOT NULL
            for lr in lmap.values():
                if _is_nullable_column(lr, src_d.name):
                    continue
                cn = str(_row_get(lr, "column_name") or "").strip()
                sql = f"SELECT COUNT(*) AS cnt FROM {_quote_db2_ident(str(resolved_src))}.{_quote_db2_ident(tbl)} WHERE {_quote_db2_ident(cn)} IS NULL"
                n = self._exec_scalar_int("source", sql)
                if n and n > 0:
                    with ci_lock:
                        details.append({
                            "source_schema": source_schema,
                            "target_schema": target_schema,
                            "schema": source_schema,
                            "table": tbl,
                            "status": "MISMATCH",
                            "element_path": f"{source_schema}.{tbl}.{cn}",
                            "object_type": "TABLE",
                            "error_code": "NOT_NULL_VIOLATION_IN_SOURCE",
                            "error_description": f"Non-nullable column has NULLs: {n}",
                            "details_json": json.dumps({"column_name": cn}),
                        })
            for rr in rmap.values():
                if _is_nullable_column(rr, tgt_d.name):
                    continue
                cn = str(_row_get(rr, "column_name") or "").strip()
                ss, tt = str(target_schema).replace("]", "]]"), str(tbl).replace("]", "]]")
                sql = f"SELECT COUNT(*) AS cnt FROM [{ss}].[{tt}] WHERE {_quote_azure_ident(cn)} IS NULL"
                n = self._exec_scalar_int("target", sql)
                if n and n > 0:
                    with ci_lock:
                        details.append({
                            "source_schema": source_schema,
                            "target_schema": target_schema,
                            "schema": source_schema,
                            "table": tbl,
                            "status": "MISMATCH",
                            "element_path": f"{target_schema}.{tbl}.{cn}",
                            "object_type": "TABLE",
                            "error_code": "NOT_NULL_VIOLATION_IN_TARGET",
                            "error_description": f"Non-nullable column has NULLs: {n}",
                            "details_json": json.dumps({"column_name": cn}),
                        })
            # CHECK constraints (best-effort: engines may reject arbitrary expressions)
            for r in checks_for(resolved_src, "source"):
                if _norm_upper(r.get("table_name")) != _norm_upper(tbl):
                    continue
                cname = str(_row_get(r, "constraint_name") or "").strip()
                expr = str(_row_get(r, "check_clause") or "").strip()
                if not expr:
                    continue
                su, tu = str(resolved_src).upper(), str(tbl).upper()
                sql = f"SELECT COUNT(*) AS cnt FROM {su}.{tu} WHERE NOT ({expr.rstrip(';')})"
                n = self._exec_scalar_int("source", sql)
                if n and n > 0:
                    with ci_lock:
                        details.append({
                            "source_schema": source_schema,
                            "target_schema": target_schema,
                            "schema": source_schema,
                            "table": tbl,
                            "status": "MISMATCH",
                            "element_path": f"{source_schema}.{tbl}.{cname}",
                            "object_type": "TABLE",
                            "error_code": "CHECK_VIOLATION_IN_SOURCE",
                            "error_description": f"Check constraint violated: {n}",
                            "details_json": json.dumps({"constraint_name": cname, "expression": expr}),
                        })
            for r in checks_for(target_schema, "target"):
                if _norm_upper(r.get("table_name")) != _norm_upper(tbl):
                    continue
                cname = str(_row_get(r, "constraint_name") or "").strip()
                expr = str(_row_get(r, "check_clause") or "").strip()
                if not expr:
                    continue
                ss, tt = str(target_schema).replace("]", "]]"), str(tbl).replace("]", "]]")
                sql = f"SELECT COUNT(*) AS cnt FROM [{ss}].[{tt}] WHERE NOT ({expr.rstrip(';')})"
                n = self._exec_scalar_int("target", sql)
                if n and n > 0:
                    with ci_lock:
                        details.append({
                            "source_schema": source_schema,
                            "target_schema": target_schema,
                            "schema": source_schema,
                            "table": tbl,
                            "status": "MISMATCH",
                            "element_path": f"{target_schema}.{tbl}.{cname}",
                            "object_type": "TABLE",
                            "error_code": "CHECK_VIOLATION_IN_TARGET",
                            "error_description": f"Check constraint violated: {n}",
                            "details_json": json.dumps({"constraint_name": cname, "expression": expr}),
                        })
            # Length exceeded (character types)
            for lr in lmap.values():
                lim = _char_length_cap(lr, src_d.name)
                if not lim or lim <= 0:
                    continue
                cn = str(_row_get(lr, "column_name") or "").strip()
                sql = f"SELECT COUNT(*) AS cnt FROM {_quote_db2_ident(str(resolved_src))}.{_quote_db2_ident(tbl)} WHERE LENGTH({_quote_db2_ident(cn)}) > {lim}"
                n = self._exec_scalar_int("source", sql)
                if n and n > 0:
                    with ci_lock:
                        details.append({
                            "source_schema": source_schema,
                            "target_schema": target_schema,
                            "schema": source_schema,
                            "table": tbl,
                            "status": "MISMATCH",
                            "element_path": f"{source_schema}.{tbl}.{cn}",
                            "object_type": "TABLE",
                            "error_code": "LENGTH_EXCEEDED_IN_SOURCE",
                            "error_description": f"Values exceed length {lim}: {n}",
                            "details_json": json.dumps({"column_name": cn, "max_length": lim}),
                        })
            for rr in rmap.values():
                lim = _char_length_cap(rr, tgt_d.name)
                if not lim or lim <= 0:
                    continue
                cn = str(_row_get(rr, "column_name") or "").strip()
                ss, tt = str(target_schema).replace("]", "]]"), str(tbl).replace("]", "]]")
                sql = f"SELECT COUNT(*) AS cnt FROM [{ss}].[{tt}] WHERE LEN({_quote_azure_ident(cn)}) > {lim}"
                n = self._exec_scalar_int("target", sql)
                if n and n > 0:
                    with ci_lock:
                        details.append({
                            "source_schema": source_schema,
                            "target_schema": target_schema,
                            "schema": source_schema,
                            "table": tbl,
                            "status": "MISMATCH",
                            "element_path": f"{target_schema}.{tbl}.{cn}",
                            "object_type": "TABLE",
                            "error_code": "LENGTH_EXCEEDED_IN_TARGET",
                            "error_description": f"Values exceed length {lim}: {n}",
                            "details_json": json.dumps({"column_name": cn, "max_length": lim}),
                        })

        workers = max(1, self.options.parallel_workers)
        try:
            cap_w = int(os.environ.get("DV_MAX_PARALLEL_TABLE_WORKERS", "8"))
        except Exception:
            cap_w = 8
        workers = max(1, min(workers, max(1, cap_w)))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(process_table, common))

        passed = len(details) == 0
        return ValidationResult(
            validation_name="constraint_integrity",
            passed=passed,
            summary=f"Constraint integrity: {len(common)} table(s); {len(details)} issue(s).",
            details=details,
            stats={"tables": len(common)},
        )

    def run_all(
        self,
        source_schema: str,
        target_schema: str,
        object_types: list[str] | None = None,
    ) -> dict[str, ValidationResult]:
        """Run selected data validations (see ``ValidationOptions.data_validation_phases`` / ``DV_DATA_VALIDATIONS``).

        Default is ``row_counts`` only. Each result's ``stats`` includes ``elapsed_seconds`` for that phase.
        """
        object_types = object_types or self.options.object_types
        enabled = frozenset(
            resolve_data_validation_phases(
                os.environ.get("DV_DATA_VALIDATIONS"),
                self.options.data_validation_phases,
            )
        )
        phases: list[tuple[str, Callable[[], ValidationResult]]] = [
            ("row_counts", lambda: self.validate_row_counts(source_schema, target_schema, object_types)),
            ("column_nulls", lambda: self.validate_column_nulls(source_schema, target_schema)),
            ("distinct_keys", lambda: self.validate_distinct_keys(source_schema, target_schema)),
            ("checksum", lambda: self.validate_checksum(source_schema, target_schema)),
            ("referential_integrity", lambda: self.validate_referential_integrity(source_schema, target_schema)),
            ("constraint_integrity", lambda: self.validate_constraint_integrity(source_schema, target_schema)),
        ]
        out: dict[str, ValidationResult] = {}
        for key, fn in phases:
            if key not in enabled:
                continue
            t0 = time.monotonic()
            r = fn()
            dt = time.monotonic() - t0
            out[key] = replace(r, stats={**(r.stats or {}), "elapsed_seconds": round(dt, 3)})
        return out
