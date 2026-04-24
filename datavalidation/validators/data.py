"""
Data validations: row counts, column nulls, distinct keys, checksum, referential integrity, constraint integrity.
"""
from typing import Any

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


class DataValidator(BaseValidator):
    """Runs all data-level validations."""

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

    def validate_column_nulls(
        self,
        source_schema: str,
        target_schema: str,
    ) -> ValidationResult:
        """Compare null/empty counts per column. Stub."""
        return ValidationResult(
            validation_name="column_nulls",
            passed=True,
            summary="Column null check: not yet implemented (stub).",
            details=[],
            stats={},
        )

    def validate_distinct_keys(
        self,
        source_schema: str,
        target_schema: str,
    ) -> ValidationResult:
        """Distinct key consistency. Stub."""
        return ValidationResult(
            validation_name="distinct_keys",
            passed=True,
            summary="Distinct key check: not yet implemented (stub).",
            details=[],
            stats={},
        )

    def validate_checksum(
        self,
        source_schema: str,
        target_schema: str,
    ) -> ValidationResult:
        """Per-table checksum comparison. Stub (requires column list per table)."""
        return ValidationResult(
            validation_name="checksum",
            passed=True,
            summary="Checksum: not yet implemented (stub).",
            details=[],
            stats={},
        )

    def validate_referential_integrity(
        self,
        source_schema: str,
        target_schema: str,
    ) -> ValidationResult:
        """FK values exist in parent. Stub."""
        return ValidationResult(
            validation_name="referential_integrity",
            passed=True,
            summary="Referential integrity: not yet implemented (stub).",
            details=[],
            stats={},
        )

    def validate_constraint_integrity(
        self,
        source_schema: str,
        target_schema: str,
    ) -> ValidationResult:
        """Data satisfies NOT NULL, CHECK. Stub."""
        return ValidationResult(
            validation_name="constraint_integrity",
            passed=True,
            summary="Constraint integrity: not yet implemented (stub).",
            details=[],
            stats={},
        )

    def run_all(
        self,
        source_schema: str,
        target_schema: str,
        object_types: list[str] | None = None,
    ) -> dict[str, ValidationResult]:
        """Run all data validations."""
        object_types = object_types or self.options.object_types
        return {
            "row_counts": self.validate_row_counts(source_schema, target_schema, object_types),
            "column_nulls": self.validate_column_nulls(source_schema, target_schema),
            "distinct_keys": self.validate_distinct_keys(source_schema, target_schema),
            "checksum": self.validate_checksum(source_schema, target_schema),
            "referential_integrity": self.validate_referential_integrity(source_schema, target_schema),
            "constraint_integrity": self.validate_constraint_integrity(source_schema, target_schema),
        }
