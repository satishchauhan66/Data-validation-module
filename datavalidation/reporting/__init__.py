"""
Legacy unified report spec and comparison helpers ported from the original FastAPI / PySpark service:
``db2-to-azure-migration-validation`` (``app/schemas/common.py``, ``app/services/pyspark_schema_comparison.py``).
"""
from datavalidation.reporting.unified_spec import UNIFIED_REPORT_COLUMNS, LEGACY_VALIDATION_TYPE_MAP
from datavalidation.reporting.cross_schema import build_table_pairs_from_catalog_rows
from datavalidation.reporting.index_comparison import compare_indexes_legacy

__all__ = [
    "UNIFIED_REPORT_COLUMNS",
    "LEGACY_VALIDATION_TYPE_MAP",
    "build_table_pairs_from_catalog_rows",
    "compare_indexes_legacy",
]
