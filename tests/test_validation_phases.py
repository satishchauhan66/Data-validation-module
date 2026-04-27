"""Tests for data validation phase selection (fast row_counts default vs full suite)."""

import unittest

from datavalidation.config import DATA_VALIDATION_PHASE_KEYS, resolve_data_validation_phases


class TestResolveDataValidationPhases(unittest.TestCase):
    def test_default_is_row_counts_only(self):
        self.assertEqual(resolve_data_validation_phases(None, None), ("row_counts",))
        self.assertEqual(resolve_data_validation_phases("", []), ("row_counts",))

    def test_env_all_expands_full_suite(self):
        self.assertEqual(
            resolve_data_validation_phases("all", ["row_counts"]),
            tuple(DATA_VALIDATION_PHASE_KEYS),
        )

    def test_env_overrides_options(self):
        self.assertEqual(
            resolve_data_validation_phases("distinct_keys", ["row_counts", "checksum"]),
            ("distinct_keys",),
        )

    def test_options_order_matches_canonical(self):
        self.assertEqual(
            resolve_data_validation_phases(None, ["checksum", "row_counts"]),
            ("row_counts", "checksum"),
        )

    def test_hyphen_alias(self):
        self.assertEqual(
            resolve_data_validation_phases("row-counts,distinct-keys", None),
            ("row_counts", "distinct_keys"),
        )


if __name__ == "__main__":
    unittest.main()
