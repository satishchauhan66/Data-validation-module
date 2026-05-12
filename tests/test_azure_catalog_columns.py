"""Azure SQL dialect: catalog column queries must include views (sys.objects type V)."""

import unittest

from datavalidation.dialects.azure_sql import AzureSQLDialect


class TestAzureCatalogColumnsQuery(unittest.TestCase):
    def test_columns_query_joins_sys_objects_not_tables_only(self):
        d = AzureSQLDialect()
        q = d.catalog_columns_query("dbo", "CLIENT_INPUT_FILE_IDS").lower()
        self.assertIn("sys.objects", q)
        self.assertIn("sys.columns", q)
        self.assertRegex(q, r"o\.type\s+in\s*\(\s*'u'\s*,\s*'v'\s*\)")
        self.assertNotRegex(q, r"join\s+sys\.tables\b")

    def test_columns_query_escapes_quotes_in_table_name(self):
        d = AzureSQLDialect()
        q = d.catalog_columns_query("dbo", "O'Reilly")
        self.assertIn("O''Reilly", q)


if __name__ == "__main__":
    unittest.main()
