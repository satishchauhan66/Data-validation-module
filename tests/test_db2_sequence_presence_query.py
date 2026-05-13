"""DB2 dialect: sequence presence query returns standalone + identity sequences with parent table info."""

import unittest

from datavalidation.dialects.db2 import DB2Dialect


class TestDb2SequencePresenceQuery(unittest.TestCase):
    def test_presence_sequences_includes_standalone_and_identity(self):
        d = DB2Dialect()
        q = d.catalog_presence_sequences_query("USERID").lower()
        self.assertIn("syscat.sequences", q)
        self.assertIn("syscat.colidentattributes", q)
        self.assertIn("seqtype", q)
        self.assertIn("parent_table", q)
        compact = q.replace(" ", "")
        self.assertIn("s.seqtypein('s','i')", compact)
        self.assertIn("c.seqid=s.seqid", compact)

    def test_presence_sequences_returns_seq_type_and_parent_table_columns(self):
        d = DB2Dialect()
        q = d.catalog_presence_sequences_query("USERID").lower()
        self.assertIn("seq_type", q)
        self.assertIn("parent_table", q)
        self.assertIn("left join", q)


if __name__ == "__main__":
    unittest.main()
