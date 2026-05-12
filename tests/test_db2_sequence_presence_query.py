"""DB2 dialect: sequence presence excludes identity-backed sequences (COLIDENTATTRIBUTES)."""

import unittest

from datavalidation.dialects.db2 import DB2Dialect


class TestDb2SequencePresenceQuery(unittest.TestCase):
    def test_presence_sequences_excludes_identity_backed(self):
        d = DB2Dialect()
        q = d.catalog_presence_sequences_query("USERID").lower()
        self.assertIn("syscat.sequences", q)
        self.assertIn("syscat.colidentattributes", q)
        self.assertIn("not exists", q)
        compact = q.replace(" ", "")
        self.assertIn("c.seqid=s.seqid", compact)


if __name__ == "__main__":
    unittest.main()
