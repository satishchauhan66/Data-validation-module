"""Unit tests for DB2 ↔ Azure default expression normalization and pairing status."""

from datavalidation.validators.schema import _defaults_equivalent, _norm_default_expr


def test_norm_unwraps_azure_parens():
    assert _norm_default_expr("((0))") == _norm_default_expr("0")
    assert _norm_default_expr("((-1))") == _norm_default_expr("-1")
    assert _norm_default_expr("(1)") == _norm_default_expr("1")


def test_norm_empty_string_literals():
    assert _norm_default_expr("''") == _norm_default_expr("('')")
    assert _norm_default_expr("N''") == "__EMPTY_STR__"


def test_norm_timestamp_aliases():
    assert _defaults_equivalent("CURRENT TIMESTAMP", "(sysdatetime())")
    assert _defaults_equivalent("CURRENT TIMESTAMP", "GETDATE()")


def test_norm_char_flags():
    assert _defaults_equivalent("'N'", "('N')")
    assert _defaults_equivalent("'NONE'", "('NONE')")


def test_norm_empty_blob():
    assert _defaults_equivalent('"SYSIBM"."BLOB"(\'\')', "0x")


def test_float_default():
    assert _defaults_equivalent("0.0", "((0.0))")
