"""Unit tests for DB2 → Azure datatype conversion classification."""

from datavalidation.rules.datatype_map import classify_type_mapping, is_compatible_type


def test_clob_to_varchar_is_mapped():
    assert classify_type_mapping("CLOB", "varchar") == "mapped"
    assert is_compatible_type("CLOB", "varchar")
    assert is_compatible_type("CLOB", "varchar(max)")


def test_integer_to_int_is_mapped():
    assert classify_type_mapping("INTEGER", "int") == "mapped"


def test_varchar_to_varchar_exact():
    assert classify_type_mapping("VARCHAR", "varchar") == "exact"
    assert classify_type_mapping("VARCHAR(100)", "varchar") == "exact"


def test_timestamp_to_datetime2():
    assert classify_type_mapping("TIMESTAMP", "datetime2") == "mapped"


def test_blob_to_varbinary():
    assert classify_type_mapping("BLOB", "varbinary") == "mapped"


def test_incompatible():
    assert classify_type_mapping("CLOB", "int") == "mismatch"
    assert not is_compatible_type("CLOB", "int")


def test_boolean_decfloat():
    assert classify_type_mapping("BOOLEAN", "bit") == "mapped"
    assert classify_type_mapping("DECFLOAT", "float") == "mapped"
