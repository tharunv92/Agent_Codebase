from drive_to_s3.utils import normalize_s3_prefix, safe_join_s3_key


def test_normalize_s3_prefix():
    assert normalize_s3_prefix("") == ""
    assert normalize_s3_prefix("/") == ""
    assert normalize_s3_prefix("raw") == "raw/"
    assert normalize_s3_prefix("raw/") == "raw/"
    assert normalize_s3_prefix("/raw/") == "raw/"


def test_safe_join_s3_key():
    assert safe_join_s3_key("", "a/b.txt") == "a/b.txt"
    assert safe_join_s3_key("raw/", "a/b.txt") == "raw/a/b.txt"
    assert safe_join_s3_key("raw", "/a/b.txt") == "raw/a/b.txt"
