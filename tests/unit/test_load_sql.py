"""The audit DDL shipped with the loader must not drift from the one that initialises the DWH volume."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_init_ddl_copies_are_identical():
    pairs = {"001_schemas.sql": "001_schemas.sql", "002_audit.sql": "005_audit.sql"}
    for src, copy in pairs.items():
        a = (ROOT / "sql" / "ddl" / src).read_text(encoding="utf-8")
        b = (ROOT / "src" / "pm25" / "load" / "sql" / copy).read_text(encoding="utf-8")
        assert a == b, src


def test_sql_files_run_in_dependency_order():
    names = sorted(p.name for p in (ROOT / "src" / "pm25" / "load" / "sql").glob("*.sql"))
    assert names == ["001_schemas.sql", "005_audit.sql", "010_dw.sql", "020_mart.sql"]
