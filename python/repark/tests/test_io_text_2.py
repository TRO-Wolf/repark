"""IO-TEXT-1 round 5 — probe5 oracle pins for the X-1/X-2/X-3 follow-up —
plus round 6 probe6 pins for the Y-2 overlay types, plus round 7 probe7
pins for the Z-1 session-zone wall clocks.

Oracle cells live in ``facade_reader_writer_oracle.json`` as
``text_probe5_<cell>`` (live PySpark 4.1.2, recorded 2026-09-15, script
``probe_iotext5.py`` beside the probe JSON), ``text_probe6_<cell>``
(live PySpark 4.1.2, recorded 2026-09-15, script ``probe_iotext6.py``
beside the probe JSON), and ``text_probe7_<cell>`` (live PySpark 4.1.2
in a ``spark.sql.session.timeZone=America/New_York`` session, recorded
2026-09-15, script ``probe_iotext7.py`` beside the probe JSON). Result
cells pin columns plus schema simpleString plus Row reprs; the two layout
cells pin the ``CONFLICTING_PARTITION_COLUMN_NAMES`` refusal before any
row is read; the timestamp cell pins midnight in the session zone with an
instant cross-check against the oracle wall. The X-4 fallback pins live at
the end of this file; the Y-2 overlay pins follow them: boolean results,
the four refusals (boolean bad, smallint over `1.50`, timestamp_ntz,
array), and the float/smallint/tinyint/binary result pins through the
shared schema-display divergence (ledger R-39). The Z-1 pins close this
file: ``timestamp_ntz`` overlay reads the raw directory text as a
zone-free wall clock under a New York session while ``timestamp`` keeps
the session-zone wall, and inference follows Spark's order (integral,
fractional, date, timestamp, string) with the space wall inferring
``timestamp``. Timestamp pins compare ``rows_as_string`` (the engine
``CAST(k AS STRING)`` in the session zone) wherever the Python repr
depends on the machine zone.

pins: io-text-1/X-1, X-2, X-3, Y-2, Z-1
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

CELLS: dict[str, Any] = json.loads(
    Path(__file__).with_name("facade_reader_writer_oracle.json").read_text()
)["cells"]


def _cell(name: str) -> dict[str, Any]:
    return CELLS[name]


def _result_pin(frame: Any, cell: str) -> None:
    expected = _cell(cell)["result"]
    assert frame.columns == expected["columns"]
    assert frame.schema.simpleString() == expected["schema"]
    assert sorted(repr(tuple(row)) for row in frame.collect()) == sorted(expected["rows"])


@pytest.fixture
def spark() -> Any:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-io-text-2").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def test_text_probe5_date_inferred(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_date_inferred — k=2024-01-02 infers date. pins: io-text-1/X-1"""
    root = tmp_path / "kdate"
    (root / "k=2024-01-02").mkdir(parents=True)
    (root / "k=2024-01-02" / "part-00000.txt").write_text("d\n", encoding="utf-8")
    _result_pin(spark.read.text(str(root)), "text_probe5_date_inferred")


def test_text_probe5_date_schema_string(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_date_schema_string — k string keeps the raw text. pins: io-text-1/X-1"""
    root = tmp_path / "kdate"
    (root / "k=2024-01-02").mkdir(parents=True)
    (root / "k=2024-01-02" / "part-00000.txt").write_text("d\n", encoding="utf-8")
    _result_pin(
        spark.read.schema("value string, k string").text(str(root)),
        "text_probe5_date_schema_string",
    )


def test_text_probe5_date_schema_date(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_date_schema_date — k date parses the raw text. pins: io-text-1/X-1"""
    root = tmp_path / "kdate"
    (root / "k=2024-01-02").mkdir(parents=True)
    (root / "k=2024-01-02" / "part-00000.txt").write_text("d\n", encoding="utf-8")
    _result_pin(
        spark.read.schema("value string, k date").text(str(root)),
        "text_probe5_date_schema_date",
    )


def test_text_probe5_date_schema_timestamp(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_date_schema_timestamp — date-only is midnight zone. pins: io-text-1/X-1"""
    import datetime
    import re
    import zoneinfo

    expected = _cell("text_probe5_date_schema_timestamp")["result"]
    root = tmp_path / "kdate"
    (root / "k=2024-01-02").mkdir(parents=True)
    (root / "k=2024-01-02" / "part-00000.txt").write_text("d\n", encoding="utf-8")
    back = spark.read.schema("value string, k timestamp").text(str(root))
    assert back.columns == expected["columns"]
    assert back.schema.simpleString() == expected["schema"]
    rows = back.collect()
    assert [(row.value, row.k) for row in rows] == [("d", datetime.datetime(2024, 1, 2, 0, 0))]
    match = re.search(r"datetime\(([^)]*)\)", expected["rows"][0])
    assert match is not None
    oracle_wall = datetime.datetime(*[int(part) for part in match.group(1).split(",")])
    oracle_epoch = oracle_wall.replace(tzinfo=zoneinfo.ZoneInfo("America/New_York")).timestamp()
    repark_epoch = rows[0].k.replace(tzinfo=datetime.UTC).timestamp()
    assert oracle_epoch == repark_epoch


def test_text_probe5_lead_schema_string(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_lead_schema_string — k string keeps 007. pins: io-text-1/X-1"""
    root = tmp_path / "klead"
    (root / "k=007").mkdir(parents=True)
    (root / "k=007" / "part-00000.txt").write_text("lead\n", encoding="utf-8")
    _result_pin(
        spark.read.schema("value string, k string").text(str(root)),
        "text_probe5_lead_schema_string",
    )


def test_text_probe5_lead_schema_int(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_lead_schema_int — k int parses 007 as 7. pins: io-text-1/X-1"""
    root = tmp_path / "klead"
    (root / "k=007").mkdir(parents=True)
    (root / "k=007" / "part-00000.txt").write_text("lead\n", encoding="utf-8")
    _result_pin(
        spark.read.schema("value string, k int").text(str(root)),
        "text_probe5_lead_schema_int",
    )


def test_text_probe5_lead_schema_bigint(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_lead_schema_bigint — k bigint parses 007 as 7. pins: io-text-1/X-1"""
    root = tmp_path / "klead"
    (root / "k=007").mkdir(parents=True)
    (root / "k=007" / "part-00000.txt").write_text("lead\n", encoding="utf-8")
    _result_pin(
        spark.read.schema("value string, k bigint").text(str(root)),
        "text_probe5_lead_schema_bigint",
    )


def test_text_probe5_dec_schema_string(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_dec_schema_string — k string keeps 1.50. pins: io-text-1/X-1"""
    root = tmp_path / "kdec"
    (root / "k=1.50").mkdir(parents=True)
    (root / "k=1.50" / "part-00000.txt").write_text("dec\n", encoding="utf-8")
    _result_pin(
        spark.read.schema("value string, k string").text(str(root)),
        "text_probe5_dec_schema_string",
    )


def test_text_probe5_dec_schema_decimal(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_dec_schema_decimal — k decimal keeps scale 1.50. pins: io-text-1/X-1"""
    root = tmp_path / "kdec"
    (root / "k=1.50").mkdir(parents=True)
    (root / "k=1.50" / "part-00000.txt").write_text("dec\n", encoding="utf-8")
    _result_pin(
        spark.read.schema("value string, k decimal(10,2)").text(str(root)),
        "text_probe5_dec_schema_decimal",
    )


def test_text_probe5_dec_schema_double(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_dec_schema_double — k double reads 1.5. pins: io-text-1/X-1"""
    root = tmp_path / "kdec"
    (root / "k=1.50").mkdir(parents=True)
    (root / "k=1.50" / "part-00000.txt").write_text("dec\n", encoding="utf-8")
    _result_pin(
        spark.read.schema("value string, k double").text(str(root)),
        "text_probe5_dec_schema_double",
    )


def test_text_probe5_escaped_schema_string(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_escaped_schema_string — k string unescapes a/b. pins: io-text-1/X-1"""
    root = tmp_path / "kesc"
    (root / "k=a%2Fb").mkdir(parents=True)
    (root / "k=a%2Fb" / "part-00000.txt").write_text("esc\n", encoding="utf-8")
    _result_pin(
        spark.read.schema("value string, k string").text(str(root)),
        "text_probe5_escaped_schema_string",
    )


def test_text_probe5_default_partition_schema_int(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_default_partition_schema_int — default stays NULL. pins: io-text-1/X-1"""
    root = tmp_path / "knull"
    (root / "k=__HIVE_DEFAULT_PARTITION__").mkdir(parents=True)
    (root / "k=__HIVE_DEFAULT_PARTITION__" / "part-00000.txt").write_text("nul\n", encoding="utf-8")
    _result_pin(
        spark.read.schema("value string, k int").text(str(root)),
        "text_probe5_default_partition_schema_int",
    )


def test_text_probe5_uneven_depth(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_uneven_depth — uneven depth refuses before any row. pins: io-text-1/X-2"""
    from repark.errors import PySparkException

    expected = _cell("text_probe5_uneven_depth")["error"]
    root = tmp_path / "uneven"
    (root / "k=x" / "n=1").mkdir(parents=True)
    (root / "k=y").mkdir(parents=True)
    (root / "k=x" / "n=1" / "part-00000.txt").write_text("deep\n", encoding="utf-8")
    (root / "k=y" / "part-00000.txt").write_text("shallow\n", encoding="utf-8")
    with pytest.raises(PySparkException) as raised:
        spark.read.text(str(root))
    assert str(raised.value).startswith(
        "[CONFLICTING_PARTITION_COLUMN_NAMES] Conflicting partition column names detected:"
    )
    for line in expected["message"].splitlines():
        if line.strip().startswith("Partition column name list"):
            assert line.strip() in str(raised.value)
    assert str(raised.value).endswith("SQLSTATE: KD009")
    assert raised.value.getCondition() == expected["condition"]
    assert raised.value.getSqlState() == expected["sqlstate"]


def test_text_probe5_nonleaf_data_file(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_nonleaf_data_file — non-leaf data file refuses. pins: io-text-1/X-3"""
    from repark.errors import PySparkException

    expected = _cell("text_probe5_nonleaf_data_file")["error"]
    root = tmp_path / "nonleaf"
    (root / "k=x" / "n=1").mkdir(parents=True)
    (root / "k=x" / "part-00000.txt").write_text("nonleaf\n", encoding="utf-8")
    (root / "k=x" / "n=1" / "part-00000.txt").write_text("leaf\n", encoding="utf-8")
    with pytest.raises(PySparkException) as raised:
        spark.read.text(str(root))
    assert str(raised.value).startswith(
        "[CONFLICTING_PARTITION_COLUMN_NAMES] Conflicting partition column names detected:"
    )
    for line in expected["message"].splitlines():
        if line.strip().startswith("Partition column name list"):
            assert line.strip() in str(raised.value)
    assert str(raised.value).endswith("SQLSTATE: KD009")
    assert raised.value.getCondition() == expected["condition"]
    assert raised.value.getSqlState() == expected["sqlstate"]


def test_text_probe5_nonleaf_success_marker_only(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe5_nonleaf_success_marker_only — markers are not data. pins: io-text-1/X-3"""
    root = tmp_path / "nonleaf_success"
    (root / "k=x" / "n=1").mkdir(parents=True)
    (root / "k=x" / "_SUCCESS").write_text("", encoding="utf-8")
    (root / "k=x" / "n=1" / "part-00000.txt").write_text("leaf\n", encoding="utf-8")
    _result_pin(spark.read.text(str(root)), "text_probe5_nonleaf_success_marker_only")


def _write_kbool(tmp_path: Path) -> Path:
    """Lay out k=true/k=TRUE/k=false leaves for the probe6 boolean pins."""
    root = tmp_path / "kbool"
    for leaf, body in (("k=true", "t\n"), ("k=TRUE", "T\n"), ("k=false", "f\n")):
        (root / leaf).mkdir(parents=True)
        (root / leaf / "part-00000.txt").write_text(body, encoding="utf-8")
    return root


def _error_pin(spark: Any, ddl: str, root: Path, cell: str, value: str, display: str) -> None:
    """Pin an INVALID_PARTITION_VALUE refusal against its oracle cell."""
    from repark.errors import PySparkException

    expected = _cell(cell)["error"]
    with pytest.raises(PySparkException) as raised:
        spark.read.schema(ddl).text(str(root)).collect()
    assert str(raised.value).startswith("[INVALID_PARTITION_VALUE] Failed to cast value '")
    assert f"'{value}' to data type \"{display}\" for partition column `k`" in str(raised.value)
    assert str(raised.value).endswith("SQLSTATE: 42846")
    assert raised.value.getCondition() == expected["condition"]
    assert raised.value.getSqlState() == expected["sqlstate"]


def test_text_probe6_bool_inferred(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe6_bool_inferred — boolean-looking dirs stay string. pins: io-text-1/Y-2"""
    _result_pin(spark.read.text(str(_write_kbool(tmp_path))), "text_probe6_bool_inferred")


def test_text_probe6_bool_schema_boolean(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe6_bool_schema_boolean — k boolean parses any case. pins: io-text-1/Y-2"""
    _result_pin(
        spark.read.schema("value string, k boolean").text(str(_write_kbool(tmp_path))),
        "text_probe6_bool_schema_boolean",
    )


def test_text_probe6_bool_schema_boolean_bad(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe6_bool_schema_boolean_bad — yes refuses as BOOLEAN. pins: io-text-1/Y-2"""
    root = tmp_path / "kboolbad"
    (root / "k=yes").mkdir(parents=True)
    (root / "k=yes" / "part-00000.txt").write_text("y\n", encoding="utf-8")
    _error_pin(
        spark,
        "value string, k boolean",
        root,
        "text_probe6_bool_schema_boolean_bad",
        "yes",
        "BOOLEAN",
    )


def test_text_probe6_float_schema_smallint(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe6_float_schema_smallint — 1.50 refuses as SMALLINT. pins: io-text-1/Y-2"""
    root = tmp_path / "kfloat"
    (root / "k=1.50").mkdir(parents=True)
    (root / "k=1.50" / "part-00000.txt").write_text("f\n", encoding="utf-8")
    _error_pin(
        spark,
        "value string, k smallint",
        root,
        "text_probe6_float_schema_smallint",
        "1.50",
        "SMALLINT",
    )


def _write_kint(tmp_path: Path) -> Path:
    """Lay out a k=7 leaf for the probe6 smallint/tinyint/binary/ntz/array pins."""
    root = tmp_path / "kint"
    (root / "k=7").mkdir(parents=True)
    (root / "k=7" / "part-00000.txt").write_text("i\n", encoding="utf-8")
    return root


def _write_kfloat(tmp_path: Path) -> Path:
    """Lay out a k=1.50 leaf for the probe6 float overlay pin."""
    root = tmp_path / "kfloat_float"
    (root / "k=1.50").mkdir(parents=True)
    (root / "k=1.50" / "part-00000.txt").write_text("f\n", encoding="utf-8")
    return root


def test_text_probe6_float_schema_float(spark: ReparkSession, tmp_path: Path) -> None:
    """float overlay: exact value, narrow label. pins: io-text-1/Y-2, logical-width-1/C-010"""
    _result_pin(
        spark.read.schema("value string, k float").text(str(_write_kfloat(tmp_path))),
        "text_probe6_float_schema_float",
    )


def test_text_probe6_int_schema_smallint(spark: ReparkSession, tmp_path: Path) -> None:
    """smallint overlay: exact value, narrow label. pins: io-text-1/Y-2, logical-width-1/C-010"""
    _result_pin(
        spark.read.schema("value string, k smallint").text(str(_write_kint(tmp_path))),
        "text_probe6_int_schema_smallint",
    )


def test_text_probe6_int_schema_tinyint(spark: ReparkSession, tmp_path: Path) -> None:
    """tinyint overlay: exact value, narrow label. pins: io-text-1/Y-2, logical-width-1/C-010"""
    _result_pin(
        spark.read.schema("value string, k tinyint").text(str(_write_kint(tmp_path))),
        "text_probe6_int_schema_tinyint",
    )


def test_text_probe6_int_schema_binary(spark: ReparkSession, tmp_path: Path) -> None:
    """binary overlay: exact bytes, narrow label. pins: io-text-1/Y-2, logical-width-1/C-010"""
    _result_pin(
        spark.read.schema("value string, k binary").text(str(_write_kint(tmp_path))),
        "text_probe6_int_schema_binary",
    )


def test_text_probe6_int_schema_timestamp_ntz(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe6_int_schema_timestamp_ntz — 7 refuses TIMESTAMP_NTZ. pins: io-text-1/Y-2"""
    _error_pin(
        spark,
        "value string, k timestamp_ntz",
        _write_kint(tmp_path),
        "text_probe6_int_schema_timestamp_ntz",
        "7",
        "TIMESTAMP_NTZ",
    )


def test_text_probe6_int_schema_array(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe6_int_schema_array — 7 refuses as ARRAY<INT>. pins: io-text-1/Y-2"""
    _error_pin(
        spark,
        "value string, k array<int>",
        _write_kint(tmp_path),
        "text_probe6_int_schema_array",
        "7",
        "ARRAY<INT>",
    )


def test_text_partition_fallback_holds_one_part_per_leaf(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Past 256 keys the sorted fallback keeps one part per leaf. pins: io-text-1/X-4"""
    out = tmp_path / "fallback"
    rows = [(f"k{key}", f"r{round}-k{key}") for round in range(4) for key in range(300)]
    spark.createDataFrame(rows, "k string, value string").write.partitionBy("k").text(str(out))
    assert (out / "_SUCCESS").is_file()
    leaves = sorted(path for path in out.iterdir() if path.is_dir())
    assert len(leaves) == 300
    total = 0
    for leaf in leaves:
        parts = sorted(leaf.glob("part-*.txt"))
        assert len(parts) == 1
        lines = parts[0].read_text(encoding="utf-8").splitlines()
        total += len(lines)
        number = leaf.name.split("=", 1)[1][1:]
        assert sorted(lines) == sorted(f"r{round}-k{number}" for round in range(4))
    assert total == 1200


@pytest.fixture
def spark_ny() -> Any:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-io-text-2-ny")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _string_pin(frame: Any, cell: str) -> None:
    """Pin columns, schema, and the session-zone string render of ``k``."""
    expected = _cell(cell)["result"]
    assert frame.columns == expected["columns"]
    assert frame.schema.simpleString() == expected["schema"]
    rendered = frame.selectExpr("value", "cast(k as string) as k").collect()
    assert sorted(repr(tuple(row)) for row in rendered) == sorted(expected["rows_as_string"])


def _write_kdate(tmp_path: Path) -> Path:
    """Lay out a k=2024-01-02 leaf for the probe7 date pins."""
    root = tmp_path / "kdate"
    (root / "k=2024-01-02").mkdir(parents=True)
    (root / "k=2024-01-02" / "part-00000.txt").write_text("d\n", encoding="utf-8")
    return root


def _write_kts_escaped(tmp_path: Path) -> Path:
    """Lay out an escaped k=2024-01-02 03:04:05 leaf for the probe7 pins."""
    root = tmp_path / "kts_escaped"
    (root / "k=2024-01-02 03%3A04%3A05").mkdir(parents=True)
    (root / "k=2024-01-02 03%3A04%3A05" / "part-00000.txt").write_text("e\n", encoding="utf-8")
    return root


def _write_kts_raw(tmp_path: Path) -> Path:
    """Lay out a k=2024-01-02T03:04:05 leaf for the probe7 ISO pins."""
    root = tmp_path / "kts_raw"
    (root / "k=2024-01-02T03:04:05").mkdir(parents=True)
    (root / "k=2024-01-02T03:04:05" / "part-00000.txt").write_text("r\n", encoding="utf-8")
    return root


def test_text_probe7_date_ntz(spark_ny: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe7_ny_date_ntz — date-only is naive midnight. pins: io-text-1/Z-1"""
    _result_pin(
        spark_ny.read.schema("value string, k timestamp_ntz").text(str(_write_kdate(tmp_path))),
        "text_probe7_ny_date_ntz",
    )


def test_text_probe7_ts_escaped_ntz(spark_ny: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe7_ny_ts_escaped_ntz — wall clock never shifts. pins: io-text-1/Z-1"""
    _result_pin(
        spark_ny.read.schema("value string, k timestamp_ntz").text(
            str(_write_kts_escaped(tmp_path))
        ),
        "text_probe7_ny_ts_escaped_ntz",
    )


def test_text_probe7_ts_iso_ntz(spark_ny: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe7_ny_ts_iso_ntz — the T wall never shifts. pins: io-text-1/Z-1"""
    _result_pin(
        spark_ny.read.schema("value string, k timestamp_ntz").text(str(_write_kts_raw(tmp_path))),
        "text_probe7_ny_ts_iso_ntz",
    )


def test_text_probe7_date_ltz(spark_ny: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe7_ny_date_ltz — zoned date stays the session wall. pins: io-text-1/Z-1"""
    _string_pin(
        spark_ny.read.schema("value string, k timestamp").text(str(_write_kdate(tmp_path))),
        "text_probe7_ny_date_ltz",
    )


def test_text_probe7_ts_escaped_ltz(spark_ny: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe7_ny_ts_escaped_ltz — zoned wall is session-local. pins: io-text-1/Z-1"""
    _string_pin(
        spark_ny.read.schema("value string, k timestamp").text(str(_write_kts_escaped(tmp_path))),
        "text_probe7_ny_ts_escaped_ltz",
    )


def test_text_probe7_ts_iso_ltz(spark_ny: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe7_ny_ts_iso_ltz — zoned T wall is session-local. pins: io-text-1/Z-1"""
    _string_pin(
        spark_ny.read.schema("value string, k timestamp").text(str(_write_kts_raw(tmp_path))),
        "text_probe7_ny_ts_iso_ltz",
    )


def test_text_probe7_date_inferred(spark_ny: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe7_ny_date_inferred — date still infers date. pins: io-text-1/Z-1"""
    _result_pin(spark_ny.read.text(str(_write_kdate(tmp_path))), "text_probe7_ny_date_inferred")


def test_text_probe7_ts_escaped_inferred(spark_ny: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe7_ny_ts_escaped_inferred — space wall infers timestamp. pins: io-text-1/Z-1"""
    _string_pin(
        spark_ny.read.text(str(_write_kts_escaped(tmp_path))),
        "text_probe7_ny_ts_escaped_inferred",
    )


def test_text_probe7_ts_iso_inferred(spark_ny: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe7_ny_ts_iso_inferred — T wall stays string. pins: io-text-1/Z-1"""
    _result_pin(spark_ny.read.text(str(_write_kts_raw(tmp_path))), "text_probe7_ny_ts_iso_inferred")


def test_text_partitioned_fallback_tiny_pool_refuses_loudly(tmp_path: Path) -> None:
    """A fallback write past the cap under a 16 MiB pool refuses loudly. pins: io-text-1/Z-2"""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-io-text-2-tiny-pool")
        .config("datafusion.runtime.memory_limit", "16M")
        .getOrCreate()
    )
    destination = tmp_path / "out"
    try:
        frame = session.range(4096).selectExpr(
            "CAST(id % 300 AS STRING) AS k", "repeat('x', 4096) AS value"
        )
        with pytest.raises(Exception, match="Resources exhausted"):
            frame.write.partitionBy("k").text(str(destination))
        assert not destination.exists()
        assert not [
            entry for entry in tmp_path.iterdir() if entry.name.startswith("repark-staging-")
        ]
    finally:
        session.stop()
        _reset_active_session_for_tests()
