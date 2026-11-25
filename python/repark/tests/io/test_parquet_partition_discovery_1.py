from __future__ import annotations

import math
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.orc as porc
import pyarrow.parquet as pq
import pytest

from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkException,
)
from repark.spark import ReparkSession

DOORS = ["parquet", "parquet_load"]
ANSI_MODES = ["true", "false"]
ZONES = ["UTC", "America/New_York"]


def _write_id_s(path: Path, rows: list[tuple[int, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "id": pa.array([row[0] for row in rows], type=pa.int64()),
            "s": pa.array([row[1] for row in rows], type=pa.string()),
        }
    )
    pq.write_table(table, path)


def _write_table(path: Path, columns: dict[str, pa.Array]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), path)


def _read_door(spark: ReparkSession, door: str, path: str) -> Any:
    if door == "parquet":
        return spark.read.parquet(path)
    return spark.read.format("parquet").load(path)


def _norm_value(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return ("nan",)
    return value


def _sorted_rows(frame: Any) -> list[tuple[Any, ...]]:
    rows = [tuple(_norm_value(cell) for cell in row) for row in frame.collect()]
    return sorted(rows, key=repr)


@pytest.fixture
def spark_factory() -> Iterator[list[ReparkSession]]:
    created: list[ReparkSession] = []
    yield created
    for session in created:
        session.stop()


def _make_session(
    created: list[ReparkSession],
    name: str,
    ansi: str = "true",
    zone: str = "UTC",
    case_sensitive: str = "false",
    infer_part: str = "true",
) -> ReparkSession:
    active = ReparkSession.getActiveSession()
    if active is not None:
        active.stop()
    session = (
        ReparkSession.builder.appName(name)
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.caseSensitive", case_sensitive)
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", infer_part)
        .getOrCreate()
    )
    created.append(session)
    return session


@pytest.fixture(scope="module")
def part_fixtures(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("parquet_partdisc_1")

    _write_id_s(root / "one_level" / "p=1" / "part-0.parquet", [(1, "a"), (2, "b")])
    _write_id_s(root / "one_level" / "p=2" / "part-0.parquet", [(3, "c"), (4, "d")])

    _write_id_s(
        root / "two_level" / "year=2024" / "month=1" / "part-0.parquet",
        [(1, "a")],
    )
    _write_id_s(
        root / "two_level" / "year=2024" / "month=2" / "part-0.parquet",
        [(2, "b")],
    )
    _write_id_s(
        root / "two_level" / "year=2025" / "month=1" / "part-0.parquet",
        [(3, "c")],
    )

    _write_id_s(root / "type_bigint" / "p=1" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "type_bigint" / "p=3000000000" / "part-0.parquet", [(2, "b")])

    _write_id_s(root / "type_double" / "p=1.5" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "type_double" / "p=2.25" / "part-0.parquet", [(2, "b")])

    _write_id_s(root / "type_decimal_like" / "p=1.50" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "type_decimal_like" / "p=2.75" / "part-0.parquet", [(2, "b")])

    _write_id_s(
        root / "type_big_decimal" / "p=123456789012345678901234567890" / "part-0.parquet",
        [(1, "a")],
    )

    _write_id_s(
        root / "type_dec_float_mix" / "p=123456789012345678901234567890" / "part-0.parquet",
        [(1, "a")],
    )
    _write_id_s(root / "type_dec_float_mix" / "p=1.5" / "part-0.parquet", [(2, "b")])

    _write_id_s(root / "type_nan_inf" / "p=NaN" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "type_nan_inf" / "p=-Infinity" / "part-0.parquet", [(2, "b")])

    _write_id_s(root / "type_nan_lower" / "p=nan" / "part-0.parquet", [(1, "a")])

    _write_id_s(root / "type_sci_dec" / "p=1e5" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "type_sci_mix" / "p=1e5" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "type_sci_mix" / "p=2.5e-2" / "part-0.parquet", [(2, "b")])

    _write_id_s(root / "type_date" / "p=2024-01-02" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "type_date" / "p=2024-07-04" / "part-0.parquet", [(2, "b")])

    _write_id_s(
        root / "type_timestamp" / "p=2024-01-02 08%3A04%3A05" / "part-0.parquet",
        [(1, "a")],
    )
    _write_id_s(
        root / "type_timestamp" / "p=2024-07-04 12%3A30%3A00.5" / "part-0.parquet",
        [(2, "b")],
    )

    _write_id_s(
        root / "type_timestamp_2frac" / "p=2024-01-02 08%3A04%3A05.12" / "part-0.parquet",
        [(1, "a")],
    )

    _write_id_s(root / "type_string" / "p=alpha" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "type_string" / "p=beta" / "part-0.parquet", [(2, "b")])

    _write_id_s(root / "type_leading_zero" / "p=01" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "type_leading_zero" / "p=02" / "part-0.parquet", [(2, "b")])

    _write_id_s(root / "hive_default_mixed" / "p=1" / "part-0.parquet", [(1, "a")])
    _write_id_s(
        root / "hive_default_mixed" / "p=__HIVE_DEFAULT_PARTITION__" / "part-0.parquet",
        [(2, "b")],
    )

    _write_id_s(
        root / "hive_default_all" / "p=__HIVE_DEFAULT_PARTITION__" / "part-0.parquet",
        [(1, "a")],
    )

    _write_id_s(root / "empty_val" / "p=" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "empty_key" / "=1" / "part-0.parquet", [(1, "a")])

    _write_id_s(root / "url_escaped" / "p=a%20b" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "url_escaped" / "p=a%2Fb" / "part-0.parquet", [(2, "b")])
    _write_id_s(root / "url_escaped" / "p=a+b" / "part-0.parquet", [(3, "c")])

    _write_table(
        root / "overlap_same" / "p=1" / "part-0.parquet",
        {
            "id": pa.array([1], type=pa.int64()),
            "p": pa.array([99], type=pa.int32()),
            "s": pa.array(["a"], type=pa.string()),
        },
    )
    _write_table(
        root / "overlap_same" / "p=2" / "part-0.parquet",
        {
            "id": pa.array([2], type=pa.int64()),
            "p": pa.array([88], type=pa.int32()),
            "s": pa.array(["b"], type=pa.string()),
        },
    )

    _write_table(
        root / "overlap_diff_type" / "p=1" / "part-0.parquet",
        {
            "id": pa.array([1], type=pa.int64()),
            "p": pa.array(["file_val"], type=pa.string()),
            "s": pa.array(["a"], type=pa.string()),
        },
    )

    _write_table(
        root / "overlap_all_shadowed" / "p=1" / "part-0.parquet",
        {"p": pa.array([99], type=pa.int32())},
    )

    _write_id_s(root / "conflict_names" / "a=1" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "conflict_names" / "b=2" / "part-0.parquet", [(2, "b")])

    _write_id_s(root / "mixed_depths" / "year=2024" / "part-0.parquet", [(1, "a")])
    _write_id_s(
        root / "mixed_depths" / "year=2024" / "month=1" / "part-0.parquet",
        [(2, "b")],
    )

    _write_id_s(root / "noise_hidden" / "p=1" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "noise_hidden" / "p=2" / "part-0.parquet", [(2, "b")])
    (root / "noise_hidden" / "_SUCCESS").write_text("", encoding="utf-8")
    (root / "noise_hidden" / ".part-0.parquet.crc").write_text("crc", encoding="utf-8")
    _write_id_s(
        root / "noise_hidden" / "_temporary" / "0" / "part-temp.parquet",
        [(99, "tmp")],
    )
    _write_id_s(root / "noise_hidden" / ".hidden.parquet", [(98, "hid")])
    _write_id_s(root / "noise_hidden" / "p=1" / ".dot.parquet", [(97, "dot")])
    _write_id_s(root / "noise_hidden" / "p=1" / "_under.parquet", [(96, "und")])

    _write_id_s(root / "root_and_part" / "part-root.parquet", [(99, "root")])
    _write_id_s(root / "root_and_part" / "p=1" / "part-0.parquet", [(1, "a")])

    _write_id_s(root / "tag=foo" / "part-0.parquet", [(1, "a"), (2, "b")])

    _write_table(
        root / "case_upper_data_lower" / "P=1" / "part-0.parquet",
        {
            "id": pa.array([1], type=pa.int64()),
            "p": pa.array([99], type=pa.int32()),
            "s": pa.array(["a"], type=pa.string()),
        },
    )
    _write_id_s(root / "case_mixed_dirs" / "p=1" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "case_mixed_dirs" / "P=2" / "part-0.parquet", [(2, "b")])

    text_dir = root / "ctrl_text" / "p=1"
    text_dir.mkdir(parents=True, exist_ok=True)
    (text_dir / "part-0.txt").write_text("hello\n", encoding="utf-8")

    orc_dir = root / "ctrl_orc" / "p=1"
    orc_dir.mkdir(parents=True, exist_ok=True)
    porc.write_table(
        pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "s": pa.array(["a"], type=pa.string()),
            }
        ),
        orc_dir / "part-0.orc",
    )

    _write_id_s(root / "d1_f1_root_sub_temp" / "root.parquet", [(1, "r")])
    _write_id_s(root / "d1_f1_root_sub_temp" / "sub" / "a.parquet", [(2, "s")])
    _write_id_s(
        root / "d1_f1_root_sub_temp" / "_temporary" / "0" / "part-tmp.parquet",
        [(99, "tmp")],
    )
    _write_id_s(root / "d1_f1_nested_sub_temp" / "sub1" / "a.parquet", [(1, "a")])
    _write_id_s(root / "d1_f1_nested_sub_temp" / "sub2" / "b.parquet", [(2, "b")])
    _write_id_s(
        root / "d1_f1_nested_sub_temp" / "_temporary" / "0" / "part-tmp.parquet",
        [(99, "tmp")],
    )

    _write_id_s(root / "d1_f2_under_two" / "_p=1" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "d1_f2_under_two" / "_p=2" / "part-0.parquet", [(2, "b")])
    _write_id_s(root / "d1_f2_under_mix" / "_p=1" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "d1_f2_under_mix" / "p=2" / "part-0.parquet", [(2, "b")])
    _write_id_s(root / "d1_f2_under_hidden" / "_p=1" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "d1_f2_under_hidden" / "_p=1" / "_hidden.parquet", [(99, "hid")])
    _write_id_s(root / "d1_f2_under_hidden" / "_p=2" / "part-0.parquet", [(2, "b")])

    _write_id_s(root / "d1_f3_key_space" / "a%20b=1" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "d1_f3_key_slash" / "a%2Fb=10" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "d1_f3_key_eq" / "a%3Db=1" / "part-0.parquet", [(1, "a")])

    _write_id_s(root / "d1_f4_between" / "a=1" / "sub" / "b=2" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "d1_f4_between" / "a=1" / "sub" / "b=3" / "part-0.parquet", [(2, "b")])
    _write_id_s(root / "d1_f4_two_sub" / "a=1" / "b=2" / "sub" / "part-0.parquet", [(1, "a")])

    _write_id_s(root / "d1_f5_date_ts" / "p=2024-01-02" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "d1_f5_date_ts" / "p=2024-07-04" / "part-0.parquet", [(2, "b")])
    _write_id_s(
        root / "d1_f5_date_ts" / "p=2024-01-02 08%3A04%3A05" / "part-0.parquet",
        [(3, "c")],
    )

    _write_id_s(root / "d1_f6_empty_bad" / "p=1" / "part-0.parquet", [(1, "one")])
    _write_id_s(root / "d1_f6_empty_bad" / "p=x" / "part-0.parquet", [])
    _write_id_s(root / "d1_f6_divzero" / "p=1" / "part-0.parquet", [(1, "one")])
    _write_id_s(root / "d1_f6_divzero" / "p=0" / "part-0.parquet", [])

    (root / "d1_f7_zero_own" / "p=1").mkdir(parents=True, exist_ok=True)
    (root / "d1_f7_zero_own" / "p=1" / "empty.parquet").write_bytes(b"")
    _write_id_s(root / "d1_f7_zero_own" / "p=2" / "good.parquet", [(2, "b")])
    _write_id_s(root / "d1_f7_zero_sib" / "p=1" / "good.parquet", [(1, "a")])
    (root / "d1_f7_zero_sib" / "p=1" / "empty.parquet").write_bytes(b"")
    _write_id_s(root / "d1_f7_zero_sib" / "p=2" / "good.parquet", [(2, "b")])

    _write_id_s(root / "d1_f8_0e5" / "p=0e5" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "d1_f8_0dot0e5" / "p=0.0e5" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "d1_f8_plus_nan" / "p=+NaN" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "d1_f8_neg_nan" / "p=-NaN" / "part-0.parquet", [(1, "a")])
    _write_id_s(root / "d1_f8_hex_float" / "p=0x1.0p0" / "part-0.parquet", [(1, "a")])

    _write_id_s(root / "d1_f9_pct_utf8" / "p=tea%C3%A9" / "part-0.parquet", [(1, "a")])

    return root


TYPE_CASES = [
    (
        "one_level",
        "struct<id:bigint,s:string,p:int>",
        [(1, "a", 1), (2, "b", 1), (3, "c", 2), (4, "d", 2)],
    ),
    (
        "two_level",
        "struct<id:bigint,s:string,year:int,month:int>",
        [(1, "a", 2024, 1), (2, "b", 2024, 2), (3, "c", 2025, 1)],
    ),
    ("type_bigint", "struct<id:bigint,s:string,p:bigint>", [(1, "a", 1), (2, "b", 3000000000)]),
    ("type_double", "struct<id:bigint,s:string,p:double>", [(1, "a", 1.5), (2, "b", 2.25)]),
    ("type_decimal_like", "struct<id:bigint,s:string,p:double>", [(1, "a", 1.5), (2, "b", 2.75)]),
    (
        "type_big_decimal",
        "struct<id:bigint,s:string,p:decimal(30,0)>",
        [(1, "a", Decimal("123456789012345678901234567890"))],
    ),
    (
        "type_dec_float_mix",
        "struct<id:bigint,s:string,p:string>",
        [(1, "a", "123456789012345678901234567890"), (2, "b", "1.5")],
    ),
    (
        "type_nan_inf",
        "struct<id:bigint,s:string,p:double>",
        [(1, "a", ("nan",)), (2, "b", float("-inf"))],
    ),
    ("type_nan_lower", "struct<id:bigint,s:string,p:string>", [(1, "a", "nan")]),
    ("type_sci_dec", "struct<id:bigint,s:string,p:decimal(6,0)>", [(1, "a", Decimal("100000"))]),
    (
        "type_sci_mix",
        "struct<id:bigint,s:string,p:string>",
        [(1, "a", "1e5"), (2, "b", "2.5e-2")],
    ),
    (
        "type_date",
        "struct<id:bigint,s:string,p:date>",
        [(1, "a", date(2024, 1, 2)), (2, "b", date(2024, 7, 4))],
    ),
    (
        "type_timestamp_2frac",
        "struct<id:bigint,s:string,p:string>",
        [(1, "a", "2024-01-02 08:04:05.12")],
    ),
    (
        "type_string",
        "struct<id:bigint,s:string,p:string>",
        [(1, "a", "alpha"), (2, "b", "beta")],
    ),
    ("type_leading_zero", "struct<id:bigint,s:string,p:int>", [(1, "a", 1), (2, "b", 2)]),
    ("hive_default_mixed", "struct<id:bigint,s:string,p:int>", [(1, "a", 1), (2, "b", None)]),
    ("hive_default_all", "struct<id:bigint,s:string,p:void>", [(1, "a", None)]),
    (
        "url_escaped",
        "struct<id:bigint,s:string,p:string>",
        [(1, "a", "a b"), (2, "b", "a/b"), (3, "c", "a+b")],
    ),
    (
        "overlap_same",
        "struct<id:bigint,p:int,s:string>",
        [(1, 1, "a"), (2, 2, "b")],
    ),
    ("overlap_diff_type", "struct<id:bigint,p:int,s:string>", [(1, 1, "a")]),
    ("overlap_all_shadowed", "struct<p:int>", [(1,)]),
    ("noise_hidden", "struct<id:bigint,s:string,p:int>", [(1, "a", 1), (2, "b", 2)]),
    ("root_and_part", "struct<id:bigint,s:string,p:int>", [(1, "a", 1)]),
]


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize(("layout", "expected_schema", "expected_rows"), TYPE_CASES)
def test_parquet_partition_discovery_matrix(
    part_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
    layout: str,
    expected_schema: str,
    expected_rows: list[tuple[Any, ...]],
) -> None:
    spark = _make_session(spark_factory, f"partdisc-{door}-{ansi}-{layout}", ansi=ansi)
    df = _read_door(spark, door, str(part_fixtures / layout))
    assert df.schema.simpleString() == expected_schema
    assert _sorted_rows(df) == sorted(expected_rows, key=repr)


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_parquet_partition_timestamp_and_date_in_zones(
    part_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
    zone: str,
) -> None:
    spark = _make_session(spark_factory, f"partdisc-tz-{door}-{ansi}-{zone}", ansi=ansi, zone=zone)
    df = _read_door(spark, door, str(part_fixtures / "type_timestamp"))
    assert df.schema.simpleString() == "struct<id:bigint,s:string,p:timestamp>"
    rows = _sorted_rows(df)
    assert rows == [
        (1, "a", datetime(2024, 1, 2, 8, 4, 5)),
        (2, "b", datetime(2024, 7, 4, 12, 30, 0, 500000)),
    ]
    date_df = _read_door(spark, door, str(part_fixtures / "type_date"))
    assert date_df.schema.simpleString() == "struct<id:bigint,s:string,p:date>"
    assert _sorted_rows(date_df) == [
        (1, "a", date(2024, 1, 2)),
        (2, "b", date(2024, 7, 4)),
    ]


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_parquet_partition_type_inference_disabled_and_conf_lifecycle(
    part_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
) -> None:
    spark = _make_session(
        spark_factory,
        f"partdisc-infer-off-{door}-{ansi}",
        ansi=ansi,
        infer_part="false",
    )
    key = "spark.sql.sources.partitionColumnTypeInference.enabled"
    assert spark.conf.get(key) == "false"

    df_zero = _read_door(spark, door, str(part_fixtures / "type_leading_zero"))
    assert df_zero.schema.simpleString() == "struct<id:bigint,s:string,p:string>"
    assert _sorted_rows(df_zero) == [(1, "a", "01"), (2, "b", "02")]

    df_null = _read_door(spark, door, str(part_fixtures / "hive_default_all"))
    assert df_null.schema.simpleString() == "struct<id:bigint,s:string,p:void>"
    assert _sorted_rows(df_null) == [(1, "a", None)]

    spark.sql(f"SET {key} = true")
    assert spark.conf.get(key) == "true"
    df_on = _read_door(spark, door, str(part_fixtures / "type_leading_zero"))
    assert df_on.schema.simpleString() == "struct<id:bigint,s:string,p:int>"

    spark.sql(f"RESET {key}")
    assert spark.conf.get(key) == "false"
    df_reset = _read_door(spark, door, str(part_fixtures / "type_leading_zero"))
    assert df_reset.schema.simpleString() == "struct<id:bigint,s:string,p:string>"

    spark.conf.unset(key)
    assert spark.conf.get(key) == "true"
    df_unset = _read_door(spark, door, str(part_fixtures / "type_leading_zero"))
    assert df_unset.schema.simpleString() == "struct<id:bigint,s:string,p:int>"

    with pytest.raises(IllegalArgumentException):
        spark.conf.set(key, "not_a_bool")


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("layout", ["conflict_names", "mixed_depths"])
def test_parquet_partition_conflicts_raise_spark_error_class(
    part_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
    layout: str,
) -> None:
    spark = _make_session(spark_factory, f"partdisc-conflict-{door}-{ansi}-{layout}", ansi=ansi)
    with pytest.raises(PySparkException) as exc_info:
        _read_door(spark, door, str(part_fixtures / layout))
    err = exc_info.value
    assert err.getCondition() == "CONFLICTING_PARTITION_COLUMN_NAMES"
    assert err.getSqlState() == "KD009"


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("layout", ["empty_val", "empty_key"])
def test_parquet_partition_empty_segment_refuses(
    part_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
    layout: str,
) -> None:
    spark = _make_session(spark_factory, f"partdisc-empty-{door}-{ansi}-{layout}", ansi=ansi)
    with pytest.raises(PySparkException):
        _read_door(spark, door, str(part_fixtures / layout))


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_parquet_partition_case_sensitivity(
    part_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
) -> None:
    spark_ci = _make_session(
        spark_factory,
        f"partdisc-cs-false-{door}-{ansi}",
        ansi=ansi,
        case_sensitive="false",
    )
    df_ci = _read_door(spark_ci, door, str(part_fixtures / "case_upper_data_lower"))
    assert df_ci.schema.simpleString() == "struct<id:bigint,P:int,s:string>"
    assert _sorted_rows(df_ci) == [(1, 1, "a")]

    df_mix_ci = _read_door(spark_ci, door, str(part_fixtures / "case_mixed_dirs"))
    assert df_mix_ci.schema.simpleString() == "struct<id:bigint,s:string,p:int>"
    assert _sorted_rows(df_mix_ci) == [(1, "a", 1), (2, "b", 2)]

    spark_cs = _make_session(
        spark_factory,
        f"partdisc-cs-true-{door}-{ansi}",
        ansi=ansi,
        case_sensitive="true",
    )
    df_cs = _read_door(spark_cs, door, str(part_fixtures / "case_upper_data_lower"))
    assert df_cs.schema.simpleString() == "struct<id:bigint,p:int,s:string,P:int>"
    assert _sorted_rows(df_cs) == [(1, 99, "a", 1)]

    with pytest.raises(PySparkException) as exc_info:
        _read_door(spark_cs, door, str(part_fixtures / "case_mixed_dirs"))
    assert exc_info.value.getCondition() == "CONFLICTING_PARTITION_COLUMN_NAMES"
    assert exc_info.value.getSqlState() == "KD009"


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_parquet_partition_queries_pruning_and_round_trip(
    part_fixtures: Path,
    tmp_path: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
) -> None:
    spark = _make_session(spark_factory, f"partdisc-q-{door}-{ansi}", ansi=ansi)
    df = _read_door(spark, door, str(part_fixtures / "one_level"))
    assert df.dtypes == [("id", "bigint"), ("s", "string"), ("p", "int")]
    assert df.count() == 4
    assert _sorted_rows(df.select("p")) == [(1,), (1,), (2,), (2,)]
    assert _sorted_rows(df.groupBy("p").count()) == [(1, 2), (2, 2)]

    filtered = df.where("p = 2")
    assert _sorted_rows(filtered) == [(3, "c", 2), (4, "d", 2)]
    scanned = filtered.inputFiles()
    assert len(scanned) == 1
    assert "/p=2/" in scanned[0]

    df.createOrReplaceTempView("v_part_one")
    sql_rows = spark.sql("SELECT id, s, p FROM v_part_one WHERE p = 2 ORDER BY id")
    assert _sorted_rows(sql_rows) == [(3, "c", 2), (4, "d", 2)]

    copy_dir = tmp_path / f"copy_{door}_{ansi}"
    df.write.parquet(str(copy_dir))
    copy_back = _read_door(spark, door, str(copy_dir))
    assert copy_back.schema.simpleString() == "struct<id:bigint,s:string,p:int>"
    assert _sorted_rows(copy_back) == [
        (1, "a", 1),
        (2, "b", 1),
        (3, "c", 2),
        (4, "d", 2),
    ]

    repark_part_dir = tmp_path / f"repark_part_{door}_{ansi}"
    df.write.partitionBy("p").parquet(str(repark_part_dir))
    repark_back = _read_door(spark, door, str(repark_part_dir))
    assert repark_back.schema.simpleString() == "struct<id:bigint,s:string,p:int>"
    assert _sorted_rows(repark_back) == [
        (1, "a", 1),
        (2, "b", 1),
        (3, "c", 2),
        (4, "d", 2),
    ]

    file_uri = f"file://{part_fixtures / 'one_level'}"
    uri_df = _read_door(spark, door, file_uri)
    assert uri_df.schema.simpleString() == "struct<id:bigint,s:string,p:int>"
    assert uri_df.count() == 4


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_parquet_controls_leaf_equals_root_options_text_and_iceberg(
    part_fixtures: Path,
    tmp_path: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
) -> None:
    warehouse = tmp_path / f"ice_wh_{door}_{ansi}"
    active = ReparkSession.getActiveSession()
    if active is not None:
        active.stop()
    spark = (
        ReparkSession.builder.appName(f"partdisc-ctrl-{door}-{ansi}")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .getOrCreate()
    )
    spark_factory.append(spark)

    leaf = _read_door(spark, door, str(part_fixtures / "one_level" / "p=1"))
    assert leaf.schema.simpleString() == "struct<id:bigint,s:string>"
    assert _sorted_rows(leaf) == [(1, "a"), (2, "b")]

    root_eq = _read_door(spark, door, str(part_fixtures / "tag=foo"))
    assert root_eq.schema.simpleString() == "struct<id:bigint,s:string>"
    assert _sorted_rows(root_eq) == [(1, "a"), (2, "b")]

    for opt_name, opt_val in [
        ("basePath", str(part_fixtures / "one_level")),
        ("recursiveFileLookup", "true"),
        ("mergeSchema", "true"),
    ]:
        with pytest.raises(AnalysisException):
            reader = spark.read.option(opt_name, opt_val)
            if door == "parquet":
                reader.parquet(str(part_fixtures / "one_level"))
            else:
                reader.format("parquet").load(str(part_fixtures / "one_level"))

    txt = spark.read.text(str(part_fixtures / "ctrl_text"))
    assert txt.schema.simpleString() == "struct<value:string,p:int>"
    assert _sorted_rows(txt) == [("hello", 1)]

    orc_df = spark.read.orc(str(part_fixtures / "ctrl_orc"))
    assert orc_df.schema.simpleString() == "struct<id:bigint,s:string,p:int>"
    assert _sorted_rows(orc_df) == [(1, "a", 1)]

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
    spark.sql(
        "CREATE TABLE ice.db.part_ice (id BIGINT, s STRING, p INT) USING iceberg PARTITIONED BY (p)"
    )
    spark.sql("INSERT INTO ice.db.part_ice VALUES (1, 'a', 1), (2, 'b', 2)")
    ice_df = spark.read.table("ice.db.part_ice")
    assert ice_df.schema.simpleString() == "struct<id:bigint,s:string,p:int>"
    assert _sorted_rows(ice_df.where("p = 2")) == [(2, "b", 2)]


DELTA1_LAYOUT_CASES = [
    ("d1_f1_root_sub_temp", "struct<id:bigint,s:string>", [(1, "r")]),
    ("d1_f2_under_two", "struct<id:bigint,s:string,_p:int>", [(1, "a", 1), (2, "b", 2)]),
    ("d1_f2_under_hidden", "struct<id:bigint,s:string,_p:int>", [(1, "a", 1), (2, "b", 2)]),
    ("d1_f3_key_space", "struct<id:bigint,s:string,a b:int>", [(1, "a", 1)]),
    ("d1_f3_key_slash", "struct<id:bigint,s:string,a/b:int>", [(1, "a", 10)]),
    ("d1_f3_key_eq", "struct<id:bigint,s:string,a=b:int>", [(1, "a", 1)]),
    ("d1_f4_between", "struct<id:bigint,s:string,b:int>", [(1, "a", 2), (2, "b", 3)]),
    ("d1_f8_0e5", "struct<id:bigint,s:string,p:decimal(6,0)>", [(1, "a", Decimal("0"))]),
    ("d1_f8_0dot0e5", "struct<id:bigint,s:string,p:decimal(5,0)>", [(1, "a", Decimal("0"))]),
    ("d1_f8_plus_nan", "struct<id:bigint,s:string,p:double>", [(1, "a", ("nan",))]),
    ("d1_f8_neg_nan", "struct<id:bigint,s:string,p:double>", [(1, "a", ("nan",))]),
    ("d1_f8_hex_float", "struct<id:bigint,s:string,p:double>", [(1, "a", 1.0)]),
    ("d1_f9_pct_utf8", "struct<id:bigint,s:string,p:string>", [(1, "a", "tea\u00c3\u00a9")]),
]


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize(("layout", "expected_schema", "expected_rows"), DELTA1_LAYOUT_CASES)
def test_delta1_parquet_partition_discovery_layouts(
    part_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
    layout: str,
    expected_schema: str,
    expected_rows: list[tuple[Any, ...]],
) -> None:
    spark = _make_session(spark_factory, f"partdisc-d1-{door}-{ansi}-{layout}", ansi=ansi)
    df = _read_door(spark, door, str(part_fixtures / layout))
    assert df.schema.simpleString() == expected_schema
    assert _sorted_rows(df) == sorted(expected_rows, key=repr)


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize(
    ("layout", "expected_cond"),
    [
        ("d1_f1_nested_sub_temp", None),
        ("d1_f2_under_mix", "CONFLICTING_PARTITION_COLUMN_NAMES"),
        ("d1_f7_zero_own", "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"),
        ("d1_f7_zero_sib", "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"),
    ],
)
def test_delta1_parquet_partition_discovery_refusals(
    part_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
    layout: str,
    expected_cond: str | None,
) -> None:
    spark = _make_session(spark_factory, f"partdisc-d1-ref-{door}-{ansi}-{layout}", ansi=ansi)
    with pytest.raises(PySparkException) as exc_info:
        _read_door(spark, door, str(part_fixtures / layout))
    if expected_cond is not None:
        assert exc_info.value.getCondition() == expected_cond


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_delta1_mixed_date_and_timestamp_widens_in_zones(
    part_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
    zone: str,
) -> None:
    spark = _make_session(
        spark_factory, f"partdisc-d1-f5-{door}-{ansi}-{zone}", ansi=ansi, zone=zone
    )
    df = _read_door(spark, door, str(part_fixtures / "d1_f5_date_ts"))
    assert df.schema.simpleString() == "struct<id:bigint,s:string,p:timestamp>"
    assert _sorted_rows(df) == [
        (1, "a", datetime(2024, 1, 2, 0, 0, 0)),
        (2, "b", datetime(2024, 7, 4, 0, 0, 0)),
        (3, "c", datetime(2024, 1, 2, 8, 4, 5)),
    ]


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize(
    ("layout", "predicate"),
    [
        ("d1_f6_empty_bad", "CAST(p AS INT) = 1"),
        ("d1_f6_empty_bad", "CAST(p AS INT) = 99"),
        ("d1_f6_empty_bad", "p = 1"),
        ("d1_f6_empty_bad", "CAST(p AS INT) = 1 AND id = 1"),
        ("d1_f6_divzero", "10 / p = 10"),
    ],
)
def test_delta1_ansi_pruning_refuses_on_zero_row_partition(
    part_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    layout: str,
    predicate: str,
) -> None:
    spark_on = _make_session(spark_factory, f"partdisc-d1-f6-on-{door}", ansi="true")
    with pytest.raises(PySparkException):
        _read_door(spark_on, door, str(part_fixtures / layout)).where(predicate).collect()

    spark_off = _make_session(spark_factory, f"partdisc-d1-f6-off-{door}", ansi="false")
    rows_off = _sorted_rows(
        _read_door(spark_off, door, str(part_fixtures / layout)).where(predicate)
    )
    if predicate == "CAST(p AS INT) = 99":
        assert rows_off == []
    elif layout == "d1_f6_divzero":
        assert rows_off == [(1, "one", 1)]
    else:
        assert rows_off == [(1, "one", "1")]

    spark_on2 = _make_session(spark_factory, f"partdisc-d1-f6-sc-{door}", ansi="true")
    short_rows = _sorted_rows(
        _read_door(spark_on2, door, str(part_fixtures / "d1_f6_empty_bad")).where(
            "p = '1' AND CAST(p AS INT) = 1"
        )
    )
    assert short_rows == [(1, "one", "1")]


D2_LAYOUTS: dict[str, list[tuple[str, list[tuple[int, str]] | None]]] = {
    "L01_sub1_sub2": [
        ("sub1/a=1/part-0.parquet", [(1, "a")]),
        ("sub2/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L02_root_and_sub": [
        ("a=1/part-0.parquet", [(1, "a")]),
        ("sub/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L03_part_sub_file": [("p=1/good.parquet", [(1, "a")]), ("sub/x.parquet", [(99, "s")])],
    "L04_part_root_file": [("p=1/good.parquet", [(1, "a")]), ("root.parquet", [(99, "r")])],
    "L05_part_sub_hidden": [("p=1/good.parquet", [(1, "a")]), ("sub/_hidden.parquet", [(99, "h")])],
    "L06_part_temporary": [("p=1/good.parquet", [(1, "a")]), ("_temporary/x.parquet", [(99, "t")])],
    "L07_part_temporary_deep": [
        ("p=1/good.parquet", [(1, "a")]),
        ("_temporary/0/x.parquet", [(99, "t")]),
    ],
    "L08_a_sub_b_alone": [("a=1/sub/b=2/part-0.parquet", [(1, "a")])],
    "L09_a_sub_b_same": [
        ("a=1/sub/b=2/part-0.parquet", [(1, "a")]),
        ("a=1/sub/b=3/part-0.parquet", [(2, "b")]),
    ],
    "L10_a_sub_b_diff": [
        ("a=1/sub/b=2/part-0.parquet", [(1, "a")]),
        ("a=2/sub/b=3/part-0.parquet", [(2, "b")]),
    ],
    "L11_deep_same": [
        ("s1/s2/a=1/part-0.parquet", [(1, "a")]),
        ("s1/s2/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L12_deep_diff_depth": [
        ("s1/s2/a=1/part-0.parquet", [(1, "a")]),
        ("s1/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L13_deep_trailing": [
        ("a=1/s1/s2/part-0.parquet", [(1, "a")]),
        ("a=2/s1/part-0.parquet", [(2, "b")]),
    ],
    "L14_deep_mid": [
        ("a=1/s1/b=1/s2/part-0.parquet", [(1, "a")]),
        ("a=1/s1/b=2/s2/part-0.parquet", [(2, "b")]),
    ],
    "L15_base_file": [("s1/a=1/part-0.parquet", [(1, "a")]), ("s1/x.parquet", [(99, "x")])],
    "L16_base_sub_file": [("s1/a=1/part-0.parquet", [(1, "a")]), ("s1/s2/x.parquet", [(99, "x")])],
    "L17_case_base": [
        ("Sub/a=1/part-0.parquet", [(1, "a")]),
        ("sub/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L18_names_vs_dirs": [
        ("a=1/part-0.parquet", [(1, "a")]),
        ("sub/b=2/part-0.parquet", [(2, "b")]),
    ],
    "L19_mixed_depth": [("a=1/b=1/part-0.parquet", [(1, "a")]), ("a=2/part-0.parquet", [(2, "b")])],
    "L20_sub_only_parts": [
        ("sub/a=1/part-0.parquet", [(1, "a")]),
        ("sub/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L22_part_sub_zero": [("p=1/good.parquet", [(1, "a")]), ("sub/empty.parquet", None)],
    "L23_three_bases": [
        ("x/a=1/part-0.parquet", [(1, "a")]),
        ("y/a=2/part-0.parquet", [(2, "b")]),
        ("z/a=3/part-0.parquet", [(3, "c")]),
    ],
    "L24_part_trailing_sub_and_root": [
        ("a=1/part-0.parquet", [(1, "a")]),
        ("a=2/sub/part-0.parquet", [(2, "b")]),
    ],
    "L25_hidden_dot_sub": [("p=1/good.parquet", [(1, "a")]), (".sub/x.parquet", [(99, "d")])],
    "L26_unpart_sub1_sub2_root": [
        ("root.parquet", [(1, "r")]),
        ("sub1/a.parquet", [(2, "s")]),
        ("sub2/b.parquet", [(3, "t")]),
    ],
    "Z01_root_first": [("empty.parquet", None), ("p=1/good.parquet", [(1, "a")])],
    "Z02_root_last": [("zzz.parquet", None), ("p=1/good.parquet", [(1, "a")])],
    "Z03_root_nonempty_first": [
        ("a.parquet", [(99, "r")]),
        ("empty.parquet", None),
        ("p=1/good.parquet", [(1, "a")]),
    ],
    "Z04_part_later": [("p=1/good.parquet", [(1, "a")]), ("p=2/empty.parquet", None)],
    "Z05_part_same_later": [
        ("p=1/a.parquet", [(1, "a")]),
        ("p=1/z.parquet", None),
        ("p=2/b.parquet", [(2, "b")]),
    ],
    "Z06_part_first": [("p=1/empty.parquet", None), ("p=2/good.parquet", [(2, "b")])],
    "Z07_case_order": [("p=1/Z.parquet", None), ("p=1/a.parquet", [(1, "a")])],
    "Z08_unpart_last": [("good.parquet", [(1, "a")]), ("zzz.parquet", None)],
    "Z09_unpart_first": [("empty.parquet", None), ("good.parquet", [(1, "a")])],
    "Z10_unpart_nested_zero_first": [("a/x.parquet", None), ("b.parquet", [(1, "a")])],
    "Z11_unpart_hidden_last": [
        ("_h.parquet", [(99, "h")]),
        ("good.parquet", [(1, "a")]),
        ("zzz.parquet", None),
    ],
    "Z12_part_all_zero": [("p=1/empty.parquet", None)],
    "Z13_root_nonempty_part_zero": [("a.parquet", [(99, "r")]), ("p=1/empty.parquet", None)],
    "Z14_unpart_case_order": [("Z.parquet", None), ("a.parquet", [(1, "a")])],
    "Z15_two_parts_zero_mid": [
        ("p=1/a.parquet", [(1, "a")]),
        ("p=1/m.parquet", None),
        ("p=2/a.parquet", [(2, "b")]),
    ],
    "Z16_part_upper_root": [("A.parquet", None), ("p=1/good.parquet", [(1, "a")])],
    "Z17_unpart_hidden_first": [
        ("_h.parquet", [(99, "h")]),
        ("empty.parquet", None),
        ("good.parquet", [(1, "a")]),
    ],
    "Z18_zero_leaf_types": [("p=1/good.parquet", [(1, "a")]), ("p=x/empty.parquet", None)],
    "Z19_names_before_zero": [("p=1/empty.parquet", None), ("q=2/good.parquet", [(2, "b")])],
    "Z20_dirs_before_zero": [("a=2/empty.parquet", None), ("sub/a=1/good.parquet", [(1, "a")])],
    "Z21_zero_leaf_only_value": [
        ("p=1/good.parquet", [(1, "a")]),
        ("p=2/empty.parquet", None),
        ("p=3/good.parquet", [(3, "c")]),
    ],
    "s1x": [("p=1/part-0.parquet", [(1, "one")]), ("p=x/part-0.parquet", [])],
    "s1xn": [("p=1/part-0.parquet", [(1, "one")]), ("p=x/part-0.parquet", [(2, "bad")])],
    "i10": [("p=1/part-0.parquet", [(1, "one")]), ("p=0/part-0.parquet", [])],
    "iovf": [("p=1/part-0.parquet", [(1, "one")]), ("p=2147483647/part-0.parquet", [])],
    "dbad": [("p=2024-01-02/part-0.parquet", [(1, "ok")]), ("p=notadate/part-0.parquet", [])],
    "ab": [
        ("a=1/b=1/part-0.parquet", [(1, "one")]),
        ("a=x/b=1/part-0.parquet", []),
        ("a=1/b=2/part-0.parquet", [(3, "three")]),
    ],
    "sub_parts": [("sub/a=1/part-0.parquet", [(1, "a")]), ("sub/a=2/part-0.parquet", [(2, "b")])],
}

D2_LAYOUT_ANSWERS = [
    ("L04_part_root_file", "struct<id:bigint,s:string,p:int>", [(1, "a", 1)]),
    ("L05_part_sub_hidden", "struct<id:bigint,s:string,p:int>", [(1, "a", 1)]),
    ("L06_part_temporary", "struct<id:bigint,s:string,p:int>", [(1, "a", 1)]),
    ("L07_part_temporary_deep", "struct<id:bigint,s:string,p:int>", [(1, "a", 1)]),
    ("L08_a_sub_b_alone", "struct<id:bigint,s:string,b:int>", [(1, "a", 2)]),
    ("L09_a_sub_b_same", "struct<id:bigint,s:string,b:int>", [(1, "a", 2), (2, "b", 3)]),
    ("L11_deep_same", "struct<id:bigint,s:string,a:int>", [(1, "a", 1), (2, "b", 2)]),
    ("L13_deep_trailing", "struct<id:bigint,s:string,a:int>", [(1, "a", 1), (2, "b", 2)]),
    ("L14_deep_mid", "struct<id:bigint,s:string,b:int>", [(1, "a", 1), (2, "b", 2)]),
    ("L15_base_file", "struct<id:bigint,s:string,a:int>", [(1, "a", 1)]),
    ("L17_case_base", "struct<id:bigint,s:string,a:int>", [(1, "a", 1), (2, "b", 2)]),
    ("L20_sub_only_parts", "struct<id:bigint,s:string,a:int>", [(1, "a", 1), (2, "b", 2)]),
    (
        "L24_part_trailing_sub_and_root",
        "struct<id:bigint,s:string,a:int>",
        [(1, "a", 1), (2, "b", 2)],
    ),
    ("L25_hidden_dot_sub", "struct<id:bigint,s:string,p:int>", [(1, "a", 1)]),
    ("L26_unpart_sub1_sub2_root", "struct<id:bigint,s:string>", [(1, "r")]),
    ("Z02_root_last", "struct<id:bigint,s:string,p:int>", [(1, "a", 1)]),
    ("Z03_root_nonempty_first", "struct<id:bigint,s:string,p:int>", [(1, "a", 1)]),
    ("Z04_part_later", "struct<id:bigint,s:string,p:int>", [(1, "a", 1)]),
    ("Z05_part_same_later", "struct<id:bigint,s:string,p:int>", [(1, "a", 1), (2, "b", 2)]),
    ("Z08_unpart_last", "struct<id:bigint,s:string>", [(1, "a")]),
    ("Z10_unpart_nested_zero_first", "struct<id:bigint,s:string>", [(1, "a")]),
    ("Z11_unpart_hidden_last", "struct<id:bigint,s:string>", [(1, "a")]),
    ("Z13_root_nonempty_part_zero", "struct<id:bigint,s:string,p:int>", []),
    ("Z15_two_parts_zero_mid", "struct<id:bigint,s:string,p:int>", [(1, "a", 1), (2, "b", 2)]),
    ("Z18_zero_leaf_types", "struct<id:bigint,s:string,p:string>", [(1, "a", "1")]),
    ("Z21_zero_leaf_only_value", "struct<id:bigint,s:string,p:int>", [(1, "a", 1), (3, "c", 3)]),
]

D2_LAYOUT_REFUSALS = [
    ("L01_sub1_sub2", "CONFLICTING_DIRECTORY_STRUCTURES"),
    ("L02_root_and_sub", "CONFLICTING_DIRECTORY_STRUCTURES"),
    ("L03_part_sub_file", "CONFLICTING_DIRECTORY_STRUCTURES"),
    ("L10_a_sub_b_diff", "CONFLICTING_DIRECTORY_STRUCTURES"),
    ("L12_deep_diff_depth", "CONFLICTING_DIRECTORY_STRUCTURES"),
    ("L16_base_sub_file", "CONFLICTING_DIRECTORY_STRUCTURES"),
    ("L18_names_vs_dirs", "CONFLICTING_DIRECTORY_STRUCTURES"),
    ("L19_mixed_depth", "CONFLICTING_PARTITION_COLUMN_NAMES"),
    ("L22_part_sub_zero", "CONFLICTING_DIRECTORY_STRUCTURES"),
    ("L23_three_bases", "CONFLICTING_DIRECTORY_STRUCTURES"),
    ("Z01_root_first", "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"),
    ("Z06_part_first", "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"),
    ("Z07_case_order", "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"),
    ("Z09_unpart_first", "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"),
    ("Z12_part_all_zero", "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"),
    ("Z14_unpart_case_order", "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"),
    ("Z16_part_upper_root", "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"),
    ("Z17_unpart_hidden_first", "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"),
    ("Z19_names_before_zero", "CONFLICTING_PARTITION_COLUMN_NAMES"),
    ("Z20_dirs_before_zero", "CONFLICTING_DIRECTORY_STRUCTURES"),
]

D2_PRUNE_CASES = [
    (
        "P01_cast_and_eq",
        "s1x",
        ("CAST(p AS INT) = 1 AND p = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P01_cast_and_eq",
        "s1x",
        ("CAST(p AS INT) = 1 AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P01_cast_and_eq",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P01_cast_and_eq",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P02_chain_cast_then_eq",
        "s1x",
        ("CAST(p AS INT) = 1", "p = '1'"),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P02_chain_cast_then_eq",
        "s1x",
        ("CAST(p AS INT) = 1", "p = '1'"),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P02_chain_cast_then_eq",
        "s1xn",
        ("CAST(p AS INT) = 1", "p = '1'"),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P02_chain_cast_then_eq",
        "s1xn",
        ("CAST(p AS INT) = 1", "p = '1'"),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    ("P03_cast_and_eq99", "s1x", ("CAST(p AS INT) = 1 AND p = '99'",), "true", "rows", []),
    ("P03_cast_and_eq99", "s1x", ("CAST(p AS INT) = 1 AND p = '99'",), "false", "rows", []),
    ("P03_cast_and_eq99", "s1xn", ("CAST(p AS INT) = 1 AND p = '99'",), "true", "rows", []),
    ("P03_cast_and_eq99", "s1xn", ("CAST(p AS INT) = 1 AND p = '99'",), "false", "rows", []),
    (
        "P04_nullsafe",
        "s1x",
        ("CAST(p AS INT) = 1 AND p <=> '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P04_nullsafe",
        "s1x",
        ("CAST(p AS INT) = 1 AND p <=> '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P04_nullsafe",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p <=> '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P04_nullsafe",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p <=> '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P05_nullsafe_flipped",
        "s1x",
        ("CAST(p AS INT) = 1 AND '1' <=> p",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P05_nullsafe_flipped",
        "s1x",
        ("CAST(p AS INT) = 1 AND '1' <=> p",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P05_nullsafe_flipped",
        "s1xn",
        ("CAST(p AS INT) = 1 AND '1' <=> p",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P05_nullsafe_flipped",
        "s1xn",
        ("CAST(p AS INT) = 1 AND '1' <=> p",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P06_in_single",
        "s1x",
        ("CAST(p AS INT) = 1 AND p IN ('1')",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P06_in_single",
        "s1x",
        ("CAST(p AS INT) = 1 AND p IN ('1')",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P06_in_single",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p IN ('1')",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P06_in_single",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p IN ('1')",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P07_not_ne",
        "s1x",
        ("CAST(p AS INT) = 1 AND NOT (p <> '1')",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P07_not_ne",
        "s1x",
        ("CAST(p AS INT) = 1 AND NOT (p <> '1')",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P07_not_ne",
        "s1xn",
        ("CAST(p AS INT) = 1 AND NOT (p <> '1')",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P07_not_ne",
        "s1xn",
        ("CAST(p AS INT) = 1 AND NOT (p <> '1')",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P08_not_eq_x",
        "s1x",
        ("CAST(p AS INT) = 1 AND NOT (p = 'x')",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P08_not_eq_x",
        "s1x",
        ("CAST(p AS INT) = 1 AND NOT (p = 'x')",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P08_not_eq_x",
        "s1xn",
        ("CAST(p AS INT) = 1 AND NOT (p = 'x')",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P08_not_eq_x",
        "s1xn",
        ("CAST(p AS INT) = 1 AND NOT (p = 'x')",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P09_binding_under_or",
        "s1x",
        ("CAST(p AS INT) = 1 AND (p = '1' OR p = '2')",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P09_binding_under_or",
        "s1x",
        ("CAST(p AS INT) = 1 AND (p = '1' OR p = '2')",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P09_binding_under_or",
        "s1xn",
        ("CAST(p AS INT) = 1 AND (p = '1' OR p = '2')",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P09_binding_under_or",
        "s1xn",
        ("CAST(p AS INT) = 1 AND (p = '1' OR p = '2')",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P10_and_inside_or",
        "s1x",
        ("(CAST(p AS INT) = 1 AND p = '1') OR p = '2'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P10_and_inside_or",
        "s1x",
        ("(CAST(p AS INT) = 1 AND p = '1') OR p = '2'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P10_and_inside_or",
        "s1xn",
        ("(CAST(p AS INT) = 1 AND p = '1') OR p = '2'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P10_and_inside_or",
        "s1xn",
        ("(CAST(p AS INT) = 1 AND p = '1') OR p = '2'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P11_two_bindings_last_2",
        "s1x",
        ("CAST(p AS INT) = 1 AND p = '1' AND p = '2'",),
        "true",
        "rows",
        [],
    ),
    (
        "P11_two_bindings_last_2",
        "s1x",
        ("CAST(p AS INT) = 1 AND p = '1' AND p = '2'",),
        "false",
        "rows",
        [],
    ),
    (
        "P11_two_bindings_last_2",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p = '1' AND p = '2'",),
        "true",
        "rows",
        [],
    ),
    (
        "P11_two_bindings_last_2",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p = '1' AND p = '2'",),
        "false",
        "rows",
        [],
    ),
    (
        "P12_two_bindings_last_1",
        "s1x",
        ("p = '2' AND p = '1' AND CAST(p AS INT) = 1",),
        "true",
        "rows",
        [],
    ),
    (
        "P12_two_bindings_last_1",
        "s1x",
        ("p = '2' AND p = '1' AND CAST(p AS INT) = 1",),
        "false",
        "rows",
        [],
    ),
    (
        "P12_two_bindings_last_1",
        "s1xn",
        ("p = '2' AND p = '1' AND CAST(p AS INT) = 1",),
        "true",
        "rows",
        [],
    ),
    (
        "P12_two_bindings_last_1",
        "s1xn",
        ("p = '2' AND p = '1' AND CAST(p AS INT) = 1",),
        "false",
        "rows",
        [],
    ),
    (
        "P13_upper_cast_col",
        "s1x",
        ("CAST(P AS INT) = 1 AND p = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P13_upper_cast_col",
        "s1x",
        ("CAST(P AS INT) = 1 AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P13_upper_cast_col",
        "s1xn",
        ("CAST(P AS INT) = 1 AND p = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P13_upper_cast_col",
        "s1xn",
        ("CAST(P AS INT) = 1 AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P14_upper_binding_col",
        "s1x",
        ("CAST(p AS INT) = 1 AND P = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P14_upper_binding_col",
        "s1x",
        ("CAST(p AS INT) = 1 AND P = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P14_upper_binding_col",
        "s1xn",
        ("CAST(p AS INT) = 1 AND P = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P14_upper_binding_col",
        "s1xn",
        ("CAST(p AS INT) = 1 AND P = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P15_case_sibling",
        "s1x",
        ("CASE WHEN CAST(p AS INT) = 1 THEN true ELSE false END AND p = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P15_case_sibling",
        "s1x",
        ("CASE WHEN CAST(p AS INT) = 1 THEN true ELSE false END AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P15_case_sibling",
        "s1xn",
        ("CASE WHEN CAST(p AS INT) = 1 THEN true ELSE false END AND p = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P15_case_sibling",
        "s1xn",
        ("CASE WHEN CAST(p AS INT) = 1 THEN true ELSE false END AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P16_fold_raise_no_match",
        "s1x",
        ("CAST(p AS INT) = 1 AND p = 'y'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    ("P16_fold_raise_no_match", "s1x", ("CAST(p AS INT) = 1 AND p = 'y'",), "false", "rows", []),
    (
        "P16_fold_raise_no_match",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p = 'y'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    ("P16_fold_raise_no_match", "s1xn", ("CAST(p AS INT) = 1 AND p = 'y'",), "false", "rows", []),
    (
        "P17_fold_raise_after_binding",
        "s1x",
        ("p = 'y' AND CAST(p AS INT) = 1",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P17_fold_raise_after_binding",
        "s1x",
        ("p = 'y' AND CAST(p AS INT) = 1",),
        "false",
        "rows",
        [],
    ),
    (
        "P17_fold_raise_after_binding",
        "s1xn",
        ("p = 'y' AND CAST(p AS INT) = 1",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P17_fold_raise_after_binding",
        "s1xn",
        ("p = 'y' AND CAST(p AS INT) = 1",),
        "false",
        "rows",
        [],
    ),
    (
        "P18_in_list_before_binding",
        "s1x",
        ("CAST(p AS INT) IN (1, 2) AND p = '1'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P18_in_list_before_binding",
        "s1x",
        ("CAST(p AS INT) IN (1, 2) AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P18_in_list_before_binding",
        "s1xn",
        ("CAST(p AS INT) IN (1, 2) AND p = '1'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P18_in_list_before_binding",
        "s1xn",
        ("CAST(p AS INT) IN (1, 2) AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P19_false_prefix_then_raise",
        "s1x",
        ("p <> 'x' AND CAST(p AS INT) = 1 AND p = 'x'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P19_false_prefix_then_raise",
        "s1x",
        ("p <> 'x' AND CAST(p AS INT) = 1 AND p = 'x'",),
        "false",
        "rows",
        [],
    ),
    (
        "P19_false_prefix_then_raise",
        "s1xn",
        ("p <> 'x' AND CAST(p AS INT) = 1 AND p = 'x'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P19_false_prefix_then_raise",
        "s1xn",
        ("p <> 'x' AND CAST(p AS INT) = 1 AND p = 'x'",),
        "false",
        "rows",
        [],
    ),
    (
        "P20a_guard_ne",
        "s1x",
        ("CAST(p AS INT) = 1 AND p != 'x'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P20a_guard_ne",
        "s1x",
        ("CAST(p AS INT) = 1 AND p != 'x'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P20a_guard_ne",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p != 'x'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P20a_guard_ne",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p != 'x'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P20b_guard_in_two",
        "s1x",
        ("CAST(p AS INT) = 1 AND p IN ('1','2')",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P20b_guard_in_two",
        "s1x",
        ("CAST(p AS INT) = 1 AND p IN ('1','2')",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P20b_guard_in_two",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p IN ('1','2')",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P20b_guard_in_two",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p IN ('1','2')",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P20c_guard_like",
        "s1x",
        ("CAST(p AS INT) = 1 AND p LIKE '1%'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P20c_guard_like",
        "s1x",
        ("CAST(p AS INT) = 1 AND p LIKE '1%'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P20c_guard_like",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p LIKE '1%'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P20c_guard_like",
        "s1xn",
        ("CAST(p AS INT) = 1 AND p LIKE '1%'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P21_eq_then_cast",
        "s1x",
        ("p = '1' AND CAST(p AS INT) = 1",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P21_eq_then_cast",
        "s1x",
        ("p = '1' AND CAST(p AS INT) = 1",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P21_eq_then_cast",
        "s1xn",
        ("p = '1' AND CAST(p AS INT) = 1",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P21_eq_then_cast",
        "s1xn",
        ("p = '1' AND CAST(p AS INT) = 1",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    ("P22_cast_alone", "s1x", ("CAST(p AS INT) = 1",), "true", "raises", "CAST_INVALID_INPUT"),
    ("P22_cast_alone", "s1x", ("CAST(p AS INT) = 1",), "false", "rows", [(1, "one", "1")]),
    ("P22_cast_alone", "s1xn", ("CAST(p AS INT) = 1",), "true", "raises", "CAST_INVALID_INPUT"),
    ("P22_cast_alone", "s1xn", ("CAST(p AS INT) = 1",), "false", "rows", [(1, "one", "1")]),
    ("P23_cast_bool_sibling", "s1x", ("CAST(p AS BOOLEAN) AND p = '1'",), "true", "raises", None),
    (
        "P23_cast_bool_sibling",
        "s1x",
        ("CAST(p AS BOOLEAN) AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    ("P23_cast_bool_sibling", "s1xn", ("CAST(p AS BOOLEAN) AND p = '1'",), "true", "raises", None),
    (
        "P23_cast_bool_sibling",
        "s1xn",
        ("CAST(p AS BOOLEAN) AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P24_chain_eq_then_cast",
        "s1x",
        ("p = '1'", "CAST(p AS INT) = 1"),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P24_chain_eq_then_cast",
        "s1x",
        ("p = '1'", "CAST(p AS INT) = 1"),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P24_chain_eq_then_cast",
        "s1xn",
        ("p = '1'", "CAST(p AS INT) = 1"),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P24_chain_eq_then_cast",
        "s1xn",
        ("p = '1'", "CAST(p AS INT) = 1"),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P25_chain_three",
        "s1x",
        ("CAST(p AS INT) = 1", "id >= 0", "p = '1'"),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P25_chain_three",
        "s1x",
        ("CAST(p AS INT) = 1", "id >= 0", "p = '1'"),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P25_chain_three",
        "s1xn",
        ("CAST(p AS INT) = 1", "id >= 0", "p = '1'"),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P25_chain_three",
        "s1xn",
        ("CAST(p AS INT) = 1", "id >= 0", "p = '1'"),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P26_eq_then_cast_with_data",
        "s1x",
        ("CAST(p AS INT) = 1 AND id >= 0 AND p = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P26_eq_then_cast_with_data",
        "s1x",
        ("CAST(p AS INT) = 1 AND id >= 0 AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P26_eq_then_cast_with_data",
        "s1xn",
        ("CAST(p AS INT) = 1 AND id >= 0 AND p = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P26_eq_then_cast_with_data",
        "s1xn",
        ("CAST(p AS INT) = 1 AND id >= 0 AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P27_explicit_not_eq_prefix",
        "s1x",
        ("NOT (p = 'x') AND CAST(p AS INT) = 1 AND p = 'x'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P27_explicit_not_eq_prefix",
        "s1x",
        ("NOT (p = 'x') AND CAST(p AS INT) = 1 AND p = 'x'",),
        "false",
        "rows",
        [],
    ),
    (
        "P27_explicit_not_eq_prefix",
        "s1xn",
        ("NOT (p = 'x') AND CAST(p AS INT) = 1 AND p = 'x'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P27_explicit_not_eq_prefix",
        "s1xn",
        ("NOT (p = 'x') AND CAST(p AS INT) = 1 AND p = 'x'",),
        "false",
        "rows",
        [],
    ),
    (
        "P28_cast_bool_cmp_sibling",
        "s1x",
        ("CAST(p AS BOOLEAN) = true AND p = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P28_cast_bool_cmp_sibling",
        "s1x",
        ("CAST(p AS BOOLEAN) = true AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P28_cast_bool_cmp_sibling",
        "s1xn",
        ("CAST(p AS BOOLEAN) = true AND p = '1'",),
        "true",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P28_cast_bool_cmp_sibling",
        "s1xn",
        ("CAST(p AS BOOLEAN) = true AND p = '1'",),
        "false",
        "rows",
        [(1, "one", "1")],
    ),
    (
        "P60_nullsafe_ne_prefix",
        "s1x",
        ("NOT (p <=> 'x') AND CAST(p AS INT) = 1 AND p <=> 'x'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P60_nullsafe_ne_prefix",
        "s1x",
        ("NOT (p <=> 'x') AND CAST(p AS INT) = 1 AND p <=> 'x'",),
        "false",
        "rows",
        [],
    ),
    (
        "P60_nullsafe_ne_prefix",
        "s1xn",
        ("NOT (p <=> 'x') AND CAST(p AS INT) = 1 AND p <=> 'x'",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P60_nullsafe_ne_prefix",
        "s1xn",
        ("NOT (p <=> 'x') AND CAST(p AS INT) = 1 AND p <=> 'x'",),
        "false",
        "rows",
        [],
    ),
    ("P30_div_and_eq", "i10", ("1 / p = 1 AND p = 1",), "true", "rows", [(1, "one", 1)]),
    ("P30_div_and_eq", "i10", ("1 / p = 1 AND p = 1",), "false", "rows", [(1, "one", 1)]),
    ("P31_div_and_eq_strlit", "i10", ("1 / p = 1 AND p = '1'",), "true", "rows", [(1, "one", 1)]),
    ("P31_div_and_eq_strlit", "i10", ("1 / p = 1 AND p = '1'",), "false", "rows", [(1, "one", 1)]),
    ("P32_ovf_and_eq", "iovf", ("p + 1 > 0 AND p = 1",), "true", "rows", [(1, "one", 1)]),
    ("P32_ovf_and_eq", "iovf", ("p + 1 > 0 AND p = 1",), "false", "rows", [(1, "one", 1)]),
    ("P33_div_alone", "i10", ("1 / p = 1",), "true", "raises", "DIVIDE_BY_ZERO"),
    ("P33_div_alone", "i10", ("1 / p = 1",), "false", "rows", [(1, "one", 1)]),
    (
        "P34_div_and_in_two",
        "i10",
        ("1 / p = 1 AND p IN (1, 2)",),
        "true",
        "raises",
        "DIVIDE_BY_ZERO",
    ),
    ("P34_div_and_in_two", "i10", ("1 / p = 1 AND p IN (1, 2)",), "false", "rows", [(1, "one", 1)]),
    ("P35_eq_then_div", "i10", ("p = 1 AND 1 / p = 1",), "true", "rows", [(1, "one", 1)]),
    ("P35_eq_then_div", "i10", ("p = 1 AND 1 / p = 1",), "false", "rows", [(1, "one", 1)]),
    ("P36_div_and_eq_flipped", "i10", ("1 / p = 1 AND 1 = p",), "true", "rows", [(1, "one", 1)]),
    ("P36_div_and_eq_flipped", "i10", ("1 / p = 1 AND 1 = p",), "false", "rows", [(1, "one", 1)]),
    (
        "P40_date_and_eq",
        "dbad",
        ("CAST(p AS DATE) = DATE '2024-01-02' AND p = '2024-01-02'",),
        "true",
        "rows",
        [(1, "ok", "2024-01-02")],
    ),
    (
        "P40_date_and_eq",
        "dbad",
        ("CAST(p AS DATE) = DATE '2024-01-02' AND p = '2024-01-02'",),
        "false",
        "rows",
        [(1, "ok", "2024-01-02")],
    ),
    ("P41_date_alone", "dbad", ("CAST(p AS DATE) = DATE '2024-01-02'",), "true", "raises", None),
    (
        "P41_date_alone",
        "dbad",
        ("CAST(p AS DATE) = DATE '2024-01-02'",),
        "false",
        "rows",
        [(1, "ok", "2024-01-02")],
    ),
    (
        "P50_guard_two_cols",
        "ab",
        ("CAST(a AS INT) = 1 AND b = 1",),
        "true",
        "raises",
        "CAST_INVALID_INPUT",
    ),
    (
        "P50_guard_two_cols",
        "ab",
        ("CAST(a AS INT) = 1 AND b = 1",),
        "false",
        "rows",
        [(1, "one", "1", 1)],
    ),
    (
        "P51_two_col_bindings",
        "ab",
        ("CAST(a AS INT) = 1 AND a = '1' AND b = 1",),
        "true",
        "rows",
        [(1, "one", "1", 1)],
    ),
    (
        "P51_two_col_bindings",
        "ab",
        ("CAST(a AS INT) = 1 AND a = '1' AND b = 1",),
        "false",
        "rows",
        [(1, "one", "1", 1)],
    ),
    (
        "P52_two_col_sum",
        "ab",
        ("CAST(a AS INT) + b = 2 AND a = '1' AND b = 1",),
        "true",
        "rows",
        [(1, "one", "1", 1)],
    ),
    (
        "P52_two_col_sum",
        "ab",
        ("CAST(a AS INT) + b = 2 AND a = '1' AND b = 1",),
        "false",
        "rows",
        [(1, "one", "1", 1)],
    ),
    (
        "P53_two_col_bindings_reorder",
        "ab",
        ("CAST(a AS INT) = 1 AND b = 1 AND a = '1'",),
        "true",
        "rows",
        [(1, "one", "1", 1)],
    ),
    (
        "P53_two_col_bindings_reorder",
        "ab",
        ("CAST(a AS INT) = 1 AND b = 1 AND a = '1'",),
        "false",
        "rows",
        [(1, "one", "1", 1)],
    ),
    (
        "P54_partial_substitution",
        "ab",
        ("CAST(a AS INT) = b AND a = '1'",),
        "true",
        "rows",
        [(1, "one", "1", 1)],
    ),
    (
        "P54_partial_substitution",
        "ab",
        ("CAST(a AS INT) = b AND a = '1'",),
        "false",
        "rows",
        [(1, "one", "1", 1)],
    ),
    ("P55_sub_base_prune", "sub_parts", ("a = 2",), "true", "rows", [(2, "b", 2)]),
    ("P55_sub_base_prune", "sub_parts", ("a = 2",), "false", "rows", [(2, "b", 2)]),
]

D2_VIEW_CASES = [
    ("V1_view_cast_outer_eq", "s1x", "true", "rows", [(1, "one", "1")]),
    ("V1_view_cast_outer_eq", "s1x", "false", "rows", [(1, "one", "1")]),
    ("V1_view_cast_outer_eq", "s1xn", "true", "rows", [(1, "one", "1")]),
    ("V1_view_cast_outer_eq", "s1xn", "false", "rows", [(1, "one", "1")]),
    ("V2_view_eq_outer_cast", "s1x", "true", "rows", [(1, "one", "1")]),
    ("V2_view_eq_outer_cast", "s1x", "false", "rows", [(1, "one", "1")]),
    ("V2_view_eq_outer_cast", "s1xn", "true", "rows", [(1, "one", "1")]),
    ("V2_view_eq_outer_cast", "s1xn", "false", "rows", [(1, "one", "1")]),
    ("V3_view_cast_outer_eq99", "s1x", "true", "rows", []),
    ("V3_view_cast_outer_eq99", "s1x", "false", "rows", []),
    ("V3_view_cast_outer_eq99", "s1xn", "true", "rows", []),
    ("V3_view_cast_outer_eq99", "s1xn", "false", "rows", []),
    ("V4_df_view_cast_outer_eq", "s1x", "true", "rows", [(1, "one", "1")]),
    ("V4_df_view_cast_outer_eq", "s1x", "false", "rows", [(1, "one", "1")]),
    ("V4_df_view_cast_outer_eq", "s1xn", "true", "rows", [(1, "one", "1")]),
    ("V4_df_view_cast_outer_eq", "s1xn", "false", "rows", [(1, "one", "1")]),
]


@pytest.fixture(scope="module")
def d2_fixtures(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("parquet_partdisc_d2")
    for name, entries in D2_LAYOUTS.items():
        for rel, rows in entries:
            target = root / name / rel
            if rows is None:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(b"")
            else:
                _write_id_s(target, rows)
    return root


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize(("layout", "expected_schema", "expected_rows"), D2_LAYOUT_ANSWERS)
def test_delta2_layouts_answer_as_spark(
    d2_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
    layout: str,
    expected_schema: str,
    expected_rows: list[tuple[Any, ...]],
) -> None:
    spark = _make_session(spark_factory, f"partdisc-d2-{door}-{ansi}-{layout}", ansi=ansi)
    df = _read_door(spark, door, str(d2_fixtures / layout))
    assert df.schema.simpleString() == expected_schema
    assert _sorted_rows(df) == sorted(expected_rows, key=repr)


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize(("layout", "expected_cond"), D2_LAYOUT_REFUSALS)
def test_delta2_layouts_refuse_as_spark(
    d2_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
    layout: str,
    expected_cond: str,
) -> None:
    spark = _make_session(spark_factory, f"partdisc-d2-ref-{door}-{ansi}-{layout}", ansi=ansi)
    with pytest.raises(PySparkException) as exc_info:
        _read_door(spark, door, str(d2_fixtures / layout)).collect()
    assert exc_info.value.getCondition() == expected_cond


def _where_all(frame: Any, predicates: tuple[str, ...]) -> Any:
    for predicate in predicates:
        frame = frame.where(predicate)
    return frame


def _assert_case(frame_fn: Any, outcome: str, expected: Any) -> None:
    if outcome == "raises":
        with pytest.raises(PySparkException) as exc_info:
            frame_fn().collect()
        error = exc_info.value
        if expected is not None:
            assert error.getCondition() == expected or f"[{expected}]" in str(error)
        return
    assert _sorted_rows(frame_fn()) == sorted(expected, key=repr)


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize(
    ("case", "layout", "predicates", "ansi", "outcome", "expected"), D2_PRUNE_CASES
)
def test_delta2_constant_propagation_matrix(
    d2_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    case: str,
    layout: str,
    predicates: tuple[str, ...],
    ansi: str,
    outcome: str,
    expected: Any,
) -> None:
    spark = _make_session(spark_factory, f"partdisc-d2-cp-{door}-{ansi}-{case}", ansi=ansi)
    path = str(d2_fixtures / layout)
    _assert_case(lambda: _where_all(_read_door(spark, door, path), predicates), outcome, expected)


@pytest.mark.parametrize(("case", "layout", "ansi", "outcome", "expected"), D2_VIEW_CASES)
def test_delta2_constant_propagation_through_views(
    d2_fixtures: Path,
    spark_factory: list[ReparkSession],
    case: str,
    layout: str,
    ansi: str,
    outcome: str,
    expected: Any,
) -> None:
    spark = _make_session(spark_factory, f"partdisc-d2-view-{ansi}-{case}", ansi=ansi)
    source = spark.read.parquet(str(d2_fixtures / layout))
    view_filters = {
        "V1_view_cast_outer_eq": ("CAST(p AS INT) = 1", "p = '1'"),
        "V2_view_eq_outer_cast": ("p = '1'", "CAST(p AS INT) = 1"),
        "V3_view_cast_outer_eq99": ("CAST(p AS INT) = 1", "p = '99'"),
        "V4_df_view_cast_outer_eq": ("CAST(p AS INT) = 1", "p = '1'"),
    }
    inner, outer = view_filters[case]
    if case.startswith("V4"):
        source.where(inner).createOrReplaceTempView("w_d2")
        query = f"SELECT * FROM w_d2 WHERE {outer}"
    else:
        source.createOrReplaceTempView("t_d2")
        spark.sql(f"CREATE OR REPLACE TEMP VIEW v_d2 AS SELECT * FROM t_d2 WHERE {inner}")
        query = f"SELECT * FROM v_d2 WHERE {outer}"
    _assert_case(lambda: spark.sql(query), outcome, expected)
