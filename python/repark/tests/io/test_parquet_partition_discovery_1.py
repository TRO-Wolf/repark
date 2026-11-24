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
