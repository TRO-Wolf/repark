from __future__ import annotations

import json
import shutil
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("parquet_int96_as_ntz_1")
ORACLE: dict[str, Any] = json.loads(
    Path(__file__).with_name("parquet_int96_as_ntz_1_oracle.json").read_text()
)
FIXTURES = Path(__file__).parent.with_name("fixtures") / "parquet_int96_as_ntz_1"
ZONES: list[str] = ORACLE["zones"]
READ_ROUTES = [route for route in ORACLE["routes"] if route != "sqlpath"]


def attempt(action: Callable[[], Any]) -> Any:
    try:
        return action()
    except Exception as error:
        return f"ERR {type(error).__name__}"


def rows_of(spark: Any, query: str) -> Any:
    try:
        return [list(row) for row in spark.sql(query).collect()]
    except Exception as error:
        return f"ERR {type(error).__name__}"


def route_frame(spark: Any, route: str, path: str) -> Any:
    if route == "parquet":
        return spark.read.parquet(path)
    if route == "format":
        return spark.read.format("parquet").load(path)
    if route == "sqlpath":
        return spark.sql(f"SELECT * FROM parquet.`{path}`")
    spark.read.parquet(path).createOrReplaceTempView("route_view")
    return spark.sql("SELECT * FROM route_view")


def schema_of(spark: Any, route: str, path: str) -> str:
    try:
        return route_frame(spark, route, path).schema.simpleString()
    except Exception as error:
        return f"ERR {type(error).__name__}"


def register_views(spark: Any) -> None:
    for view, name in ORACLE["views"].items():
        spark.catalog.dropTempView(view)
        attempt(
            lambda view=view, name=name: spark.read.parquet(
                str(FIXTURES / name)
            ).createOrReplaceTempView(view)
        )


def zone_tag(zone: str) -> str:
    return zone.replace("/", "_")


def measure_reads(spark: Any, zone: str, found: dict[str, Any]) -> None:
    spark.conf.set("spark.sql.session.timeZone", zone)
    register_views(spark)
    for route in ORACLE["routes"]:
        found[f"schema/{route}/{zone}"] = schema_of(spark, route, str(FIXTURES / "spark_int96"))
    for name, query in {**ORACLE["pins"], **ORACLE["controls"], **ORACLE["gap_pins"]}.items():
        found[f"rows/{name}/{zone}"] = rows_of(spark, query)


def read_back(spark: Any, out: Path) -> list[Any]:
    frame = spark.read.parquet(str(out))
    frame.createOrReplaceTempView("w")
    return [frame.schema.simpleString(), rows_of(spark, ORACLE["write_read"])]


def read_table(spark: Any, table: str) -> list[Any]:
    return [
        spark.table(table).schema.simpleString(),
        rows_of(spark, ORACLE["iceberg_read"].format(table=table)),
    ]


def measure_writes(spark: Any, root: Path, mode: str, found: dict[str, Any]) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lc.db")
    for written in ZONES:
        spark.conf.set("spark.sql.session.timeZone", written)
        register_views(spark)
        out = root / f"w_{zone_tag(written)}_{mode}"
        table = f"lc.db.w_{zone_tag(written).lower()}_{mode}"
        attempt(
            lambda out=out: (
                spark.sql(ORACLE["write_source"]).write.mode("overwrite").parquet(str(out))
            )
        )
        attempt(
            lambda table=table: spark.sql(
                f"CREATE TABLE {table} USING iceberg AS {ORACLE['iceberg_source']}"
            )
        )
        for read in ZONES:
            spark.conf.set("spark.sql.session.timeZone", read)
            found[f"write_parquet/{written}/{read}"] = attempt(
                lambda out=out: read_back(spark, out)
            )
            found[f"write_iceberg/{written}/{read}"] = attempt(
                lambda table=table: read_table(spark, table)
            )


def append_once(spark: Any, out: Path, case: list[str]) -> list[Any]:
    dst, query, read = case
    shutil.copytree(FIXTURES / dst, out)
    source = spark.sql(query) if query else spark.read.parquet(str(FIXTURES / dst))
    source.write.mode("append").parquet(str(out))
    return read_cold(spark, out, read)


def read_cold(spark: Any, out: Path, read: str) -> list[Any]:
    frame = spark.read.parquet(str(out))
    frame.createOrReplaceTempView("w")
    return [frame.schema.simpleString(), rows_of(spark, read)]


def refused_append(spark: Any, out: Path, case: list[str]) -> list[Any]:
    dst, query, read = case
    shutil.copytree(FIXTURES / dst, out)
    source = spark.sql(query) if query else spark.read.parquet(str(FIXTURES / dst))
    outcome = attempt(lambda: source.write.mode("append").parquet(str(out)))
    return [outcome, attempt(lambda: read_cold(spark, out, read))]


def measure_appends(spark: Any, root: Path, mode: str, found: dict[str, Any]) -> None:
    for zone in ORACLE["append_zones"]:
        spark.conf.set("spark.sql.session.timeZone", zone)
        for name, case in ORACLE["appends"].items():
            out = root / f"append_{name}_{zone_tag(zone)}_{mode}"
            found[f"append/{name}/{zone}"] = attempt(
                lambda out=out, case=case: append_once(spark, out, case)
            )
        for name, case in ORACLE["refusals"].items():
            out = root / f"refusal_{name}_{zone_tag(zone)}_{mode}"
            found[f"refusal/{name}/{zone}"] = attempt(
                lambda out=out, case=case: refused_append(spark, out, case)
            )


LATER = "TIMESTAMP'2025-01-16 00:00:00'"
LAYOUTS: dict[str, tuple[str, str, str, list[int]]] = {
    "naive_hint_utc_literal": (
        "pyarrow_int96_naive",
        f"SELECT 9 AS id, {LATER} AS ts",
        "refuse",
        list(range(1, 8)),
    ),
    "naive_hint_read_then_append": (
        "pyarrow_int96_naive",
        "",
        "append",
        sorted(list(range(1, 8)) * 2),
    ),
    "naive_hint_ntz_literal": (
        "pyarrow_int96_naive",
        "SELECT 9 AS id, TIMESTAMP_NTZ'2025-01-16 00:00:00' AS ts",
        "append",
        [*range(1, 8), 9],
    ),
    "fixed_size_list_read_then_append": ("", "", "append", [1, 1, 2, 2]),
}


def fixed_size_list_seed(root: Path) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    seed = root / "fixed_size_list_seed"
    seed.mkdir(parents=True, exist_ok=True)
    pair = pa.struct([("a", pa.int32()), ("b", pa.int32())])
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int32()),
            "f": pa.array([[{"a": 1, "b": 2}, {"a": 3, "b": 4}]] * 2, pa.list_(pair, 2)),
            "ts": pa.array(
                [1_736_902_800_000_000_000, 1_751_371_200_000_000_000], pa.timestamp("ns")
            ),
        }
    )
    pq.write_table(table, str(seed / "part-00000.parquet"), use_deprecated_int96_timestamps=True)
    return seed


def layout_ids(spark: Any, out: Path) -> list[Any]:
    frame = spark.read.parquet(str(out))
    return sorted(row[0] for row in frame.select("id").collect())


def layout_append(spark: Any, seed: Path, out: Path, query: str) -> list[Any]:
    shutil.copytree(seed, out)
    source = spark.sql(query) if query else spark.read.parquet(str(seed))
    outcome = attempt(lambda: source.write.mode("append").parquet(str(out)))
    return [outcome, attempt(lambda: layout_ids(spark, out))]


def measure_layouts(spark: Any, root: Path, mode: str, found: dict[str, Any]) -> None:
    fixed = fixed_size_list_seed(root)
    for zone in ORACLE["append_zones"]:
        spark.conf.set("spark.sql.session.timeZone", zone)
        for name, (dst, query, _, _) in LAYOUTS.items():
            seed = FIXTURES / dst if dst else fixed
            out = root / f"layout_{name}_{zone_tag(zone)}_{mode}"
            found[f"layout/{name}/{zone}"] = layout_append(spark, seed, out, query)


@pytest.fixture(scope="module", params=["true", "false"])
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, Any]]]:
    mode = request.param
    root = tmp_path_factory.mktemp(f"parquet-int96-{mode}")
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-parquet-int96-as-ntz-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "wh"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", mode)
        .getOrCreate()
    )
    found: dict[str, Any] = {}
    for zone in ZONES:
        measure_reads(session, zone, found)
    measure_writes(session, root, mode, found)
    measure_appends(session, root, mode, found)
    measure_layouts(session, root, mode, found)
    session.stop()
    _reset_active_session_for_tests()
    yield mode, found


def assert_cells(measured: tuple[str, dict[str, Any]], keys: list[str]) -> None:
    mode, found = measured
    truth = ORACLE["truth"][mode]
    for key in keys:
        assert found[key] == truth[key], key


@pytest.mark.parametrize("route", READ_ROUTES)
def test_every_parquet_route_infers_a_spark_int96_leaf_as_timestamp(
    measured: tuple[str, dict[str, Any]], route: str
) -> None:
    assert_cells(measured, [f"schema/{route}/{zone}" for zone in ZONES])


@pytest.mark.xfail(strict=True, reason="SQL over a parquet.`<path>` is not a RePark route")
def test_sql_over_a_parquet_path_infers_a_spark_int96_leaf_as_timestamp(
    measured: tuple[str, dict[str, Any]],
) -> None:
    assert_cells(measured, [f"schema/sqlpath/{zone}" for zone in ZONES])


@pytest.mark.parametrize("name", sorted(ORACLE["pins"]))
def test_an_int96_leaf_reads_as_an_instant_in_the_session_zone(
    measured: tuple[str, dict[str, Any]], name: str
) -> None:
    assert_cells(measured, [f"rows/{name}/{zone}" for zone in ZONES])


@pytest.mark.parametrize("name", sorted(ORACLE["controls"]))
def test_int64_leaves_and_zone_free_answers_do_not_move(
    measured: tuple[str, dict[str, Any]], name: str
) -> None:
    assert_cells(measured, [f"rows/{name}/{zone}" for zone in ZONES])


@pytest.mark.parametrize("sink", ["write_parquet", "write_iceberg"])
def test_a_write_after_the_read_stores_the_instant(
    measured: tuple[str, dict[str, Any]], sink: str
) -> None:
    assert_cells(measured, [f"{sink}/{written}/{read}" for written in ZONES for read in ZONES])


@pytest.mark.parametrize("case", sorted(ORACLE["appends"]))
def test_a_path_append_onto_a_spark_int96_directory_reads_back_as_spark(
    measured: tuple[str, dict[str, Any]], case: str
) -> None:
    assert_cells(measured, [f"append/{case}/{zone}" for zone in ORACLE["append_zones"]])


@pytest.mark.parametrize("case", sorted(ORACLE["refusals"]))
def test_a_map_append_onto_a_spark_int96_directory_refuses_and_leaves_it_readable(
    measured: tuple[str, dict[str, Any]], case: str
) -> None:
    mode, found = measured
    for zone in ORACLE["append_zones"]:
        outcome, back = found[f"refusal/{case}/{zone}"]
        assert outcome == "ERR AnalysisException", zone
        assert back == ORACLE["truth"][mode][f"refusal/{case}/{zone}"], zone


@pytest.mark.xfail(
    strict=True, reason="an INT96 leaf under a zone-less embedded Arrow hint stays NTZ"
)
@pytest.mark.parametrize("name", sorted(ORACLE["gap_pins"]))
def test_an_int96_leaf_under_a_zone_less_arrow_hint_reads_as_an_instant(
    measured: tuple[str, dict[str, Any]], name: str
) -> None:
    assert_cells(measured, [f"rows/{name}/{zone}" for zone in ZONES])


@pytest.mark.parametrize("gap", sorted(ORACLE["gaps"]))
def test_a_recorded_gap_keeps_its_card_and_its_spark_answer(gap: str) -> None:
    record = ORACLE["gaps"][gap]
    assert (Path(__file__).resolve().parents[4] / record["card"]).is_file()
    for mode in ("true", "false"):
        spark_rows = ORACLE["truth"][mode][record["cell"]]
        assert [row[0] for row in spark_rows] == [row[0] for row in record["repark"]]
        assert all(row[1] is not None for row in spark_rows)
        assert all(row[1] is None for row in record["repark"])


@pytest.mark.parametrize("case", sorted(LAYOUTS))
def test_an_int96_append_follows_the_arrow_hint_and_every_list_layout(
    measured: tuple[str, dict[str, Any]], case: str
) -> None:
    _, found = measured
    _, _, expected, ids = LAYOUTS[case]
    for zone in ORACLE["append_zones"]:
        outcome, back = found[f"layout/{case}/{zone}"]
        if expected == "refuse":
            assert outcome == "ERR AnalysisException", zone
        else:
            assert outcome is None, (zone, outcome)
        assert back == ids, zone
