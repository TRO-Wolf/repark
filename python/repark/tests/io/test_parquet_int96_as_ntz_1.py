from __future__ import annotations

import json
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
    for name, query in {**ORACLE["pins"], **ORACLE["controls"]}.items():
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
