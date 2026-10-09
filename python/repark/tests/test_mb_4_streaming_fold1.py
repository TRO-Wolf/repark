from collections.abc import Callable
from pathlib import Path

import pytest

from repark import _native
from repark.errors import AnalysisException
from repark.spark.session.session_core import ReparkSession

_MBE8_TEXT = (
    "[REPARK_MICROBATCH.LOCAL_CATALOG_REFUSED] streaming needs a shared catalog; "
    "sc is a local filesystem catalog. Use Glue, S3 Tables, the Postgres catalog or REST"
)


def _memory_session(app: str, warehouse: Path) -> ReparkSession:
    session = ReparkSession.builder.appName(app).getOrCreate()
    session.register_memory_catalog("sc", str(warehouse))
    return session


def _hadoop_session(app: str, warehouse: Path) -> ReparkSession:
    return (
        ReparkSession.builder.appName(app)
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .getOrCreate()
    )


def _stream_tables(spark: ReparkSession) -> tuple[str, str]:
    spark.sql("CREATE NAMESPACE sc.mb4")
    spark.sql("CREATE TABLE sc.mb4.orders (id BIGINT, k STRING)")
    spark.sql("CREATE TABLE sc.mb4.silver (id BIGINT, k STRING)")
    spark.sql("INSERT INTO sc.mb4.orders VALUES (1, 'k1'), (2, 'k0'), (3, 'k1')")
    return ("sc.mb4.orders", "sc.mb4.silver")


def _start_doors(
    spark: ReparkSession, source: str, sink: str, checkpoint: str
) -> dict[str, Callable[[], object]]:
    return {
        "toTable": lambda: (
            spark.readStream.table(source)
            .writeStream.option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(sink)
        ),
        "start(path)": lambda: (
            spark.readStream.table(source)
            .writeStream.format("iceberg")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start(sink)
        ),
        "foreachBatch": lambda: (
            spark.readStream.table(source)
            .writeStream.foreachBatch(lambda frame, epoch: None)
            .option("repark.cdc.sink", sink)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        ),
    }


def _assert_mbe8(run: Callable[[], object], door: str) -> None:
    with pytest.raises(AnalysisException) as excinfo:
        run()
    assert excinfo.value.getCondition() == "REPARK_MICROBATCH.LOCAL_CATALOG_REFUSED", door
    assert excinfo.value.getSqlState() is None, door
    assert str(excinfo.value) == _MBE8_TEXT, door


def test_public_doors_refuse_memory_catalog_mbe8(tmp_path: Path) -> None:
    spark = _memory_session("pytest-mb-4-fold1-mbe8-memory", tmp_path / "wh")
    try:
        source, sink = _stream_tables(spark)
        for door, run in _start_doors(spark, source, sink, str(tmp_path / "ck")).items():
            _assert_mbe8(run, door)
    finally:
        spark.stop()


def test_public_doors_refuse_hadoop_catalog_mbe8(tmp_path: Path) -> None:
    spark = _hadoop_session("pytest-mb-4-fold1-mbe8-hadoop", tmp_path / "wh")
    try:
        source, sink = _stream_tables(spark)
        for door, run in _start_doors(spark, source, sink, str(tmp_path / "ck")).items():
            _assert_mbe8(run, door)
    finally:
        spark.stop()


def test_test_seam_runs_on_memory_catalog(tmp_path: Path) -> None:
    spark = _memory_session("pytest-mb-4-fold1-mbe8-seam", tmp_path / "wh")
    try:
        _native._streaming_tests_allow_local_catalog(spark._ensure_alive())
        source, sink = _stream_tables(spark)
        query = (
            spark.readStream.table(source)
            .writeStream.option("checkpointLocation", str(tmp_path / "ck"))
            .trigger(availableNow=True)
            .toTable(sink)
        )
        assert query.awaitTermination() is None
        assert spark.table(sink).count() == 3
    finally:
        spark.stop()


def test_test_seam_not_referenced_by_public_package() -> None:
    assert callable(_native._streaming_tests_allow_local_catalog)
    package = Path(__file__).resolve().parents[1] / "src" / "repark"
    hits = sorted(
        str(path.relative_to(package))
        for path in package.rglob("*.py")
        if "_streaming_tests_allow_local_catalog" in path.read_text(encoding="utf-8")
    )
    assert hits == []
