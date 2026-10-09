from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from test_v3_lineage_columns import _PART_DV_DEST, _PART_DV_SRC, _materialize

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with memory catalogs ``sc`` and ``ice``."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-empty-projection-count-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", tmp_path / "wh")
    session.register_memory_catalog("ice", tmp_path / "wh-ice")
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql("CREATE NAMESPACE ice.sales")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _snaps(session: Any, table: str) -> list[int]:
    arrow = session.sql(f"SELECT snapshot_id, committed_at FROM {table}.snapshots").to_arrow()
    pairs = sorted(
        zip(
            arrow.column("snapshot_id").to_pylist(),
            arrow.column("committed_at").to_pylist(),
            strict=True,
        ),
        key=lambda pair: (pair[1], pair[0]),
    )
    return [pair[0] for pair in pairs]


def _seed(session: Any, table: str, version: str = "2", props: str = "") -> list[int]:
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}'{props})"
    )
    session.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    session.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x')")
    session.sql(f"INSERT INTO {table} VALUES (4, 'd', 'y'), (5, 'e', 'x')")
    return _snaps(session, table)


def _incremental(session: Any, table: str, **options: Any) -> Any:
    reader = session.read.format("iceberg")
    for key, value in options.items():
        reader = reader.option(key.replace("_", "-"), str(value))
    return reader.load(table)


def _count_value(frame: Any) -> int:
    arrow = frame.to_arrow()
    assert arrow.schema.field(0).type == pa.int64()
    values = arrow.column(0).to_pylist()
    assert len(values) == 1
    return int(values[0])


@pytest.fixture
def partdv(spark: Any) -> Iterator[None]:
    """Register the v3 partitioned-DV fixture at the path its metadata names."""
    with _materialize(_PART_DV_SRC, _PART_DV_DEST) as metadata_file:
        spark.sql(
            "CALL ice.system.register_table("
            f"table => 'sales.partdv', metadata_file => '{metadata_file}')"
        )
        yield


def test_changelog_count_star_answers_five(spark: Any) -> None:
    """``SELECT COUNT(*)`` over ``t.changes`` answers the changelog row count.

    pins: empty-projection-count-1/C-002
    """
    table = "sc.ns.epc1_ch"
    _seed(spark, table)
    assert _count_value(spark.sql(f"SELECT COUNT(*) FROM {table}.changes")) == 5


def test_changelog_count_one_select_one_and_df_count(spark: Any) -> None:
    """Every empty-projection spelling over ``t.changes`` answers five.

    pins: empty-projection-count-1/C-002
    """
    table = "sc.ns.epc1_ch1"
    _seed(spark, table)
    assert _count_value(spark.sql(f"SELECT COUNT(1) FROM {table}.changes")) == 5
    assert spark.sql(f"SELECT 1 FROM {table}.changes").to_arrow().num_rows == 5
    assert spark.sql(f"SELECT * FROM {table}.changes").count() == 5


def test_changelog_count_star_single_append(spark: Any) -> None:
    """``COUNT(*)`` over a one-file changelog answers two.

    pins: empty-projection-count-1/C-002
    """
    table = "sc.ns.epc1_ch_single"
    spark.sql(f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    assert _count_value(spark.sql(f"SELECT COUNT(*) FROM {table}.changes")) == 2


def test_changelog_count_star_many_appends(spark: Any) -> None:
    """``COUNT(*)`` over a ten-file changelog answers twenty.

    pins: empty-projection-count-1/C-002
    """
    table = "sc.ns.epc1_ch_many"
    spark.sql(f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg")
    for batch in range(10):
        spark.sql(f"INSERT INTO {table} VALUES ({2 * batch + 1}, 'a'), ({2 * batch + 2}, 'b')")
    assert _count_value(spark.sql(f"SELECT COUNT(*) FROM {table}.changes")) == 20


def test_changelog_count_star_v3(spark: Any) -> None:
    """``COUNT(*)`` over a v3 changelog answers four.

    pins: empty-projection-count-1/C-002
    """
    table = "sc.ns.epc1_ch_v3"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='3')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    spark.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x'), (4, 'd', 'y')")
    assert _count_value(spark.sql(f"SELECT COUNT(*) FROM {table}.changes")) == 4


def test_changelog_count_star_over_merge_on_read_delete(spark: Any) -> None:
    """``COUNT(*)`` over a changelog holding a delete answers the row form's count.

    pins: empty-projection-count-1/C-002
    """
    table = "sc.ns.epc1_ch_del"
    mor = (
        ", 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read'"
        ", 'write.merge.mode'='merge-on-read'"
    )
    _seed(spark, table, props=mor)
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    spark.sql(f"INSERT INTO {table} VALUES (6, 'f', 'x')")
    expected = spark.sql(f"SELECT id, _change_type FROM {table}.changes").to_arrow().num_rows
    assert expected == 7
    assert _count_value(spark.sql(f"SELECT COUNT(*) FROM {table}.changes")) == expected


def test_changelog_exists_counts_one(spark: Any) -> None:
    """An ``EXISTS`` probe over ``t.changes`` keeps its single outer row.

    pins: empty-projection-count-1/C-002
    """
    table = "sc.ns.epc1_ch_exists"
    _seed(spark, table)
    assert (
        _count_value(
            spark.sql(
                "SELECT COUNT(*) FROM (SELECT 1 AS one) AS t "
                f"WHERE EXISTS (SELECT * FROM {table}.changes)"
            )
        )
        == 1
    )


def test_incremental_count_star_window(spark: Any) -> None:
    """``COUNT(*)`` over an incremental window answers the appended rows.

    pins: empty-projection-count-1/C-003
    """
    table = "sc.ns.epc1_inc"
    ids = _seed(spark, table)
    frame = _incremental(spark, table, start_snapshot_id=ids[0], end_snapshot_id=ids[2])
    frame.createOrReplaceTempView("v_epc1_inc")
    assert _count_value(spark.sql("SELECT COUNT(*) FROM v_epc1_inc")) == 3


def test_incremental_df_count_window(spark: Any) -> None:
    """``df.count()`` over an incremental window answers three.

    pins: empty-projection-count-1/C-003
    """
    table = "sc.ns.epc1_inc_df"
    ids = _seed(spark, table)
    frame = _incremental(spark, table, start_snapshot_id=ids[0], end_snapshot_id=ids[2])
    assert frame.count() == 3


def test_incremental_count_star_many_appends(spark: Any) -> None:
    """``COUNT(*)`` over a ten-snapshot window answers eighteen.

    pins: empty-projection-count-1/C-003
    """
    table = "sc.ns.epc1_inc_many"
    spark.sql(f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg")
    for batch in range(10):
        spark.sql(f"INSERT INTO {table} VALUES ({2 * batch + 1}, 'a'), ({2 * batch + 2}, 'b')")
    ids = _snaps(spark, table)
    frame = _incremental(spark, table, start_snapshot_id=ids[0], end_snapshot_id=ids[-1])
    frame.createOrReplaceTempView("v_epc1_inc_many")
    assert _count_value(spark.sql("SELECT COUNT(*) FROM v_epc1_inc_many")) == 18


def test_incremental_count_star_v3(spark: Any) -> None:
    """``COUNT(*)`` over a v3 incremental window answers two.

    pins: empty-projection-count-1/C-003
    """
    table = "sc.ns.epc1_inc_v3"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='3')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    spark.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x'), (4, 'd', 'y')")
    ids = _snaps(spark, table)
    frame = _incremental(spark, table, start_snapshot_id=ids[0], end_snapshot_id=ids[1])
    frame.createOrReplaceTempView("v_epc1_inc_v3")
    assert _count_value(spark.sql("SELECT COUNT(*) FROM v_epc1_inc_v3")) == 2


def test_incremental_count_star_over_delete(spark: Any) -> None:
    """``COUNT(*)`` over a window holding a delete skips the delete snapshot.

    pins: empty-projection-count-1/C-003
    """
    table = "sc.ns.epc1_inc_del"
    mor = (
        ", 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read'"
        ", 'write.merge.mode'='merge-on-read'"
    )
    _seed(spark, table, props=mor)
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    spark.sql(f"INSERT INTO {table} VALUES (6, 'f', 'x')")
    ids = _snaps(spark, table)
    frame = _incremental(spark, table, start_snapshot_id=ids[0])
    frame.createOrReplaceTempView("v_epc1_inc_del")
    assert _count_value(spark.sql("SELECT COUNT(*) FROM v_epc1_inc_del")) == 4


def test_incremental_exists_counts_one(spark: Any) -> None:
    """An ``EXISTS`` probe over an incremental window keeps its single outer row.

    pins: empty-projection-count-1/C-003
    """
    table = "sc.ns.epc1_inc_exists"
    ids = _seed(spark, table)
    frame = _incremental(spark, table, start_snapshot_id=ids[0], end_snapshot_id=ids[2])
    frame.createOrReplaceTempView("v_epc1_inc_exists")
    assert (
        _count_value(
            spark.sql(
                "SELECT COUNT(*) FROM (SELECT 1 AS one) AS t "
                "WHERE EXISTS (SELECT * FROM v_epc1_inc_exists)"
            )
        )
        == 1
    )


def test_lineage_count_star_alias_over_v3_partdv(spark: Any, partdv: None) -> None:
    """``SELECT COUNT(*) AS _row_id`` over the v3 DV fixture answers four.

    pins: empty-projection-count-1/C-004
    """
    assert _count_value(spark.sql("SELECT COUNT(*) AS _row_id FROM ice.sales.partdv")) == 4


def test_lineage_df_count_over_row_id(spark: Any, partdv: None) -> None:
    """``df.count()`` over a lineage projection answers four.

    pins: empty-projection-count-1/C-004
    """
    assert spark.sql("SELECT _row_id FROM ice.sales.partdv").count() == 4


def test_lineage_count_star_alias_over_v2_still_answers(spark: Any) -> None:
    """``SELECT COUNT(*) AS _row_id`` over a v2 table still answers one.

    pins: empty-projection-count-1/C-004, C-007
    """
    spark.sql("CREATE TABLE ice.sales.epc1_lin2 (id INT, name STRING) USING iceberg")
    spark.sql("INSERT INTO ice.sales.epc1_lin2 VALUES (1, 'a')")
    assert _count_value(spark.sql("SELECT COUNT(*) AS _row_id FROM ice.sales.epc1_lin2")) == 1


def test_empty_window_counts_stay_zero(spark: Any, partdv: None) -> None:
    """A ``COUNT(*)`` matching no rows answers zero on all three readers.

    pins: empty-projection-count-1/C-007
    """
    table = "sc.ns.epc1_empty"
    ids = _seed(spark, table)
    frame = _incremental(spark, table, start_snapshot_id=ids[0], end_snapshot_id=ids[2])
    frame.createOrReplaceTempView("v_epc1_empty")
    assert _count_value(spark.sql(f"SELECT COUNT(*) FROM {table}.changes WHERE 1 = 0")) == 0
    assert _count_value(spark.sql("SELECT COUNT(*) FROM v_epc1_empty WHERE 1 = 0")) == 0
    assert (
        _count_value(spark.sql("SELECT COUNT(*) AS _row_id FROM ice.sales.partdv WHERE 1 = 0")) == 0
    )


def test_filtered_counts_unchanged(spark: Any, partdv: None) -> None:
    """Filtered ``COUNT(*)`` answers stay as measured on the base tree.

    pins: empty-projection-count-1/C-007
    """
    table = "sc.ns.epc1_filt"
    ids = _seed(spark, table)
    frame = _incremental(spark, table, start_snapshot_id=ids[0], end_snapshot_id=ids[2])
    frame.createOrReplaceTempView("v_epc1_filt")
    assert _count_value(spark.sql(f"SELECT COUNT(*) FROM {table}.changes WHERE id > 2")) == 3
    assert _count_value(spark.sql("SELECT COUNT(*) FROM v_epc1_filt WHERE id > 3")) == 2
    assert (
        _count_value(spark.sql("SELECT COUNT(*) FROM ice.sales.partdv WHERE _row_id IS NOT NULL"))
        == 4
    )
    assert _count_value(spark.sql("SELECT COUNT(*) FROM ice.sales.partdv WHERE id > 2")) == 3


def test_lineage_exists_still_refuses_v3_rowid2(spark: Any, partdv: None) -> None:
    """A lineage ``EXISTS`` subquery still refuses with the composed-statement class.

    pins: empty-projection-count-1/C-007
    """
    with pytest.raises(Exception, match="V3-ROWID-2"):
        spark.sql(
            "SELECT COUNT(*) FROM (SELECT 1 AS one) AS t "
            "WHERE EXISTS (SELECT _row_id FROM ice.sales.partdv)"
        ).to_arrow()


def test_plain_counts_unchanged(spark: Any) -> None:
    """Neighbouring ``COUNT(*)`` answers stay as measured on the base tree.

    pins: empty-projection-count-1/C-007
    """
    table = "sc.ns.epc1_plain"
    _seed(spark, table)
    assert _count_value(spark.sql(f"SELECT COUNT(*) FROM {table}")) == 5
    assert _count_value(spark.sql(f"SELECT COUNT(*) FROM {table}.snapshots")) == 3
    assert _count_value(spark.sql(f"SELECT COUNT(_file) FROM {table}")) == 5
    spark.sql(
        "CALL sc.system.create_changelog_view("
        "table => 'ns.epc1_plain', changelog_view => 'v_epc1_clv')"
    )
    assert _count_value(spark.sql("SELECT COUNT(*) FROM v_epc1_clv")) == 5
