"""I6 / R-ALTER-TABLE — Spark ALTER TABLE schema evolution facade pins.

READY core: ADD/DROP/RENAME COLUMN + SET/UNSET TBLPROPERTIES (already landed).
Stretch: TYPE widen with narrow-refuse twin; DROP NOT NULL.
Tests use fully-qualified ``catalog.ns.table`` only (F1 — no bare-name dependency).

Schema-equality + read-after: added→NULL, rename→data intact / field-id.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-alter-table").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _schema_names_types(table: pa.Table) -> list[tuple[str, str]]:
    return [(field.name, str(field.type)) for field in table.schema]


def test_alter_add_rename_drop_column_schema_and_read_after(spark: ReparkSession) -> None:
    """ADD (COMMENT + AFTER) → NULL on existing rows; RENAME keeps data; DROP removes column."""
    spark.sql("CREATE TABLE mem.ns.ev (id INT, name STRING) USING iceberg")
    spark.sql("INSERT INTO mem.ns.ev VALUES (1, 'a'), (2, 'b')")

    spark.sql("ALTER TABLE mem.ns.ev ADD COLUMN note STRING COMMENT 'free text' AFTER id")
    after_add = spark.sql("SELECT * FROM mem.ns.ev").to_arrow()
    assert _schema_names_types(after_add) == [
        ("id", "int32"),
        ("note", "string"),
        ("name", "string"),
    ]
    assert all(value is None for value in after_add.column("note").to_pylist())

    ids_before = after_add.column("id").to_pylist()
    spark.sql("ALTER TABLE mem.ns.ev RENAME COLUMN id TO event_id")
    after_rename = spark.sql("SELECT event_id, name FROM mem.ns.ev").to_arrow()
    assert [field.name for field in after_rename.schema] == ["event_id", "name"]
    assert after_rename.column("event_id").to_pylist() == ids_before

    spark.sql("ALTER TABLE mem.ns.ev DROP COLUMN note")
    after_drop = spark.sql("SELECT * FROM mem.ns.ev").to_arrow()
    assert [field.name for field in after_drop.schema] == ["event_id", "name"]


def test_alter_add_columns_plural_and_first(spark: ReparkSession) -> None:
    spark.sql("CREATE TABLE mem.ns.plural (id BIGINT) USING iceberg")
    spark.sql("ALTER TABLE mem.ns.plural ADD COLUMNS (a INT, b STRING)")
    arrow = spark.sql("SELECT * FROM mem.ns.plural").to_arrow()
    names = [field.name for field in arrow.schema]
    assert "a" in names and "b" in names

    spark.sql("ALTER TABLE mem.ns.plural ADD COLUMN lead BOOLEAN FIRST")
    arrow2 = spark.sql("SELECT * FROM mem.ns.plural").to_arrow()
    assert arrow2.schema.field(0).name == "lead"


def test_alter_column_type_widen_and_narrow_refuse(spark: ReparkSession) -> None:
    spark.sql("CREATE TABLE mem.ns.widen (n INT, label STRING) USING iceberg")
    spark.sql("INSERT INTO mem.ns.widen VALUES (1, 'a'), (2, 'b')")
    spark.sql("ALTER TABLE mem.ns.widen ALTER COLUMN n TYPE BIGINT")
    arrow = spark.sql("SELECT n FROM mem.ns.widen").to_arrow()
    assert str(arrow.schema.field(0).type) == "int64"
    assert sorted(arrow.column("n").to_pylist()) == [1, 2]

    with pytest.raises((AnalysisException, UnsupportedOperationException, Exception)) as caught:
        spark.sql("ALTER TABLE mem.ns.widen ALTER COLUMN n TYPE INT")
    message = str(caught.value).lower()
    assert "cannot" in message or "promote" in message or "type" in message


def test_alter_column_drop_not_null(spark: ReparkSession) -> None:
    spark.sql("CREATE TABLE mem.ns.req (id BIGINT NOT NULL, name STRING) USING iceberg")
    spark.sql("ALTER TABLE mem.ns.req ALTER COLUMN id DROP NOT NULL")
    # Insert a NULL into the relaxed column — proves optional at the write path.
    spark.sql("INSERT INTO mem.ns.req VALUES (NULL, 'x')")
    arrow = spark.sql("SELECT id FROM mem.ns.req").to_arrow()
    assert None in arrow.column("id").to_pylist()


def test_alter_unsupported_forms_refuse_loud(spark: ReparkSession) -> None:
    spark.sql("CREATE TABLE mem.ns.loud (id INT, name STRING) USING iceberg")
    # I7 identity-trap twin: same-name incompatible type on REPLACE COLUMNS.
    with pytest.raises((UnsupportedOperationException, AnalysisException, Exception)) as caught:
        spark.sql("ALTER TABLE mem.ns.loud REPLACE COLUMNS (id STRING, name STRING)")
    message = str(caught.value).lower()
    assert "identity trap" in message or "replace columns" in message or "not supported" in message

    with pytest.raises((UnsupportedOperationException, AnalysisException, Exception)) as caught:
        spark.sql("ALTER TABLE mem.ns.loud ADD COLUMN flag BOOLEAN NOT NULL")
    assert "NOT NULL" in str(caught.value) or "not supported" in str(caught.value).lower()


def test_alter_add_drop_partition_field_and_write_after(spark: ReparkSession) -> None:
    """I7 READY: ADD/DROP PARTITION FIELD; write-after-evo; mixed-spec read; time-travel."""
    spark.sql("CREATE TABLE mem.ns.pevo (id INT, category STRING) USING iceberg")
    spark.sql("INSERT INTO mem.ns.pevo VALUES (1, 'a'), (2, 'b')")
    pre = spark.sql("SELECT * FROM mem.ns.pevo").to_arrow()
    assert pre.num_rows == 2
    # Octo C5 — pin pre-evolution snapshot for VERSION AS OF after later writes.
    pre_snaps = spark._testing_list_snapshots("mem.ns.pevo")
    assert pre_snaps, "seed insert must create a snapshot"
    pre_snap_id = int(pre_snaps[-1][0])

    spark.sql("ALTER TABLE mem.ns.pevo ADD PARTITION FIELD category")
    spark.sql("INSERT INTO mem.ns.pevo VALUES (3, 'c')")
    after = spark.sql("SELECT * FROM mem.ns.pevo ORDER BY id").to_arrow()
    assert after.num_rows == 3
    assert after.column("id").to_pylist() == [1, 2, 3]
    assert after.column("category").to_pylist() == ["a", "b", "c"]

    # Time-travel pre-evolution: only seed rows (mixed-spec files exist under current head).
    pre_tt = spark.sql(
        f"SELECT id FROM mem.ns.pevo VERSION AS OF {pre_snap_id} ORDER BY id"
    ).to_arrow()
    assert pre_tt.column("id").to_pylist() == [1, 2]

    spark.sql("ALTER TABLE mem.ns.pevo DROP PARTITION FIELD category")
    # Still readable after DROP (files keep their own spec-ids).
    still = spark.sql("SELECT id FROM mem.ns.pevo ORDER BY id").to_arrow()
    assert still.column("id").to_pylist() == [1, 2, 3]
    # Case-insensitive DROP name (octo C1 adapter) — re-ADD then DROP with different case.
    spark.sql("ALTER TABLE mem.ns.pevo ADD PARTITION FIELD category AS cat")
    spark.sql("ALTER TABLE mem.ns.pevo DROP PARTITION FIELD CAT")


def test_alter_replace_partition_field_and_replace_columns(spark: ReparkSession) -> None:
    """I7 stretch: REPLACE PARTITION FIELD; REPLACE COLUMNS promote + identity-trap twin."""
    spark.sql("CREATE TABLE mem.ns.prepl (id INT, label STRING) USING iceberg")
    spark.sql("ALTER TABLE mem.ns.prepl ADD PARTITION FIELD bucket(8, id) AS id_b8")
    spark.sql(
        "ALTER TABLE mem.ns.prepl REPLACE PARTITION FIELD id_b8 WITH bucket(16, id) AS id_b16"
    )
    spark.sql("INSERT INTO mem.ns.prepl VALUES (1, 'x'), (2, 'y')")
    rows = spark.sql("SELECT id FROM mem.ns.prepl ORDER BY id").to_arrow()
    assert rows.column("id").to_pylist() == [1, 2]

    spark.sql("CREATE TABLE mem.ns.rcols (id INT, name STRING, junk INT) USING iceberg")
    spark.sql("INSERT INTO mem.ns.rcols VALUES (1, 'a', 9)")
    spark.sql("ALTER TABLE mem.ns.rcols REPLACE COLUMNS (id BIGINT, name STRING)")
    arrow = spark.sql("SELECT * FROM mem.ns.rcols").to_arrow()
    assert [field.name for field in arrow.schema] == ["id", "name"]
    assert str(arrow.schema.field(0).type) == "int64"
    assert arrow.column("id").to_pylist() == [1]

    with pytest.raises((UnsupportedOperationException, AnalysisException, Exception)) as caught:
        spark.sql("ALTER TABLE mem.ns.rcols REPLACE COLUMNS (id STRING, name STRING)")
    assert "identity trap" in str(caught.value).lower()


def test_alter_float_decimal_twins_and_case_insensitive(spark: ReparkSession) -> None:
    """Octo C3/C5 facade: float→double + decimal widen twins; case-insensitive DROP."""
    spark.sql("CREATE TABLE mem.ns.fd (measure FLOAT, amount DECIMAL(5,2)) USING iceberg")
    spark.sql("INSERT INTO mem.ns.fd VALUES (1.5, 12.34)")
    spark.sql("ALTER TABLE mem.ns.fd ALTER COLUMN measure TYPE DOUBLE")
    arrow = spark.sql("SELECT measure FROM mem.ns.fd").to_arrow()
    assert str(arrow.schema.field(0).type) == "double"
    with pytest.raises((AnalysisException, UnsupportedOperationException, Exception)) as caught:
        spark.sql("ALTER TABLE mem.ns.fd ALTER COLUMN measure TYPE FLOAT")
    message = str(caught.value).lower()
    assert "cannot" in message or "promote" in message or "type" in message

    spark.sql("ALTER TABLE mem.ns.fd ALTER COLUMN amount TYPE DECIMAL(10,2)")
    amount = spark.sql("SELECT amount FROM mem.ns.fd").to_arrow()
    assert "decimal" in str(amount.schema.field(0).type).lower()
    with pytest.raises((AnalysisException, UnsupportedOperationException, Exception)) as caught:
        spark.sql("ALTER TABLE mem.ns.fd ALTER COLUMN amount TYPE DECIMAL(5,2)")
    message = str(caught.value).lower()
    assert "cannot" in message or "promote" in message or "decimal" in message or "type" in message

    spark.sql("CREATE TABLE mem.ns.cased (id INT, note STRING) USING iceberg")
    spark.sql("ALTER TABLE mem.ns.cased DROP COLUMN NOTE")
    names = [field.name for field in spark.sql("SELECT * FROM mem.ns.cased").to_arrow().schema]
    assert "note" not in names
