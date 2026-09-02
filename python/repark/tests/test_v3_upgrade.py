"""V3-10: in-place v2 to v3 upgrade through ALTER TABLE on the facade door.

pins: v3-10-upgrade-v2-to-v3/C-003, C-004
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import UnsupportedOperationException

_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"
_UPGRADE = "ALTER TABLE ice.sales.up SET TBLPROPERTIES ('format-version' = '3')"


def _lineage(session: object, table: str) -> list[tuple[int, int | None, int | None]]:
    """Return ordered (id, _row_id, seq) triples and pin the Arrow types."""
    arrow = session.sql(  # type: ignore[attr-defined]
        f"SELECT id, _row_id, _last_updated_sequence_number FROM {table} ORDER BY id"
    ).to_arrow()
    assert arrow.schema.field("_row_id").type == pa.int64(), arrow.schema
    assert arrow.schema.field("_last_updated_sequence_number").type == pa.int64(), arrow.schema
    return list(
        zip(
            [int(value) for value in arrow.column("id").to_pylist()],
            arrow.column("_row_id").to_pylist(),
            arrow.column("_last_updated_sequence_number").to_pylist(),
            strict=True,
        )
    )


def _seed_v2(session: object) -> None:
    """Create ice.sales.up as a v2 table holding three rows in one file."""
    session.sql("CREATE NAMESPACE ice.sales")  # type: ignore[attr-defined]
    session.sql(  # type: ignore[attr-defined]
        "CREATE TABLE ice.sales.up (id INT, name STRING) USING iceberg "
        "TBLPROPERTIES ('format-version' = '2')"
    ).collect()
    session.sql(  # type: ignore[attr-defined]
        "INSERT INTO ice.sales.up VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ).collect()


def test_alter_upgrade_refuses_without_the_opt_in(tmp_path: Path) -> None:
    """Default session: the upgrade refuses naming the conf, and the table stays v2."""
    session = ReparkSession.builder.appName("v3-10-default").getOrCreate()
    try:
        session.register_memory_catalog("ice", tmp_path)
        _seed_v2(session)
        with pytest.raises(UnsupportedOperationException, match=_ALLOW_CREATE_V3_KEY) as refusal:
            session.sql(_UPGRADE).collect()
        assert "create" not in str(refusal.value)
        with pytest.raises(Exception, match="_row_id"):
            session.sql("SELECT _row_id FROM ice.sales.up").collect()
    finally:
        session.stop()


def test_alter_upgrade_with_the_opt_in_serves_v3_lineage(tmp_path: Path) -> None:
    """Opt-in upgrade lands v3; pre-upgrade rows gain lineage only on the next append."""
    session = (
        ReparkSession.builder.appName("v3-10-opt-in")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("ice", tmp_path)
        _seed_v2(session)
        session.sql(_UPGRADE).collect()
        assert _lineage(session, "ice.sales.up") == [
            (1, None, None),
            (2, None, None),
            (3, None, None),
        ]
        session.sql("INSERT INTO ice.sales.up VALUES (4, 'd'), (5, 'e')").collect()
        assert _lineage(session, "ice.sales.up") == [
            (1, 2, 1),
            (2, 3, 1),
            (3, 4, 1),
            (4, 0, 2),
            (5, 1, 2),
        ]
    finally:
        session.stop()


def test_alter_downgrade_and_unsupported_versions_refuse(tmp_path: Path) -> None:
    """A downgrade, v1, v4 and an unparsable value all refuse naming the key."""
    session = (
        ReparkSession.builder.appName("v3-10-refusals")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("ice", tmp_path)
        _seed_v2(session)
        session.sql(_UPGRADE).collect()
        with pytest.raises(UnsupportedOperationException, match="cannot downgrade a v3 table"):
            session.sql(
                "ALTER TABLE ice.sales.up SET TBLPROPERTIES ('format-version' = '2')"
            ).collect()
        for value, needle in (
            ("4", "v1 through v3"),
            ("-1", "v-1"),
            ("x", "not an Iceberg format version"),
            ("3.0", "not an Iceberg format version"),
        ):
            with pytest.raises(UnsupportedOperationException, match=needle):
                session.sql(
                    f"ALTER TABLE ice.sales.up SET TBLPROPERTIES ('format-version' = '{value}')"
                ).collect()
        assert _lineage(session, "ice.sales.up") == [
            (1, None, None),
            (2, None, None),
            (3, None, None),
        ]
    finally:
        session.stop()
