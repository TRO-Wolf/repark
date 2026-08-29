"""Case-insensitive by-name column conform through the real facade (WG-4, BUG-007).

Spark resolves columns case-insensitively (``spark.sql.caseSensitive=false``): a MERGE
``UPDATE SET *`` / ``INSERT *`` must conform by name even when the source spells its columns
in a different case than the target. Requires the compiled wheel (``maturin develop``).

Scope: the ``ON`` predicate is resolved by DataFusion case-sensitively, so the source column
is named explicitly there; that resolution is a separate tracked follow-up, not this pin.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession

TABLE = "cat.ns.t"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-ci-conform").getOrCreate()
    session.register_memory_catalog("cat", tmp_path)
    session.sql("CREATE NAMESPACE cat.ns")
    # Target columns are lowercase.
    session.sql(f"CREATE TABLE {TABLE} AS SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'")
    return session


def _table(spark: ReparkSession) -> pa.Table:
    return spark.sql(f"SELECT id, name FROM {TABLE} ORDER BY id").to_arrow()


def test_merge_star_conforms_case_differing_source_by_name(spark: ReparkSession) -> None:
    # Source columns are UPPERCASE versus the lowercase target; `UPDATE SET *` / `INSERT *`
    # must resolve each target to its case-differing source column by name.
    spark.sql(
        f"MERGE INTO {TABLE} AS t "
        f'USING (SELECT 1 AS "ID", \'updated\' AS "NAME" '
        f'       UNION ALL SELECT 3 AS "ID", \'c\' AS "NAME") AS s '
        f'ON t.id = s."ID" '
        f"WHEN MATCHED THEN UPDATE SET * "
        f"WHEN NOT MATCHED THEN INSERT *"
    )

    table = _table(spark)
    assert table.to_pylist() == [
        {"id": 1, "name": "updated"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]
    # Value AND Arrow type on the export path (to_arrow), never show.
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()


def test_merge_star_ambiguous_case_source_raises(spark: ReparkSession) -> None:
    # Two source columns colliding on one target (`id` AND `ID`) must raise a loud error
    # naming both, not silently bind one.
    with pytest.raises(Exception) as raised:
        spark.sql(
            f"MERGE INTO {TABLE} AS t "
            f'USING (SELECT 1 AS "id", 1 AS "ID", \'x\' AS name) AS s '
            f'ON t.id = s."id" '
            f"WHEN MATCHED THEN UPDATE SET *"
        )
    message = str(raised.value)
    assert "ambiguous" in message and "`id`, `ID`" in message

    assert _table(spark).to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
