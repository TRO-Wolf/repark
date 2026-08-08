"""Case-insensitive by-name column conform through the real facade (WG-4, BUG-007).

Spark's default is case-insensitive column resolution (``spark.sql.caseSensitive=false``). A
``MERGE INTO … UPDATE SET * / INSERT *`` whose source frame spells its columns in a different case
than the target must still conform by name. This exercises the star-conform fix end-to-end through
``spark.sql`` — the migrated caller's real surface — with every value AND Arrow **type** checked on
the ``to_arrow`` export path, never ``show``. Requires the compiled wheel (``maturin develop``).

Scope note (disclosed): the by-name CONFORM (``UPDATE SET *`` / ``INSERT *``) is case-insensitive.
The ``ON`` predicate and explicit column references are resolved by DataFusion, which matches quoted
identifiers case-sensitively, so the source column is named explicitly in ``ON`` here — that
DataFusion-controlled resolution is a separate, tracked follow-up, not the conform this pins.
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
    # Target columns are lowercase: `id` (int64), `name` (string).
    session.sql(f"CREATE TABLE {TABLE} AS SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'")
    return session


def _table(spark: ReparkSession) -> pa.Table:
    return spark.sql(f"SELECT id, name FROM {TABLE} ORDER BY id").to_arrow()


def test_merge_star_conforms_case_differing_source_by_name(spark: ReparkSession) -> None:
    # Source columns are UPPERCASE (`"ID"`, `"NAME"`) versus the lowercase target. `UPDATE SET *`
    # and `INSERT *` must resolve each target to its case-differing source column BY NAME. Pre-fix
    # the star expanded to `s."id"` / `s."name"` — columns absent from this source — so the MERGE
    # errored; post-fix it binds the actual `s."ID"` / `s."NAME"`.
    spark.sql(
        f"MERGE INTO {TABLE} AS t "
        f'USING (SELECT 1 AS "ID", \'updated\' AS "NAME" '
        f'       UNION ALL SELECT 3 AS "ID", \'c\' AS "NAME") AS s '
        f'ON t.id = s."ID" '
        f"WHEN MATCHED THEN UPDATE SET * "
        f"WHEN NOT MATCHED THEN INSERT *"
    )

    table = _table(spark)
    # id=1 updated (matched UPDATE *), id=2 untouched, id=3 inserted (not-matched INSERT *).
    assert table.to_pylist() == [
        {"id": 1, "name": "updated"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]
    # Value AND Arrow type on the export path (to_arrow), never show.
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()


def test_merge_star_ambiguous_case_source_raises(spark: ReparkSession) -> None:
    # Two source columns colliding on one target (`id` AND `ID`) is a loud error naming both —
    # Spark rejects the ambiguity rather than silently binding one. The table is unchanged.
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
