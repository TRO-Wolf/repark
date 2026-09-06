"""Demonstrate the ``F.*`` JSON family: read a field, count, list keys, parse, and render."""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.get_json_object",
    "F.json_array_length",
    "F.json_object_keys",
    "F.from_json",
    "F.to_json",
    "F.schema_of_json",
    "F.struct",
]

FIRST = '{"id": 1, "tags": ["a", "b"], "meta": {"ok": true}}'
SECOND = '{"id": 2, "tags": [], "meta": null}'


def main() -> None:
    """Read one JSON column six ways; a malformed document answers NULL, never an error."""
    repark = ReparkSession.builder.appName("ex-json-family").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(FIRST,), (SECOND,), ("{bad",), (None,)], "doc STRING")
        rows = frame.select(
            F.get_json_object("doc", "$.id").alias("id_text"),
            F.get_json_object("doc", "$.tags[0]").alias("first_tag"),
            F.get_json_object("doc", "$.tags").alias("tags_text"),
            F.get_json_object("doc", "$.meta").alias("meta_text"),
            F.json_array_length(F.get_json_object("doc", "$.tags")).alias("tag_count"),
            F.json_object_keys("doc").alias("keys"),
            F.from_json("doc", "id INT, tags ARRAY<STRING>").alias("parsed"),
        ).collect()
        checked = (
            ("id_text", ["1", "2", None, None]),
            ("first_tag", ["a", None, None, None]),
            ("tags_text", ['["a","b"]', "[]", None, None]),
            ("meta_text", ['{"ok":true}', None, None, None]),
            ("tag_count", [2, 0, None, None]),
            ("keys", [["id", "tags", "meta"], ["id", "tags", "meta"], None, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"{name} gave {values!r}, expected {expected!r}")
        parsed = [
            None if row["parsed"] is None else (row["parsed"]["id"], row["parsed"]["tags"])
            for row in rows
        ]
        print(f"F.from_json: {parsed!r}")
        if parsed != [(1, ["a", "b"]), (2, []), (None, None), None]:
            raise SystemExit(f"F.from_json gave {parsed!r}")
        shapes = repark.createDataFrame([(1, ["a"])], "id INT, tags ARRAY<STRING>")
        rendered = [
            row["as_json"]
            for row in shapes.select(F.to_json(F.struct("id", "tags")).alias("as_json")).collect()
        ]
        print(f"F.to_json: {rendered!r}")
        if rendered != ['{"id":1,"tags":["a"]}']:
            raise SystemExit(f"F.to_json gave {rendered!r}")
        inferred = [
            row["shape"] for row in frame.select(F.schema_of_json(FIRST).alias("shape")).collect()
        ][:1]
        print(f"F.schema_of_json: {inferred!r}")
        if inferred != ["STRUCT<id: BIGINT, meta: STRUCT<ok: BOOLEAN>, tags: ARRAY<STRING>>"]:
            raise SystemExit(f"F.schema_of_json gave {inferred!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
