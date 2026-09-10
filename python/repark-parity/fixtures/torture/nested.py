"""The nested torture family: deep struct/list chains, mixed list element types, null lists."""

from __future__ import annotations

import csv
import json
import random
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq

from repark_parity.torture.family import (
    CSV_NAME,
    PARQUET_NAME,
    FamilyOutput,
    refuse_bad_rows,
    refuse_bad_seed,
    refuse_repository_output,
)

DEFAULT_DEPTH = 6
DEFAULT_WIDTH = 3
MAX_DEPTH = 16
MAX_WIDTH = 64
_SCALAR_KINDS = ("int32", "string", "float64")


def _scalar_type(kind: str) -> pa.DataType:
    """Map one scalar kind name to its Arrow type."""
    if kind == "int32":
        return pa.int32()
    if kind == "string":
        return pa.string()
    return pa.float64()


def _scalar_array(rng: random.Random, kind: str, count: int) -> pa.Array:
    """Draw one deterministic scalar column of the requested kind and length."""
    if kind == "int32":
        return pa.array([rng.randrange(1_000_000) for _ in range(count)], type=pa.int32())
    if kind == "string":
        return pa.array([f"s{rng.randrange(100_000)}" for _ in range(count)], type=pa.string())
    return pa.array([round(rng.random(), 6) for _ in range(count)], type=pa.float64())


def _level_sizes(rows: int, depth: int) -> list[int]:
    """Count the struct elements at every level: half the parents carry one child."""
    sizes = [sum(1 for index in range(rows) if index % 4 in (0, 3))]
    for level in range(1, depth):
        sizes.append(sum(1 for index in range(sizes[level - 1]) if index % 4 in (0, 3)))
    return sizes


def _pattern_list(values: pa.Array, parent_count: int) -> pa.ListArray:
    """Wrap one struct level into per-parent lists: populated, empty, or null by index."""
    offsets: list[int | None] = []
    consumed = 0
    for parent in range(parent_count):
        if parent % 4 == 2:
            offsets.append(None)
        else:
            offsets.append(consumed)
        if parent % 4 in (0, 3):
            consumed += 1
    offsets.append(consumed)
    return pa.ListArray.from_arrays(pa.array(offsets, type=pa.int32()), values)


def declared_write_schema(depth: int, width: int) -> pa.Schema:
    """The Arrow schema the generator writes for one depth/width scale."""
    value_fields = [
        pa.field(f"Value{index}", _scalar_type(_SCALAR_KINDS[index % len(_SCALAR_KINDS)]))
        for index in range(width)
    ]
    leg_type: pa.DataType = pa.struct(value_fields)
    for _level in range(depth - 1):
        leg_type = pa.struct([*value_fields, pa.field("Legs", pa.list_(leg_type))])
    return pa.schema(
        [
            pa.field("Id", pa.int32()),
            pa.field("Legs", pa.list_(leg_type)),
            pa.field("Tags", pa.list_(pa.string())),
            pa.field("Scores", pa.list_(pa.int32())),
            pa.field("user_properties", pa.list_(pa.null())),
            pa.field("Mixed", pa.list_(pa.struct([pa.field("V", pa.string())]))),
        ]
    )


def declared_read_schema(depth: int, width: int) -> pa.Schema:
    """The declared read expectation: the write schema with the null list widened to int32."""
    fields = [
        pa.field(field.name, pa.list_(pa.int32())) if field.name == "user_properties" else field
        for field in declared_write_schema(depth, width)
    ]
    return pa.schema(fields)


def _leg_chain(rng: random.Random, rows: int, depth: int, width: int) -> pa.Array:
    """Build the deep Legs column bottom-up so every populated row reaches the deepest level."""
    sizes = _level_sizes(rows, depth)
    value_fields = [
        pa.field(f"Value{index}", _scalar_type(_SCALAR_KINDS[index % len(_SCALAR_KINDS)]))
        for index in range(width)
    ]
    current: pa.Array = pa.StructArray.from_arrays(
        [
            _scalar_array(rng, _SCALAR_KINDS[index % len(_SCALAR_KINDS)], sizes[depth - 1])
            for index in range(width)
        ],
        fields=value_fields,
    )
    for level in range(depth - 2, -1, -1):
        scalars = [
            _scalar_array(rng, _SCALAR_KINDS[index % len(_SCALAR_KINDS)], sizes[level])
            for index in range(width)
        ]
        current = pa.StructArray.from_arrays(
            [*scalars, _pattern_list(current, sizes[level])],
            fields=[*value_fields, pa.field("Legs", pa.list_(current.type))],
        )
    return _pattern_list(current, rows)


def _flat_columns(rng: random.Random, rows: int) -> dict[str, pa.Array]:
    """Build the flat torture columns: mixed element types and null-typed lists."""
    tags: list[list[str] | None] = []
    scores: list[list[int] | None] = []
    properties: list[list[None] | None] = []
    mixed: list[list[dict[str, str]] | None] = []
    for index in range(rows):
        if index % 3 == 0:
            tags.append([f"t{index}", f"u{index}"])
            scores.append([index, index + 1])
            properties.append([None, None])
        elif index % 3 == 1:
            tags.append([])
            scores.append(None)
            properties.append([])
        else:
            tags.append(None)
            scores.append([])
            properties.append(None)
        mixed.append([{"V": f"m{index}"}] if index % 2 == 0 else None)
    return {
        "Tags": pa.array(tags, type=pa.list_(pa.string())),
        "Scores": pa.array(scores, type=pa.list_(pa.int32())),
        "user_properties": pa.array(properties, type=pa.list_(pa.null())),
        "Mixed": pa.array(mixed, type=pa.list_(pa.struct([pa.field("V", pa.string())]))),
    }


def _write_csv(table: pa.Table, path: Path) -> None:
    """Write the CSV leg: nested columns as JSON text, scalars plain."""
    nested_names = {
        name for name in table.schema.names if pa.types.is_nested(table.schema.field(name).type)
    }
    columns = {name: table.column(name).to_pylist() for name in table.schema.names}
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(table.schema.names)
        for index in range(table.num_rows):
            writer.writerow(
                [
                    json.dumps(columns[name][index])
                    if name in nested_names
                    else columns[name][index]
                    for name in table.schema.names
                ]
            )


class NestedFamily:
    """The nested family implementor: the dynamicFlatten measurement bed."""

    name = "nested"

    def generate(
        self,
        rows: int,
        seed: int,
        out: Path,
        depth: int | None = None,
        width: int | None = None,
    ) -> FamilyOutput:
        """Write the family's Parquet and JSON-rendered CSV files under out."""
        resolved_depth = DEFAULT_DEPTH if depth is None else depth
        resolved_width = DEFAULT_WIDTH if width is None else width
        refuse_bad_rows(rows)
        refuse_bad_seed(seed)
        if not 1 <= resolved_depth <= MAX_DEPTH:
            raise ValueError(f"depth must be between 1 and {MAX_DEPTH}: {resolved_depth}")
        if not 1 <= resolved_width <= MAX_WIDTH:
            raise ValueError(f"width must be between 1 and {MAX_WIDTH}: {resolved_width}")
        refuse_repository_output(out)
        out.mkdir(parents=True, exist_ok=True)
        rng = random.Random(seed)
        table = pa.table(
            {
                "Id": pa.array(range(rows), type=pa.int32()),
                "Legs": _leg_chain(rng, rows, resolved_depth, resolved_width),
                **_flat_columns(rng, rows),
            }
        )
        pq.write_table(table, out / PARQUET_NAME)
        _write_csv(table, out / CSV_NAME)
        return FamilyOutput(rows=rows, parquet_path=out / PARQUET_NAME, csv_path=out / CSV_NAME)

    def expected_read_schema(self, fmt: Literal["parquet", "csv"]) -> pa.Schema | None:
        """Return the declared Parquet expectation; the JSON-text CSV leg declares none."""
        if fmt == "parquet":
            return declared_read_schema(DEFAULT_DEPTH, DEFAULT_WIDTH)
        return None


NESTED_FAMILY = NestedFamily()
