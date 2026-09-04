"""Measurement-bed generator for dynamicFlatten (PERF-DYNFLATTEN-1)."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import sys
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Final, Literal

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field

LOGGER = logging.getLogger(__name__)

FAMILY: Final[str] = "nested-bed"
DEFAULT_SEED: Final[int] = 42
GATE_ROWS: Final[int] = 64
QUICK_ROWS: Final[int] = 100_000
FULL_ROWS: Final[int] = 1_000_000
MAX_ROWS: Final[int] = 10_000_000
BATCH_SIZE: Final[int] = 4_096
NULL_PARENT_RATE: Final[float] = 0.30
CARTESIAN_LIST_WIDTH: Final[int] = 4
NAME_POOL: Final[tuple[str, ...]] = ("alpha", "bravo", "charlie", "delta")

FORBIDDEN_ENV_KEYS: Final[tuple[str, ...]] = (
    "REPARK_DATASET_PATH",
    "REPARK_DATASET",
    "DYNFLATTEN_INPUT",
    "DYNFLATTEN_DATASET",
    "REPARK_FLATTEN_PARQUET",
    "REPARK_BED_INPUT",
)
FORBIDDEN_CLI_FLAGS: Final[tuple[str, ...]] = (
    "--input",
    "--from",
    "--source",
    "--dataset",
    "--from-parquet",
    "--from-csv",
)

ShapeKind = Literal["struct", "list_struct", "cartesian", "null_typed_list", "tags_only"]


class ShapeSpec(BaseModel):
    """One named nested fixture shape."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    kind: ShapeKind
    struct_depth: int = Field(ge=1, le=8)
    list_width: int | None = Field(default=None, ge=1, le=64)
    null_parent_rate: float = Field(default=NULL_PARENT_RATE, ge=0.0, le=1.0)
    isolation: bool = False


SHAPES: Final[tuple[ShapeSpec, ...]] = (
    ShapeSpec(name="struct_d3", kind="struct", struct_depth=3),
    ShapeSpec(name="struct_d6", kind="struct", struct_depth=6),
    ShapeSpec(name="list_struct_1", kind="list_struct", struct_depth=1, list_width=1),
    ShapeSpec(name="list_struct_8", kind="list_struct", struct_depth=1, list_width=8),
    ShapeSpec(name="list_struct_64", kind="list_struct", struct_depth=1, list_width=64),
    ShapeSpec(
        name="cartesian_two_lists",
        kind="cartesian",
        struct_depth=1,
        list_width=CARTESIAN_LIST_WIDTH,
    ),
    ShapeSpec(name="null_typed_list", kind="null_typed_list", struct_depth=2),
    ShapeSpec(
        name="struct_d3_nonull",
        kind="struct",
        struct_depth=3,
        null_parent_rate=0.0,
        isolation=True,
    ),
    ShapeSpec(
        name="struct_d6_nonull",
        kind="struct",
        struct_depth=6,
        null_parent_rate=0.0,
        isolation=True,
    ),
    ShapeSpec(
        name="cartesian_legs_only",
        kind="list_struct",
        struct_depth=1,
        list_width=CARTESIAN_LIST_WIDTH,
        isolation=True,
    ),
    ShapeSpec(
        name="cartesian_tags_only",
        kind="tags_only",
        struct_depth=1,
        list_width=CARTESIAN_LIST_WIDTH,
        isolation=True,
    ),
)

SHAPE_BY_NAME: Final[dict[str, ShapeSpec]] = {shape.name: shape for shape in SHAPES}
FULL_SKIP_SHAPES: Final[frozenset[str]] = frozenset({"list_struct_64"})


def _bootstrap_repark_datasets() -> None:
    """Register ``repark_datasets`` so script invocation can import ``_cache``."""
    if "repark_datasets" in sys.modules:
        return
    datasets_dir = Path(__file__).resolve().parent.parent
    package = types.ModuleType("repark_datasets")
    package.__dict__["__path__"] = [str(datasets_dir)]
    sys.modules["repark_datasets"] = package


def _cache_mod() -> Any:
    _bootstrap_repark_datasets()
    import importlib

    return importlib.import_module("repark_datasets._cache")


def refuse_real_dataset_inputs(
    argv: list[str] | None = None,
    environ: dict[str, str] | None = None,
) -> None:
    """Refuse every plausible real-dataset flag or environment variable."""
    tokens = sys.argv[1:] if argv is None else argv
    env = os.environ if environ is None else environ
    for flag in FORBIDDEN_CLI_FLAGS:
        if flag in tokens or any(token.startswith(f"{flag}=") for token in tokens):
            msg = f"refusing real-dataset flag {flag}; this generator is synthetic only"
            raise ValueError(msg)
    for key in FORBIDDEN_ENV_KEYS:
        if env.get(key):
            msg = f"refusing real-dataset environment {key}; this generator is synthetic only"
            raise ValueError(msg)


def _validate(rows: int, seed: int) -> None:
    if not isinstance(rows, int) or isinstance(rows, bool) or rows < 1:
        msg = f"rows must be an integer >= 1; got {rows!r}"
        raise ValueError(msg)
    if rows > MAX_ROWS:
        msg = f"rows must be <= {MAX_ROWS}; got {rows}"
        raise ValueError(msg)
    if not isinstance(seed, int) or isinstance(seed, bool) or seed < 0:
        msg = f"seed must be an integer >= 0; got {seed!r}"
        raise ValueError(msg)


def _dict_name_type() -> pa.DataType:
    return pa.dictionary(pa.int32(), pa.string())


def _leaf_struct(*, with_row_id: bool) -> pa.DataType:
    fields = []
    if with_row_id:
        fields.append(pa.field("id", pa.int64(), nullable=True))
    fields.extend(
        [
            pa.field("Name", _dict_name_type(), nullable=True),
            pa.field("Val", pa.int64(), nullable=True),
        ]
    )
    return pa.struct(fields)


def _wrap_struct(depth: int, *, with_row_id: bool) -> pa.DataType:
    current = _leaf_struct(with_row_id=with_row_id)
    for level in range(depth - 1, 0, -1):
        current = pa.struct([pa.field(f"L{level}", current, nullable=True)])
    return current


def _leg_struct() -> pa.DataType:
    return pa.struct(
        [
            pa.field("leg_id", pa.int64(), nullable=True),
            pa.field("Name", _dict_name_type(), nullable=True),
        ]
    )


def ddl_for(shape: ShapeSpec | str) -> str:
    """Spark-facade schema DDL for ``createDataFrame`` (dict leaves as STRING)."""
    resolved = resolve_shape(shape)
    void_list = "ARRAY<VOID>"
    if resolved.kind == "struct":
        leaf = "STRUCT<id: LONG, Name: STRING, Val: LONG>"
        wrapped = leaf
        for level in range(resolved.struct_depth - 1, 0, -1):
            wrapped = f"STRUCT<L{level}: {wrapped}>"
        return f"Payload {wrapped}"
    leaf = "STRUCT<Name: STRING, Val: LONG>"
    wrapped = leaf
    for level in range(resolved.struct_depth - 1, 0, -1):
        wrapped = f"STRUCT<L{level}: {wrapped}>"
    if resolved.kind == "null_typed_list":
        return f"id LONG, Payload {wrapped}, user_properties {void_list}"
    leg = "ARRAY<STRUCT<leg_id: LONG, Name: STRING>>"
    if resolved.kind == "tags_only":
        return f"id LONG, Tags ARRAY<STRING>, user_properties {void_list}"
    if resolved.kind == "list_struct":
        return f"id LONG, Legs {leg}, user_properties {void_list}"
    return f"id LONG, Legs {leg}, Tags ARRAY<STRING>, user_properties {void_list}"


def schema_for(shape: ShapeSpec) -> pa.Schema:
    """Return the Arrow schema for ``shape`` (dictionary-encoded ``Name`` leaves)."""
    void_list = pa.list_(pa.null())
    if shape.kind == "struct":
        return pa.schema(
            [
                pa.field(
                    "Payload",
                    _wrap_struct(shape.struct_depth, with_row_id=True),
                    nullable=True,
                ),
            ]
        )
    if shape.kind == "list_struct":
        return pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("Legs", pa.list_(_leg_struct()), nullable=True),
                pa.field("user_properties", void_list, nullable=True),
            ]
        )
    if shape.kind == "tags_only":
        return pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("Tags", pa.list_(_dict_name_type()), nullable=True),
                pa.field("user_properties", void_list, nullable=True),
            ]
        )
    if shape.kind == "cartesian":
        return pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("Legs", pa.list_(_leg_struct()), nullable=True),
                pa.field("Tags", pa.list_(_dict_name_type()), nullable=True),
                pa.field("user_properties", void_list, nullable=True),
            ]
        )
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field(
                "Payload",
                _wrap_struct(shape.struct_depth, with_row_id=False),
                nullable=True,
            ),
            pa.field("user_properties", void_list, nullable=True),
        ]
    )


def _arrow_type_utf8_leaves(data_type: pa.DataType) -> pa.DataType:
    """Replace dictionary leaves with utf8 so ``from_pylist`` can build the batch."""
    if pa.types.is_dictionary(data_type):
        return pa.string()
    if pa.types.is_struct(data_type):
        fields = [
            pa.field(field.name, _arrow_type_utf8_leaves(field.type), nullable=field.nullable)
            for field in data_type
        ]
        return pa.struct(fields)
    if pa.types.is_list(data_type):
        return pa.list_(_arrow_type_utf8_leaves(data_type.value_type))
    return data_type


def _utf8_schema(schema: pa.Schema) -> pa.Schema:
    """Schema identical to ``schema`` except dictionary leaves are utf8."""
    return pa.schema(
        [
            pa.field(field.name, _arrow_type_utf8_leaves(field.type), nullable=field.nullable)
            for field in schema
        ]
    )


def _is_null_parent(rng: random.Random, rate: float = NULL_PARENT_RATE) -> bool:
    return rng.random() < rate


def _leaf_value(rng: random.Random, value: int, *, with_row_id: bool) -> dict[str, Any]:
    payload = {"Name": rng.choice(NAME_POOL), "Val": int(value)}
    if with_row_id:
        payload = {"id": int(value), **payload}
    return payload


def _nested_payload(
    rng: random.Random,
    depth: int,
    value: int,
    *,
    with_row_id: bool,
    null_rate: float = NULL_PARENT_RATE,
) -> dict[str, Any] | None:
    if _is_null_parent(rng, null_rate):
        return None
    current: Any = _leaf_value(rng, value, with_row_id=with_row_id)
    for level in range(depth - 1, 0, -1):
        if _is_null_parent(rng, null_rate):
            current = None
        current = {f"L{level}": current}
    return current


def _legs(
    rng: random.Random, row_index: int, width: int, null_rate: float = NULL_PARENT_RATE
) -> list[dict[str, Any]] | None:
    if _is_null_parent(rng, null_rate):
        return None
    return [
        {
            "leg_id": int(row_index * width + offset + 1),
            "Name": rng.choice(NAME_POOL),
        }
        for offset in range(width)
    ]


def _tags(rng: random.Random, width: int, null_rate: float = NULL_PARENT_RATE) -> list[str] | None:
    if _is_null_parent(rng, null_rate):
        return None
    return [rng.choice(NAME_POOL) for _ in range(width)]


def _properties(rng: random.Random, row_index: int) -> list[None] | None:
    pattern = row_index % 4
    if pattern == 0:
        return None
    if pattern == 1:
        return []
    _ = rng.random()
    return []


def _build_row(shape: ShapeSpec, rng: random.Random, row_index: int) -> dict[str, Any]:
    width = shape.list_width or 1
    rate = shape.null_parent_rate
    if shape.kind == "struct":
        return {
            "Payload": _nested_payload(
                rng, shape.struct_depth, row_index, with_row_id=True, null_rate=rate
            ),
        }
    if shape.kind == "list_struct":
        return {
            "id": int(row_index),
            "Legs": _legs(rng, row_index, width, rate),
            "user_properties": _properties(rng, row_index),
        }
    if shape.kind == "tags_only":
        return {
            "id": int(row_index),
            "Tags": _tags(rng, width, rate),
            "user_properties": _properties(rng, row_index),
        }
    if shape.kind == "cartesian":
        return {
            "id": int(row_index),
            "Legs": _legs(rng, row_index, width, rate),
            "Tags": _tags(rng, width, rate),
            "user_properties": _properties(rng, row_index),
        }
    return {
        "id": int(row_index),
        "Payload": _nested_payload(
            rng, shape.struct_depth, row_index, with_row_id=False, null_rate=rate
        ),
        "user_properties": _properties(rng, row_index),
    }


def _iter_batches(shape: ShapeSpec, rows: int, seed: int) -> Iterator[pa.RecordBatch]:
    schema = schema_for(shape)
    utf8 = _utf8_schema(schema)
    rng = random.Random(seed)
    start = 0
    while start < rows:
        end = min(start + BATCH_SIZE, rows)
        batch_rows = [_build_row(shape, rng, row_index) for row_index in range(start, end)]
        utf8_batch = pa.RecordBatch.from_pylist(batch_rows, schema=utf8)
        yield utf8_batch.cast(schema)
        start = end


def resolve_shape(shape: ShapeSpec | str) -> ShapeSpec:
    """Return the named shape spec."""
    if isinstance(shape, ShapeSpec):
        return shape
    resolved = SHAPE_BY_NAME.get(shape)
    if resolved is None:
        msg = f"unknown shape {shape!r}"
        raise ValueError(msg)
    return resolved


def generate(shape: ShapeSpec | str, rows: int, seed: int = DEFAULT_SEED) -> pa.Table:
    """Build one shape as an in-memory table (no wall-clock entropy)."""
    _validate(rows, seed)
    resolved = resolve_shape(shape)
    batches = list(_iter_batches(resolved, rows, seed))
    return pa.Table.from_batches(batches, schema=schema_for(resolved))


def small(shape: ShapeSpec | str, rows: int = GATE_ROWS, seed: int = DEFAULT_SEED) -> pa.Table:
    """CI / test door. Defaults are 64 rows, seed 42."""
    return generate(shape, rows, seed)


def table_digest(table: pa.Table) -> str:
    """SHA-256 of the Arrow IPC file bytes for ``table``."""
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_file(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def parquet_name(shape: ShapeSpec, rows: int) -> str:
    """File name for one fixture parquet."""
    return f"{shape.name}_r{rows}.parquet"


def write_shape(
    shape: ShapeSpec | str,
    *,
    rows: int,
    seed: int = DEFAULT_SEED,
    out: Path,
) -> tuple[Path, str]:
    """Write one shape as parquet under ``out``. Returns ``(path, batch-digest)``."""
    _validate(rows, seed)
    resolved = resolve_shape(shape)
    cache = _cache_mod()
    prepared = cache.prepare_output_dir(Path(out), root=Path(out))
    path = prepared / parquet_name(resolved, rows)
    cache.refuse_symlink_file(path)
    schema = schema_for(resolved)
    LOGGER.info("nested bed: %s rows=%s seed=%s -> %s", resolved.name, rows, seed, path)
    hasher = hashlib.sha256()
    hasher.update(str(schema).encode("utf-8"))
    with path.open("wb") as handle:
        writer = pq.ParquetWriter(handle, schema)
        try:
            for batch in _iter_batches(resolved, rows, seed):
                writer.write_batch(batch)
                hasher.update(table_digest(pa.Table.from_batches([batch], schema=schema)).encode())
        finally:
            writer.close()
    return path, hasher.hexdigest()


def scale_rows(scale: str) -> int:
    """Row count for ``gate`` / ``quick`` / ``full``."""
    if scale == "gate":
        return GATE_ROWS
    if scale == "quick":
        return QUICK_ROWS
    if scale == "full":
        return FULL_ROWS
    msg = f"unknown scale {scale!r}"
    raise ValueError(msg)


def shapes_for_scale(scale: str) -> tuple[ShapeSpec, ...]:
    """Shapes generated at ``scale``. ``list_struct_64`` is omitted from ``full``."""
    if scale == "full":
        return tuple(shape for shape in SHAPES if shape.name not in FULL_SKIP_SHAPES)
    return SHAPES


def write_bed(
    *,
    scale: str,
    seed: int = DEFAULT_SEED,
    out: Path,
) -> dict[str, Any]:
    """Write every in-scale shape plus a manifest. Returns the manifest mapping."""
    rows = scale_rows(scale)
    cache = _cache_mod()
    prepared = cache.prepare_output_dir(Path(out), root=Path(out))
    files: list[dict[str, Any]] = []
    for shape in shapes_for_scale(scale):
        path, digest = write_shape(shape, rows=rows, seed=seed, out=prepared)
        files.append(
            {
                "shape": shape.name,
                "kind": shape.kind,
                "struct_depth": shape.struct_depth,
                "list_width": shape.list_width,
                "isolation": shape.isolation,
                "null_parent_rate": shape.null_parent_rate,
                "rows": rows,
                "path": path.name,
                "bytes": path.stat().st_size,
                "digest": digest,
            }
        )
    skipped = [
        {"shape": name, "rows": rows, "reason": "list_struct_64 at 1e6 is skipped (file size)"}
        for name in sorted(FULL_SKIP_SHAPES)
        if scale == "full"
    ]
    manifest = {
        "family": FAMILY,
        "scale": scale,
        "rows": rows,
        "seed": seed,
        "null_parent_rate": NULL_PARENT_RATE,
        "files": files,
        "skipped": skipped,
    }
    manifest_path = prepared / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    """CLI: ``--scale gate|quick|full``, ``--seed``, ``--out``."""
    tokens = sys.argv[1:] if argv is None else argv
    try:
        refuse_real_dataset_inputs(tokens)
    except ValueError as error:
        print(f"usage error: {error}", file=sys.stderr)
        return 2
    parser = argparse.ArgumentParser(description="Generate the dynamicFlatten measurement bed")
    parser.add_argument("--scale", choices=("gate", "quick", "full"), default="gate")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--out", type=Path, required=True, help="output directory outside the repo")
    parser.add_argument("--shape", choices=tuple(SHAPE_BY_NAME), default=None)
    parser.add_argument("--rows", type=int, default=None)
    args = parser.parse_args(tokens)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.shape is not None:
            rows = args.rows if args.rows is not None else scale_rows(args.scale)
            written, _digest = write_shape(args.shape, rows=rows, seed=args.seed, out=args.out)
            print(written)
            return 0
        manifest = write_bed(scale=args.scale, seed=args.seed, out=args.out)
        print(json.dumps({"out": str(args.out), "files": len(manifest["files"])}))
    except ValueError as error:
        print(f"usage error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
