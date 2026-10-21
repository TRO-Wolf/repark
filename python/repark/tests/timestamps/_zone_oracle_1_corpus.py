"""The ZONE-ORACLE-1 corpus: a deterministic generator of timestamp and zone expressions.

Every cell is one plain ``SELECT`` run in one session zone, so a write that stores what the
``SELECT`` yields inherits the cell's answer on every door. The cell ids are stable: an id is
the dotted expression key plus the zone tag, and nothing in this module is random or
time-dependent. Run it as a script to print the cell count and the corpus hash::

    python python/repark/tests/timestamps/_zone_oracle_1_corpus.py

Axes:

- value sources: an instant literal, a wall literal, a string, a date, a ``BIGINT`` epoch, a
  ``NULL``, and one column of each type from the one-row temp view ``zone_oracle_1_one_row``
  (selected ``FROM range(1)`` so the planner cannot fold it into the query as a literal);
- leaf conversions: ``CAST``, ``TRY_CAST``, ``::``, ``to_timestamp_ntz``, ``to_timestamp_ltz``,
  ``to_utc_timestamp``, ``from_utc_timestamp``, ``date_format``, ``to_date``, ``unix_micros``,
  ``to_json``;
- containers: bare, ``named_struct``, ``array``, ``map`` with a literal key, one and two deep,
  and a struct inside an array;
- target types: ``TIMESTAMP_NTZ``, ``TIMESTAMP``, ``STRING``, ``DATE``, ``BIGINT``;
- zones: ``UTC``, ``America/New_York``, ``Asia/Kolkata``, ``+05:30``, ``Etc/UTC``.

A conversion sits either on the leaf inside a container (the ``inner`` level), on the container
itself as a cast to the same shape with the target type at the leaf (the ``container`` level),
or chains two container casts (the ``through`` level).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

ZONES: tuple[str, ...] = ("UTC", "America/New_York", "Asia/Kolkata", "+05:30", "Etc/UTC")
ZONE_TAGS: dict[str, str] = {
    "UTC": "utc",
    "America/New_York": "new_york",
    "Asia/Kolkata": "kolkata",
    "+05:30": "p0530",
    "Etc/UTC": "etc_utc",
}
TARGETS: dict[str, str] = {
    "ntz": "TIMESTAMP_NTZ",
    "ltz": "TIMESTAMP",
    "string": "STRING",
    "date": "DATE",
    "bigint": "BIGINT",
}
WALL_TEXT = "2026-01-02 03:04:05.123456"
EPOCH_SECONDS = 1767323045
VIEW_NAME = "zone_oracle_1_one_row"
SAMPLE_SIZE = 20

LITERALS: dict[str, str] = {
    "inst": f"TIMESTAMP '{WALL_TEXT}+00:00'",
    "wall": f"TIMESTAMP_NTZ '{WALL_TEXT}'",
    "str": f"'{WALL_TEXT}'",
    "date": "DATE '2026-01-02'",
    "epoch": f"CAST({EPOCH_SECONDS} AS BIGINT)",
}
NULL_SOURCE: dict[str, str] = {"null": "NULL"}
COLUMNS: dict[str, str] = {
    "col_inst": "c_inst",
    "col_wall": "c_wall",
    "col_str": "c_str",
    "col_date": "c_date",
    "col_epoch": "c_epoch",
}
VIEW_SQL = (
    f"CREATE OR REPLACE TEMP VIEW {VIEW_NAME} AS SELECT "
    f"{LITERALS['inst']} AS c_inst, {LITERALS['wall']} AS c_wall, {LITERALS['str']} AS c_str, "
    f"{LITERALS['date']} AS c_date, {LITERALS['epoch']} AS c_epoch FROM range(1)"
)


@dataclass(frozen=True)
class Shape:
    """One container: how it wraps a leaf, its cast type, and how to read the leaf back."""

    wrap: str
    cast_type: str
    read: tuple[str, ...]


SHAPES: dict[str, Shape] = {
    "struct": Shape("named_struct('a', {x})", "STRUCT<a: {t}>", (".a",)),
    "array": Shape("array({x})", "ARRAY<{t}>", ("[0]",)),
    "map": Shape("map('k', {x})", "MAP<STRING, {t}>", ("['k']",)),
    "struct2": Shape(
        "named_struct('a', named_struct('b', {x}))", "STRUCT<a: STRUCT<b: {t}>>", (".a", ".b")
    ),
    "array2": Shape("array(array({x}))", "ARRAY<ARRAY<{t}>>", ("[0][0]",)),
    "array_struct": Shape("array(named_struct('a', {x}))", "ARRAY<STRUCT<a: {t}>>", ("[0]", ".a")),
}
SHALLOW: tuple[str, ...] = ("struct", "array", "map")
DEEP: tuple[str, ...] = ("struct2", "array2", "array_struct")

FUNCTIONS: dict[str, str] = {
    "to_timestamp_ntz": "to_timestamp_ntz({x})",
    "to_timestamp_ltz": "to_timestamp_ltz({x})",
    "to_utc_timestamp": "to_utc_timestamp({x}, 'Asia/Kolkata')",
    "from_utc_timestamp": "from_utc_timestamp({x}, 'America/New_York')",
    "date_format": "date_format({x}, 'yyyy-MM-dd HH:mm:ss')",
    "to_date": "to_date({x})",
    "unix_micros": "unix_micros({x})",
}
MICROS_FORMAT: dict[str, str] = {
    "date_format_micros": "date_format({x}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
}


@dataclass(frozen=True)
class Expression:
    """One zone-free expression with the axis values that name it."""

    key: str
    container: str
    source: str
    conversion: str
    level: str
    target: str
    sql: str
    column: bool


@dataclass(frozen=True)
class Cell:
    """One expression run in one session zone."""

    cell_id: str
    zone: str
    expression: Expression

    @property
    def sql(self) -> str:
        """Return the statement of this cell."""
        return self.expression.sql


def _source_text(source: str) -> str:
    """Return the SQL text of a value source."""
    for table in (LITERALS, NULL_SOURCE, COLUMNS):
        if source in table:
            return table[source]
    raise KeyError(source)


def _conversion(label: str, text: str) -> str:
    """Apply the leaf conversion ``label`` to the expression ``text``."""
    kind, _, tag = label.partition("_")
    if label in FUNCTIONS:
        return FUNCTIONS[label].replace("{x}", text)
    if label in MICROS_FORMAT:
        return MICROS_FORMAT[label].replace("{x}", text)
    target = TARGETS[tag]
    if kind == "cast":
        return f"CAST({text} AS {target})"
    if kind == "try":
        return f"TRY_CAST({text} AS {target})"
    if kind == "colons":
        return f"{text}::{target}"
    raise KeyError(label)


def _tag_of(label: str) -> str:
    """Return the target tag a conversion label names, or an empty string."""
    tag = label.rsplit("_", 1)[-1]
    return tag if tag in TARGETS and label not in FUNCTIONS else ""


def _query(container: str, source: str, inner: str, *, read: bool) -> str:
    """Build the ``SELECT`` that runs ``inner`` and, when ``read`` is set, reads the leaf back."""
    suffix = f" FROM {VIEW_NAME}" if source in COLUMNS else ""
    if not read:
        return f"SELECT {inner} AS v{suffix}"
    sql = f"SELECT {inner} AS c0{suffix}"
    steps = SHAPES[container].read
    for index, step in enumerate(steps):
        name = "v" if index == len(steps) - 1 else f"c{index + 1}"
        sql = f"SELECT c{index}{step} AS {name} FROM ({sql}) q{index}"
    return sql


def _bare(source: str, label: str) -> Expression:
    """Build a leaf conversion of a value source with no container."""
    sql = _query("", source, _conversion(label, _source_text(source)), read=False)
    return Expression(
        key=f"bare.{source}.{label}",
        container="bare",
        source=source,
        conversion=label,
        level="leaf",
        target=_tag_of(label),
        sql=sql,
        column=source in COLUMNS,
    )


def _inner(container: str, source: str, label: str) -> Expression:
    """Build a container whose leaf is converted inside it; the whole container is selected."""
    leaf = _conversion(label, _source_text(source))
    wrapped = SHAPES[container].wrap.replace("{x}", leaf)
    return Expression(
        key=f"{container}.{source}.{label}.inner",
        container=container,
        source=source,
        conversion=label,
        level="inner",
        target=_tag_of(label),
        sql=_query(container, source, wrapped, read=False),
        column=source in COLUMNS,
    )


def _container_cast(container: str, source: str, tag: str, *, safe: bool) -> Expression:
    """Build a cast of a container to its own shape with ``tag`` at the leaf; read the leaf."""
    shape = SHAPES[container]
    wrapped = shape.wrap.replace("{x}", _source_text(source))
    target = shape.cast_type.replace("{t}", TARGETS[tag])
    verb = "TRY_CAST" if safe else "CAST"
    label = f"ctry_{tag}" if safe else f"ccast_{tag}"
    return Expression(
        key=f"{container}.{source}.{label}",
        container=container,
        source=source,
        conversion=label,
        level="container",
        target=tag,
        sql=_query(container, source, f"{verb}({wrapped} AS {target})", read=True),
        column=source in COLUMNS,
    )


def _to_json(container: str, source: str) -> Expression:
    """Build ``to_json`` of a container holding a source; the JSON text is selected."""
    wrapped = SHAPES[container].wrap.replace("{x}", _source_text(source))
    return Expression(
        key=f"{container}.{source}.to_json",
        container=container,
        source=source,
        conversion="to_json",
        level="container",
        target="",
        sql=_query(container, source, f"to_json({wrapped})", read=False),
        column=source in COLUMNS,
    )


def _through(name: str, container: str, source: str, middle: str, tag: str) -> Expression:
    """Build two chained container casts, source to ``middle`` to ``tag``; read the leaf."""
    shape = SHAPES[container]
    wrapped = shape.wrap.replace("{x}", _source_text(source))
    first = shape.cast_type.replace("{t}", TARGETS[middle])
    last = shape.cast_type.replace("{t}", TARGETS[tag])
    return Expression(
        key=f"through.{name}",
        container=container,
        source=source,
        conversion=f"through_{middle}_{tag}",
        level="through",
        target=tag,
        sql=_query(container, source, f"CAST(CAST({wrapped} AS {first}) AS {last})", read=True),
        column=source in COLUMNS,
    )


def _leaf_over_container(source: str) -> Expression:
    """Build a leaf cast over a string a container cast made."""
    shape = SHAPES["array"]
    wrapped = shape.wrap.replace("{x}", _source_text(source))
    made = f"CAST({wrapped} AS {shape.cast_type.replace('{t}', 'STRING')})[0]"
    return Expression(
        key=f"through.leaf_over_array_{source}",
        container="array",
        source=source,
        conversion="through_string_ntz_leaf",
        level="through",
        target="ntz",
        sql=f"SELECT CAST({made} AS TIMESTAMP_NTZ) AS v",
        column=False,
    )


def _json_round_trip(source: str, schema: str) -> Expression:
    """Build ``from_json(to_json(...))`` back to a struct with a timestamp field; read it."""
    wrapped = SHAPES["struct"].wrap.replace("{x}", _source_text(source))
    tag = "ntz" if "NTZ" in schema else "ltz"
    return Expression(
        key=f"through.json_{source}_{tag}",
        container="struct",
        source=source,
        conversion=f"through_json_{tag}",
        level="through",
        target=tag,
        sql=_query("struct", source, f"from_json(to_json({wrapped}), 'a {schema}')", read=True),
        column=False,
    )


def _bare_expressions() -> list[Expression]:
    """Return the expressions with no container."""
    out: list[Expression] = []
    for source in (*LITERALS, *NULL_SOURCE, *COLUMNS):
        out.extend(_bare(source, f"cast_{tag}") for tag in TARGETS)
    for source in LITERALS:
        out.extend(_bare(source, f"try_{tag}") for tag in TARGETS)
    for source in ("inst", "wall", "str"):
        out.extend(_bare(source, f"colons_{tag}") for tag in ("ntz", "ltz", "string"))
    for source in (*LITERALS, *NULL_SOURCE):
        out.extend(_bare(source, name) for name in FUNCTIONS)
    out.extend(_bare(source, "date_format_micros") for source in ("inst", "wall"))
    for source in ("col_inst", "col_wall", "col_str"):
        out.extend(
            _bare(source, name)
            for name in ("to_utc_timestamp", "date_format", "to_date", "unix_micros")
        )
    return out


def _container_cast_expressions() -> list[Expression]:
    """Return the container-level casts, TRY_CAST and ``to_json`` expressions."""
    out: list[Expression] = []
    for container in SHALLOW:
        for source in ("inst", "wall", "str", "date"):
            out.extend(_container_cast(container, source, tag, safe=False) for tag in TARGETS)
    for container in DEEP:
        out.extend(_container_cast(container, "inst", tag, safe=False) for tag in TARGETS)
        out.append(_container_cast(container, "wall", "ltz", safe=False))
    for container in SHALLOW:
        for tag in ("ntz", "string", "date"):
            out.append(_container_cast(container, "inst", tag, safe=True))
        out.append(_container_cast(container, "wall", "ltz", safe=True))
    for container in SHAPES:
        out.extend(_to_json(container, source) for source in ("inst", "wall", "date"))
    out.extend(_to_json(container, "col_inst") for container in SHALLOW)
    return out


def _inner_expressions() -> list[Expression]:
    """Return the containers whose leaf is converted inside them."""
    out: list[Expression] = []
    inner_inst = (
        "cast_ntz",
        "cast_string",
        "cast_date",
        "to_utc_timestamp",
        "from_utc_timestamp",
        "date_format",
        "to_date",
        "unix_micros",
    )
    for container in SHALLOW:
        out.extend(_inner(container, "inst", name) for name in inner_inst)
        out.extend(_inner(container, "wall", name) for name in ("cast_ltz", "cast_string"))
        out.extend(_inner(container, "str", name) for name in ("cast_ltz", "cast_ntz"))
    for container in DEEP:
        out.extend(_inner(container, "inst", name) for name in ("cast_ntz", "cast_string"))
    return out


def _column_expressions() -> list[Expression]:
    """Return the container expressions over a column of the one-row view."""
    out: list[Expression] = []
    for container in SHALLOW:
        out.extend(_inner(container, "col_inst", name) for name in ("cast_ntz", "cast_string"))
        out.append(_inner(container, "col_wall", "cast_ltz"))
        out.extend(
            _container_cast(container, "col_inst", tag, safe=False) for tag in ("ntz", "string")
        )
        out.append(_container_cast(container, "col_wall", "ltz", safe=False))
    return out


def _through_expressions() -> list[Expression]:
    """Return the chained-cast expressions of card CONTAINER-CAST-ZONE-1."""
    out: list[Expression] = [
        _through("struct_inst_string_ntz", "struct", "inst", "string", "ntz"),
        _through("struct_inst_date_ntz", "struct", "inst", "date", "ntz"),
        _through("struct_inst_bigint_ntz", "struct", "inst", "bigint", "ntz"),
        _through("struct_wall_string_ltz", "struct", "wall", "string", "ltz"),
        _through("array_inst_string_ntz", "array", "inst", "string", "ntz"),
        _through("array_inst_date_ntz", "array", "inst", "date", "ntz"),
        _through("map_inst_string_ntz", "map", "inst", "string", "ntz"),
        _leaf_over_container("inst"),
        _json_round_trip("inst", "TIMESTAMP_NTZ"),
        _json_round_trip("wall", "TIMESTAMP_NTZ"),
        _json_round_trip("inst", "TIMESTAMP"),
        _json_round_trip("wall", "TIMESTAMP"),
    ]
    return out


def expressions() -> list[Expression]:
    """Return every zone-free expression, in generation order, with unique keys."""
    out = [
        *_bare_expressions(),
        *_container_cast_expressions(),
        *_inner_expressions(),
        *_column_expressions(),
        *_through_expressions(),
    ]
    keys = [expression.key for expression in out]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate expression keys in the zone-oracle corpus")
    return out


def cells() -> list[Cell]:
    """Return every cell: each expression in each zone, expression-major."""
    return [
        Cell(f"{expression.key}.{ZONE_TAGS[zone]}", zone, expression)
        for expression in expressions()
        for zone in ZONES
    ]


def corpus_hash() -> str:
    """Return the SHA-256 of the ordered cell ids, zones and statements and the view statement."""
    payload = json.dumps(
        [VIEW_SQL, *([cell.cell_id, cell.zone, cell.sql] for cell in cells())],
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sample_ids(size: int = SAMPLE_SIZE) -> list[str]:
    """Return the ``size`` cell ids with the smallest SHA-256, in hash order."""
    ranked = sorted(
        (hashlib.sha256(cell.cell_id.encode("utf-8")).hexdigest(), cell.cell_id) for cell in cells()
    )
    return [cell_id for _, cell_id in ranked[:size]]


def main() -> int:
    """Print the cell count, the expression count and the corpus hash."""
    all_cells = cells()
    print(f"cells={len(all_cells)} expressions={len(expressions())} sha256={corpus_hash()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
