"""Write-path partition-key VALUE audit — CTAS + INSERT vs live Spark 4.1.2 + Iceberg.

Pin what each engine *writes* into Iceberg partition slots, not what a SELECT over the
source would extract. Three classes:

* **carry_check** — identity (int / string / date / timestamp), ``bucket``, ``truncate``,
  and Iceberg ``years``/``months``/``days``/``hours``(ts); the temporal transforms are
  UTC-epoch *from 1970* (fork ``Transform::Year`` etc.), pinned Rust-side at
  ``crates/repark-spark/src/tests/partitioned_ctas.rs``
  ``ctas_temporal_partition_spec_and_routing`` (distinct-count) — this corpus pins VALUES.
* **load_bearing** — identity partitions of SQL ``year(ts)`` / ``date_format(ts, …)`` under a
  **non-UTC** session (the classic silent-corruption hole).
* **tz8** — identity partitions of ``CAST(ts AS DATE)`` / ``to_date(ts)`` as session-zone
  date keys; ``datediff`` rides CAST, ``last_day`` / ``date_add`` over TIMESTAMP stay
  residual.

Transforms the engine refuses get **refusal-class** pins (needle + class), never silent
skips; the swept transform x type matrix is enumerated in
``task/v4-partition-values-ledger.md``.

**Oracle.** Every Spark half is RECORDED against live PySpark 4.1.2 + Iceberg
(``org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0``) under zulu-17,
``master("local[2]")``, ANSI on, ``spark.sql.shuffle.partitions=2``. One multi-step recipe
per row (create → write → read data + ``.files`` / ``.partitions``) runs on BOTH engines.

**Rows assert on the Arrow path** (``to_arrow`` / Spark ``toArrow``): value AND type AND
nullability — never ``show``. Partition VALUES are canonical JSON of the metadata-table
``partition`` struct; the spec is the field-name + transform suffix; manifest summaries are
per-slot ``record_count`` on both ``files`` and ``partitions``.

**Re-deriving the goldens (record mode).** The driver is committed beside this module::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_partition_value_goldens.py

It imports ``ROWS`` from THIS module. Never collected by pytest. CI stays JVM-free.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
from collections.abc import Iterator
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
import pytest
from _oracle_pins import (
    ICEBERG_RUNTIME_VERSION,
    ICEBERG_SPARK_RUNTIME_GAV,
    ICEBERG_SPARK_RUNTIME_NOTE,
    ICEBERG_SPARK_SCALA_BINARY,
    _pinned_pyspark_version,
    _spark_major_minor,
)

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.spark.session import ReparkSession

# Re-export so the record driver / GAV pin keep one importable home (_oracle_pins).
_ICEBERG_SPARK_SCALA_BINARY = ICEBERG_SPARK_SCALA_BINARY
_ICEBERG_RUNTIME_VERSION = ICEBERG_RUNTIME_VERSION

REPARK_CATALOG = "mem"
REPARK_NAMESPACE = "ns"
SOURCE_VIEW = "src"
SESSION_TIME_ZONE_KEY = "spark.sql.session.timeZone"

ZONE_NEW_YORK = "America/New_York"
ZONE_TOKYO = "Asia/Tokyo"
ZONE_UTC = "UTC"

# Budget gate (CP-10); name-gated coverage lives in the budget pin.
BUDGET_MIN = 24
BUDGET_MAX = 34

# Fork #192 projects timestamptz/timestamptz_ns in data_file metadata tables (F-V4-1 unlocked).


# Arrow helpers

_I64 = pa.int64()
_I32 = pa.int32()
_STR = pa.string()
_DATE = pa.date32()
_TS_UTC = pa.timestamp("us", "UTC")


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, then values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


def _meta(
    spec: str,
    slots: list[tuple[str, str, int]],
) -> pa.Table:
    """Uniform metadata golden: one row per (surface, partition slot, record_count)."""
    surfaces = [row[0] for row in slots]
    specs = [spec] * len(slots)
    slot_json = [row[1] for row in slots]
    counts = [row[2] for row in slots]
    return _table(
        [
            ("surface", _STR, False),
            ("spec", _STR, False),
            ("slot", _STR, False),
            ("record_count", _I64, False),
        ],
        {
            "surface": surfaces,
            "spec": specs,
            "slot": slot_json,
            "record_count": counts,
        },
    )


# Instants — RFC-3339 checkable without epoch arithmetic

# Iceberg years/months/days/hours carry-check (matches partitioned_ctas.rs micros + the
# year-boundary instant that separates UTC-epoch 54 from SQL year 2023 under New York).
EPOCH = dt.datetime(1970, 1, 1, 0, 0, tzinfo=dt.UTC)
EPOCH_PLUS_DAY = dt.datetime(1970, 1, 2, 0, 0, tzinfo=dt.UTC)
EPOCH_PLUS_DAY_5H = dt.datetime(1970, 1, 2, 5, 0, tzinfo=dt.UTC)
# 2024-01-01 04:30Z = 2023-12-31 23:30 America/New_York; UTC/Tokyo stay 2024.
NY_YEAR_BOUNDARY = dt.datetime(2024, 1, 1, 4, 30, tzinfo=dt.UTC)
MODERN = dt.datetime(2024, 6, 15, 12, 0, tzinfo=dt.UTC)
# 2023-12-31 14:30Z = 2023-12-31 23:30 Asia/Tokyo; 2024-01-01 00:30 would be the other side.
TOKYO_YEAR_BOUNDARY = dt.datetime(2023, 12, 31, 14, 30, tzinfo=dt.UTC)

TEMPORAL_INSTANTS: tuple[dt.datetime, ...] = (
    EPOCH,
    EPOCH_PLUS_DAY,
    EPOCH_PLUS_DAY_5H,
    NY_YEAR_BOUNDARY,
    MODERN,
)
LOAD_INSTANTS: tuple[dt.datetime, ...] = (
    NY_YEAR_BOUNDARY,
    TOKYO_YEAR_BOUNDARY,
    MODERN,
)
IDENTITY_TS_INSTANTS: tuple[dt.datetime, ...] = (NY_YEAR_BOUNDARY, MODERN)
NAIVE_WALLS: tuple[dt.datetime, ...] = (
    dt.datetime(2024, 1, 1, 4, 30),
    dt.datetime(2024, 6, 15, 12, 0),
)
INT_ROWS: tuple[tuple[int, str], ...] = ((1, "a"), (2, "b"), (15, "aa"))
STRING_ROWS: tuple[tuple[int, str], ...] = ((1, "alpha"), (2, "beta"))
DATE_ROWS: tuple[tuple[int, dt.date], ...] = (
    (1, dt.date(2024, 3, 15)),
    (2, dt.date(2025, 6, 1)),
)


# Row shape


Family = Literal["carry_check", "load_bearing", "tz8", "refuse"]
Kind = Literal["content", "split", "error"]
WriteForm = Literal["ctas", "insert"]
SourceKind = Literal["ints", "strings", "dates", "instants", "naive", "temporal", "load"]


@dataclass(frozen=True)
class PartitionValueRow:
    """One write-path audit row: recipe + recorded Spark halves + repark half.

    ``kind="content"`` — write succeeds on the engine under test. ``repark_data is None``
    means DATA equality against ``spark_data``; a non-None table is a DATA disclosure.
    Same rule for ``repark_meta`` / ``spark_meta``. ``repark_meta_error_needle`` pins a
    metadata-projection refuse after a successful write (timestamptz identity).

    ``kind="split"`` — repark REFUSES the write; Spark succeeds (``spark_data`` recorded).

    ``kind="error"`` — both engines refuse; needles are the refusing component's own tokens.
    """

    name: str
    family: Family
    kind: Kind
    session_time_zone: str
    write_form: WriteForm
    source: SourceKind
    partition_clause: str
    select_sql: str
    data_sql: str
    note: str
    create_columns: str | None = None
    spark_data: pa.Table | None = None
    spark_meta: pa.Table | None = None
    repark_data: pa.Table | None = None
    repark_meta: pa.Table | None = None
    spark_error_needle: str | None = None
    repark_error_needle: str | None = None
    spark_meta_error_needle: str | None = None
    repark_meta_error_needle: str | None = None


@dataclass(frozen=True)
class WriteAudit:
    """Observation of one write: table contents, metadata slots, or a write/meta error."""

    data: pa.Table | None
    meta: pa.Table | None
    write_error: str | None
    meta_error: str | None


# Canonical partition-slot JSON (engine-agnostic)


def _json_ready(value: object) -> object:
    """Turn an Arrow/Python partition cell into a JSON-stable value (UTC ISO for instants)."""
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): _json_ready(value[key]) for key in sorted(value)}
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.strftime("%Y-%m-%dT%H:%M:%S.%f")
        return value.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    return value


def _slot_json(partition_cell: object) -> str:
    """Canonical JSON for one ``files.partition`` / ``partitions.partition`` struct cell."""
    return json.dumps(_json_ready(partition_cell), separators=(",", ":"), sort_keys=True)


def _transform_from_field_name(field_name: str) -> str:
    """Iceberg Java suffix → transform token (identity if no known suffix)."""
    for suffix, token in (
        ("_bucket", "bucket"),
        ("_trunc", "truncate"),
        ("_year", "year"),
        ("_month", "month"),
        ("_day", "day"),
        ("_hour", "hour"),
    ):
        if field_name.endswith(suffix):
            return token
    return "identity"


def _spec_from_partition_type(partition_type: pa.DataType) -> str:
    """``id:identity`` / ``ts_year:year`` joined in struct-field order."""
    if not pa.types.is_struct(partition_type):
        return f"not-a-struct:{partition_type}"
    parts: list[str] = []
    for index in range(partition_type.num_fields):
        field = partition_type.field(index)
        parts.append(f"{field.name}:{_transform_from_field_name(field.name)}")
    return "|".join(parts) if parts else "unpartitioned"


def extract_meta(session: Any, fq_table: str) -> pa.Table:
    """Read ``.files`` + ``.partitions`` and return the uniform META table.

    Fork #192 projects timestamptz partition fields (F-V4-1 unlocked).
    """
    files = read_table(session, f"SELECT partition, record_count FROM {fq_table}.files")
    partitions = read_table(session, f"SELECT partition, record_count FROM {fq_table}.partitions")
    spec = _spec_from_partition_type(files.schema.field("partition").type)
    rows: list[tuple[str, str, int]] = []
    for surface, table in (("files", files), ("partitions", partitions)):
        slots = table.column("partition").to_pylist()
        counts = table.column("record_count").to_pylist()
        for slot, count in zip(slots, counts, strict=True):
            rows.append((surface, _slot_json(slot), int(count)))
    return _meta(spec, rows)


# Source registration + lifecycle (recipe SSOT the record driver imports)


def target_fqn(catalog: str, namespace: str, table: str) -> str:
    """Three-part Iceberg table name both engines accept."""
    return f"{catalog}.{namespace}.{table}"


def ensure_namespace(session: Any, catalog: str, namespace: str) -> None:
    """Create ``catalog.namespace`` if missing (idempotent on both engines)."""
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def drop_table_if_exists(session: Any, fq_table: str) -> None:
    """Drop an Iceberg table if present. Best-effort: a missing table is fine."""
    with contextlib.suppress(Exception):
        session.sql(f"DROP TABLE IF EXISTS {fq_table}")
    with contextlib.suppress(Exception):
        session.sql(f"DROP TABLE {fq_table}")


def drop_source_view(session: Any) -> None:
    """Drop the shared source temp view if the session still holds it."""
    catalog = getattr(session, "catalog", None)
    if catalog is not None and hasattr(catalog, "dropTempView"):
        with contextlib.suppress(Exception):
            catalog.dropTempView(SOURCE_VIEW)
    with contextlib.suppress(Exception):
        session.sql(f"DROP VIEW IF EXISTS {SOURCE_VIEW}")


def read_table(session: Any, read_sql: str) -> pa.Table:
    """Run SQL and return Arrow (facade ``to_arrow`` or Spark ``toArrow``)."""
    frame = session.sql(read_sql)
    to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow
    return to_arrow()  # type: ignore[no-any-return]


def register_source(session: Any, source: SourceKind) -> None:
    """Register :data:`SOURCE_VIEW` from the named fixture on either engine."""
    drop_source_view(session)
    if source == "ints":
        frame = session.createDataFrame(list(INT_ROWS), ["id", "name"])
    elif source == "strings":
        frame = session.createDataFrame(list(STRING_ROWS), ["id", "name"])
    elif source == "dates":
        frame = session.createDataFrame(list(DATE_ROWS), ["id", "d"])
    elif source == "instants":
        frame = session.createDataFrame(
            [(index, instant) for index, instant in enumerate(IDENTITY_TS_INSTANTS, start=1)],
            ["id", "ts"],
        )
    elif source == "temporal":
        frame = session.createDataFrame(
            [(index, instant) for index, instant in enumerate(TEMPORAL_INSTANTS, start=1)],
            ["id", "ts"],
        )
    elif source == "load":
        frame = session.createDataFrame(
            [(index, instant) for index, instant in enumerate(LOAD_INSTANTS, start=1)],
            ["id", "ts"],
        )
    elif source == "naive":
        frame = session.createDataFrame(
            [(index, wall) for index, wall in enumerate(NAIVE_WALLS, start=1)],
            ["id", "ts"],
        )
    else:
        raise AssertionError(f"unknown source kind {source!r}")
    frame.createOrReplaceTempView(SOURCE_VIEW)


def _write_sql(row: PartitionValueRow, fq_table: str) -> list[str]:
    """The CREATE / INSERT statements for ``row`` (one or two)."""
    if row.write_form == "ctas":
        return [
            f"CREATE TABLE {fq_table} USING iceberg "
            f"PARTITIONED BY ({row.partition_clause}) AS {row.select_sql}"
        ]
    if row.create_columns is None:
        raise AssertionError(f"{row.name}: insert form requires create_columns")
    return [
        f"CREATE TABLE {fq_table} ({row.create_columns}) USING iceberg "
        f"PARTITIONED BY ({row.partition_clause})",
        f"INSERT INTO {fq_table} {row.select_sql}",
    ]


def run_write_lifecycle(
    session: Any, row: PartitionValueRow, *, catalog: str, namespace: str
) -> WriteAudit:
    """Register source → CREATE/INSERT → read data + metadata. Drops the table in ``finally``.

    A write error is returned (not raised) so split/error rows can classify. A successful
    write whose metadata projection fails sets ``meta_error`` and still returns ``data``.
    """
    fq_table = target_fqn(catalog, namespace, row.name)
    ensure_namespace(session, catalog, namespace)
    register_source(session, row.source)
    try:
        try:
            for statement in _write_sql(row, fq_table):
                session.sql(statement)
        except Exception as exc:  # both engines' error types; the message is the pin
            return WriteAudit(data=None, meta=None, write_error=str(exc), meta_error=None)
        data = read_table(session, row.data_sql.format(target=fq_table))
        try:
            meta = extract_meta(session, fq_table)
        except Exception as exc:
            return WriteAudit(data=data, meta=None, write_error=None, meta_error=str(exc))
        return WriteAudit(data=data, meta=meta, write_error=None, meta_error=None)
    finally:
        drop_table_if_exists(session, fq_table)
        drop_source_view(session)


# The corpus


def _carry(
    name: str,
    *,
    write_form: WriteForm,
    source: SourceKind,
    partition_clause: str,
    select_sql: str,
    data_sql: str,
    note: str,
    create_columns: str | None = None,
    zone: str = ZONE_UTC,
    repark_meta_error_needle: str | None = None,
) -> PartitionValueRow:
    """A carry-check content row. Spark goldens are filled from the record driver."""
    return PartitionValueRow(
        name=name,
        family="carry_check",
        kind="content",
        session_time_zone=zone,
        write_form=write_form,
        source=source,
        partition_clause=partition_clause,
        select_sql=select_sql,
        data_sql=data_sql,
        note=note,
        create_columns=create_columns,
        repark_meta_error_needle=repark_meta_error_needle,
    )


def _load(
    name: str,
    *,
    write_form: WriteForm,
    source: SourceKind,
    partition_clause: str,
    select_sql: str,
    data_sql: str,
    note: str,
    zone: str,
    create_columns: str | None = None,
) -> PartitionValueRow:
    """A load-bearing identity-of-SQL-extractor row under ``zone``."""
    return PartitionValueRow(
        name=name,
        family="load_bearing",
        kind="content",
        session_time_zone=zone,
        write_form=write_form,
        source=source,
        partition_clause=partition_clause,
        select_sql=select_sql,
        data_sql=data_sql,
        note=note,
        create_columns=create_columns,
    )


def _tz8(
    name: str,
    *,
    select_sql: str,
    note: str,
) -> PartitionValueRow:
    """TZ-8 identity-partition pin: CAST/to_date as a session-zone date key."""
    return PartitionValueRow(
        name=name,
        family="tz8",
        kind="content",
        session_time_zone=ZONE_NEW_YORK,
        write_form="ctas",
        source="load",
        partition_clause="d",
        select_sql=select_sql,
        data_sql="SELECT id, d FROM {target} ORDER BY id",
        note=note,
    )


def _refuse(
    name: str,
    *,
    source: SourceKind,
    partition_clause: str,
    select_sql: str,
    repark_error_needle: str,
    note: str,
    kind: Kind = "error",
) -> PartitionValueRow:
    """A refusal-class row. Spark needle / kind is locked from the record run."""
    return PartitionValueRow(
        name=name,
        family="refuse",
        kind=kind,
        session_time_zone=ZONE_UTC,
        write_form="ctas",
        source=source,
        partition_clause=partition_clause,
        select_sql=select_sql,
        data_sql="SELECT * FROM {target}",
        note=note,
        repark_error_needle=repark_error_needle,
    )


ROWS: list[PartitionValueRow] = [
    # ----- carry-check: identity ---------------------------------------------------------------
    _carry(
        "carry_identity_int_ctas",
        write_form="ctas",
        source="ints",
        partition_clause="id",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, name FROM {target} ORDER BY id",
        note="identity int CTAS: partition slot equals the int column (carry-check).",
    ),
    _carry(
        "carry_identity_int_insert",
        write_form="insert",
        source="ints",
        partition_clause="id",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, name FROM {target} ORDER BY id",
        create_columns="id BIGINT, name STRING",
        note="identity int INSERT: same slots as the CTAS twin (write-path, not CTAS inference).",
    ),
    _carry(
        "carry_identity_string_ctas",
        write_form="ctas",
        source="strings",
        partition_clause="name",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, name FROM {target} ORDER BY id",
        note="identity string CTAS.",
    ),
    _carry(
        "carry_identity_date_ctas",
        write_form="ctas",
        source="dates",
        partition_clause="d",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, d FROM {target} ORDER BY id",
        note="identity date CTAS.",
    ),
    _carry(
        "carry_identity_timestamp_ctas",
        write_form="ctas",
        source="instants",
        partition_clause="ts",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, ts FROM {target} ORDER BY id",
        note=(
            "identity timestamp CTAS post-#79/#85 (timestamptz). Data must round-trip. "
            "Fork #192 unlocks timestamptz metadata projection (F-V4-1)."
        ),
    ),
    _carry(
        "carry_identity_timestamp_insert",
        write_form="insert",
        source="instants",
        partition_clause="ts",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, ts FROM {target} ORDER BY id",
        create_columns="id BIGINT, ts TIMESTAMP",
        note="identity timestamp INSERT twin of the CTAS row (F-V4-1 unlocked by fork #192).",
    ),
    # ----- carry-check: bucket / truncate ------------------------------------------------------
    _carry(
        "carry_bucket_int_ctas",
        write_form="ctas",
        source="ints",
        partition_clause="bucket(4, id)",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, name FROM {target} ORDER BY id",
        note="bucket(4, id) CTAS: murmur3 slots (must be recorded, never hand-computed).",
    ),
    _carry(
        "carry_truncate_int_ctas",
        write_form="ctas",
        source="ints",
        partition_clause="truncate(10, id)",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, name FROM {target} ORDER BY id",
        note="truncate(10, id): Iceberg int truncate (1,2→0; 15→10).",
    ),
    _carry(
        "carry_truncate_string_ctas",
        write_form="ctas",
        source="ints",
        partition_clause="truncate(2, name)",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, name FROM {target} ORDER BY id",
        note="truncate(2, name): string prefix slots (a / b / aa).",
    ),
    # ----- carry-check: Iceberg temporal UTC-epoch ---------------------------------------------
    _carry(
        "carry_years_ts_ctas",
        write_form="ctas",
        source="temporal",
        partition_clause="years(ts)",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, ts FROM {target} ORDER BY id",
        note=(
            "Iceberg years(ts) is UTC-epoch years from 1970 (0 and 54), NOT SQL year() "
            "(2023 under NY). Companion of partitioned_ctas.rs:446-488."
        ),
    ),
    _carry(
        "carry_year_singular_ts_ctas",
        write_form="ctas",
        source="temporal",
        partition_clause="year(ts)",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, ts FROM {target} ORDER BY id",
        note=(
            "PARTITIONED BY (year(ts)) is the Iceberg Year transform (alias of years), "
            "NOT identity of SQL year(). Slots must match carry_years_ts_ctas (0/54)."
        ),
    ),
    _carry(
        "carry_months_ts_ctas",
        write_form="ctas",
        source="temporal",
        partition_clause="months(ts)",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, ts FROM {target} ORDER BY id",
        note="Iceberg months(ts): months from 1970-01 (0 / 648 / 653), UTC-epoch.",
    ),
    _carry(
        "carry_days_ts_ctas",
        write_form="ctas",
        source="temporal",
        partition_clause="days(ts)",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, ts FROM {target} ORDER BY id",
        note=(
            "Iceberg days(ts): UTC calendar date (2024-01-01 for the NY-boundary instant, "
            "NOT 2023-12-31)."
        ),
    ),
    _carry(
        "carry_hours_ts_ctas",
        write_form="ctas",
        source="temporal",
        partition_clause="hours(ts)",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, ts FROM {target} ORDER BY id",
        note="Iceberg hours(ts): hours from 1970-01-01T00Z (0/24/29/…).",
    ),
    _carry(
        "carry_years_ts_insert",
        write_form="insert",
        source="temporal",
        partition_clause="years(ts)",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, ts FROM {target} ORDER BY id",
        create_columns="id BIGINT, ts TIMESTAMP",
        note="years(ts) INSERT twin — same UTC-epoch slots as the CTAS row.",
    ),
    _carry(
        "carry_years_date_ctas",
        write_form="ctas",
        source="dates",
        partition_clause="years(d)",
        select_sql="SELECT * FROM src",
        data_sql="SELECT id, d FROM {target} ORDER BY id",
        note="years(date): 2024-03-15 → 54, 2025-06-01 → 55.",
    ),
    # ----- load-bearing: SQL year / date_format identity under a non-UTC session ---------------
    _load(
        "load_year_ts_identity_new_york_ctas",
        write_form="ctas",
        source="load",
        partition_clause="y",
        select_sql="SELECT id, year(ts) AS y FROM src",
        data_sql="SELECT id, y FROM {target} ORDER BY id",
        zone=ZONE_NEW_YORK,
        note=(
            "LOAD-BEARING: identity of SQL year(ts) under America/New_York. The NY-boundary "
            "instant must land in partition y=2023 (not Iceberg years=54, not UTC year=2024)."
        ),
    ),
    _load(
        "load_year_ts_identity_tokyo_ctas",
        write_form="ctas",
        source="load",
        partition_clause="y",
        select_sql="SELECT id, year(ts) AS y FROM src",
        data_sql="SELECT id, y FROM {target} ORDER BY id",
        zone=ZONE_TOKYO,
        note=(
            "LOAD-BEARING Tokyo twin: 2023-12-31T14:30Z is y=2023 in Tokyo; "
            "2024-01-01T04:30Z is y=2024."
        ),
    ),
    _load(
        "load_year_ts_identity_utc_ctas",
        write_form="ctas",
        source="load",
        partition_clause="y",
        select_sql="SELECT id, year(ts) AS y FROM src",
        data_sql="SELECT id, y FROM {target} ORDER BY id",
        zone=ZONE_UTC,
        note="UTC control for the year() identity family (must not be satisfied by a NY row).",
    ),
    _load(
        "load_year_ts_identity_new_york_insert",
        write_form="insert",
        source="load",
        partition_clause="y",
        select_sql="SELECT id, year(ts) AS y FROM src",
        data_sql="SELECT id, y FROM {target} ORDER BY id",
        zone=ZONE_NEW_YORK,
        create_columns="id BIGINT, y INT",
        note="INSERT twin of the NY year() identity CTAS (same 2023 slot).",
    ),
    _load(
        "load_date_format_ts_new_york_ctas",
        write_form="ctas",
        source="load",
        partition_clause="formatted",
        select_sql="SELECT id, date_format(ts, 'yyyy-MM-dd') AS formatted FROM src",
        data_sql="SELECT id, formatted FROM {target} ORDER BY id",
        zone=ZONE_NEW_YORK,
        note=(
            "LOAD-BEARING: identity of date_format(ts, 'yyyy-MM-dd') under NY. "
            "NY-boundary instant → '2023-12-31'."
        ),
    ),
    _load(
        "load_zoneless_year_ts_identity_new_york_ctas",
        write_form="ctas",
        source="naive",
        partition_clause="y",
        select_sql="SELECT id, year(ts) AS y FROM src",
        data_sql="SELECT id, y FROM {target} ORDER BY id",
        zone=ZONE_NEW_YORK,
        note=(
            "Post-#85 zoneless (naive datetime) year() identity under NY: wall 2024-01-01 "
            "04:30 localizes to 09:30Z so y=2024. Distinct from the tz-aware NY-boundary row."
        ),
    ),
    # ----- TZ-8: CAST(ts AS DATE) / to_date as partition key (equality after R-4) --------------
    _tz8(
        "tz8_cast_ts_as_date_identity_new_york_ctas",
        select_sql="SELECT id, CAST(ts AS DATE) AS d FROM src",
        note=(
            "TZ-8 FIXED: CAST(ts AS DATE) as identity partition under NY. Both engines write "
            "the session-zone date (2023-12-31 for the NY-boundary instant). Flip evidence."
        ),
    ),
    _tz8(
        "tz8_to_date_ts_identity_new_york_ctas",
        select_sql="SELECT id, to_date(ts) AS d FROM src",
        note="TZ-8 FIXED: to_date(ts) as identity partition — same class as CAST AS DATE.",
    ),
    # ----- refuse (needles from repark probe; Spark needles locked at record) ------------------
    _refuse(
        "refuse_bucket_zero",
        source="ints",
        partition_clause="bucket(0, id)",
        select_sql="SELECT * FROM src",
        repark_error_needle="> 0",
        note="bucket(0, id) must be a loud analysis refuse (Spark/Iceberg reject numBuckets<=0).",
    ),
    _refuse(
        "refuse_truncate_zero",
        source="ints",
        partition_clause="truncate(0, name)",
        select_sql="SELECT * FROM src",
        repark_error_needle="> 0",
        note="truncate(0, name) must refuse width <= 0.",
    ),
    _refuse(
        "refuse_bucket_negative",
        source="ints",
        partition_clause="bucket(-1, id)",
        select_sql="SELECT * FROM src",
        repark_error_needle="> 0",
        note="bucket(-1, id) must refuse.",
    ),
    _refuse(
        "refuse_unknown_transform",
        source="ints",
        partition_clause="nonsense(id)",
        select_sql="SELECT * FROM src",
        repark_error_needle="not a supported partition transform",
        note="unknown transform name is a loud refuse, never a created table.",
    ),
    _refuse(
        "refuse_hours_on_date",
        source="dates",
        partition_clause="hours(d)",
        select_sql="SELECT * FROM src",
        repark_error_needle="Invalid source type",
        note="hours(date) is not a legal Iceberg Hour source (timestamp only).",
    ),
    _refuse(
        "refuse_void_transform",
        source="ints",
        partition_clause="void(id)",
        select_sql="SELECT * FROM src",
        repark_error_needle="not a supported partition transform",
        note=(
            "void() is refused by both engines on this spelling (Spark: Transform is not "
            "supported; repark: not a supported partition transform). Refusal-class pin."
        ),
    ),
]


# Recorded Spark 4.1.2 + Iceberg 1.11.0 halves (record mode: zulu-17, local[2], ANSI on).
# Fork #193: timestamptz Arrow annotation is UTC (F-V4-2). TZ-8 date-key rows are equality.
_GOLDENS: dict[str, dict[str, object]] = {
    "carry_identity_int_ctas": {
        "spark_data": _table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 15], "name": ["a", "b", "aa"]},
        ),
        "spark_meta": _meta(
            "id:identity",
            [
                ("files", '{"id":1}', 1),
                ("files", '{"id":15}', 1),
                ("files", '{"id":2}', 1),
                ("partitions", '{"id":1}', 1),
                ("partitions", '{"id":15}', 1),
                ("partitions", '{"id":2}', 1),
            ],
        ),
    },
    "carry_identity_int_insert": {
        "spark_data": _table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 15], "name": ["a", "b", "aa"]},
        ),
        "spark_meta": _meta(
            "id:identity",
            [
                ("files", '{"id":1}', 1),
                ("files", '{"id":15}', 1),
                ("files", '{"id":2}', 1),
                ("partitions", '{"id":1}', 1),
                ("partitions", '{"id":15}', 1),
                ("partitions", '{"id":2}', 1),
            ],
        ),
    },
    "carry_identity_string_ctas": {
        "spark_data": _table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2], "name": ["alpha", "beta"]},
        ),
        "spark_meta": _meta(
            "name:identity",
            [
                ("files", '{"name":"alpha"}', 1),
                ("files", '{"name":"beta"}', 1),
                ("partitions", '{"name":"alpha"}', 1),
                ("partitions", '{"name":"beta"}', 1),
            ],
        ),
    },
    "carry_identity_date_ctas": {
        "spark_data": _table(
            [("id", _I64, True), ("d", _DATE, True)],
            {"id": [1, 2], "d": [dt.date(2024, 3, 15), dt.date(2025, 6, 1)]},
        ),
        "spark_meta": _meta(
            "d:identity",
            [
                ("files", '{"d":"2025-06-01"}', 1),
                ("files", '{"d":"2024-03-15"}', 1),
                ("partitions", '{"d":"2025-06-01"}', 1),
                ("partitions", '{"d":"2024-03-15"}', 1),
            ],
        ),
    },
    "carry_bucket_int_ctas": {
        "spark_data": _table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 15], "name": ["a", "b", "aa"]},
        ),
        "spark_meta": _meta(
            "id_bucket:bucket",
            [("files", '{"id_bucket":0}', 3), ("partitions", '{"id_bucket":0}', 3)],
        ),
    },
    "carry_truncate_int_ctas": {
        "spark_data": _table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 15], "name": ["a", "b", "aa"]},
        ),
        "spark_meta": _meta(
            "id_trunc:truncate",
            [
                ("files", '{"id_trunc":10}', 1),
                ("files", '{"id_trunc":0}', 2),
                ("partitions", '{"id_trunc":10}', 1),
                ("partitions", '{"id_trunc":0}', 2),
            ],
        ),
    },
    "carry_truncate_string_ctas": {
        "spark_data": _table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 15], "name": ["a", "b", "aa"]},
        ),
        "spark_meta": _meta(
            "name_trunc:truncate",
            [
                ("files", '{"name_trunc":"a"}', 1),
                ("files", '{"name_trunc":"b"}', 1),
                ("files", '{"name_trunc":"aa"}', 1),
                ("partitions", '{"name_trunc":"a"}', 1),
                ("partitions", '{"name_trunc":"b"}', 1),
                ("partitions", '{"name_trunc":"aa"}', 1),
            ],
        ),
    },
    "load_year_ts_identity_new_york_ctas": {
        "spark_data": _table(
            [("id", _I64, True), ("y", _I32, True)],
            {"id": [1, 2, 3], "y": [2023, 2023, 2024]},
        ),
        "spark_meta": _meta(
            "y:identity",
            [
                ("files", '{"y":2024}', 1),
                ("files", '{"y":2023}', 2),
                ("partitions", '{"y":2024}', 1),
                ("partitions", '{"y":2023}', 2),
            ],
        ),
    },
    "load_year_ts_identity_tokyo_ctas": {
        "spark_data": _table(
            [("id", _I64, True), ("y", _I32, True)],
            {"id": [1, 2, 3], "y": [2024, 2023, 2024]},
        ),
        "spark_meta": _meta(
            "y:identity",
            [
                ("files", '{"y":2024}', 2),
                ("files", '{"y":2023}', 1),
                ("partitions", '{"y":2024}', 2),
                ("partitions", '{"y":2023}', 1),
            ],
        ),
    },
    "load_year_ts_identity_utc_ctas": {
        "spark_data": _table(
            [("id", _I64, True), ("y", _I32, True)],
            {"id": [1, 2, 3], "y": [2024, 2023, 2024]},
        ),
        "spark_meta": _meta(
            "y:identity",
            [
                ("files", '{"y":2024}', 2),
                ("files", '{"y":2023}', 1),
                ("partitions", '{"y":2024}', 2),
                ("partitions", '{"y":2023}', 1),
            ],
        ),
    },
    "load_year_ts_identity_new_york_insert": {
        "spark_data": _table(
            [("id", _I64, True), ("y", _I32, True)],
            {"id": [1, 2, 3], "y": [2023, 2023, 2024]},
        ),
        "spark_meta": _meta(
            "y:identity",
            [
                ("files", '{"y":2024}', 1),
                ("files", '{"y":2023}', 2),
                ("partitions", '{"y":2024}', 1),
                ("partitions", '{"y":2023}', 2),
            ],
        ),
    },
    "load_date_format_ts_new_york_ctas": {
        "spark_data": _table(
            [("id", _I64, True), ("formatted", _STR, True)],
            {"id": [1, 2, 3], "formatted": ["2023-12-31", "2023-12-31", "2024-06-15"]},
        ),
        "spark_meta": _meta(
            "formatted:identity",
            [
                ("files", '{"formatted":"2023-12-31"}', 2),
                ("files", '{"formatted":"2024-06-15"}', 1),
                ("partitions", '{"formatted":"2024-06-15"}', 1),
                ("partitions", '{"formatted":"2023-12-31"}', 2),
            ],
        ),
    },
    "load_zoneless_year_ts_identity_new_york_ctas": {
        "spark_data": _table(
            [("id", _I64, True), ("y", _I32, True)],
            {"id": [1, 2], "y": [2024, 2024]},
        ),
        "spark_meta": _meta(
            "y:identity",
            [("files", '{"y":2024}', 2), ("partitions", '{"y":2024}', 2)],
        ),
    },
    "refuse_bucket_zero": {"spark_error_needle": "Unsupported width"},
    "refuse_truncate_zero": {"spark_error_needle": "Unsupported width"},
    "refuse_bucket_negative": {"spark_error_needle": "Unsupported width"},
    "refuse_unknown_transform": {"spark_error_needle": "Transform is not supported"},
    "refuse_hours_on_date": {"spark_error_needle": "Invalid source type"},
    "refuse_void_transform": {"spark_error_needle": "Transform is not supported"},
}


def _temporal_spark_data() -> pa.Table:
    """The five-instant temporal fixture Spark wrote (UTC-annotated)."""
    return _table(
        [("id", _I64, True), ("ts", _TS_UTC, True)],
        {
            "id": [1, 2, 3, 4, 5],
            "ts": [EPOCH, EPOCH_PLUS_DAY, EPOCH_PLUS_DAY_5H, NY_YEAR_BOUNDARY, MODERN],
        },
    )


def _identity_ts_spark_data() -> pa.Table:
    """The two-instant identity-timestamp fixture Spark wrote."""
    return _table(
        [("id", _I64, True), ("ts", _TS_UTC, True)],
        {"id": [1, 2], "ts": [NY_YEAR_BOUNDARY, MODERN]},
    )


_GOLDENS["carry_identity_timestamp_ctas"] = {
    "spark_data": _identity_ts_spark_data(),
    "repark_data": None,
    "spark_meta": _meta(
        "ts:identity",
        [
            ("files", '{"ts":"2024-06-15T12:00:00.000000Z"}', 1),
            ("files", '{"ts":"2024-01-01T04:30:00.000000Z"}', 1),
            ("partitions", '{"ts":"2024-06-15T12:00:00.000000Z"}', 1),
            ("partitions", '{"ts":"2024-01-01T04:30:00.000000Z"}', 1),
        ],
    ),
}
_GOLDENS["carry_identity_timestamp_insert"] = {
    "spark_data": _identity_ts_spark_data(),
    "repark_data": None,
    "spark_meta": _meta(
        "ts:identity",
        [
            ("files", '{"ts":"2024-06-15T12:00:00.000000Z"}', 1),
            ("files", '{"ts":"2024-01-01T04:30:00.000000Z"}', 1),
            ("partitions", '{"ts":"2024-06-15T12:00:00.000000Z"}', 1),
            ("partitions", '{"ts":"2024-01-01T04:30:00.000000Z"}', 1),
        ],
    ),
}
_GOLDENS["carry_years_ts_ctas"] = {
    "spark_data": _temporal_spark_data(),
    "repark_data": None,
    "spark_meta": _meta(
        "ts_year:year",
        [
            ("files", '{"ts_year":54}', 2),
            ("files", '{"ts_year":0}', 3),
            ("partitions", '{"ts_year":54}', 2),
            ("partitions", '{"ts_year":0}', 3),
        ],
    ),
}
_GOLDENS["carry_year_singular_ts_ctas"] = {
    "spark_data": _temporal_spark_data(),
    "repark_data": None,
    "spark_meta": _meta(
        "ts_year:year",
        [
            ("files", '{"ts_year":54}', 2),
            ("files", '{"ts_year":0}', 3),
            ("partitions", '{"ts_year":54}', 2),
            ("partitions", '{"ts_year":0}', 3),
        ],
    ),
}
_GOLDENS["carry_months_ts_ctas"] = {
    "spark_data": _temporal_spark_data(),
    "repark_data": None,
    "spark_meta": _meta(
        "ts_month:month",
        [
            ("files", '{"ts_month":648}', 1),
            ("files", '{"ts_month":653}', 1),
            ("files", '{"ts_month":0}', 3),
            ("partitions", '{"ts_month":648}', 1),
            ("partitions", '{"ts_month":653}', 1),
            ("partitions", '{"ts_month":0}', 3),
        ],
    ),
}
_GOLDENS["carry_days_ts_ctas"] = {
    "spark_data": _temporal_spark_data(),
    "repark_data": None,
    "spark_meta": _meta(
        "ts_day:day",
        [
            ("files", '{"ts_day":"2024-01-01"}', 1),
            ("files", '{"ts_day":"1970-01-01"}', 1),
            ("files", '{"ts_day":"1970-01-02"}', 2),
            ("files", '{"ts_day":"2024-06-15"}', 1),
            ("partitions", '{"ts_day":"2024-01-01"}', 1),
            ("partitions", '{"ts_day":"1970-01-01"}', 1),
            ("partitions", '{"ts_day":"2024-06-15"}', 1),
            ("partitions", '{"ts_day":"1970-01-02"}', 2),
        ],
    ),
}
_GOLDENS["carry_hours_ts_ctas"] = {
    "spark_data": _temporal_spark_data(),
    "repark_data": None,
    "spark_meta": _meta(
        "ts_hour:hour",
        [
            ("files", '{"ts_hour":24}', 1),
            ("files", '{"ts_hour":29}', 1),
            ("files", '{"ts_hour":477348}', 1),
            ("files", '{"ts_hour":0}', 1),
            ("files", '{"ts_hour":473356}', 1),
            ("partitions", '{"ts_hour":24}', 1),
            ("partitions", '{"ts_hour":29}', 1),
            ("partitions", '{"ts_hour":477348}', 1),
            ("partitions", '{"ts_hour":0}', 1),
            ("partitions", '{"ts_hour":473356}', 1),
        ],
    ),
}
_GOLDENS["carry_years_ts_insert"] = {
    "spark_data": _temporal_spark_data(),
    "repark_data": None,
    "spark_meta": _meta(
        "ts_year:year",
        [
            ("files", '{"ts_year":54}', 2),
            ("files", '{"ts_year":0}', 3),
            ("partitions", '{"ts_year":54}', 2),
            ("partitions", '{"ts_year":0}', 3),
        ],
    ),
}
_GOLDENS["carry_years_date_ctas"] = {
    "spark_data": _table(
        [("id", _I64, True), ("d", _DATE, True)],
        {"id": [1, 2], "d": [dt.date(2024, 3, 15), dt.date(2025, 6, 1)]},
    ),
    "spark_meta": _meta(
        "d_year:year",
        [
            ("files", '{"d_year":54}', 1),
            ("files", '{"d_year":55}', 1),
            ("partitions", '{"d_year":54}', 1),
            ("partitions", '{"d_year":55}', 1),
        ],
    ),
}
_GOLDENS["tz8_cast_ts_as_date_identity_new_york_ctas"] = {
    "spark_data": _table(
        [("id", _I64, True), ("d", _DATE, True)],
        {
            "id": [1, 2, 3],
            "d": [dt.date(2023, 12, 31), dt.date(2023, 12, 31), dt.date(2024, 6, 15)],
        },
    ),
    # R-4: session-zone dates match Spark; flip both halves to equality.
    "repark_data": None,
    "spark_meta": _meta(
        "d:identity",
        [
            ("files", '{"d":"2023-12-31"}', 2),
            ("files", '{"d":"2024-06-15"}', 1),
            ("partitions", '{"d":"2023-12-31"}', 2),
            ("partitions", '{"d":"2024-06-15"}', 1),
        ],
    ),
    "repark_meta": None,
}
_GOLDENS["tz8_to_date_ts_identity_new_york_ctas"] = {
    "spark_data": _GOLDENS["tz8_cast_ts_as_date_identity_new_york_ctas"]["spark_data"],
    "repark_data": None,
    "spark_meta": _GOLDENS["tz8_cast_ts_as_date_identity_new_york_ctas"]["spark_meta"],
    "repark_meta": None,
}


def _with_recorded_halves(row: PartitionValueRow) -> PartitionValueRow:
    """Attach the recorded Spark (and disclosure) halves. Unknown names stay unrecorded."""
    golden = _GOLDENS.get(row.name)
    if golden is None:
        return row
    return replace(row, **golden)  # type: ignore[arg-type]


ROWS = [_with_recorded_halves(row) for row in ROWS]


# Comparator + session


def _frames_differ(actual: pa.Table, expected: pa.Table) -> bool:
    """True when the parity comparator rejects the pair."""
    try:
        assert_frames_equal(actual, expected)
    except FrameMismatchError:
        return True
    return False


def _repark_session(warehouse: Any, zone: str) -> ReparkSession:
    """A repark session with a memory catalog and the row's session zone (build-time knob)."""
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("partition-value-audit")
        .config(SESSION_TIME_ZONE_KEY, zone)
        .getOrCreate()
    )
    session.register_memory_catalog(REPARK_CATALOG, warehouse)
    ensure_namespace(session, REPARK_CATALOG, REPARK_NAMESPACE)
    return session


def _classify_content_half(
    *,
    name: str,
    actual: pa.Table,
    spark: pa.Table,
    repark: pa.Table | None,
    half: str,
    note: str,
) -> None:
    """Equality or disclosure classifier for one recorded half (data or meta)."""
    if repark is None:
        assert_frames_equal(actual, spark)
        return
    try:
        assert_frames_equal(actual, repark)
    except FrameMismatchError as mismatch:
        if not _frames_differ(actual, spark):
            raise AssertionError(
                f"{name}: repark and Spark have CONVERGED on the {half} half — repark now "
                f"produces the RECORDED SPARK {half}, so this disclosure is stale. Do not "
                f"delete the row: flip the {half} half to equality (repark_{half}=None) and "
                f"record the convergence. {note}"
            ) from mismatch
        raise AssertionError(
            f"{name}: repark moved OFF its pinned {half} disclosure and does NOT match the "
            f"recorded Spark {half} either — this is a regression, not a convergence. "
            f"Re-derive both halves in record mode before touching the pin. {note}"
        ) from mismatch
    assert _frames_differ(repark, spark), (
        f"{name}: the row's two recorded {half} halves are IDENTICAL, so it is not a "
        f"disclosure — flip repark_{half}=None or re-record it. {note}"
    )


def test_partition_value_row(row: PartitionValueRow, repark: ReparkSession) -> None:
    """Every recorded row: data + metadata on the Arrow path, or a classified refuse."""
    audit = run_write_lifecycle(repark, row, catalog=REPARK_CATALOG, namespace=REPARK_NAMESPACE)

    if row.kind == "error":
        assert row.repark_error_needle is not None
        assert audit.write_error is not None, (
            f"{row.name}: expected a write refuse, but the statement committed. {row.note}"
        )
        assert row.repark_error_needle in audit.write_error, (
            f"{row.name}: repark error missing {row.repark_error_needle!r}: "
            f"{audit.write_error!r}. {row.note}"
        )
        return

    if row.kind == "split":
        assert row.repark_error_needle is not None
        assert row.spark_data is not None
        if audit.write_error is not None:
            assert row.repark_error_needle in audit.write_error, (
                f"{row.name}: repark was expected to refuse with {row.repark_error_needle!r}, "
                f"got: {audit.write_error!r}. {row.note}"
            )
            assert row.spark_data.num_rows >= 1, f"{row.name}: spark golden is empty — re-record"
            return
        assert audit.data is not None
        if not _frames_differ(audit.data, row.spark_data):
            raise AssertionError(
                f"{row.name}: repark and Spark have CONVERGED — repark now succeeds with the "
                f"RECORDED SPARK data, so this split disclosure is stale. Do not delete the "
                f"row: flip it to kind='content', repark_data=None, clear the error needle. "
                f"{row.note}"
            )
        raise AssertionError(
            f"{row.name}: repark no longer refuses (write committed) but the data does NOT "
            f"match the recorded Spark golden — regression/partial, not a clean convergence. "
            f"Re-derive both halves in record mode. {row.note}"
        )

    # kind == "content"
    assert row.spark_data is not None, f"{row.name}: content row is missing its Spark data golden"
    assert audit.write_error is None, (
        f"{row.name}: write refused unexpectedly: {audit.write_error!r}. {row.note}"
    )
    assert audit.data is not None
    _classify_content_half(
        name=row.name,
        actual=audit.data,
        spark=row.spark_data,
        repark=row.repark_data,
        half="data",
        note=row.note,
    )

    if row.repark_meta_error_needle is not None:
        assert audit.meta_error is not None, (
            f"{row.name}: expected metadata-projection refuse "
            f"{row.repark_meta_error_needle!r}, but meta read succeeded. {row.note}"
        )
        assert row.repark_meta_error_needle in audit.meta_error, (
            f"{row.name}: meta error missing {row.repark_meta_error_needle!r}: "
            f"{audit.meta_error!r}. {row.note}"
        )
        return

    if row.spark_meta_error_needle is not None and row.spark_meta is None:
        # Both engines refuse the metadata projection — pin repark's token only here;
        # Spark's token is the record-driver concern.
        assert audit.meta_error is not None
        return

    assert row.spark_meta is not None, f"{row.name}: content row is missing its Spark meta golden"
    assert audit.meta_error is None, (
        f"{row.name}: metadata projection refused unexpectedly: {audit.meta_error!r}. {row.note}"
    )
    assert audit.meta is not None
    _classify_content_half(
        name=row.name,
        actual=audit.meta,
        spark=row.spark_meta,
        repark=row.repark_meta,
        half="meta",
        note=row.note,
    )


@pytest.fixture
def repark_warehouse(tmp_path: Any) -> Any:
    """Per-test warehouse directory for the repark memory catalog."""
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return warehouse


@pytest.fixture
def repark(repark_warehouse: Any, row: PartitionValueRow) -> Iterator[ReparkSession]:
    """Repark session zoned to the parametrized row (build-time session zone)."""
    session = _repark_session(repark_warehouse, row.session_time_zone)
    try:
        yield session
    finally:
        with contextlib.suppress(Exception):
            session.stop()


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize the row test from :data:`ROWS` (ids = row names)."""
    if "row" in metafunc.fixturenames:
        metafunc.parametrize("row", ROWS, ids=[item.name for item in ROWS])


# Budget / coverage / GAV / cleanup / classifier reachability


def test_partition_value_row_set_covers_the_v4_budget() -> None:
    """Corpus size and name-gated family coverage — a control row cannot satisfy a family pin."""
    assert BUDGET_MIN <= len(ROWS) <= BUDGET_MAX, (
        f"V-4 budget {BUDGET_MIN}-{BUDGET_MAX} rows (got {len(ROWS)})"
    )
    assert len({row.name for row in ROWS}) == len(ROWS), "row names are unique"

    families = {row.family for row in ROWS}
    assert families == {"carry_check", "load_bearing", "tz8", "refuse"}, families

    names = {row.name for row in ROWS}
    for needle in (
        "carry_identity_int",
        "carry_identity_string",
        "carry_identity_date",
        "carry_identity_timestamp",
        "carry_bucket_int",
        "carry_truncate_int",
        "carry_truncate_string",
        "carry_years_ts",
        "carry_year_singular_ts",
        "carry_months_ts",
        "carry_days_ts",
        "carry_hours_ts",
        "carry_years_date",
        "load_year_ts_identity_new_york",
        "load_year_ts_identity_tokyo",
        "load_year_ts_identity_utc",
        "load_date_format_ts_new_york",
        "load_zoneless_year_ts_identity",
        "tz8_cast_ts_as_date",
        "tz8_to_date_ts",
        "refuse_bucket_zero",
        "refuse_truncate_zero",
        "refuse_bucket_negative",
        "refuse_unknown_transform",
        "refuse_hours_on_date",
        "refuse_void_transform",
    ):
        assert any(needle in name for name in names), f"missing coverage for {needle!r}"

    assert any(row.write_form == "insert" for row in ROWS), "at least one INSERT write form"
    assert any(row.write_form == "ctas" for row in ROWS), "at least one CTAS write form"
    assert any(row.session_time_zone == ZONE_NEW_YORK for row in ROWS)
    assert any(row.session_time_zone == ZONE_TOKYO for row in ROWS)

    equality = [
        row
        for row in ROWS
        if row.kind == "content" and row.repark_data is None and row.family == "carry_check"
    ]
    assert equality, (
        "at least one carry-check content equality must assert repark == Spark — "
        "an all-disclosure corpus cannot tell agreement from a broken comparator"
    )

    refuses = [row for row in ROWS if row.family == "refuse"]
    assert len(refuses) >= 4, (
        f"refusal-class pins must enumerate the swept refuses (got {len(refuses)})"
    )
    for row in refuses:
        assert row.repark_error_needle, f"{row.name}: a refuse row must pin repark's own needle"

    tz8 = [row for row in ROWS if row.family == "tz8"]
    assert {row.name for row in tz8} == {
        "tz8_cast_ts_as_date_identity_new_york_ctas",
        "tz8_to_date_ts_identity_new_york_ctas",
    }
    assert all(row.repark_data is None and row.repark_meta is None for row in tz8), (
        "TZ-8 date-key rows are equality after the session-zone CAST/to_date fix"
    )
    for row in ROWS:
        if row.kind == "error":
            assert row.spark_error_needle, f"{row.name}: error row missing Spark needle"
            continue
        assert row.spark_data is not None, (
            f"{row.name}: content/split row missing Spark data golden"
        )
        # F-V4-1 still records Spark's successful meta projection even though repark refuses it.
        assert row.spark_meta is not None, f"{row.name}: missing Spark meta golden"


def test_iceberg_years_slots_are_utc_epoch_not_calendar_year() -> None:
    """Semantics-gated: years(ts) goldens must carry 0/54, never SQL year 2023/2024.

    A control identity-int row cannot satisfy this (CP-2). Unfilled goldens fail closed.
    """
    row = next(item for item in ROWS if item.name == "carry_years_ts_ctas")
    assert row.spark_meta is not None, "carry_years_ts_ctas Spark meta golden is not yet recorded"
    slots = " ".join(str(value) for value in row.spark_meta.column("slot").to_pylist())
    assert "54" in slots, f"years(ts) must write Iceberg years-from-1970 (54), got {slots}"
    assert "2023" not in slots, f"years(ts) must NOT write SQL year 2023: {slots}"
    assert "2024" not in slots, f"years(ts) must NOT write calendar year 2024: {slots}"


def test_sql_year_identity_new_york_slot_is_session_zone_year() -> None:
    """Semantics-gated: NY year() identity goldens must carry 2023 for the boundary instant."""
    row = next(item for item in ROWS if item.name == "load_year_ts_identity_new_york_ctas")
    assert row.spark_meta is not None, "NY year() identity Spark meta golden is not yet recorded"
    slots = " ".join(str(value) for value in row.spark_meta.column("slot").to_pylist())
    assert "2023" in slots, f"NY year() identity must write session-zone 2023, got {slots}"


def test_lifecycle_cleanup_after_refused_write(tmp_path: Any) -> None:
    """A refused CREATE leaves no stray table (lifecycle helper cleanup)."""
    warehouse = tmp_path / "cleanup"
    warehouse.mkdir()
    session = _repark_session(warehouse, ZONE_UTC)
    try:
        row = next(item for item in ROWS if item.name == "refuse_unknown_transform")
        audit = run_write_lifecycle(
            session, row, catalog=REPARK_CATALOG, namespace=REPARK_NAMESPACE
        )
        assert audit.write_error is not None
        assert row.repark_error_needle is not None
        assert row.repark_error_needle in audit.write_error
        tables = session.catalog.listTables(f"{REPARK_CATALOG}.{REPARK_NAMESPACE}")
        managed = [table.name for table in tables if not getattr(table, "isTemporary", False)]
        assert row.name not in managed, (
            f"refused write left stray table {row.name!r}; managed={managed}"
        )
    finally:
        with contextlib.suppress(Exception):
            session.stop()


def test_iceberg_gav_pin_is_exact_spark_minor() -> None:
    """Record-time GAV Spark-minor is derived from the pinned pyspark version (CP-8)."""
    pinned = _pinned_pyspark_version()
    major_minor = _spark_major_minor(pinned)
    expected_token = f"{major_minor}_{_ICEBERG_SPARK_SCALA_BINARY}"
    assert expected_token in ICEBERG_SPARK_RUNTIME_GAV, (
        f"prefer iceberg-spark-runtime whose Spark minor matches pinned pyspark {pinned} "
        f"(expected token {expected_token!r} in {ICEBERG_SPARK_RUNTIME_GAV!r})"
    )
    assert ICEBERG_SPARK_RUNTIME_GAV.endswith(f":{_ICEBERG_RUNTIME_VERSION}")
    assert major_minor in ICEBERG_SPARK_RUNTIME_NOTE
    assert pinned in ICEBERG_SPARK_RUNTIME_NOTE


_SYNTHETIC_SPLIT = PartitionValueRow(
    name="synthetic_split_exemplar",
    family="refuse",
    kind="split",
    session_time_zone=ZONE_UTC,
    write_form="ctas",
    source="ints",
    partition_clause="void(id)",
    select_sql="SELECT * FROM src",
    data_sql="SELECT id FROM {target}",
    note="synthetic CP-1 exemplar for the split classifier; not in ROWS.",
    spark_data=_table([("id", _I64, True)], {"id": [1]}),
    repark_error_needle="not a supported partition transform",
)

_SYNTHETIC_DISCLOSURE = PartitionValueRow(
    name="synthetic_disclosure_exemplar",
    family="tz8",
    kind="content",
    session_time_zone=ZONE_NEW_YORK,
    write_form="ctas",
    source="load",
    partition_clause="d",
    select_sql="SELECT id, CAST(ts AS DATE) AS d FROM src",
    data_sql="SELECT id, d FROM {target} ORDER BY id",
    note="synthetic CP-1 exemplar for the content-disclosure classifier; not in ROWS.",
    spark_data=_table(
        [("id", _I64, True), ("d", _DATE, True)], {"id": [1], "d": [dt.date(2023, 12, 31)]}
    ),
    repark_data=_table(
        [("id", _I64, True), ("d", _DATE, True)], {"id": [1], "d": [dt.date(2024, 1, 1)]}
    ),
    spark_meta=_meta("d:identity", [("files", '{"d":"2023-12-31"}', 1)]),
    repark_meta=_meta("d:identity", [("files", '{"d":"2024-01-01"}', 1)]),
)


def test_split_classifier_converged_arm(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """CP-1: split matching the Spark golden → CONVERGED flip guidance."""
    import test_partition_value_audit as audit_mod

    golden = _SYNTHETIC_SPLIT.spark_data
    assert golden is not None

    def _fake_success(
        _session: Any, _row: PartitionValueRow, *, catalog: str, namespace: str
    ) -> WriteAudit:
        _ = catalog, namespace
        return WriteAudit(data=golden, meta=None, write_error=None, meta_error=None)

    monkeypatch.setattr(audit_mod, "run_write_lifecycle", _fake_success)
    session = _repark_session(tmp_path, ZONE_UTC)
    try:
        with pytest.raises(AssertionError, match="CONVERGED") as excinfo:
            test_partition_value_row(_SYNTHETIC_SPLIT, session)
        message = str(excinfo.value)
        assert "Do not delete" in message
        assert "kind='content'" in message
    finally:
        with contextlib.suppress(Exception):
            session.stop()


def test_split_classifier_regression_arm(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """CP-1: split commits a non-Spark result → regression guidance."""
    import test_partition_value_audit as audit_mod

    wrong = _table([("id", _I64, True)], {"id": [99]})

    def _fake_wrong(
        _session: Any, _row: PartitionValueRow, *, catalog: str, namespace: str
    ) -> WriteAudit:
        _ = catalog, namespace
        return WriteAudit(data=wrong, meta=None, write_error=None, meta_error=None)

    monkeypatch.setattr(audit_mod, "run_write_lifecycle", _fake_wrong)
    session = _repark_session(tmp_path, ZONE_UTC)
    try:
        with pytest.raises(AssertionError, match="regression") as excinfo:
            test_partition_value_row(_SYNTHETIC_SPLIT, session)
        assert "Re-derive" in str(excinfo.value)
    finally:
        with contextlib.suppress(Exception):
            session.stop()


def test_content_disclosure_classifier_converged_arm(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """CP-1: content disclosure landing ON the recorded Spark data → CONVERGED."""
    import test_partition_value_audit as audit_mod

    spark_data = _SYNTHETIC_DISCLOSURE.spark_data
    spark_meta = _SYNTHETIC_DISCLOSURE.spark_meta
    assert spark_data is not None and spark_meta is not None

    def _fake_spark(
        _session: Any, _row: PartitionValueRow, *, catalog: str, namespace: str
    ) -> WriteAudit:
        _ = catalog, namespace
        return WriteAudit(data=spark_data, meta=spark_meta, write_error=None, meta_error=None)

    monkeypatch.setattr(audit_mod, "run_write_lifecycle", _fake_spark)
    session = _repark_session(tmp_path, ZONE_NEW_YORK)
    try:
        with pytest.raises(AssertionError, match="CONVERGED") as excinfo:
            test_partition_value_row(_SYNTHETIC_DISCLOSURE, session)
        assert "flip the data half to equality" in str(excinfo.value)
        assert "Do not delete" in str(excinfo.value)
    finally:
        with contextlib.suppress(Exception):
            session.stop()


def test_content_disclosure_classifier_regression_arm(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """CP-1: content disclosure landing on NEITHER half → regression guidance."""
    import test_partition_value_audit as audit_mod

    wrong = _table(
        [("id", _I64, True), ("d", _DATE, True)], {"id": [1], "d": [dt.date(1999, 1, 1)]}
    )
    spark_meta = _SYNTHETIC_DISCLOSURE.spark_meta
    assert spark_meta is not None

    def _fake_wrong(
        _session: Any, _row: PartitionValueRow, *, catalog: str, namespace: str
    ) -> WriteAudit:
        _ = catalog, namespace
        return WriteAudit(data=wrong, meta=spark_meta, write_error=None, meta_error=None)

    monkeypatch.setattr(audit_mod, "run_write_lifecycle", _fake_wrong)
    session = _repark_session(tmp_path, ZONE_NEW_YORK)
    try:
        with pytest.raises(AssertionError, match="regression") as excinfo:
            test_partition_value_row(_SYNTHETIC_DISCLOSURE, session)
        assert "moved OFF its pinned data disclosure" in str(excinfo.value)
        assert "Re-derive" in str(excinfo.value)
    finally:
        with contextlib.suppress(Exception):
            session.stop()
