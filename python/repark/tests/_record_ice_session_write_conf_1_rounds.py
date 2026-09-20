"""Record the ICE-SESSION-WRITE-CONF-1 round-3 and round-4 Spark cells.

The verification critics' later rounds measured six families the round-1
fixture did not hold:

``QK-*``
    the session conf KEY's case — the prefix is case-sensitive (a title-cased
    or upper-cased ``spark.sql.iceberg.`` prefix is silently ignored), the
    snapshot-property SUFFIX is carried verbatim.
``QM-*``
    ``CALL … rewrite_manifests`` / ``rewrite_data_files`` under a session
    property: the replace snapshot carries neither the property nor a refusal.
``QP-*``
    dynamic ``overwritePartitions`` — a removal-fed key is free when the
    overwrite lands only in partitions the table did not have, and refuses
    naming the engine's computed value when it replaces a live one.
``QO-*``
    static ``INSERT OVERWRITE … PARTITION (k = v)`` — the same rule, resolved
    from the files the row filter actually removes.
``QC-*``
    a summary-metric name in a DIFFERENT case is a different key: Spark stamps
    ``Deleted-Records`` beside its own ``deleted-records``.
``QU-*``
    the copy-on-write layout itself — one live data file and one added data
    file for the identity UPDATE and the DELETE, with a session conf and
    without.

:mod:`_record_ice_session_write_conf_1_oracle` appends these to the SP / CZ /
QS / QZ / QR cells, so one ``record`` / ``check`` run re-derives the whole
fixture.

pins: ice-session-write-conf-1/C-047, C-048, C-049, C-054, C-056
"""

from __future__ import annotations

from typing import Any

from _record_ice_session_write_conf_1_paths import (
    _apply_conf,
    _error_info,
    _release_conf,
    _table_name,
    codec_rows,
    data_rows,
)

PREFIX = "spark.sql.iceberg.snapshot-property."
CODEC_KEY = "spark.sql.iceberg.compression-codec"
KEY_CASE_KEYS = ("team", "TEAM", "Team")
CALL_KEYS = ("team", "total-records")
PARTITION_KEYS = (
    "team",
    "added-records",
    "deleted-records",
    "added-data-files",
    "deleted-data-files",
    "changed-partition-count",
    "total-records",
    "total-data-files",
)
SUFFIX_KEYS = ("Deleted-Records", "DELETED-RECORDS", "deleted-records", "total-records")
LAYOUT_KEYS = (
    "team",
    "added-data-files",
    "deleted-data-files",
    "added-records",
    "deleted-records",
    "total-data-files",
    "total-records",
)
PARTITIONED_SEED = "INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'x'), (3, 'c', 'y')"
COW_PROPS = ", 'write.update.mode'='copy-on-write', 'write.delete.mode'='copy-on-write'"


def summary_rows(spark: Any, table: str, keys: tuple[str, ...]) -> list[list[Any]]:
    """Read operation plus the named summary keys per snapshot, oldest-first."""
    rows = spark.sql(
        f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at, snapshot_id"
    ).collect()
    return [[row["operation"]] + [dict(row["summary"]).get(key) for key in keys] for row in rows]


def _cell(
    spark: Any, cell_id: str, table: str, conf: dict[str, str], run: Any, observe: Any
) -> dict[str, Any]:
    """Run one cell's conf-scoped statements and record its Spark answer."""
    record: dict[str, Any] = {"id": cell_id}
    applied: list[str] = []
    try:
        applied = _apply_conf(spark, conf)
        run(spark, table)
        record["status"] = "ok"
        record["obs"] = observe(spark, table)
    except Exception as error:
        record["status"] = "error"
        record["error"] = _error_info(error)
        record["obs"] = {}
    finally:
        _release_conf(spark, applied, table)
    return record


KEY_CASE_CELLS: tuple[tuple[str, str, str], ...] = (
    ("QK-EXACT", PREFIX + "team", "a"),
    ("QK-PREFIX-TITLE", "Spark.sql.iceberg.snapshot-property.team", "a"),
    ("QK-PREFIX-UPPER", "SPARK.SQL.ICEBERG.SNAPSHOT-PROPERTY.team", "a"),
    ("QK-SUFFIX-UPPER", PREFIX + "TEAM", "a"),
    ("QK-CODEC-TITLE", "Spark.SQL.Iceberg.Compression-Codec", "gzip"),
    ("QK-CODEC-EXACT", CODEC_KEY, "gzip"),
)

CALL_CELLS: tuple[tuple[str, str, str, str], ...] = (
    ("QM-REWRITE-MANIFESTS", PREFIX + "team", "a", "rewrite_manifests('{short}')"),
    (
        "QM-REWRITE-MANIFESTS-COLLIDE",
        PREFIX + "total-records",
        "77",
        "rewrite_manifests('{short}')",
    ),
    (
        "QM-REWRITE-DATA-FILES",
        PREFIX + "team",
        "a",
        "rewrite_data_files(table => '{short}', options => map('min-input-files','2'))",
    ),
)

REPLACE_PARTITION_CELLS: tuple[
    tuple[str, dict[str, str], tuple[tuple[int, str, str], ...]], ...
] = (
    ("QP-NEW-PARTITION-FREE", {PREFIX + "team": "a"}, ((9, "z", "w"),)),
    (
        "QP-NEW-PARTITION-DELETED-RECORDS",
        {PREFIX + "deleted-records": "5", PREFIX + "team": "a"},
        ((9, "z", "w"),),
    ),
    ("QP-EXISTING-PARTITION-FREE", {PREFIX + "team": "a"}, ((9, "z", "x"),)),
    (
        "QP-EXISTING-PARTITION-DELETED-RECORDS",
        {PREFIX + "deleted-records": "5", PREFIX + "team": "a"},
        ((9, "z", "x"),),
    ),
    (
        "QP-NOOP-EMPTY-DELETED-RECORDS",
        {PREFIX + "deleted-records": "5", PREFIX + "team": "a"},
        (),
    ),
    ("QP-NOOP-EMPTY-FREE", {PREFIX + "team": "a"}, ()),
)

STATIC_OVERWRITE_CELLS: tuple[tuple[str, dict[str, str], str], ...] = (
    ("QO-EXISTING-FREE", {PREFIX + "team": "a"}, "x"),
    (
        "QO-EXISTING-DELETED-RECORDS",
        {PREFIX + "deleted-records": "5", PREFIX + "team": "a"},
        "x",
    ),
    ("QO-EXISTING-TOTAL-RECORDS", {PREFIX + "total-records": "77", PREFIX + "team": "a"}, "x"),
    (
        "QO-EXISTING-DELETED-DATA-FILES",
        {PREFIX + "deleted-data-files": "9", PREFIX + "team": "a"},
        "x",
    ),
    ("QO-NEW-FREE", {PREFIX + "team": "a"}, "w"),
    ("QO-NEW-DELETED-RECORDS", {PREFIX + "deleted-records": "5", PREFIX + "team": "a"}, "w"),
    ("QO-NEW-TOTAL-RECORDS", {PREFIX + "total-records": "77", PREFIX + "team": "a"}, "w"),
    (
        "QO-NEW-DELETED-DATA-FILES",
        {PREFIX + "deleted-data-files": "9", PREFIX + "team": "a"},
        "w",
    ),
)

SUFFIX_CASE_CELLS: tuple[tuple[str, str], ...] = (
    ("QC-SUFFIX-TITLE-CASE", "Deleted-Records"),
    ("QC-SUFFIX-UPPER-CASE", "DELETED-RECORDS"),
    ("QC-SUFFIX-EXACT", "deleted-records"),
)

LAYOUT_CELLS: tuple[tuple[str, dict[str, str], str], ...] = (
    ("QU-UPDATE-COW-PLAIN", {}, "UPDATE {table} SET data = 'z' WHERE id = 1"),
    ("QU-UPDATE-COW-CONF", {PREFIX + "team": "a"}, "UPDATE {table} SET data = 'z' WHERE id = 1"),
    ("QU-DELETE-COW-PLAIN", {}, "DELETE FROM {table} WHERE id = 1"),
    ("QU-DELETE-COW-CONF", {PREFIX + "team": "a"}, "DELETE FROM {table} WHERE id = 1"),
    ("QU-INSERT-PLAIN", {}, "INSERT INTO {table} VALUES (9, 'i')"),
    ("QU-INSERT-CONF", {PREFIX + "team": "a"}, "INSERT INTO {table} VALUES (9, 'i')"),
)


def _run_key_case_cell(spark: Any, case: tuple[str, str, str]) -> dict[str, Any]:
    """Run one QK conf-key-case cell."""
    cell_id, key, value = case
    table = _table_name(cell_id, "hc")
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, s STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )

    def run(session: Any, name: str) -> None:
        session.sql(f"INSERT INTO {name} VALUES (1, 'a')")

    def observe(session: Any, name: str) -> dict[str, Any]:
        return {
            "summaries": summary_rows(session, name, KEY_CASE_KEYS),
            "codecs": codec_rows(session, f"SELECT file_path FROM {name}.all_data_files"),
        }

    return _cell(spark, cell_id, table, {key: value}, run, observe)


def _run_call_cell(spark: Any, case: tuple[str, str, str, str]) -> dict[str, Any]:
    """Run one QM maintenance-CALL cell."""
    cell_id, key, value, call = case
    table = _table_name(cell_id, "sc")
    short = table.split(".", 1)[1]
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, s STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a')")
    spark.sql(f"INSERT INTO {table} VALUES (2, 'b')")

    def run(session: Any, name: str) -> None:
        session.sql(f"CALL {name.split('.', 1)[0]}.system.{call.format(short=short)}")

    def observe(session: Any, name: str) -> dict[str, Any]:
        return {
            "summaries": summary_rows(session, name, CALL_KEYS),
            "data": data_rows(session, f"SELECT * FROM {name}"),
        }

    return _cell(spark, cell_id, table, {key: value}, run, observe)


def _seed_partitioned(spark: Any, table: str) -> None:
    """Create and seed the three-row ``cat``-partitioned table."""
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(PARTITIONED_SEED.format(table=table))


def _partition_observe(spark: Any, table: str) -> dict[str, Any]:
    """Observe a partition-overwrite cell."""
    return {
        "summaries": summary_rows(spark, table, PARTITION_KEYS),
        "data": data_rows(spark, f"SELECT * FROM {table}"),
    }


def _run_replace_partition_cell(
    spark: Any, case: tuple[str, dict[str, str], tuple[tuple[int, str, str], ...]]
) -> dict[str, Any]:
    """Run one QP dynamic ``overwritePartitions`` cell."""
    cell_id, conf, rows = case
    table = _table_name(cell_id, "sc")
    _seed_partitioned(spark, table)

    def run(session: Any, name: str) -> None:
        frame = session.createDataFrame(list(rows), "id BIGINT, data STRING, cat STRING")
        frame.writeTo(name).overwritePartitions()

    return _cell(spark, cell_id, table, conf, run, _partition_observe)


def _run_static_overwrite_cell(spark: Any, case: tuple[str, dict[str, str], str]) -> dict[str, Any]:
    """Run one QO static ``INSERT OVERWRITE … PARTITION`` cell."""
    cell_id, conf, partition = case
    table = _table_name(cell_id, "sc")
    _seed_partitioned(spark, table)

    def run(session: Any, name: str) -> None:
        session.sql(f"INSERT OVERWRITE {name} PARTITION (cat = '{partition}') VALUES (9, 'z')")

    return _cell(spark, cell_id, table, conf, run, _partition_observe)


def _run_suffix_case_cell(spark: Any, case: tuple[str, str]) -> dict[str, Any]:
    """Run one QC mixed-case summary-metric cell."""
    cell_id, suffix = case
    table = _table_name(cell_id, "sc")
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='copy-on-write')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    def run(session: Any, name: str) -> None:
        session.sql(f"DELETE FROM {name} WHERE id = 1")

    def observe(session: Any, name: str) -> dict[str, Any]:
        return {
            "summaries": summary_rows(session, name, SUFFIX_KEYS),
            "data": data_rows(session, f"SELECT * FROM {name}"),
        }

    return _cell(spark, cell_id, table, {PREFIX + suffix: "5"}, run, observe)


def _run_layout_cell(spark: Any, case: tuple[str, dict[str, str], str]) -> dict[str, Any]:
    """Run one QU copy-on-write layout cell."""
    cell_id, conf, statement = case
    table = _table_name(cell_id, "sc")
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='2'{COW_PROPS})"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    def run(session: Any, name: str) -> None:
        session.sql(statement.format(table=name))

    def observe(session: Any, name: str) -> dict[str, Any]:
        return {
            "summaries": summary_rows(session, name, LAYOUT_KEYS),
            "live-files": len(session.sql(f"SELECT file_path FROM {name}.files").collect()),
            "data": data_rows(session, f"SELECT * FROM {name}"),
        }

    return _cell(spark, cell_id, table, conf, run, observe)


def derive_round_cells(spark: Any) -> list[dict[str, Any]]:
    """Derive every QK / QM / QP / QO / QC / QU cell on the live session."""
    cells = [_run_key_case_cell(spark, case) for case in KEY_CASE_CELLS]
    cells.extend(_run_call_cell(spark, case) for case in CALL_CELLS)
    cells.extend(_run_replace_partition_cell(spark, case) for case in REPLACE_PARTITION_CELLS)
    cells.extend(_run_static_overwrite_cell(spark, case) for case in STATIC_OVERWRITE_CELLS)
    cells.extend(_run_suffix_case_cell(spark, case) for case in SUFFIX_CASE_CELLS)
    cells.extend(_run_layout_cell(spark, case) for case in LAYOUT_CELLS)
    return cells
