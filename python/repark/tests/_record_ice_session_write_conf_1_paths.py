"""Record the ICE-SESSION-WRITE-CONF-1 path cells (QS / QZ / QR) on live PySpark.

The round-1 additions to the unit's oracle: branch writes, metadata-only
deletes, MoR UPDATE / MERGE, position-delete codecs and the summary-key
collision family. :mod:`_record_ice_session_write_conf_1_oracle` imports
:func:`derive_path_cells` and appends its records to the SP / CZ cells, so one
``record`` / ``check`` run re-derives the whole fixture.

Every cell mirrors the orchestrator recording ``spark-qc1.json`` cell for cell:
the same statements, the same observations (``summaries`` rows of operation
plus the observed summary keys, ``refs``, ``data``, ``branch-data``,
``delete-codecs``, ``all-codecs``).

pins: ice-session-write-conf-1/C-036, C-037
"""

from __future__ import annotations

import re
from contextlib import suppress
from typing import Any

SUMMARY_KEYS = (
    "team",
    "added-records",
    "deleted-records",
    "added-delete-files",
    "added-position-deletes",
    "total-records",
)
PROPERTY_PREFIX = "spark.sql.iceberg.snapshot-property."
CODEC_KEY = "spark.sql.iceberg.compression-codec"
MOR_PROPS = (
    ", 'write.delete.mode'='merge-on-read'"
    ", 'write.update.mode'='merge-on-read'"
    ", 'write.merge.mode'='merge-on-read'"
)
SEED_INSERT = "INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z')"
MERGE_ONE = (
    "MERGE INTO {table} t USING (SELECT 1 AS id, 'm' AS data, 'x' AS cat) s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
)
CODEC_MERGE_ONE = (
    "MERGE INTO {table} t USING (SELECT 1 AS id, 'm' AS s) src ON t.id = src.id "
    "WHEN MATCHED THEN UPDATE SET *"
)


def _table_name(cell_id: str, catalog: str) -> str:
    """Derive the harness table name for one cell."""
    return f"{catalog}.ns.t_{re.sub(r'[^a-z0-9]', '_', cell_id.lower())}"


def snapshot_rows(spark: Any, table: str) -> list[list[Any]]:
    """Read operation plus the observed summary keys per snapshot, oldest-first."""
    rows = spark.sql(
        f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at, snapshot_id"
    ).collect()
    return [
        [row["operation"]] + [dict(row["summary"]).get(key) for key in SUMMARY_KEYS] for row in rows
    ]


def ref_rows(spark: Any, table: str) -> list[list[Any]]:
    """Read each ref with the ordinal of the snapshot it points at."""
    ordered = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at, snapshot_id"
    ).collect()
    ordinal = {row["snapshot_id"]: index for index, row in enumerate(ordered)}
    refs = spark.sql(f"SELECT name, snapshot_id FROM {table}.refs").collect()
    return sorted([row["name"], ordinal.get(row["snapshot_id"])] for row in refs)


def data_rows(spark: Any, query: str) -> list[list[Any]]:
    """Read one query's rows in repr order."""
    return sorted((list(row) for row in spark.sql(query).collect()), key=repr)


def codec_rows(spark: Any, query: str) -> list[list[str]]:
    """Read the footer codec set of every file the query names, sorted."""
    import pyarrow.parquet as parquet

    out: list[list[str]] = []
    for row in spark.sql(query).collect():
        path = str(row[0])
        if not path.endswith(".parquet"):
            out.append(["non-parquet"])
            continue
        handle = parquet.ParquetFile(path.replace("file:", ""))
        out.append(
            sorted(
                {
                    handle.metadata.row_group(index).column(column).compression
                    for index in range(handle.metadata.num_row_groups)
                    for column in range(handle.metadata.num_columns)
                }
            )
        )
    return sorted(out)


def _error_info(error: BaseException) -> dict[str, str]:
    """Reduce a Spark failure to its stable type plus trimmed message."""
    message = re.sub(r"\n\s*(JVM stacktrace|at |\tat ).*", "", str(error).strip(), flags=re.S)
    return {"type": type(error).__name__, "msg": message[:700]}


def _apply_conf(spark: Any, conf: dict[str, str]) -> list[str]:
    """Set one cell's session confs and return the keys applied."""
    applied: list[str] = []
    for key, value in conf.items():
        spark.conf.set(key, value)
        applied.append(key)
    return applied


def _release_conf(spark: Any, applied: list[str], table: str) -> None:
    """Unset one cell's session confs and drop its table."""
    for key in applied:
        with suppress(Exception):
            spark.conf.unset(key)
    with suppress(Exception):
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def _seed_property_table(spark: Any, table: str, extra: str, version: str, part: str) -> None:
    """Create and seed the three-row snapshot-property table."""
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg {part} "
        f"TBLPROPERTIES ('format-version'='{version}'{extra})"
    )
    spark.sql(SEED_INSERT.format(table=table))


def _run_property_cell(spark: Any, case: tuple[Any, ...]) -> dict[str, Any]:
    """Run one QS snapshot-property cell and record its Spark answer."""
    cell_id, statements, extra, version, part, branch = case
    table = _table_name(cell_id, "sc")
    record: dict[str, Any] = {"id": cell_id}
    applied: list[str] = []
    try:
        _seed_property_table(spark, table, extra, version, part)
        if branch:
            spark.sql(f"ALTER TABLE {table} CREATE BRANCH b")
        applied = _apply_conf(spark, cell_conf(cell_id))
        for statement in statements:
            spark.sql(statement.format(table=table))
        observed = {
            "summaries": snapshot_rows(spark, table),
            "refs": ref_rows(spark, table),
            "data": data_rows(spark, f"SELECT * FROM {table}"),
        }
        if branch:
            observed["branch-data"] = data_rows(spark, f"SELECT * FROM {table} VERSION AS OF 'b'")
        record["status"] = "ok"
        record["obs"] = observed
    except Exception as error:
        record["status"] = "error"
        record["error"] = _error_info(error)
        record["obs"] = {}
    finally:
        _release_conf(spark, applied, table)
    return record


def _run_branch_df_append_cell(spark: Any) -> dict[str, Any]:
    """Run the branch DataFrame-append cell, the one path with no SQL spelling."""
    cell_id = "QS-BRANCH-DF-APPEND"
    table = _table_name(cell_id, "sc")
    record: dict[str, Any] = {"id": cell_id}
    applied: list[str] = []
    try:
        _seed_property_table(spark, table, "", "2", "")
        spark.sql(f"ALTER TABLE {table} CREATE BRANCH b")
        applied = _apply_conf(spark, cell_conf(cell_id))
        spark.createDataFrame([(4, "d", "x")], "id BIGINT, data STRING, cat STRING").writeTo(
            f"{table}.branch_b"
        ).append()
        record["status"] = "ok"
        record["obs"] = {
            "summaries": snapshot_rows(spark, table),
            "refs": ref_rows(spark, table),
            "data": data_rows(spark, f"SELECT * FROM {table}"),
            "branch-data": data_rows(spark, f"SELECT * FROM {table} VERSION AS OF 'b'"),
        }
    except Exception as error:
        record["status"] = "error"
        record["error"] = _error_info(error)
        record["obs"] = {}
    finally:
        _release_conf(spark, applied, table)
    return record


def _run_codec_cell(spark: Any, case: tuple[Any, ...]) -> dict[str, Any]:
    """Run one QZ codec cell and record its Spark answer."""
    cell_id, statements, extra, version, branch, delete_query = case
    table = _table_name(cell_id, "hc")
    record: dict[str, Any] = {"id": cell_id}
    applied: list[str] = []
    try:
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT, s STRING) USING iceberg "
            f"TBLPROPERTIES ('format-version'='{version}'{extra})"
        )
        spark.sql(f"INSERT INTO {table} SELECT id, concat('v', id) FROM range(50)")
        if branch:
            spark.sql(f"ALTER TABLE {table} CREATE BRANCH b")
        applied = _apply_conf(spark, cell_conf(cell_id))
        for statement in statements:
            spark.sql(statement.format(table=table))
        record["status"] = "ok"
        record["obs"] = {
            "delete-codecs": codec_rows(spark, delete_query.format(table=table)),
            "all-codecs": codec_rows(spark, f"SELECT file_path FROM {table}.all_files"),
            "summaries": snapshot_rows(spark, table),
        }
    except Exception as error:
        record["status"] = "error"
        record["error"] = _error_info(error)
        record["obs"] = {}
    finally:
        _release_conf(spark, applied, table)
    return record


def _run_reserved_cell(spark: Any, case: tuple[Any, ...]) -> dict[str, Any]:
    """Run one QR reserved-key cell and record its Spark answer."""
    cell_id, _key, _value, statements, extra = case
    table = _table_name(cell_id, "sc")
    record: dict[str, Any] = {"id": cell_id}
    applied: list[str] = []
    try:
        _seed_property_table(spark, table, extra, "2", "")
        applied = _apply_conf(spark, cell_conf(cell_id))
        for statement in statements:
            spark.sql(statement.format(table=table))
        record["status"] = "ok"
        record["obs"] = {
            "summaries": snapshot_rows(spark, table),
            "data": data_rows(spark, f"SELECT * FROM {table}"),
        }
    except Exception as error:
        record["status"] = "error"
        record["error"] = _error_info(error)
        record["obs"] = {}
    finally:
        _release_conf(spark, applied, table)
    return record


PROPERTY_CELLS: tuple[tuple[str, tuple[str, ...], str, str, str, bool], ...] = (
    ("QS-TRUNCATE", ("TRUNCATE TABLE {table}",), "", "2", "", False),
    ("QS-TRUNCATE-V3", ("TRUNCATE TABLE {table}",), "", "3", "", False),
    ("QS-DELETE-ALL-META", ("DELETE FROM {table} WHERE true",), "", "2", "", False),
    (
        "QS-DELETE-PART-META",
        ("DELETE FROM {table} WHERE cat = 'x'",),
        "",
        "2",
        "PARTITIONED BY (cat)",
        False,
    ),
    (
        "QS-BRANCH-INSERT",
        ("INSERT INTO {table}.branch_b VALUES (4, 'd', 'x')",),
        "",
        "2",
        "",
        True,
    ),
    ("QS-BRANCH-DELETE-COW", ("DELETE FROM {table}.branch_b WHERE id = 1",), "", "2", "", True),
    (
        "QS-BRANCH-DELETE-MOR",
        ("DELETE FROM {table}.branch_b WHERE id = 1",),
        MOR_PROPS,
        "2",
        "",
        True,
    ),
    (
        "QS-BRANCH-UPDATE-COW",
        ("UPDATE {table}.branch_b SET data = 'z' WHERE id = 1",),
        "",
        "2",
        "",
        True,
    ),
    (
        "QS-BRANCH-UPDATE-MOR",
        ("UPDATE {table}.branch_b SET data = 'z' WHERE id = 1",),
        MOR_PROPS,
        "2",
        "",
        True,
    ),
    (
        "QS-BRANCH-MERGE",
        (MERGE_ONE.replace("{table}", "{table}.branch_b"),),
        "",
        "2",
        "",
        True,
    ),
    ("QS-UPDATE-MOR", ("UPDATE {table} SET data = 'z' WHERE id = 1",), MOR_PROPS, "2", "", False),
    ("QS-MERGE-MOR", (MERGE_ONE,), MOR_PROPS, "2", "", False),
    (
        "QS-UPDATE-MOR-V3",
        ("UPDATE {table} SET data = 'z' WHERE id = 1",),
        MOR_PROPS,
        "3",
        "",
        False,
    ),
)

ALL_DELETE_FILES = "SELECT file_path FROM {table}.all_delete_files"
ALL_DATA_FILES = "SELECT file_path FROM {table}.all_data_files"

CODEC_CELLS: tuple[tuple[str, tuple[str, ...], str, str, bool, str], ...] = (
    (
        "QZ-POSDEL-DELETE",
        ("DELETE FROM {table} WHERE id < 5",),
        MOR_PROPS,
        "2",
        False,
        ALL_DELETE_FILES,
    ),
    (
        "QZ-POSDEL-UPDATE",
        ("UPDATE {table} SET s = 'u' WHERE id < 5",),
        MOR_PROPS,
        "2",
        False,
        ALL_DELETE_FILES,
    ),
    ("QZ-POSDEL-MERGE", (CODEC_MERGE_ONE,), MOR_PROPS, "2", False, ALL_DELETE_FILES),
    (
        "QZ-POSDEL-PROP-DELETE-CODEC",
        ("DELETE FROM {table} WHERE id < 5",),
        MOR_PROPS + ", 'write.delete.parquet.compression-codec'='snappy'",
        "2",
        False,
        ALL_DELETE_FILES,
    ),
    (
        "QZ-POSDEL-PROP-DATA-CODEC",
        ("DELETE FROM {table} WHERE id < 5",),
        MOR_PROPS + ", 'write.parquet.compression-codec'='snappy'",
        "2",
        False,
        ALL_DELETE_FILES,
    ),
    (
        "QZ-POSDEL-V3-DV",
        ("DELETE FROM {table} WHERE id < 5",),
        MOR_PROPS,
        "3",
        False,
        ALL_DELETE_FILES,
    ),
    (
        "QZ-BRANCH-INSERT",
        ("INSERT INTO {table}.branch_b SELECT id, 'b' FROM range(5)",),
        "",
        "2",
        True,
        ALL_DATA_FILES,
    ),
    (
        "QZ-BRANCH-DELETE-MOR",
        ("DELETE FROM {table}.branch_b WHERE id < 5",),
        MOR_PROPS,
        "2",
        True,
        ALL_DELETE_FILES,
    ),
    ("QZ-TRUNCATE-NOFILES", ("TRUNCATE TABLE {table}",), "", "2", False, ALL_DATA_FILES),
)

RESERVED_CELLS: tuple[tuple[str, str, str, tuple[str, ...], str], ...] = (
    ("QR-INSERT-DELETED-RECORDS", "deleted-records", "5", (SEED_INSERT,), ""),
    ("QR-INSERT-ADDED-DELETE-FILES", "added-delete-files", "7", (SEED_INSERT,), ""),
    (
        "QR-DELETE-COW-DELETED-RECORDS",
        "deleted-records",
        "5",
        ("DELETE FROM {table} WHERE id = 1",),
        "",
    ),
    (
        "QR-DELETE-COW-ADDED-RECORDS",
        "added-records",
        "999",
        ("DELETE FROM {table} WHERE id = 1",),
        "",
    ),
    (
        "QR-DELETE-MOR-ADDED-RECORDS",
        "added-records",
        "999",
        ("DELETE FROM {table} WHERE id = 1",),
        MOR_PROPS,
    ),
    (
        "QR-DELETE-MOR-ADDED-DELETE-FILES",
        "added-delete-files",
        "7",
        ("DELETE FROM {table} WHERE id = 1",),
        MOR_PROPS,
    ),
    (
        "QR-DELETE-META-DELETED-RECORDS",
        "deleted-records",
        "5",
        ("DELETE FROM {table} WHERE true",),
        "",
    ),
    (
        "QR-DELETE-META-ADDED-RECORDS",
        "added-records",
        "999",
        ("DELETE FROM {table} WHERE true",),
        "",
    ),
    ("QR-MERGE-ADDED-RECORDS", "added-records", "999", (MERGE_ONE,), ""),
    ("QR-MERGE-MOR-ADDED-DELETE-FILES", "added-delete-files", "7", (MERGE_ONE,), MOR_PROPS),
    (
        "QR-UPDATE-COW-CHANGED-PARTITION",
        "changed-partition-count",
        "9",
        ("UPDATE {table} SET data = 'z' WHERE id = 1",),
        "",
    ),
    (
        "QR-OVERWRITE-DELETED-RECORDS",
        "deleted-records",
        "5",
        ("INSERT OVERWRITE {table} VALUES (9, 'z', 'x')",),
        "",
    ),
    ("QR-TRUNCATE-DELETED-RECORDS", "deleted-records", "5", ("TRUNCATE TABLE {table}",), ""),
    ("QR-ENGINE-NAME", "engine-name", "fake", (SEED_INSERT,), ""),
    ("QR-SPARK-APP-ID", "spark.app.id", "fake", (SEED_INSERT,), ""),
)


def cell_conf(cell_id: str) -> dict[str, str]:
    """Return the session confs one path cell sets."""
    for reserved_id, key, value, _statements, _extra in RESERVED_CELLS:
        if reserved_id == cell_id:
            return {PROPERTY_PREFIX + key: value, PROPERTY_PREFIX + "team": "a"}
    if cell_id.startswith("QZ-"):
        return {CODEC_KEY: "gzip"}
    return {PROPERTY_PREFIX + "team": "a"}


def derive_path_cells(spark: Any) -> list[dict[str, Any]]:
    """Derive every QS / QZ / QR path cell on the live session."""
    cells = [_run_property_cell(spark, case) for case in PROPERTY_CELLS]
    cells.append(_run_branch_df_append_cell(spark))
    cells.extend(_run_codec_cell(spark, case) for case in CODEC_CELLS)
    cells.extend(_run_reserved_cell(spark, case) for case in RESERVED_CELLS)
    return cells
