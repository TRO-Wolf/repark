"""Format-version-3 leg body for the real-AWS acceptance harness (LIVE-v3).

Non-``test_`` module so pytest does not collect it. Sibling of ``_acceptance``: the shared
publish-job constants and the merge-on-read v2 flow stay there; this module owns the v3 CTAS,
deletion-vector DML, maintenance and adopt sequence that the Glue and S3 Tables v3 legs run.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from functools import partial
from typing import NamedTuple

from _acceptance import (
    COMMIT_CONFLICT_RETRY_ATTEMPTS,
    EXPIRE_OLDER_THAN_FUTURE_MS,
    POSITION_DELETE_CONTENT,
    TARGET_FILE_SIZE_BYTES,
    _sql,
    _sql_arrow,
    drop_temp_view,
    fq_table,
    maintenance_call_sql,
    mask_account_ids,
    merge_sql,
    retry_on_commit_conflict,
    snapshot_ids_oldest_first,
)

V3_ALLOW_CREATE_KEY = "repark.sql.allowCreateFormatVersion3"
V3_ICEBERG_TABLE_PROPERTIES = (
    "'format-version' = '3', "
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read', "
    f"'write.target-file-size-bytes' = '{TARGET_FILE_SIZE_BYTES}'"
)
V3_PARTITION_COLUMN = "part"
V3_CTAS_ROW_COUNT = 1
V3_SEED_ROW_COUNT = 10
V3_FILES_PER_PARTITION = 5
V3_DELETED_ID = 3
V3_MERGE_UPDATED_ID = 2
V3_MERGE_INSERTED_ID = 11
V3_TEMP_VIEW = "v3_staging_view"
V3_MERGE_VIEW = "v3_merge_view"
V3_ADOPTED_SUFFIX = "_adopted"
DELETION_VECTOR_CONTENT = POSITION_DELETE_CONTENT
DELETION_VECTOR_FILE_FORMAT = "PUFFIN"
V3_EXPECTED_DELETE_FILES_AFTER_DELETE = 1
V3_EXPECTED_DELETE_FILES_AFTER_MERGE = 2
V3_EXPECTED_DELETE_FILES_AFTER_REWRITE = 0
V3_EXPECTED_REWRITTEN_DATA_FILES = 12
V3_EXPECTED_ADDED_DATA_FILES = 2
V3_EXPECTED_REMOVED_DELETE_FILES = 2
V3_EXPECTED_SNAPSHOTS_BEFORE_EXPIRE = 14
V3_EXPECTED_SNAPSHOTS_AFTER_EXPIRE = 1
V3_EXPECTED_PRE_MERGE_LINEAGE: tuple[tuple[int, int, int], ...] = (
    (1, 0, 1),
    (2, 1, 2),
    (4, 3, 4),
    (5, 4, 5),
    (6, 5, 6),
    (7, 6, 7),
    (8, 7, 8),
    (9, 8, 9),
    (10, 9, 10),
)
V3_EXPECTED_SURVIVOR_LINEAGE: tuple[tuple[int, int, int], ...] = (
    (1, 0, 1),
    (2, 1, 12),
    (4, 3, 4),
    (5, 4, 5),
    (6, 5, 6),
    (7, 6, 7),
    (8, 7, 8),
    (9, 8, 9),
    (10, 9, 10),
)
V3_EXPECTED_MERGE_SEQUENCE = 12
V3_EXPECTED_INSERTED_ROW_ID = 11
S3T_V3_ROW = "S3T-V3-1"
S3T_V3_SUPPORTED = "supported"
S3T_V3_REFUSED_AT_CREATE = "refused-at-create"
S3T_V3_UNCLASSIFIED = "unclassified"
_V3_VERSION_TOKENS = ("format-version", "format version", "formatversion")
_V3_REFUSAL_TOKENS = (
    "not supported",
    "unsupported",
    "not allowed",
    "invalid",
    "must be",
    "only supports",
    "cannot",
)


class V3AcceptanceOutcome(NamedTuple):
    """Every count the v3 CTAS / DV-DML / maintenance / adopt sequence observed."""

    table: str
    data_files_per_partition: list[tuple[int, int]]
    delete_files_after_delete: list[tuple[int, str]]
    lineage_before_merge: list[tuple[int, int | None, int | None]]
    rows_after_merge: list[dict[str, object]]
    lineage_after_merge: list[tuple[int, int | None, int | None]]
    delete_files_after_merge: list[tuple[int, str]]
    rewritten_data_files_count: int
    added_data_files_count: int
    removed_delete_files_count: int
    delete_files_after_rewrite: list[tuple[int, str]]
    rows_after_rewrite: list[dict[str, object]]
    lineage_after_rewrite: list[tuple[int, int | None, int | None]]
    snapshots_before_expire: int
    snapshots_after_expire: int
    adopted_table: str | None
    rows_after_adopt: list[dict[str, object]] | None


def v3_row_values_sql(ids: list[int]) -> str:
    """``(id, 'nID', id % 2)`` value tuples for ``ids``."""
    return ", ".join(f"({index}, 'n{index}', {index % 2})" for index in ids)


def v3_seed_select_sql() -> str:
    """The CTAS source frame: one ``(id, name, part)`` row; the appends build the rest."""
    values = v3_row_values_sql(list(range(1, V3_CTAS_ROW_COUNT + 1)))
    return f"SELECT * FROM (VALUES {values}) AS t(id, name, {V3_PARTITION_COLUMN})"


def v3_insert_batches() -> list[list[int]]:
    """One id per append, so each identity partition reaches ``V3_FILES_PER_PARTITION`` files."""
    return [[index] for index in range(V3_CTAS_ROW_COUNT + 1, V3_SEED_ROW_COUNT + 1)]


def v3_insert_sql(table: str, ids: list[int]) -> str:
    """One append per batch, so each identity partition accumulates its own data files."""
    return f"INSERT INTO {table} VALUES {v3_row_values_sql(ids)}"


def v3_ctas_sql(table: str, source_view: str) -> str:
    """CTAS a merge-on-read v3 table partitioned by identity ``part``. No IF NOT EXISTS."""
    return (
        f"CREATE TABLE {table} USING iceberg PARTITIONED BY ({V3_PARTITION_COLUMN}) "
        f"TBLPROPERTIES ({V3_ICEBERG_TABLE_PROPERTIES}) AS SELECT * FROM {source_view}"
    )


def v3_row_delete_sql(table: str, id_col: str, row_id: int) -> str:
    """The one row-scoped delete in this harness: a single key, never an unfiltered statement."""
    return f"DELETE FROM {table} WHERE {id_col} = {row_id}"


def v3_merge_source_sql(id_col: str) -> str:
    """One matched-update row and one not-matched insert row for the v3 MERGE."""
    return (
        f"SELECT {V3_MERGE_UPDATED_ID} AS {id_col}, 'm{V3_MERGE_UPDATED_ID}' AS name, "
        f"{V3_MERGE_UPDATED_ID % 2} AS {V3_PARTITION_COLUMN} "
        f"UNION ALL SELECT {V3_MERGE_INSERTED_ID}, 'n{V3_MERGE_INSERTED_ID}', "
        f"{V3_MERGE_INSERTED_ID % 2}"
    )


def v3_acceptance_expected_rows() -> list[dict[str, object]]:
    """Post-DELETE, post-MERGE oracle: id 3 gone, id 2 renamed ``m2``, id 11 inserted."""
    ids = [index for index in range(1, V3_SEED_ROW_COUNT + 1) if index != V3_DELETED_ID]
    ids.append(V3_MERGE_INSERTED_ID)
    return [
        {
            "id": index,
            "name": f"m{index}" if index == V3_MERGE_UPDATED_ID else f"n{index}",
            V3_PARTITION_COLUMN: index % 2,
        }
        for index in ids
    ]


def delete_file_rows(spark: object, table: str) -> list[tuple[int, str]]:
    """``(content, file_format)`` for every live delete file on ``table``, sorted."""
    arrow = spark.sql(f"SELECT content, file_format FROM {table}.delete_files").to_arrow()
    contents = arrow.column("content").to_pylist()
    formats = arrow.column("file_format").to_pylist()
    rows = [
        (int(content), str(file_format))
        for content, file_format in zip(contents, formats, strict=True)
    ]
    rows.sort()
    return rows


def v3_lineage_rows(
    spark: object, table: str, id_col: str
) -> list[tuple[int, int | None, int | None]]:
    """``(id, _row_id, _last_updated_sequence_number)`` in id order (single-table read)."""
    arrow = spark.sql(
        f"SELECT {id_col}, _row_id, _last_updated_sequence_number FROM {table} ORDER BY {id_col}"
    ).to_arrow()
    ids = arrow.column(id_col).to_pylist()
    row_ids = arrow.column("_row_id").to_pylist()
    sequences = arrow.column("_last_updated_sequence_number").to_pylist()
    return [
        (int(row), None if row_id is None else int(row_id), None if seq is None else int(seq))
        for row, row_id, seq in zip(ids, row_ids, sequences, strict=True)
    ]


def v3_rows_and_lineage(
    spark: object, table: str, id_col: str
) -> tuple[list[dict[str, object]], list[tuple[int, int | None, int | None]]]:
    """Rows and lineage from ONE ordered scan, so a pair of reads is a single snapshot open."""
    arrow = spark.sql(
        f"SELECT {id_col}, name, {V3_PARTITION_COLUMN}, _row_id, "
        f"_last_updated_sequence_number FROM {table} ORDER BY {id_col}"
    ).to_arrow()
    ids = arrow.column(id_col).to_pylist()
    names = arrow.column("name").to_pylist()
    parts = arrow.column(V3_PARTITION_COLUMN).to_pylist()
    row_ids = arrow.column("_row_id").to_pylist()
    sequences = arrow.column("_last_updated_sequence_number").to_pylist()
    rows = [
        {"id": int(row), "name": name, V3_PARTITION_COLUMN: int(part)}
        for row, name, part in zip(ids, names, parts, strict=True)
    ]
    lineage = [
        (int(row), None if row_id is None else int(row_id), None if seq is None else int(seq))
        for row, row_id, seq in zip(ids, row_ids, sequences, strict=True)
    ]
    return rows, lineage


def v3_data_files_per_partition(spark: object, table: str) -> list[tuple[int, int]]:
    """``(part, data-file count)`` pairs in partition order."""
    arrow = spark.sql(
        f"SELECT partition.{V3_PARTITION_COLUMN} AS part, count(*) AS n "
        f"FROM {table}.data_files GROUP BY partition.{V3_PARTITION_COLUMN} ORDER BY part"
    ).to_arrow()
    parts = arrow.column("part").to_pylist()
    counts = arrow.column("n").to_pylist()
    return [(int(part), int(count)) for part, count in zip(parts, counts, strict=True)]


def v3_ordered_rows(spark: object, table: str, id_col: str) -> list[dict[str, object]]:
    """Live ``(id, name, part)`` rows in id order as Python dicts."""
    arrow = spark.sql(
        f"SELECT {id_col}, name, {V3_PARTITION_COLUMN} FROM {table} ORDER BY {id_col}"
    ).to_arrow()
    return [
        {
            "id": int(row[id_col]),
            "name": row["name"],
            V3_PARTITION_COLUMN: int(row[V3_PARTITION_COLUMN]),
        }
        for row in arrow.to_pylist()
    ]


def current_metadata_location(spark: object, table: str) -> str:
    """The newest ``metadata_log_entries`` file for ``table`` (the register_table argument)."""
    arrow = spark.sql(
        f"SELECT timestamp, file FROM {table}.metadata_log_entries ORDER BY timestamp"
    ).to_arrow()
    files = [str(value) for value in arrow.column("file").to_pylist() if value is not None]
    if not files:
        raise AssertionError(f"no metadata_log_entries rows on {table}")
    return files[-1]


def is_format_version_3_refusal(error: BaseException) -> bool:
    """True when ``error`` reads as a service refusal of ``format-version = 3``."""
    text = str(error)
    if V3_ALLOW_CREATE_KEY in text:
        return False
    lowered = text.lower()
    if not any(token in lowered for token in _V3_VERSION_TOKENS):
        return False
    if "3" not in text:
        return False
    return any(token in lowered for token in _V3_REFUSAL_TOKENS)


def classify_v3_create_outcome(error: BaseException | None) -> str:
    """The ``S3T-V3-1`` decision table: supported, refused at CREATE, or unclassified."""
    if error is None:
        return S3T_V3_SUPPORTED
    if is_format_version_3_refusal(error):
        return S3T_V3_REFUSED_AT_CREATE
    return S3T_V3_UNCLASSIFIED


def format_v3_refusal_record(error: BaseException) -> str:
    """The one-line ``S3T-V3-1`` disposition recorded when the service refuses v3, ids masked."""
    return (
        f"{S3T_V3_ROW} {S3T_V3_REFUSED_AT_CREATE}: the service refused format-version 3 at "
        f"CREATE; message={mask_account_ids(str(error))}"
    )


def _sql_collect(spark: object, statement: str) -> object:
    """Run ``spark.sql(statement).collect()`` — the action that drives a DML statement."""
    return spark.sql(statement).collect()


def _register_table_sql(catalog: str, namespace: str, table_name: str, metadata_file: str) -> str:
    """``CALL catalog.system.register_table(table => 'ns.tbl', metadata_file => '…')``."""
    return (
        f"CALL {catalog}.system.register_table(table => '{namespace}.{table_name}', "
        f"metadata_file => '{metadata_file}')"
    )


def run_v3_acceptance(
    spark: object,
    catalog: str,
    namespace: str,
    table_name: str,
    id_col: str = "id",
    attempts: int = COMMIT_CONFLICT_RETRY_ATTEMPTS,
    adopt_with: Callable[[], object] | None = None,
) -> V3AcceptanceOutcome:
    """CTAS v3 MoR → appends → DV delete → MERGE → lineage → rewrite → expire → adopt.

    Catalog-agnostic: shared by the local pin and the Glue / S3 Tables live legs. Never drops
    a table. ``adopt_with`` returns the second session that registers the same metadata
    location; ``None`` skips the adopt step where the catalog does not support it.
    """
    table = fq_table(catalog, namespace, table_name)
    table_arg = f"{namespace}.{table_name}"

    spark.sql(v3_seed_select_sql()).createOrReplaceTempView(V3_TEMP_VIEW)
    spark.sql(v3_ctas_sql(table, V3_TEMP_VIEW))
    drop_temp_view(spark, V3_TEMP_VIEW)
    for ids in v3_insert_batches():
        retry_on_commit_conflict(
            partial(_sql_collect, spark, v3_insert_sql(table, ids)), attempts=attempts
        )
    data_files_per_partition = v3_data_files_per_partition(spark, table)

    retry_on_commit_conflict(
        partial(_sql_collect, spark, v3_row_delete_sql(table, id_col, V3_DELETED_ID)),
        attempts=attempts,
    )
    delete_files_after_delete = delete_file_rows(spark, table)
    lineage_before_merge = v3_lineage_rows(spark, table, id_col)

    spark.sql(v3_merge_source_sql(id_col)).createOrReplaceTempView(V3_MERGE_VIEW)
    retry_on_commit_conflict(
        partial(_sql, spark, merge_sql(table, V3_MERGE_VIEW, id_col)), attempts=attempts
    )
    drop_temp_view(spark, V3_MERGE_VIEW)
    rows_after_merge, lineage_after_merge = v3_rows_and_lineage(spark, table, id_col)
    delete_files_after_merge = delete_file_rows(spark, table)

    rewrite_data = maintenance_call_sql(catalog, "rewrite_data_files", table_arg)
    rewrite_result, _ = retry_on_commit_conflict(
        partial(_sql_arrow, spark, rewrite_data), attempts=attempts
    )
    rewritten = int(rewrite_result.column("rewritten_data_files_count")[0].as_py())
    added = int(rewrite_result.column("added_data_files_count")[0].as_py())
    removed = int(rewrite_result.column("removed_delete_files_count")[0].as_py())
    delete_files_after_rewrite = delete_file_rows(spark, table)
    rows_after_rewrite, lineage_after_rewrite = v3_rows_and_lineage(spark, table, id_col)

    snapshots_before_expire = len(snapshot_ids_oldest_first(spark, table))
    older_than_ms = int(time.time() * 1000) + EXPIRE_OLDER_THAN_FUTURE_MS
    expire_extra = f"older_than => {older_than_ms}, retain_last => 1"
    expire = maintenance_call_sql(catalog, "expire_snapshots", table_arg, extra=expire_extra)
    retry_on_commit_conflict(partial(_sql_arrow, spark, expire), attempts=attempts)
    snapshots_after_expire = len(snapshot_ids_oldest_first(spark, table))

    adopted_table: str | None = None
    rows_after_adopt: list[dict[str, object]] | None = None
    if adopt_with is not None:
        metadata_file = current_metadata_location(spark, table)
        adopted_name = f"{table_name}{V3_ADOPTED_SUFFIX}"
        second = adopt_with()
        second.sql(_register_table_sql(catalog, namespace, adopted_name, metadata_file)).to_arrow()
        adopted_table = fq_table(catalog, namespace, adopted_name)
        rows_after_adopt = v3_ordered_rows(second, adopted_table, id_col)

    return V3AcceptanceOutcome(
        table=table,
        data_files_per_partition=data_files_per_partition,
        delete_files_after_delete=delete_files_after_delete,
        lineage_before_merge=lineage_before_merge,
        rows_after_merge=rows_after_merge,
        lineage_after_merge=lineage_after_merge,
        delete_files_after_merge=delete_files_after_merge,
        rewritten_data_files_count=rewritten,
        added_data_files_count=added,
        removed_delete_files_count=removed,
        delete_files_after_rewrite=delete_files_after_rewrite,
        rows_after_rewrite=rows_after_rewrite,
        lineage_after_rewrite=lineage_after_rewrite,
        snapshots_before_expire=snapshots_before_expire,
        snapshots_after_expire=snapshots_after_expire,
        adopted_table=adopted_table,
        rows_after_adopt=rows_after_adopt,
    )


def assert_deletion_vectors(rows: list[tuple[int, str]], expected: int, label: str) -> None:
    """Fail unless ``rows`` is exactly ``expected`` Puffin deletion vectors."""
    if len(rows) != expected:
        raise AssertionError(f"{label}: {len(rows)} delete files, expected {expected}: {rows!r}")
    for content, file_format in rows:
        if content != DELETION_VECTOR_CONTENT:
            raise AssertionError(f"{label}: delete-file content {content} != position deletes")
        if file_format != DELETION_VECTOR_FILE_FORMAT:
            raise AssertionError(f"{label}: delete-file format {file_format!r} is not a DV")


def assert_v3_lineage(
    lineage: list[tuple[int, int | None, int | None]],
    label: str,
    exact_commit_counts: bool = True,
) -> None:
    """Survivors keep their stored ``_row_id``; the MERGE insert takes Spark's exact one."""
    survivors = [row for row in lineage if row[0] != V3_MERGE_INSERTED_ID]
    inserted = [row for row in lineage if row[0] == V3_MERGE_INSERTED_ID]
    expected = [tuple(row) for row in V3_EXPECTED_SURVIVOR_LINEAGE]
    if exact_commit_counts:
        if survivors != expected:
            raise AssertionError(f"{label} survivor lineage {survivors!r} != {expected!r}")
    else:
        got_ids = [(row[0], row[1]) for row in survivors]
        want_ids = [(row[0], row[1]) for row in expected]
        if got_ids != want_ids:
            raise AssertionError(f"{label} survivor (id, _row_id) {got_ids!r} != {want_ids!r}")
    if len(inserted) != 1:
        raise AssertionError(f"{label} has {len(inserted)} rows for the MERGE insert: {lineage!r}")
    inserted_id, inserted_row_id, inserted_sequence = inserted[0]
    if inserted_row_id != V3_EXPECTED_INSERTED_ROW_ID:
        raise AssertionError(
            f"{label} MERGE insert id {inserted_id} took _row_id {inserted_row_id!r}, "
            f"not the {V3_EXPECTED_INSERTED_ROW_ID} Spark assigns"
        )
    changed = [row[2] for row in survivors if row[0] == V3_MERGE_UPDATED_ID]
    unchanged = [row[2] for row in survivors if row[0] != V3_MERGE_UPDATED_ID]
    if len(changed) != 1 or changed[0] is None:
        raise AssertionError(f"{label} has no sequence for the MERGE-updated row: {lineage!r}")
    if any(value is None or value >= changed[0] for value in unchanged):
        raise AssertionError(
            f"{label} MERGE-updated sequence {changed[0]!r} does not lead every untouched row: "
            f"{unchanged!r}"
        )
    if inserted_sequence != changed[0]:
        raise AssertionError(
            f"{label} MERGE insert sequence {inserted_sequence!r} != the update's {changed[0]!r}"
        )
    if exact_commit_counts and changed[0] != V3_EXPECTED_MERGE_SEQUENCE:
        raise AssertionError(f"{label} MERGE sequence {changed[0]} != {V3_EXPECTED_MERGE_SEQUENCE}")


def assert_v3_row_ids_are_stable(
    before: list[tuple[int, int | None, int | None]],
    after: list[tuple[int, int | None, int | None]],
    label: str,
) -> None:
    """Every id present in both reads must carry the same stored ``_row_id``."""
    before_ids = {row[0]: row[1] for row in before}
    for row_key, row_id, _sequence in after:
        if row_key in before_ids and before_ids[row_key] != row_id:
            raise AssertionError(
                f"{label} moved _row_id for id {row_key}: {before_ids[row_key]!r} -> {row_id!r}"
            )


def assert_v3_acceptance_outcome(
    outcome: V3AcceptanceOutcome,
    exact_commit_counts: bool = True,
) -> None:
    """Pin every count the local engine produces on this exact v3 statement sequence.

    ``exact_commit_counts`` is False only on S3 Tables, whose automatic snapshot management
    commits on its own (docs/tier2-aws.md §2), which moves sequence and snapshot counts;
    row sets, ``_row_id`` values and every file count stay exact there.
    """
    expected_rows = v3_acceptance_expected_rows()
    expected_files = [(0, V3_FILES_PER_PARTITION), (1, V3_FILES_PER_PARTITION)]
    if outcome.data_files_per_partition != expected_files:
        raise AssertionError(
            f"appends left {outcome.data_files_per_partition!r} data files per partition, "
            f"expected {expected_files!r} — rewrite_data_files needs the min-input-files floor"
        )

    assert_deletion_vectors(
        outcome.delete_files_after_delete,
        V3_EXPECTED_DELETE_FILES_AFTER_DELETE,
        "after DELETE",
    )
    assert_deletion_vectors(
        outcome.delete_files_after_merge,
        V3_EXPECTED_DELETE_FILES_AFTER_MERGE,
        "after MERGE",
    )
    assert_deletion_vectors(
        outcome.delete_files_after_rewrite,
        V3_EXPECTED_DELETE_FILES_AFTER_REWRITE,
        "after rewrite_data_files",
    )

    if outcome.rows_after_merge != expected_rows:
        raise AssertionError(f"rows after MERGE {outcome.rows_after_merge!r} != {expected_rows!r}")
    if outcome.rows_after_rewrite != expected_rows:
        raise AssertionError(
            f"rows after rewrite {outcome.rows_after_rewrite!r} != {expected_rows!r}"
        )
    expected_pre_merge = [tuple(row) for row in V3_EXPECTED_PRE_MERGE_LINEAGE]
    if exact_commit_counts and outcome.lineage_before_merge != expected_pre_merge:
        raise AssertionError(
            f"lineage after DELETE {outcome.lineage_before_merge!r} != {expected_pre_merge!r}"
        )
    assert_v3_row_ids_are_stable(outcome.lineage_before_merge, outcome.lineage_after_merge, "MERGE")
    assert_v3_row_ids_are_stable(
        outcome.lineage_after_merge, outcome.lineage_after_rewrite, "rewrite_data_files"
    )
    assert_v3_lineage(outcome.lineage_after_merge, "after MERGE", exact_commit_counts)
    assert_v3_lineage(outcome.lineage_after_rewrite, "after rewrite", exact_commit_counts)
    if outcome.lineage_after_rewrite != outcome.lineage_after_merge:
        raise AssertionError(
            f"rewrite_data_files moved row lineage: {outcome.lineage_after_merge!r} → "
            f"{outcome.lineage_after_rewrite!r}"
        )

    if outcome.rewritten_data_files_count != V3_EXPECTED_REWRITTEN_DATA_FILES:
        raise AssertionError(
            f"rewritten_data_files_count {outcome.rewritten_data_files_count} != "
            f"{V3_EXPECTED_REWRITTEN_DATA_FILES}"
        )
    if outcome.added_data_files_count != V3_EXPECTED_ADDED_DATA_FILES:
        raise AssertionError(
            f"added_data_files_count {outcome.added_data_files_count} != "
            f"{V3_EXPECTED_ADDED_DATA_FILES}"
        )
    if outcome.removed_delete_files_count != V3_EXPECTED_REMOVED_DELETE_FILES:
        raise AssertionError(
            f"removed_delete_files_count {outcome.removed_delete_files_count} != "
            f"{V3_EXPECTED_REMOVED_DELETE_FILES}"
        )

    if exact_commit_counts:
        if outcome.snapshots_before_expire != V3_EXPECTED_SNAPSHOTS_BEFORE_EXPIRE:
            raise AssertionError(
                f"snapshots before expire {outcome.snapshots_before_expire} != "
                f"{V3_EXPECTED_SNAPSHOTS_BEFORE_EXPIRE}"
            )
        if outcome.snapshots_after_expire != V3_EXPECTED_SNAPSHOTS_AFTER_EXPIRE:
            raise AssertionError(
                f"snapshots after expire {outcome.snapshots_after_expire} != "
                f"{V3_EXPECTED_SNAPSHOTS_AFTER_EXPIRE}"
            )
    elif outcome.snapshots_before_expire < V3_EXPECTED_SNAPSHOTS_BEFORE_EXPIRE:
        raise AssertionError(
            f"snapshots before expire {outcome.snapshots_before_expire} below the engine floor "
            f"{V3_EXPECTED_SNAPSHOTS_BEFORE_EXPIRE}"
        )
    if outcome.snapshots_after_expire >= outcome.snapshots_before_expire:
        raise AssertionError(
            f"expire_snapshots dropped nothing: {outcome.snapshots_before_expire} → "
            f"{outcome.snapshots_after_expire}"
        )

    if outcome.adopted_table is not None and outcome.rows_after_adopt != expected_rows:
        raise AssertionError(
            f"rows read through register_table {outcome.rows_after_adopt!r} != {expected_rows!r}"
        )
