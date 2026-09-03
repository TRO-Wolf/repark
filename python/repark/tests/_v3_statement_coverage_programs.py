"""V3-COV inventory — the 81 statement programs and the v3 seeds they run on.

pins: v3-cov-statement-coverage/C-001
"""

from __future__ import annotations

from typing import Any, NamedTuple

_CATALOG = "v3cov"
_NAMESPACE = "cov"

_MOR = (
    "'format-version' = '3', 'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read'"
)
_COW = (
    "'format-version' = '3', 'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write'"
)

_FLAT_SCHEMA = "id INT, name STRING"
_FLAT_VALUES = "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
_FLAT_ROWS = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
_PART_SCHEMA = "id INT, name STRING, part INT"
_PART_VALUES = "(1, 'a', 10), (2, 'b', 10), (3, 'c', 20), (4, 'd', 20)"
_PART_ROWS = [(1, "a", 10), (2, "b", 10), (3, "c", 20), (4, "d", 20)]

_P_FLAT = "SELECT id, name FROM {t} ORDER BY id"
_P_PART = "SELECT id, name, part FROM {t} ORDER BY id"
_P_LINEAGE = "SELECT id, _row_id, _last_updated_sequence_number FROM {t} ORDER BY id"
_P_DELETES = "SELECT content, file_format, record_count FROM {t}.delete_files ORDER BY 1, 2, 3"
_P_SNAPSHOTS = "SELECT operation FROM {t}.snapshots ORDER BY committed_at"
_P_FILES = "SELECT content, record_count FROM {t}.files ORDER BY 1, 2"

_FLAT_PROBES = (_P_FLAT, _P_LINEAGE)
_PART_PROBES = (_P_PART, _P_LINEAGE)
_FLAT_MOR_PROBES = (_P_FLAT, _P_LINEAGE, _P_DELETES)
_PART_MOR_PROBES = (_P_PART, _P_LINEAGE, _P_DELETES)


class _Seed(NamedTuple):
    """A v3 table shape: DDL text, seed row values and the engine-side row tuples."""

    schema: str
    partition: str
    properties: str
    values: str
    rows: list[tuple]


_SEEDS: dict[str, _Seed] = {
    "mor": _Seed(_FLAT_SCHEMA, "", _MOR, _FLAT_VALUES, _FLAT_ROWS),
    "cow": _Seed(_FLAT_SCHEMA, "", _COW, _FLAT_VALUES, _FLAT_ROWS),
    "pmor": _Seed(_PART_SCHEMA, " PARTITIONED BY (part)", _MOR, _PART_VALUES, _PART_ROWS),
    "pcow": _Seed(_PART_SCHEMA, " PARTITIONED BY (part)", _COW, _PART_VALUES, _PART_ROWS),
    "v2": _Seed(_FLAT_SCHEMA, "", "'format-version' = '2'", _FLAT_VALUES, _FLAT_ROWS),
    "v2mor": _Seed(
        _FLAT_SCHEMA,
        "",
        "'format-version' = '2', 'write.delete.mode' = 'merge-on-read'",
        _FLAT_VALUES,
        _FLAT_ROWS,
    ),
}


class _Program(NamedTuple):
    """One inventory row: the statements under test on a v3 seed plus its comparison probes."""

    name: str
    group: str
    seed: str
    statements: tuple[Any, ...]
    probes: tuple[str, ...]
    source: bool = False
    meta: tuple[str, ...] = ()


def _merge(clause: str, source: str = "SELECT 2 AS id") -> str:
    """A MERGE program body over an inline source, in the arm the caller names."""
    return f"MERGE INTO {{t}} AS t USING ({source}) AS s ON t.id = s.id WHEN {clause}"


_PROGRAMS: tuple[_Program, ...] = (
    _Program(
        "create-v3-flat",
        "create",
        "",
        (
            "CREATE TABLE {t} ("
            + _FLAT_SCHEMA
            + ") USING iceberg TBLPROPERTIES ('format-version' = '3')",
        ),
        (_P_FLAT,),
        False,
        ("format-version", "schema", "partition-fields"),
    ),
    _Program(
        "create-v3-partitioned",
        "create",
        "",
        (
            "CREATE TABLE {t} ("
            + _PART_SCHEMA
            + ") USING iceberg PARTITIONED BY (part) TBLPROPERTIES ('format-version' = '3')",
        ),
        (_P_PART,),
        False,
        ("format-version", "schema", "partition-fields"),
    ),
    _Program(
        "create-v3-bucket-transform",
        "create",
        "",
        (
            "CREATE TABLE {t} ("
            + _PART_SCHEMA
            + ") USING iceberg PARTITIONED BY (bucket(4, id)) "
            + "TBLPROPERTIES ('format-version' = '3')",
        ),
        (_P_PART,),
        False,
        ("format-version", "schema", "partition-fields"),
    ),
    _Program(
        "create-v3-write-order",
        "create",
        "",
        (
            "CREATE TABLE {t} ("
            + _FLAT_SCHEMA
            + ") USING iceberg TBLPROPERTIES ('format-version' = '3') WRITE ORDERED BY id",
        ),
        (_P_FLAT,),
    ),
    _Program(
        "create-v3-properties",
        "create",
        "",
        ("CREATE TABLE {t} (" + _FLAT_SCHEMA + ") USING iceberg TBLPROPERTIES (" + _MOR + ")",),
        (_P_FLAT,),
        False,
        ("format-version", "schema", "partition-fields", "write-properties"),
    ),
    _Program(
        "ctas-v3",
        "create",
        "",
        (
            "CREATE TABLE {t} USING iceberg TBLPROPERTIES ('format-version' = '3') AS SELECT 1 AS "
            "id, 'a' AS name",
        ),
        (_P_FLAT,),
        False,
        ("format-version", "schema", "partition-fields"),
    ),
    _Program("insert-into", "insert", "mor", ("INSERT INTO {t} VALUES (5, 'e')",), _FLAT_PROBES),
    _Program(
        "insert-into-select",
        "insert",
        "mor",
        ("INSERT INTO {t} SELECT id + 10, name FROM {t}",),
        _FLAT_PROBES,
    ),
    _Program(
        "insert-overwrite-table",
        "insert",
        "mor",
        ("INSERT OVERWRITE {t} VALUES (9, 'z')",),
        _FLAT_PROBES,
    ),
    _Program(
        "insert-overwrite-partition-static-values",
        "insert",
        "pmor",
        ("INSERT OVERWRITE {t} PARTITION (part = 10) VALUES (CAST(7 AS INT), 'g')",),
        _PART_PROBES,
    ),
    _Program(
        "insert-overwrite-partition-static-select",
        "insert",
        "pmor",
        (
            "INSERT OVERWRITE {t} PARTITION (part = 10) SELECT CAST(id AS INT), CAST(name AS "
            "STRING) FROM {t} WHERE id = 1",
        ),
        _PART_PROBES,
    ),
    _Program(
        "insert-overwrite-partition-dynamic",
        "insert",
        "pmor",
        (
            "INSERT OVERWRITE {t} PARTITION (part) SELECT CAST(7 AS INT), CAST('g' AS STRING), "
            "CAST(10 AS INT)",
        ),
        _PART_PROBES,
    ),
    _Program(
        "delete-where-mor", "delete", "mor", ("DELETE FROM {t} WHERE id = 2",), _FLAT_MOR_PROBES
    ),
    _Program("delete-where-cow", "delete", "cow", ("DELETE FROM {t} WHERE id = 2",), _FLAT_PROBES),
    _Program(
        "delete-where-partitioned-mor",
        "delete",
        "pmor",
        ("DELETE FROM {t} WHERE id = 2",),
        _PART_MOR_PROBES,
    ),
    _Program(
        "delete-where-partitioned-cow",
        "delete",
        "pcow",
        ("DELETE FROM {t} WHERE id = 2",),
        _PART_PROBES,
    ),
    _Program(
        "delete-in-subquery-mor",
        "delete",
        "mor",
        ("DELETE FROM {t} WHERE id IN (SELECT id FROM {s})",),
        _FLAT_MOR_PROBES,
        True,
    ),
    _Program(
        "delete-not-in-subquery-mor",
        "delete",
        "mor",
        ("DELETE FROM {t} WHERE id NOT IN (SELECT id FROM {s})",),
        _FLAT_MOR_PROBES,
        True,
    ),
    _Program(
        "delete-exists-subquery-mor",
        "delete",
        "mor",
        ("DELETE FROM {t} WHERE EXISTS (SELECT 1 FROM {s} WHERE {s}.id = {t}.id)",),
        _FLAT_MOR_PROBES,
        True,
    ),
    _Program(
        "delete-not-exists-subquery-mor",
        "delete",
        "mor",
        ("DELETE FROM {t} WHERE NOT EXISTS (SELECT 1 FROM {s} WHERE {s}.id = {t}.id)",),
        _FLAT_MOR_PROBES,
        True,
    ),
    _Program(
        "delete-in-subquery-cow",
        "delete",
        "cow",
        ("DELETE FROM {t} WHERE id IN (SELECT id FROM {s})",),
        _FLAT_PROBES,
        True,
    ),
    _Program(
        "delete-all-rows-mor",
        "delete",
        "mor",
        ("DELETE FROM {t} WHERE id > 0",),
        (*_FLAT_MOR_PROBES, _P_FILES),
    ),
    _Program(
        "update-where-mor",
        "update",
        "mor",
        ("UPDATE {t} SET name = 'z' WHERE id = 2",),
        _FLAT_MOR_PROBES,
    ),
    _Program(
        "update-where-cow",
        "update",
        "cow",
        ("UPDATE {t} SET name = 'z' WHERE id = 2",),
        _FLAT_PROBES,
    ),
    _Program(
        "update-where-partitioned-mor",
        "update",
        "pmor",
        ("UPDATE {t} SET name = 'z' WHERE id = 2",),
        _PART_MOR_PROBES,
    ),
    _Program(
        "update-in-subquery-mor",
        "update",
        "mor",
        ("UPDATE {t} SET name = 'z' WHERE id IN (SELECT id FROM {s})",),
        _FLAT_MOR_PROBES,
        True,
    ),
    _Program(
        "update-not-in-subquery-mor",
        "update",
        "mor",
        ("UPDATE {t} SET name = 'z' WHERE id NOT IN (SELECT id FROM {s})",),
        _FLAT_MOR_PROBES,
        True,
    ),
    _Program(
        "update-exists-subquery-mor",
        "update",
        "mor",
        ("UPDATE {t} SET name = 'z' WHERE EXISTS (SELECT 1 FROM {s} WHERE {s}.id = {t}.id)",),
        _FLAT_MOR_PROBES,
        True,
    ),
    _Program(
        "update-partition-key-cow",
        "update",
        "pcow",
        ("UPDATE {t} SET part = 30 WHERE id = 2",),
        _PART_PROBES,
    ),
    _Program(
        "merge-matched-update-mor",
        "merge",
        "mor",
        (_merge("MATCHED THEN UPDATE SET t.name = 'z'"),),
        _FLAT_MOR_PROBES,
    ),
    _Program(
        "merge-matched-update-cow",
        "merge",
        "cow",
        (_merge("MATCHED THEN UPDATE SET t.name = 'z'"),),
        _FLAT_PROBES,
    ),
    _Program(
        "merge-matched-delete-mor",
        "merge",
        "mor",
        (_merge("MATCHED THEN DELETE"),),
        _FLAT_MOR_PROBES,
    ),
    _Program(
        "merge-matched-delete-cow", "merge", "cow", (_merge("MATCHED THEN DELETE"),), _FLAT_PROBES
    ),
    _Program(
        "merge-not-matched-insert",
        "merge",
        "mor",
        (_merge("NOT MATCHED THEN INSERT (id, name) VALUES (s.id, 'n')", "SELECT 9 AS id"),),
        _FLAT_MOR_PROBES,
    ),
    _Program(
        "merge-not-matched-by-source-delete",
        "merge",
        "mor",
        (_merge("NOT MATCHED BY SOURCE THEN DELETE"),),
        _FLAT_MOR_PROBES,
    ),
    _Program(
        "merge-not-matched-by-source-update",
        "merge",
        "mor",
        (_merge("NOT MATCHED BY SOURCE THEN UPDATE SET t.name = 'z'"),),
        _FLAT_MOR_PROBES,
    ),
    _Program(
        "merge-mixed-arms",
        "merge",
        "mor",
        (
            "MERGE INTO {t} AS t USING (SELECT 2 AS id UNION ALL SELECT 9) AS s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.name = 'z' WHEN NOT MATCHED THEN INSERT (id, name) "
            "VALUES (s.id, 'n') WHEN NOT MATCHED BY SOURCE THEN DELETE",
        ),
        _FLAT_MOR_PROBES,
    ),
    _Program(
        "merge-matched-conditional",
        "merge",
        "mor",
        (_merge("MATCHED AND t.name = 'b' THEN UPDATE SET t.name = 'z'"),),
        _FLAT_MOR_PROBES,
    ),
    _Program(
        "merge-partitioned-mor", "merge", "pmor", (_merge("MATCHED THEN DELETE"),), _PART_MOR_PROBES
    ),
    _Program(
        "merge-source-table-mor",
        "merge",
        "mor",
        (
            "MERGE INTO {t} AS t USING {s} AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name "
            "= 'z'",
        ),
        _FLAT_MOR_PROBES,
        True,
    ),
    _Program(
        "alter-add-column",
        "alter",
        "mor",
        ("ALTER TABLE {t} ADD COLUMN extra INT",),
        ("SELECT id, name, extra FROM {t} ORDER BY id", _P_LINEAGE),
    ),
    _Program(
        "alter-drop-column",
        "alter",
        "pmor",
        ("ALTER TABLE {t} DROP COLUMN name",),
        ("SELECT id, part FROM {t} ORDER BY id",),
    ),
    _Program(
        "alter-rename-column",
        "alter",
        "mor",
        ("ALTER TABLE {t} RENAME COLUMN name TO label",),
        ("SELECT id, label FROM {t} ORDER BY id", _P_LINEAGE),
    ),
    _Program(
        "alter-alter-column-type",
        "alter",
        "mor",
        ("ALTER TABLE {t} ALTER COLUMN id TYPE BIGINT",),
        _FLAT_PROBES,
    ),
    _Program(
        "alter-add-partition-field",
        "alter",
        "mor",
        ("ALTER TABLE {t} ADD PARTITION FIELD name", "INSERT INTO {t} VALUES (5, 'e')"),
        (_P_FLAT, _P_LINEAGE, "SELECT spec_id, record_count FROM {t}.files ORDER BY 1, 2"),
    ),
    _Program(
        "alter-drop-partition-field",
        "alter",
        "pmor",
        ("ALTER TABLE {t} DROP PARTITION FIELD part", "INSERT INTO {t} VALUES (5, 'e', 30)"),
        (_P_PART, "SELECT spec_id, record_count FROM {t}.files ORDER BY 1, 2"),
    ),
    _Program(
        "alter-replace-partition-field",
        "alter",
        "pmor",
        (
            "ALTER TABLE {t} REPLACE PARTITION FIELD part WITH bucket(2, id)",
            "INSERT INTO {t} VALUES (5, 'e', 30)",
        ),
        (_P_PART, "SELECT spec_id, record_count FROM {t}.files ORDER BY 1, 2"),
        False,
        ("partition-fields",),
    ),
    _Program(
        "alter-add-column-partitioned",
        "alter",
        "pmor",
        ("ALTER TABLE {t} ADD COLUMN extra INT",),
        ("SELECT id, name, part, extra FROM {t} ORDER BY id", _P_LINEAGE),
    ),
    _Program(
        "alter-set-tblproperties",
        "alter",
        "mor",
        (
            "ALTER TABLE {t} SET TBLPROPERTIES ('write.delete.granularity' = 'partition')",
            "DELETE FROM {t} WHERE id = 2",
        ),
        _FLAT_MOR_PROBES,
    ),
    _Program(
        "alter-unset-tblproperties",
        "alter",
        "mor",
        (
            "ALTER TABLE {t} UNSET TBLPROPERTIES ('write.delete.mode')",
            "DELETE FROM {t} WHERE id = 2",
        ),
        (_P_FLAT, _P_LINEAGE, _P_DELETES),
    ),
    _Program(
        "alter-write-ordered-by",
        "alter",
        "mor",
        ("ALTER TABLE {t} WRITE ORDERED BY id",),
        _FLAT_PROBES,
    ),
    _Program(
        "alter-set-format-version-3",
        "alter",
        "v2",
        (
            "ALTER TABLE {t} SET TBLPROPERTIES ('format-version' = '3')",
            "DELETE FROM {t} WHERE id = 2",
        ),
        (_P_FLAT, _P_LINEAGE, _P_DELETES),
    ),
    _Program(
        "alter-set-format-version-3-mor",
        "alter",
        "v2mor",
        (
            "ALTER TABLE {t} SET TBLPROPERTIES ('format-version' = '3')",
            "DELETE FROM {t} WHERE id = 2",
        ),
        (_P_FLAT, _P_LINEAGE, _P_DELETES),
    ),
    _Program(
        "truncate-table", "lifecycle", "mor", ("TRUNCATE TABLE {t}",), (_P_FLAT, _P_SNAPSHOTS)
    ),
    _Program("drop-table", "lifecycle", "mor", ("DROP TABLE {t}",), (_P_FLAT,)),
    _Program(
        "meta-snapshots", "metadata", "mor", ("DELETE FROM {t} WHERE id = 2",), (_P_SNAPSHOTS,)
    ),
    _Program("meta-files", "metadata", "mor", ("DELETE FROM {t} WHERE id = 2",), (_P_FILES,)),
    _Program(
        "meta-delete-files", "metadata", "mor", ("DELETE FROM {t} WHERE id = 2",), (_P_DELETES,)
    ),
    _Program(
        "meta-manifests",
        "metadata",
        "mor",
        ("DELETE FROM {t} WHERE id = 2",),
        (
            "SELECT content, added_data_files_count, existing_data_files_count, "
            "deleted_data_files_count FROM {t}.manifests ORDER BY 1, 2, 3, 4",
        ),
    ),
    _Program(
        "meta-history",
        "metadata",
        "mor",
        ("DELETE FROM {t} WHERE id = 2",),
        ("SELECT is_current_ancestor FROM {t}.history ORDER BY made_current_at",),
    ),
    _Program(
        "meta-refs",
        "metadata",
        "mor",
        ("ALTER TABLE {t} CREATE TAG t1",),
        ("SELECT name, type, max_reference_age_in_ms FROM {t}.refs ORDER BY 1, 2",),
    ),
    _Program(
        "meta-partitions",
        "metadata",
        "pmor",
        ("DELETE FROM {t} WHERE id = 2",),
        (
            "SELECT record_count, file_count, position_delete_record_count FROM {t}.partitions "
            "ORDER BY 1, 2, 3",
        ),
    ),
    _Program(
        "meta-entries",
        "metadata",
        "mor",
        ("DELETE FROM {t} WHERE id = 2",),
        ("SELECT status FROM {t}.entries ORDER BY 1",),
    ),
    _Program(
        "meta-all-data-files",
        "metadata",
        "mor",
        ("DELETE FROM {t} WHERE id = 2",),
        ("SELECT content, record_count FROM {t}.all_data_files ORDER BY 1, 2",),
    ),
    _Program(
        "meta-position-deletes",
        "metadata",
        "mor",
        ("DELETE FROM {t} WHERE id = 2",),
        ("SELECT pos FROM {t}.position_deletes ORDER BY 1",),
    ),
    _Program(
        "lineage-projection",
        "lineage",
        "mor",
        ("UPDATE {t} SET name = 'z' WHERE id = 2",),
        (_P_LINEAGE, "SELECT _row_id FROM {t} ORDER BY _row_id"),
    ),
    _Program(
        "time-travel-version-as-of",
        "time travel",
        "mor",
        ("DELETE FROM {t} WHERE id = 2",),
        ("SELECT id, name FROM {t} VERSION AS OF {snapshot0} ORDER BY id",),
    ),
    _Program(
        "time-travel-timestamp-as-of",
        "time travel",
        "mor",
        ("DELETE FROM {t} WHERE id = 2",),
        ("SELECT id, name FROM {t} TIMESTAMP AS OF '{timestamp0}' ORDER BY id",),
    ),
    _Program(
        "branch-create-and-read",
        "refs",
        "mor",
        ("ALTER TABLE {t} CREATE BRANCH b1", "DELETE FROM {t} WHERE id = 2"),
        ("SELECT id, name FROM {t}.branch_b1 ORDER BY id", _P_FLAT),
    ),
    _Program(
        "branch-write",
        "refs",
        "mor",
        ("ALTER TABLE {t} CREATE BRANCH b1", "INSERT INTO {t}.branch_b1 VALUES (5, 'e')"),
        ("SELECT id, name FROM {t}.branch_b1 ORDER BY id", _P_FLAT),
    ),
    _Program(
        "branch-create-replace-and-drop",
        "refs",
        "mor",
        (
            "ALTER TABLE {t} CREATE BRANCH b1",
            "ALTER TABLE {t} CREATE OR REPLACE BRANCH b1",
            "ALTER TABLE {t} DROP BRANCH b1",
        ),
        ("SELECT name, type FROM {t}.refs ORDER BY 1, 2",),
    ),
    _Program(
        "tag-create-and-read",
        "refs",
        "mor",
        ("ALTER TABLE {t} CREATE TAG t1", "DELETE FROM {t} WHERE id = 2"),
        ("SELECT id, name FROM {t}.tag_t1 ORDER BY id", _P_FLAT),
    ),
    _Program(
        "tag-retention",
        "refs",
        "mor",
        ("ALTER TABLE {t} CREATE TAG t1 RETAIN 10 DAYS",),
        ("SELECT name, type, max_reference_age_in_ms FROM {t}.refs ORDER BY 1, 2",),
    ),
    _Program(
        "branch-retention",
        "refs",
        "mor",
        ("ALTER TABLE {t} CREATE BRANCH b1 WITH SNAPSHOT RETENTION 2 SNAPSHOTS",),
        ("SELECT name, type, min_snapshots_to_keep FROM {t}.refs ORDER BY 1, 2",),
    ),
    _Program(
        "call-expire-snapshots",
        "call",
        "mor",
        (
            "DELETE FROM {t} WHERE id = 2",
            (
                "CALL {c}.system.expire_snapshots(table => '{q}', older_than => TIMESTAMP "
                "'2999-01-01 00:00:00', retain_last => 1)",
                (
                    "deleted_data_files_count",
                    "deleted_position_delete_files_count",
                    "deleted_equality_delete_files_count",
                    "deleted_manifest_files_count",
                    "deleted_manifest_lists_count",
                ),
            ),
        ),
        (_P_FLAT, _P_SNAPSHOTS),
    ),
    _Program(
        "call-remove-orphan-files",
        "call",
        "mor",
        (
            (
                "CALL {c}.system.remove_orphan_files(table => '{q}', older_than => TIMESTAMP "
                "'2020-01-01 00:00:00')",
                ("orphan_file_location",),
            ),
        ),
        (_P_FLAT,),
    ),
    _Program(
        "call-rewrite-data-files",
        "call",
        "mor",
        (
            "DELETE FROM {t} WHERE id = 2",
            (
                "CALL {c}.system.rewrite_data_files(table => '{q}')",
                ("rewritten_data_files_count", "added_data_files_count", "failed_data_files_count"),
            ),
        ),
        (_P_FLAT, _P_LINEAGE, _P_DELETES),
    ),
    _Program(
        "call-rewrite-manifests",
        "call",
        "mor",
        (
            (
                "CALL {c}.system.rewrite_manifests(table => '{q}')",
                ("rewritten_manifests_count", "added_manifests_count"),
            ),
        ),
        (_P_FLAT,),
    ),
    _Program(
        "call-rewrite-position-delete-files",
        "call",
        "mor",
        (
            "DELETE FROM {t} WHERE id = 2",
            (
                "CALL {c}.system.rewrite_position_delete_files(table => '{q}')",
                ("rewritten_delete_files_count", "added_delete_files_count"),
            ),
        ),
        (_P_FLAT, _P_DELETES),
    ),
    _Program(
        "call-rollback-to-snapshot",
        "call",
        "mor",
        (
            "DELETE FROM {t} WHERE id = 2",
            "CALL {c}.system.rollback_to_snapshot(table => '{q}', snapshot_id => {snapshot0})",
        ),
        (_P_FLAT, _P_LINEAGE),
    ),
    _Program(
        "call-register-table",
        "call",
        "mor",
        ("CALL {c}.system.register_table(table => '{q}_reg', metadata_file => '{metadata}')",),
        ("SELECT id, name FROM {t}_reg ORDER BY id",),
    ),
)


def _mentions(program: _Program, token: str) -> bool:
    """Whether any statement or probe of one program interpolates ``token``."""
    texts = [item[0] if isinstance(item, tuple) else item for item in program.statements]
    texts.extend(program.probes)
    return any(token in text for text in texts)


NEEDS_SNAPSHOT_MARKS: frozenset[str] = frozenset(
    program.name
    for program in _PROGRAMS
    if _mentions(program, "{snapshot0}") or _mentions(program, "{timestamp0}")
)
NEEDS_METADATA_PATH: frozenset[str] = frozenset(
    program.name for program in _PROGRAMS if _mentions(program, "{metadata}")
)
