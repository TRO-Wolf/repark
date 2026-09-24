"""R-DF-LOAD-PATH / R-DF-LOAD-METADATA-JSON — ``format("iceberg").load(<path>)`` reads.

Spark's IcebergSource rule: a ``load`` argument containing ``/`` is a filesystem path —
a ``*.metadata.json`` path pins that metadata file's snapshot, and any other path is a
table location whose current metadata resolves through ``version-hint.text`` or the
highest-numbered metadata file under ``<location>/metadata``. The pinned near-miss
identifiers take the catalog route. Arrow ``to_arrow`` pins carry value AND type (docs/testing.md).

pins: dfload-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException

TABLE = "mem.ns.events"
EXPECTED_CURRENT = [[2, "b", "y"], [3, "c", "x"]]
EXPECTED_PRE_DELETE = [[1, "a", "z"], [2, "b", "y"], [3, "c", "x"]]
PATH_OPTION_REFUSAL = (
    "format('iceberg').load(<path>) reads one pinned metadata snapshot and does not "
    "support time-travel or incremental options; got {keys}"
)
PATH_REFUSED_OPTIONS = [
    ("snapshot-id", "1", "snapshot-id"),
    ("SNAPSHOT-ID", "1", "snapshot-id"),
    ("Snapshot-Id", "1", "snapshot-id"),
    ("as-of-timestamp", "1", "as-of-timestamp"),
    ("branch", "main", "branch"),
    ("tag", "main", "tag"),
    ("versionAsOf", "1", "versionasof"),
    ("timestampAsOf", "1", "timestampasof"),
    ("start-snapshot-id", "1", "start-snapshot-id"),
    ("end-snapshot-id", "1", "end-snapshot-id"),
    ("start-timestamp", "1", "start-timestamp"),
    ("end-timestamp", "1", "end-timestamp"),
]
WRONG_FS_EXPECTED = ", expected: file:///"
NO_METADATA_REFUSAL = (
    "no Iceberg table found at '{path}': expected metadata files under "
    "'<location>/metadata' or a '<location>/metadata/version-hint.text'"
)
HINTED_MISSING_REFUSAL = (
    "no Iceberg table found at '{path}': version-hint.text names "
    "'v7.metadata.json', which does not exist"
)
METADATA_TABLE_REFUSAL = "Error during planning: table 'ns.events.snapshots' not found"
SNAPSHOT_ID_REFUSAL = (
    "Time travel option `snapshot-id` is no longer supported, "
    "use Spark built-in `versionAsOf` instead"
)


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with a ``mem`` memory catalog and the ``mem.ns`` namespace."""
    session = ReparkSession.builder.appName("pytest-iceberg-load-path").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


@pytest.fixture
def loaded(spark: ReparkSession, tmp_path: Path) -> dict[str, object]:
    """The fixture table (insert three rows, delete one) and its metadata file list."""
    spark.sql(
        f"CREATE TABLE {TABLE} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'a', 'z'), (2, 'b', 'y'), (3, 'c', 'x')")
    spark.sql(f"DELETE FROM {TABLE} WHERE id = 1")
    table_dir = _table_dir(tmp_path)
    metadata_files = sorted((table_dir / "metadata").glob("*.metadata.json"))
    assert len(metadata_files) >= 3, (
        f"expected create+insert+delete metadata files under {table_dir / 'metadata'}"
    )
    return {"table_dir": table_dir, "metadata_files": metadata_files}


def _table_dir(root: Path) -> Path:
    """Return the ``events`` table directory discovered under the warehouse root."""
    hits = [p.parent for p in root.rglob("metadata") if p.parent.name == "events"]
    assert hits, f"no events table directory under {root}"
    return hits[0]


def _pin_schema(table: pa.Table) -> None:
    """Pin the Arrow schema: BIGINT ``id``, STRING ``data`` / ``cat`` (value AND type)."""
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("data").type == pa.string()
    assert table.schema.field("cat").type == pa.string()


def _sorted_rows(table: pa.Table) -> list[list[object]]:
    """Sorted full-row multiset for the ``(id, data, cat)`` fixture table."""
    rows = table.select(["id", "data", "cat"]).to_pylist()
    return sorted([int(row["id"]), str(row["data"]), str(row["cat"])] for row in rows)


def _registration_inventory(spark: ReparkSession) -> dict[str, object]:
    """Every catalog, its namespaces and their tables, and every temp view the session lists."""
    current = spark.catalog.currentCatalog()
    catalogs = sorted(c.name for c in spark.catalog.listCatalogs())
    per_catalog: dict[str, dict[str, list[str]]] = {}
    try:
        for catalog in catalogs:
            spark.catalog.setCurrentCatalog(catalog)
            namespaces = sorted(d.name for d in spark.catalog.listDatabases())
            per_catalog[catalog] = {
                ns: sorted(t.name for t in spark.catalog.listTables(ns)) for ns in namespaces
            }
    finally:
        spark.catalog.setCurrentCatalog(current)
    temp_views = sorted(t.name for t in spark.catalog.listTables())
    return {
        "current_catalog": spark.catalog.currentCatalog(),
        "catalogs": catalogs,
        "tables": per_catalog,
        "temp_views": temp_views,
        "temp_view_names": sorted(spark.list_temp_view_names()),
    }


def _assert_current(frame: object) -> None:
    """Pin the current-snapshot rows and schema of a loaded DataFrame."""
    arrow = frame.to_arrow()
    _pin_schema(arrow)
    assert _sorted_rows(arrow) == EXPECTED_CURRENT


def test_load_table_location_reads_current_rows(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load(<table location>)`` answers the current snapshot like Spark.

    pins: dfload-1/C-001
    """
    frame = spark.read.format("iceberg").load(str(loaded["table_dir"]))
    _assert_current(frame)


def test_load_table_location_trailing_slash(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """One trailing ``/`` on the location resolves the same current snapshot.

    pins: dfload-1/C-001
    """
    frame = spark.read.format("iceberg").load(f"{loaded['table_dir']}/")
    _assert_current(frame)


def test_load_latest_metadata_file_reads_current_rows(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load(<latest metadata.json>)`` reads that file's snapshot (the current one).

    pins: dfload-1/C-002
    """
    latest = loaded["metadata_files"][-1]
    frame = spark.read.format("iceberg").load(str(latest))
    _assert_current(frame)


def test_load_older_metadata_file_reads_that_version(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load(<an older metadata.json>)`` answers the pre-delete three-row snapshot.

    pins: dfload-1/C-002
    """
    pre_delete = loaded["metadata_files"][-2]
    frame = spark.read.format("iceberg").load(str(pre_delete))
    arrow = frame.to_arrow()
    _pin_schema(arrow)
    assert _sorted_rows(arrow) == EXPECTED_PRE_DELETE


def test_load_path_registers_no_catalog_table(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """A path read leaves no registration behind: the inventory is equal after collection.

    pins: dfload-1/C-007
    """
    spark.catalog.setCurrentCatalog("mem")
    before = _registration_inventory(spark)
    assert before["current_catalog"] == "mem"
    frame = spark.read.format("iceberg").load(str(loaded["table_dir"]))
    _assert_current(frame)
    assert _registration_inventory(spark) == before
    assert spark.catalog.currentCatalog() == "mem"
    assert not spark.catalog.table_exists("path.events")


@pytest.mark.parametrize(("key", "value", "reported"), PATH_REFUSED_OPTIONS)
def test_load_path_with_travel_or_incremental_option_refuses(
    spark: ReparkSession, loaded: dict[str, object], key: str, value: str, reported: str
) -> None:
    """Each time-travel and incremental option beside a path refuses the full pinned text.

    pins: dfload-1/C-004
    """
    with pytest.raises(AnalysisException) as raised:
        spark.read.format("iceberg").option(key, value).load(str(loaded["table_dir"]))
    assert str(raised.value) == PATH_OPTION_REFUSAL.format(keys=reported)


def test_load_path_with_two_refused_options_names_them_sorted(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """Two refused options name their lower-cased keys sorted and ``, ``-joined.

    pins: dfload-1/C-004
    """
    reader = spark.read.format("iceberg").option("tag", "main").option("start-timestamp", "1")
    with pytest.raises(AnalysisException) as raised:
        reader.load(str(loaded["table_dir"]))
    assert str(raised.value) == PATH_OPTION_REFUSAL.format(keys="start-timestamp, tag")


def test_load_missing_location_names_the_path(spark: ReparkSession, tmp_path: Path) -> None:
    """A location with no resolvable metadata raises AnalysisException naming the path.

    pins: dfload-1/C-005
    """
    missing = str(tmp_path / "no" / "such" / "dir")
    with pytest.raises(AnalysisException) as raised:
        spark.read.format("iceberg").load(missing)
    assert str(raised.value) == NO_METADATA_REFUSAL.format(path=missing)


def test_load_empty_metadata_dir_names_the_path(spark: ReparkSession, tmp_path: Path) -> None:
    """An existing ``metadata/`` directory with no metadata files refuses naming the path.

    pins: dfload-1/C-005
    """
    location = tmp_path / "empty-table"
    (location / "metadata").mkdir(parents=True)
    with pytest.raises(AnalysisException) as raised:
        spark.read.format("iceberg").load(str(location))
    assert str(raised.value) == NO_METADATA_REFUSAL.format(path=location)


def test_load_hinted_missing_metadata_names_the_path(spark: ReparkSession, tmp_path: Path) -> None:
    """A ``version-hint.text`` naming a metadata file that does not exist refuses
    AnalysisException naming the path, not an internal Iceberg error.

    pins: dfload-1/C-003
    """
    location = tmp_path / "hinted-table"
    (location / "metadata").mkdir(parents=True)
    (location / "metadata" / "version-hint.text").write_text("7\n")
    with pytest.raises(AnalysisException) as raised:
        spark.read.format("iceberg").load(str(location))
    assert str(raised.value) == HINTED_MISSING_REFUSAL.format(path=location)


def test_load_two_part_identifier_keeps_catalog_route(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("ns.events")`` resolves under the current catalog, not the path branch.

    pins: dfload-1/C-006
    """
    frame = spark.read.format("iceberg").load("ns.events")
    _assert_current(frame)


def test_load_three_part_identifier_keeps_catalog_route(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("mem.ns.events")`` keeps the catalog route.

    pins: dfload-1/C-006
    """
    frame = spark.read.format("iceberg").load("mem.ns.events")
    _assert_current(frame)


def test_load_metadata_table_identifier_keeps_catalog_route(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("ns.events.snapshots")`` keeps the catalog route's measured refusal.

    pins: dfload-1/C-006
    """
    with pytest.raises(AnalysisException) as raised:
        spark.read.format("iceberg").load("ns.events.snapshots")
    assert str(raised.value) == METADATA_TABLE_REFUSAL


def test_load_quoted_identifier_keeps_catalog_route(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("`ns`.`events`")`` parses as a quoted identifier, not a path.

    pins: dfload-1/C-006
    """
    frame = spark.read.format("iceberg").load("`ns`.`events`")
    _assert_current(frame)


def test_load_identifier_snapshot_id_keeps_spark_refusal(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``snapshot-id`` on a catalog identifier keeps Spark's IllegalArgumentException.

    pins: dfload-1/C-006
    """
    with pytest.raises(IllegalArgumentException) as raised:
        spark.read.format("iceberg").option("snapshot-id", "1").load("ns.events")
    assert str(raised.value) == SNAPSHOT_ID_REFUSAL


def test_load_file_scheme_location_reads_current_rows(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("file:///<abs>")`` and ``load("file:///<abs>/")`` read the current snapshot.

    pins: dfload-1/C-008
    """
    _assert_current(spark.read.format("iceberg").load(f"file://{loaded['table_dir']}"))
    _assert_current(spark.read.format("iceberg").load(f"file://{loaded['table_dir']}/"))


def test_load_file_scheme_latest_metadata_file_reads_current_rows(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("file:///<abs>/metadata/<latest>.metadata.json")`` reads the current snapshot.

    pins: dfload-1/C-008
    """
    latest = loaded["metadata_files"][-1]
    _assert_current(spark.read.format("iceberg").load(f"file://{latest}"))


def test_load_file_authority_location_refuses_wrong_fs(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("file://tmp/...")`` refuses Spark's Wrong FS text naming ``<arg>/metadata``.

    pins: dfload-1/C-008
    """
    argument = "file://" + str(loaded["table_dir"]).lstrip("/")
    with pytest.raises(IllegalArgumentException) as raised:
        spark.read.format("iceberg").load(argument)
    assert str(raised.value) == f"Wrong FS: {argument}/metadata{WRONG_FS_EXPECTED}"


def test_load_file_authority_metadata_file_refuses_wrong_fs(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("file://tmp/.../<latest>.metadata.json")`` refuses Wrong FS naming the argument.

    pins: dfload-1/C-008
    """
    argument = "file://" + str(loaded["metadata_files"][-1]).lstrip("/")
    with pytest.raises(IllegalArgumentException) as raised:
        spark.read.format("iceberg").load(argument)
    assert str(raised.value) == f"Wrong FS: {argument}{WRONG_FS_EXPECTED}"


def test_load_file_single_slash_location_reads_current_rows(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``file:/<abs>``, ``FILE:/<abs>`` and ``fIlE:/<abs>`` read like ``file:///<abs>``.

    pins: dfload-1/C-009
    """
    _assert_current(spark.read.format("iceberg").load(f"file:{loaded['table_dir']}"))
    _assert_current(spark.read.format("iceberg").load(f"FILE:{loaded['table_dir']}"))
    _assert_current(spark.read.format("iceberg").load(f"fIlE:{loaded['table_dir']}"))


def test_load_file_single_slash_latest_metadata_file_reads_current_rows(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("file:/<abs>/metadata/<latest>.metadata.json")`` reads the current snapshot.

    pins: dfload-1/C-009
    """
    latest = loaded["metadata_files"][-1]
    _assert_current(spark.read.format("iceberg").load(f"file:{latest}"))


def test_load_file_four_slash_location_reads_current_rows(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("file:////<abs>")`` keeps reading the current snapshot.

    pins: dfload-1/C-009
    """
    _assert_current(spark.read.format("iceberg").load(f"file:///{loaded['table_dir']}"))


def test_load_file_relative_location_refuses_uri_syntax(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("file:tmp/...")`` refuses Spark's relative-path URISyntaxException text.

    pins: dfload-1/C-009
    """
    argument = "file:" + str(loaded["table_dir"]).lstrip("/")
    with pytest.raises(IllegalArgumentException) as raised:
        spark.read.format("iceberg").load(argument)
    assert str(raised.value) == (
        f"java.net.URISyntaxException: Relative path in absolute URI: {argument}"
    )


def test_load_file_relative_location_two_trailing_slashes_keeps_one(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("file:tmp/...//")`` refuses naming the argument with exactly one trailing ``/``.

    pins: dfload-1/C-009
    """
    argument = "file:" + str(loaded["table_dir"]).lstrip("/")
    with pytest.raises(IllegalArgumentException) as raised:
        spark.read.format("iceberg").load(f"{argument}//")
    assert str(raised.value) == (
        f"java.net.URISyntaxException: Relative path in absolute URI: {argument}/"
    )


def test_load_v_form_beyond_java_int_is_not_a_candidate(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """A ``v2147483648.metadata.json`` copy of the create snapshot does not win the listing.

    pins: dfload-1/C-010
    """
    table_dir = loaded["table_dir"]
    shutil.copy(loaded["metadata_files"][0], table_dir / "metadata" / "v2147483648.metadata.json")
    _assert_current(spark.read.format("iceberg").load(str(table_dir)))


def test_load_path_option_refuses_before_resolution(spark: ReparkSession, tmp_path: Path) -> None:
    """The option refusal wins over a location that would refuse for missing metadata.

    pins: dfload-1/C-004
    """
    missing = str(tmp_path / "no" / "such" / "dir")
    with pytest.raises(AnalysisException) as raised:
        spark.read.format("iceberg").option("snapshot-id", "1").load(missing)
    assert str(raised.value) == PATH_OPTION_REFUSAL.format(keys="snapshot-id")


@pytest.mark.parametrize("key", ["foo", "split-size"])
def test_load_path_ignores_an_unrelated_option(
    spark: ReparkSession, loaded: dict[str, object], key: str
) -> None:
    """An option outside the refused sets and the semantic gate is ignored beside a path.

    pins: dfload-1/C-004
    """
    _assert_current(spark.read.format("iceberg").option(key, "1").load(str(loaded["table_dir"])))


@pytest.mark.parametrize("key", ["compression", "mergeSchema"])
def test_load_path_semantic_gate_refuses_first(
    spark: ReparkSession, loaded: dict[str, object], key: str
) -> None:
    """The reader's semantic-option gate refuses its keys before the path door runs.

    pins: dfload-1/C-004
    """
    with pytest.raises(AnalysisException) as raised:
        spark.read.format("iceberg").option(key, "1").load(str(loaded["table_dir"]))
    assert str(raised.value) == (
        f"reader option '{key}' is not supported by repark yet "
        "(would silently change load semantics if ignored)"
    )


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        (
            "as-of-timestamp",
            "1",
            "Time travel option `as-of-timestamp` (in millis) is no longer supported, "
            "use Spark built-in `timestampAsOf` instead (properly formatted timestamp)",
        ),
        (
            "tag",
            "main",
            "Time travel option `tag` is no longer supported, "
            "use Spark built-in `versionAsOf` instead",
        ),
        ("versionAsOf", "1", "Cannot find snapshot with ID 1"),
        ("timestampAsOf", "1", "Cannot find a snapshot older than 1970-01-01T00:00:01+00:00"),
    ],
)
def test_load_identifier_travel_option_keeps_catalog_route(
    spark: ReparkSession, loaded: dict[str, object], key: str, value: str, message: str
) -> None:
    """Catalog time-travel options on ``ns.events`` keep the catalog route's own answers.

    pins: dfload-1/C-006
    """
    with pytest.raises(IllegalArgumentException) as raised:
        spark.read.format("iceberg").option(key, value).load("ns.events")
    assert str(raised.value) == message


def test_load_identifier_branch_keeps_catalog_route(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``branch=main`` on ``ns.events`` reads the current rows through the catalog route.

    pins: dfload-1/C-006
    """
    _assert_current(spark.read.format("iceberg").option("branch", "main").load("ns.events"))


@pytest.mark.parametrize("selector", ["history", "files"])
def test_load_metadata_table_selectors_keep_catalog_route(
    spark: ReparkSession, loaded: dict[str, object], selector: str
) -> None:
    """``ns.events.<selector>`` keeps the catalog route's measured table-not-found refusal.

    pins: dfload-1/C-006
    """
    with pytest.raises(AnalysisException) as raised:
        spark.read.format("iceberg").load(f"ns.events.{selector}")
    assert str(raised.value) == f"Error during planning: table 'ns.events.{selector}' not found"
