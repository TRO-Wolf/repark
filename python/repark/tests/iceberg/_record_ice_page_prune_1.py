"""Record the ICE-PAGE-PRUNE-1 Spark oracle — compacted truth plus fixture warehouses.

NOT a ``test_`` module: pytest never collects it. It builds the five page-prune
tables on live PySpark in a staging Hadoop warehouse, records every predicate
answer, cross-checks the answers cell for cell against the fork lane's truth
(``/tmp/oc-worker/pb-oracle/page_prune_truth.json``), rewrites each table with
``CALL …rewrite_table_path`` to the canonical prefix
``/tmp/repark-ice-page-prune-1/wh``, copies the canonical tables into
``fixtures/ice_page_prune_1/``, and writes the compacted
``fixtures/ice_page_prune_1/truth.json``. The pin test copies the fixtures back
to the same canonical paths before ``register_table``, so manifest file URIs
stay valid.

Seeds, table properties and predicates match the fork lane's recorder
(``/tmp/oc-worker/pb-oracle/record_page_prune.py``) exactly: five tables
``base_v2``, ``base_v3``, ``del_v2``, ``del_v3`` (delete + update, v3 deletion
vectors) and ``evo_v2`` (type promotion int to bigint, float to double, decimal
widen, rename, drop/re-add, add column); ``write.parquet.page-row-limit`` 100
and small row groups so every file carries many pages; 21 predicates including
NaN, all-null pages, truncated-string prefixes and timestamp ranges; v3 answers
carry ``_row_id`` and ``_last_updated_sequence_number``.

Row lists are stored as inclusive id runs; :func:`expand_cell` is the pinned
decoder the pin test uses. Re-running re-derives every cell from live Spark and
exits non-zero on drift; it rewrites the truth JSON and the fixtures only when
``--rewrite`` is passed, so routine runs verify rather than launder.

Run it (needs a JVM and ``pyspark`` 4.1.2, one JVM at a time)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        /tmp/oc-worker/_lib/jvm-lock.sh /tmp/sparkenv/bin/python \\
        python/repark/tests/_record_ice_page_prune_1.py --rewrite

Spark basis: ``local[4]``, UTC, shuffle partitions 1, UI off,
``org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`` (see
``_oracle_pins``).

pins: ice-page-prune-1/C-001, C-002
"""

from __future__ import annotations

import csv
import json
import shutil
import sys
from pathlib import Path
from typing import Any

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

_STAGE_ROOT = Path("/tmp/repark-ice-page-prune-1-stage")
_STAGE_WAREHOUSE = _STAGE_ROOT / "wh"
_CANONICAL_ROOT = Path("/tmp/repark-ice-page-prune-1")
_CANONICAL_WAREHOUSE = _CANONICAL_ROOT / "wh"
_CATALOG = "rec"
_HERE = Path(__file__).resolve().parent
_FIXTURE_DIR = _HERE / "fixtures" / "ice_page_prune_1"
_TRUTH_PATH = _FIXTURE_DIR / "truth.json"
_FORK_TRUTH_PATH = Path("/tmp/oc-worker/pb-oracle/page_prune_truth.json")
_TABLES = ("base_v2", "base_v3", "del_v2", "del_v3", "evo_v2")
PREFIX = "p" * 80
PROPS = (
    "'write.parquet.page-row-limit'='100', 'write.parquet.row-group-size-bytes'='16384', "
    "'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', "
    "'write.merge.mode'='merge-on-read', 'write.distribution-mode'='none'"
)
QUERIES: dict[str, str] = {
    "id_range": "id BETWEEN 400 AND 520",
    "id_eq": "id = 1234",
    "id_lt": "id < 150",
    "i_gt": "i > 1800",
    "s_eq": f"s = '{PREFIX}001234'",
    "s_ge": f"s >= '{PREFIX}001900'",
    "s_starts": f"s LIKE '{PREFIX}0019%'",
    "s_not_starts": f"s NOT LIKE '{PREFIX}0%'",
    "n_is_null": "n IS NULL",
    "n_not_null": "n IS NOT NULL",
    "n_eq": "n = 900",
    "n_ne": "n != 900",
    "n_not_in": "n NOT IN (900, 901)",
    "d_isnan": "isnan(d)",
    "d_not_nan": "NOT isnan(d)",
    "d_lt": "d < 100.0",
    "d_gt": "d > 1000.0",
    "d_not_lt": "NOT (d < 1000.0)",
    "f_gt": "f > 2000.0",
    "ts_range": "ts >= TIMESTAMP '2026-01-01 00:10:00' AND ts < TIMESTAMP '2026-01-01 00:12:00'",
}
EVO_QUERIES: dict[str, str] = {
    "i_promoted_gt": "i > 1800",
    "i_promoted_big": "i > 3000000000",
    "f_promoted_gt": "f > 2000.0",
    "dec_promoted_gt": "dec > 1500.00",
    "renamed_eq": f"s2 = '{PREFIX}001234'",
    "added_is_null": "addc IS NULL",
    "added_eq": "addc = 7",
    "added_not_null": "addc IS NOT NULL",
    "readded_is_null": "n IS NULL",
    "readded_eq": "n = 900",
}
_TABLE_QUERIES: dict[str, dict[str, str]] = {
    "base_v2": QUERIES,
    "base_v3": QUERIES,
    "del_v2": QUERIES,
    "del_v3": QUERIES,
    "evo_v2": EVO_QUERIES,
}
_LINEAGE_TABLES = frozenset({"base_v3", "del_v3"})


def _spark_session() -> Any:
    """The recorded basis, built once over the staging warehouse."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[4]")
        .appName("repark-ice-page-prune-1-record")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.jars.ivy", "/tmp/oc-worker/ice-rating/scratch/.ivy2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{_CATALOG}.warehouse", str(_STAGE_WAREHOUSE))
        .getOrCreate()
    )


def _seed_table(spark: Any, table: str, version: str) -> None:
    """One 2000-row seed table with NaN, null and truncated-string pages."""
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, i INT, f FLOAT, d DOUBLE, s STRING, n INT, "
        f"ts TIMESTAMP, dec DECIMAL(9,2)) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}', {PROPS})"
    )
    spark.sql(
        f"""INSERT INTO {table}
        SELECT id,
               CAST(id AS INT),
               CAST(id * 1.25 AS FLOAT),
               CASE WHEN id >= 700 AND id < 800 THEN CAST('NaN' AS DOUBLE) ELSE id * 1.5 END,
               concat('{PREFIX}', lpad(CAST(id AS STRING), 6, '0')),
               CASE WHEN id < 500 THEN NULL ELSE CAST(id AS INT) END,
               TIMESTAMP '2026-01-01 00:00:00' + make_interval(0, 0, 0, 0, 0, 0, id * 0.5),
               CAST(id AS DECIMAL(9,2))
        FROM range(0, 2000) ORDER BY id"""
    ).collect()


def _apply_deletes(spark: Any, table: str, version: str) -> None:
    """The delete and update legs, position deletes at v2 and vectors at v3."""
    if version == "3":
        spark.sql(f"DELETE FROM {table} WHERE id % 7 = 0 AND id < 500").collect()
        spark.sql(f"DELETE FROM {table} WHERE id % 7 = 0 AND id >= 500 AND id < 600").collect()
        spark.sql(f"DELETE FROM {table} WHERE id BETWEEN 1500 AND 1510").collect()
    else:
        spark.sql(
            f"DELETE FROM {table} WHERE (id % 7 = 0 AND id < 600) OR id BETWEEN 1500 AND 1510"
        ).collect()
    if version == "3":
        spark.sql(f"UPDATE {table} SET i = i + 1000000 WHERE id BETWEEN 1000 AND 1020").collect()


def _build_evo(spark: Any, table: str) -> None:
    """The evolution table: promote, rename, drop/re-add, add, then append."""
    _seed_table(spark, table, "2")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN i TYPE BIGINT").collect()
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN f TYPE DOUBLE").collect()
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN dec TYPE DECIMAL(18,2)").collect()
    spark.sql(f"ALTER TABLE {table} RENAME COLUMN s TO s2").collect()
    spark.sql(f"ALTER TABLE {table} DROP COLUMN n").collect()
    spark.sql(f"ALTER TABLE {table} ADD COLUMN n INT").collect()
    spark.sql(f"ALTER TABLE {table} ADD COLUMN addc INT").collect()
    spark.sql(
        f"INSERT INTO {table} SELECT id, id + 3000000000, id * 1.25, id * 1.5, "
        f"concat('{PREFIX}', lpad(CAST(id AS STRING), 6, '0')), "
        f"TIMESTAMP '2026-01-01 00:00:00', CAST(id AS DECIMAL(18,2)), "
        f"CAST(id AS INT), CAST(id % 10 AS INT) FROM range(2000, 2300) ORDER BY id"
    ).collect()


def _build_tables(spark: Any) -> dict[str, str]:
    """Create the five tables; return table key to fully qualified name."""
    if _STAGE_ROOT.exists():
        shutil.rmtree(_STAGE_ROOT)
    _STAGE_WAREHOUSE.mkdir(parents=True)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.ns")
    names: dict[str, str] = {}
    for version in ("2", "3"):
        base = f"{_CATALOG}.ns.base_v{version}"
        _seed_table(spark, base, version)
        names[f"base_v{version}"] = base
        delete_table = f"{_CATALOG}.ns.del_v{version}"
        _seed_table(spark, delete_table, version)
        _apply_deletes(spark, delete_table, version)
        names[f"del_v{version}"] = delete_table
    evo = f"{_CATALOG}.ns.evo_v2"
    _build_evo(spark, evo)
    names["evo_v2"] = evo
    return names


def _collect_rows(spark: Any, table: str, predicate: str, lineage: bool) -> list[list[int]]:
    """Sorted answer rows of one query as plain ints."""
    extra = ", _row_id, _last_updated_sequence_number" if lineage else ""
    query = f"SELECT id{extra} FROM {table} WHERE {predicate} ORDER BY id"
    return [[int(value) for value in row] for row in spark.sql(query).collect()]


def _record_answers(spark: Any, names: dict[str, str]) -> dict[str, dict[str, list[list[int]]]]:
    """Every predicate answer plus the unfiltered read, per table key."""
    raw: dict[str, dict[str, list[list[int]]]] = {}
    for key, table in names.items():
        lineage = key in _LINEAGE_TABLES
        cells: dict[str, list[list[int]]] = {}
        for name, predicate in _TABLE_QUERIES[key].items():
            cells[name] = _collect_rows(spark, table, predicate, lineage)
        extra = ", _row_id, _last_updated_sequence_number" if lineage else ""
        cells["_unfiltered"] = [
            [int(value) for value in row]
            for row in spark.sql(f"SELECT id{extra} FROM {table} ORDER BY id").collect()
        ]
        raw[key] = cells
    return raw


def _runs(values: list[int]) -> list[list[int]]:
    """Ascending ints as inclusive runs."""
    runs: list[list[int]] = []
    for value in values:
        if runs and value == runs[-1][1] + 1:
            runs[-1][1] = value
        else:
            runs.append([value, value])
    return runs


def encode_cell(rows: list[list[int]], lineage: bool) -> dict[str, Any]:
    """One answer as id runs, or row-id and sequence segments on v3."""
    ids = [row[0] for row in rows]
    assert ids == sorted(ids), "answers arrive ordered by id"
    assert len(set(ids)) == len(ids), "ids are unique"
    if not lineage:
        return {"ids": _runs(ids)}
    segments: list[list[int]] = []
    for row in rows:
        if (
            segments
            and row[0] == segments[-1][1] + 1
            and row[1] == segments[-1][2] + (row[0] - segments[-1][0])
            and row[2] == segments[-1][3]
        ):
            segments[-1][1] = row[0]
        else:
            segments.append([row[0], row[0], row[1], row[2]])
    return {"segments": segments}


def expand_cell(cell: dict[str, Any]) -> list[list[int]]:
    """The pinned decoder: runs and segments back to ordered answer rows."""
    if "segments" not in cell:
        rows: list[list[int]] = []
        for start, end in cell["ids"]:
            rows.extend([[value] for value in range(start, end + 1)])
        return rows
    expanded: list[list[int]] = []
    for id_start, id_end, row_start, sequence in cell["segments"]:
        for offset in range(id_end - id_start + 1):
            expanded.append([id_start + offset, row_start + offset, sequence])
    return expanded


def _encode_answers(raw: dict[str, dict[str, list[list[int]]]]) -> dict[str, dict[str, Any]]:
    """Every cell of every table in run form."""
    return {
        key: {name: encode_cell(rows, key in _LINEAGE_TABLES) for name, rows in cells.items()}
        for key, cells in raw.items()
    }


def _fork_rows() -> dict[str, dict[str, list[list[int]]]]:
    """The fork lane's recorded rows, plain ints, keyed by table and cell."""
    truth = json.loads(_FORK_TRUTH_PATH.read_text(encoding="utf-8"))
    return {
        key: {
            name: [[int(value) for value in row] for row in cell["rows"]]
            for name, cell in entry["answers"].items()
        }
        for key, entry in truth["tables"].items()
    }


def _compare_fork_truth(raw: dict[str, dict[str, list[list[int]]]]) -> None:
    """Fail unless the live answers agree with the fork lane's truth cell for cell."""
    fork = _fork_rows()
    assert set(fork) == set(raw), f"table keys differ: {sorted(fork)} vs {sorted(raw)}"
    mismatches: list[str] = []
    for key, cells in raw.items():
        assert set(fork[key]) == set(cells), f"cells differ on {key}"
        for name, rows in cells.items():
            if fork[key][name] != rows:
                fork_count = len(fork[key][name])
                mismatches.append(f"{key}/{name}: fork rows={fork_count} live={len(rows)}")
    if mismatches:
        raise SystemExit("FORK TRUTH MISMATCH:\n" + "\n".join(mismatches))
    print(f"live answers agree with {_FORK_TRUTH_PATH} cell for cell")


def _rewrite_table(spark: Any, table: str) -> None:
    """Rewrite one staging table to the canonical prefix and copy its live files."""
    staging = _STAGE_ROOT / "rewrite" / table
    row = spark.sql(
        f"CALL {_CATALOG}.system.rewrite_table_path(table => '{_CATALOG}.ns.{table}', "
        f"source_prefix => '{_STAGE_WAREHOUSE}', target_prefix => '{_CANONICAL_WAREHOUSE}', "
        f"staging_location => '{staging}')"
    ).collect()[0]
    file_list = row.file_list_location.replace("file:", "")
    parts = sorted(Path(file_list).glob("*.csv")) if Path(file_list).is_dir() else [Path(file_list)]
    dest_root = _CANONICAL_WAREHOUSE / "ns" / table
    if dest_root.exists():
        shutil.rmtree(dest_root)
    for part in parts:
        with part.open(newline="", encoding="utf-8") as handle:
            for source, target in csv.reader(handle):
                clean_target = target.replace("file:", "")
                relative = clean_target.removeprefix(str(_CANONICAL_WAREHOUSE) + "/")
                dest = _CANONICAL_WAREHOUSE / relative
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source.replace("file:", ""), dest)
    latest = str(row.latest_version)
    number = latest[1:].split(".", 1)[0] if latest.startswith("v") else latest
    hint = _CANONICAL_WAREHOUSE / "ns" / table / "metadata" / "version-hint.text"
    hint.write_text(number, encoding="utf-8")


def _copy_fixtures() -> None:
    """Copy the five canonical tables into the fixture dir, keeping truth and map."""
    for table in _TABLES:
        src = _CANONICAL_WAREHOUSE / "ns" / table
        dest = _FIXTURE_DIR / table
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dest)


def _payload(encoded: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """The truth JSON document."""
    return {
        "oracle": "PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, local[4], UTC",
        "tables": {
            "base_v2": "ids 0..1999 ordered, NaN d on 700..799, NULL n below 500",
            "base_v3": "same rows as base_v2 at format-version 3",
            "del_v2": "base rows minus (id % 7 = 0 below 600) and 1500..1510, position deletes",
            "del_v3": "base rows minus id % 7 = 0 below 500, below 600 and 1500..1510, "
            "deletion vectors, i + 1000000 on 1000..1020",
            "evo_v2": "seed then i to BIGINT, f to DOUBLE, dec to (18,2), s to s2, "
            "DROP n, ADD n, ADD addc, append ids 2000..2299",
        },
        "answers": encoded,
    }


def _report_drift(recorded: dict[str, Any], payload: dict[str, Any]) -> None:
    """Print every truth-vs-live cell difference."""
    for key, cells in payload["answers"].items():
        for name, live in cells.items():
            old = recorded["answers"].get(key, {}).get(name)
            if old != live:
                print(f"  {key}/{name}: truth={old} live={live}")


def main(argv: list[str]) -> int:
    """Build, record, cross-check the fork truth, verify or rewrite the pins."""
    spark = _spark_session()
    try:
        names = _build_tables(spark)
        raw = _record_answers(spark, names)
        _compare_fork_truth(raw)
        if "--rewrite" in argv:
            for table in _TABLES:
                _rewrite_table(spark, table)
    finally:
        spark.stop()
    payload = _payload(_encode_answers(raw))
    if _TRUTH_PATH.exists() and "--rewrite" not in argv:
        recorded = json.loads(_TRUTH_PATH.read_text(encoding="utf-8"))
        if recorded["answers"] == payload["answers"]:
            print("oracle matches the checked-in truth")
            return 0
        print("ORACLE DRIFT versus the checked-in truth:")
        _report_drift(recorded, payload)
        return 1
    _FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    _TRUTH_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _copy_fixtures()
    print(f"wrote {_TRUTH_PATH} and {_FIXTURE_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
