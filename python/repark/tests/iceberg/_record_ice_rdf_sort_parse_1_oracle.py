"""Record the ICE-RDF-SORT-PARSE-1 Spark oracle — in-file row order after a sorted rewrite.

NOT a ``test_`` module: pytest never collects it. The three inventory cells this unit
closes (``P-RDF-SORT``, ``P-RDF-SORT-TABLE-ORDER``, ``P-RDF-ZORDER``) record the same
output row, the same ``files_after`` and the same row set on Spark — the only thing that
differs is the order of rows *inside* each rewritten file, which the inventory harness
never reads. A bin-pack implementation that ignored ``strategy`` and ``sort_order``
entirely would make all three cells EQUAL. So this driver records what the harness
cannot: the id sequence of every rewritten data file, read straight out of parquet.

Five fixtures:

``sort``
    the cells' fixture — identity-``cat``-partitioned, three appends, 8 rows — rewritten
    with ``sort_order => 'id DESC NULLS LAST'``.
``tableorder``
    the same shape but with the order declared on the table and no ``sort_order``
    argument, so the fallback to the table's own order is measured rather than assumed.
``zorder``
    the same shape rewritten with ``sort_order => 'zorder(id, data)'``.
``zdisc``
    an unpartitioned fixture built so that ``zorder(p, q)``, ``zorder(q, p)`` and a plain
    ``id DESC`` all land in three different orders. Without it the z-order pin is vacuous: a
    rewriter that interleaved the columns in the wrong order, or that just sorted by the
    first column, would still "differ from ``id DESC``". Getting there took a measurement:
    the obvious fixture — a BIGINT ``id`` beside a single-character STRING — does **not**
    discriminate, because a long's leading bytes are constant across small values while the
    string's first byte varies, so the string dominates the interleave and swapping the
    argument order changes nothing. Both z-order columns are therefore single-character
    strings drawn from ``A``..``D``, whose low three bits are the only varying bits in
    either key; the interleave then reads ``p q p q p q`` and swapping the pair genuinely
    reverses which column is the more significant.
``nulls``
    one NULL ``id`` rewritten with a bare ``id DESC`` and no ``NULLS`` clause, so the
    direction-tied default (DESC → NULLS LAST) is measured at runtime and not only in the
    parser's unit tests.

Every cell also records its post-rewrite table metadata: the ``sort-orders`` array, the
``default-sort-order-id`` (the rewrite is a one-shot instruction and must not move them),
and the current snapshot's ``operation`` (resolved by ``current-snapshot-id`` — the
``snapshots`` array order is not a clock).

Run it (one JVM at a time)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        /tmp/sparkenv/bin/python \\
        python/repark/tests/_record_ice_rdf_sort_parse_1_oracle.py --rewrite

Re-running it re-derives every cell from live Spark and exits non-zero on drift; it
rewrites the truth JSON only when ``--rewrite`` is passed, so routine runs verify rather
than launder.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from typing import Any

_ROOT = Path("/tmp/repark-ice-rdf-sort-parse-1")
_WAREHOUSE = _ROOT / "wh"
_CATALOG = "rec"
_HERE = Path(__file__).resolve().parent
_TRUTH_PATH = _HERE / "ice_rdf_sort_parse_1_spark_oracle.json"

APPENDS = (
    ((1, "a", "x"), (2, "b", "y"), (3, "c", "x")),
    ((4, "d", "x"), (6, "f", "x")),
    ((5, "e", "y"), (7, "g", "x"), (8, "h", "x")),
)
ZDISC_ROWS = (
    (1, "A", "D"),
    (2, "D", "A"),
    (3, "B", "C"),
    (4, "C", "B"),
    (5, "A", "A"),
    (6, "D", "D"),
    (7, "B", "D"),
    (8, "C", "A"),
)
NULL_ROWS = ((3, "c", "c"), (None, "n", "n"), (1, "a", "a"), (8, "h", "h"), (5, "e", "e"))
OUT_COLUMNS = (
    "rewritten_data_files_count",
    "added_data_files_count",
    "rewritten_bytes_count",
    "failed_data_files_count",
    "removed_delete_files_count",
)


def _spark_session() -> Any:
    """The recorded basis, built once."""
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[4]")
        .appName("repark-ice-rdf-sort-parse-1-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.jars.ivy", "/tmp/ic-build/.ivy2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{_CATALOG}.warehouse", str(_WAREHOUSE))
        .getOrCreate()
    )


def _literal(value: Any) -> str:
    """One SQL literal for an int, a string or a NULL."""
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return f"'{value}'"
    return str(value)


def _insert(spark: Any, table: str, rows: tuple[tuple[Any, ...], ...]) -> None:
    """One INSERT, one snapshot."""
    values = ", ".join("(" + ", ".join(_literal(cell) for cell in row) + ")" for row in rows)
    spark.sql(f"INSERT INTO {table} VALUES {values}")


def _file_ids(spark: Any, table: str) -> list[dict[str, Any]]:
    """Every live data file's partition and its ids in parquet row order."""
    import pyarrow.parquet as pq

    files = spark.sql(f"SELECT * FROM {table}.files")
    partitioned = "partition" in files.columns
    columns = "file_path, record_count" + (", partition" if partitioned else "")
    rows = spark.sql(f"SELECT {columns} FROM {table}.files").collect()
    out = []
    for row in rows:
        path = row["file_path"]
        local = path[len("file:") :] if path.startswith("file:") else path
        ids = pq.read_table(local, columns=["id"]).column("id").to_pylist()
        partition = row["partition"].asDict() if partitioned else None
        out.append(
            {
                "partition": partition,
                "record_count": int(row["record_count"]),
                "ids": ids,
            }
        )
    return sorted(out, key=lambda entry: str(entry["partition"]))


def _out_row(spark: Any, call: str) -> list[Any]:
    """The procedure's single output row, with the byte count reduced to a sign."""
    row = spark.sql(call).collect()[0]
    values = [row[name] for name in OUT_COLUMNS]
    values[2] = ">0" if int(values[2]) > 0 else "0"
    return values


def _current_meta(meta_dir: Path) -> dict[str, Any]:
    """The newest metadata.json, resolved by version number."""
    hint = meta_dir / "version-hint.text"
    if hint.exists():
        pointed = meta_dir / f"v{hint.read_text().strip()}.metadata.json"
        if pointed.exists():
            return json.loads(pointed.read_text())
    numbered = {}
    for path in meta_dir.glob("v*.metadata.json"):
        try:
            numbered[int(path.name[1:].removesuffix(".metadata.json"))] = path
        except ValueError:
            continue
    if numbered:
        return json.loads(numbered[max(numbered)].read_text())
    metas = sorted(meta_dir.glob("*.metadata.json"))
    return json.loads(metas[-1].read_text())


def _table_meta(table: str) -> dict[str, Any]:
    """The sort state and the current snapshot's operation from metadata.json."""
    meta = _current_meta(_WAREHOUSE / table.split(".", 1)[1].replace(".", "/") / "metadata")
    current = next(
        snap for snap in meta["snapshots"] if snap["snapshot-id"] == meta["current-snapshot-id"]
    )
    return {
        "sort_orders": meta["sort-orders"],
        "default_sort_order_id": meta["default-sort-order-id"],
        "operation": current["summary"]["operation"],
    }


def _cell_table(spark: Any, name: str, order_ddl: str | None) -> str:
    """The cells' fixture: cat-partitioned, three appends, 8 rows."""
    table = f"{_CATALOG}.ns.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) "
        "USING iceberg PARTITIONED BY (cat)"
    )
    if order_ddl is not None:
        spark.sql(f"ALTER TABLE {table} {order_ddl}")
    for batch in APPENDS:
        _insert(spark, table, batch)
    return table


def _flat_table(spark: Any, name: str, rows: tuple[tuple[Any, ...], ...]) -> str:
    """An unpartitioned two-column fixture, one row per append."""
    table = f"{_CATALOG}.ns.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} (id BIGINT, p STRING, q STRING) USING iceberg")
    for row in rows:
        _insert(spark, table, (row,))
    return table


def _rewrite(spark: Any, table: str, sort_order: str | None, strategy: str | None) -> list[Any]:
    """One rewrite_data_files CALL with rewrite-all, returning its output row."""
    args = [f"table => '{table.split('.', 1)[1]}'"]
    if strategy is not None:
        args.append(f"strategy => '{strategy}'")
    if sort_order is not None:
        args.append(f"sort_order => '{sort_order}'")
    args.append("options => map('rewrite-all', 'true')")
    return _out_row(spark, f"CALL {_CATALOG}.system.rewrite_data_files({', '.join(args)})")


def _record(spark: Any) -> dict[str, Any]:
    """Every fixture's measured truth."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.ns")
    truth: dict[str, Any] = {}

    table = _cell_table(spark, "sort", None)
    truth["sort"] = {
        "out": _rewrite(spark, table, "id DESC NULLS LAST", "sort"),
        "files": _file_ids(spark, table),
        **_table_meta(table),
    }

    table = _cell_table(spark, "tableorder", "WRITE ORDERED BY (id DESC NULLS LAST)")
    truth["tableorder"] = {
        "out": _rewrite(spark, table, None, "sort"),
        "files": _file_ids(spark, table),
        **_table_meta(table),
    }

    table = _cell_table(spark, "zorder", None)
    truth["zorder"] = {
        "out": _rewrite(spark, table, "zorder(id, data)", "sort"),
        "files": _file_ids(spark, table),
        **_table_meta(table),
    }

    for key, order in (
        ("zdisc_id_desc", "id DESC"),
        ("zdisc_zorder_p_q", "zorder(p, q)"),
        ("zdisc_zorder_q_p", "zorder(q, p)"),
    ):
        table = _flat_table(spark, key, ZDISC_ROWS)
        truth[key] = {
            "out": _rewrite(spark, table, order, "sort"),
            "files": _file_ids(spark, table),
            **_table_meta(table),
        }

    table = _flat_table(spark, "nulls", NULL_ROWS)
    truth["nulls"] = {
        "out": _rewrite(spark, table, "id DESC", "sort"),
        "files": _file_ids(spark, table),
        **_table_meta(table),
    }
    return truth


def main() -> int:
    """Record or verify the oracle; return the process exit code."""
    sys.path.insert(0, str(_HERE))
    rewrite = "--rewrite" in sys.argv
    if _ROOT.exists():
        shutil.rmtree(_ROOT)
    _WAREHOUSE.mkdir(parents=True)
    spark = _spark_session()
    try:
        truth = _record(spark)
    finally:
        spark.stop()
    rendered = json.dumps(truth, indent=2, sort_keys=True) + "\n"
    if rewrite or not _TRUTH_PATH.exists():
        _TRUTH_PATH.write_text(rendered)
        print(f"wrote {_TRUTH_PATH}")
        return 0
    if _TRUTH_PATH.read_text() != rendered:
        print("DRIFT: live Spark no longer matches the recorded oracle", file=sys.stderr)
        return 1
    print("oracle verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
