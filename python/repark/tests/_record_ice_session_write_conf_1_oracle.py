"""Record or re-check the ICE-SESSION-WRITE-CONF-1 Spark oracle cells on live PySpark.

Usage (PySpark 4.1.2 interpreter, e.g. ``/tmp/sparkenv/bin/python``)::

    /tmp/sparkenv/bin/python python/repark/tests/_record_ice_session_write_conf_1_oracle.py \\
        --warehouse /tmp/session-conf-oracle-wh record
    ... check

``record`` prints the fixture JSON to stdout; ``check`` re-derives every cell
and exits non-zero naming the first mismatch against the committed
``ice_session_write_conf_1_spark_oracle.json``. Error cells compare by status
plus a stable message needle (Spark stage and task ids vary per run); the
``QR-*`` collision cells compare by their whole first message line. The QS / QZ
/ QR path cells are derived by
:mod:`_record_ice_session_write_conf_1_paths`. The
Iceberg runtime GAV comes from :mod:`_oracle_pins` (CP-8: never restate a
version literal).

pins: ice-session-write-conf-1/C-001, C-002, C-036, C-037
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
from _record_ice_session_write_conf_1_paths import derive_path_cells

FIXTURE = Path(__file__).with_name("ice_session_write_conf_1_spark_oracle.json")
ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

BOGUS_CODEC_NEEDLE = "Unsupported compression codec: bogus"
SET_SYNTAX_NEEDLE = "INVALID_SET_SYNTAX"

MOR_PROPS = (
    ", 'write.delete.mode'='merge-on-read'"
    ", 'write.update.mode'='merge-on-read'"
    ", 'write.merge.mode'='merge-on-read'"
)

Statement = Callable[[Any, str, str, str], None]


def _spark_session(warehouse: Path) -> Any:
    """Build the dual-catalog Spark session the oracle cells run on."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("ice-session-write-conf-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", ICEBERG_SPARK_EXTENSIONS)
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .config("spark.sql.catalog.hc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hc.type", "hadoop")
        .config("spark.sql.catalog.hc.warehouse", str(warehouse / "hc"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    return builder.getOrCreate()


def _names(cell_id: str, catalog: str) -> tuple[str, str, str]:
    """Derive the harness table, second table and short names for one cell."""
    safe = re.sub(r"[^a-z0-9]", "_", cell_id.lower())
    return f"{catalog}.ns.t_{safe}", f"{catalog}.ns.u_{safe}", f"ns.t_{safe}"


def _summaries(spark: Any, table: str) -> list[dict[str, Any]]:
    """Read snapshot summaries oldest-first for one table."""
    rows = spark.sql(
        f"SELECT operation, summary, committed_at FROM {table}.snapshots ORDER BY committed_at"
    ).collect()
    return [{"operation": row["operation"], **dict(row["summary"])} for row in rows]


def _data_rows(spark: Any, table: str) -> list[list[Any]]:
    """Read all rows of one table in repr order."""
    return sorted((list(row) for row in spark.sql(f"SELECT * FROM {table}").collect()), key=repr)


def _file_codecs(spark: Any, table: str) -> list[list[str]]:
    """Read the footer codec set of every live file, sorted."""
    import pyarrow.parquet as parquet

    paths = [row[0] for row in spark.sql(f"SELECT file_path FROM {table}.all_files").collect()]
    out = []
    for path in paths:
        handle = parquet.ParquetFile(path.replace("file:", ""))
        codecs = {
            handle.metadata.row_group(index).column(column).compression
            for index in range(handle.metadata.num_row_groups)
            for column in range(handle.metadata.num_columns)
        }
        out.append(sorted(codecs))
    return sorted(out)


def _error_info(error: BaseException) -> dict[str, str]:
    """Reduce a Spark failure to its stable type plus trimmed message."""
    message = re.sub(r"\n\s*(JVM stacktrace|at |\tat ).*", "", str(error).strip(), flags=re.S)
    return {"type": type(error).__name__, "msg": message[:700]}


def _sp_run(
    spark: Any,
    cell_id: str,
    conf: dict[str, str],
    body: Statement,
    observed: tuple[str, ...] = ("team",),
    create_extra: str = "",
) -> dict[str, Any]:
    """Run one snapshot-property cell and record its Spark answer."""
    table, table_two, short = _names(cell_id, "sc")
    record: dict[str, Any] = {"id": cell_id}
    applied = []
    try:
        for key, value in conf.items():
            spark.conf.set(key, value)
            applied.append(key)
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
            f"TBLPROPERTIES ('format-version'='2'{create_extra})"
        )
        body(spark, table, table_two, short)
        summaries = _summaries(spark, table)
        record["status"] = "ok"
        record["obs"] = {
            "ops+keys": [
                [summary.get("operation")] + [summary.get(key) for key in observed]
                for summary in summaries
            ],
            "data": _data_rows(spark, table),
        }
        if cell_id == "SP-CTAS":
            ctas = _summaries(spark, table_two)
            record["obs"]["ctas"] = [summary.get("team") for summary in ctas]
        return record
    except Exception as error:
        record["status"] = "error"
        record["error"] = _error_info(error)
        record["obs"] = {}
        return record
    finally:
        for key in applied:
            with suppress(Exception):
                spark.conf.unset(key)
        for name in (table, table_two):
            with suppress(Exception):
                spark.sql(f"DROP TABLE IF EXISTS {name}")


def _cz_run(
    spark: Any,
    cell_id: str,
    conf: dict[str, str],
    body: Statement,
    props: str = "",
) -> dict[str, Any]:
    """Run one compression-codec cell and record its Spark answer."""
    table, table_two, short = _names(cell_id, "hc")
    record: dict[str, Any] = {"id": cell_id}
    applied = []
    try:
        for key, value in conf.items():
            spark.conf.set(key, value)
            applied.append(key)
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT, s STRING) USING iceberg "
            f"TBLPROPERTIES ('format-version'='2'{props})"
        )
        body(spark, table, table_two, short)
        record["status"] = "ok"
        record["obs"] = {"codec": _file_codecs(spark, table)}
        return record
    except Exception as error:
        record["status"] = "error"
        record["error"] = _error_info(error)
        record["obs"] = {}
        return record
    finally:
        for key in applied:
            with suppress(Exception):
                spark.conf.unset(key)
        for name in (table, table_two):
            with suppress(Exception):
                spark.sql(f"DROP TABLE IF EXISTS {name}")


def _insert_rows(spark: Any, table: str, table_two: str, short: str) -> None:
    """Insert the shared two-row seed."""
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")


def _df_one_row(spark: Any) -> Any:
    """Build the shared one-row DataFrame."""
    return spark.createDataFrame([(1, "a", "x")], "id BIGINT, data STRING, cat STRING")


def _cz_df_rows(spark: Any) -> Any:
    """Build the shared fifty-row codec DataFrame."""
    return spark.createDataFrame(
        [(index, f"v{index}") for index in range(50)], "id BIGINT, s STRING"
    )


def _cz_insert_sql(spark: Any, table: str, table_two: str, short: str) -> None:
    """Insert fifty rows through Spark SQL."""
    spark.sql(f"INSERT INTO {table} SELECT id, concat('v', id) FROM range(50)")


def _body_df_append(spark: Any, table: str, table_two: str, short: str) -> None:
    """Append one row through writeTo."""
    _df_one_row(spark).writeTo(table).append()


def _body_df_saveastable(spark: Any, table: str, table_two: str, short: str) -> None:
    """Append one row through format iceberg saveAsTable."""
    _df_one_row(spark).write.format("iceberg").mode("append").saveAsTable(table)


def _body_df_option_wins(spark: Any, table: str, table_two: str, short: str) -> None:
    """Append one row with a winning writer option."""
    _df_one_row(spark).writeTo(table).option("snapshot-property.team", "opt").append()


def _body_delete(spark: Any, table: str, table_two: str, short: str) -> None:
    """Seed two rows then delete one."""
    _insert_rows(spark, table, table_two, short)
    spark.sql(f"DELETE FROM {table} WHERE id = 1")


def _body_update(spark: Any, table: str, table_two: str, short: str) -> None:
    """Seed two rows then update one."""
    _insert_rows(spark, table, table_two, short)
    spark.sql(f"UPDATE {table} SET data = 'z' WHERE id = 1")


def _body_merge(spark: Any, table: str, table_two: str, short: str) -> None:
    """Seed two rows then merge one matched row."""
    _insert_rows(spark, table, table_two, short)
    spark.sql(
        f"MERGE INTO {table} t USING (SELECT 1 AS id, 'm' AS data, 'x' AS cat) s "
        "ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )


def _body_overwrite(spark: Any, table: str, table_two: str, short: str) -> None:
    """Seed two rows then overwrite the table."""
    _insert_rows(spark, table, table_two, short)
    spark.sql(f"INSERT OVERWRITE {table} VALUES (9, 'z', 'x')")


def _body_ctas(spark: Any, table: str, table_two: str, short: str) -> None:
    """Create the second table from a one-row query."""
    spark.sql(f"CREATE TABLE {table_two} USING iceberg AS SELECT 1 AS id")


def _body_call_rdf(spark: Any, table: str, table_two: str, short: str) -> None:
    """Seed three rows then run rewrite_data_files."""
    _insert_rows(spark, table, table_two, short)
    spark.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x')")
    spark.sql(f"CALL sc.system.rewrite_data_files(table => '{short}')")


def _body_rollback(spark: Any, table: str, table_two: str, short: str) -> None:
    """Seed two rows then roll back to the first snapshot."""
    _insert_rows(spark, table, table_two, short)
    first = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at LIMIT 1"
    ).collect()[0][0]
    spark.sql(f"CALL sc.system.rollback_to_snapshot('{short}', {first})")


def _body_set_sql(spark: Any, table: str, table_two: str, short: str) -> None:
    """SET the conf, insert, then RESET it."""
    spark.sql("SET spark.sql.iceberg.snapshot-property.team = s")
    _insert_rows(spark, table, table_two, short)
    spark.sql("RESET spark.sql.iceberg.snapshot-property.team")


def _body_cz_opt_over_conf(spark: Any, table: str, table_two: str, short: str) -> None:
    """Append fifty rows with a winning codec option."""
    _cz_df_rows(spark).writeTo(table).option("compression-codec", "snappy").append()


def _body_cz_delete_cow(spark: Any, table: str, table_two: str, short: str) -> None:
    """Insert fifty rows then delete ten."""
    _cz_insert_sql(spark, table, table_two, short)
    spark.sql(f"DELETE FROM {table} WHERE id < 10")


def _body_cz_rdf(spark: Any, table: str, table_two: str, short: str) -> None:
    """Insert rows then rewrite all files."""
    _cz_insert_sql(spark, table, table_two, short)
    spark.sql(f"INSERT INTO {table} SELECT id, 'w' FROM range(5)")
    spark.sql(
        f"CALL hc.system.rewrite_data_files(table => '{short}', "
        "options => map('rewrite-all', 'true'))"
    )


def _derive(spark: Any) -> list[dict[str, Any]]:
    """Derive every oracle cell on the live session."""
    c1 = {"spark.sql.iceberg.snapshot-property.team": "a"}
    cells: list[dict[str, Any]] = [
        _sp_run(spark, "SP-INSERT", c1, _insert_rows),
        _sp_run(
            spark,
            "SP-TWO-KEYS",
            {
                "spark.sql.iceberg.snapshot-property.team": "a",
                "spark.sql.iceberg.snapshot-property.run": "r1",
            },
            _insert_rows,
            observed=("team", "run"),
        ),
        _sp_run(spark, "SP-DF-APPEND", c1, _body_df_append),
        _sp_run(spark, "SP-DF-SAVEASTABLE", c1, _body_df_saveastable),
        _sp_run(spark, "SP-DF-OPTION-WINS", c1, _body_df_option_wins),
        _sp_run(spark, "SP-DELETE-COW", c1, _body_delete),
        _sp_run(spark, "SP-DELETE-MOR", c1, _body_delete, create_extra=MOR_PROPS),
        _sp_run(spark, "SP-UPDATE", c1, _body_update),
        _sp_run(spark, "SP-MERGE", c1, _body_merge),
        _sp_run(spark, "SP-OVERWRITE", c1, _body_overwrite),
        _sp_run(spark, "SP-CTAS", c1, _body_ctas),
        _sp_run(spark, "SP-CALL-RDF", c1, _body_call_rdf),
        _sp_run(spark, "SP-CALL-EXPIRE-NOSNAP", c1, _body_rollback),
        _sp_run(
            spark,
            "SP-RESERVED-OPERATION",
            {"spark.sql.iceberg.snapshot-property.operation": "fake"},
            _insert_rows,
            observed=("operation",),
        ),
        _sp_run(spark, "SP-SET-SQL", {}, _body_set_sql),
        _sp_run(
            spark,
            "SP-EMPTY-VALUE",
            {"spark.sql.iceberg.snapshot-property.team": ""},
            _insert_rows,
        ),
        _cz_run(
            spark,
            "CZ-CONF-GZIP-DF",
            {"spark.sql.iceberg.compression-codec": "gzip"},
            lambda s, t, _u, _h: _cz_df_rows(s).writeTo(t).append(),
        ),
        _cz_run(
            spark,
            "CZ-CONF-GZIP-SQL",
            {"spark.sql.iceberg.compression-codec": "gzip"},
            _cz_insert_sql,
        ),
        _cz_run(
            spark,
            "CZ-CONF-SNAPPY-SQL",
            {"spark.sql.iceberg.compression-codec": "snappy"},
            _cz_insert_sql,
        ),
        _cz_run(
            spark,
            "CZ-CONF-UPPER",
            {"spark.sql.iceberg.compression-codec": "GZIP"},
            _cz_insert_sql,
        ),
        _cz_run(
            spark,
            "CZ-CONF-OVER-PROP",
            {"spark.sql.iceberg.compression-codec": "snappy"},
            _cz_insert_sql,
            props=", 'write.parquet.compression-codec'='gzip'",
        ),
        _cz_run(
            spark,
            "CZ-OPT-OVER-CONF",
            {"spark.sql.iceberg.compression-codec": "gzip"},
            _body_cz_opt_over_conf,
        ),
        _cz_run(
            spark,
            "CZ-CONF-BOGUS",
            {"spark.sql.iceberg.compression-codec": "bogus"},
            _cz_insert_sql,
        ),
        _cz_run(
            spark,
            "CZ-CONF-DELETE-COW",
            {"spark.sql.iceberg.compression-codec": "gzip"},
            _body_cz_delete_cow,
        ),
        _cz_run(
            spark,
            "CZ-CONF-RDF",
            {"spark.sql.iceberg.compression-codec": "gzip"},
            _body_cz_rdf,
        ),
    ]
    cells.extend(derive_path_cells(spark))
    return cells


def _sorted_data(obs: dict[str, Any]) -> dict[str, Any]:
    """Return ``obs`` with its ``data`` rows in repr order, the order the fixture does not fix."""
    if "data" not in obs:
        return obs
    return {**obs, "data": sorted(obs["data"], key=repr)}


def _needle(cell_id: str, cell: dict[str, Any]) -> str:
    """The stable message needle one error cell is compared by."""
    if cell_id.startswith("QR-"):
        return cell["error"]["msg"].split("\n")[0]
    return BOGUS_CODEC_NEEDLE if cell_id == "CZ-CONF-BOGUS" else SET_SYNTAX_NEEDLE


def _checked(records: list[dict[str, Any]], fixture: dict[str, Any]) -> None:
    """Compare re-derived cells against the committed fixture."""
    expected = fixture["cells"]
    assert len(records) == len(expected), (len(records), len(expected))
    for record in records:
        cell = expected[record["id"]]
        assert record["status"] == cell["status"], record["id"]
        if record["status"] == "error":
            needle = _needle(record["id"], cell)
            assert needle in record["error"]["msg"], record["id"]
            assert needle in cell["error"]["msg"], record["id"]
        else:
            assert _sorted_data(record["obs"]) == _sorted_data(cell["obs"]), record["id"]


def main() -> None:
    """Parse arguments and run the record or check leg."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("mode", choices=("record", "check"))
    args = parser.parse_args()
    warehouse = Path(args.warehouse)
    shutil.rmtree(warehouse, ignore_errors=True)
    warehouse.mkdir(parents=True)
    spark = _spark_session(warehouse)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hc.ns")
    records = _derive(spark)
    spark.stop()
    if args.mode == "record":
        print(json.dumps(records, indent=1, default=str))
        return
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    _checked(records, fixture)
    print(f"ice-session-write-conf-1 oracle check clean ({len(records)} cells)")


if __name__ == "__main__":
    main()
