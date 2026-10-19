from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

FIXTURE_DIR = (
    Path(__file__).resolve().parents[2] / "repark-parity/fixtures/torture/data/ice_evo_dml_1"
)
TRUTH_FILE = FIXTURE_DIR / "truth.json"
ADOPTED_WAREHOUSE = Path("/tmp/repark-ice-evo-dml-1")
ADOPTED_CATALOG = "ice_evo_dml_1"
ADOPTED_NAMESPACE = "ns"
ADOPTED_TABLE = "adopt_v2"
SPARK_CATALOG = "sc"
FORMAT_VERSIONS = ("2", "3")
MODES = ("copy-on-write", "merge-on-read")
CHECK_SQL = "SELECT id, v, extra FROM {t}"

GRID_SOURCE = (
    "SELECT CAST(1 AS BIGINT) AS id, 'a2' AS v, 'x1' AS extra UNION ALL "
    "SELECT CAST(9 AS BIGINT) AS id, 'z' AS v, 'x9' AS extra"
)
ADOPTED_SOURCE = (
    "SELECT CAST(1 AS BIGINT) AS id, 'm1' AS v, 'x1' AS extra UNION ALL "
    "SELECT CAST(3 AS BIGINT) AS id, 'c' AS v, 'x3' AS extra"
)
APPEND_SOURCE = "SELECT CAST(7 AS BIGINT) AS id, 'g' AS v, 'x7' AS extra"

EVOLUTIONS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    (
        "add_column",
        "(id BIGINT, v STRING)",
        "(1, 'a'), (2, 'b')",
        ("ALTER TABLE {t} ADD COLUMN extra STRING",),
    ),
    (
        "rename_column",
        "(id BIGINT, w STRING, extra STRING)",
        "(1, 'a', 'e1'), (2, 'b', NULL)",
        ("ALTER TABLE {t} RENAME COLUMN w TO v",),
    ),
    (
        "rename_key",
        "(k BIGINT, v STRING, extra STRING)",
        "(1, 'a', 'e1'), (2, 'b', NULL)",
        ("ALTER TABLE {t} RENAME COLUMN k TO id",),
    ),
    (
        "rename_swap",
        "(id BIGINT, extra STRING, v STRING)",
        "(1, 'a', 'e1'), (2, 'b', NULL)",
        (
            "ALTER TABLE {t} RENAME COLUMN extra TO tmp",
            "ALTER TABLE {t} RENAME COLUMN v TO extra",
            "ALTER TABLE {t} RENAME COLUMN tmp TO v",
        ),
    ),
)


def merge_spec(source: str, **clauses: Any) -> dict[str, Any]:
    return {"merge": {"source": source, "key": "id", **clauses}}


GRID_STATEMENTS: tuple[tuple[str, str, dict[str, Any] | None], ...] = (
    (
        "merge_star",
        f"MERGE INTO {{t}} t USING ({GRID_SOURCE}) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        merge_spec(GRID_SOURCE, update_all=True, insert_all=True),
    ),
    (
        "merge_explicit",
        f"MERGE INTO {{t}} t USING ({GRID_SOURCE}) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET extra = s.extra "
        "WHEN NOT MATCHED THEN INSERT (id, v, extra) VALUES (s.id, s.v, s.extra)",
        merge_spec(GRID_SOURCE, update=["extra"], insert=["id", "v", "extra"]),
    ),
    (
        "merge_explicit_old_cols_only",
        f"MERGE INTO {{t}} t USING ({GRID_SOURCE}) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.v",
        merge_spec(GRID_SOURCE, update=["v"]),
    ),
    ("update_new_col", "UPDATE {t} SET extra = 'u' WHERE id = 1", None),
    ("update_old_col", "UPDATE {t} SET v = 'u' WHERE id = 1", None),
    ("delete", "DELETE FROM {t} WHERE id = 2", None),
    ("insert_new_col", "INSERT INTO {t} VALUES (7, 'g', 'x7')", {"append": APPEND_SOURCE}),
    (
        "insert_overwrite",
        f"INSERT OVERWRITE {{t}} SELECT id, v, extra FROM ({GRID_SOURCE}) s",
        {"overwrite_partitions": GRID_SOURCE},
    ),
)

L02_EVOLUTIONS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    (
        "drop_add",
        "(id BIGINT, v STRING, extra STRING)",
        "(1, 'a', 'e1'), (2, 'b', NULL)",
        (
            "ALTER TABLE {t} DROP COLUMN extra",
            "ALTER TABLE {t} ADD COLUMN extra STRING",
        ),
    ),
    (
        "rename_onto_dropped",
        "(id BIGINT, v STRING, extra STRING, w STRING)",
        "(1, 'a', 'e1', 'w1'), (2, 'b', NULL, 'w2')",
        (
            "ALTER TABLE {t} DROP COLUMN extra",
            "ALTER TABLE {t} RENAME COLUMN w TO extra",
        ),
    ),
)

L02_EVOLUTION_STATEMENTS: tuple[tuple[str, str, dict[str, Any] | None], ...] = (
    (
        "merge_star",
        f"MERGE INTO {{t}} t USING ({GRID_SOURCE}) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        merge_spec(GRID_SOURCE, update_all=True, insert_all=True),
    ),
    ("update_new_col", "UPDATE {t} SET extra = 'u' WHERE id = 1", None),
    ("delete", "DELETE FROM {t} WHERE id = 2", None),
)

ADD_DEFAULT_DDL = "ALTER TABLE {t} ADD COLUMN extra STRING DEFAULT 'd'"
ADD_DEFAULT_CHECK = "SELECT id, v FROM {t}"

MERGE_ON_EXTRA_SQL = (
    f"MERGE INTO {{t}} t USING ({GRID_SOURCE}) s ON t.extra = s.extra "
    "WHEN MATCHED THEN UPDATE SET v = s.v "
    "WHEN NOT MATCHED THEN INSERT (id, v, extra) VALUES (s.id, s.v, s.extra)"
)
MERGE_NMBS_SQL = (
    f"MERGE INTO {{t}} t USING ({GRID_SOURCE}) s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * "
    "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET extra = 'z'"
)

PREDICATE_STATEMENTS: tuple[tuple[str, str, dict[str, Any] | None], ...] = (
    ("update_where_null", "UPDATE {t} SET v = 'u' WHERE extra IS NULL", None),
    (
        "merge_on_extra",
        MERGE_ON_EXTRA_SQL,
        merge_spec(GRID_SOURCE, key="extra", update=["v"], insert=["id", "v", "extra"]),
    ),
    (
        "merge_nmbs",
        MERGE_NMBS_SQL,
        merge_spec(GRID_SOURCE, update_all=True, insert_all=True, by_source_update={"extra": "z"}),
    ),
)


def table_properties(format_version: str, mode: str) -> str:
    properties = [f"'format-version'='{format_version}'"]
    properties.extend(f"'write.{kind}.mode'='{mode}'" for kind in ("delete", "update", "merge"))
    return ", ".join(properties)


def create_sql(columns: str, format_version: str, mode: str) -> str:
    return (
        f"CREATE TABLE {{t}} {columns} USING iceberg "
        f"TBLPROPERTIES ({table_properties(format_version, mode)})"
    )


def query_step(step_id: str, sql: str) -> dict[str, Any]:
    return {"id": step_id, "kind": "query", "sql": sql, "dataframe": None}


def statement_step(
    step_id: str, sql: str, dataframe: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {"id": step_id, "kind": "statement", "sql": sql, "dataframe": dataframe}


def grid_cases() -> list[dict[str, Any]]:
    cases = []
    for evolution, columns, rows, ddl in EVOLUTIONS:
        for format_version in FORMAT_VERSIONS:
            for mode in MODES:
                for statement_id, sql, dataframe in GRID_STATEMENTS:
                    cases.append(
                        {
                            "id": f"grid/{evolution}/v{format_version}/{mode[:3]}/{statement_id}",
                            "group": "grid",
                            "format_version": format_version,
                            "mode": mode,
                            "setup": [
                                create_sql(columns, format_version, mode),
                                f"INSERT INTO {{t}} VALUES {rows}",
                                *ddl,
                            ],
                            "steps": [
                                statement_step(statement_id, sql, dataframe),
                                query_step("after", CHECK_SQL),
                            ],
                        }
                    )
    return cases


def emptied_cases() -> list[dict[str, Any]]:
    cases = []
    for format_version in FORMAT_VERSIONS:
        for mode in MODES:
            cases.append(
                {
                    "id": f"emptied/v{format_version}/{mode[:3]}",
                    "group": "emptied",
                    "format_version": format_version,
                    "mode": mode,
                    "setup": [
                        create_sql("(id BIGINT, v STRING)", format_version, mode),
                        "INSERT INTO {t} VALUES (1, 'old')",
                        "DELETE FROM {t} WHERE id = 1",
                        "ALTER TABLE {t} ADD COLUMN extra STRING",
                    ],
                    "steps": [
                        statement_step(
                            "merge_star",
                            f"MERGE INTO {{t}} t USING ({GRID_SOURCE}) s ON t.id = s.id "
                            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
                            merge_spec(GRID_SOURCE, update_all=True, insert_all=True),
                        ),
                        query_step("after", CHECK_SQL),
                    ],
                }
            )
    return cases


def rewrite_cases() -> list[dict[str, Any]]:
    cases = []
    for format_version in FORMAT_VERSIONS:
        mode = "copy-on-write"
        cases.append(
            {
                "id": f"rewrite/v{format_version}/{mode[:3]}",
                "group": "rewrite",
                "format_version": format_version,
                "mode": mode,
                "setup": [
                    create_sql("(id BIGINT, v STRING)", format_version, mode),
                    "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')",
                    "ALTER TABLE {t} ADD COLUMN extra STRING",
                    "CALL {cat}.system.rewrite_data_files(table => '{rel}')",
                ],
                "steps": [
                    statement_step(
                        "merge_star",
                        f"MERGE INTO {{t}} t USING ({GRID_SOURCE}) s ON t.id = s.id "
                        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
                        merge_spec(GRID_SOURCE, update_all=True, insert_all=True),
                    ),
                    query_step("after", CHECK_SQL),
                ],
            }
        )
    return cases


MERGE_STAR_SOURCES: tuple[tuple[str, str, bool], ...] = (
    (
        "narrow",
        "SELECT CAST(1 AS BIGINT) AS id, 'a2' AS v UNION ALL "
        "SELECT CAST(3 AS BIGINT) AS id, 'c' AS v",
        False,
    ),
    (
        "wide",
        "SELECT CAST(2 AS BIGINT) AS id, 'b2' AS v, 'x2' AS extra, 'new' AS bronze_only UNION ALL "
        "SELECT CAST(4 AS BIGINT) AS id, 'd' AS v, 'x4' AS extra, 'new' AS bronze_only",
        True,
    ),
    ("exact", "SELECT CAST(5 AS BIGINT) AS id, 'e' AS v, 'x5' AS extra", True),
    ("reordered", "SELECT 'x6' AS extra, 'f' AS v, CAST(6 AS BIGINT) AS id", True),
)


def merge_star_source_cases() -> list[dict[str, Any]]:
    cases = []
    for source_id, source_sql, succeeds in MERGE_STAR_SOURCES:
        kind = "statement" if succeeds else "statement_error"
        step = statement_step(
            f"merge_star_{source_id}",
            f"MERGE INTO {{t}} t USING ({source_sql}) s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
            merge_spec(source_sql, update_all=True, insert_all=True) if succeeds else None,
        )
        step["kind"] = kind
        cases.append(
            {
                "id": f"merge_star_source/{source_id}",
                "group": "merge_star_source",
                "format_version": "2",
                "mode": "copy-on-write",
                "setup": [
                    create_sql("(id BIGINT, v STRING)", "2", "copy-on-write"),
                    "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')",
                    "ALTER TABLE {t} ADD COLUMN extra STRING",
                ],
                "steps": [step, query_step("after", CHECK_SQL)],
            }
        )
    return cases


def adopted_setup() -> list[str]:
    return [
        create_sql("(id BIGINT, w STRING)", "2", "copy-on-write"),
        "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')",
        "ALTER TABLE {t} RENAME COLUMN w TO v",
        "ALTER TABLE {t} ADD COLUMN extra STRING",
    ]


def adopted_cases() -> list[dict[str, Any]]:
    statements = (
        statement_step(
            "merge_star",
            f"MERGE INTO {{t}} t USING ({ADOPTED_SOURCE}) s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
            merge_spec(ADOPTED_SOURCE, update_all=True, insert_all=True),
        ),
        statement_step("update_new_col", "UPDATE {t} SET extra = 'u' WHERE id = 2"),
        statement_step("delete", "DELETE FROM {t} WHERE id = 1"),
    )
    return [
        {
            "id": f"adopted/{step['id']}",
            "group": "adopted",
            "format_version": "2",
            "mode": "copy-on-write",
            "table": ADOPTED_TABLE,
            "setup": adopted_setup(),
            "steps": [step, query_step("after", CHECK_SQL)],
        }
        for step in statements
    ]


def lineage_read_cases() -> list[dict[str, Any]]:
    return [
        {
            "id": f"lineage_read/{evolution}",
            "group": "lineage_read",
            "format_version": "3",
            "mode": "copy-on-write",
            "setup": [
                create_sql(columns, "3", "copy-on-write"),
                f"INSERT INTO {{t}} VALUES {rows}",
                *ddl,
            ],
            "steps": [
                query_step(
                    "row_ids",
                    "SELECT _row_id, _last_updated_sequence_number, id, v, extra FROM {t}",
                ),
                query_step(
                    "row_ids_where_v", "SELECT _row_id, id, v, extra FROM {t} WHERE v = 'a'"
                ),
            ],
        }
        for evolution, columns, rows, ddl in EVOLUTIONS
    ]


def l02_evolution_cases() -> list[dict[str, Any]]:
    """Round-3 critic shapes: DROP then ADD of one name, RENAME onto a dropped name."""
    cases = []
    for evolution, columns, rows, ddl in L02_EVOLUTIONS:
        for format_version in FORMAT_VERSIONS:
            for mode in MODES:
                for statement_id, sql, dataframe in L02_EVOLUTION_STATEMENTS:
                    cases.append(
                        {
                            "id": f"{evolution}/v{format_version}/{mode[:3]}/{statement_id}",
                            "group": evolution,
                            "format_version": format_version,
                            "mode": mode,
                            "setup": [
                                create_sql(columns, format_version, mode),
                                f"INSERT INTO {{t}} VALUES {rows}",
                                *ddl,
                            ],
                            "steps": [
                                statement_step(statement_id, sql, dataframe),
                                query_step("after", CHECK_SQL),
                            ],
                        }
                    )
    return cases


def add_default_cases() -> list[dict[str, Any]]:
    """Round-3 critic shape: v3 ADD COLUMN with an initial default, refused on 4.1.2."""
    cases = []
    for mode in MODES:
        cases.append(
            {
                "id": f"add_default/v3/{mode[:3]}/ddl_refused",
                "group": "add_default",
                "format_version": "3",
                "mode": mode,
                "setup": [
                    create_sql("(id BIGINT, v STRING)", "3", mode),
                    "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')",
                ],
                "steps": [
                    {
                        "id": "ddl_refused",
                        "kind": "statement_error",
                        "sql": ADD_DEFAULT_DDL,
                        "dataframe": None,
                    },
                    query_step("after", ADD_DEFAULT_CHECK),
                ],
            }
        )
    return cases


def new_col_predicate_cases() -> list[dict[str, Any]]:
    """Round-3 critic shape: the added column in a predicate, a merge key, NMBS."""
    cases = []
    for format_version in FORMAT_VERSIONS:
        for mode in MODES:
            for statement_id, sql, dataframe in PREDICATE_STATEMENTS:
                cases.append(
                    {
                        "id": f"new_col_predicate/v{format_version}/{mode[:3]}/{statement_id}",
                        "group": "new_col_predicate",
                        "format_version": format_version,
                        "mode": mode,
                        "setup": [
                            create_sql("(id BIGINT, v STRING)", format_version, mode),
                            "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')",
                            "ALTER TABLE {t} ADD COLUMN extra STRING",
                        ],
                        "steps": [
                            statement_step(statement_id, sql, dataframe),
                            query_step("after", CHECK_SQL),
                        ],
                    }
                )
    return cases


def build_cases() -> list[dict[str, Any]]:
    return (
        grid_cases()
        + emptied_cases()
        + rewrite_cases()
        + merge_star_source_cases()
        + adopted_cases()
        + lineage_read_cases()
        + l02_evolution_cases()
        + add_default_cases()
        + new_col_predicate_cases()
    )


def render(sql: str, table: str) -> str:
    catalog, _, relative = table.partition(".")
    return sql.format(t=table, cat=catalog, rel=relative)


def arrow_answer(table: Any) -> dict[str, Any]:
    rows = [[row[name] for name in table.column_names] for row in table.to_pylist()]
    rows.sort(key=repr)
    return {
        "columns": list(table.column_names),
        "types": {field.name: str(field.type) for field in table.schema},
        "rows": rows,
    }


def error_answer(error: BaseException) -> dict[str, Any]:
    lines = str(error).strip().splitlines()
    return {"error": {"type": type(error).__name__, "message": lines[0] if lines else ""}}


def catalog_digest(cases: list[dict[str, Any]]) -> str:
    return hashlib.sha256(json.dumps(cases, sort_keys=True).encode("utf-8")).hexdigest()


def write_truth(truth: dict[str, Any]) -> None:
    lines = [
        json.dumps(answer, sort_keys=True, separators=(",", ":")) for answer in truth["answers"]
    ]
    header = {key: value for key, value in truth.items() if key != "answers"}
    TRUTH_FILE.write_text(
        json.dumps(header, sort_keys=True, separators=(",", ":"))[:-1]
        + ',"answers":[\n'
        + ",\n".join(lines)
        + "\n]}\n",
        encoding="utf-8",
    )


def spark_merge(spark: Any, functions: Any, table: str, spec: dict[str, Any]) -> None:
    target = table.rsplit(".", 1)[-1]
    condition = functions.col(f"{target}.{spec['key']}") == functions.col(f"source.{spec['key']}")
    writer = spark.sql(spec["source"]).alias("source").mergeInto(table, condition)
    if spec.get("update_all"):
        writer = writer.whenMatched().updateAll()
    if spec.get("update"):
        writer = writer.whenMatched().update(
            {name: functions.col(f"source.{name}") for name in spec["update"]}
        )
    if spec.get("insert_all"):
        writer = writer.whenNotMatched().insertAll()
    if spec.get("insert"):
        writer = writer.whenNotMatched().insert(
            {name: functions.col(f"source.{name}") for name in spec["insert"]}
        )
    if spec.get("by_source_update"):
        writer = writer.whenNotMatchedBySource().update(
            {name: functions.lit(value) for name, value in spec["by_source_update"].items()}
        )
    writer.merge()


def spark_dataframe_statement(spark: Any, functions: Any, table: str, spec: dict[str, Any]) -> None:
    if "merge" in spec:
        spark_merge(spark, functions, table, spec["merge"])
    elif "append" in spec:
        spark.sql(spec["append"]).writeTo(table).append()
    else:
        spark.sql(spec["overwrite_partitions"]).writeTo(table).overwritePartitions()


def run_case_steps(spark: Any, table: str, case: dict[str, Any]) -> list[dict[str, Any]]:
    answers = []
    for step in case["steps"]:
        sql = render(step["sql"], table)
        try:
            if step["kind"] == "query":
                answer: dict[str, Any] = arrow_answer(spark.sql(sql).toArrow())
            else:
                spark.sql(sql).collect()
                answer = {"ok": True}
        except Exception as error:
            answer = error_answer(error)
        answers.append({"id": step["id"], "sql": answer})
    return answers


def run_dataframe_steps(
    spark: Any, functions: Any, table: str, case: dict[str, Any]
) -> list[dict[str, Any]] | None:
    statement = case["steps"][0]
    if statement["dataframe"] is None:
        return None
    try:
        spark_dataframe_statement(spark, functions, table, statement["dataframe"])
        outcome: dict[str, Any] = {"ok": True}
    except Exception as error:
        outcome = error_answer(error)
    check = case["steps"][1]
    return [
        {"id": statement["id"], "dataframe": outcome},
        {
            "id": check["id"],
            "dataframe": arrow_answer(spark.sql(render(check["sql"], table)).toArrow()),
        },
    ]


def spark_session(warehouse: Path) -> Any:
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("record-ice-evo-dml-1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{SPARK_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.warehouse", str(warehouse))
        .config(f"spark.sql.catalog.{ADOPTED_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ADOPTED_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ADOPTED_CATALOG}.cache-enabled", "false")
        .config(f"spark.sql.catalog.{ADOPTED_CATALOG}.warehouse", str(ADOPTED_WAREHOUSE))
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


def adopted_root() -> Path:
    return ADOPTED_WAREHOUSE / ADOPTED_NAMESPACE / ADOPTED_TABLE


def freeze_adopted_table() -> dict[str, Any]:
    target = FIXTURE_DIR / ADOPTED_TABLE
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(adopted_root(), target)
    for crc in [*target.rglob("*.crc"), *target.rglob(".*.crc")]:
        crc.unlink(missing_ok=True)
    versions = sorted(
        (target / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    return {"table_root": str(adopted_root()), "metadata_file": versions[-1].name}


def restore_adopted_table() -> None:
    root = adopted_root()
    if root.exists():
        shutil.rmtree(root)
    shutil.copytree(FIXTURE_DIR / ADOPTED_TABLE, root)


def record(cases: list[dict[str, Any]], warehouse: Path) -> dict[str, Any]:
    import pyspark
    from pyspark.sql import functions

    spark = spark_session(warehouse)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.ns")
    adopted_table = f"{ADOPTED_CATALOG}.{ADOPTED_NAMESPACE}.{ADOPTED_TABLE}"
    for statement in adopted_setup():
        spark.sql(render(statement, adopted_table)).collect()
    frozen = freeze_adopted_table()
    recorded = []
    try:
        for index, case in enumerate(cases):
            entry: dict[str, Any] = {"id": case["id"]}
            if case["group"] == "adopted":
                restore_adopted_table()
                entry["frozen"] = frozen
                entry["steps"] = run_case_steps(spark, adopted_table, case)
                restore_adopted_table()
                dataframe = run_dataframe_steps(spark, functions, adopted_table, case)
            else:
                table = f"{SPARK_CATALOG}.ns.t{index}"
                twin = f"{SPARK_CATALOG}.ns.twin{index}"
                for statement in case["setup"]:
                    spark.sql(render(statement, table)).collect()
                entry["steps"] = run_case_steps(spark, table, case)
                dataframe = None
                if case["steps"][0]["dataframe"] is not None:
                    for statement in case["setup"]:
                        spark.sql(render(statement, twin)).collect()
                    dataframe = run_dataframe_steps(spark, functions, twin, case)
            if dataframe is not None:
                entry["dataframe_steps"] = dataframe
            recorded.append(entry)
            print(f"recorded {case['id']}", flush=True)
        return {
            "oracle": {
                "pyspark": pyspark.__version__,
                "iceberg_runtime": spark.conf.get("spark.jars.packages"),
                "ansi": spark.conf.get("spark.sql.ansi.enabled"),
            },
            "catalog_sha256": catalog_digest(cases),
            "answers": recorded,
        }
    finally:
        spark.stop()


def main() -> int:
    if ADOPTED_WAREHOUSE.exists():
        shutil.rmtree(ADOPTED_WAREHOUSE)
    ADOPTED_WAREHOUSE.mkdir(parents=True)
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="record-ice-evo-dml-1-") as scratch:
        truth = record(build_cases(), Path(scratch) / "warehouse")
    write_truth(truth)
    shutil.rmtree(ADOPTED_WAREHOUSE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
