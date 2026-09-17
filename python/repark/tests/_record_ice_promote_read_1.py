from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

FIXTURE_DIR = (
    Path(__file__).resolve().parents[2] / "repark-parity/fixtures/torture/data/ice_promote_read_1"
)
TRUTH_FILE = FIXTURE_DIR / "truth.json"
ADOPTED_WAREHOUSE = Path("/tmp/repark-ice-promote-read-1")
ADOPTED_CATALOG = "ice_promote_read_1"
ADOPTED_NAMESPACE = "ns"
SPARK_CATALOG = "sc"
FORMAT_VERSIONS = ("2", "3")
MODES = ("copy-on-write", "merge-on-read")
ERAS = ("single", "mixed")
LONG_IN = (1, 2, *range(4, 26))
LONG_IN_SQL = ", ".join(str(value) for value in LONG_IN)
DYNAMIC_OVERWRITE_CONF = {"spark.sql.sources.partitionOverwriteMode": "dynamic"}

READ_PREDICATES: tuple[tuple[str, str, dict[str, Any]], ...] = (
    ("id_eq_1", "id = 1", {"column": "id", "op": "eq", "value": 1}),
    ("id_lt_2", "id < 2", {"column": "id", "op": "lt", "value": 2}),
    ("id_le_2", "id <= 2", {"column": "id", "op": "le", "value": 2}),
    ("id_between_1_2", "id BETWEEN 1 AND 2", {"column": "id", "op": "between", "value": [1, 2]}),
    ("id_gt_1", "id > 1", {"column": "id", "op": "gt", "value": 1}),
    ("id_ge_2", "id >= 2", {"column": "id", "op": "ge", "value": 2}),
    ("id_ne_1", "id <> 1", {"column": "id", "op": "ne", "value": 1}),
    (
        "id_in_short",
        "id IN (1, 3000000000)",
        {"column": "id", "op": "in", "value": [1, 3000000000]},
    ),
    ("id_in_long", f"id IN ({LONG_IN_SQL})", {"column": "id", "op": "in", "value": list(LONG_IN)}),
    ("id_not_in", "id NOT IN (1, 5)", {"column": "id", "op": "not_in", "value": [1, 5]}),
    ("f_lt_2", "f < 2.0D", {"column": "f", "op": "lt", "value": 2.0}),
    ("f_gt_2", "f > 2.0D", {"column": "f", "op": "gt", "value": 2.0}),
    ("f_eq_1_5", "f = 1.5D", {"column": "f", "op": "eq", "value": 1.5}),
    ("d_lt_2", "d < CAST(2.00 AS DECIMAL(12,2))", {"column": "d", "op": "lt", "decimal": "2.00"}),
    ("d_gt_2", "d > CAST(2.00 AS DECIMAL(12,2))", {"column": "d", "op": "gt", "decimal": "2.00"}),
)

PARTITION_PREDICATES: tuple[tuple[str, str, dict[str, Any]], ...] = (
    ("p_eq_1", "p = 1", {"column": "p", "op": "eq", "value": 1}),
    ("p_lt_20", "p < 20", {"column": "p", "op": "lt", "value": 20}),
    ("p_gt_1", "p > 1", {"column": "p", "op": "gt", "value": 1}),
    (
        "p_in",
        "p IN (22, 3000000000)",
        {"column": "p", "op": "in", "value": [22, 3000000000]},
    ),
)

PARTITION_SPECS = (("identity", "p"), ("bucket", "bucket(4, p)"), ("truncate", "truncate(10, p)"))


def table_properties(format_version: str, mode: str | None) -> str:
    properties = [f"'format-version'='{format_version}'"]
    if mode is not None:
        properties.extend(f"'write.{kind}.mode'='{mode}'" for kind in ("delete", "update", "merge"))
    return ", ".join(properties)


def query_step(step_id: str, sql: str, dataframe: dict[str, Any] | None) -> dict[str, Any]:
    return {"id": step_id, "kind": "query", "sql": sql, "dataframe": dataframe}


def statement_step(
    step_id: str,
    sql: str,
    dataframe: dict[str, Any] | None = None,
    conf: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "id": step_id,
        "kind": "statement",
        "sql": sql,
        "dataframe": dataframe,
        "conf": conf or {},
    }


def read_steps(
    columns: list[str], predicates: tuple[tuple[str, str, dict[str, Any]], ...]
) -> list[dict[str, Any]]:
    projection = ", ".join(columns)
    steps = [
        query_step(
            predicate_id,
            f"SELECT {projection} FROM {{t}} WHERE {predicate_sql}",
            {"select": columns, "filter": spec},
        )
        for predicate_id, predicate_sql, spec in predicates
    ]
    steps.append(query_step("unfiltered", f"SELECT {projection} FROM {{t}}", {"select": columns}))
    return steps


def read_cases() -> list[dict[str, Any]]:
    cases = []
    columns = ["id", "f", "d", "s"]
    for format_version in FORMAT_VERSIONS:
        for era in ERAS:
            setup = [
                "CREATE TABLE {t} (id INT, f FLOAT, d DECIMAL(9,2), s STRING) USING iceberg "
                f"TBLPROPERTIES ({table_properties(format_version, None)})",
                "INSERT INTO {t} VALUES (1, CAST(1.5 AS FLOAT), "
                "CAST(1.25 AS DECIMAL(9,2)), 'old1'), "
                "(2, CAST(2.5 AS FLOAT), CAST(2.50 AS DECIMAL(9,2)), 'old2')",
                "ALTER TABLE {t} ALTER COLUMN id TYPE BIGINT",
                "ALTER TABLE {t} ALTER COLUMN f TYPE DOUBLE",
                "ALTER TABLE {t} ALTER COLUMN d TYPE DECIMAL(12,2)",
            ]
            if era == "mixed":
                setup.append(
                    "INSERT INTO {t} VALUES (3000000000, CAST(3.5 AS DOUBLE), "
                    "CAST(3.75 AS DECIMAL(12,2)), 'new3')"
                )
            steps = read_steps(columns, READ_PREDICATES)
            steps.append(
                query_step(
                    "count_id_lt_2",
                    "SELECT count(*) AS c FROM {t} WHERE id < 2",
                    {"count": True, "filter": {"column": "id", "op": "lt", "value": 2}},
                )
            )
            cases.append(
                {
                    "id": f"read/v{format_version}/{era}",
                    "group": "read",
                    "format_version": format_version,
                    "mode": None,
                    "era": era,
                    "setup": setup,
                    "steps": steps,
                }
            )
    return cases


def read_partition_cases() -> list[dict[str, Any]]:
    cases = []
    for format_version in FORMAT_VERSIONS:
        for spec_name, spec_sql in PARTITION_SPECS:
            for era in ERAS:
                setup = [
                    "CREATE TABLE {t} (p INT, s STRING) USING iceberg "
                    f"PARTITIONED BY ({spec_sql}) "
                    f"TBLPROPERTIES ({table_properties(format_version, None)})",
                    "INSERT INTO {t} VALUES (1, 'old1'), (22, 'old22')",
                    "ALTER TABLE {t} ALTER COLUMN p TYPE BIGINT",
                ]
                if era == "mixed":
                    setup.append("INSERT INTO {t} VALUES (3000000000, 'new3')")
                cases.append(
                    {
                        "id": f"read_partition/v{format_version}/{spec_name}/{era}",
                        "group": "read_partition",
                        "format_version": format_version,
                        "mode": None,
                        "era": era,
                        "setup": setup,
                        "steps": read_steps(["p", "s"], PARTITION_PREDICATES),
                    }
                )
    return cases


MERGE_KEY_ACTIONS: tuple[tuple[str, str, bool], ...] = (
    ("matched_only_bigint", "SELECT CAST(1 AS BIGINT) AS id, 'm1' AS v", False),
    (
        "matched_insert_bigint",
        "SELECT CAST(1 AS BIGINT) AS id, 'm1' AS v UNION ALL "
        "SELECT CAST(4 AS BIGINT) AS id, 'm4' AS v",
        True,
    ),
    ("matched_insert_int", "SELECT 1 AS id, 'm1' AS v UNION ALL SELECT 4 AS id, 'm4' AS v", True),
)


def merge_key_cases() -> list[dict[str, Any]]:
    cases = []
    for format_version in FORMAT_VERSIONS:
        for mode in MODES:
            for action_id, source_sql, insert_all in MERGE_KEY_ACTIONS:
                merge_sql = (
                    f"MERGE INTO {{t}} t USING ({source_sql}) s ON t.id = s.id "
                    "WHEN MATCHED THEN UPDATE SET v = s.v"
                )
                if insert_all:
                    merge_sql += " WHEN NOT MATCHED THEN INSERT *"
                cases.append(
                    {
                        "id": f"merge_key/v{format_version}/{mode}/{action_id}",
                        "group": "merge_key",
                        "format_version": format_version,
                        "mode": mode,
                        "era": "mixed",
                        "setup": [
                            "CREATE TABLE {t} (id INT, v STRING) USING iceberg "
                            f"TBLPROPERTIES ({table_properties(format_version, mode)})",
                            "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')",
                            "ALTER TABLE {t} ALTER COLUMN id TYPE BIGINT",
                            "INSERT INTO {t} VALUES (3, 'c')",
                        ],
                        "steps": [
                            statement_step(
                                action_id,
                                merge_sql,
                                {
                                    "merge": {
                                        "source": source_sql,
                                        "key": "id",
                                        "update": ["v"],
                                        "insert_all": insert_all,
                                    }
                                },
                            ),
                            query_step("check", "SELECT id, v FROM {t}", None),
                        ],
                    }
                )
    return cases


def dml_range_cases() -> list[dict[str, Any]]:
    actions = (
        ("update_id_lt_3", "UPDATE {t} SET s = 'u' WHERE id < 3"),
        ("delete_id_lt_2", "DELETE FROM {t} WHERE id < 2"),
        ("delete_f_lt_2", "DELETE FROM {t} WHERE f < 2.0D"),
        ("update_id_in_long", f"UPDATE {{t}} SET s = 'u' WHERE id IN ({LONG_IN_SQL})"),
        ("delete_id_in_long", f"DELETE FROM {{t}} WHERE id IN ({LONG_IN_SQL})"),
    )
    cases = []
    for format_version in FORMAT_VERSIONS:
        for mode in MODES:
            for action_id, action_sql in actions:
                cases.append(
                    {
                        "id": f"dml_range/v{format_version}/{mode}/{action_id}",
                        "group": "dml_range",
                        "format_version": format_version,
                        "mode": mode,
                        "era": "mixed",
                        "setup": [
                            "CREATE TABLE {t} (id INT, f FLOAT, s STRING) USING iceberg "
                            f"TBLPROPERTIES ({table_properties(format_version, mode)})",
                            "INSERT INTO {t} VALUES (1, CAST(1.5 AS FLOAT), 'old1'), "
                            "(2, CAST(2.5 AS FLOAT), 'old2')",
                            "ALTER TABLE {t} ALTER COLUMN id TYPE BIGINT",
                            "ALTER TABLE {t} ALTER COLUMN f TYPE DOUBLE",
                            "INSERT INTO {t} VALUES (3000000000, CAST(3.5 AS DOUBLE), 'new3')",
                        ],
                        "steps": [
                            statement_step(action_id, action_sql),
                            query_step("check", "SELECT id, f, s FROM {t}", None),
                        ],
                    }
                )
    return cases


def dml_single_cases() -> list[dict[str, Any]]:
    upsert_source = (
        "SELECT CAST(5 AS BIGINT) AS id, 'm5' AS s UNION ALL "
        "SELECT CAST(21 AS BIGINT) AS id, 'm21' AS s"
    )
    actions: tuple[tuple[str, str, dict[str, Any] | None], ...] = (
        (
            "merge_key_upsert",
            f"MERGE INTO {{t}} t USING ({upsert_source}) src ON t.id = src.id "
            "WHEN MATCHED THEN UPDATE SET s = src.s "
            "WHEN NOT MATCHED THEN INSERT (id, s) VALUES (src.id, src.s)",
            {
                "merge": {
                    "source": upsert_source,
                    "key": "id",
                    "update": ["s"],
                    "insert": ["id", "s"],
                }
            },
        ),
        (
            "merge_float_update",
            "MERGE INTO {t} t USING (SELECT CAST(5 AS BIGINT) AS id, CAST(9.5 AS DOUBLE) AS f) src "
            "ON t.id = src.id WHEN MATCHED THEN UPDATE SET f = src.f",
            None,
        ),
        (
            "merge_decimal_delete",
            "MERGE INTO {t} t USING (SELECT CAST(6 AS BIGINT) AS id) src "
            "ON t.id = src.id WHEN MATCHED THEN DELETE",
            None,
        ),
        ("update_eq", "UPDATE {t} SET s = 'u5' WHERE id = 5", None),
        ("delete_eq", "DELETE FROM {t} WHERE id = 6", None),
    )
    cases = []
    for format_version in FORMAT_VERSIONS:
        for mode in MODES:
            for action_id, action_sql, dataframe in actions:
                cases.append(
                    {
                        "id": f"dml_single/v{format_version}/{mode}/{action_id}",
                        "group": "dml_single",
                        "format_version": format_version,
                        "mode": mode,
                        "era": "single",
                        "setup": [
                            "CREATE TABLE {t} (id INT, f FLOAT, d DECIMAL(9,2), s STRING) "
                            "USING iceberg "
                            f"TBLPROPERTIES ({table_properties(format_version, mode)})",
                            "INSERT INTO {t} VALUES (5, CAST(1.5 AS FLOAT), "
                            "CAST(1.25 AS DECIMAL(9,2)), 'e5'), "
                            "(6, CAST(2.5 AS FLOAT), CAST(2.50 AS DECIMAL(9,2)), 'e6')",
                            "ALTER TABLE {t} ALTER COLUMN id TYPE BIGINT",
                            "ALTER TABLE {t} ALTER COLUMN f TYPE DOUBLE",
                            "ALTER TABLE {t} ALTER COLUMN d TYPE DECIMAL(12,2)",
                        ],
                        "steps": [
                            statement_step(action_id, action_sql, dataframe),
                            query_step("check", "SELECT id, f, d, s FROM {t}", None),
                        ],
                    }
                )
    return cases


def dml_partition_cases() -> list[dict[str, Any]]:
    actions: tuple[tuple[str, str, dict[str, Any] | None, dict[str, str] | None], ...] = (
        ("delete_nonkey", "DELETE FROM {t} WHERE s = 'old1'", None, None),
        ("update_nonkey", "UPDATE {t} SET s = 'u' WHERE s = 'old2'", None, None),
        ("delete_p_eq", "DELETE FROM {t} WHERE p = 1", None, None),
        ("update_p_lt", "UPDATE {t} SET s = 'u' WHERE p < 20", None, None),
        (
            "merge_p",
            "MERGE INTO {t} t USING (SELECT CAST(22 AS BIGINT) AS p, 'm22' AS s) src "
            "ON t.p = src.p WHEN MATCHED THEN UPDATE SET s = src.s",
            None,
            None,
        ),
        ("static_overwrite", "INSERT OVERWRITE {t} PARTITION (p = 1) SELECT 7, 'ow'", None, None),
        (
            "dynamic_overwrite",
            "INSERT OVERWRITE {t} PARTITION (p) SELECT 7, 1, 'ow'",
            {
                "overwrite_partitions": (
                    "SELECT CAST(7 AS BIGINT) AS id, CAST(1 AS BIGINT) AS p, 'ow' AS s"
                )
            },
            DYNAMIC_OVERWRITE_CONF,
        ),
    )
    cases = []
    for format_version in FORMAT_VERSIONS:
        for mode in MODES:
            for era in ERAS:
                for action_id, action_sql, dataframe, conf in actions:
                    setup = [
                        "CREATE TABLE {t} (id INT, p INT, s STRING) USING iceberg "
                        "PARTITIONED BY (p) "
                        f"TBLPROPERTIES ({table_properties(format_version, mode)})",
                        "INSERT INTO {t} VALUES (1, 1, 'old1'), (2, 22, 'old2')",
                        "ALTER TABLE {t} ALTER COLUMN id TYPE BIGINT",
                        "ALTER TABLE {t} ALTER COLUMN p TYPE BIGINT",
                    ]
                    if era == "mixed":
                        setup.append("INSERT INTO {t} VALUES (3000000000, 3000000000, 'new3')")
                    cases.append(
                        {
                            "id": f"dml_partition/v{format_version}/{mode}/{era}/{action_id}",
                            "group": "dml_partition",
                            "format_version": format_version,
                            "mode": mode,
                            "era": era,
                            "setup": setup,
                            "steps": [
                                statement_step(action_id, action_sql, dataframe, conf),
                                query_step("check", "SELECT id, p, s FROM {t}", None),
                            ],
                        }
                    )
    return cases


def adopted_setup(format_version: str) -> list[str]:
    return [
        "CREATE TABLE {t} (id INT, f FLOAT, d DECIMAL(9,2), p INT, s STRING) USING iceberg "
        f"PARTITIONED BY (p) TBLPROPERTIES ({table_properties(format_version, None)})",
        "INSERT INTO {t} VALUES (1, CAST(1.5 AS FLOAT), CAST(1.25 AS DECIMAL(9,2)), 1, 'old1'), "
        "(2, CAST(2.5 AS FLOAT), CAST(2.50 AS DECIMAL(9,2)), 22, 'old2')",
        "ALTER TABLE {t} ALTER COLUMN id TYPE BIGINT",
        "ALTER TABLE {t} ALTER COLUMN f TYPE DOUBLE",
        "ALTER TABLE {t} ALTER COLUMN d TYPE DECIMAL(12,2)",
        "ALTER TABLE {t} ALTER COLUMN p TYPE BIGINT",
        "INSERT INTO {t} VALUES (3000000000, CAST(3.5 AS DOUBLE), CAST(3.75 AS DECIMAL(12,2)), "
        "3000000000, 'new3')",
    ]


def adopted_cases() -> list[dict[str, Any]]:
    cases = []
    upsert_source = (
        "SELECT CAST(1 AS BIGINT) AS id, CAST(9.5 AS DOUBLE) AS f, "
        "CAST(9.25 AS DECIMAL(12,2)) AS d, "
        "CAST(1 AS BIGINT) AS p, 'm1' AS s UNION ALL SELECT CAST(4 AS BIGINT) AS id, "
        "CAST(4.5 AS DOUBLE) AS f, CAST(4.25 AS DECIMAL(12,2)) AS d, "
        "CAST(4 AS BIGINT) AS p, 'm4' AS s"
    )
    for format_version in FORMAT_VERSIONS:
        columns = ["id", "f", "d", "p", "s"]
        steps = read_steps(columns, READ_PREDICATES + PARTITION_PREDICATES)
        steps.extend(
            [
                statement_step(
                    "merge_upsert",
                    f"MERGE INTO {{t}} t USING ({upsert_source}) src ON t.id = src.id "
                    "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
                ),
                query_step("after_merge", "SELECT id, f, d, p, s FROM {t}", None),
                statement_step("update_id_lt_3", "UPDATE {t} SET s = 'u' WHERE id < 3"),
                query_step("after_update", "SELECT id, f, d, p, s FROM {t}", None),
                statement_step("delete_f_lt_3", "DELETE FROM {t} WHERE f < 3.0D"),
                query_step("after_delete", "SELECT id, f, d, p, s FROM {t}", None),
            ]
        )
        cases.append(
            {
                "id": f"adopted/v{format_version}",
                "group": "adopted",
                "format_version": format_version,
                "mode": None,
                "era": "mixed",
                "table": f"adopt_v{format_version}",
                "setup": adopted_setup(format_version),
                "steps": steps,
            }
        )
    return cases


def build_cases() -> list[dict[str, Any]]:
    return (
        read_cases()
        + read_partition_cases()
        + merge_key_cases()
        + dml_range_cases()
        + dml_single_cases()
        + dml_partition_cases()
        + adopted_cases()
    )


def json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    return value


def arrow_answer(table: Any) -> dict[str, Any]:
    rows = [[json_value(row[name]) for name in table.column_names] for row in table.to_pylist()]
    rows.sort(key=repr)
    return {
        "columns": list(table.column_names),
        "types": {field.name: str(field.type) for field in table.schema},
        "rows": rows,
    }


def error_answer(error: BaseException) -> dict[str, Any]:
    lines = str(error).strip().splitlines()
    return {"error": {"type": type(error).__name__, "message": lines[0] if lines else ""}}


def dataframe_matches_sql(dataframe_answer: dict[str, Any], sql_answer: dict[str, Any]) -> bool:
    if "count" in dataframe_answer:
        return sql_answer.get("rows") == [[dataframe_answer["count"]]]
    return dataframe_answer == sql_answer


def catalog_digest(cases: list[dict[str, Any]]) -> str:
    return hashlib.sha256(json.dumps(cases, sort_keys=True).encode("utf-8")).hexdigest()


def write_truth(truth: dict[str, Any]) -> None:
    lines = [
        json.dumps(answer, sort_keys=True, separators=(",", ":")) for answer in truth["answers"]
    ]
    header = {key: value for key, value in truth.items() if key != "answers"}
    body = ",\n".join(lines)
    TRUTH_FILE.write_text(
        json.dumps(header, sort_keys=True, separators=(",", ":"))[:-1]
        + ',"answers":[\n'
        + body
        + "\n]}\n",
        encoding="utf-8",
    )


def spark_filter(functions: Any, spec: dict[str, Any]) -> Any:
    column = functions.col(spec["column"])
    if "decimal" in spec:
        literal = functions.lit(Decimal(spec["decimal"]))
    elif spec["op"] in {"in", "not_in", "between"}:
        literal = None
    else:
        literal = functions.lit(spec["value"])
    operations = {
        "eq": lambda: column == literal,
        "ne": lambda: column != literal,
        "lt": lambda: column < literal,
        "le": lambda: column <= literal,
        "gt": lambda: column > literal,
        "ge": lambda: column >= literal,
        "in": lambda: column.isin(*spec["value"]),
        "not_in": lambda: ~column.isin(*spec["value"]),
        "between": lambda: column.between(*spec["value"]),
    }
    return operations[spec["op"]]()


def spark_dataframe_query(spark: Any, functions: Any, table: str, spec: dict[str, Any]) -> Any:
    frame = spark.table(table)
    if "filter" in spec:
        frame = frame.filter(spark_filter(functions, spec["filter"]))
    if spec.get("count"):
        return {"count": frame.count()}
    return arrow_answer(frame.select(*spec["select"]).toArrow())


def run_statement(spark: Any, sql: str, conf: dict[str, str]) -> dict[str, Any]:
    previous = {key: spark.conf.get(key, None) for key in conf}
    for key, value in conf.items():
        spark.conf.set(key, value)
    try:
        spark.sql(sql).collect()
        return {"ok": True}
    except Exception as error:
        return error_answer(error)
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def run_case_steps(
    spark: Any, functions: Any, table: str, case: dict[str, Any]
) -> list[dict[str, Any]]:
    answers = []
    for step in case["steps"]:
        sql = step["sql"].format(t=table)
        if step["kind"] == "query":
            try:
                answer = {"sql": arrow_answer(spark.sql(sql).toArrow())}
            except Exception as error:
                answer = {"sql": error_answer(error)}
            if step["dataframe"] is not None:
                try:
                    dataframe_answer = spark_dataframe_query(
                        spark, functions, table, step["dataframe"]
                    )
                except Exception as error:
                    dataframe_answer = error_answer(error)
                if not dataframe_matches_sql(dataframe_answer, answer["sql"]):
                    answer["dataframe"] = dataframe_answer
        else:
            answer = {"sql": run_statement(spark, sql, step.get("conf", {}))}
        answers.append({"id": step["id"], **answer})
    return answers


def run_overwrite_partitions_twin(
    spark: Any, functions: Any, case: dict[str, Any], index: int, catalog: str = SPARK_CATALOG
) -> list[dict[str, Any]]:
    table = f"{catalog}.ns.twin{index}"
    for statement in case["setup"]:
        spark.sql(statement.format(t=table)).collect()
    step = case["steps"][0]
    try:
        spark.sql(step["dataframe"]["overwrite_partitions"]).writeTo(table).overwritePartitions()
        outcome: dict[str, Any] = {"ok": True}
    except Exception as error:
        outcome = error_answer(error)
    check = case["steps"][1]
    return [
        {"id": step["id"], "dataframe": outcome},
        {
            "id": check["id"],
            "dataframe": arrow_answer(spark.sql(check["sql"].format(t=table)).toArrow()),
        },
    ]


def spark_session(warehouse: Path) -> Any:
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("record-ice-promote-read-1")
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
        .config(f"spark.sql.catalog.{ADOPTED_CATALOG}.warehouse", str(ADOPTED_WAREHOUSE))
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


def freeze_adopted_table(table_root: Path) -> dict[str, Any]:
    target = FIXTURE_DIR / table_root.name
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(table_root, target)
    for crc in target.rglob("*.crc"):
        crc.unlink()
    for crc in target.rglob(".*.crc"):
        crc.unlink()
    versions = sorted(
        (target / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    return {"table_root": str(table_root), "metadata_file": versions[-1].name}


def record(cases: list[dict[str, Any]], warehouse: Path) -> dict[str, Any]:
    import pyspark
    from pyspark.sql import functions

    spark = spark_session(warehouse)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.ns")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ADOPTED_CATALOG}.{ADOPTED_NAMESPACE}")
    recorded = []
    try:
        for index, case in enumerate(cases):
            if case["group"] == "adopted":
                table = f"{ADOPTED_CATALOG}.{ADOPTED_NAMESPACE}.{case['table']}"
            else:
                table = f"{SPARK_CATALOG}.ns.t{index}"
            setup_error = None
            for statement in case["setup"]:
                try:
                    spark.sql(statement.format(t=table)).collect()
                except Exception as error:
                    setup_error = error_answer(error)
                    break
            entry: dict[str, Any] = {"id": case["id"]}
            if setup_error is not None:
                entry["setup_error"] = setup_error
                recorded.append(entry)
                continue
            if case["group"] == "adopted":
                entry["frozen"] = freeze_adopted_table(
                    ADOPTED_WAREHOUSE / ADOPTED_NAMESPACE / case["table"]
                )
            entry["steps"] = run_case_steps(spark, functions, table, case)
            if (
                case["steps"][0].get("dataframe")
                and "overwrite_partitions" in case["steps"][0]["dataframe"]
            ):
                entry["dataframe_steps"] = run_overwrite_partitions_twin(
                    spark, functions, case, index
                )
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
    with tempfile.TemporaryDirectory(prefix="record-ice-promote-read-1-") as scratch:
        truth = record(build_cases(), Path(scratch) / "warehouse")
    write_truth(truth)
    shutil.rmtree(ADOPTED_WAREHOUSE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
