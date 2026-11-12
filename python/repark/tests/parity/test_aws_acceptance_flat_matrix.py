from __future__ import annotations

import os
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from _acceptance import (
    ACCEPTANCE_NAMESPACE,
    GLUE_WAREHOUSE,
    S3TABLES_CATALOG,
    SILVER_CATALOG,
    assert_glue_scratch_namespace_location,
    assert_real_buckets_configured,
    glue_catalog_config,
    run_table_prefix,
    s3tables_catalog_config,
    s3tables_spark_catalog_config,
)
from _flat_matrix_cells import (
    FLOAT_SEED_T,
    FLOAT_SEED_U,
    FLOAT_SQL,
    FLOAT_WRITE_SHAPES,
    FOLDED_PREDICATES,
    KNOWN_OPEN,
    PREDICATE_TEXT,
    SEED_ROWS,
    Cell,
    cells_for_group,
    classify,
    committed_commits,
    committed_ids,
    expected_for,
    load_expected,
    passes,
    record_line,
    target_for,
    typed_predicate,
)
from test_aws_acceptance_cross_read import _spark_session_for_catalog

from repark import ReparkSession, functions

GROUP = os.environ.get("REPARK_FLAT_GROUP", "pilot")
CATALOG = os.environ.get("REPARK_FLAT_CATALOG", "glue")
EXPECTED = load_expected()

pytestmark = pytest.mark.skipif(
    os.environ.get("REPARK_AWS_ACCEPTANCE") != "1",
    reason=(
        "real-AWS flat-matrix harness: set REPARK_AWS_ACCEPTANCE=1 to run "
        "(companion to the AWS acceptance legs — never routine development)"
    ),
)

if CATALOG not in {"glue", "s3tables"}:
    raise ValueError(f"REPARK_FLAT_CATALOG must be glue or s3tables, got {CATALOG!r}")


def _params() -> list[Any]:
    params = []
    for cell in cells_for_group(GROUP):
        marks = []
        unit = KNOWN_OPEN.get(cell.cell_id)
        if unit is not None:
            marks.append(pytest.mark.xfail(strict=True, reason=unit))
        params.append(pytest.param(cell, id=cell.cell_id, marks=marks))
    return params


@dataclass(frozen=True)
class Observed:
    outcome: str
    answer: list[tuple[Any, ...]] | None
    commits: int | None
    error: str | None
    probe_error: str | None = None


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _fq(catalog: str, name: str) -> str:
    return f"{catalog}.{ACCEPTANCE_NAMESPACE}.{name}"


def _normal(value: Any) -> Any:
    if isinstance(value, float):
        return repr(value)
    return value


def _rows(frame_rows: list[Any]) -> list[tuple[Any, ...]]:
    return sorted((tuple(_normal(v) for v in row) for row in frame_rows), key=repr)


def _commit_count(session: Any, table: str) -> int:
    return int(session.sql(f"SELECT count(*) FROM {table}.snapshots").collect()[0][0])


def _create_matrix_table(session: Any, table: str, layout: str) -> None:
    mode = "copy-on-write" if layout == "v2-cow" else "merge-on-read"
    session.sql(
        f"CREATE TABLE {table} (id BIGINT NOT NULL, i INT, l BIGINT, dec DECIMAL(10,2), "
        "d DOUBLE, s STRING, b BOOLEAN, dt DATE, ts TIMESTAMP, ntz TIMESTAMP_NTZ, flag STRING) "
        "USING iceberg TBLPROPERTIES ('format-version'='2', "
        f"'write.delete.mode'='{mode}', 'write.update.mode'='{mode}', 'write.merge.mode'='{mode}')"
    )


def _create_float_table(session: Any, table: str, float_type: str, rows: tuple[str, ...]) -> None:
    session.sql(
        f"CREATE TABLE {table} (id BIGINT NOT NULL, d {float_type}, flag STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    session.sql(f"INSERT INTO {table} VALUES " + ", ".join(rows))


def _matrix_sql(seat: str, names: dict[str, str], predicate: str) -> str:
    t, t2, t3 = names["t"], names["t2"], names["t3"]
    texts = {
        "select": f"SELECT id FROM {t} WHERE {predicate}",
        "delete": f"DELETE FROM {t} WHERE {predicate}",
        "update": f"UPDATE {t} SET flag = 'z' WHERE {predicate}",
        "update-set": f"UPDATE {t} SET flag = CASE WHEN {predicate} THEN 'z' ELSE flag END",
        "insert": f"INSERT INTO {t2} SELECT * FROM {t} WHERE {predicate}",
        "ctas": f"CREATE TABLE {t3} AS SELECT * FROM {t} WHERE {predicate}",
        "merge-when": (
            f"MERGE INTO {t} AS t USING (SELECT id FROM {t}) s ON t.id = s.id "
            f"WHEN MATCHED AND {predicate} THEN UPDATE SET flag = 'z'"
        ),
        "merge-on": (
            f"MERGE INTO {t} AS t USING (SELECT id FROM {t}) s ON t.id = s.id AND {predicate} "
            "WHEN MATCHED THEN UPDATE SET flag = 'z'"
        ),
    }
    return texts[seat]


def _short(table: str) -> str:
    return table.rsplit(".", 1)[-1]


def _frame_run(
    session: Any,
    fns: Any,
    seat: str,
    door: str,
    names: dict[str, str],
    predicate: str,
    pid: str,
) -> Any:
    t, t2, t3 = names["t"], names["t2"], names["t3"]
    column = typed_predicate(fns, pid) if door == "column-api" else fns.expr(predicate)
    condition = predicate if door == "filter-str" else column
    source = session.table(t)
    if seat == "select":
        if door == "select-expr":
            return source.selectExpr("id", f"({predicate}) AS hit").where("hit").select("id")
        return source.filter(condition).select("id")
    if seat == "insert":
        return source.filter(condition).write.insertInto(t2)
    if seat == "append":
        return source.filter(condition).writeTo(t2).append()
    if seat == "overwrite":
        return source.writeTo(t).overwrite(column)
    if seat == "create-or-replace":
        return source.filter(column).writeTo(t3).createOrReplace()
    if seat in {"merge-into-on", "merge-into-matched"}:
        target = _short(t)
        join = (
            f"{target}.id = s.id AND ({predicate})"
            if seat == "merge-into-on"
            else f"{target}.id = s.id"
        )
        clause = source.select("id").alias("s").mergeInto(t, fns.expr(join))
        if seat == "merge-into-on":
            return clause.whenMatched().update({"flag": fns.lit("z")}).merge()
        return clause.whenMatched(column).update({"flag": fns.lit("z")}).merge()
    raise ValueError(f"no DataFrame run for seat {seat!r}")


def _observe_matrix(
    session: Any, fns: Any, cell: Cell, names: dict[str, str], predicate: str
) -> Observed:
    combo = cell.combo
    assert combo is not None
    seat = combo.seat
    target = target_for(combo)
    before = None
    error = None
    probe_error = None
    try:
        before = _commit_count(session, names[target])
    except Exception as failure:
        probe_error = f"{type(failure).__name__}: {failure}"[:500]
    try:
        if combo.door == "spark-sql":
            result = session.sql(_matrix_sql(seat, names, predicate))
        else:
            result = _frame_run(session, fns, seat, combo.door, names, predicate, cell.pid)
        answer = _rows(result.collect()) if combo.is_read else None
        outcome = "ANSWERED"
    except Exception as failure:
        outcome = "PANIC" if "panic" in type(failure).__name__.lower() else "REFUSED"
        error = f"{type(failure).__name__}: {failure}"[:500]
        answer = None
    if not combo.is_read and outcome == "ANSWERED":
        answer = _rows(
            [
                (row[0],)
                for row in session.sql(f"SELECT id FROM {names[target]} ORDER BY id").collect()
            ]
        )
    commits = None
    if before is not None:
        try:
            commits = _commit_count(session, names[target]) - before
        except Exception as failure:
            probe_error = f"{type(failure).__name__}: {failure}"[:500]
            commits = None
    return Observed(outcome, answer, commits, error, probe_error)


def _observe_float(session: Any, cell: Cell, names: dict[str, str]) -> Observed:
    assert cell.shape is not None
    statement = FLOAT_SQL[cell.shape].format(t=names["t"], u=names["u"])
    error = None
    try:
        result = session.sql(statement)
        if cell.shape in FLOAT_WRITE_SHAPES:
            result.collect()
            answer = _rows(session.sql(f"SELECT id, flag FROM {names['t']}").collect())
        else:
            answer = _rows(result.collect())
        outcome = "ANSWERED"
    except Exception as failure:
        outcome = "PANIC" if "panic" in type(failure).__name__.lower() else "REFUSED"
        error = f"{type(failure).__name__}: {failure}"[:500]
        answer = None
    return Observed(outcome, answer, None, error)


def _names(cell: Cell, prefix: str) -> dict[str, str]:
    base = f"{prefix}{_slug(cell.cell_id)}"
    return {
        "t": f"{base}_t",
        "t2": f"{base}_t2",
        "t3": f"{base}_t3",
        "u": f"{base}_u",
        "o": f"{base}_o",
        "o2": f"{base}_o2",
        "o3": f"{base}_o3",
        "uo": f"{base}_uo",
    }


def _oracle_names(names: dict[str, str]) -> dict[str, str]:
    return {"t": names["o"], "t2": names["o2"], "t3": names["o3"], "u": names["uo"]}


@pytest.fixture(autouse=True)
def _isolate_active_session() -> Iterator[None]:
    yield


@pytest.fixture(scope="module")
def sessions() -> Any:
    pytest.importorskip("pyspark")
    assert_real_buckets_configured()
    if CATALOG == "glue":
        catalog = SILVER_CATALOG
        config = glue_catalog_config(SILVER_CATALOG, GLUE_WAREHOUSE)
        spark_config = config
    else:
        arn = os.environ.get("TABLE_BUCKET_ARN")
        if not arn:
            pytest.skip("S3 Tables flat matrix needs TABLE_BUCKET_ARN; absent, so skipped")
        catalog = S3TABLES_CATALOG
        config = s3tables_catalog_config(S3TABLES_CATALOG, arn)
        spark_config = s3tables_spark_catalog_config(S3TABLES_CATALOG, arn)
    builder = ReparkSession.builder.appName(f"flat-matrix-{CATALOG}")
    for key, value in config.items():
        builder = builder.config(key, value)
    repark = builder.getOrCreate()
    try:
        if CATALOG == "glue":
            assert_glue_scratch_namespace_location(repark, GLUE_WAREHOUSE)
        spark = _spark_session_for_catalog(spark_config, f"flat-matrix-{CATALOG}-spark")
        yield catalog, repark, spark
    finally:
        repark.stop()


@pytest.fixture(scope="module")
def records_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.getbasetemp() / "flat_matrix.jsonl"


def _record(path: Path, **fields: Any) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(record_line(**fields) + "\n")


def _describe(session: Any, table: str) -> list[list[str]]:
    fields: list[list[str]] = []
    for row in session.sql(f"DESCRIBE TABLE {table}").collect():
        name, data_type = str(row[0]), str(row[1])
        if not name or not data_type:
            break
        fields.append([name, data_type])
    return fields


def _file_counts(session: Any, table: str) -> dict[str, int]:
    counts = {"data": 0, "delete": 0}
    rows = session.sql(f"SELECT content, count(*) FROM {table}.files GROUP BY content").collect()
    for content, count in rows:
        counts["data" if int(content) == 0 else "delete"] += int(count)
    return counts


def _shape_fields(
    prefix: str, schema_session: Any, file_session: Any, table: str
) -> dict[str, Any]:
    fields: dict[str, Any] = {f"{prefix}_schema": None, f"{prefix}_files": None}
    fields[f"{prefix}_schema_error"] = None
    fields[f"{prefix}_files_error"] = None
    try:
        fields[f"{prefix}_schema"] = _describe(schema_session, table)
    except Exception as failure:
        fields[f"{prefix}_schema_error"] = f"{type(failure).__name__}: {failure}"[:500]
    try:
        fields[f"{prefix}_files"] = _file_counts(file_session, table)
    except Exception as failure:
        fields[f"{prefix}_files_error"] = f"{type(failure).__name__}: {failure}"[:500]
    return fields


def _absent_shape(prefix: str) -> dict[str, Any]:
    return {
        f"{prefix}_schema": None,
        f"{prefix}_schema_error": None,
        f"{prefix}_files": None,
        f"{prefix}_files_error": None,
    }


@pytest.mark.timeout(3600)
@pytest.mark.parametrize("cell", _params())
def test_flat_matrix_cell(cell: Cell, sessions: Any, records_path: Path) -> None:
    catalog, repark, spark = sessions
    prefix = run_table_prefix()
    names = _names(cell, prefix)
    glue = CATALOG == "glue"
    known = KNOWN_OPEN.get(cell.cell_id)
    if cell.is_float:
        float_type = cell.float_type or "DOUBLE"
        for key in ("t", "u"):
            _create_float_table(
                repark,
                _fq(catalog, names[key]),
                float_type,
                FLOAT_SEED_T if key == "t" else FLOAT_SEED_U,
            )
        repark_names = {key: _fq(catalog, names[key]) for key in ("t", "u")}
        mine = _observe_float(repark, cell, repark_names)
        oracle_fq = {"t": _fq(catalog, names["o"]), "u": _fq(catalog, names["uo"])}
        for key in ("t", "u"):
            _create_float_table(
                spark, oracle_fq[key], float_type, FLOAT_SEED_T if key == "t" else FLOAT_SEED_U
            )
        oracle = _observe_float(spark, cell, oracle_fq)
        source = "live-spark-oracle"
        label = classify(
            seat=cell.shape or "",
            pid=None,
            repark_outcome=mine.outcome,
            spark_outcome=oracle.outcome,
            repark_answer=mine.answer,
            spark_answer=oracle.answer,
        )
        seat_name = cell.shape
        repark_final = repark_names["t"]
        oracle_final: str | None = oracle_fq["t"]
    else:
        combo = cell.combo
        assert combo is not None and cell.pid is not None
        predicate = PREDICATE_TEXT[cell.pid]
        fq_names = {key: _fq(catalog, value) for key, value in names.items()}
        _create_matrix_table(repark, fq_names["t"], combo.layout)
        _create_matrix_table(repark, fq_names["t2"], combo.layout)
        repark.sql(f"INSERT INTO {fq_names['t']} VALUES " + ", ".join(SEED_ROWS))
        mine = _observe_matrix(repark, functions, cell, fq_names, predicate)
        expected_entry = expected_for(cell, EXPECTED)
        oracle_final = None
        if expected_entry is not None:
            source = "committed-spark"
            ids = committed_ids(combo, expected_entry)
            expected_answer = _rows([(value,) for value in ids])
            oracle = Observed(
                expected_entry["outcome"],
                expected_answer if expected_entry["outcome"] == "ANSWERED" else None,
                committed_commits(combo, expected_entry) if glue else None,
                None,
            )
        else:
            source = "live-spark-oracle"
            oracle_fq = {
                key: _fq(catalog, value) for key, value in names.items() if key in {"o", "o2", "o3"}
            }
            oracle_map = {"t": oracle_fq["o"], "t2": oracle_fq["o2"], "t3": oracle_fq["o3"]}
            _create_matrix_table(spark, oracle_map["t"], combo.layout)
            _create_matrix_table(spark, oracle_map["t2"], combo.layout)
            spark.sql(f"INSERT INTO {oracle_map['t']} VALUES " + ", ".join(SEED_ROWS))
            from pyspark.sql import functions as spark_functions

            oracle = _observe_matrix(spark, spark_functions, cell, oracle_map, predicate)
            oracle_final = oracle_map[target_for(combo)]
        seat_name = combo.seat
        repark_final = fq_names[target_for(combo)]
        label = classify(
            seat=seat_name,
            pid=cell.pid,
            repark_outcome=mine.outcome,
            spark_outcome=oracle.outcome,
            repark_answer=mine.answer,
            spark_answer=oracle.answer,
            repark_commits=mine.commits if glue else None,
            spark_commits=oracle.commits if glue else None,
            folded=cell.pid in FOLDED_PREDICATES,
        )
    shape = _shape_fields("repark", repark, spark, repark_final)
    if oracle_final is None:
        shape.update(_absent_shape("oracle"))
    else:
        shape.update(_shape_fields("oracle", spark, spark, oracle_final))
    _record(
        records_path,
        cell=cell.cell_id,
        catalog=CATALOG,
        group=GROUP,
        expected_source=source,
        known_open=known,
        repark_outcome=mine.outcome,
        repark_answer=repr(mine.answer),
        repark_commits=mine.commits,
        repark_error=mine.error,
        repark_probe_error=mine.probe_error,
        oracle_outcome=oracle.outcome,
        oracle_answer=repr(oracle.answer),
        oracle_commits=oracle.commits,
        oracle_error=oracle.error,
        oracle_probe_error=oracle.probe_error,
        label=label,
        **shape,
    )
    assert passes(label), f"{cell.cell_id}: label {label}"
