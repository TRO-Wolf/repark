from __future__ import annotations

import os
import re
from collections.abc import Iterator
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
    mask_account_ids,
    run_table_prefix,
    s3tables_catalog_config,
    s3tables_spark_catalog_config,
)
from _flat_matrix_cells import (
    ABSENT,
    CANONICAL_EXPRESSIONS,
    COLUMNS,
    FLOAT_SEED_T,
    FLOAT_SEED_U,
    FLOAT_SQL,
    FLOAT_WRITE_SHAPES,
    KNOWN_OPEN,
    PREDICATE_TEXT,
    SEED_ROWS,
    STAGE_CREATE_CATALOG,
    Cell,
    Observation,
    canonical_rows,
    cells_for_group,
    classify,
    committed_observation,
    expected_for,
    grade_cell,
    http_status,
    is_stage_create_cell,
    load_expected,
    passes,
    record_line,
    sorted_rows,
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


ERROR_LIMIT = 4000
CHAIN_LIMIT = 8
SESSION_ZONE = "UTC"


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _fq(catalog: str, name: str) -> str:
    return f"{catalog}.{ACCEPTANCE_NAMESPACE}.{name}"


def _normal(value: Any) -> Any:
    if isinstance(value, float):
        return repr(value)
    return value


def _rows(frame_rows: list[Any]) -> list[tuple[Any, ...]]:
    return sorted_rows(tuple(_normal(v) for v in row) for row in frame_rows)


def _error_text(failure: BaseException) -> str:
    return mask_account_ids(f"{type(failure).__name__}: {failure}")[:ERROR_LIMIT]


def _java_chain(failure: BaseException) -> list[tuple[str, str]]:
    node = getattr(failure, "java_exception", None) or getattr(failure, "_origin", None)
    chain: list[tuple[str, str]] = []
    while node is not None and len(chain) < CHAIN_LIMIT:
        try:
            chain.append((str(node.getClass().getName()), str(node.getMessage())))
            node = node.getCause()
        except Exception:
            break
    return chain


def _condition(failure: BaseException) -> str | None:
    getter = getattr(failure, "getCondition", None)
    if getter is None:
        return None
    try:
        value = getter()
    except Exception:
        return None
    return None if value is None else str(value)


def _failure_parts(failure: BaseException) -> tuple[str, str, str, int | None]:
    chain = _java_chain(failure)
    condition = _condition(failure)
    names = [type(failure).__name__, *(name for name, _ in chain)]
    if condition is not None:
        names.append(f"[{condition}]")
    text = " | ".join([*(f"{name}: {message}" for name, message in chain), _error_text(failure)])
    error = mask_account_ids(text)[:ERROR_LIMIT]
    outcome = "PANIC" if "panic" in type(failure).__name__.lower() else "REFUSED"
    return outcome, error, " > ".join(names), http_status(error)


def _snapshot_count(session: Any, table: str) -> int:
    return int(session.sql(f"SELECT count(*) FROM {table}.snapshots").collect()[0][0])


def _probe(session: Any, table: str) -> tuple[int | None, str | None]:
    try:
        return _snapshot_count(session, table), None
    except Exception as failure:
        return None, _error_text(failure)


def _spark_state(reader: Any, table: str) -> tuple[Any, Any, str | None]:
    try:
        if not reader.catalog.tableExists(table):
            return ABSENT, None, None
        reader.catalog.refreshTable(table)
        frame = reader.table(table)
        schema = tuple(
            (item.name, item.dataType.simpleString(), bool(item.nullable))
            for item in frame.schema.fields
        )
        return canonical_rows(frame.selectExpr(*CANONICAL_EXPRESSIONS).collect()), schema, None
    except Exception as failure:
        return None, None, _error_text(failure)


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


def _run_statement(
    session: Any, fns: Any, cell: Cell, names: dict[str, str], predicate: str
) -> Any:
    combo = cell.combo
    assert combo is not None and cell.pid is not None
    if combo.door == "spark-sql":
        return session.sql(_matrix_sql(combo.seat, names, predicate))
    return _frame_run(session, fns, combo.seat, combo.door, names, predicate, cell.pid)


def _observe_matrix(
    session: Any, fns: Any, reader: Any, cell: Cell, names: dict[str, str], predicate: str
) -> Observation:
    combo = cell.combo
    assert combo is not None
    target = names[target_for(combo)]
    before, probe_error = (0, None) if target_for(combo) == "t3" else _probe(session, target)
    error = error_class = None
    status = None
    try:
        result = _run_statement(session, fns, cell, names, predicate)
        answer = _rows(result.collect()) if combo.is_read else None
        outcome = "ANSWERED"
    except Exception as failure:
        outcome, error, error_class, status = _failure_parts(failure)
        answer = None
    if not combo.is_read and outcome == "ANSWERED":
        answer = _rows(
            [(row[0],) for row in session.sql(f"SELECT id FROM {target} ORDER BY id").collect()]
        )
    state, schema, read_error = _spark_state(reader, target)
    commits = None
    if before is not None:
        after, after_error = (0, None) if state == ABSENT else _probe(session, target)
        probe_error = probe_error or after_error
        commits = None if after is None else after - before
    return Observation(
        outcome, answer, state, schema, commits, error, error_class, status, read_error, probe_error
    )


def _stage_create_reference(spark: Any, fns: Any, pid: str) -> tuple[Any, str | None]:
    values = ", ".join(SEED_ROWS)
    try:
        seed = spark.sql(f"SELECT * FROM VALUES {values} AS v({', '.join(COLUMNS)})")
        rows = seed.filter(typed_predicate(fns, pid)).selectExpr(*CANONICAL_EXPRESSIONS).collect()
    except Exception as failure:
        return None, _error_text(failure)
    return canonical_rows(rows), None


def _observe_float(session: Any, cell: Cell, names: dict[str, str]) -> Observation:
    assert cell.shape is not None
    statement = FLOAT_SQL[cell.shape].format(t=names["t"], u=names["u"])
    error = error_class = None
    try:
        result = session.sql(statement)
        if cell.shape in FLOAT_WRITE_SHAPES:
            result.collect()
            answer = _rows(session.sql(f"SELECT id, flag FROM {names['t']}").collect())
        else:
            answer = _rows(result.collect())
        outcome = "ANSWERED"
    except Exception as failure:
        outcome, error, error_class, _ = _failure_parts(failure)
        answer = None
    return Observation(outcome, answer, error=error, error_class=error_class)


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
    builder = builder.config("spark.sql.session.timeZone", SESSION_ZONE)
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


def _file_counts(session: Any, table: str) -> dict[str, int]:
    counts = {"data": 0, "delete": 0}
    rows = session.sql(f"SELECT content, count(*) FROM {table}.files GROUP BY content").collect()
    for content, count in rows:
        counts["data" if int(content) == 0 else "delete"] += int(count)
    return counts


def _files(prefix: str, session: Any, table: str | None) -> dict[str, Any]:
    fields: dict[str, Any] = {f"{prefix}_files": None, f"{prefix}_files_error": None}
    if table is None:
        return fields
    try:
        fields[f"{prefix}_files"] = _file_counts(session, table)
    except Exception as failure:
        fields[f"{prefix}_files_error"] = _error_text(failure)
    return fields


def _side(prefix: str, observed: Observation) -> dict[str, Any]:
    return {
        f"{prefix}_outcome": observed.outcome,
        f"{prefix}_answer": repr(observed.answer),
        f"{prefix}_state": observed.state,
        f"{prefix}_schema": observed.schema,
        f"{prefix}_commits": observed.commits,
        f"{prefix}_error": observed.error,
        f"{prefix}_error_class": observed.error_class,
        f"{prefix}_status": observed.status,
        f"{prefix}_read_error": observed.read_error,
        f"{prefix}_probe_error": observed.probe_error,
    }


def _float_cell(cell: Cell, catalog: str, repark: Any, spark: Any, names: dict[str, str]) -> Any:
    float_type = cell.float_type or "DOUBLE"
    mine_fq = {key: _fq(catalog, names[key]) for key in ("t", "u")}
    oracle_fq = {"t": _fq(catalog, names["o"]), "u": _fq(catalog, names["uo"])}
    for session, tables in ((repark, mine_fq), (spark, oracle_fq)):
        for key in ("t", "u"):
            seed = FLOAT_SEED_T if key == "t" else FLOAT_SEED_U
            _create_float_table(session, tables[key], float_type, seed)
    mine = _observe_float(repark, cell, mine_fq)
    oracle = _observe_float(spark, cell, oracle_fq)
    label = classify(
        seat=cell.shape or "",
        pid=None,
        repark_outcome=mine.outcome,
        spark_outcome=oracle.outcome,
        repark_answer=mine.answer,
        spark_answer=oracle.answer,
    )
    fields = {"expected_source": "live-spark-oracle", "label": label}
    return label, mine, oracle, mine_fq["t"], oracle_fq["t"], fields


def _matrix_cell(
    cell: Cell, catalog_kind: str, catalog: str, repark: Any, spark: Any, names: dict[str, str]
) -> Any:
    combo = cell.combo
    assert combo is not None and cell.pid is not None
    predicate = PREDICATE_TEXT[cell.pid]
    fq = {key: _fq(catalog, value) for key, value in names.items()}
    _create_matrix_table(repark, fq["t"], combo.layout)
    _create_matrix_table(repark, fq["t2"], combo.layout)
    repark.sql(f"INSERT INTO {fq['t']} VALUES " + ", ".join(SEED_ROWS))
    mine = _observe_matrix(repark, functions, spark, cell, fq, predicate)
    entry = expected_for(cell, EXPECTED)
    oracle_final = None
    reference, reference_error = None, None
    if entry is not None:
        oracle = committed_observation(cell, entry)
    else:
        from pyspark.sql import functions as spark_functions

        oracle_map = {"t": fq["o"], "t2": fq["o2"], "t3": fq["o3"]}
        _create_matrix_table(spark, oracle_map["t"], combo.layout)
        _create_matrix_table(spark, oracle_map["t2"], combo.layout)
        spark.sql(f"INSERT INTO {oracle_map['t']} VALUES " + ", ".join(SEED_ROWS))
        oracle = _observe_matrix(spark, spark_functions, spark, cell, oracle_map, predicate)
        oracle_final = oracle_map[target_for(combo)]
        if is_stage_create_cell(cell) and catalog_kind == STAGE_CREATE_CATALOG:
            reference, reference_error = _stage_create_reference(spark, spark_functions, cell.pid)
    grade = grade_cell(
        cell, catalog_kind, mine, oracle, live_oracle=entry is None, reference_state=reference
    )
    fields = {
        "expected_source": "committed-spark" if entry is not None else "live-spark-oracle",
        "label": grade.label,
        "commits_graded": grade.commits_graded,
        "missing": list(grade.missing),
        "truth_answer": grade.truth,
        "oracle_truth": grade.oracle_truth,
        "expected_answer": repr(grade.expected.answer),
        "expected_state": grade.expected.state,
        "expected_schema": grade.expected.schema,
        "expected_commits": grade.expected.commits,
        "stage_create": grade.stage_create,
        "stage_create_reference_error": reference_error,
    }
    return grade.label, mine, oracle, fq[target_for(combo)], oracle_final, fields


def run_cell(
    cell: Cell,
    catalog_kind: str,
    catalog: str,
    repark: Any,
    spark: Any,
    records_path: Path,
    group: str,
) -> str:
    names = _names(cell, run_table_prefix())
    if cell.is_float:
        result = _float_cell(cell, catalog, repark, spark, names)
    else:
        result = _matrix_cell(cell, catalog_kind, catalog, repark, spark, names)
    label, mine, oracle, repark_final, oracle_final, fields = result
    _record(
        records_path,
        cell=cell.cell_id,
        catalog=catalog_kind,
        group=group,
        known_open=KNOWN_OPEN.get(cell.cell_id),
        **fields,
        **_side("repark", mine),
        **_side("oracle", oracle),
        **_files("repark", spark, repark_final),
        **_files("oracle", spark, oracle_final),
    )
    return label


@pytest.mark.timeout(3600)
@pytest.mark.parametrize("cell", _params())
def test_flat_matrix_cell(cell: Cell, sessions: Any, records_path: Path) -> None:
    catalog, repark, spark = sessions
    label = run_cell(cell, CATALOG, catalog, repark, spark, records_path, GROUP)
    assert passes(label), f"{cell.cell_id}: label {label}"
