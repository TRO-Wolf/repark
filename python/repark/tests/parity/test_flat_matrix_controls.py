from __future__ import annotations

from typing import Any

import pytest
from _flat_matrix_cells import (
    DDL_SCHEMA,
    SEED_CANONICAL,
    STAGE_CREATE_ERROR_CLASS,
    STAGE_CREATE_MESSAGE,
    Observation,
    committed_observation,
    expected_for,
    grade_cell,
    load_expected,
    matrix_cells,
    passes,
)

UPDATE_P15 = "spark-sql|update|v2-cow|p15"
UPDATE_P08 = "spark-sql|update|v2-cow|p08"
CREATE_P15 = "column-api|create-or-replace|v2-cow|p15"
CREATE_P01 = "column-api|create-or-replace|v2-cow|p01"

AUDIT_FLAGS = {
    "correct update": "f1 f2 f3 z f5 z f7 f8",
    "opposite rows": "z z z f4 z f6 z z",
    "no-op": "f1 f2 f3 f4 f5 f6 f7 f8",
    "missing id": "f1 f2 f3 z f5 z f7",
}
SEED_FLAGS = AUDIT_FLAGS["no-op"]
STAGE_ERROR = (
    f"org.apache.iceberg.exceptions.BadRequestException: Malformed request: {STAGE_CREATE_MESSAGE}"
)
STAGE_CLASS = f"Py4JJavaError > org.apache.iceberg.exceptions.{STAGE_CREATE_ERROR_CLASS}"


def _cell(cell_id: str) -> Any:
    return next(cell for cell in matrix_cells() if cell.cell_id == cell_id)


def _seed_row(row_id: int) -> tuple[str | None, ...]:
    return next(row for row in SEED_CANONICAL if row[0] == str(row_id))


def _state(flags: str) -> list[tuple[str | None, ...]]:
    rows = [(*_seed_row(n)[:-1], flag) for n, flag in enumerate(flags.split(), start=1)]
    return sorted(rows, key=repr)


def _rows(ids: list[int]) -> list[tuple[str | None, ...]]:
    return sorted((_seed_row(n) for n in ids), key=repr)


def _answer(state: list[tuple[str | None, ...]]) -> list[tuple[int]]:
    return sorted(((int(str(row[0])),) for row in state), key=repr)


def _written(flags: str, *, outcome: str = "ANSWERED", commits: int | None = 1) -> Observation:
    state = _state(flags)
    answer = _answer(state) if outcome == "ANSWERED" else None
    return Observation(outcome, answer=answer, state=state, schema=DDL_SCHEMA, commits=commits)


def _committed(cell_id: str) -> Observation:
    entry = expected_for(_cell(cell_id), load_expected())
    assert entry is not None
    return committed_observation(_cell(cell_id), entry)


def _created(ids: list[int]) -> Observation:
    state = _rows(ids)
    return Observation("ANSWERED", answer=_answer(state), state=state, schema=DDL_SCHEMA, commits=1)


def _refused(error: str, error_class: str, status: int | None = None) -> Observation:
    return Observation("REFUSED", error=error, error_class=error_class, status=status)


UPDATE_CONTROLS: tuple[tuple[str, str, str, Observation, str], ...] = (
    ("correct update", "glue", UPDATE_P15, _written(AUDIT_FLAGS["correct update"]), "EQUAL"),
    ("opposite rows", "glue", UPDATE_P15, _written(AUDIT_FLAGS["opposite rows"]), "WRONG-WRITE"),
    ("no-op", "glue", UPDATE_P15, _written(AUDIT_FLAGS["no-op"]), "WRONG-WRITE"),
    ("missing id", "glue", UPDATE_P15, _written(AUDIT_FLAGS["missing id"]), "WRONG-WRITE"),
    (
        "opposite rows, S3 Tables",
        "s3tables",
        UPDATE_P15,
        _written(AUDIT_FLAGS["opposite rows"]),
        "WRONG-WRITE",
    ),
    ("no-op, S3 Tables", "s3tables", UPDATE_P15, _written(AUDIT_FLAGS["no-op"]), "WRONG-WRITE"),
    (
        "refusal that left changed state",
        "glue",
        UPDATE_P08,
        _written(AUDIT_FLAGS["correct update"], outcome="REFUSED", commits=0),
        "BOTH-REFUSED, WRONG-WRITE",
    ),
    (
        "refusal that left changed state and a commit",
        "glue",
        UPDATE_P08,
        _written(AUDIT_FLAGS["correct update"], outcome="REFUSED", commits=1),
        "BOTH-REFUSED, EXTRA-COMMIT, WRONG-WRITE",
    ),
    (
        "clean refusal",
        "glue",
        UPDATE_P08,
        _written(SEED_FLAGS, outcome="REFUSED", commits=0),
        "BOTH-REFUSED",
    ),
    (
        "unreadable Glue commit count",
        "glue",
        UPDATE_P15,
        _written(AUDIT_FLAGS["correct update"], commits=None),
        "OBSERVATION-MISSING",
    ),
    (
        "unreadable state",
        "s3tables",
        UPDATE_P15,
        Observation("ANSWERED", answer=_answer(_state(SEED_FLAGS)), read_error="read failed"),
        "OBSERVATION-MISSING",
    ),
    (
        "S3 Tables commit count not graded",
        "s3tables",
        UPDATE_P15,
        _written(AUDIT_FLAGS["correct update"], commits=None),
        "EQUAL",
    ),
)

STAGE_CONTROLS: tuple[tuple[str, str, str, Observation, Observation, Any, str], ...] = (
    (
        "exact refusal, readback equal",
        "s3tables",
        CREATE_P15,
        _created([4, 6]),
        _refused(STAGE_ERROR, STAGE_CLASS),
        None,
        "DECLARED-STAGE-CREATE",
    ),
    (
        "exact refusal with a status 400",
        "s3tables",
        CREATE_P15,
        _created([4, 6]),
        _refused(STAGE_ERROR, STAGE_CLASS, 400),
        None,
        "DECLARED-STAGE-CREATE",
    ),
    (
        "exact refusal, live reference",
        "s3tables",
        CREATE_P01,
        _created([1, 8]),
        _refused(STAGE_ERROR, STAGE_CLASS),
        _rows([1, 8]),
        "DECLARED-STAGE-CREATE",
    ),
    (
        "exact refusal, no reference",
        "s3tables",
        CREATE_P01,
        _created([1, 8]),
        _refused(STAGE_ERROR, STAGE_CLASS),
        None,
        "ACCEPTED-WHERE-SPARK-REFUSED",
    ),
    (
        "different Spark message",
        "s3tables",
        CREATE_P15,
        _created([4, 6]),
        _refused("BadRequestException: Malformed request: table exists", STAGE_CLASS),
        None,
        "ACCEPTED-WHERE-SPARK-REFUSED",
    ),
    (
        "message without the error class",
        "s3tables",
        CREATE_P15,
        _created([4, 6]),
        _refused(STAGE_CREATE_MESSAGE, "Py4JJavaError > java.lang.RuntimeException"),
        None,
        "ACCEPTED-WHERE-SPARK-REFUSED",
    ),
    (
        "Spark success",
        "s3tables",
        CREATE_P15,
        _created([4, 6]),
        _created([4, 6]),
        None,
        "STAGE-CREATE-ANSWERED",
    ),
    (
        "same refusal on Glue",
        "glue",
        CREATE_P15,
        _created([4, 6]),
        _refused(STAGE_ERROR, STAGE_CLASS),
        None,
        "ACCEPTED-WHERE-SPARK-REFUSED",
    ),
    (
        "readback differs",
        "s3tables",
        CREATE_P15,
        _created([6]),
        _refused(STAGE_ERROR, STAGE_CLASS),
        None,
        "ACCEPTED-WHERE-SPARK-REFUSED, WRONG-WRITE",
    ),
)


@pytest.mark.parametrize(
    ("name", "catalog", "cell_id", "repark", "label"),
    UPDATE_CONTROLS,
    ids=[control[0] for control in UPDATE_CONTROLS],
)
def test_update_control_grades_on_the_persisted_rows(
    name: str, catalog: str, cell_id: str, repark: Observation, label: str
) -> None:
    grade = grade_cell(_cell(cell_id), catalog, repark, _committed(cell_id), live_oracle=False)
    assert grade.label == label, name
    assert passes(grade.label) is (label in {"EQUAL", "BOTH-REFUSED"}), name


@pytest.mark.parametrize(
    ("name", "catalog", "cell_id", "repark", "oracle", "reference", "label"),
    STAGE_CONTROLS,
    ids=[control[0] for control in STAGE_CONTROLS],
)
def test_stage_create_control(
    name: str,
    catalog: str,
    cell_id: str,
    repark: Observation,
    oracle: Observation,
    reference: Any,
    label: str,
) -> None:
    grade = grade_cell(
        _cell(cell_id), catalog, repark, oracle, live_oracle=True, reference_state=reference
    )
    assert grade.label == label, name
    assert passes(grade.label) is (label == "DECLARED-STAGE-CREATE"), name
