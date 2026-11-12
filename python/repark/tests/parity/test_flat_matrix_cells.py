from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

import pytest
from _flat_matrix_cells import (
    COMBINATIONS,
    FLOAT_SEED_T,
    FLOAT_SEED_U,
    FOLDED_PREDICATES,
    GROUP_NAMES,
    KNOWN_OPEN,
    NEGATIVE_ZERO,
    ORACLE_AGREES_WITH_TRUTH,
    PREDICATES,
    PRUNED_WRONG,
    SIGNED_ZERO_PR_B,
    TRUTH_PREDICATES,
    TYPED_BUILDERS,
    cells_for_group,
    class_a_key,
    classify,
    committed_commits,
    committed_ids,
    expected_for,
    float_cells,
    grade_against_truth,
    load_expected,
    matrix_cells,
    oracle_truth_label,
    passes,
    seed_doubles,
    truth_ids,
    truth_match_ids,
    typed_predicate,
    universe,
)

ENVELOPE_DIR = (
    Path(__file__).resolve().parents[4]
    / "task"
    / "wo"
    / "write-path-envelope-matrix-1"
    / "evidence"
)
ENVELOPE_SEAT_NAMES = {"merge": "merge-when"}

EXPECTED_GROUP_CELLS = {
    "pilot": 30,
    "floatkeys": 28,
    "J1": 168,
    "J2": 112,
    "J3a": 140,
    "J3b": 56,
    "J4a": 112,
    "J4b": 112,
    "J5a": 112,
    "J5b": 28,
}


def test_predicate_count_is_the_design_28() -> None:
    assert len(PREDICATES) == 28


def test_combination_count_is_the_design_30() -> None:
    assert len(COMBINATIONS) == 30
    assert [combo.number for combo in COMBINATIONS] == list(range(1, 31))


def test_matrix_has_840_cells() -> None:
    assert len(matrix_cells()) == 840


def test_float_group_has_28_cells() -> None:
    assert len(float_cells()) == 28


@pytest.mark.parametrize("group", GROUP_NAMES)
def test_group_cell_count_matches_design(group: str) -> None:
    assert len(cells_for_group(group)) == EXPECTED_GROUP_CELLS[group]


def test_job_groups_partition_the_matrix() -> None:
    job_groups = [name for name in GROUP_NAMES if name.startswith("J")]
    members = [cell.cell_id for name in job_groups for cell in cells_for_group(name)]
    assert len(members) == 840
    assert len(set(members)) == 840


def test_all_group_is_the_whole_universe() -> None:
    assert len(cells_for_group("all")) == 868 == len(universe())


def test_cell_ids_are_unique() -> None:
    ids = [cell.cell_id for cell in universe()]
    assert len(ids) == len(set(ids))


def test_unknown_group_is_refused() -> None:
    with pytest.raises(ValueError):
        cells_for_group("J9")


def test_every_class_a_cell_finds_its_committed_value() -> None:
    table = load_expected()
    class_a = [cell for cell in matrix_cells() if class_a_key(cell) is not None]
    assert len(class_a) == 420
    for cell in class_a:
        found = expected_for(cell, table)
        assert found is not None
        assert found["outcome"] in {"ANSWERED", "REFUSED"}


def test_class_b_cells_have_no_committed_key() -> None:
    class_b = [
        cell for cell in matrix_cells() if cell.combo is not None and cell.combo.klass == "B"
    ]
    assert len(class_b) == 420
    assert all(class_a_key(cell) is None for cell in class_b)


def test_known_open_dict_covers_the_float_key_cells_only() -> None:
    assert len(KNOWN_OPEN) == 28
    assert set(KNOWN_OPEN.values()) == {SIGNED_ZERO_PR_B}
    assert set(KNOWN_OPEN) == {cell.cell_id for cell in float_cells()}
    assert not [cell_id for cell_id in KNOWN_OPEN if cell_id.endswith("|p15")]


ALL_IDS = [1, 2, 3, 4, 5, 6, 7, 8]
P15_TRUTH_BY_SEAT = {
    "select": [4, 6],
    "insert": [4, 6],
    "append": [4, 6],
    "ctas": [4, 6],
    "create-or-replace": [4, 6],
    "delete": [1, 2, 3, 5, 7, 8],
    "update": ALL_IDS,
    "update-set": ALL_IDS,
    "merge-when": ALL_IDS,
    "merge-on": ALL_IDS,
    "merge-into-on": ALL_IDS,
    "merge-into-matched": ALL_IDS,
    "overwrite": [1, 1, 2, 2, 3, 3, 4, 5, 5, 6, 7, 7, 8, 8],
}


def _p15_cells() -> list[Any]:
    return [cell for cell in matrix_cells() if cell.pid == "p15"]


def test_the_seed_holds_both_zeros_on_rows_4_and_6() -> None:
    doubles = seed_doubles()
    assert set(doubles) == set(ALL_IDS)
    negative, positive, nan = doubles[4], doubles[6], doubles[3]
    assert negative is not None and positive is not None and nan is not None
    assert negative == 0.0 and math.copysign(1.0, negative) == -1.0
    assert positive == 0.0 and math.copysign(1.0, positive) == 1.0
    assert math.isnan(nan)
    assert doubles[2] is None and doubles[7] is None


def test_p15_truth_matches_both_zeros_under_ieee_equality() -> None:
    assert {"p15"} == TRUTH_PREDICATES
    assert truth_match_ids("p15") == [4, 6]
    with pytest.raises(ValueError):
        truth_match_ids("p01")


def test_the_truth_table_names_every_seat_in_the_matrix() -> None:
    assert {combo.seat for combo in COMBINATIONS} == set(P15_TRUTH_BY_SEAT)


@pytest.mark.parametrize("cell", _p15_cells(), ids=lambda cell: cell.cell_id)
def test_p15_truth_per_seat_comes_from_the_seed(cell: Any) -> None:
    assert cell.combo is not None
    assert truth_ids(cell) == P15_TRUTH_BY_SEAT[cell.combo.seat]


def test_the_committed_p15_values_already_agree_with_the_truth() -> None:
    table = load_expected()
    committed = [cell for cell in _p15_cells() if class_a_key(cell) is not None]
    assert len(committed) == 15
    for cell in committed:
        entry = expected_for(cell, table)
        assert entry is not None and cell.combo is not None
        assert entry["outcome"] == "ANSWERED"
        assert committed_ids(cell.combo, entry) == truth_ids(cell), cell.cell_id


def test_no_truth_column_outside_p15() -> None:
    others = [cell for cell in universe() if cell.pid != "p15"]
    assert len(others) == 868 - 30
    assert all(truth_ids(cell) is None for cell in others)
    assert all(grade_against_truth(cell, "ANSWERED", [1]) == (None, None) for cell in others)


def test_the_effective_class_a_table_equals_the_committed_table() -> None:
    table = load_expected()
    before: dict[str, Any] = {}
    after: dict[str, Any] = {}
    for cell in matrix_cells():
        entry = expected_for(cell, table)
        if entry is None or cell.combo is None:
            continue
        ids = committed_ids(cell.combo, entry)
        before[cell.cell_id] = (entry["outcome"], ids, committed_commits(cell.combo, entry))
        truth, _ = grade_against_truth(cell, entry["outcome"], ids)
        graded = ids if truth is None else truth
        after[cell.cell_id] = (entry["outcome"], graded, committed_commits(cell.combo, entry))
    assert len(before) == 420
    assert after == before


def test_a_zero_only_oracle_answer_is_labelled_pruned_wrong() -> None:
    select = next(cell for cell in _p15_cells() if cell.cell_id == "column-api|select|v2-cow|p15")
    delete = next(cell for cell in _p15_cells() if cell.cell_id == "spark-sql|delete|v2-cow|p15")
    assert grade_against_truth(select, "ANSWERED", [6]) == ([4, 6], PRUNED_WRONG)
    assert grade_against_truth(select, "ANSWERED", [6, 4]) == ([4, 6], ORACLE_AGREES_WITH_TRUTH)
    assert grade_against_truth(delete, "ANSWERED", [1, 2, 3, 4, 5, 7, 8]) == (
        [1, 2, 3, 5, 7, 8],
        PRUNED_WRONG,
    )
    assert oracle_truth_label(None, [4, 6]) == PRUNED_WRONG


def test_a_refused_oracle_keeps_its_refusal() -> None:
    overwrite = next(cell for cell in _p15_cells() if cell.cell_id == "f-expr|overwrite|v2-cow|p15")
    assert grade_against_truth(overwrite, "REFUSED", None) == (None, None)
    label = classify(
        seat="overwrite",
        pid="p15",
        repark_outcome="REFUSED",
        spark_outcome="REFUSED",
        repark_answer=None,
        spark_answer=None,
    )
    assert label == "BOTH-REFUSED" and passes(label)


def test_repark_is_graded_against_the_truth_not_the_pruned_oracle() -> None:
    cell = next(cell for cell in _p15_cells() if cell.cell_id == "column-api|select|v2-cow|p15")
    truth, oracle_label = grade_against_truth(cell, "ANSWERED", [6])
    assert truth is not None and oracle_label == PRUNED_WRONG
    graded = classify(
        seat="select",
        pid="p15",
        repark_outcome="ANSWERED",
        spark_outcome="ANSWERED",
        repark_answer=[(value,) for value in [4, 6]],
        spark_answer=[(value,) for value in truth],
    )
    pruned_repark = classify(
        seat="select",
        pid="p15",
        repark_outcome="ANSWERED",
        spark_outcome="ANSWERED",
        repark_answer=[(6,)],
        spark_answer=[(value,) for value in truth],
    )
    assert graded == "EQUAL"
    assert pruned_repark == "WRONG-READ"


def test_classifier_equal_when_answers_match() -> None:
    label = classify(
        seat="select",
        pid="p01",
        repark_outcome="ANSWERED",
        spark_outcome="ANSWERED",
        repark_answer=[(1,)],
        spark_answer=[(1,)],
    )
    assert label == "EQUAL"


def test_classifier_wrong_read_and_wrong_write() -> None:
    read = classify(
        seat="select",
        pid="p15",
        repark_outcome="ANSWERED",
        spark_outcome="ANSWERED",
        repark_answer=[(6,)],
        spark_answer=[(2,), (4,), (6,)],
    )
    write = classify(
        seat="delete",
        pid="p15",
        repark_outcome="ANSWERED",
        spark_outcome="ANSWERED",
        repark_answer=[(6,)],
        spark_answer=[(2,), (4,), (6,)],
    )
    assert read == "WRONG-READ"
    assert write == "WRONG-WRITE"


def test_classifier_refusal_directions() -> None:
    both = classify(
        seat="select",
        pid="p08",
        repark_outcome="REFUSED",
        spark_outcome="REFUSED",
        repark_answer=None,
        spark_answer=None,
    )
    over = classify(
        seat="select",
        pid="p36",
        repark_outcome="REFUSED",
        spark_outcome="ANSWERED",
        repark_answer=None,
        spark_answer=[(1,)],
    )
    under = classify(
        seat="select",
        pid="p01",
        repark_outcome="ANSWERED",
        spark_outcome="REFUSED",
        repark_answer=[(1,)],
        spark_answer=None,
    )
    assert both == "BOTH-REFUSED"
    assert over == "REFUSED-WHERE-SPARK-ACCEPTED"
    assert under == "ACCEPTED-WHERE-SPARK-REFUSED"


def test_classifier_declared_empty_dml_and_commit_labels() -> None:
    declared = classify(
        seat="update",
        pid="p04",
        repark_outcome="ANSWERED",
        spark_outcome="ANSWERED",
        repark_answer=[(1,)],
        spark_answer=[(1,)],
        repark_commits=0,
        spark_commits=1,
        folded=True,
    )
    fewer = classify(
        seat="update",
        pid="p01",
        repark_outcome="ANSWERED",
        spark_outcome="ANSWERED",
        repark_answer=[(1,)],
        spark_answer=[(1,)],
        repark_commits=0,
        spark_commits=1,
    )
    assert declared == "KNOWN-EMPTY-DML, FEWER-COMMIT"
    assert fewer == "FEWER-COMMIT"
    assert passes(declared)
    assert not passes(fewer)


def test_folded_empty_dml_with_a_wrong_write_still_fails() -> None:
    label = classify(
        seat="delete",
        pid="p04",
        repark_outcome="ANSWERED",
        spark_outcome="ANSWERED",
        repark_answer=[(6,)],
        spark_answer=[(2,), (4,)],
        folded=True,
    )
    assert label == "KNOWN-EMPTY-DML, WRONG-WRITE"
    assert not passes(label)


def test_folded_predicates_are_the_ones_spark_folds_to_empty() -> None:
    spark_records = _raw_cells("spark-*.jsonl")
    folded = {record["pid"] for record in spark_records.values() if record["plan_empty"] is True}
    assert folded == FOLDED_PREDICATES == {"p04"}


def test_every_scalar_predicate_has_a_column_api_builder() -> None:
    assert set(TYPED_BUILDERS) == {pid for pid, _ in PREDICATES}
    assert len(TYPED_BUILDERS) == 28
    for pid, builder in TYPED_BUILDERS.items():
        assert "expr" not in builder.__code__.co_names, pid


def test_typed_predicate_refuses_an_unknown_predicate() -> None:
    with pytest.raises(ValueError):
        typed_predicate(object(), "p99")


def _raw_cells(pattern: str) -> dict[str, dict[str, Any]]:
    cells: dict[str, dict[str, Any]] = {}
    for path in sorted(ENVELOPE_DIR.glob("raw/" + pattern)):
        for line in path.read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            if record.get("record") == "cell" and record["door"] != "native":
                cells[record["key"]] = record
    return cells


def _envelope_labels() -> dict[str, str]:
    labels: dict[str, str] = {}
    table = (ENVELOPE_DIR / "envelope-table.md").read_text(encoding="utf-8")
    for line in table.splitlines():
        if not re.match(r"\| p\d+ \|", line):
            continue
        parts = [part.strip() for part in re.split(r"(?<!\\)\|", line)]
        pid, door, seat, kind = parts[1], parts[2], parts[3], parts[4]
        if door == "native":
            continue
        labels[f"{door}|{seat}|{kind}|{pid}"] = parts[9]
    return labels


def _delta(record: dict[str, Any], before: str, after: str) -> int:
    if record[before] is None or record[after] is None:
        return 0
    return record[after]["count"] - record[before]["count"]


def _target_delta(record: dict[str, Any]) -> int:
    return 0 if record["target"] is None else record["target"]["snapshot"]["count"]


def _own_ids(record: dict[str, Any]) -> list[Any] | None:
    pre = record["pre_select"]
    return pre["ids"] if pre is not None and pre["outcome"] == "ANSWERED" else None


def test_classifier_reproduces_the_committed_envelope_labels() -> None:
    repark_cells = _raw_cells("repark-main-*.jsonl")
    spark_cells = _raw_cells("spark-*.jsonl")
    folded = FOLDED_PREDICATES
    expected = _envelope_labels()
    compared: list[str] = []
    seats: set[str] = set()
    doors: set[str] = set()
    for key, label in sorted(expected.items()):
        record = repark_cells[key]
        oracle = spark_cells[key]
        seat = ENVELOPE_SEAT_NAMES.get(record["seat"], record["seat"])
        actual = classify(
            seat=seat,
            pid=record["pid"],
            repark_outcome=record["outcome"],
            spark_outcome=oracle["outcome"],
            repark_answer=record["ids"],
            spark_answer=oracle["ids"],
            repark_commits=_delta(record, "snapshot_before", "snapshot_after"),
            spark_commits=_delta(oracle, "snapshot_before", "snapshot_after"),
            repark_target_commits=_target_delta(record),
            spark_target_commits=_target_delta(oracle),
            repark_damaged=record["damaged"],
            spark_damaged=oracle["damaged"],
            repark_lost=record["lost"],
            spark_lost=oracle["lost"],
            repark_own=_own_ids(record),
            repark_lineage=record["lineage_advanced"],
            spark_lineage=oracle["lineage_advanced"],
            folded=record["pid"] in folded,
        )
        assert actual == label, f"{key}: classify gave {actual!r}, envelope gave {label!r}"
        compared.append(key)
        seats.add(seat)
        doors.add(record["door"])
    assert len(compared) >= 200
    assert {"select", "insert", "ctas", "delete", "update", "merge-when"} <= seats
    assert {"spark-sql", "f-expr", "filter-str"} <= doors


def test_copy_seats_compare_the_target_ids_and_skip_the_source_commits() -> None:
    table = load_expected()
    copies = [
        cell
        for cell in matrix_cells()
        if class_a_key(cell) is not None
        and cell.combo is not None
        and cell.combo.seat in {"insert", "ctas"}
    ]
    assert copies
    for cell in copies:
        entry = expected_for(cell, table)
        assert entry is not None
        assert cell.combo is not None
        assert committed_ids(cell.combo, entry) == [int(value) for value in entry["ids"] or []]
        assert committed_commits(cell.combo, entry) is None


def test_p01_insert_expects_the_two_copied_rows() -> None:
    table = load_expected()
    cell = next(cell for cell in matrix_cells() if cell.cell_id == "spark-sql|insert|v2-cow|p01")
    entry = expected_for(cell, table)
    assert entry is not None
    assert cell.combo is not None
    assert committed_ids(cell.combo, entry) == [1, 8]


def test_dml_seats_compare_the_table_after_the_statement() -> None:
    table = load_expected()
    cell = next(cell for cell in matrix_cells() if cell.cell_id == "spark-sql|delete|v2-cow|p01")
    entry = expected_for(cell, table)
    assert entry is not None
    assert cell.combo is not None
    assert committed_ids(cell.combo, entry) == [int(value) for value in entry["after_ids"] or []]
    assert committed_commits(cell.combo, entry) == entry["commits"]


def test_an_unset_expected_commit_count_is_not_compared() -> None:
    label = classify(
        seat="insert",
        pid="p01",
        repark_outcome="ANSWERED",
        spark_outcome="ANSWERED",
        repark_answer=[(1,), (8,)],
        spark_answer=[(1,), (8,)],
        repark_commits=1,
        spark_commits=None,
    )
    assert label == "EQUAL"


def test_a_set_expected_commit_count_still_flags_extra_and_fewer() -> None:
    extra = classify(
        seat="delete",
        pid="p01",
        repark_outcome="ANSWERED",
        spark_outcome="ANSWERED",
        repark_answer=[(2,)],
        spark_answer=[(2,)],
        repark_commits=2,
        spark_commits=1,
    )
    fewer = classify(
        seat="delete",
        pid="p01",
        repark_outcome="ANSWERED",
        spark_outcome="ANSWERED",
        repark_answer=[(2,)],
        spark_answer=[(2,)],
        repark_commits=0,
        spark_commits=1,
    )
    assert "EXTRA-COMMIT" in extra
    assert "FEWER-COMMIT" in fewer


def test_the_float_seeds_store_a_negative_zero_through_a_cast() -> None:
    for seed in (FLOAT_SEED_T, FLOAT_SEED_U):
        assert any(NEGATIVE_ZERO in row for row in seed)
        assert not any(", -0.0," in row for row in seed)
