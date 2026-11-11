from __future__ import annotations

import pytest
from _flat_matrix_cells import (
    COMBINATIONS,
    GROUP_NAMES,
    KNOWN_OPEN,
    PREDICATES,
    SIGNED_ZERO_PR_A,
    SIGNED_ZERO_PR_B,
    cells_for_group,
    class_a_key,
    classify,
    committed_commits,
    committed_ids,
    expected_for,
    float_cells,
    load_expected,
    matrix_cells,
    universe,
)

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


def test_known_open_dict_covers_p15_and_float_cells_only() -> None:
    assert len(KNOWN_OPEN) == 58
    p15 = [cell_id for cell_id, unit in KNOWN_OPEN.items() if unit == SIGNED_ZERO_PR_A]
    floats = [cell_id for cell_id, unit in KNOWN_OPEN.items() if unit == SIGNED_ZERO_PR_B]
    assert len(p15) == 30
    assert all(cell_id.endswith("|p15") for cell_id in p15)
    assert len(floats) == 28
    assert all(cell_id.startswith("float|") for cell_id in floats)


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
    assert declared == "KNOWN-EMPTY-DML"
    assert fewer == "FEWER-COMMIT"


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
