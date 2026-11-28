from __future__ import annotations

from pathlib import Path

import pytest

from repark_parity.scoreboard.classify import diff_files, diff_rows
from repark_parity.scoreboard.corpus import (
    DATA_DIR,
    CorpusError,
    check_corpus,
    check_lane,
    ingest,
    load_exclusions,
    select_cells,
    stage,
    stored_files,
    write_manifest,
)
from repark_parity.scoreboard.encoding import cell_digest, read_records, write_lines
from repark_parity.scoreboard.replay import replay
from repark_parity.scoreboard.settings import MissingSettingError, Settings
from repark_parity.scoreboard.summary import read_json

CELL = {
    "ansi": True,
    "id": "c1",
    "seat": "sql-literal",
    "sql": "SELECT 1 AS v",
    "surface": "K7",
    "zone": "UTC",
}
SCHEMA = {"type": "struct", "fields": [{"name": "v", "type": "integer", "nullable": False}]}


def answer(cell: dict, engine: str, value: int = 1) -> dict:
    return {
        "id": cell["id"],
        "input_sha256": cell_digest(cell),
        "engine": engine,
        "version": "4.1.2",
        "outcome": "answered",
        "schema": SCHEMA,
        "value": [[value]],
    }


def test_digest_is_the_sorted_key_ascii_line() -> None:
    reordered = dict(reversed(list(CELL.items())))
    assert cell_digest(reordered) == cell_digest(CELL)
    assert cell_digest({**CELL, "sql": "SELECT 2 AS v"}) != cell_digest(CELL)


def test_diff_refuses_a_changed_definition() -> None:
    changed = {**CELL, "sql": "SELECT 2 AS v"}
    with pytest.raises(ValueError, match="wrong engine or input digest: c1"):
        diff_rows({"c1": changed}, {"c1": answer(CELL, "spark")}, {"c1": answer(changed, "repark")})


def test_diff_refuses_a_wrong_engine_tag() -> None:
    with pytest.raises(ValueError, match="wrong engine or input digest"):
        diff_rows({"c1": CELL}, {"c1": answer(CELL, "spark")}, {"c1": answer(CELL, "spark")})


def test_diff_refuses_unequal_id_sets() -> None:
    with pytest.raises(ValueError, match="ID sets differ"):
        diff_rows({"c1": CELL}, {"c1": answer(CELL, "spark")}, {})


def test_duplicate_ids_fail_the_read(tmp_path: Path) -> None:
    path = tmp_path / "cases.jsonl"
    write_lines(path, [CELL, CELL])
    with pytest.raises(ValueError, match="duplicate ID c1"):
        read_records(path)


def test_diff_writes_one_verdict_per_cell(tmp_path: Path) -> None:
    write_lines(tmp_path / "cases.jsonl", [CELL])
    write_lines(tmp_path / "spark.jsonl", [answer(CELL, "spark")])
    write_lines(tmp_path / "repark.jsonl", [answer(CELL, "repark", 2)])
    rows = diff_files(
        *(tmp_path / f"{name}.jsonl" for name in ("cases", "spark", "repark", "diff"))
    )
    assert [row["class"] for row in rows] == ["WRONG-VALUE"]
    assert read_records(tmp_path / "diff.jsonl")["c1"]["surface"] == "K7"


def test_stage_refuses_an_existing_destination(tmp_path: Path) -> None:
    stage(tmp_path / "replay", {"lane": [(CELL, answer(CELL, "spark"))]})
    with pytest.raises(FileExistsError):
        stage(tmp_path / "replay", {"lane": [(CELL, answer(CELL, "spark"))]})


def empty_corpus(folder: Path) -> None:
    (folder / "cells").mkdir()
    write_lines(folder / "exclusions.jsonl.gz", [])
    write_manifest(stored_files(folder), folder)


def test_ingest_never_overwrites_a_recorded_lane(tmp_path: Path) -> None:
    empty_corpus(tmp_path)
    write_lines(tmp_path / "cases.jsonl", [CELL])
    write_lines(tmp_path / "spark.jsonl", [answer(CELL, "spark")])
    cases, spark = tmp_path / "cases.jsonl", tmp_path / "spark.jsonl"
    assert ingest("new", cases, spark, replace=False, data_dir=tmp_path) == 1
    with pytest.raises(FileExistsError, match="--replace"):
        ingest("new", cases, spark, replace=False, data_dir=tmp_path)
    with pytest.raises(CorpusError, match="reuses ids"):
        ingest("other", cases, spark, replace=False, data_dir=tmp_path)
    stale = tmp_path / "stale.jsonl"
    write_lines(stale, [answer({**CELL, "sql": "SELECT 3 AS v"}, "spark")])
    with pytest.raises(ValueError, match="wrong engine or input digest"):
        ingest("new", cases, stale, replace=True, data_dir=tmp_path)


def test_ingest_refuses_a_lane_name_that_leaves_the_corpus(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    empty_corpus(data)
    write_lines(tmp_path / "cases.jsonl", [CELL])
    write_lines(tmp_path / "spark.jsonl", [answer(CELL, "spark")])
    with pytest.raises(CorpusError, match="lane name"):
        ingest("../../escaped", tmp_path / "cases.jsonl", tmp_path / "spark.jsonl", False, data)
    assert not list(tmp_path.parent.glob("escaped*")) and not list(tmp_path.glob("escaped*"))


def test_a_hand_edited_stored_file_fails_the_corpus_check(tmp_path: Path) -> None:
    empty_corpus(tmp_path)
    write_lines(tmp_path / "cases.jsonl", [CELL])
    write_lines(tmp_path / "spark.jsonl", [answer(CELL, "spark")])
    ingest("new", tmp_path / "cases.jsonl", tmp_path / "spark.jsonl", False, tmp_path)
    assert check_corpus(tmp_path) == {"new": 1}
    write_lines(tmp_path / "cells" / "new.spark.jsonl.gz", [answer(CELL, "spark", 2)])
    with pytest.raises(CorpusError, match="checksums"):
        check_corpus(tmp_path)


def test_a_machine_path_in_a_stored_answer_is_refused() -> None:
    scrubbed = {**answer(CELL, "spark"), "message": "cannot read <tmp>/part-0.parquet"}
    check_lane("lane", {"c1": CELL}, {"c1": scrubbed})
    leaked = {**scrubbed, "message": "cannot read file:/srv/run/part-0.parquet"}
    with pytest.raises(CorpusError, match="machine path"):
        check_lane("lane", {"c1": CELL}, {"c1": leaked})


def test_an_empty_cell_list_selects_no_cell() -> None:
    assert select_cells(cells=[]) == {}


def test_replay_checks_its_settings_before_it_stages(tmp_path: Path) -> None:
    settings = Settings(output_root=tmp_path, package=tmp_path)
    with pytest.raises(MissingSettingError, match="--cpus"):
        replay(settings, tmp_path / "run1")
    assert not (tmp_path / "run1").exists()


def test_stored_corpus_binds_every_answer_to_its_cell() -> None:
    counts = check_corpus()
    assert len(counts) == 51
    assert sum(counts.values()) == 17264
    assert len(load_exclusions()) == 619


def test_baseline_scoreboard_reproduces_the_audit_counts() -> None:
    board = read_json(DATA_DIR / "scoreboards" / "2026-11-21-dac2af61.json.gz")
    assert board["totals"]["comparable"] == 16645
    assert board["totals"]["recorded_exclusions"] == 619
    assert board["totals"]["classes"] == {
        "EQUAL": 8583,
        "BOTH-REFUSED": 1795,
        "WRONG-VALUE": 1512,
        "WRONG-TYPE": 1008,
        "NULLABILITY-ONLY": 1248,
        "ACCEPTED-WHERE-SPARK-REFUSED": 426,
        "REFUSED-WHERE-SPARK-ANSWERS": 2073,
    }
