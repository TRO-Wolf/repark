from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _sm2_shared as sm2
import _using_corpus as corpus
import pytest

_HERE = Path(__file__).parent
_SPARK = json.loads((_HERE / "using_per_side_keys_1_corpus_spark.json").read_text())
_MAIN = json.loads((_HERE / "using_per_side_keys_1_corpus_main.json").read_text())
_STATEMENTS = corpus.statements()


def _in_head_order(head: dict[str, Any], recorded: dict[str, Any], text: str) -> dict[str, Any]:
    cols = recorded.get("cols")
    if not cols or cols == head.get("cols") or len(set(cols)) != len(cols):
        return recorded
    if sorted(cols) != sorted(head.get("cols") or []):
        return recorded
    order = [cols.index(name) for name in head["cols"]]
    rows = [[row[position] for position in order] for row in recorded["rows"]]
    if "ORDER BY" not in text.rsplit(")", 1)[-1]:
        rows = sorted(rows, key=lambda row: [str(value) for value in row])
    return {"cols": list(head["cols"]), "rows": rows}


def _same(head: dict[str, Any], recorded: dict[str, Any], text: str) -> bool:
    if "refused" in head or "refused" in recorded:
        return head.get("refused") == recorded.get("refused")
    return head == _in_head_order(head, recorded, text)


@pytest.fixture(scope="module")
def answers(tmp_path_factory: pytest.TempPathFactory) -> dict[str, dict[str, Any]]:
    session = sm2._open(tmp_path_factory.mktemp("upsk-diff"), "upsk-diff")
    corpus.load(session)
    found = {name: corpus.answer(session, text) for name, text in _STATEMENTS}
    session.stop()
    return found


def test_corpus_holds_the_agreed_shape() -> None:
    names = [name for name, _text in _STATEMENTS]
    assert len(names) == len(set(names))
    assert set(names) == set(_SPARK) == set(_MAIN)
    plain = [text for _name, text in _STATEMENTS if "USING" not in text and "NATURAL" not in text]
    assert len(plain) == 32
    assert len(names) - len(plain) >= 60


@pytest.mark.parametrize(("name", "text"), _STATEMENTS)
def test_every_statement_answers_as_spark_or_as_main(
    answers: dict[str, dict[str, Any]], name: str, text: str
) -> None:
    head = answers[name]
    assert _same(head, _SPARK[name], text) or _same(head, _MAIN[name], text), (
        text,
        head,
        _SPARK[name],
        _MAIN[name],
    )


@pytest.mark.parametrize(
    ("name", "text"), [item for item in _STATEMENTS if item[0].startswith("plain-")]
)
def test_statements_without_using_answer_exactly_as_main(
    answers: dict[str, dict[str, Any]], name: str, text: str
) -> None:
    assert answers[name] == _MAIN[name], text
