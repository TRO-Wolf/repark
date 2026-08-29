"""Unit battery for the census-artifact path redaction (docs/port/census.md §3).

The transform is a mandatory step of the recorded procedure — both sides must apply the
identical one — and it operates on artifacts that are the acceptance gate's only inputs. Its
one hard property is therefore that **the artifact still parses afterwards**: a redacted
census report that is not valid JSON, or a redacted JUnit XML that is not well-formed, cannot
be compared at all, and a comparator that exits 2 on the baseline is not a gate.

Two of these tests are regressions of exactly that failure, reproduced here as the naive
text-substitution the parser-based transform replaces.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from xml.etree import ElementTree

import pytest

# compat/ lives next to src/ under python/repark-parity (not the wheel package).
_PARITY_ROOT = Path(__file__).resolve().parents[1]
if str(_PARITY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PARITY_ROOT))

from compat.redact import (  # noqa: E402
    EXIT_LOUD_FAIL,
    EXIT_OK,
    RedactionError,
    main,
    parse_mapping,
    redact_document,
    redact_file,
    redact_json_text,
    redact_string,
    redact_xml_text,
)

_MAP = [("/home/dev/scratch/census", "<scratch>"), ("/home/dev", "<home>")]


# JSON — the census report


def test_a_traceback_bearing_report_is_still_valid_json() -> None:
    """REGRESSION: the escaped quote that closes a traceback path must survive.

    A traceback lives inside a JSON string, so its path is delimited by ``\\"``. The
    transform must not touch that escaping.
    """
    payload = {
        "modules": [
            {
                "rows": [
                    {
                        "test_id": "pyspark.sql.tests.test_udf.T.test_assert_true",
                        "raw_traceback": (
                            'Traceback:\n  File "/home/dev/lib/unittest/case.py", line 589\n'
                            '  File "/home/dev/scratch/census/t.py", line 2375, in test_x\n'
                        ),
                    }
                ]
            }
        ]
    }
    rendered = redact_json_text(json.dumps(payload), _MAP)
    reloaded = json.loads(rendered)  # the property: it still parses
    traceback = reloaded["modules"][0]["rows"][0]["raw_traceback"]
    assert 'File "<home>/lib/unittest/case.py"' in traceback
    assert 'File "<scratch>/t.py"' in traceback
    assert "/home/dev" not in rendered


def test_naive_text_substitution_over_json_is_what_the_parser_path_replaces() -> None:
    """The failure mode this module exists to prevent, pinned as an executable contrast."""
    source = json.dumps({"msg": 'loaded from "/home/dev" ok'})
    # A textual transform that is not encoding-aware can emit an unescaped quote...
    naive = source.replace('\\"/home/dev\\"', '"<home>"')
    with pytest.raises(json.JSONDecodeError):
        json.loads(naive)
    # ...whereas the parser path cannot, by construction.
    assert json.loads(redact_json_text(source, _MAP))["msg"] == 'loaded from "<home>" ok'


def test_json_object_keys_are_redacted_too() -> None:
    rendered = redact_json_text(json.dumps({"/home/dev/x": 1}), _MAP)
    assert json.loads(rendered) == {"<home>/x": 1}


def test_non_string_scalars_are_untouched() -> None:
    payload = {"pass": 142, "ratio": 0.41, "timed_out": False, "error": None}
    assert json.loads(redact_json_text(json.dumps(payload), _MAP)) == payload


def test_malformed_json_is_a_loud_failure() -> None:
    with pytest.raises(RedactionError, match="not valid JSON"):
        redact_json_text("{not json", _MAP)


# XML — the facade JUnit report


_JUNIT = (
    '<?xml version="1.0" encoding="utf-8"?>'
    "<testsuites><testsuite>"
    '<testcase classname="" name="tests.test_fuzz_smoke" time="0.000">'
    '<skipped message="collection skipped">'
    "('/home/dev/scratch/census/python/repark/tests/t.py', 18, 'no pyspark')"
    "</skipped></testcase>"
    "</testsuite></testsuites>"
)


def test_a_redacted_junit_xml_is_still_well_formed() -> None:
    """REGRESSION: an angle-bracketed token in character data must not become a start tag."""
    rendered = redact_xml_text(_JUNIT, _MAP)
    root = ElementTree.fromstring(rendered)  # the property: it still parses
    skipped = root.find(".//skipped")
    assert skipped is not None
    assert skipped.text is not None
    assert skipped.text.startswith("('<scratch>/python/repark/tests/t.py'")
    # The token is escaped on the wire and recovered by the parser, not stored raw.
    assert "&lt;scratch&gt;" in rendered
    assert "/home/dev" not in rendered


def test_naive_text_substitution_over_xml_is_what_the_parser_path_replaces() -> None:
    naive = _JUNIT.replace("/home/dev/scratch/census", "<scratch>")
    with pytest.raises(ElementTree.ParseError):
        ElementTree.fromstring(naive)
    ElementTree.fromstring(redact_xml_text(_JUNIT, _MAP))  # no raise


def test_xml_attributes_are_redacted() -> None:
    source = '<testsuite><testcase classname="/home/dev/a" name="b"/></testsuite>'
    root = ElementTree.fromstring(redact_xml_text(source, _MAP))
    assert root.find("testcase").get("classname") == "<home>/a"


def test_malformed_xml_is_a_loud_failure() -> None:
    with pytest.raises(RedactionError, match="not well-formed XML"):
        redact_xml_text("<a><b></a>", _MAP)


# Mapping semantics + the file/CLI surface


def test_longest_prefix_is_applied_first() -> None:
    """Otherwise the nested scratch path redacts to ``<home>/scratch/...`` and the two sides
    disagree about which token a directory got."""
    mapping = parse_mapping(["/home/dev=<home>", "/home/dev/scratch/census=<scratch>"])
    assert mapping[0][1] == "<scratch>"
    assert redact_string("/home/dev/scratch/census/x", mapping) == "<scratch>/x"


def test_trailing_slashes_do_not_change_the_mapping() -> None:
    assert parse_mapping(["/home/dev/=<home>"]) == [("/home/dev", "<home>")]


def test_a_malformed_map_argument_is_a_loud_failure() -> None:
    for bad in ("/home/dev", "=<home>", "/home/dev="):
        with pytest.raises(RedactionError, match="PREFIX=TOKEN"):
            parse_mapping([bad])


def test_plain_text_artifacts_are_substituted_verbatim() -> None:
    text = "-e file:///home/dev/scratch/census/python/repark\npandas==2.2.3\n"
    rendered = redact_document(text, _MAP, suffix=".txt")
    assert rendered == "-e file://<scratch>/python/repark\npandas==2.2.3\n"


def test_redact_file_rewrites_in_place_and_reports_whether_it_changed(tmp_path) -> None:
    path = tmp_path / "compat-report.json"
    path.write_text(json.dumps({"scratch": "/home/dev/x"}), encoding="utf-8")
    assert redact_file(path, _MAP) is True
    assert json.loads(path.read_text(encoding="utf-8")) == {"scratch": "<home>/x"}
    assert redact_file(path, _MAP) is False


def test_cli_redacts_every_named_artifact(tmp_path, capsys) -> None:
    report = tmp_path / "compat-report.json"
    junit = tmp_path / "facade.xml"
    report.write_text(json.dumps({"cwd": "/home/dev/scratch/census"}), encoding="utf-8")
    junit.write_text(_JUNIT, encoding="utf-8")
    code = main(
        [
            "--map",
            "/home/dev/scratch/census=<scratch>",
            "--map",
            "/home/dev=<home>",
            str(report),
            str(junit),
        ]
    )
    assert code == EXIT_OK
    assert "redacted" in capsys.readouterr().out
    json.loads(report.read_text(encoding="utf-8"))
    ElementTree.parse(junit)


def test_cli_requires_a_mapping(tmp_path, capsys) -> None:
    path = tmp_path / "a.txt"
    path.write_text("x", encoding="utf-8")
    assert main([str(path)]) == EXIT_LOUD_FAIL
    assert "at least one --map" in capsys.readouterr().err


def test_cli_refuses_a_missing_artifact(tmp_path, capsys) -> None:
    assert main(["--map", "/home/dev=<home>", str(tmp_path / "nope.json")]) == EXIT_LOUD_FAIL
    assert "artifact not found" in capsys.readouterr().err
