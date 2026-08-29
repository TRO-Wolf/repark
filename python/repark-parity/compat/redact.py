"""Path redaction for census artifacts — the recorded, executable transform.

Census artifacts are committed as evidence into a public repository, so the absolute scratch
paths they carry (pip-freeze editable URLs, report metadata, traceback frames, JUnit skip
messages) must be replaced by stable tokens before commit. Both sides must apply the
**identical** transform, or the manifests and the traceback-bearing rows differ for a reason
that has nothing to do with the port.

**Why code, not a `sed` line:** a path appears inside a structured string — escape-encoded
in JSON, entity-encoded in XML — and blind textual substitution destroys the encoding, so
the artifact stops parsing. This module redacts through the parser (load, rewrite string
values, re-serialize), so the output is valid by construction; validity is re-asserted
before the file is written.

CLI::

    PYTHONPATH=python/repark-parity python -m compat.redact \\
        --map "/tmp/repark-census-2026-08-08=<scratch>" \\
        --map "$PWD=<repo>" \\
        --map "$HOME=<home>" \\
        task/census/baseline-<pin>/**/compat-report.json \\
        task/census/baseline-<pin>/facade/facade.xml

Longer prefixes are applied first, so a nested scratch path never gets swallowed by the home
directory that contains it. Exit codes: ``0`` every file rewritten, ``2`` a file could not be
parsed or would not round-trip.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

EXIT_OK = 0
EXIT_LOUD_FAIL = 2

# Suffixes handled through their parser. Everything else is treated as plain text (manifests,
# freezes, `collected.txt`, run tails), where a substitution cannot break an encoding.
_JSON_SUFFIXES = frozenset({".json"})
_XML_SUFFIXES = frozenset({".xml"})


class RedactionError(Exception):
    """Loud failure: an artifact could not be parsed, or would not survive the transform."""


def parse_mapping(entries: Sequence[str]) -> list[tuple[str, str]]:
    """Parse ``--map PREFIX=TOKEN`` pairs, longest prefix first.

    Ordering is load-bearing: ``/home/x/scratch`` must be tried before ``/home/x``, or the
    nested path redacts under the wrong token and the two sides disagree.
    """
    mapping: list[tuple[str, str]] = []
    for entry in entries:
        prefix, separator, token = entry.partition("=")
        if not separator or not prefix or not token:
            raise RedactionError(f"--map expects PREFIX=TOKEN, got {entry!r}")
        mapping.append((prefix.rstrip("/"), token))
    return sorted(mapping, key=lambda pair: len(pair[0]), reverse=True)


def redact_string(value: str, mapping: Sequence[tuple[str, str]]) -> str:
    """Apply every mapping to one string value."""
    for prefix, token in mapping:
        value = value.replace(prefix, token)
    return value


def _redact_json_value(value: Any, mapping: Sequence[tuple[str, str]]) -> Any:
    if isinstance(value, str):
        return redact_string(value, mapping)
    if isinstance(value, list):
        return [_redact_json_value(item, mapping) for item in value]
    if isinstance(value, dict):
        return {
            redact_string(str(key), mapping): _redact_json_value(item, mapping)
            for key, item in value.items()
        }
    return value


def redact_json_text(text: str, mapping: Sequence[tuple[str, str]]) -> str:
    """Redact a JSON document through the parser; the result is valid JSON by construction."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as error:
        raise RedactionError(f"not valid JSON before redaction ({error})") from error
    rendered = json.dumps(_redact_json_value(payload, mapping), indent=2, sort_keys=True) + "\n"
    try:
        json.loads(rendered)
    except json.JSONDecodeError as error:  # pragma: no cover - defensive round-trip assert
        raise RedactionError(f"redaction produced invalid JSON ({error})") from error
    return rendered


def _redact_element(element: ElementTree.Element, mapping: Sequence[tuple[str, str]]) -> None:
    for key, value in list(element.attrib.items()):
        element.attrib[key] = redact_string(value, mapping)
    if element.text:
        element.text = redact_string(element.text, mapping)
    if element.tail:
        element.tail = redact_string(element.tail, mapping)
    for child in element:
        _redact_element(child, mapping)


def redact_xml_text(text: str, mapping: Sequence[tuple[str, str]]) -> str:
    """Redact an XML document through the parser.

    The serializer entity-escapes tokens written into character data, so an angle-bracketed
    token does not become an element start tag.
    """
    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError as error:
        raise RedactionError(f"not well-formed XML before redaction ({error})") from error
    _redact_element(root, mapping)
    rendered = ElementTree.tostring(root, encoding="unicode")
    try:
        ElementTree.fromstring(rendered)
    except ElementTree.ParseError as error:  # pragma: no cover - defensive round-trip assert
        raise RedactionError(f"redaction produced malformed XML ({error})") from error
    return rendered


def redact_document(text: str, mapping: Sequence[tuple[str, str]], *, suffix: str) -> str:
    """Dispatch on the artifact's format. Unknown formats are plain text."""
    if suffix in _JSON_SUFFIXES:
        return redact_json_text(text, mapping)
    if suffix in _XML_SUFFIXES:
        return redact_xml_text(text, mapping)
    return redact_string(text, mapping)


def redact_file(path: Path, mapping: Sequence[tuple[str, str]]) -> bool:
    """Rewrite one artifact in place. Returns ``True`` when the bytes changed."""
    if not path.is_file():
        raise RedactionError(f"artifact not found: {path}")
    original = path.read_text(encoding="utf-8")
    try:
        rendered = redact_document(original, mapping, suffix=path.suffix.lower())
    except RedactionError as error:
        raise RedactionError(f"{path}: {error}") from error
    if rendered != original:
        path.write_text(rendered, encoding="utf-8")
        return True
    return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="compat.redact",
        description=(
            "Redact absolute paths out of census artifacts, through each artifact's parser "
            "so the result is valid JSON / well-formed XML by construction."
        ),
    )
    parser.add_argument(
        "--map",
        dest="maps",
        action="append",
        default=[],
        metavar="PREFIX=TOKEN",
        help='path prefix to replace, e.g. "$HOME=<home>" (repeatable; longest applied first)',
    )
    parser.add_argument("paths", nargs="+", type=Path, help="artifacts to rewrite in place")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry. Returns 0 (all rewritten) or 2 (loud failure)."""
    args = build_parser().parse_args(argv)
    try:
        mapping = parse_mapping(args.maps)
        if not mapping:
            raise RedactionError("at least one --map PREFIX=TOKEN is required")
        for path in args.paths:
            changed = redact_file(path, mapping)
            print(f"{'redacted' if changed else 'unchanged'}: {path}")
    except RedactionError as error:
        print("LOUD FAILURE — redaction refused", file=sys.stderr)
        print(str(error), file=sys.stderr)
        return EXIT_LOUD_FAIL
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
