"""Census report comparator — the port's acceptance gate (design §6.4).

NEW CODE in the V2 port: v1 emits census reports, but nothing in either repository turns
two reports into a verdict. This module does exactly that and nothing else.

Two modes:

* **census mode** (default) — two ``compat.runner`` JSON reports in, ``{test_id → census
  class}`` per side.
* **``--junit`` mode** — two pytest JUnit XMLs in, ``{node id → outcome}`` per side, where
  outcome ∈ ``passed | failed | skipped | xfailed | error``. Skips are first-class: a test
  that silently stops skipping is exactly as interesting as one that stops passing.

Procedure, in order (each step is a hard gate):

1. **Environment manifests are compared FIRST** and any difference is a loud failure — the
   comparator refuses to diff two runs that are not the same measurement (§6.1). Two
   properties make that gate real rather than nominal: an external manifest may only
   **augment** a report's own manifest (a contradiction is a loud failure, so the CLI cannot
   fabricate agreement), and the keys that decide the measurement — ``python_version``,
   ``pandas_version``, and ``pyspark_version`` in census mode — must be **recorded**, because
   a key nobody records compares equal by absence.
2. The deferred ledger is subtracted **from the baseline (v1) side only**, and the
   subtraction is **echoed** so the reconciliation identity is visible.
3. Quarantined-unstable rows are excluded on **both** sides and reported separately.
4. The two sides are rendered sorted and compared **byte for byte** — no fuzzy matching, no
   aggregate-only comparison, no per-class tolerance.
5. Both denominators (``pass / all_collected`` and ``pass / engine_relevant``) are
   re-asserted over the compared row sets; any difference fails. Separately — and this is the
   half the byte comparison does not already imply — each report's **own recorded**
   denominator block is validated against the rows that report carries; a disagreement is a
   malformed report and a loud failure.
6. The delta is grouped by direction (pass→fail, fail→pass, class-change, appeared,
   vanished) and the process exits non-zero on **any** difference. An empty diff is the only
   pass.

**The checked-in ledger files are the ONLY subtraction inputs.** There is no flag,
environment variable, or config path by which a row can be excluded without appearing in a
ledger file: this module reads no environment at all, and its option set is frozen and
pinned by a unit test that provokes an undeclared subtraction.

CLI::

    PYTHONPATH=python/repark-parity python -m compat.compare_reports \\
        --baseline task/census/baseline-<pin>/classic.json \\
        --candidate task/census/v2-<sha>/classic.json \\
        --deferred task/port/deferred-python-tests.txt \\
        --quarantine task/census/baseline-<pin>/quarantine.txt

Exit codes: ``0`` identical, ``1`` any difference, ``2`` loud failure (manifest mismatch,
unreadable input, malformed report).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from compat.classify import CensusRow, denominators

# Exit codes (distinct: "they differ" is a finding, "I cannot compare" is an error).
EXIT_IDENTICAL = 0
EXIT_DIFFERENT = 1
EXIT_LOUD_FAIL = 2

# Census-JSON keys that constitute the environment manifest. Equality is required.
MANIFEST_KEYS: tuple[str, ...] = (
    "python_version",
    "pyspark_version",
    "spark_tag",
    "spark_commit_sha",
    "pandas_version",
    "pyarrow_version",
)
# Manifest keys that must be RECORDED (present and non-empty) on both sides, per mode. A key
# that nobody records compares equal-by-absence, which is how an unrecorded environment
# sneaks past an equality gate — design §5 F2: "a baseline whose environment is not recorded
# is not a baseline". ``pandas_version`` is required in both modes because the pandas major
# changes the measurement (docs/port/census.md §1); ``pyspark_version`` is required in census
# mode only — the facade cohort is defined by pyspark being ABSENT (§4).
REQUIRED_MANIFEST_KEYS_CENSUS: tuple[str, ...] = (
    "python_version",
    "pyspark_version",
    "pandas_version",
)
REQUIRED_MANIFEST_KEYS_JUNIT: tuple[str, ...] = (
    "python_version",
    "pandas_version",
)
# The denominator keys re-asserted over the compared rows AND validated against each
# report's own recorded block.
GATED_DENOMINATOR_KEYS: tuple[str, ...] = ("pass", "all_collected", "engine_relevant")
# Keys deliberately NOT part of the manifest, each with its reason. They are echoed in the
# report for the human reader but never gate the comparison.
MANIFEST_EXCLUDED: dict[str, str] = {
    "generated_at": "wall-clock stamp; two runs are never simultaneous",
    "repark_version": "the engine version differs across the two repositories by construction",
}

# JUnit outcomes that mean "this test was not executed against the engine" — the junit-mode
# analogue of the census classes excluded from the engine-relevant denominator.
_JUNIT_NON_ENGINE_OUTCOMES: frozenset[str] = frozenset({"skipped", "xfailed"})

# The complete, frozen set of CLI option strings. Pinned by a unit test: a new option is a
# deliberate act, and no option may ever subtract a row (only the ledger files do).
FROZEN_OPTIONS: tuple[str, ...] = (
    "--baseline",
    "--candidate",
    "--deferred",
    "--quarantine",
    "--manifest-baseline",
    "--manifest-candidate",
    "--junit",
    "--label-baseline",
    "--label-candidate",
    "-h",
    "--help",
)


class ComparatorError(Exception):
    """Loud failure: the two runs cannot be compared at all (exit 2)."""


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


@dataclass
class Side:
    """One side of the comparison: its rows, its manifest, and where it came from."""

    label: str
    path: Path
    manifest: dict[str, str]
    classes: dict[str, str]
    recorded_denominators: dict[str, Any] = field(default_factory=dict)
    # The raw status multiset as CARRIED by the report, duplicates included — the recorded
    # denominator block is validated against this, while `classes` (deduped; quarantined ids
    # may repeat at load) drives the comparison. None for junit sides.
    carried_statuses: list[str] | None = None


def load_ledger(path: Path | None) -> list[str]:
    """Read a checked-in ledger file: one id per line, ``#`` comments and blanks ignored.

    This is the ONLY way a row is ever removed from the comparison.
    """
    if path is None:
        return []
    if not path.is_file():
        raise ComparatorError(f"ledger file not found: {path}")
    entries: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        entries.append(line)
    return entries


def junit_node_id(node_id: str) -> str:
    """Canonicalize a collect-only node id into the JUnit id space.

    A ledger is written in the id form a human can check against the tree —
    ``tests/test_excel_reader.py::test_excel_skip_rows`` — while JUnit XML carries the
    ``classname``/``name`` pair, which this module keys as
    ``tests.test_excel_reader::test_excel_skip_rows``. Without a translation the ledger
    subtracts nothing in ``--junit`` mode, which is precisely the drift EC-4 forbids.

    The translation runs in this direction on purpose: *path → dotted module* is total and
    injective (strip ``.py``, ``/`` → ``.``), whereas *dotted → path* is ambiguous — nothing in
    ``tests.test_facade.TestX`` says where the module ends and the class begins. Ids already in
    the JUnit form (no ``::``, or a left side that is not a ``.py`` path) are returned unchanged,
    so the function is idempotent and safe to apply to any ledger.
    """
    path, separator, remainder = node_id.partition("::")
    if not separator or not path.endswith(".py"):
        return node_id
    module = path[: -len(".py")].replace("/", ".")
    *classes, name = remainder.split("::")
    return ".".join([module, *classes]) + "::" + name


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ComparatorError(f"report not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ComparatorError(f"{path}: not valid JSON ({error})") from error
    if not isinstance(payload, dict):
        raise ComparatorError(f"{path}: expected a JSON object at the top level")
    return payload


def load_census_report(path: Path, *, label: str, quarantine: frozenset[str] = frozenset()) -> Side:
    """Load a ``compat.runner`` JSON report into a comparison side.

    A duplicate ``test_id`` is a loud refusal — a report with duplicate keys cannot be
    multiset-compared — with exactly one escape: ids named in the QUARANTINE ledger may appear
    more than once (the source repo's runner emits duplicate rows with conflicting classes for
    a known pair of ids; quarantined rows are excluded from the gate and echoed separately, so
    keeping the first row is sufficient and the conflict never reaches the comparison).
    """
    payload = _read_json(path)
    manifest = {key: str(payload.get(key, "")) for key in MANIFEST_KEYS}
    modules = payload.get("modules")
    if not isinstance(modules, list):
        raise ComparatorError(f"{path}: report has no 'modules' list")
    classes: dict[str, str] = {}
    carried: list[str] = []
    for module in modules:
        for row in module.get("rows") or []:
            test_id = str(row.get("test_id", "")).strip()
            status = str(row.get("status", "")).strip()
            if not test_id:
                raise ComparatorError(f"{path}: a census row has no test_id")
            carried.append(status)
            if test_id in classes:
                if test_id in quarantine:
                    continue
                raise ComparatorError(
                    f"{path}: duplicate test id {test_id!r} — a report with duplicate keys "
                    f"cannot be multiset-compared (only QUARANTINED ids may repeat)"
                )
            classes[test_id] = status
    recorded = payload.get("denominators")
    return Side(
        label=label,
        path=path,
        manifest=manifest,
        classes=classes,
        recorded_denominators=recorded if isinstance(recorded, dict) else {},
        carried_statuses=carried,
    )


def _junit_outcome(case: ElementTree.Element) -> str:
    """Map one ``<testcase>`` to ``passed | failed | skipped | xfailed | error``."""
    if case.find("error") is not None:
        return "error"
    if case.find("failure") is not None:
        return "failed"
    skipped = case.find("skipped")
    if skipped is not None:
        if (skipped.get("type") or "").strip().lower().endswith("xfail"):
            return "xfailed"
        return "skipped"
    return "passed"


def load_junit_report(path: Path, *, label: str, manifest: dict[str, str]) -> Side:
    """Load a pytest JUnit XML into a comparison side.

    Node ids are reconstructed as ``classname::name`` — the only node-identifying pair JUnit
    XML carries without a pytest plugin, and stable across two identical trees. Ledger ids,
    which are written in collect-only ``path::name`` form, are translated into this same space
    by :func:`junit_node_id` before subtraction.
    """
    if not path.is_file():
        raise ComparatorError(f"report not found: {path}")
    try:
        root = ElementTree.parse(path).getroot()
    except ElementTree.ParseError as error:
        raise ComparatorError(f"{path}: not valid XML ({error})") from error
    classes: dict[str, str] = {}
    for case in root.iter("testcase"):
        classname = (case.get("classname") or "").strip()
        name = (case.get("name") or "").strip()
        if not name:
            raise ComparatorError(f"{path}: a <testcase> has no name attribute")
        node_id = f"{classname}::{name}" if classname else name
        if node_id in classes:
            raise ComparatorError(
                f"{path}: duplicate node id {node_id!r} — a report with duplicate keys "
                f"cannot be multiset-compared"
            )
        classes[node_id] = _junit_outcome(case)
    return Side(label=label, path=path, manifest=dict(manifest), classes=classes)


# ---------------------------------------------------------------------------
# Manifests
# ---------------------------------------------------------------------------


def load_manifest_file(path: Path | None) -> dict[str, str]:
    """Read an external environment manifest (JSON object of scalars)."""
    if path is None:
        return {}
    payload = _read_json(path)
    return {str(key): str(value) for key, value in payload.items()}


def merge_external_manifest(
    manifest: dict[str, str], external: dict[str, str], *, label: str, source: Path | None
) -> dict[str, str]:
    """Merge an external manifest INTO a report's own manifest — augment only, never override.

    The external file carries the half of the environment the JSON report does not (the
    ``pip freeze``: pandas, pyarrow, …). It may **fill** a key the report does not record, and
    it may **restate** a key with the same value; it may never *change* one. Overwriting is
    how a CLI flag would silently defeat the comparator's first gate — two runs from
    genuinely different interpreters would render an identical, fabricated manifest and exit
    0. A contradiction is therefore a loud failure, named key by key.
    """
    merged = dict(manifest)
    conflicts: list[str] = []
    for key in sorted(external):
        value = external[key]
        recorded = merged.get(key, "")
        if recorded and recorded != value:
            conflicts.append(f"  {key}: report={recorded!r}  external={value!r}")
            continue
        merged[key] = value
    if conflicts:
        raise ComparatorError(
            f"EXTERNAL MANIFEST CONTRADICTS THE {label} REPORT — an external manifest may "
            f"only supply keys the report does not record, never overwrite one "
            f"({source}). Refusing to diff.\n" + "\n".join(conflicts)
        )
    return merged


def check_manifest_recorded(manifest: dict[str, str], *, label: str, junit: bool) -> None:
    """Fail loudly when a required environment key is not recorded at all.

    Equality alone is not a gate: a key that neither side records compares equal by absence.
    """
    required = REQUIRED_MANIFEST_KEYS_JUNIT if junit else REQUIRED_MANIFEST_KEYS_CENSUS
    missing = [key for key in required if not manifest.get(key, "").strip()]
    if missing:
        raise ComparatorError(
            f"ENVIRONMENT NOT RECORDED on the {label} side: {', '.join(missing)} "
            f"(missing or empty). A run whose environment is not recorded is not a baseline "
            f"(design §5 F2); supply the pip-freeze half via --manifest-baseline / "
            f"--manifest-candidate. Refusing to diff."
        )


def compare_manifests(
    baseline: dict[str, str],
    candidate: dict[str, str],
    *,
    baseline_label: str,
    candidate_label: str,
) -> list[str]:
    """Return the rendered manifest differences (empty list = identical)."""
    differences: list[str] = []
    for key in sorted(set(baseline) | set(candidate)):
        left = baseline.get(key, "<absent>")
        right = candidate.get(key, "<absent>")
        if left != right:
            differences.append(f"  {key}: {baseline_label}={left!r}  {candidate_label}={right!r}")
    return differences


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def render_side(classes: dict[str, str]) -> str:
    """Sorted, canonical rendering of one side. The gate is a byte comparison of these."""
    return "".join(f"{test_id}\t{classes[test_id]}\n" for test_id in sorted(classes))


def compute_denominators(classes: dict[str, str], *, junit: bool) -> dict[str, Any]:
    """Both charter denominators over a compared row set.

    In census mode the rule is imported from :mod:`compat.classify` verbatim, so the
    comparator can never drift from the runner's own definition. In junit mode the
    charter classes do not exist, so the documented mapping is: ``pass`` = ``passed``;
    ``engine_relevant`` = everything except ``skipped`` / ``xfailed`` (the outcomes that
    mean the test never reached the engine).
    """
    if not junit:
        rows = [
            CensusRow(test_id=test_id, module="", status=status)
            for test_id, status in sorted(classes.items())
        ]
        return denominators(rows)
    total = len(classes)
    n_pass = sum(1 for outcome in classes.values() if outcome == "passed")
    engine = [o for o in classes.values() if o not in _JUNIT_NON_ENGINE_OUTCOMES]
    n_engine = len(engine)
    n_pass_engine = sum(1 for outcome in engine if outcome == "passed")
    return {
        "pass": n_pass,
        "all_collected": total,
        "pass_over_all": (n_pass / total) if total else 0.0,
        "engine_relevant": n_engine,
        "pass_over_engine_relevant": (n_pass_engine / n_engine) if n_engine else 0.0,
    }


def check_recorded_denominators(side: Side, *, junit: bool) -> None:
    """Validate a report's OWN recorded denominators against its own rows (census mode).

    The post-subtraction re-assert in :func:`compare` is implied by the byte comparison — two
    row sets that render identically necessarily produce identical denominators — so on its
    own it can never catch a report whose *recorded* counts disagree with the rows it ships.
    That is the failure this gate exists for: a report that claims 171 collected while
    carrying 169 rows is malformed, and a malformed report is a loud failure (exit 2), not a
    silent baseline.
    """
    if junit or not side.recorded_denominators:
        return
    carried = (
        side.carried_statuses if side.carried_statuses is not None else list(side.classes.values())
    )
    actual = compute_denominators(dict(enumerate(carried)), junit=False)
    mismatches: list[str] = []
    for key in GATED_DENOMINATOR_KEYS:
        if key not in side.recorded_denominators:
            continue
        recorded = side.recorded_denominators[key]
        if not isinstance(recorded, int) or recorded != actual[key]:
            mismatches.append(
                f"  {key}: recorded={recorded!r}  actual over its own rows={actual[key]}"
            )
    if mismatches:
        raise ComparatorError(
            f"{side.path}: the report's recorded denominators disagree with the rows it "
            f"carries — the report is malformed and cannot anchor a gate.\n" + "\n".join(mismatches)
        )


def _is_pass(value: str, *, junit: bool) -> bool:
    return value == ("passed" if junit else "PASS")


@dataclass
class Delta:
    """The grouped difference between two sides."""

    pass_to_fail: list[tuple[str, str, str]] = field(default_factory=list)
    fail_to_pass: list[tuple[str, str, str]] = field(default_factory=list)
    class_change: list[tuple[str, str, str]] = field(default_factory=list)
    appeared: list[tuple[str, str]] = field(default_factory=list)
    vanished: list[tuple[str, str]] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not (
            self.pass_to_fail
            or self.fail_to_pass
            or self.class_change
            or self.appeared
            or self.vanished
        )

    def total(self) -> int:
        return (
            len(self.pass_to_fail)
            + len(self.fail_to_pass)
            + len(self.class_change)
            + len(self.appeared)
            + len(self.vanished)
        )


def compute_delta(baseline: dict[str, str], candidate: dict[str, str], *, junit: bool) -> Delta:
    """Group every difference by direction (sorted, deterministic)."""
    delta = Delta()
    for test_id in sorted(set(baseline) | set(candidate)):
        left = baseline.get(test_id)
        right = candidate.get(test_id)
        if left is None:
            delta.appeared.append((test_id, str(right)))
            continue
        if right is None:
            delta.vanished.append((test_id, str(left)))
            continue
        if left == right:
            continue
        if _is_pass(left, junit=junit) and not _is_pass(right, junit=junit):
            delta.pass_to_fail.append((test_id, left, right))
        elif not _is_pass(left, junit=junit) and _is_pass(right, junit=junit):
            delta.fail_to_pass.append((test_id, left, right))
        else:
            delta.class_change.append((test_id, left, right))
    return delta


def apply_ledgers(
    baseline: Side,
    candidate: Side,
    *,
    deferred: Sequence[str],
    quarantined: Sequence[str],
) -> tuple[dict[str, str], dict[str, str], dict[str, list[str]]]:
    """Subtract the ledgers and return ``(baseline_rows, candidate_rows, echo)``.

    Deferred entries are removed from the **baseline side only** (they are v1 tests this
    repository deliberately did not port). Quarantined-unstable entries are removed from
    **both** sides. Everything removed is echoed.
    """
    deferred_set = set(deferred)
    quarantine_set = set(quarantined)
    echo: dict[str, list[str]] = {
        "deferred_subtracted": sorted(deferred_set & set(baseline.classes)),
        "deferred_not_present_in_baseline": sorted(deferred_set - set(baseline.classes)),
        "deferred_present_in_candidate": sorted(deferred_set & set(candidate.classes)),
        "quarantined_baseline": sorted(quarantine_set & set(baseline.classes)),
        "quarantined_candidate": sorted(quarantine_set & set(candidate.classes)),
        "quarantined_not_present": sorted(
            quarantine_set - set(baseline.classes) - set(candidate.classes)
        ),
    }
    baseline_rows = {
        test_id: status
        for test_id, status in baseline.classes.items()
        if test_id not in deferred_set and test_id not in quarantine_set
    }
    candidate_rows = {
        test_id: status
        for test_id, status in candidate.classes.items()
        if test_id not in quarantine_set
    }
    return baseline_rows, candidate_rows, echo


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _echo_lines(echo: dict[str, list[str]]) -> list[str]:
    lines = ["== ledger subtraction (echoed — the reconciliation identity is visible) =="]
    for key in (
        "deferred_subtracted",
        "deferred_not_present_in_baseline",
        "deferred_present_in_candidate",
        "quarantined_baseline",
        "quarantined_candidate",
        "quarantined_not_present",
    ):
        entries = echo[key]
        lines.append(f"{key}: {len(entries)}")
        lines.extend(f"    {entry}" for entry in entries)
    return lines


def _denominator_lines(
    baseline_denoms: dict[str, Any],
    candidate_denoms: dict[str, Any],
    *,
    baseline_label: str,
    candidate_label: str,
) -> tuple[list[str], list[str]]:
    """Return ``(rendered_lines, differences)`` for the two required denominators."""
    lines = ["== denominators (recomputed over the compared rows) =="]
    differences: list[str] = []
    for key in ("pass", "all_collected", "engine_relevant"):
        left = baseline_denoms.get(key)
        right = candidate_denoms.get(key)
        marker = "" if left == right else "   <== DIFFERS"
        lines.append(f"  {key}: {baseline_label}={left}  {candidate_label}={right}{marker}")
        if left != right:
            differences.append(f"{key}: {baseline_label}={left} {candidate_label}={right}")
    lines.append(
        f"  pass/all_collected:     {baseline_label}="
        f"{baseline_denoms.get('pass')}/{baseline_denoms.get('all_collected')}  "
        f"{candidate_label}={candidate_denoms.get('pass')}/"
        f"{candidate_denoms.get('all_collected')}"
    )
    lines.append(
        f"  pass/engine_relevant:   {baseline_label}="
        f"{baseline_denoms.get('pass')}/{baseline_denoms.get('engine_relevant')}  "
        f"{candidate_label}={candidate_denoms.get('pass')}/"
        f"{candidate_denoms.get('engine_relevant')}"
    )
    return lines, differences


def _delta_lines(delta: Delta, *, baseline_label: str, candidate_label: str) -> list[str]:
    lines = ["== delta by direction =="]
    groups: tuple[tuple[str, list[tuple[str, str, str]]], ...] = (
        ("pass -> fail", delta.pass_to_fail),
        ("fail -> pass", delta.fail_to_pass),
        ("class change", delta.class_change),
    )
    for title, rows in groups:
        lines.append(f"{title}: {len(rows)}")
        lines.extend(
            f"    {test_id}  {baseline_label}={left}  {candidate_label}={right}"
            for test_id, left, right in rows
        )
    lines.append(f"appeared (only in {candidate_label}): {len(delta.appeared)}")
    lines.extend(f"    {test_id}  {value}" for test_id, value in delta.appeared)
    lines.append(f"vanished (only in {baseline_label}): {len(delta.vanished)}")
    lines.extend(f"    {test_id}  {value}" for test_id, value in delta.vanished)
    return lines


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


@dataclass
class ComparisonResult:
    """The verdict plus everything the report printed."""

    exit_code: int
    lines: list[str]
    delta: Delta
    denominator_differences: list[str]
    byte_identical: bool


def compare(
    baseline: Side,
    candidate: Side,
    *,
    deferred: Sequence[str],
    quarantined: Sequence[str],
    junit: bool,
) -> ComparisonResult:
    """Run the whole procedure over two loaded sides.

    Raises :class:`ComparatorError` (a loud failure) when the environment manifests differ —
    before any row is looked at.
    """
    check_manifest_recorded(baseline.manifest, label=baseline.label, junit=junit)
    check_manifest_recorded(candidate.manifest, label=candidate.label, junit=junit)
    manifest_differences = compare_manifests(
        baseline.manifest,
        candidate.manifest,
        baseline_label=baseline.label,
        candidate_label=candidate.label,
    )
    if manifest_differences:
        raise ComparatorError(
            "ENVIRONMENT MANIFESTS DIFFER — the two runs are not the same measurement, "
            "so they are not comparable. Refusing to diff.\n" + "\n".join(manifest_differences)
        )

    check_recorded_denominators(baseline, junit=junit)
    check_recorded_denominators(candidate, junit=junit)

    if junit:
        # The checked-in ledgers are written in collect-only id form; JUnit rows are keyed
        # ``classname::name``. Canonicalize the ledger side ONCE, here, so a ledger cannot
        # silently subtract nothing (EC-4).
        deferred = [junit_node_id(entry) for entry in deferred]
        quarantined = [junit_node_id(entry) for entry in quarantined]

    baseline_rows, candidate_rows, echo = apply_ledgers(
        baseline, candidate, deferred=deferred, quarantined=quarantined
    )

    rendered_baseline = render_side(baseline_rows)
    rendered_candidate = render_side(candidate_rows)
    byte_identical = rendered_baseline.encode("utf-8") == rendered_candidate.encode("utf-8")

    baseline_denoms = compute_denominators(baseline_rows, junit=junit)
    candidate_denoms = compute_denominators(candidate_rows, junit=junit)
    denominator_lines, denominator_differences = _denominator_lines(
        baseline_denoms,
        candidate_denoms,
        baseline_label=baseline.label,
        candidate_label=candidate.label,
    )

    delta = compute_delta(baseline_rows, candidate_rows, junit=junit)

    lines: list[str] = [
        f"== census comparison ({'junit' if junit else 'census-json'} mode) ==",
        f"  {baseline.label}: {baseline.path}",
        f"  {candidate.label}: {candidate.path}",
        "== environment manifest (identical — gate passed) ==",
    ]
    lines.extend(f"  {key}: {value}" for key, value in sorted(baseline.manifest.items()))
    lines.extend(
        f"  (not gated) {key}: {reason}" for key, reason in sorted(MANIFEST_EXCLUDED.items())
    )
    if baseline.recorded_denominators or candidate.recorded_denominators:
        lines.append("== denominators as recorded in the reports (pre-subtraction, FYI only) ==")
        lines.append(
            f"  {baseline.label}: {json.dumps(baseline.recorded_denominators, sort_keys=True)}"
        )
        lines.append(
            f"  {candidate.label}: {json.dumps(candidate.recorded_denominators, sort_keys=True)}"
        )
    lines.extend(_echo_lines(echo))
    lines.extend(denominator_lines)
    lines.extend(
        _delta_lines(delta, baseline_label=baseline.label, candidate_label=candidate.label)
    )
    lines.append(
        f"== sorted-rendering byte comparison: {'IDENTICAL' if byte_identical else 'DIFFERENT'} =="
    )

    if byte_identical and not denominator_differences and delta.is_empty():
        lines.append("VERDICT: empty diff — exit 0")
        exit_code = EXIT_IDENTICAL
    else:
        lines.append(
            f"VERDICT: {delta.total()} moved cell(s), "
            f"{len(denominator_differences)} denominator difference(s) — exit 1"
        )
        exit_code = EXIT_DIFFERENT

    return ComparisonResult(
        exit_code=exit_code,
        lines=lines,
        delta=delta,
        denominator_differences=denominator_differences,
        byte_identical=byte_identical,
    )


def build_parser() -> argparse.ArgumentParser:
    """The CLI. Its option set is frozen (:data:`FROZEN_OPTIONS`) and unit-pinned."""
    parser = argparse.ArgumentParser(
        prog="compat.compare_reports",
        description=(
            "Compare two census reports (or two JUnit XMLs) and emit a pass/fail multiset "
            "diff. The checked-in ledger files are the only subtraction inputs."
        ),
    )
    parser.add_argument("--baseline", type=Path, required=True, help="v1-side report path")
    parser.add_argument("--candidate", type=Path, required=True, help="v2-side report path")
    parser.add_argument(
        "--deferred",
        type=Path,
        default=None,
        help="checked-in deferred-node-id ledger; subtracted from the BASELINE side only",
    )
    parser.add_argument(
        "--quarantine",
        type=Path,
        default=None,
        help="checked-in quarantined-unstable ledger; excluded on BOTH sides",
    )
    parser.add_argument(
        "--manifest-baseline",
        type=Path,
        default=None,
        help="external environment manifest (JSON) for the baseline side",
    )
    parser.add_argument(
        "--manifest-candidate",
        type=Path,
        default=None,
        help="external environment manifest (JSON) for the candidate side",
    )
    parser.add_argument(
        "--junit",
        action="store_true",
        help="inputs are pytest JUnit XMLs (node id -> outcome, skips first-class)",
    )
    parser.add_argument("--label-baseline", default="v1", help="label for the baseline side")
    parser.add_argument("--label-candidate", default="v2", help="label for the candidate side")
    return parser


def _load_sides(args: argparse.Namespace) -> tuple[Side, Side]:
    external_baseline = load_manifest_file(args.manifest_baseline)
    external_candidate = load_manifest_file(args.manifest_candidate)
    if args.junit:
        if not external_baseline or not external_candidate:
            raise ComparatorError(
                "--junit requires --manifest-baseline and --manifest-candidate: JUnit XML "
                "carries no environment, and a run whose environment is not recorded is not "
                "comparable (design §6.1)."
            )
        baseline = load_junit_report(
            args.baseline, label=args.label_baseline, manifest=external_baseline
        )
        candidate = load_junit_report(
            args.candidate, label=args.label_candidate, manifest=external_candidate
        )
        return baseline, candidate
    quarantine = frozenset(load_ledger(args.quarantine))
    baseline = load_census_report(args.baseline, label=args.label_baseline, quarantine=quarantine)
    candidate = load_census_report(
        args.candidate, label=args.label_candidate, quarantine=quarantine
    )
    baseline.manifest = merge_external_manifest(
        baseline.manifest,
        external_baseline,
        label=args.label_baseline,
        source=args.manifest_baseline,
    )
    candidate.manifest = merge_external_manifest(
        candidate.manifest,
        external_candidate,
        label=args.label_candidate,
        source=args.manifest_candidate,
    )
    return baseline, candidate


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry. Returns 0 (identical), 1 (any difference), or 2 (loud failure)."""
    args = build_parser().parse_args(argv)
    try:
        baseline, candidate = _load_sides(args)
        deferred = load_ledger(args.deferred)
        quarantined = load_ledger(args.quarantine)
        result = compare(
            baseline,
            candidate,
            deferred=deferred,
            quarantined=quarantined,
            junit=bool(args.junit),
        )
    except ComparatorError as error:
        print("LOUD FAILURE — comparison refused", file=sys.stderr)
        print(str(error), file=sys.stderr)
        return EXIT_LOUD_FAIL
    print("\n".join(result.lines))
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
