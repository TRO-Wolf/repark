#!/usr/bin/env python3
"""Fail loud when `make parity-live` and parity-live.yml drift.

Compares the two surfaces to EACH OTHER — never to a third hand-maintained flag list.
Fail-closed: a parse miss on either side is a failure, not a skip.

Scope: the one Makefile ``parity-live`` target ↔ ``.github/workflows/parity-live.yml`` pair.
# Extensibility: additional dual-wired pairs can become additional compare_* functions;
# this checker deliberately does not implement a multi-pair framework.

SSOT is the comparison itself. Prose (AGENTS.md / Makefile comments) points here.

Load-bearing command selection: **exactly one** matching invocation per class
(``uv sync`` / maturin develop / ``uv run … pytest``). Zero or more than one is a fail-closed
parse error — neither first-match nor last-wins decoys can greenwash. Env pins are taken
from the ``env:`` map of the unique step that runs ``uv run … pytest``. Absolute floors also
require ``REPARK_PARITY_LIVE=1``, ``SPARK_LOCAL_IP=127.0.0.1``, and pytest path
``python/repark/tests``.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MAKEFILE = REPO_ROOT / "Makefile"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "parity-live.yml"

# Floor: both sides must carry these known-critical tokens (fail-closed against coordinated
# drop). The comparison itself is still pairwise — this is not a substitute for comparing the
# two surfaces to each other.
REQUIRED_SYNC_EXTRAS = frozenset({"record", "numpy", "pandas", "polars", "ml-ext"})
REQUIRED_SYNC_FLAGS = frozenset({"--locked", "--no-install-package"})
REQUIRED_UV_RUN_FLAGS = frozenset({"--locked", "--no-sync"})
REQUIRED_ENV = frozenset({"REPARK_PARITY_LIVE", "SPARK_LOCAL_IP"})


@dataclass(frozen=True)
class Surface:
    """Normalized load-bearing command surface for one side of the dual-wire pair."""

    side: str
    sync_flags: frozenset[str]
    sync_extras: frozenset[str]
    no_install_package: str
    maturin_version: str
    maturin_subcommand: str
    uv_run_flags: frozenset[str]
    pytest_path: str
    env: dict[str, str]


class ParseError(Exception):
    """A required token could not be extracted — fail closed, never skip."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ParseError(message)


def _unique_match(pattern: str, text: str, flags: int = 0) -> re.Match[str] | None:
    """Return the sole match of ``pattern`` in ``text``; None if zero; raise ParseError if >1."""
    matches = list(re.finditer(pattern, text, flags))
    if len(matches) > 1:
        raise ParseError(
            f"ambiguous load-bearing token (expected exactly one match for {pattern!r}; "
            f"found {len(matches)}) — a decoy or duplicate step would greenwash; fail-closed"
        )
    return matches[0] if matches else None


def _strip_shell_trailing_comment(line: str) -> str:
    """Drop trailing ``# …`` comments that shell would ignore (not mid-string-aware)."""
    if "#" not in line:
        return line
    # Only treat `#` as comment when preceded by whitespace or start (simple shell form).
    return re.sub(r"(^|\s)#.*$", r"\1", line).rstrip()


def _makefile_maturin_global_pin(makefile_text: str) -> str:
    """Exactly one ``MATURIN := uvx maturin@X.Y.Z`` — Make's last assignment would otherwise
    diverge from a first-match pin (W3-SEC-003).
    """
    match = _unique_match(r"(?m)^MATURIN\s*:=\s*uvx\s+maturin@([0-9.]+)\s*$", makefile_text)
    _require(match is not None, "Makefile: MATURIN := uvx maturin@X.Y.Z pin not found")
    assert match is not None
    return match.group(1)


def _makefile_parity_live_recipe(makefile_text: str) -> str:
    """Extract the recipe body of the unique single-colon ``parity-live:`` target.

    Exactly one ``parity-live:`` rule (not ``parity-live::``) — multi-fire ``::`` recipes and
    redefinitions are fail-closed so the checker cannot greenwash a stripped executable body
    against a decoy twin (W3-SEC-002).
    """
    if re.search(r"(?m)^parity-live::", makefile_text):
        raise ParseError(
            "Makefile: parity-live:: double-colon rules are refuse-loud "
            "(Make runs every :: recipe; the checker would only see one body)"
        )
    matches = list(
        re.finditer(
            r"(?m)^parity-live:[^\n]*\n((?:[ \t]+[^\n]*\n|[\t][^\n]*\n|\n)*)",
            makefile_text,
        )
    )
    _require(bool(matches), "Makefile: parity-live target recipe not found")
    if len(matches) > 1:
        raise ParseError(
            f"Makefile: expected exactly one parity-live: recipe, found {len(matches)} "
            "(redefinitions greenwash; fail-closed)"
        )
    match = matches[0]
    body = match.group(1)
    _require(body.strip() != "", "Makefile: parity-live recipe is empty")
    lines: list[str] = []
    for raw in body.splitlines():
        stripped = raw.lstrip("\t ").rstrip()
        if not stripped or stripped.startswith("@#") or stripped.startswith("#"):
            continue
        if stripped.startswith("@"):
            stripped = stripped[1:]
        stripped = _strip_shell_trailing_comment(stripped)
        if not stripped:
            continue
        # Shell no-ops / documentation decoys must not supply load-bearing tokens.
        # Drop any line that *contains* a leading decoy head after `;` split, and pure heads.
        if re.search(r"(?:^|&&|;)\s*(?:echo|printf|:|true|false)\b", stripped):
            continue
        lines.append(stripped)
    joined = " ".join(lines)
    joined = re.sub(r"\\\s*", " ", joined)
    joined = re.sub(r"\s+", " ", joined).strip()
    return joined


def _maturin_version_from_makefile_recipe(recipe: str, global_pin: str) -> str:
    """Version the ``parity-live`` recipe actually runs (unique maturin develop)."""
    match = _unique_match(
        r"(?:\$\(MATURIN\)|uvx\s+maturin@([0-9.]+))\s+develop\b",
        recipe,
    )
    _require(
        match is not None,
        "Makefile parity-live: maturin develop step not found (via $(MATURIN) or uvx maturin@…)",
    )
    assert match is not None
    if match.group(1) is not None:
        return match.group(1)
    return global_pin


def parse_makefile(makefile_text: str) -> Surface:
    global_pin = _makefile_maturin_global_pin(makefile_text)
    recipe = _makefile_parity_live_recipe(makefile_text)
    maturin_version = _maturin_version_from_makefile_recipe(recipe, global_pin)

    # Unique `uv sync` (exactly one).
    sync_match = _unique_match(
        r"\buv\s+sync\b([^;&]+?)(?=\s+(?:cd|\$\(|uvx|uv\s+run|JAVA_HOME|$))", recipe
    )
    if sync_match is None:
        sync_match = _unique_match(r"\buv\s+sync\b(.+?)(?=\s+uv\s+run|\s+\$\(|$)", recipe)
    _require(sync_match is not None, "Makefile parity-live: `uv sync` invocation not found")
    assert sync_match is not None
    sync_tail = sync_match.group(1)
    sync_flags = frozenset(
        flag for flag in re.findall(r"--[a-z0-9-]+", sync_tail) if flag != "--extra"
    )
    sync_extras = frozenset(re.findall(r"--extra\s+(\S+)", sync_tail))
    no_install = re.search(r"--no-install-package\s+(\S+)", sync_tail)
    _require(no_install is not None, "Makefile parity-live: --no-install-package <pkg> not found")
    assert no_install is not None

    # Unique `uv run … pytest` including any KEY=value prefixes on the same invocation span.
    run_match = _unique_match(
        r"((?:[A-Z_][A-Z0-9_]*=\S+\s+)*)\buv\s+run\b([^;&]*\bpytest\b[^;&]*)",
        recipe,
    )
    _require(
        run_match is not None,
        "Makefile parity-live: `uv run … pytest …` invocation not found",
    )
    assert run_match is not None
    run_prefix = run_match.group(1)
    run_tail = run_match.group(2)
    uv_run_flags = frozenset(re.findall(r"--[a-z0-9-]+", run_tail))
    pytest_match = re.search(r"\bpytest\s+(\S+)", run_tail)
    _require(pytest_match is not None, "Makefile parity-live: pytest path not found after uv run")
    assert pytest_match is not None

    # Env pins MUST sit on the same invocation as `uv run … pytest` (Make is not .ONESHELL).
    env: dict[str, str] = {}
    for key in REQUIRED_ENV:
        env_match = re.search(rf"\b{key}=(\S+)", run_prefix)
        _require(
            env_match is not None,
            f"Makefile parity-live: env pin {key}=… must prefix the uv-run-pytest invocation "
            "(a pin on a different recipe line does not arm the live tier under Make)",
        )
        assert env_match is not None
        env[key] = env_match.group(1).strip("\"'")

    return Surface(
        side="Makefile parity-live",
        sync_flags=sync_flags,
        sync_extras=sync_extras,
        no_install_package=no_install.group(1),
        maturin_version=maturin_version,
        maturin_subcommand="develop",
        uv_run_flags=uv_run_flags,
        pytest_path=pytest_match.group(1),
        env=env,
    )


@dataclass(frozen=True)
class _WorkflowStep:
    """One GHA step, in document order, with optional env map and run body text."""

    start: int
    env: dict[str, str]
    run_body: str


def _parse_workflow_steps(workflow_text: str) -> list[_WorkflowStep]:
    """Parse top-level job steps roughly enough to get env + run in document order.

    Fail-closed: if we cannot find any run bodies, callers raise ParseError.
    """
    steps: list[_WorkflowStep] = []
    # Split on step starts: lines like `      - name:` or `      - uses:` at the steps indent.
    step_starts = list(re.finditer(r"(?m)^([ \t]+)-\s+(?:name|uses|run):", workflow_text))
    for index, start_match in enumerate(step_starts):
        start = start_match.start()
        end = step_starts[index + 1].start() if index + 1 < len(step_starts) else len(workflow_text)
        chunk = workflow_text[start:end]

        env: dict[str, str] = {}
        env_block = re.search(
            r"(?m)^[ \t]+env:\s*\n((?:[ \t]+[A-Za-z_][A-Za-z0-9_]*:[^\n]*\n)+)", chunk
        )
        if env_block is not None:
            for line in env_block.group(1).splitlines():
                pair = re.match(
                    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*[\"']?([^\"'\n]+)[\"']?\s*$", line
                )
                if pair is not None:
                    env[pair.group(1)] = pair.group(2).strip()

        run_body = ""
        multi = re.search(r"(?m)^([ \t]+)run:\s*\|\s*\n((?:\1[ \t]+.*\n)+)", chunk)
        kept_lines: list[str] = []
        if multi is not None:
            for line in multi.group(2).splitlines():
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                stripped = _strip_shell_trailing_comment(stripped.rstrip("\\").strip())
                if not stripped:
                    continue
                # Per-line no-op drop (mirror Makefile) — not whole-body head only.
                if re.search(r"(?:^|&&|;)\s*(?:echo|printf|:|true|false)\b", stripped):
                    continue
                kept_lines.append(stripped)
        else:
            single = re.search(r"(?m)^[ \t]+run:\s+([^|\n][^\n]*)$", chunk)
            if single is not None:
                stripped = _strip_shell_trailing_comment(single.group(1).strip())
                if stripped and not re.search(
                    r"(?:^|&&|;)\s*(?:echo|printf|:|true|false)\b", stripped
                ):
                    kept_lines.append(stripped)
        run_body = re.sub(r"\s+", " ", " ".join(kept_lines)).strip()
        steps.append(_WorkflowStep(start=start, env=env, run_body=run_body))
    return steps


def parse_workflow(workflow_text: str) -> Surface:
    steps = _parse_workflow_steps(workflow_text)
    bodies_in_order = [step.run_body for step in steps if step.run_body]
    _require(bool(bodies_in_order), "parity-live.yml: no `run:` step bodies found")
    # Document-order join; last-match selection picks the final decoy-resistant invocation.
    run_bodies = " \n ".join(bodies_in_order)

    maturin_match = _unique_match(r"uvx\s+maturin@([0-9.]+)\s+(develop)\b", run_bodies)
    _require(
        maturin_match is not None,
        "parity-live.yml: `uvx maturin@X.Y.Z develop` step not found in run bodies",
    )
    assert maturin_match is not None

    sync_match = _unique_match(
        r"\buv\s+sync\b([^;]+?)(?=\s+\(cd|\s+uvx\s+maturin|\s+uv\s+run|$)",
        run_bodies,
    )
    _require(
        sync_match is not None,
        "parity-live.yml: `uv sync` invocation not found in run bodies",
    )
    assert sync_match is not None
    sync_tail = sync_match.group(1)
    sync_flags = frozenset(
        flag for flag in re.findall(r"--[a-z0-9-]+", sync_tail) if flag != "--extra"
    )
    sync_extras = frozenset(re.findall(r"--extra\s+(\S+)", sync_tail))
    no_install = re.search(r"--no-install-package\s+(\S+)", sync_tail)
    _require(no_install is not None, "parity-live.yml: --no-install-package <pkg> not found")
    assert no_install is not None

    run_match = _unique_match(r"\buv\s+run\b([^\n]*\bpytest\b[^\n]*)", run_bodies)
    _require(
        run_match is not None,
        "parity-live.yml: `uv run … pytest …` invocation not found in run bodies",
    )
    assert run_match is not None
    run_tail = run_match.group(1)
    uv_run_flags = frozenset(re.findall(r"--[a-z0-9-]+", run_tail))
    pytest_match = re.search(r"\bpytest\s+(\S+)", run_tail)
    _require(pytest_match is not None, "parity-live.yml: pytest path not found after uv run")
    assert pytest_match is not None

    # Env from the unique step whose run body contains `uv run … pytest`.
    pytest_steps = [
        step
        for step in steps
        if re.search(r"\buv\s+run\b[^\n]*\bpytest\b", step.run_body) is not None
    ]
    _require(
        bool(pytest_steps),
        "parity-live.yml: no step with `uv run … pytest` found for env extraction",
    )
    if len(pytest_steps) > 1:
        raise ParseError(
            f"parity-live.yml: expected exactly one uv-run-pytest step, found {len(pytest_steps)}"
        )
    pytest_env = pytest_steps[0].env
    env: dict[str, str] = {}
    for key in REQUIRED_ENV:
        _require(
            key in pytest_env,
            f"parity-live.yml: env pin {key} missing on the uv-run-pytest step",
        )
        env[key] = pytest_env[key]

    return Surface(
        side="parity-live.yml",
        sync_flags=sync_flags,
        sync_extras=sync_extras,
        no_install_package=no_install.group(1),
        maturin_version=maturin_match.group(1),
        maturin_subcommand=maturin_match.group(2),
        uv_run_flags=uv_run_flags,
        pytest_path=pytest_match.group(1),
        env=env,
    )


def compare(left: Surface, right: Surface) -> list[str]:
    """Return human-readable drift lines naming the side and the differing token."""
    errors: list[str] = []

    def field(  # nested-def: comparator closes over both surfaces and the error list
        name: str, left_value: object, right_value: object
    ) -> None:
        if left_value != right_value:
            errors.append(
                f"parity-live dual-wire drift on {name}: "
                f"{left.side}={left_value!r}  {right.side}={right_value!r}"
            )

    for surface in (left, right):
        missing_sync = REQUIRED_SYNC_FLAGS - surface.sync_flags
        if missing_sync:
            errors.append(
                f"parity-live dual-wire parse incomplete on {surface.side}: "
                f"uv sync missing flags {sorted(missing_sync)}"
            )
        missing_extras = REQUIRED_SYNC_EXTRAS - surface.sync_extras
        if missing_extras:
            errors.append(
                f"parity-live dual-wire parse incomplete on {surface.side}: "
                f"uv sync missing --extra {sorted(missing_extras)}"
            )
        missing_run = REQUIRED_UV_RUN_FLAGS - surface.uv_run_flags
        if missing_run:
            errors.append(
                f"parity-live dual-wire parse incomplete on {surface.side}: "
                f"uv run missing flags {sorted(missing_run)}"
            )
        if surface.no_install_package != "repark":
            errors.append(
                f"parity-live dual-wire unexpected --no-install-package on {surface.side}: "
                f"{surface.no_install_package!r} (expected 'repark')"
            )
        if surface.maturin_subcommand != "develop":
            errors.append(
                f"parity-live dual-wire unexpected maturin subcommand on {surface.side}: "
                f"{surface.maturin_subcommand!r}"
            )

    field("uv-sync-flags", sorted(left.sync_flags), sorted(right.sync_flags))
    field("uv-sync-extras", sorted(left.sync_extras), sorted(right.sync_extras))
    field("no-install-package", left.no_install_package, right.no_install_package)
    field("maturin-version", left.maturin_version, right.maturin_version)
    field("maturin-subcommand", left.maturin_subcommand, right.maturin_subcommand)
    field("uv-run-flags", sorted(left.uv_run_flags), sorted(right.uv_run_flags))
    field("pytest-path", left.pytest_path, right.pytest_path)
    for key in sorted(REQUIRED_ENV):
        field(f"env:{key}", left.env.get(key), right.env.get(key))

    # Absolute value floors (coordinated disarm / denominator shrink must not green).
    for surface in (left, right):
        if surface.env.get("REPARK_PARITY_LIVE") != "1":
            errors.append(
                f"parity-live dual-wire absolute floor on {surface.side}: "
                f"REPARK_PARITY_LIVE must be '1' (got {surface.env.get('REPARK_PARITY_LIVE')!r})"
            )
        if surface.env.get("SPARK_LOCAL_IP") != "127.0.0.1":
            errors.append(
                f"parity-live dual-wire absolute floor on {surface.side}: "
                f"SPARK_LOCAL_IP must be '127.0.0.1' "
                f"(got {surface.env.get('SPARK_LOCAL_IP')!r})"
            )
        if surface.pytest_path != "python/repark/tests":
            errors.append(
                f"parity-live dual-wire absolute floor on {surface.side}: "
                f"pytest path must be 'python/repark/tests' "
                f"(got {surface.pytest_path!r}) — shrunken denominator is not a gate"
            )
    return errors


def main() -> int:
    if not MAKEFILE.is_file():
        print(f"ERROR: Makefile not found at {MAKEFILE}", file=sys.stderr)
        return 2
    if not WORKFLOW.is_file():
        print(f"ERROR: workflow not found at {WORKFLOW}", file=sys.stderr)
        return 2

    makefile_text = MAKEFILE.read_text(encoding="utf-8")
    workflow_text = WORKFLOW.read_text(encoding="utf-8")

    try:
        makefile_surface = parse_makefile(makefile_text)
        workflow_surface = parse_workflow(workflow_text)
    except ParseError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        print(
            "parity-live dual-wire: FAIL (parse miss — fail-closed; fix the surface or the "
            "extractor in scripts/check_parity_live_dual_wire.py)",
            file=sys.stderr,
        )
        return 1

    errors = compare(makefile_surface, workflow_surface)
    if errors:
        for line in errors:
            print(f"ERROR: {line}", file=sys.stderr)
        print(
            "parity-live dual-wire: FAIL — Makefile `parity-live` and "
            ".github/workflows/parity-live.yml disagree on load-bearing tokens "
            "(change one, change the other).",
            file=sys.stderr,
        )
        return 1

    print(
        "parity-live dual-wire: OK "
        f"(maturin@{makefile_surface.maturin_version}, "
        f"extras={sorted(makefile_surface.sync_extras)}, "
        f"uv-run={sorted(makefile_surface.uv_run_flags)})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
