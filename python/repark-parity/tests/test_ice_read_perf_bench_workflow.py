"""ICE-READ-PERF-0 pins over the dispatch-only bench job of ``aws-acceptance.yml``.

pins: ice-read-perf-0/C-018, C-019; ice-bench-baseline-1/C-004
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_AWS_YML = _REPO / ".github" / "workflows" / "aws-acceptance.yml"

LIVE_AWS_CONDITION = "github.event_name == 'schedule' || inputs.leg == 'acceptance'"
BENCH_CONDITION = "github.event_name == 'workflow_dispatch' && inputs.leg == 'ice-read-perf-bench'"
UPLOAD_ARTIFACT = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
MODES = ("cold", "warm", "concurrent", "concurrent-cold")
REF_GUARD = "if: github.ref != 'refs/heads/main'"
ORDERED_MARKERS = (
    REF_GUARD,
    "actions/checkout@",
    "dtolnay/rust-toolchain@",
    "Swatinem/rust-cache@",
    "--bench ice_read_perf --no-run",
    "aws-actions/configure-aws-credentials@",
    "setup --catalog s3tables",
    "put-table-maintenance-configuration",
    "get-table-maintenance-configuration",
    "--phase write",
    "setup --catalog glue",
    'run --mode "${mode}" --catalog s3tables',
    'run --mode "${mode}" --catalog glue',
    "run_io_total.bytes",
    UPLOAD_ARTIFACT,
)


def _text() -> str:
    """Return the workflow text."""
    return _AWS_YML.read_text(encoding="utf-8")


def _job_block(text: str, job: str) -> str:
    """Return the body of ``job`` (everything up to the next two-space key)."""
    match = re.search(rf"(?ms)^  {re.escape(job)}:\s*\n(.*?)(?=^  \S|\Z)", text)
    assert match is not None, f"no job {job}"
    return match.group(1)


def _job_if(block: str) -> str:
    """Return the job-level ``if:`` expression."""
    match = re.search(r"(?m)^    if:\s*(.+?)\s*$", block)
    assert match is not None, "job has no if:"
    return match.group(1)


def _steps(block: str) -> list[str]:
    """Split a job body into its step chunks."""
    return re.split(r"(?m)^      - ", block)[1:]


def _run_script(step: str) -> str | None:
    """Return a step's ``run:`` script as bash sees it (``>`` folded, ``|`` kept)."""
    lines = step.splitlines()
    for index, line in enumerate(lines):
        match = re.match(r"^(\s*)run:\s*(.*?)\s*$", line)
        if match is None:
            continue
        indent, value = len(match.group(1)), match.group(2)
        if value not in ("|", ">"):
            return value
        body = []
        for following in lines[index + 1 :]:
            if following.strip() and len(following) - len(following.lstrip()) <= indent:
                break
            body.append(following.strip())
        if value == ">":
            return " ".join(part for part in body if part)
        return "\n".join(body)
    return None


def _run_scripts(block: str) -> list[str]:
    """Return every ``run:`` script of a job body, in step order."""
    return [script for step in _steps(block) if (script := _run_script(step)) is not None]


def _logical_lines(script: str) -> list[str]:
    """Join backslash continuations so each entry is one shell command line."""
    return re.sub(r"\\\n\s*", " ", script).splitlines()


def _expansions(script: str) -> tuple[list[str], list[str]]:
    """Return ``(quoted, unquoted)`` ``${…}`` expansions, tracking quotes through ``$(…)``."""
    quoted: list[str] = []
    unquoted: list[str] = []
    stack = ["plain"]
    index = 0
    while index < len(script):
        top = stack[-1]
        if script[index] == "\\":
            index += 2
        elif top == "plain" and script[index] == "'":
            index = script.index("'", index + 1) + 1
        elif script.startswith("$((", index):
            stack.append("arith")
            index += 3
        elif top == "arith" and script.startswith("))", index):
            stack.pop()
            index += 2
        elif script.startswith("${", index):
            end = script.index("}", index) + 1
            (quoted if top == "double" else unquoted).append(script[index:end])
            index = end
        elif script.startswith("$(", index):
            stack.append("plain")
            index += 2
        elif script[index] == '"' and top in ("plain", "double"):
            if top == "double":
                stack.pop()
            else:
                stack.append("double")
            index += 1
        elif script[index] == ")" and top == "plain" and len(stack) > 1:
            stack.pop()
            index += 1
        else:
            index += 1
    assert stack == ["plain"], (stack, script)
    return quoted, unquoted


def _uses_pins(block: str) -> dict[str, str]:
    """Map each action used in ``block`` to its pinned ref."""
    return dict(re.findall(r"uses:\s*([\w./-]+)@([0-9a-f]{40})", block))


def test_the_nightly_job_runs_only_on_the_schedule_or_the_acceptance_leg() -> None:
    """The existing job keeps its nightly and gains only the leg filter."""
    text = _text()
    assert _job_if(_job_block(text, "live-aws")) == LIVE_AWS_CONDITION
    assert 'cron: "43 8 * * *"' in text


def test_the_dispatch_offers_the_two_legs_with_acceptance_by_default() -> None:
    """``leg`` is a choice defaulting to acceptance; ``purpose`` is a free string."""
    text = _text()
    dispatch = text[text.index("  workflow_dispatch:") : text.index("\npermissions:")]
    assert re.search(r"(?m)^      leg:\s*$", dispatch)
    assert "type: choice" in dispatch
    assert re.search(r"(?m)^          - acceptance\s*$", dispatch)
    assert re.search(r"(?m)^          - ice-read-perf-bench\s*$", dispatch)
    assert re.search(r"(?m)^        default: acceptance\s*$", dispatch)
    assert re.search(r"(?m)^      purpose:\s*$", dispatch)


def test_the_bench_job_runs_only_on_its_dispatch_behind_the_same_gate() -> None:
    """Dispatch-only, the aws-acceptance environment, the ref guard, job-scoped OIDC."""
    block = _job_block(_text(), "ice-read-perf-bench")
    assert _job_if(block) == BENCH_CONDITION
    assert re.search(r"(?m)^    environment: aws-acceptance\s*$", block)
    assert re.search(r"(?m)^      id-token: write\s*$", block)
    assert "if: github.ref != 'refs/heads/main'" in block
    assert "persist-credentials: false" in block
    assert "continue-on-error" not in block


def test_the_bench_job_uses_the_same_pinned_actions_as_the_acceptance_job() -> None:
    """Every action the bench shares with live-aws sits at the same SHA."""
    text = _text()
    live = _uses_pins(_job_block(text, "live-aws"))
    bench = _uses_pins(_job_block(text, "ice-read-perf-bench"))
    for action in (
        "actions/checkout",
        "dtolnay/rust-toolchain",
        "Swatinem/rust-cache",
        "aws-actions/configure-aws-credentials",
    ):
        assert bench[action] == live[action], action
    assert f"uses: {UPLOAD_ARTIFACT}" in _job_block(text, "ice-read-perf-bench")


def test_the_bench_builds_before_credentials_and_disables_compaction_before_any_write() -> None:
    """Step order: ref guard, build, credentials, create, compaction off, write, runs."""
    block = _job_block(_text(), "ice-read-perf-bench")
    positions = [block.index(marker) for marker in ORDERED_MARKERS]
    assert positions == sorted(positions), list(zip(ORDERED_MARKERS, positions, strict=True))
    assert block.count(REF_GUARD) == 1
    first_step = _steps(block)[0]
    assert REF_GUARD in first_step, first_step
    assert _run_script(first_step) is not None
    assert "exit 1" in (_run_script(first_step) or ""), first_step
    assert "--type icebergCompaction" in block
    assert '--value \'{"status":"disabled"}\'' in block
    assert "--query 'configuration.icebergCompaction.status'" in block
    assert '!= "disabled"' in block
    assert "s3tables:PutTableMaintenanceConfiguration" in block
    assert "s3tables:GetTableMaintenanceConfiguration" in block


def test_every_mode_runs_on_both_catalogs_at_one_repeat_with_r3_failing_the_job() -> None:
    """Four modes per catalog, ``--repeat 1`` (Q-24a-1), every step under ``set -e``."""
    block = _job_block(_text(), "ice-read-perf-bench")
    assert re.search(r'(?m)^      BENCH_REPEAT: "1"\s*$', block)
    assert block.count(f"for mode in {' '.join(MODES)}; do") == 2
    assert block.count('--repeat "${BENCH_REPEAT}"') == 2
    assert "BENCH_NAMESPACE: testing_repark_acceptance" in block
    assert "BENCH_TABLE: ice_read_perf_bench" in block
    for step in _steps(block):
        if "run: |" in step and ("cargo bench" in step or "aws s3tables" in step):
            assert "set -euo pipefail" in step, step


def test_no_expression_reaches_a_run_script_and_the_job_carries_no_comment() -> None:
    """Inputs, vars and secrets pass through ``env:``; no ``#`` comment in the new job."""
    block = _job_block(_text(), "ice-read-perf-bench")
    for step in _steps(block):
        if "run:" in step:
            script = step[step.index("run:") :]
            assert "${{" not in script, script
    for line in block.splitlines():
        assert not line.lstrip().startswith("#"), line
        assert not re.search(r"@[0-9a-f]{40}\s+#", line), line


def test_no_bench_or_s3tables_failure_can_be_swallowed() -> None:
    """R-3's exit 3 and every AWS failure reach ``set -e``: no ``||``, ``&&`` or ``;`` recovery."""
    scripts = _run_scripts(_job_block(_text(), "ice-read-perf-bench"))
    recoveries = [match for script in scripts for match in re.finditer(r"\|\|", script)]
    assert len(recoveries) == 2, [script for script in scripts if "||" in script]
    for script in scripts:
        for match in re.finditer(r"\|\|", script):
            assert re.match(r'\|\| stop "[^"]+"\s*$', script[match.start() :].splitlines()[0]), (
                script[match.start() :].splitlines()[0]
            )
    compaction = [script for script in scripts if "aws s3tables" in script]
    assert len(compaction) == 1
    assert compaction[0].count('|| stop "') == 2
    bench_lines = [
        line.strip()
        for script in scripts
        for line in _logical_lines(script)
        if "cargo bench" in line
    ]
    assert len(bench_lines) == 7, bench_lines
    for line in bench_lines:
        assert line.startswith("cargo bench --locked -p repark-spark --bench ice_read_perf"), line
        assert not re.search(r"\|\||&&|;|\|", line), line
    aws_lines = [
        line.strip()
        for script in compaction
        for line in _logical_lines(script)
        if "aws s3tables" in line
    ]
    assert len(aws_lines) == 2, aws_lines
    for line in aws_lines:
        assert line.count("||") == 1, line
        assert re.search(r'\|\| stop "[^"]+"$', line), line
        assert not re.search(r"&&|;", line), line


def test_errexit_is_never_turned_off_in_the_bench_scripts() -> None:
    """A ``set +e`` (or ``set +o errexit``) would let the R-3 exit 3 pass unseen."""
    scripts = _run_scripts(_job_block(_text(), "ice-read-perf-bench"))
    for script in scripts:
        assert not re.search(r"\bset\s+\+[A-Za-z]*e", script), script
        assert not re.search(r"\bset\s+\+o\s+(errexit|pipefail)", script), script
    for doctored in ("set +e", "set +xe", "set +o errexit", "set +o pipefail"):
        assert re.search(r"\bset\s+\+[A-Za-z]*e|\bset\s+\+o\s+(errexit|pipefail)", doctored)


def test_every_variable_expansion_in_the_bench_scripts_is_braced() -> None:
    """An unbraced ``$NAME`` would dodge the double-quoting pin, so every expansion is braced."""
    for script in _run_scripts(_job_block(_text(), "ice-read-perf-bench")):
        assert re.findall(r"\$[A-Za-z_][A-Za-z0-9_]*", script) == [], script


def test_every_variable_expansion_in_the_bench_scripts_is_double_quoted() -> None:
    """No ``${…}`` in a bench ``run:`` script is left to word splitting or globbing."""
    quoted: list[str] = []
    for script in _run_scripts(_job_block(_text(), "ice-read-perf-bench")):
        inside, outside = _expansions(script)
        assert outside == [], (outside, script)
        quoted.extend(inside)
    assert "${PURPOSE:-unstated}" in quoted
    assert quoted.count("${TABLE_BUCKET_ARN}") >= 6
    assert quoted.count("${mode}") == 4
    assert _expansions('a ${X} "b ${Y}" \'${Z}\' "$(c "${W}") ${V}" $((1 + 2))') == (
        ["${Y}", "${W}", "${V}"],
        ["${X}"],
    )


def test_the_dispatch_offers_a_baseline_before_half_input() -> None:
    """``baseline`` is a boolean defaulting to false: the before half of a pair."""
    text = _text()
    dispatch = text[text.index("  workflow_dispatch:") : text.index("\npermissions:")]
    assert re.search(r"(?m)^      baseline:\s*$", dispatch)
    assert "run with page selection off and no shared caches" in dispatch
    assert "the before half of a pair on one head" in dispatch
    assert re.search(r"(?m)^        type: boolean\s*$", dispatch)
    assert re.search(r"(?m)^        default: false\s*$", dispatch)


def test_the_bench_run_steps_thread_the_baseline_flag_through_env() -> None:
    """Both mode loops take ``BASELINE`` via ``env:`` and pass ``--baseline`` when true."""
    block = _job_block(_text(), "ice-read-perf-bench")
    assert block.count("BASELINE: ${{ inputs.baseline }}") == 3
    loops = [
        script
        for script in _run_scripts(block)
        if "cargo bench" in script and "for mode in" in script
    ]
    assert len(loops) == 2, loops
    for script in loops:
        assert "${{" not in script, script
        assert 'if [ "${BASELINE}" = "true" ]; then' in script, script
        assert "baseline_flags=(--baseline)" in script, script
        assert '"${baseline_flags[@]}"' in script, script


def test_the_step_summary_names_the_baseline_beside_the_purpose() -> None:
    """The summary header carries ``baseline=<value>`` next to the purpose."""
    block = _job_block(_text(), "ice-read-perf-bench")
    summary = [step for step in _steps(block) if "run_io_total.bytes" in step]
    assert len(summary) == 1, summary
    assert "BASELINE: ${{ inputs.baseline }}" in summary[0], summary[0]
    script = _run_script(summary[0])
    assert script is not None
    assert "baseline=${BASELINE:-false}" in script, script
