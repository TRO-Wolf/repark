"""Render a W-0 :class:`RunResult` as dated markdown."""

from __future__ import annotations

from .classify import OUTCOME_REFUSE
from .models import CellResult, RunResult


def one_line(text: str, *, limit: int = 180) -> str:
    """Collapse whitespace so a markdown table cell stays one row.

    Args:
        text: raw engine message.
        limit: max characters after collapse.

    Returns:
        A single-line string with pipes neutralized.
    """
    collapsed = " ".join(text.split()).replace("|", "/")
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 3] + "..."


def _timing_line(cell: CellResult) -> list[str]:
    """Format one cell's per-engine timings."""
    lines = [f"### `{cell.label}` — {cell.rows} rows", "", "```sql", cell.sql, "```", ""]
    lines.append("| engine | outcome | median_ms | samples_ms | plan | message |")
    lines.append("|---|---|---:|---|---|---|")
    for timing in cell.timings:
        samples = ",".join(f"{sample:.1f}" for sample in timing.samples_ms)
        median = "" if timing.median_ms is None else f"{timing.median_ms:.1f}"
        plan = " ".join(timing.plan_tokens)
        message = one_line(timing.message or "")
        lines.append(
            f"| {timing.engine} | {timing.outcome} | {median} | {samples} | {plan} | {message} |"
        )
    lines.append("")
    return lines


def render_markdown(result: RunResult) -> str:
    """Render the dated results document.

    Args:
        result: a completed run.

    Returns:
        GitHub-flavored markdown.
    """
    refuse = [row for row in result.probe if row.outcome == OUTCOME_REFUSE]
    lines = [
        "# W-0 window-shape bench results",
        "",
        "Measure-only. Ratios over absolutes on a noisy `schedutil` box (P-2 posture).",
        "The 1e7 wall clock is one host's number, not a CI pin.",
        "",
        "## Machine and pins",
        "",
        f"- scale: `{result.scale}`",
        f"- seed: `{result.seed}`",
        f"- engine: `{result.engine_version}`",
        f"- native: `{result.native_build}`",
        f"- DuckDB: `{result.duckdb_version or result.duckdb_skip_reason}`",
        f"- PySpark: `{result.pyspark_version or result.pyspark_skip_reason}`",
        f"- cpu: `{result.machine.get('cpu', 'unknown')}`",
        f"- cores: `{result.machine.get('cores', 'unknown')}`",
        f"- governor: `{result.machine.get('governor', 'unknown')}`",
        f"- ram_gib: `{result.machine.get('ram_gib', 'unknown')}`",
        f"- process_hwm_rss_bytes: `{result.peak_rss_bytes}`",
        f"- wall_seconds: `{result.wall_seconds:.1f}`",
        f"- scratch_deleted: `{result.scratch_deleted}`",
        "",
        "## Generated dataset sizes (bytes, before delete)",
        "",
        "| path | bytes |",
        "|---|---:|",
    ]
    for path, size in sorted(result.dataset_bytes.items()):
        lines.append(f"| `{path}` | {size} |")
    lines.extend(["", "## Sliding-frame probe (C-002 / C-009)", ""])
    lines.append("| name | intake_class | outcome | message |")
    lines.append("|---|---|---|---|")
    for row in result.probe:
        message = one_line(row.message or "")
        lines.append(f"| `{row.name}` | {row.intake_class} | {row.outcome} | {message} |")
    lines.extend(
        [
            "",
            f"Refuse count: **{len(refuse)}**. Each refuse name is a registry row "
            "`WIN-SLIDE-<name>`.",
            "",
            "## Method",
            "",
            "- `approx_count_distinct` is probed on int64 (`vi`). Float64 fails earlier",
            "  (`approx_distinct` not implemented for Float64) and is not a sliding refuse.",
            "- RSS is a process high-water mark (`ru_maxrss`), reported once per run as",
            "  `process_hwm_rss_bytes`. It is not a per-cell figure.",
            "- Over-`memory_limit` cells that sort before the window die in the upstream",
            "  `SortExec` / FairSpillPool `ExternalSorter`. Window-exec spill behavior is",
            "  **UNMEASURED**.",
            "- Generated scratch is deleted in a `finally` block, including Spark-start abort.",
            "- Unpartitioned `ORDER BY` at full scale is 10_000_000 rows (C-004).",
            "- Iceberg lead/lag is the RePark shape; DuckDB and PySpark run the same SQL",
            "  on in-memory tables.",
            "",
            "## Cells",
            "",
        ]
    )
    for cell in result.cells:
        lines.extend(_timing_line(cell))
    lines.extend(
        [
            "## Notes",
            "",
            "- Over-`memory_limit` records the outcome class; it does not retry a different",
            "  query (C-006 / C-010).",
            "",
        ]
    )
    return "\n".join(lines) + "\n"
