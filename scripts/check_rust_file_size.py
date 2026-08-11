#!/usr/bin/env python3
"""Enforce per-file line ceilings over every crates/**/*.rs source file.

SSOT for general Rust file size (G-8 companion to check_lib_rs). Prose
(AGENTS.md / CLAUDE.md / scripts/map.md) points here and never restates the
ceilings. Mirrors the check_lib_rs dual-wire shape (py = logic + SSOT,
sh = wrapper).

Rules over every *.rs under crates/ (recursive):
1. Per-file line ceiling: default DEFAULT_CEILING (docs count — what a reader
   scrolls past). EXCEPTIONS table overrides with reason + ratchet note.
2. Ceilings ratchet DOWN only. Raising a ceiling needs a stated reason in the
   commit that raises it — that raise-with-reason duty is a **convention**,
   not a mechanical check (the table cannot know whether a reason string is
   real). Deleting an exception row whose file still exceeds the default IS
   mechanical and fails the gate.

Exit 0 on clean; non-zero with path, measured count, ceiling, and sanctioned
outs. Fail-closed: unreadable file or empty scan set is an error, never a skip.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Seeded from post-G-4 measured reality (2026-08-11): 181 crates/**/*.rs files,
# p50≈269 / p75≈630 / p90≈1282 / max=3290. Default 1500 lets ~92% pass unlisted
# (13 exception rows). The former 14.5-KLOC tests.rs monolith no longer exists
# and is deliberately not grandfathered.
DEFAULT_CEILING = 1500

# repo-relative posix path -> (ceiling, reason). Keys sorted alphabetically.
# Ceilings only go DOWN as follow-ups land; never up without a stated reason
# in the commit that raises them. Measured counts at G-8 seed are noted in
# each reason; ceilings include small slack so a one-line edit does not force
# a table churn.
EXCEPTIONS: dict[str, tuple[int, str]] = {
    "crates/repark-core/src/session.rs": (
        1650,  # measured 1579
        "Session builder + catalog register + everything-through-Session surface "
        "(ExecutionBackend seam, Iceberg register, read_* entrypoints); "
        "RATCHET: after further session extract",
    ),
    "crates/repark-functions/src/datetime.rs": (
        2100,  # measured 2020
        "Spark-semantics calendar/datetime family (Time+Timestamp extractors, "
        "dayofweek/weekofyear shims, TZ-aware path); "
        "RATCHET: after per-family split",
    ),
    "crates/repark-iceberg/src/catalog/tests.rs": (
        2000,  # measured 1926
        "Catalog-adapter unit battery (memory/Glue/S3 Tables builders + "
        "metadata-projection pins); "
        "RATCHET: after production-aligned split",
    ),
    "crates/repark-iceberg/src/write/alter.rs": (
        1850,  # measured 1769
        "Iceberg ALTER TABLE adapter over the fork public Transaction API "
        "(properties, rename, UpdateSchema ADD/DROP/RENAME/widen); "
        "RATCHET: after per-operation modules",
    ),
    "crates/repark-iceberg/src/write/append.rs": (
        2300,  # measured 2207
        "Public append entry point (fast_append commit path, writer props, "
        "partitioned write); "
        "RATCHET: after writer/commit extract",
    ),
    "crates/repark-iceberg/src/write/merge/mod.rs": (
        2700,  # measured 2616
        "MERGE INTO executor — RePark-owned COW/MOR (fork ENGINE_CONTRACT §6 "
        "deliberately carries no MERGE); "
        "RATCHET: after COW/MOR/plan split out of mod.rs",
    ),
    "crates/repark-iceberg/src/write/merge/streaming_scan_tests.rs": (
        3400,  # measured 3290
        "MERGE streaming-scan unit battery (position-delete + rewrite pins); "
        "RATCHET: after per-scenario module split",
    ),
    "crates/repark-python/src/column.rs": (
        2200,  # measured 2136
        "PyO3 Column expression surface (thin adapter; large method surface "
        "mirrors PySpark Column operators); "
        "RATCHET: after operator-group extract",
    ),
    "crates/repark-spark/src/alter.rs": (
        2000,  # measured 1915
        "Spark-dialect ALTER TABLE planner (token rewrites sqlparser cannot "
        "model + dispatch to repark-iceberg write::alter); "
        "RATCHET: after rewrite/dispatch split",
    ),
    "crates/repark-sql/src/tests.rs": (
        1600,  # measured 1556
        "Native ANSI-door end-to-end unit battery (still monolithic; not yet "
        "production-aligned split like the Spark door's G-4); "
        "RATCHET: after production-aligned tests/ split",
    ),
    "crates/repark-ta/src/momentum.rs": (
        2600,  # measured 2508
        "TA-Lib C 0.4.0 momentum indicators battery (verbatim port: RSI/ADX/"
        "STOCH family + rate-of-change); "
        "RATCHET: after per-indicator modules if identity-diff allows",
    ),
    "crates/repark-ta/src/overlap.rs": (
        1900,  # measured 1837
        "TA-Lib C 0.4.0 overlap studies battery (verbatim port: SMA/EMA/BBANDS "
        "family); "
        "RATCHET: after per-indicator modules if identity-diff allows",
    ),
    "crates/repark-ta/src/udf.rs": (
        2200,  # measured 2098
        "DataFusion window-UDF wrappers for every TA kernel (feature "
        "`datafusion`; PartitionEvaluator::evaluate_all shape); "
        "RATCHET: after per-family UDF modules",
    ),
}


def check_file(path: Path, repo: Path) -> list[str]:
    errors: list[str] = []
    rel = path.relative_to(repo).as_posix()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"ERROR: {rel}: unreadable ({exc})")
        return errors

    # wc-style: number of lines as splitlines length (docs count toward ceiling).
    line_count = len(text.splitlines())
    ceiling, reason = EXCEPTIONS.get(rel, (DEFAULT_CEILING, "default ceiling"))
    if line_count > ceiling:
        errors.append(
            f"ERROR: {rel} is {line_count} lines (ceiling {ceiling}). "
            f"Reason on file: {reason}. "
            f"Sanctioned outs: (1) split the module, or (2) edit EXCEPTIONS in "
            f"scripts/check_rust_file_size.py with a reason (ceilings ratchet "
            f"down only)."
        )
    return errors


def main() -> int:
    repo = Path(__file__).resolve().parent.parent
    crates_root = repo / "crates"
    if not crates_root.is_dir():
        print("ERROR: crates/ not found", file=sys.stderr)
        return 2

    paths = sorted(crates_root.rglob("*.rs"))
    if not paths:
        print(
            "ERROR: crates/**/*.rs scan set is empty — refuse to pass closed",
            file=sys.stderr,
        )
        return 2

    all_errors: list[str] = []
    checked = 0
    for path in paths:
        if not path.is_file():
            continue
        checked += 1
        all_errors.extend(check_file(path, repo))

    if checked == 0:
        print(
            "ERROR: crates/**/*.rs scan set is empty — refuse to pass closed",
            file=sys.stderr,
        )
        return 2

    if all_errors:
        for err in all_errors:
            print(err, file=sys.stderr)
        print(
            f"rust-file-size: FAIL — {len(all_errors)} violation(s) across {checked} files",
            file=sys.stderr,
        )
        return 1

    print(
        f"rust-file-size: {checked} files clean "
        f"(default ceiling {DEFAULT_CEILING}; {len(EXCEPTIONS)} exceptions)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
