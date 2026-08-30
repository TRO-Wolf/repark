#!/usr/bin/env python3
"""Enforce exact per-file line baselines over every crates/**/*.rs source file.

SSOT for general Rust file size (companion to check_lib_rs). Prose points here and never
restates the ceilings. Mirrors the check_lib_rs dual-wire shape (py = logic + SSOT,
sh = wrapper).

Rules over every *.rs under crates/ (recursive):
1. The default is DEFAULT_CEILING. Blank lines count.
2. EXCEPTIONS records the exact current line count, debt reason, and cohesive
   split seam for each existing offender.
3. An excepted file must equal its baseline. Growth fails. Shrinkage also fails
   until the row ratchets down, or is removed when the file reaches the default.
4. Sources under tests/goldens/ and tests/fixtures/ are generated-test inputs
   and are outside the scan.

Exit 0 on clean; non-zero with path, measured count, ceiling, and sanctioned
outs. Fail-closed: unreadable file, empty scan set, or an EXCEPTIONS key whose
path no longer exists is an error, never a skip.
"""

from __future__ import annotations

import sys
from pathlib import Path

DEFAULT_CEILING = 1000
EXEMPT_PATHS: tuple[tuple[str, ...], ...] = (("tests", "goldens"), ("tests", "fixtures"))

# repo-relative posix path -> (exact baseline, debt reason, cohesive split seam).
# Every row retires when its file reaches DEFAULT_CEILING. A baseline increase
# requires explicit owner approval; ordinary edits only ratchet rows down.
EXCEPTIONS: dict[str, tuple[int, str, str]] = {
    "crates/repark-core/src/catalog_config.rs": (
        1044,
        "Session catalog configuration still owns every backend shape.",
        "Split backend-specific option parsing from shared session installation.",
    ),
    "crates/repark-core/src/dynamic_flatten/tests.rs": (
        1469,
        "Dynamic-flatten behavior and refusal scenarios share one test module.",
        "Split structural cases from list and refusal cases with an identity check.",
    ),
    "crates/repark-core/src/session.rs": (
        1040,
        "The session root still combines construction, planning, and execution entry points.",
        "Extract one existing responsibility when a charter already changes that region.",
    ),
    "crates/repark-core/src/session/tests/session.rs": (
        1415,
        "Session behavior scenarios remain in one file-backed test module.",
        "Split by configuration, planning, and execution scenario families.",
    ),
    "crates/repark-core/tests/declared_sorted.rs": (
        1381,
        "Declared-order integration cases share one end-to-end battery.",
        "Split by ordering source while preserving public-entry coverage.",
    ),
    "crates/repark-functions/src/analyzer.rs": (
        1194,
        "Spark analyzer rewrites remain grouped in one rule implementation.",
        "Extract a cohesive rewrite family when that family next changes.",
    ),
    "crates/repark-functions/src/datetime.rs": (
        1783,
        "Calendar and timestamp Spark-semantics functions share one module.",
        "Split calendar extractors from timezone-aware timestamp functions.",
    ),
    "crates/repark-iceberg/src/catalog/tests/catalog.rs": (
        1843,
        "Memory, Glue, and S3 Tables catalog adapter tests share one battery.",
        "Split tests by catalog backend with shared helpers kept local.",
    ),
    "crates/repark-iceberg/src/write/alter.rs": (
        1641,
        "Iceberg ALTER operations share one transaction adapter.",
        "Split property, rename, and schema-evolution operation families.",
    ),
    "crates/repark-iceberg/src/write/append.rs": (
        1950,
        "Append planning, file writing, and commit assembly share one entry module.",
        "Extract writer preparation from transaction commit assembly.",
    ),
    "crates/repark-iceberg/src/write/merge/mod.rs": (
        2131,
        "The RePark-owned MERGE executor combines plan, COW, and MOR paths.",
        "Split plan preparation from COW and MOR execution modules.",
    ),
    "crates/repark-iceberg/src/write/merge/tests/merge.rs": (
        1091,
        "General MERGE behavior cases remain in one file-backed module.",
        "Split common plan cases from write-mode-specific cases.",
    ),
    "crates/repark-iceberg/src/write/merge/tests/occ_conflict.rs": (
        1023,
        "Optimistic-concurrency conflict cases share one scenario battery.",
        "Split retryable conflicts from terminal conflict cases.",
    ),
    "crates/repark-iceberg/src/write/merge/tests/streaming_scan.rs": (
        3028,
        "Streaming MERGE scan and rewrite scenarios share one test battery.",
        "Split position-delete, rewrite, and scan-shape scenario families.",
    ),
    "crates/repark-iceberg/src/write/overwrite.rs": (
        1070,
        "Overwrite planning and commit behavior share one module.",
        "Extract predicate and file-selection logic from commit assembly.",
    ),
    "crates/repark-iceberg/src/write/predicate_dml.rs": (
        1227,
        "Predicate DELETE and UPDATE planning share one adapter.",
        "Split predicate validation from operation-specific plan construction.",
    ),
    "crates/repark-iceberg/src/write/predicate_dml/tests/predicate_dml.rs": (
        1442,
        "Predicate DML scenarios share one consolidated test module.",
        "Split DELETE and UPDATE scenario families with shared setup retained.",
    ),
    "crates/repark-python/src/column/mod.rs": (
        1105,
        "PyO3 Column methods remain grouped in one binding module.",
        "Extract the remaining date or window method family.",
    ),
    "crates/repark-python/src/dataframe.rs": (
        1171,
        "PyO3 DataFrame methods share one binding surface.",
        "Split action methods from plan-building methods without moving row work to Python.",
    ),
    "crates/repark-python/src/session.rs": (
        1178,
        "PyO3 session construction and query entry points share one module.",
        "Split configuration bindings from query and catalog bindings.",
    ),
    "crates/repark-spark/src/alter.rs": (
        1831,
        "Spark ALTER token rewrites and dispatch share one planner module.",
        "Split syntax normalization from Iceberg operation dispatch.",
    ),
    "crates/repark-spark/src/metadata_tables.rs": (
        1062,
        "Metadata-table parsing and plan construction share one module.",
        "Extract identifier resolution from metadata plan assembly.",
    ),
    "crates/repark-spark/src/ref_ddl.rs": (
        1028,
        "Reference DDL parsing and execution routing share one module.",
        "Split branch and tag statement families.",
    ),
    "crates/repark-spark/src/tests/alter.rs": (
        1436,
        "Spark ALTER behavior cases share one test module.",
        "Split property operations from schema-evolution operations.",
    ),
    "crates/repark-spark/src/tests/call.rs": (
        1307,
        "Spark CALL procedure cases share one test module.",
        "Split parsing failures from procedure execution scenarios.",
    ),
    "crates/repark-spark/src/tests/ctas.rs": (
        1361,
        "CTAS behavior and property scenarios share one test module.",
        "Split format and property cases from query-shape cases.",
    ),
    "crates/repark-spark/src/tests/dml.rs": (
        1154,
        "Spark DML door scenarios share one integration module.",
        "Split DELETE, UPDATE, and shared refusal families.",
    ),
    "crates/repark-spark/src/tests/insert_overwrite.rs": (
        1249,
        "INSERT OVERWRITE modes share one scenario battery.",
        "Split partitioned from unpartitioned overwrite cases.",
    ),
    "crates/repark-spark/src/tests/merge.rs": (
        1303,
        "Spark MERGE syntax and execution cases share one module.",
        "Split matched-action from not-matched-action scenarios.",
    ),
    "crates/repark-spark/src/tests/partitioned_merge.rs": (
        1068,
        "Partitioned MERGE cases share one integration battery.",
        "Split partition transforms from delete-file interaction cases.",
    ),
    "crates/repark-spark/src/tests/transform_overwrite.rs": (
        1181,
        "Transform-partition overwrite cases share one module.",
        "Split transform families while keeping door-level coverage.",
    ),
    "crates/repark-spark/src/window_range.rs": (
        1225,
        "Window RANGE validation and rewrite behavior share one module.",
        "Split frame validation from expression lowering.",
    ),
    "crates/repark-sql/src/guards/tests.rs": (
        1207,
        "ANSI guard refusal cases share one file-backed module.",
        "Split guards by statement or expression family.",
    ),
    "crates/repark-sql/src/tests.rs": (
        1523,
        "Native ANSI-door end-to-end cases remain consolidated.",
        "Split statement families into production-aligned test modules.",
    ),
    "crates/repark-sql/tests/cross_door.rs": (
        1259,
        "Cross-door parity cases share one integration battery.",
        "Split syntax-equivalence from deliberate-divergence cases.",
    ),
    "crates/repark-ta/src/momentum.rs": (
        2098,
        "TA-Lib momentum indicators share one verbatim-port module.",
        "Split by indicator family only with an identity-diff proof.",
    ),
    "crates/repark-ta/src/overlap.rs": (
        1578,
        "TA-Lib overlap indicators share one verbatim-port module.",
        "Split moving-average and band families only with identity proof.",
    ),
    "crates/repark-ta/src/udf/mod.rs": (
        1821,
        "Window UDF cache, densification, specs, and dispatch share one module.",
        "Extract statistic and math dispatch from shared evaluation mechanics.",
    ),
}


def _is_exempt(path: Path, repo: Path) -> bool:
    """Return whether a source path is under an approved generated-test directory."""
    parts = path.relative_to(repo).parts
    return any(
        parts[index : index + len(exempt)] == exempt
        for exempt in EXEMPT_PATHS
        for index in range(len(parts) - len(exempt) + 1)
    )


def _validate_exception(relative: str, exception: tuple[int, str, str]) -> list[str]:
    """Validate one exception row as actionable debt above the default."""
    baseline, reason, split_seam = exception
    errors: list[str] = []
    if baseline <= DEFAULT_CEILING:
        errors.append(
            f"ERROR: {relative}: exception baseline {baseline} is not above default "
            f"{DEFAULT_CEILING}; remove the exception row."
        )
    if not reason.strip():
        errors.append(f"ERROR: {relative}: exception debt reason must not be empty.")
    if not split_seam.strip():
        errors.append(f"ERROR: {relative}: exception split seam must not be empty.")
    return errors


def check_file(path: Path, repo: Path) -> list[str]:
    errors: list[str] = []
    rel = path.relative_to(repo).as_posix()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"ERROR: {rel}: unreadable ({exc})")
        return errors

    # Docs count toward the ceiling.
    line_count = len(text.splitlines())
    exception = EXCEPTIONS.get(rel)
    if exception is None:
        if line_count <= DEFAULT_CEILING:
            return errors
        errors.append(
            f"ERROR: {rel} is {line_count} lines (default {DEFAULT_CEILING}). "
            "Sanctioned outs: (1) split at a cohesive boundary, or (2) add an "
            "owner-approved EXCEPTIONS row with the exact baseline, debt reason, and split seam."
        )
        return errors

    baseline, reason, split_seam = exception
    errors.extend(_validate_exception(rel, exception))
    if line_count > baseline:
        errors.append(
            f"ERROR: {rel} grew to {line_count} lines (exact baseline {baseline}). "
            f"Debt: {reason} Split seam: {split_seam} "
            "Split the file, make the change line-neutral, or obtain explicit owner approval "
            "for a reviewed baseline amendment."
        )
    elif line_count < baseline:
        action = (
            "remove the exception row"
            if line_count <= DEFAULT_CEILING
            else f"ratchet the baseline down to {line_count}"
        )
        errors.append(
            f"ERROR: {rel} shrank to {line_count} lines below exact baseline {baseline}; {action}."
        )
    return errors


def main() -> int:
    repo = Path(__file__).resolve().parent.parent
    crates_root = repo / "crates"
    if not crates_root.is_dir():
        print("ERROR: crates/ not found", file=sys.stderr)
        return 2

    paths = sorted(
        path for path in crates_root.rglob("*.rs") if path.is_file() and not _is_exempt(path, repo)
    )
    if not paths:
        print(
            "ERROR: crates/**/*.rs scan set is empty — refuse to pass closed",
            file=sys.stderr,
        )
        return 2

    all_errors: list[str] = []
    scanned = {path.relative_to(repo).as_posix() for path in paths}
    for rel in sorted(EXCEPTIONS):
        if rel not in scanned:
            all_errors.append(
                f"ERROR: EXCEPTIONS key is outside the scan set: {rel} "
                "(remove the row or restore the source path)"
            )

    checked = 0
    for path in paths:
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
