# WC — check_lib_rs stale-exception backport

**Date:** 2026-08-11 · **Lane:** repark chore · **Engine:** direct (conductor) · **claims_critic:** N/A (tiny gate)

## Charge

Backport G-8's fail-closed stale-EXCEPTIONS check into `scripts/check_lib_rs.py`.
Keys are **crate directory names**, not repo-relative paths (unlike
`check_rust_file_size.py`).

## Decision

**D-WC-1 — Crate-key adaptation.** ERROR when `crates/<key>/src/lib.rs` is missing:
`ERROR: EXCEPTIONS key has no crate root on disk: <key> (remove the row or restore crates/<key>/src/lib.rs)`.
No new wiring — script already in `make ci`.

## Gate evidence

```text
$ python3 scripts/check_lib_rs.py
lib-rs: 9 crate roots clean (no inline test modules; ceilings held)
(exit 0)
```

## Provocation (must-FAIL stale key)

Temporarily injected
`EXCEPTIONS["does_not_exist_wc_provocation"] = (150, "provocation")`; restored after.

```text
ERROR: EXCEPTIONS key has no crate root on disk: does_not_exist_wc_provocation (remove the row or restore crates/does_not_exist_wc_provocation/src/lib.rs)
lib-rs: FAIL — 1 violation(s) across 9 crate roots
(exit 1)
```

## Deviations

none.

## Out of scope

check_lib_py stale backport; pre-commit path HOLD from G-8; unit-queue discharge
(orchestrator-side).

## Landing note (L-1, 2026-08-12)

Classified **ALREADY-LANDED** / no-registry-surface — mechanical gate, not a Spark
divergence.
