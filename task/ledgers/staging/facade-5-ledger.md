# Unit ledger — FACADE-5 · display renderer in Rust — step 0

**Date:** 2026-09-14 · **Branch:** `perf/facade-5-s0` (C-001..C-008) · **Base:** `e147685b`
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card FACADE-5 (audit §8): the display renderer — the `display.py`
bodies (514 lines) and the `plan_collapse.py` / `polars_cells.py` formatters —
moves to Rust **byte-identical or not at all**. The audit left its format cost
UNMEASURED (§6): the quoted eager walls were scan effects, never render costs.
Step 0 is measurement and pins only — the fetch/format split baseline per
renderer, the renderer × truncation-rule census, the missing goldens with
mutation proofs, and the step-1 target. No product code under
`python/repark/src/` or `crates/` changes in this step.

**Not in this step:** `STATUS.md`, `python/repark/src/`, `crates/`,
`dataframe/core.py`, `dataframe/eager.py`, `dataframe/cache_handle.py`,
`spark/catalog.py`, `spark/functions_collections.py`, the fenced Rust files the
brief names. No JVM. Steps 1+ are later branches.

**Interpreter.** This clone has no `.venv` and builds nothing. Every Python
command runs `/tmp/f-types4/.venv/bin/python` (release native,
`repark._native.__debug_assertions__` False); `repark` resolves to
`/tmp/f-types4/python/repark/src`, whose product files equal `main`'s:

```
$ git -C /tmp/f-types4 diff origin/main -- python/repark/src crates
$ /tmp/f-types4/.venv/bin/python -c "import repark._native; print(repark._native.__debug_assertions__)"
False
```

Timing processes run under `systemd-run --user --scope -p MemoryMax=8G -p
MemorySwapMax=0` with `OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8`; each cell
waits for an idle box (no cargo/rustc/maturin) and a 1-minute load under 6,
recorded beside the cell.

## PROPOSITION LEDGER — FACADE-5 step 0 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Provenance: release native (`__debug_assertions__` False), types4 product tree equals `main`, no JVM, memory-capped timing harness. | Header block above; `docs/perf/facade-5-display-baseline-2026-09-14.md` machine header. | **OPEN** | Verified at session start; runner re-asserts `native_is_release()` per run. |
| C-002 | Fetch/format baseline: FETCH (capped rows to Arrow) and FORMAT (formatter over the pre-materialized table) timed separately for ASCII `show`, vertical `show`, `_repr_html_`, duckdb show, polars show, and `repr`, at n ∈ {20, 1000} × truncate on/off × {flat 7-type, 50-column wide, nested struct/array/map}, warmup + 5 reps, medians, idle box + load<6 per cell; each leg's share of the wall stated. | `docs/perf/facade-5-display-baseline-2026-09-14.md` + committed runner. | **OPEN** | Pending measurement. |
| C-003 | Census: every renderer × truncation-rule pair names the §8 pin that binds its bytes today, and every unbound pair is a row of its own; the `test_dfcore_6_eager_preview.py` scan counts recorded. | Census section below. | **OPEN** | Pending. |
| C-004 | Missing goldens only: every unbound pair gets a golden recorded from this base tree under `python/repark/tests/`; record mode refused under CI. | `test_facade_5_display_goldens.py` + committed JSON golden. | **OPEN** | Pending. |
| C-005 | Mutation proof: a one-line mutation turns the new goldens red; restore leaves them green. | Scratch edit, red output, `git checkout` restore, all recorded here. | **OPEN** | Pending. |
| C-006 | The §8 named pins stay green and unedited: `test_dfcore_4b_eager_goldens.py`, `test_dfcore_4b_show_goldens.py`, `test_df_eager_1.py`, `test_dfcore_6_eager_preview.py`, `test_display_styles.py`, `test_display_polars_default.py`. | Those files, same commit, no edits; run counts. | **OPEN** | Pending. |
| C-007 | Step-1 target named from C-002's numbers: (A) a measured format wall and the Rust move that removes it byte-identically, or (B) "no format wall: ship the smallest byte-identical consolidation", naming which formatters move and what stays because it touches `eager.py`. | Step-1 target section. | **OPEN** | Pending. |
| C-008 | Gates: §8 pins green, the new goldens, `make verify`, `test_production_file_size.py` green; staged-diff comment scan empty. | Commands and counts in Evidence. | **OPEN** | Pending. |

## Evidence

Skeleton commit; sections fill as the step lands.
