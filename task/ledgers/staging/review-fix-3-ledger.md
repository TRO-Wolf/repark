# Unit ledger — REVIEW-FIX-3 step 1 · the polars display honours every legal `max_rows`

**Unit:** REVIEW-FIX-3 step 1 · **Date:** 2026-09-10 · **Branch:** `fix/review-fix-3-9-14` · **Base:** `origin/main`
**Model:** swe-2-high
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** The review-1 sweep confirmed that `_render_styled_show`'s `edge =
max_rows // 2` cap makes `head_n + tail_n` sum to `max_rows - 1` at every odd value
and empties the body at `max_rows = 1` (Q-9, Q-26), that the `show(truncate=True)` →
`str_len` remap is unpinned (Q-10), and that `show`'s docstring still describes the
pre-D-5 counting rule (Q-11).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** the `n < total_rows <= max_rows` keep-set corner (existing
`test_polars_style_honors_n_keep_set` / `test_polars_style_n_caps_small_frame`
behaviour, untouched), the duckdb renderer, any dependency file, `STATUS.md`,
`briefs/next-sequence.md`, `gh`, pushes.

## PROPOSITION LEDGER — REVIEW-FIX-3 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1: `head_n + tail_n == min(n, max_rows)` for every `max_rows >= 1`, with the odd row going to the head, byte-identical to live polars 1.43.2 at the same `tbl_rows`. | `test_polars_max_rows_boundaries_match_live_polars` | **PROVEN** | Red 2026-09-10 (`pytest -k max_rows_boundaries`, base tree): `assert _capture_show(frame) == oracle` failed at `max_rows=1` — repark emitted `shape: (12, 1)` with no body rows and no ellipsis where live polars prints `│ 1 │` then `│ … │`. Green after `edge` was dropped for `head_n = (keep + 1) // 2`, `tail_n = keep - head_n` in both the probe and the bridge-peek paths: `show()` at `max_rows` 1, 3 and 5 on a 12-row frame is byte-identical to `str(pl.from_arrow(table))` under `POLARS_FMT_MAX_ROWS` set to the same value — `1, …` / `1, 2, …, 12` / `1, 2, 3, …, 11, 12`. |
| C-002 | D-2: `max_rows = 1` prints the first row and the ellipsis, never an empty body under a non-zero shape line. | `test_polars_max_rows_boundaries_match_live_polars` (the `max_rows=1` arm) | **PROVEN** | Red 2026-09-10 (same run): the `max_rows=1` arm produced an empty body (the failing diff's `- │ 1   │` / `- │ …   │` pair is the polars output repark lacked). Green: `keep=1` gives `head_n=1`, `tail_n=0`, and `use_ellipsis = tail_n > 0 or n >= max_rows` is true because the budget itself bound (`20 >= 1`) — rendering `│ 1 │` then `│ … │` exactly as polars `tbl_rows=1` does. The `n >= max_rows` disjunct keeps `show(1)` under `max_rows=10` a head-only keep-set, so the C8-Q-001 bare-ellipsis pin stays green. |
| C-003 | D-3: the `show(truncate=True)` → `str_len` remap is pinned such that deleting the remap fails the pin. | `test_show_truncate_true_remaps_to_str_len` | **PROVEN** | Green on the base tree by construction (the remap exists — a mutation pin, not a behaviour defect). Mutation evidence 2026-09-10: with the two-line remap deleted, the pin reds — `assert 'abcdefg…' in out` fails against `shape: (1, 1)\n…\n│ abcdefghijklmnopqrst… │` (cells cut at Spark's 20, not `str_len=7`). With the remap restored the pin greens on `abcdefg…`. |
| C-004 | D-4: the `show` docstring states the probe-first count D-5 introduced (probe `max_rows + 1` first, count only when the probe fills). | docstring read; `check_lib_py` exact baseline held | **PROVEN** | `core.py`'s `DataFrame.show` docstring now reads "Polars and DuckDB styles show head and tail rows after probing ``max_rows + 1``, counting only when it fills" (line-neutral at the exact 4485 baseline); the same wording lands in `display.py::_show`'s docstring. `scripts/check_lib_py.py` 2026-09-10: 640 files clean. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

## Green

`pytest python/repark/tests/test_display_polars_default.py
python/repark/tests/test_display_lazy_1.py python/repark/tests/test_display_styles.py -q`
2026-09-10: `79 passed`. One pre-existing pin needed the `use_ellipsis` guard refined
mid-fix: `test_polars_style_honors_n_keep_set` reds if a bare `…` prints for
`show(1)` under `max_rows=10` (C8-Q-001 class), so the ellipsis rule is "tail exists
or the budget bound" rather than "any row shown".

## Red first

## Residue

- `show(n)` where `n < total_rows <= max_rows` still renders the first `n` rows with
  no ellipsis (head-only keep-set, unchanged by this card); `head_n + tail_n ==
  min(n, max_rows)` holds there trivially since the whole keep-set is head.

## Gates

- `pytest … test_display_polars_default.py test_display_lazy_1.py test_display_styles.py -q`
  2026-09-10: `79 passed` (exit 0).
- `python scripts/check_lib_py.py` 2026-09-10: `640 files clean` (exit 0).
- Comment fence 2026-09-10: `git diff --cached -- '*.rs' '*.py' '*.toml' '*.sh'
  '*.yml' | grep -P '^\+\s*(//|#(?! noqa))'` prints nothing.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-3
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: All four clauses walked pin by pin. C-001/C-002 red on the base tree with the empty max_rows=1 body pasted in the cells, green with byte-identical polars output at max_rows 1/3/5; C-003 red under the deliberate remap deletion, green restored; C-004 verified by docstring read plus the exact line baseline.
      artifacts: [python/repark/src/repark/spark/dataframe/display.py, python/repark/src/repark/spark/dataframe/core.py, python/repark/tests/test_display_polars_default.py]
    - id: AT-2
      status: ATTACKED
      evidence: max_rows values 1, 3 and 5 pinned against live polars at matching tbl_rows on a 12-row frame; the odd-value path (head gets the extra row) and the keep==1 no-tail path both exercised. The 12-row frame is taller than every pinned max_rows, so the probe-fills branch is the one under test.
      artifacts: [python/repark/tests/test_display_polars_default.py]
    - id: AT-3
      status: ATTACKED
      evidence: The pre-existing C8-Q-001 keep-set pin was red-caught mid-fix (bare ellipsis under show(1)) and the ellipsis rule was refined to `tail_n > 0 or n >= max_rows` rather than weakened; show(0) still renders an empty body with no ellipsis.
      artifacts: [python/repark/tests/test_display_styles.py, python/repark/src/repark/spark/dataframe/display.py]
    - id: AT-4
      status: ATTACKED
      evidence: max_rows rides the shared alive token, so the pin mutates it through conf.set on a live session and the very next show() re-renders under the new budget; the mutation pin mutates str_len the same way.
      artifacts: [python/repark/tests/test_display_polars_default.py]
    - id: AT-5
      status: N/A
      justification: Display-only change; no secret, catalog, or auth surface. INFO/DEBUG log split unchanged (counts INFO, row data DEBUG-only).
    - id: AT-6
      status: ATTACKED
      evidence: The full display trio (79 tests) plus show goldens, t3-ux-polish, metadata tables, session and cache/persist suites (110 tests) run green with no existing pin weakened; the C8-Q-001 keep-set pin passes unchanged.
      artifacts: [python/repark/tests/test_display_styles.py, python/repark/tests/test_dfcore_4b_show_goldens.py]
    - id: AT-7
      status: ATTACKED
      evidence: The fix removes work, not adds: the probe still fetches max_rows + 1 once and the tail fetch still runs only when a tail exists; dropping the edge cap cannot enlarge a fetch.
      artifacts: [python/repark/src/repark/spark/dataframe/display.py]
    - id: AT-8
      status: ATTACKED
      evidence: Head/tail semantics measured against live polars 1.43.2 at tbl_rows 1 through 5 before implementation; the pin compares against str(pl.from_arrow(...)) on the same Arrow table, so a future polars-format drift reds the oracle rather than the copy.
      artifacts: [python/repark/tests/test_display_polars_default.py]
    - id: AT-9
      status: N/A
      justification: No log or metric surface changed; the INFO row-count breadcrumb and DEBUG-only row-data rule are untouched.
    - id: AT-10
      status: ATTACKED
      evidence: Every behavioural clause carries a red-first pin with the failing output pasted in the clause cell; the docstring clause is verified by reading plus the exact line baseline. The mutation pin's red was produced by deleting the remap, not by hand-editing the expected string.
      artifacts: [task/ledgers/staging/review-fix-3-ledger.md, python/repark/tests/test_display_polars_default.py]
```
