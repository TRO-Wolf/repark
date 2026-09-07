# Charter ledger — DFCORE-6 · eager previews fetch N+1, never `count()`

**Date:** 2026-09-07 · **Branch:** `perf/dfcore-6` · **Base:** `origin/main`
`a0edf802` (PR #420, DFCORE-5) · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `PERF-EAGER-PREVIEW-1` **FIXED**.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The decomposition plan
(`task/roadmap/epic-term/dataframe-core-decomposition-plan-2026-09-07.md`, §4 row DFCORE-6)
opens the second behaviour unit of the slate: eager `__repr__` / `_repr_html_`
fetch `limit(maxNumRows)` then run a full `count()` when the preview is full,
only to decide the "only showing top N rows" footer (the plan's §1 C-005), and
the Spark vertical `show` door does the same. On `mapInArrow`-backed frames the
eager doors are worse: the rows leg materializes the whole bridge and the
footer leg counts it, so a preview runs the UDF over every row twice. This unit
fetches one row past the cap, renders the cap, and reads the footer from the
extra row — no `count()` — and peeks bridged frames with the same bound.

**Not in this unit:** the `mapInArrow` vertical-`show` footer shape (that door
never counted and never footers — adding a footer is a behaviour change, not
this perf fix); honoring any eager cap beyond today's parsing (0/negative/
non-int rules stay exactly as today); styled totals (polars/duckdb keep their
one count — they render tails, a separate contract); `STATUS.md`,
`briefs/next-sequence.md`, `.github/`, `Cargo.lock`, dependency lists,
completed ledgers, any move.

**Environment.** The lane native arrived debug AND stale
(`_native.logical_column_names` absent → `RecursionError` through
`__getattr__` on `frame.columns`, the DFCORE-1/2/3/4a/4b R2-S2 class); the unit
rebuilt it release (`uvx maturin@1.14.1 develop --release`,
`CARGO_BUILD_JOBS=8`) before measuring. All numbers below are release with
`__debug_assertions__` False.

## PROPOSITION LEDGER — DFCORE-6 — 2026-09-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Plain preview doors never call `count()`: repr, HTML, and vertical show go 1 → 0 each on a full 1e6-row preview, cached included; styled shows keep their one count. | `test_preview_doors_never_count`, `test_styled_show_keeps_its_count`, the two retallied 4b tally tests. | **PROVEN** | No-count pin green after, red pre-fix; styled pin green on the fresh native; 4b tally tests pin zero with every golden string unchanged. pins: dfcore-6/C-001 |
| C-002 | Footers preserved exactly: presence at 0/N/N+1 rows on plain and bridged frames, caps 1 and 20, repr and HTML; cap 0/negative/non-int and truncate shapes; singular/plural; every 4b golden byte-identical. | The footer/edge pins in `test_dfcore_6_eager_preview.py` plus the 4b goldens. | **PROVEN** | 3 footer pins green before and after (preservation); M1 (footer flip) reds 3 pins + 4 goldens; goldens identical. pins: dfcore-6/C-002 |
| C-003 | Bridged eager previews peek at most `maxNumRows + 1` UDF rows (bench 2,000,000 → 65,536 computed, pin ≤ 21 yielded); a raising bridge fails every door with the same error. | `test_mapinarrow_preview_bounds_udf_rows`, `test_raising_action_surfaces_identically`. | **PROVEN** | UDF pin red pre-fix (200 > 21 on a 100-row frame), green after; raising pin asserts one head line across all four doors; M3 (peek dropped) reds only the UDF pin. pins: dfcore-6/C-003 |
| C-004 | A non-golden vertical truncation pin closes DFCORE-4b F2: it reds when the truncation decision is wrong, where the goldens stay green. | `test_vertical_footer_without_golden` plus mutation M2. | **PROVEN** | M2 (vertical flip) reds exactly the new pin, 1 failed / 14 passed — the goldens never cover exact-N vertical. pins: dfcore-6/C-004 |
| C-005 | Registry row FIXED with before/after counts and medians; dated baseline plus map row; ledger, staging map, and every touched map in lockstep; `display.py` 322 → 320 under the default ceiling; gates green. | The gates + §3. | **PROVEN** | `PERF-EAGER-PREVIEW-1` FIXED; `docs/perf/eager-preview-baseline.md` filed; battery 126 passed on the release module; facade 5799 passed + 356 skipped. pins: dfcore-6/C-005 |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## 1. Scope audit

The brief's five steps plus the plan's DFCORE-6 row (§4) and unit-specific
pins (§5: 0, N and N+1 rows on a plain frame and on a `mapInArrow`-backed
frame, `maxNumRows` 1, and the HTML door) are the charter. The plan's §1 C-005
is the defect statement; the DFCORE-4b tallies (repr-full 1 / repr-exact 1 /
html-full 1 / vertical-full 1, plus the mia-repr 1) are the pre-fix baseline
this unit must zero. The DFCORE-4b critic report names the two gaps this unit
inherits: F1 is served (the `stacklevel=3` warning is untouched here), F2 (the
vertical-branch `count()` held only by goldens) closes with the C-004 pin. No
clause needed killing: every step reduces to a checkable proposition above.

## 2. Design

Each footer door fetches one row past its cap and decides the footer from
whether the extra row arrived: `limit(max_rows + 1)` then `slice(0, max_rows)`
for the eager doors, `limit(n + 1)` on the vertical `show` door only (the
horizontal door renders no footer, so it fetches no extra row). The vertical
branch passes `total_rows = n + 1` when the extra row arrived and `None`
otherwise, which reproduces the formatter's `total > n and n > 0` rule exactly
at every edge (n = 0 renders empty with no footer; negative n normalizes the
same way). Eager doors on uncached, unpersisted bridges call
`_consume_map_in_arrow_batches(max_output_rows=max_rows + 1)` — the same peek
`show`, `take`, and `isEmpty` already use — instead of materializing the full
bridge through `_plan()` and counting it. The three doors share one
`_use_bridge_peek` predicate (the exact `_show` condition, extracted verbatim),
so cached, persist-requested, and checkpointing frames keep the engine path in
all three doors. Styled previews are untouched: they need the total for tails.
Reasons live in the touched `map.md` files.

## 3. Before/after numbers

Release module, one session per run, 1e6-row single-column parquet (`id`
int32, 4,571,825 bytes) plus the same frame behind a doubling bridge, eager
eval on at the default cap. Counts via an autospec `count` mock carrying the
real method; UDF rows via a counting bridge; wall cells are five-run medians
with the load beside each run (throwaway `/tmp/dfcore6-bench.py`, run before
on the stashed pre-fix tree and after on the tip).

| frame | door | before counts | after counts | before median | after median |
|---|---|---|---|---|---|
| parquet | repr | 1 | 0 | 0.004 s (load 6.6) | 0.002 s (load 5.5) |
| parquet | HTML | 1 | 0 | 0.004 s (load 6.6) | 0.002 s (load 5.5) |
| parquet | vertical show | 1 | 0 | 0.004 s (load 6.6) | 0.002 s (load 5.5) |
| mapInArrow | repr | 1 + 2,000,000 UDF rows | 0 + 65,536 UDF rows | 0.821 s (load 6.5) | 0.031 s (load 5.5) |
| mapInArrow | HTML | 1 + 2,000,000 UDF rows | 0 + 65,536 UDF rows | 0.845 s (load 6.2) | 0.031 s (load 5.5) |
| mapInArrow | vertical show | 0 + 65,536 UDF rows | 0 + 65,536 UDF rows | 0.031 s (load 6.2) | 0.032 s (load 5.5) |

The loads differ between runs, so no wall ratio is claimed — counts are exact,
wall is recorded. The bridged vertical-show row is the control: that door
already peeked and is untouched, so its column must equal before and after —
and does. Rendered output is byte-identical: the 4b goldens (19 shapes show,
22 eager) pass unchanged on the tip.
pins: dfcore-6/C-001

## 4. Mutation score

3/3 red, each mutant applied, run, and reverted with the restore verified by
md5 against a pristine copy (`6f887aef…` before and after every mutant):

- Pre-fix tree (pins committed ahead of the fix): the no-count pin and the
  UDF-bound pin red (`3 failed, 5 passed`; the third red is the styled pin on
  the stale native, green after the rebuild).
- M1 (eager `has_more` `>` → `>=`, both doors): 7 red — the 3 new footer pins
  plus 4 goldens (`7 failed, 8 passed`).
- M2 (vertical `has_more` `>` → `>=`): exactly the new non-golden pin reds
  (`1 failed, 14 passed`) — the goldens never cover exact-N vertical, which
  is the F2 gap the pin closes.
- M3 (bridge peek forced off in both eager doors): exactly the UDF-bound pin
  reds (`1 failed, 7 passed`) — the peek branch is load-bearing for the bound
  and for nothing else.
  pins: dfcore-6/C-004

## 5. Limitations

- The `mapInArrow` vertical-`show` door keeps its no-footer shape: it never
  counted, so this unit leaves it byte-identical. Showing a footer there is a
  behaviour change for its own unit, not a passenger here.
- UDF computed rows are batch-granular: the bench UDF yields whole engine
  batches, so the peek computes one 65,536-row batch and keeps 21 rows. The
  pin uses a row-at-a-time bridge and proves the peek stops it after
  `maxNumRows + 1` yielded rows; a coarse UDF always computes its whole
  first batch on any peek path (`show` included).
- Raising-bridge messages embed path-dependent traceback frames: the head
  line (`mapInArrow user function raised ValueError: preview boom`) is
  identical across all four doors and pinned so; only the attached frames
  name the calling path, as they already did between `show` and `count`.
- The old `count()`-failure swallow is gone with the `count()`: a footer no
  longer depends on a second full scan, so a failing second scan can no
  longer suppress one. No test ever pinned the swallow.
- The two 4b tally tests and their module docstrings now pin zero instead of
  the pre-slice tallies — the baseline they were recorded to retire. The
  golden strings are untouched.

## 6. Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dfcore-6
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-005 walked one by one against behavior; the verdict table carries the evidence per clause, and every clause is cited from the tests and dataframe maps.
      artifacts: [task/ledgers/staging/dfcore-6-ledger.md, python/repark/tests/test_dfcore_6_eager_preview.py]
    - id: AT-2
      status: ATTACKED
      evidence: Footer presence at 0/N/N+1 on plain, cached, and bridged frames, caps 1/20/0/negative/non-int, truncate, empty frames, unicode goldens, and styled doors all exercised; the battery reports 126 passed on the release module.
      artifacts: [python/repark/tests/test_dfcore_6_eager_preview.py, python/repark/tests/test_dfcore_4b_show_goldens.py, python/repark/tests/test_dfcore_4b_eager_goldens.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal and edge branch (bool n, bad truncate, NOT_INT/NOT_BOOL, eager conf spellings, empty/zero caps, raising bridges on all four doors with one head line) pinned; error classes unchanged.
      artifacts: [python/repark/tests/test_dfcore_6_eager_preview.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state, no ordering change, no async spawn, no lock; one bounded fetch replaces a bounded fetch plus a full count.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no trust-boundary crossing; HTML escaping and table structure unchanged, XSS goldens green.
    - id: AT-6
      status: ATTACKED
      evidence: All three doors keep signatures, footer text, plural rules, and cap parsing; every 4b golden string byte-identical; styled totals unchanged; the mia vertical no-footer shape disclosed in section 5.
      artifacts: [python/repark/tests/test_dfcore_6_eager_preview.py, python/repark/src/repark/spark/dataframe/display.py]
    - id: AT-7
      status: ATTACKED
      evidence: The unit is the AT-7 fix: counts 1 to 0 per plain door, bridged UDF rows 2,000,000 to 65,536 computed (21 kept), before/after medians recorded with loads, values identical; the pins carry no wall gate by charter.
      artifacts: [docs/perf/eager-preview-baseline.md, python/repark/tests/test_dfcore_6_eager_preview.py]
    - id: AT-8
      status: ATTACKED
      evidence: Facade-only change in one leaf module; ceilings and maps current; display.py 322 to 320 with no new row; the battery reports 126 passed and the facade suite 5799 passed + 356 skipped.
      artifacts: [python/repark/src/repark/spark/dataframe/display.py, python/repark/tests/test_dfcore_6_eager_preview.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; show INFO/DEBUG split unchanged, and shown-row counts still log from the rendered table.
    - id: AT-10
      status: ATTACKED
      evidence: Mutation score 3/3 with each mutant verified present and each restore verified by md5; the pre-fix red run proves the chartered pins fail without the fix.
      artifacts: [task/ledgers/staging/dfcore-6-ledger.md]
```
