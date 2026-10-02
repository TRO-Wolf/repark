# NVL-TYPE-COERCION-2 — one pre-coercion, mode-aware rewrite for the nvl family

**Filed: 2026-10-02 from the #888 park** (owner "Okay park it", 2026-10-01;
sources: the #888 park note (<https://github.com/TRO-Wolf/repark/pull/888#issuecomment-5933337339>),
`/tmp/oc-worker/direct/wo/review-888-fable/README.md`,
`/tmp/oc-worker/direct/wo/review-888-fable/exp-rule-before-coercion.diff`,
`/tmp/oc-worker/direct/wo/review-888-fable/p12-exp.json`, and
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md` lines 93–101;
all verified present 2026-10-02. PR #888 stays a draft at `be5193c3`
(`fix/nvl-type-coercion-1`); the round-10 fold head `54bc9ac2` stays parked on
`wip/fix/nvl-type-coercion-1-2026-10-01`. The PR's comments were not re-read in
this lane). **Severity:** S1 in default mode (round-11 finding VN12-2).
**Status:** parked design unit. **Target:** mid-term, design first — **an Opus
design sketch comes before any code.**

## The divergence it records

Spark types `nvl` / `ifnull` / `nvl2` / `nullif` per ANSI mode (`nvl('a', 5)`
is BIGINT with ANSI on, STRING with ANSI off). RePark's UDFs cannot see the
flag, and `SparkNvlFamilyRewrite` runs after `type_coercion`, so a parent
coerces against a declared type the rewrite then changes. Every fold round
from VN4 to VN12 layered a heuristic on this fault instead of removing it;
round 11 found the fold made the default mode worse (VN12-2, S1: 61
base==Spark cells regress under ANSI on; 141 PR pins fail at `54bc9ac2`).

## Repro and measured answers

A measured experiment (an uncommitted 15-line edit moving
`SparkNvlFamilyRewrite` to immediately before `type_coercion`, saved as
`exp-rule-before-coercion.diff`; the lane was reverted to a clean `54bc9ac2`
afterwards) was run against the fold head `54bc9ac2`, base `02c2f1be` and the
round-11 Spark data. From `review-888-fable/README.md`:

| | `54bc9ac2` | experiment |
|---|---|---|
| p12 (172 nested cells) cap-b | 31 | 15 |
| p12 head == Spark | 76 | 92 (base 98) |
| p12 ANSI-off cells moved | – | 0 |
| p12 cells that were == Spark and now are not | – | 1 (`greatest(nvl(ndec, dec), s15)` F.expr: Spark refuses, head answers) |
| nvl pin files (9) failing | 141 | 170 (19 fixed, 48 new) |
| `tests::ltz_store::stacked_minus_under_nvl_probes_and_stores` | red | red |

The 48 new pin failures share one cause: the rewrite now runs before the
Spark TIMESTAMP literal gets its session zone, so `nvl(TIMESTAMP, ...)` types
`timestamp_ntz`. The review calls this an ordering detail (literal typing
must precede the rewrite), not a flaw in the direction. The experiment's
per-cell answers are recorded in `p12-exp.json`.

## Scope

The design direction, per the memory: one pre-coercion, mode-aware rewrite,
and delete the base route, `core_declared`, the fexpr markers, the F.expr
`type_coercion` strip, and the ceiling mirror evaluator. The 15 remaining
cap-b cells are the OUTER function's coercion and belong to
COALESCE-ANSI-WIDEN-1, not to this unit.

## Out of scope

- COALESCE-ANSI-WIDEN-1 (bare `coalesce`/`array` widening; its own card).
- Any further fold on `54bc9ac2` (owner ruling: the round-10 fold was the
  last; "Do not resume Muse folds on #888").
- ANSI-off behaviour: the experiment moved 0 ANSI-off cells, and the unit
  must keep it that way.

## Clauses (draft)

- **C-001** The nvl family binds Spark's per-mode type on both doors.
- **C-002** The 61 VN12-2 regressed cells answer Spark again.
- **C-003** The listed machinery is deleted, not bypassed.
- **C-004** Zero ANSI-off movement against the round-11 data.

## Pointers

- Up: [map.md](map.md) · Sibling card:
  [coalesce-ansi-widen-1-card-2026-10-02.md](coalesce-ansi-widen-1-card-2026-10-02.md)
  · Design direction (evidence location):
  the #888 park note (<https://github.com/TRO-Wolf/repark/pull/888#issuecomment-5933337339>)
  · Experiment (evidence location):
  `/tmp/oc-worker/direct/wo/review-888-fable/README.md` with
  `exp-rule-before-coercion.diff` and `p12-exp.json`
