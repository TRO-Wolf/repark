# Run 20a report — 2026-09-17 morning follow-on: Iceberg read path, DML planner and writer paths, conflict detection

**Run:** 20a (unit `morning-20a`, lane prefix `ja-`), the follow-on of run 19a. **Window:** 05:07–15:00 EDT.
**Orchestrator:** claude-opus-5. **Actor tier:** Muse Spark 1.3 contributor on every round (no Opus actor
sessions). **Reviewer tier:** Grok 4.6. **Rating:** `/tmp/oc-worker/ice-rating/report.md` (2026-09-16), §3, §4, §7.

## 1. Starting point (reconstructed)

Run 19a took SIGTERM at 00:38 with no report. Its state, rebuilt from the clones, the PR list and the
salvage patch:

| Unit | State found at 05:07 |
|---|---|
| ICE-PROMOTE-READ-1 (V2-10c, V2-06b, V3-11, V3-14) | Fork PR #285 was open and green on the old base; Grok logic critic NEEDS_REMEDIATION (L-01 inspect tables, P2); rust-perf PASS. The RePark branch existed only in `/tmp/ia-build` (never pushed). |
| ICE-EVO-DML-1 (V2-10e) | RePark fix committed; registry rows, one Rust pin and ledger edits uncommitted (the salvage patch equalled the worktree). Not pushed. |
| ICE-DYN-OVERWRITE-1, ICE-OCC-SCOPED-1, ICE-NESTED-EVO-1, V3-MULTIARG-1 | Not started (19a's draft briefs `unit3.md`…`unit6.md`). |

The point-2(c) list named `fix/ice-evo-dml-1` as pushed; it was not — the local clone was the only copy.

## 2. Residual matrix — before and after

Grades are the rating's own classes. "After" is measured on the unit's head with the pins named in
its ledger; rows whose PR had not merged when the run closed say so.

| Rating row | Before (2026-09-16) | After (2026-09-17) | Where |
|---|---|---|---|
| V2-10c, V2-06b, V3-11, V3-14 (promotion) | MISSING — silent row loss, duplicate MERGE inserts, refused single-era DML | **FIXED** — 128 recorded-oracle cases, both doors, v2 and v3, live tier green; fork half merged | fork #285 (merged `96fc9f1f`), RePark #673 |
| V2-10d (inspect tables after an identity-source promotion) | not measured by the rating; found by the run-19a critic | **FIXED** — `partitions`/`files`/`entries` answer Long values, cross-era values group into one row | fork #285 |
| V2-10e (DML after ADD / RENAME COLUMN, 24 of 24 shapes) | MISSING — every MERGE/UPDATE/DELETE refuses; a name swap wrote the other field's values | **FIXED** — 393 cases pass, including all 33 plain-UPDATE cells; the swap arm is Spark-equal | fork #289 (merged `f7ecd855`), RePark branch `fix/ice-evo-dml-1` (PR pending the pin bump) |
| V2-24b (`partitionOverwriteMode=dynamic`) | MISSING — silent whole-table replace | **FIXED on the branch** — routed in Rust, Spark-equal on every measured cell incl. empty source, unpartitioned, evolved spec | branch `fix/ice-dyn-overwrite-1` (no PR; one review round outstanding) |
| V2-20a / DML-5 (serializable MERGE aborts on any concurrent commit) | MISSING — 1 of 8 disjoint MERGEs commit | **FIXED fork-side** — partition projection before metrics, retry-on-rebase; 23 fault-injected pins | fork #291 (green, queued) — the RePark half (pass each DML target filter) is NOT written |
| V2-10d (nested struct child added → Spark table unreadable) | MISSING | **fork half fixed, unreviewed** — NULL-fill by field id through struct/list/map | fork #292 (draft) — RePark half and the nested DDL row not written |
| V3-05 (multi-argument transforms) | MISSING (loud, unregistered) | unchanged — not opened (ruling Q-20a-7) | card below |

Nothing in this run was closed by a DECLARED refusal; every closed row is Spark-equal against a
recorded oracle.

## 3. Pull requests

| PR | Repo | Unit | Actor | Rounds | Reviews | State |
|---|---|---|---|---|---|---|
| #285 | fork | F-PROMOTE-READ-1 (+ critic L-01) | Opus (19a) + Muse (20a) | 1 + 1 | 19a logic NEEDS_REMEDIATION → 20a verify PASS; 19a rust-perf PASS | **MERGED** `96fc9f1f` |
| #289 | fork | F-EVO-SCAN-1 | Muse | 2 | logic NEEDS_REMEDIATION (L-001 P1) → verify PASS; rust-perf PASS | **MERGED** `f7ecd855` |
| #291 | fork | F-OCC-SCOPED-1 | Muse | 2 | logic + rust-perf NEEDS_REMEDIATION → verify PASS | green, queued |
| #292 | fork | F-NESTED-EVO-1 | Muse | 1 | none (clock) | draft |
| #673 | RePark | ICE-PROMOTE-READ-1 | Opus (19a) + Muse (20a) | 2 | as #285 | green, queued |
| `fix/ice-evo-dml-1` | RePark | ICE-EVO-DML-1 | Opus (19a) + Muse (20a) | 2 | pending | pushed; PR waits on the pin bump to `f7ecd855` (20c #671) |
| `fix/ice-dyn-overwrite-1` | RePark | ICE-DYN-OVERWRITE-1 | Muse | 2 | logic NEEDS_REMEDIATION (P1) remediated; rust-perf + py-perf PASS | pushed; no PR |
| `fix/nested-evo-1` | fork | F-NESTED-EVO-1 | Muse | 1 | none | see #292 |

Pin bumps: 20c's #667 (`96fc9f1f`, carries #285) merged; #671 (`4151b488`) carries #289 and is 20c's.
20a opened no bump PR (Q-20a-2).

## 4. Rulings

- **Q-20a-1** — ICE-EVO-DML-1's 33 red plain-UPDATE cells are fixed fork-side (F-EVO-SCAN-1), not
  narrowed out of the unit: V2-10e names UPDATE, and on the base the swap-UPDATE arm committed
  swapped values silently. Measured right: all 33 pass on `f7ecd855`.
- **Q-20a-2** — the pin bump stayed with 20c, which already had a warm clone; 20a asked for the
  target rev instead of opening a second bump PR.
- **Q-20a-3** — run 19a's clones `/tmp/ia-build` and `/tmp/ia-fork` were resumed rather than rebuilt;
  `/tmp/ja-forksrc` (a source-only worktree) carried the fork override so a fork round and a RePark
  round could run at once without a third build clone.
- **Q-20a-4** — the critic's L-01 (inspect tables) was fixed inside #285 rather than deferred.
- **Q-20a-5** — the inspect-table oracle cells alias their projections to leaf names; the SQL-door
  nested-projection naming (`t$partitions.partition[p]` vs Spark's `p`) is pre-existing and got a
  dated note on registry row EX-COL-2 instead of holding the unit.
- **Q-20a-6** — the in-band `/*RSOW*/` statement-text marker is removed; `saveAsTable`'s static
  overwrite reaches Rust as a typed flag. A user's string literal or comment must not change
  overwrite semantics.
- **Q-20a-7** — ICE-NESTED-EVO-1's RePark half and V3-MULTIARG-1 do not open in this run; cards below.

## 5. Owner questions

1. **The RePark half of scoped conflict detection.** Fork #291 gives RePark the API
   (`conflict_detection_filter` on the row-delta and overwrite actions) but nothing calls it yet, so
   a serializable MERGE still aborts on any concurrent commit. **Recommendation:** first unit of the
   next run, with the 8-disjoint-MERGE probe measured before and after.
2. **`fix/ice-dyn-overwrite-1` has no PR.** The typed-flag remediation landed at the end of the run
   and its gates did not finish. **Recommendation:** re-gate and open it next run; the branch is
   pushed and its ledger is in staging.
3. **The AWS acceptance run** is 20c's; 20a has no evidence to add.
4. **V3-MULTIARG-1 (V3-05)** needs a producer: Spark 4.1.2 DDL cannot create a multi-argument
   transform, so the oracle has to come through the Iceberg Java API. **Recommendation:** treat it as
   a fork spec unit with a hand-built metadata fixture, after the v2 rows are closed.
5. **Nested DDL** (`CREATE TABLE … (s STRUCT<…>)`, `ADD COLUMN s.b`) is still refused with no
   registry row. **Recommendation:** the next run either implements it or files DECLARED rows with
   the measured Spark error shapes.

## 6. STATUS.md lines that need correction

`STATUS.md` is not edited by a unit run; these are for the orchestrating session.

1. The history paragraph still says every §3 row of the v1.0 Iceberg-v3 north star is "✅ or dated
   DECLARED". The 2026-09-16 rating measured silent wrong answers behind several of those rows
   (V2-10c, V2-10e, V2-24b). The line needs a dated qualifier pointing at the rating and at the
   remediation runs.
2. The open-defects section carries no entry for the rating's findings. ICE-EVO-DML-1,
   ICE-DYN-OVERWRITE-1, the RePark half of scoped conflict detection, nested evolution and
   multi-argument transforms are defects with a fix scheduled, so they belong there (one line each)
   until their PRs land.

## 7. Rust-first roll-call

| Change | Language | Note |
|---|---|---|
| Promotion of manifest bounds, partition tuples, page index, inspect tables | Rust (fork) | `spec/promotion.rs`, `inspect/partition_values.rs` |
| DML batch widening after a promotion | Rust (fork + RePark) | `physical_plan/promotion.rs`, `write/conform.rs` |
| Scan binding after ADD / RENAME COLUMN | Rust (fork) | RePark's own re-point was deleted in favour of the fork API |
| Dynamic partition overwrite routing and the conf decision | Rust | `crates/repark-spark/src/insert_overwrite.rs`, `repark-core` carrier |
| `saveAsTable` static overwrite | Rust (typed flag) | the Python facade only forwards, after Q-20a-6 |
| Filter-scoped conflict detection, retry-on-rebase | Rust (fork) | `transaction/snapshot/conflict_filter.rs` |
| Nested projection by field id | Rust (fork) | `arrow/nested_projection.rs` |

No unit added a decision to Python.

## 8. Cards for the next run

- **ICE-OCC-SCOPED-1 (RePark half)** — pass each MERGE/UPDATE/DELETE target filter into
  `conflict_detection_filter`; keep `snapshot` isolation as the opt-down; measure the 8-disjoint-MERGE
  and 16-INSERT probes before and after.
- **ICE-NESTED-EVO-1 (RePark half)** — adopt a Spark-evolved nested table and pin
  `[(1, {a:1, b:None}), (2, {a:2, b:'y'})]` on both doors; then nested CREATE / `ADD COLUMN s.b`
  either implemented or DECLARED with Spark's measured error shape.
- **V3-MULTIARG-1** — `bucket(4, id, name)` is refused at CTAS and ALTER with
  `AnalysisException: … expects (numBuckets, column), got 3 argument(s)` and `source-ids` is unhandled;
  needs a Java-API-built fixture before RePark can be held to anything.
- **ICE-EVO-DML-1 PR** and **ICE-DYN-OVERWRITE-1 PR** — finish the gates and open both.

## 9. Disk

Started at 782 G free, ended at about 400 G with five other lanes building on the same box. 20a kept
two build clones (`/tmp/ia-build`, `/tmp/ia-fork`) plus source-only trees (`/tmp/ja-forksrc`,
`/tmp/ja-rv285`, `/tmp/ja-rv-dyn`), removed `/tmp/ia-build/target/debug` after the gate run, and left
the two build clones in place for the next run (both carry unmerged branches). Run 19a's review
clones `/tmp/ia-r1-fork` and `/tmp/ia-r1-rev` are finished with and can be reclaimed.

## 10. Worker accounting

Muse Spark 1.3 contributor: 9 rounds (ja-evo ×2, ja-fork, ja-promote ×2, ja-fupd ×2, ja-dyn ×2,
ja-occ ×3, ja-nest), every round re-gated by the orchestrator. Grok 4.6: 9 review rounds,
$4.62 total (logic, rust-perf, py-perf and four verification critics). Three of the six reviewed
heads came back NEEDS_REMEDIATION, each with a P1 or P2 that a gate would not have caught: the
inspect-table hole, `use_ref("main")` binding the snapshot schema, and the `/*RSOW*/` statement-text
marker.
