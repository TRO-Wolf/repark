# Unit ledger — OCC-DISJOINT-MERGE-FLAKE-1 · eight concurrent disjoint MERGEs sometimes commit two instead of one

**Date:** 2026-10-18 · **Branch:** `fix/occ-disjoint-merge-flake-1` · **Base:** `02bf6fe5` (origin/main at pickup)
**Model:** claude-opus-5-5 (delegated executor, high effort) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** READING. Step 0 found no product race, so the unit adds no code and no test. Every
clause is discharged by a section of this ledger. The probes ran outside the repository and are
not committed. **risk_tier: standard.**
**Card:** [occ-disjoint-merge-flake-1-card-2026-10-14.md](../../roadmap/mid-term/occ-disjoint-merge-flake-1-card-2026-10-14.md).

**Retires:** this ledger moves to `../completed/` in the unit's last commit, after the owner rules
on C-006.

**Not in this unit:** product code, the fork, the Python storm pin, `STATUS.md`, `Cargo.toml`.

## PROPOSITION LEDGER — OCC-DISJOINT-MERGE-FLAKE-1 — 2026-10-18

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | Two MERGEs that pinned the same base snapshot cannot both commit: the fork validates on every commit attempt against a fresh catalog load, the retry after `CatalogCommitConflicts` included, and the in-memory catalog swaps the pointer under one lock against the base the attempt validated. | docs: task/ledgers/staging/occ-disjoint-merge-flake-1-ledger.md#commit-path | PROVEN | The window between validation and swap is already pinned: `occ_scoped.rs::disjoint_key_merges_on_an_unpartitioned_table_still_conflict_on_true` (cited under ice-occ-scoped-1/C-012) lands the racing write inside the victim's `update_table`. It is green on this base. |
| C-002 | With no injection the storm commits exactly 1 of 8 on this box, on both doors, both isolations and both format versions, idle and under load; every loser carries Spark's message for its isolation level. | docs: task/ledgers/staging/occ-disjoint-merge-flake-1-ledger.md#natural-replay | PROVEN | 870 harness storms and 120 runs of the real test id; 0 double wins. The CI double win did not reproduce. |
| C-003 | A second winner appears exactly when one MERGE loads its base after another MERGE has committed, and that second winner read the first winner's snapshot. | docs: task/ledgers/staging/occ-disjoint-merge-flake-1-ledger.md#forced-late-start | PROVEN | Timestamps on all 75 stall-injected double wins; the v3 deletion-vector rewrite on all 50 v3 double wins. Rows after each double win hold both updates. |
| C-004 | Live Spark 4.1.2 + Iceberg 1.11.0 gives the same split: 1 of 8 on a barrier storm, 2 of 8 when one MERGE starts after the first commit. | docs: task/ledgers/staging/occ-disjoint-merge-flake-1-ledger.md#spark | PROVEN | 120 of 120 barrier runs commit 1; 40 of 40 late-start runs commit 2. |
| C-005 | Both doors run the same `execute_merge`; the DataFrame door differs only in the GIL-bound work between the barrier and the MERGE's table load, and the SQL door double-wins too when a stall is injected there. | docs: task/ledgers/staging/occ-disjoint-merge-flake-1-ledger.md#the-two-doors | PROVEN | Measured start spread: DataFrame door idle median 3.6–5.0 ms, SQL door 1.4–4.2 ms. |
| C-006 | The Python storm pin's expected answer: exact 1 of 8, or a tolerance that admits a serial second winner. | docs: task/ledgers/staging/occ-disjoint-merge-flake-1-ledger.md#questions-for-the-owner | OPEN | Owner ruling. The pin is unchanged until then (no retries, no raised bounds, no loosened pin without a dated ruling). |

VERDICT: 6 clauses, 5 PROVEN, 1 OPEN (owner ruling), 0 REJECTED.

## Commit path

RePark, `crates/repark-iceberg/src/write/` (base `02bf6fe5`):

1. `merge/mod.rs:114` — `execute_merge` loads the target table once.
2. `merge/mod.rs:131`, `commit_target.rs:73-81` — the snapshot id is that table's current snapshot.
3. `merge/mod.rs:140` — the target scan is pinned to that snapshot; `merge/mod.rs:157` builds the
   conflict filter from `ON` (`TRUE` for `t.id = s.id`).
4. `merge/merge_on_read.rs:100`, `merge/snapshot_commit.rs:426-452` — `Transaction::new` on the
   same loaded table, the conflict filter (`:430`), `validate_no_conflicting_delete_files`
   (`:437`), `validate_no_conflicting_data_files` under serializable (`:442`),
   `validate_from_snapshot(snapshot_id)` (`:445`), then `tx.commit` (`:452`).

The fork, `TRO-Wolf/iceberg-rust` at `409ff330`, `crates/iceberg/src/`:

5. `transaction/mod.rs:152-155` — the transaction captures its starting snapshot from the table it
   is given; the `validate_from_snapshot` pin wins over it (`transaction/row_delta.rs:651`).
6. `transaction/mod.rs:373-400` — `commit` retries `do_commit` on retryable errors.
   `CatalogCommitConflicts` is retryable. A validation failure (`DataInvalid`) is not, so it ends
   the loop.
7. `transaction/mod.rs:495-526` — every attempt, the first included, loads the table
   (`:497`), re-bases onto it if stale (`:506-509`) and runs each action's `validate` against it
   (`:520-525`). The row delta's checks (`transaction/row_delta.rs:645-730`) walk every snapshot
   newer than the start.
8. `transaction/mod.rs:567-574` — the commit carries `base_metadata_location`, the location of the
   table the attempt validated against.
9. `catalog/memory/catalog.rs:548-598` — `update_table` compares the stored pointer with that
   base. The last compare (`:587-597`) and the swap (`:598`) run under one lock. A mismatch is a
   retryable `CatalogCommitConflicts` (`:241-260`).

So for two MERGEs on base S0, the first swap wins. The second either refreshes after the swap and
fails validation, or validated before the swap, fails the compare, reloads, validates again from
S0 and fails. A second winner therefore never has base S0.

## Natural replay

Native module: `maturin develop --profile ci` (opt-level 2). Harness: the test module's own
`_session`, `_merge_action` and statements, one barrier, 30 storms per cell; per thread it
records the barrier, MERGE-call and return times, and per storm the snapshot summaries. N = 30 per
cell, 8 cells (`v2`/`v3` × `serializable`/`snapshot` × `dataframe`/`sql`).

| condition | cells | storms | committed 1 of 8 | committed 2+ |
|---|---|---|---|---|
| idle, 26 cores | 8 | 240 | 240 | 0 |
| 4 cores shared with three facade-suite pytest processes | 8 | 240 | 240 | 0 |
| the same plus four busy-loop processes on those 4 cores | 8 | 240 | 240 | 0 |
| 2 of those cores | 4 DataFrame | 120 | 120 | 0 |
| idle, 3 M-object heap ballast, GC callbacks on | 1 DataFrame | 30 | 30 | 0 |
| the real test id, 30 runs per DataFrame parameter, loaded 4 cores | 4 | 120 runs | 120 pass | 0 |

Every loser: `Found conflicting files that can contain records matching TRUE` (serializable) or
`Found new conflicting delete files that can apply to records matching TRUE` (snapshot). Rows
after every storm: `[[100, 1, 100]]`.

Slack before a late thread starts after the commit (first winner's return minus the last
thread's MERGE call), DataFrame door, ms:

| condition | start spread median / max | winner MERGE median | slack min / median |
|---|---|---|---|
| idle | 3.6–5.0 / 8.5–14.2 | 58–65 | 40–51 / 54–62 |
| 4 loaded cores | 13.7–19.4 / 22.9–29.2 | 80–85 | 42–49 / 68–72 |
| 4 loaded cores plus hogs | 19.8–20.9 / 27.2–42.2 | 100–126 | 45–67 / 89–108 |

A double win needs one thread's stall to exceed the slack. CI tests a release wheel, whose MERGE
is shorter, on a 4-core runner with four xdist workers.

## Forced late start

Thread 7 sleeps 300 ms after the barrier, 10 storms per cell:

| cell | committed 2 | committed 1 | late thread among the winners | late MERGE call after the first winner returned |
|---|---|---|---|---|
| 2-serializable-dataframe | 6 | 4 | 6 of 6 | 6 of 6 |
| 2-snapshot-dataframe | 9 | 1 | 9 of 9 | 9 of 9 |
| 3-serializable-dataframe | 10 | 0 | 10 of 10 | 10 of 10 |
| 3-snapshot-dataframe | 10 | 0 | 10 of 10 | 10 of 10 |
| 2-serializable-sql | 10 | 0 | 10 of 10 | 10 of 10 |
| 2-snapshot-sql | 10 | 0 | 10 of 10 | 10 of 10 |
| 3-serializable-sql | 10 | 0 | 10 of 10 | 10 of 10 |
| 3-snapshot-sql | 10 | 0 | 10 of 10 | 10 of 10 |

On v3 the second winner's snapshot summary reads `removed-dvs=1`, `removed-delete-files=1`,
`added-position-deletes=2` in all 40 stall runs and in the 10 GC runs below. It rewrote the first
winner's deletion vector into its own, so its pinned scan read the first winner's snapshot. Every
double win leaves `[[100, 2, 100]]`: both updates, no lost update.

A full `gc.collect()` (about 443 ms with the ballast, GIL held) on thread 0 right after the
barrier: `3-serializable-sql` committed 2 of 8 in 10 of 10 storms; `3-serializable-dataframe`
committed 1 of 8 in 10 of 10.

## Spark

`<pyspark-4.1.2-oracle>` with `iceberg-spark-runtime-4.1_2.13:1.11.0`, Hadoop catalog,
`local[1]`, `spark.ui.enabled=false`, the same table, seed and statements through `spark.sql`,
one fresh table per storm:

| schedule | cell | storms | committed 1 | committed 2 |
|---|---|---|---|---|
| barrier | 2-serializable | 30 | 30 | 0 |
| barrier | 2-snapshot | 30 | 30 | 0 |
| barrier | 3-serializable | 30 | 30 | 0 |
| barrier | 3-snapshot | 30 | 30 | 0 |
| thread 7 starts 3 s late | each of the four | 10 each | 0 | 10 each |

Losers carry `ValidationException: Found conflicting files that can contain records matching
true` or `Found new conflicting delete files that can apply to records matching true`. In every
late-start storm thread 7 is a winner and the rows are `[[100, 2, 100]]`. A Spark MERGE took
0.5–2.4 s here against RePark's 60–125 ms, so Spark's barrier storm absorbs a stall about ten
times longer before its schedule turns serial.

## The two doors

The DataFrame door (`python/repark/src/repark/spark/merge.py:146-164`) registers the source as a
temp view and runs the rendered `MERGE INTO` through `sql_built`. That plans through the same
session SQL path as the SQL door (`crates/repark-python/src/session.rs:93-104`) and reaches the
same `execute_merge`. The SQL door releases the GIL almost at once (`session.rs:171-188`). The
DataFrame door first runs builder code and native calls that hold the GIL:
`create_or_replace_temp_view` (`session.rs:351-362`), `strip_attribute_ids`,
`_native_for_registration` and `_resolve_writer_table`. Its threads reach `load_table` later and
further apart, so a stall in one of them is more likely to outlast the winner's MERGE.

## Questions for the owner

C-006. The pin asserts exactly 1 of 8 and Spark's recording says 1 of 8, but a serial schedule
legitimately commits more on both engines. Options:

1. **Keep the pin as it is.** Cost: the DataFrame cells keep failing CI now and then (four
   sightings, 2026-10-07 to 2026-10-18), and every failure needs a re-run.
2. **Gate the storm deterministically**, as `spark_occ_oracle3.json` gates its MERGE with a UDF.
   Each MERGE's source blocks on a shared barrier that it can reach only after its base load, so
   no MERGE commits before all eight have loaded. Cost: a rewritten test and a new Spark recording
   with the same gate. Risk to check first: eight MERGEs that block inside a UDF can starve the
   runtime's worker threads.
3. **Assert a witnessed tolerance.** Keep 1 of 8 as the expected answer. Accept a further winner
   only when the storm proves it serial: on v3, the extra winner's summary carries `removed-dvs`.
   Also check that the rows hold one update per winner. Cost: a loosened pin needs a dated ruling,
   and v2 has no summary witness, so on v2 this falls back to the row check alone.
4. **Pin the storm through the SQL door only and move the DataFrame cells to a sequential
   two-MERGE check.** Cost: the DataFrame door loses its concurrent cell, and the matrix rule wants
   one row per door.

My lean is option 2. It keeps the exact 1-of-8 answer and removes the timing assumption rather than
widening the answer.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: occ-disjoint-merge-flake-1
  categories:
    - id: AT-1
      status: N/A
      justification: No Spark-visible behaviour changes; the unit adds a ledger and a card section.
    - id: AT-2
      status: ATTACKED
      evidence: The double win was forced on all eight cells and its schedule was read from
        timestamps and the v3 deletion-vector rewrite; live Spark was run on both schedules.
      artifacts: [task/ledgers/staging/occ-disjoint-merge-flake-1-ledger.md]
    - id: AT-3
      status: ATTACKED
      evidence: Both doors, both isolation levels and both format versions were replayed idle and
        under three load shapes.
      artifacts: [task/ledgers/staging/occ-disjoint-merge-flake-1-ledger.md]
    - id: AT-4
      status: ATTACKED
      evidence: The commit path was read from the MERGE's table load to the catalog's pointer swap,
        including the retry after a commit conflict; the window between validation and swap is
        held by an existing green pin.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/occ_scoped.rs]
    - id: AT-5
      status: N/A
      justification: No credentials, network or unsafe code change.
    - id: AT-6
      status: N/A
      justification: No error path changes.
    - id: AT-7
      status: N/A
      justification: No performance-relevant change.
    - id: AT-8
      status: ATTACKED
      evidence: The tree carries this ledger, the card's dated section and the map lines.
      artifacts: [task/ledgers/staging/map.md, task/roadmap/mid-term/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: The card carries a dated section that points here and keeps the card open for the
        owner's ruling.
      artifacts: [task/roadmap/mid-term/occ-disjoint-merge-flake-1-card-2026-10-14.md]
    - id: AT-10
      status: N/A
      justification: No test or CI change; the storm pin is unchanged pending C-006.
  complete: true
```
