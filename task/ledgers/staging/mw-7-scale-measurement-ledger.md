# MW-7 — scale measurement (measure-only)

**Date:** 2026-08-23 · **Branch:** `feat/mw6-wave` · **Base:** `0ac7553` (MW-6's departure
commit on this branch) · **Charter:** owner, 2026-08-23 (slate:
[../../../briefs/next-sequence.md](../../../briefs/next-sequence.md) "MW-7"; evidence:
[../../roadmap/mid-term/roadmap-intake-2026-08-23.md](../../roadmap/mid-term/roadmap-intake-2026-08-23.md)
row MW-7) · **Fork pin:** `5e7b2e4` (RP-1)

**Retires:** moved to `completed/` in this departure commit.

Measure-only. Nothing under `crates/` changed. The unit adds a driver
([../../../python/repark-parity/bench/mw7/map.md](../../../python/repark-parity/bench/mw7/map.md)),
a CI pin on that driver's machinery, and the numbers below. The charter's two questions —
**is MW-9 urgent** and **what are MW-8's defaults** — are answered in §5 and §6 from the
numbers, not from the shape of the code.

`PROVEN` clauses are the machinery claims a gate can hold. The 1e7 numbers are not clauses:
one host's wall clock is not a proposition. They are dated MEASUREMENTS in §4.

## 1. What was run, and the substitution the feasibility protocol forced

**The charter said 1e7 rows x 100 MERGEs. This unit ran 1e7 rows x 50 MERGEs.** The row
count is the charter's; the merge count is one rung down the charter's own ladder. Here is
the arithmetic that took it there, in the order it was measured.

**Step 1 — the mandated calibration (1e6 x 10, both legs).** 4.5 min wall, peak RSS
3,261 MiB. MOR merges 1.5 1.6 1.6 1.6 1.6 1.7 2.3 2.3 2.3 3.0 s; COW merges 3.0 2.7 2.7 4.1
3.7 2.7 4.7 4.0 3.4 3.6 s; CTAS 9.0 s / 9.3 s.

**Step 2 — the naive projection, and why it was thrown away.** The driver's `--project-to`
models merge cost as quadratic in the row ratio (the table grows AND the touched slice grows)
and linear in the merge count. From the calibration that gives **15.62 h** for 1e7 x 100 —
over budget by 4x, and the answer would have been "cut to 30 merges". It is wrong. A hash
join's probe side grows with the table and its build side with the source; the product does
not. **A projection is a measurement or it is a guess**, so the law was measured at the row
count the charter fixed.

**Step 3 — the scaling probes at 1e7.**

| Probe | Merge wall seconds, in order | Reads as |
|---|---|---|
| 1e7 x 3, both legs (9:28 wall, peak RSS 3,936 MiB) | MOR 11.2 15.6 18.4 · COW 30.1 52.7 74.0 | COW +22 s per merge — a straight line, projecting **31 h** at 100 merges |
| 1e7 x 6, COW only (10.2 min) | 38.8 64.2 74.1 90.9 91.2 90.4 | not a line: a ramp into a **plateau at ~91 s** |
| 1e7 x 10, MOR only (6.3 min) | 11.5 11.6 16.3 20.0 22.2 22.3 22.3 22.4 22.6 23.9 | a ramp into a **plateau at ~23 s** |

Three points could not tell a ramp from a line, and the line said 31 h. Six points said 2.5 h.
Both legs reach a steady state because each MERGE does a bounded amount of work: COW rewrites
essentially the whole table every time (the warehouse grew ~185 MB per merge, and the table
IS ~185 MB), and MOR rewrites the 2 % it touches plus the delete files for the partitions it
touches.

**Step 4 — the projection that decided the run.**

| Term | 1e7 x 100 | 1e7 x 50 |
|---|---|---|
| MOR merges | 195 s measured (1–10) + 90 x 26 s = 2,535 s = **0.70 h** | 195 s + 40 x 25 s = 1,195 s = **0.33 h** |
| COW merges | 450 s measured (1–6) + 94 x 91 s = 9,004 s = **2.50 h** | 450 s + 44 x 91 s = 4,454 s = **1.24 h** |
| CTAS, both legs | 2 x 88 s = 176 s = 0.05 h | 0.05 h |
| Checkpoints (per 10 merges, both legs) | 12 x 2 x ~45 s = 1,080 s = 0.30 h | 7 x 2 x ~45 s = 630 s = 0.18 h |
| Maintenance, both legs | ~600 s = 0.17 h | ~400 s = 0.11 h |
| **TOTAL** | **3.72 h** | **1.91 h** |

The budget is "~4 hours for **everything**". The calibration and the two scaling probes had
already spent **0.75 h** of it, so 1e7 x 100 lands the unit at **~4.5 h** — over. One rung
down the ladder, 1e7 x 50 lands at **~2.7 h including what was already spent**, with margin
for the +-25 % that a plateau extrapolated from ten points deserves. **The substitution is
1e7 x 50 and this paragraph is the reason.**

Two things the substitution does not cost, and one it does:

* 50 x 200,000 = 10,000,000 exactly, so the merge windows tile the id space once with no
  wrap. Every row is updated exactly once across the run.
* Five per-10-merge rows plus the baseline is enough to read a trend and fit a rate.
* It does cost the top half of the curve. Where a claim below depends on extrapolating past
  merge 50, it says so.

**The knobs the charter did not fix, and why these values.** 8 identity partitions on
`part = id % 8`; `write.target-file-size-bytes` = 4 MiB; 7 timed repetitions after 1 untimed
warm-up; checkpoints every 10 merges. The target file size is load-bearing: at the engine
default a 185 MB table writes one data file per partition, and one data file per partition
gives delete-file layout nothing to attach to. At 4 MiB each partition holds ~12 data files,
which is the file COUNT a production partition has even though the bytes are smaller.

## 2. What the driver measures, and what it found on the way

Per checkpoint: data and delete file counts and bytes, delete records, manifest count, the
CURRENT snapshot's manifest-list size on disk, `COUNT(*)`, and three fixed scans at p50/p99
over 7 repetitions. Then the five-procedure maintenance sequence with a census after EVERY
step, then the same scans again. Peak RSS from `resource.getrusage`, cross-checked against
`/usr/bin/time -v` on every run.

The three probes and why each exists are in
[../../../python/repark-parity/bench/mw7/map.md](../../../python/repark-parity/bench/mw7/map.md).
The one that decides MW-9 is `predicate_point`: a 2,000-id window, which Iceberg can prune to
a few data files, but which `partition`-granularity deletes force to consider every delete
file in the partitions it touches.

Three things the charter did not have, all found by measuring:

1. **A 1e7-row table is not automatically a big table.** The first seed was `id`, `id % 8`
   and two derived counters. It compressed to 15 MB — the whole 1e7 rows — and wrote one data
   file per partition. The seed now hashes `id` into two doubles, which do not compress:
   185 MB and ~12 data files per partition. A measurement whose fixture collapses under
   compression measures the compressor.
2. **The merge-0 baseline was not comparable to the later checkpoints.** It is the only
   checkpoint whose files have never been read, and at 1e6 its `COUNT(*)` p50 ran 2.8x the
   merge-6 figure — cache, pointing the wrong way, straight through the middle of every
   ratio the unit exists to report. Every checkpoint now takes one untimed warm-up pass.
3. **`SUM` over a float column is not stable across compaction.** `SUM(value)` moved by one
   ULP across `rewrite_data_files` (`3679734.185675822` -> `...823`). Compaction re-groups
   rows and float addition is order-dependent — correct behaviour, named in
   [../../../docs/testing.md](../../../docs/testing.md) under "float aggregation across
   partitions". The probes sum an integer column, so the before/after identity check in
   C-007 is exact rather than approximately true.

## 3. PROPOSITION LEDGER — MW-7 — 2026-08-23

Every clause below is a claim about the DRIVER, pinned by
`python/repark/tests/test_mw7_scale_smoke.py` at gate scale. A measurement is only worth
reading if the thing that produced it is checked, and this is what checks it.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The census counts what the table's metadata tables actually hold: data / delete file counts and bytes by `content`, `manifests`, `snapshots`, and the CURRENT snapshot's manifest-list size read off disk. | Rebuild a merge-on-read table by hand, count every field independently through SQL and `Path.stat`, require equality on all of them. | PROVEN | `test_census_matches_the_metadata_tables`. Provocation P1: dropping the `content` test makes `data_files` count the delete files too and the pin reds. |
| C-002 | On the merge-on-read leg, position-delete files grow by exactly `partitions` per MERGE — one per `(spec, partition)` per commit, which is Iceberg `partition` granularity (registry `MOR-2`) — and `COUNT(*)` never moves. | Assert `delete_files == merges x partitions` at every checkpoint and `row_count == rows`. | PROVEN | `test_delete_files_grow_one_per_partition_per_merge`; delete records equal `merges x rows_per_merge`. Provocation P2: freezing `generation` in the merge source makes the MERGE a no-op update and the pin reds. |
| C-003 | The copy-on-write leg writes ZERO delete files across the same MERGEs, so MOR-minus-COW on one predicate is the delete-read cost and nothing else. | Assert every COW checkpoint and the post-maintenance checkpoint report zero delete files, and that the leg still rewrites data files. | PROVEN | `test_copy_on_write_leg_is_a_zero_delete_control`. |
| C-004 | `rewrite_position_delete_files` folds the accumulated delete files to one per partition, and `rewrite_data_files` reduces the data-file count; both are visible in the per-step census the driver records. | Compare the last checkpoint's census with the census after each maintenance step. | PROVEN | `test_compaction_reclaims_delete_files_and_data_files`. |
| C-005 | `rewrite_manifests` reduces the manifest count on BOTH legs, and its result is Spark's two columns. | Assert the manifest count after the step is below the count after `rewrite_data_files`, on each leg, and check the result column names. | PROVEN | `test_rewrite_manifests_drops_the_manifest_count`. Rests on MW-6's wiring; this pin only holds that the driver observes the drop. |
| C-006 | The sequence the driver runs is the charter's five procedures in the charter's order, with `remove_orphan_files` LAST, carrying no `dry_run` argument (the engine's default is true — `ORPHAN-2`) and an `older_than` clear of Spark's 24-hour floor. | Assert the recorded procedure list and the orphan SQL text. | PROVEN | `test_maintenance_is_the_charters_sequence`. Provocation P3: moving `remove_orphan_files` to the front reds it. |
| C-007 | Every timing keeps its raw samples, reports `min <= p50 <= p99 <= max`, carries the rows the timed query returned, and is measured after at least one untimed warm-up pass so checkpoints are comparable to each other. The recorded answer does not move across the maintenance sequence. | Assert the ordering, the sample count, `warmups >= 1`, a non-empty answer, and equality of the last checkpoint's answer with the post-maintenance answer, per leg per scan. | PROVEN | `test_timings_carry_their_answer_and_are_ordered`. Provocation P4: reporting `p99` from the minimum sample reds it. The identity half caught a real effect — see §2. |
| C-008 | The three probes keep byte-identical SQL at every checkpoint and after maintenance, so a checkpoint-to-checkpoint ratio compares like with like. | Assert the recorded `sql` per label equals `scan_specs`' output at every point. | PROVEN | `test_scan_battery_is_fixed_across_checkpoints`. Provocation P5: varying the probe partition per call reds it. |
| C-009 | Peak RSS is reported as a process-wide monotone high-water mark, so the figure covers every leg the run executed. | Assert the per-leg peaks are non-decreasing and the run peak is at least the maximum of them. | PROVEN | `test_peak_rss_is_a_monotone_high_water_mark`; cross-checked against `/usr/bin/time -v` "Maximum resident set size" on every run in §4. |
| C-010 | The generator is deterministic and typed: the same arguments rebuild byte-identical seed and merge frames with `id int64` / `part int32` / `value float64`, and every mutable column moves with the merge generation. | Build each frame twice and compare; compare two generations. | PROVEN | `test_generated_frames_are_deterministic`. This is what makes "generators checked in, data never committed" (PROJECT.md) true rather than aspirational. |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 10/10.

```yaml
KILLED_ASSUMPTIONS:
  - "MERGE cost grows with the table size squared, so the calibration projects by rows^2": REMOVED (measured: both legs PLATEAU. MOR settles at ~23 s/merge after ~5 merges, COW at ~91 s after ~4. The rows^2 model projected 15.6 h for 1e7x100 where the measured law projects 3.7 h — the naive model was wrong by 4x in the direction that would have cancelled the unit)
  - "Three points are enough to extrapolate a merge-cost curve": REMOVED (COW's first three merges at 1e7 read as a straight line of +22 s/merge and projected 31 h at 100 merges; six points showed a ramp into a plateau and projected 2.5 h)
  - "A partitioned 1e7-row table is a big table": REMOVED (the first seed compressed to 15 MB because every column was a counter or a modulo. It wrote ONE data file per partition, which leaves delete-file layout nothing to attach to. The seed now hashes id into two doubles: 185 MB, ~12 data files per partition)
  - "The merge-0 baseline is comparable to the later checkpoints": REMOVED (it was the only checkpoint whose files had never been read, and its COUNT(*) p50 ran 2.8x the merge-6 figure at 1e6 — a page-cache artefact pointing the wrong way. Every checkpoint now takes an untimed warm-up pass)
  - "A float SUM is a safe identity probe across compaction": REMOVED (SUM(value) moved by one ULP across rewrite_data_files, because compaction re-groups rows and float addition is order-dependent. Correct engine behaviour; the probe now sums an integer column)
CLARIFYING_QUESTIONS:
  - "The charter fixed 1e7 x 100. The measured projection put that at 3.72 h of RUN time on top of the 0.75 h the calibration and the two scaling probes had already spent, i.e. ~4.5 h against a ~4 h budget for everything. The charter's ladder was taken one rung: 1e7 x 50, projected 1.91 h. The arithmetic is section 1."
  - "No COVERAGE_ATTESTATION is filed here, per the charter — the Critic files it. `make check-ledger-grammar` reds on exactly that finding until they do, because every clause above is PROVEN."
```
