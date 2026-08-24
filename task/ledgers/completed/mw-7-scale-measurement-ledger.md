# MW-7 — scale measurement (measure-only)

**Date:** 2026-08-23/24 (chartered and calibrated 08-23; the 1e7 run finished 08-24) ·
**Branch:** `feat/mw6-wave` · **Base:** `0ac7553` (MW-6's departure
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

**Step 1 — the mandated calibration (1e6 x 10, both legs), 2026-08-23.** 4.5 min wall, peak RSS
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
| 1e7 x 3, both legs (9:28 wall, peak RSS 4,032 MiB) | MOR 11.2 15.6 18.4 · COW 30.1 52.7 74.0 | COW +22 s per merge — a straight line, projecting **31 h** at 100 merges |
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

The budget is "~4 hours for **everything**". The calibration and the THREE scaling probes had
already spent **0.509 h** of it, measured (270.5 + 568.2 + 612.9 + 379.1 s), so 1e7 x 100 lands
the unit at **~4.23 h** — over. One rung down the ladder, 1e7 x 50 lands at **~2.42 h including
what was already spent**, with margin for the +-25 % that a plateau extrapolated from ten points
deserves. **The substitution is 1e7 x 50 and this paragraph is the reason.**

*(Corrected 2026-08-24, Critic S3: the first draft said 0.75 h and "two probes". The measured
figure is 0.509 h over three probes. The substitution still stands on the corrected number —
4.23 h is over a ~4 h budget — and it would still stand anywhere in the +-25 % band around the
3.72 h projection.)*

**Projected 1.91 h, actual 2.158 h — +13.0 %.** The plateau was read off the COW probe at merge
6, where it had reached 91 s; it kept climbing to ~113 s. The projection was low by exactly that
amount, which is inside the band it was given, and it is stated here so a reader meets both
numbers in one place instead of computing the gap from two sections.

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

## 3. PROPOSITION LEDGER — MW-7 — 2026-08-24

Every clause below is a claim about the DRIVER, pinned by
`python/repark/tests/test_mw7_scale_smoke.py` at gate scale. A measurement is only worth
reading if the thing that produced it is checked, and this is what checks it.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The census counts what the table's metadata tables actually hold: data / delete file counts and bytes by `content`, `manifests`, `snapshots`, and the CURRENT snapshot's manifest-list size read off disk. | Rebuild a merge-on-read table by hand, count every field independently through SQL and `Path.stat`, require equality on all of them. | PROVEN | `test_census_matches_the_metadata_tables`. Provocation P1: dropping the `content` test makes `data_files` count the delete files too and the pin reds. |
| C-002 | On the merge-on-read leg, position-delete files grow by exactly `partitions` per MERGE — one per `(spec, partition)` per commit, which is Iceberg `partition` granularity (registry `MOR-2`) — and `COUNT(*)` never moves. | Assert `delete_files == merges x partitions` at every checkpoint and `row_count == rows`. | PROVEN | `test_delete_files_grow_one_per_partition_per_merge`; delete records equal `merges x rows_per_merge`. Provocation P2: freezing `generation` in the merge source makes the MERGE a no-op update and the pin reds. |
| C-003 | The copy-on-write leg writes ZERO delete files across the same MERGEs, so MOR-minus-COW on one predicate isolates what merge-on-read costs on read: the delete files PLUS the data-file fan-out merge-on-read leaves behind. | Assert every COW checkpoint and the post-maintenance checkpoint report zero delete files, and that the leg still rewrites data files. | PROVEN | `test_copy_on_write_leg_is_a_zero_delete_control`. |
| C-004 | `rewrite_position_delete_files` folds the accumulated delete files to one per partition, and `rewrite_data_files` reduces the data-file count; both are visible in the per-step census the driver records. | Compare the last checkpoint's census with the census after each maintenance step. | PROVEN | `test_compaction_reclaims_delete_files_and_data_files`. |
| C-005 | `rewrite_manifests` reduces the manifest count on BOTH legs, and its result is Spark's two columns. | Assert the manifest count after the step is below the count after `rewrite_data_files`, on each leg, and check the result column names. | PROVEN | `test_rewrite_manifests_drops_the_manifest_count`. Rests on MW-6's wiring; this pin only holds that the driver observes the drop. |
| C-006 | The sequence the driver runs is the charter's five procedures in the charter's order, with `remove_orphan_files` LAST, carrying no `dry_run` argument (the engine's default is true — `ORPHAN-2`) and an `older_than` clear of Spark's 24-hour floor. | Assert the recorded procedure list and the orphan SQL text. | PROVEN | `test_maintenance_is_the_charters_sequence`. Provocation P3: moving `remove_orphan_files` to the front reds it. |
| C-007 | Every timing keeps its raw samples, reports `min <= p50 <= p99 <= max`, carries the rows the timed query returned, and is measured after at least one untimed warm-up pass so checkpoints are comparable to each other. The recorded answer does not move across the maintenance sequence. | Assert the ordering, the sample count, `warmups >= 1`, a non-empty answer, and equality of the last checkpoint's answer with the post-maintenance answer, per leg per scan. | PROVEN | `test_timings_carry_their_answer_and_are_ordered`. Provocation P4: reporting `p99` from the minimum sample reds it. The identity half caught a real effect — see §2. |
| C-008 | The three probes keep byte-identical SQL at every checkpoint and after maintenance, so a checkpoint-to-checkpoint ratio compares like with like. | Assert the recorded `sql` per label equals `scan_specs`' output at every point. | PROVEN | `test_scan_battery_is_fixed_across_checkpoints`. Provocation P5: varying the probe partition per call reds it. |
| C-009 | Peak RSS is reported as a process-wide monotone high-water mark, so the figure covers every leg the run executed. | Assert the per-leg peaks are non-decreasing and the run peak is at least the maximum of them. | PROVEN | `test_peak_rss_is_a_monotone_high_water_mark`; cross-checked against `/usr/bin/time -v` "Maximum resident set size" on every run in §4. |
| C-011 | A data file that is correctly sized and whose rows are ALL deleted is never a `rewrite_data_files` candidate: it survives the complete maintenance sequence with its dead rows, `removed_delete_files_count` is 0, and the position-delete file covering it also survives — naming a data file that is still LIVE, not a dangling one. | Build a 2,500-row v2 merge-on-read table as one data file inside Java's bin-pack band; assert the band precondition; MERGE every id; run all five procedures; assert the seeded file is still live, the delete file's references are a subset of the live data files, and the row set is unchanged. | PROVEN | `test_delete_laden_in_band_file_survives_the_runbook`. Mechanism, oracle and remedy: finding F-MW7-1, registry `RDF-1`, fork ask F-16. Provocations P7a/P7b: moving the seeded file OUT of the band makes it a candidate, it is rewritten, and both the precondition and the still-live assertion red. |
| C-010 | The generator is deterministic and typed: the same arguments rebuild byte-identical seed and merge frames with `id int64` / `part int32` / `value float64`, and every mutable column moves with the merge generation. | Build each frame twice and compare; compare two generations. | PROVEN | `test_generated_frames_are_deterministic`. This is what makes "generators checked in, data never committed" (PROJECT.md) true rather than aspirational. |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 11/11.

```yaml
KILLED_ASSUMPTIONS:
  - "MERGE cost grows with the table size squared, so the calibration projects by rows^2": REMOVED (measured: both legs PLATEAU. MOR settles at ~23 s/merge after ~5 merges, COW at ~91 s after ~4. The rows^2 model projected 15.6 h for 1e7x100 where the measured law projects 3.7 h — the naive model was wrong by 4x in the direction that would have cancelled the unit)
  - "Three points are enough to extrapolate a merge-cost curve": REMOVED (COW's first three merges at 1e7 read as a straight line of +22 s/merge and projected 31 h at 100 merges; six points showed a ramp into a plateau and projected 2.5 h)
  - "A partitioned 1e7-row table is a big table": REMOVED (the first seed compressed to 15 MB because every column was a counter or a modulo. It wrote ONE data file per partition, which leaves delete-file layout nothing to attach to. The seed now hashes id into two doubles: 185 MB, ~12 data files per partition)
  - "The merge-0 baseline is comparable to the later checkpoints": REMOVED (it was the only checkpoint whose files had never been read, and its COUNT(*) p50 ran 2.8x the merge-6 figure at 1e6 — a page-cache artefact pointing the wrong way. Every checkpoint now takes an untimed warm-up pass)
  - "The delete files that survive the maintenance sequence are dangling, and the missing remove-dangling-deletes option is why": REMOVED (Critic, 2026-08-24. They are not dangling — they name LIVE data files. Spark ends the same sequence at ZERO delete files with that option OFF, at both write.delete.granularity settings. The real mechanism is the fork DEFERRING Java's tooHighDeleteRatio candidate clause, so a correctly sized 100 %-dead file is never selected for rewrite. One measured number — removed_delete_files_count = 0 — fit three stories and the first draft picked the wrong one without testing it. Now C-011, F-MW7-1, registry RDF-1, fork ask F-16)
  - "The MOR-minus-COW gap is the delete files and nothing else": REMOVED (Critic, 2026-08-24. At merge 50 the merge-on-read leg also carries 16.3x the data files and 1.83x the live bytes of the control, because every MERGE appends rather than rewrites. The gap is delete files PLUS that fan-out, and this unit does not separate them)
  - "A float SUM is a safe identity probe across compaction": REMOVED (SUM(value) moved by one ULP across rewrite_data_files, because compaction re-groups rows and float addition is order-dependent. Correct engine behaviour; the probe now sums an integer column)
CLARIFYING_QUESTIONS:
  - "The charter fixed 1e7 x 100. The measured projection put that at 3.72 h of RUN time on top of the 0.509 h the calibration and three scaling probes had already spent, i.e. ~4.23 h against a ~4 h budget for everything. The charter's ladder was taken one rung: 1e7 x 50, projected 1.91 h, actual 2.158 h (+13.0 %). The arithmetic is section 1."
  - "No COVERAGE_ATTESTATION is filed here, per the charter — the Critic files it. `make check-ledger-grammar` reds on exactly that finding until they do, because every clause above is PROVEN."
```

## 4. MEASUREMENTS — 1e7 rows x 50 MERGEs — measured 2026-08-24

**One host, one run.** Wall clock here is not a CI pin and not an SLA. **Ratios are the
claim**: MOR against the COW control at the same merge count, and each table against itself
at merge 0. The absolutes are recorded so a later run can tell a regression from a faster box.

Run: `run_mw7.py --rows 10000000 --merges 50 --partitions 8 --touch-fraction 0.02
--checkpoint-every 10 --reps 7 --target-file-size-bytes 4194304 --modes mor,cow`.
Started 2026-08-24T01:45:39-04:00. `wall_seconds` **7,768.9** (2:09:29 by
`/usr/bin/time -v`, exit 0). Fork pin `5e7b2e4`; nothing under `crates/` changed.

**Peak RSS: 4,461 MiB** (`resource.getrusage` 4,677,332,992 B; `/usr/bin/time -v` "Maximum
resident set size" 4,567,708 kB). The two agree exactly, which is C-009's cross-check. The
peak belongs to the COW leg — MOR's own peak was 2,970 MiB.

### 4.1 Merge-on-read leg, every 10 merges

| merges | data files | delete files | delete records | manifests | manifest-list B | `COUNT(*)` p50 | partition p50 / p99 | point p50 / p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 96 | 0 | 0 | 1 | 1,718 | 3,049 ms | 423 / 450 ms | 858 / 904 ms |
| 10 | 416 | 80 | 2,000,000 | 21 | 6,522 | 1,795 ms | 424 / 525 ms | 643 / 689 ms |
| 20 | 736 | 160 | 4,000,000 | 41 | 11,307 | 4,146 ms | 952 / 1,000 ms | 1,581 / 1,672 ms |
| 30 | 1,056 | 240 | 6,000,000 | 61 | 16,087 | 6,720 ms | 1,426 / 1,468 ms | 2,350 / 2,395 ms |
| 40 | 1,376 | 320 | 8,000,000 | 81 | 20,889 | 9,817 ms | 1,862 / 1,957 ms | 3,107 / 3,203 ms |
| 50 | 1,696 | 400 | 10,000,000 | 101 | 25,665 | 12,605 ms | 2,341 / 2,354 ms | 3,878 / 3,968 ms |

Everything is exactly linear in the merge count: **+32 data files, +8 delete files
(one per partition — C-002), +200,000 delete records, +2 manifests and ~479 manifest-list
bytes per merge.** The first four are exact at every checkpoint; the manifest-list figure is a
mean — the per-10-merge deltas are 4,804 / 4,785 / 4,780 / 4,802 / 4,776 B, so that file grows
at a near-constant rate rather than an exactly constant one. `COUNT(*)` holds 10,000,000 at every row. Live data bytes 260 MB -> 560 MB;
live delete bytes 0 -> 28.6 MB.

**The merge-10 row reads faster than merge 0 and that is real, not noise.** At merge 0 the
table is 96 data files; at merge 10 it is 416, and the scan gets more parallel tasks out of
them. From merge 20 the delete-file cost swamps that, and every probe climbs linearly.
Because of it, **the honest denominator for a ratio is the COW control at the same merge
count, not this table at merge 0** (§4.3).

MERGE wall seconds, in order: 11 18 18 20 23 25 25 25 25 26 27 28 28 28 28 28 28 28 28 28 28
28 28 28 28 28 29 29 29 29 29 29 29 29 29 29 29 27 27 27 27 27 27 27 27 26 26 20 17 13.
Ramp to a **plateau of ~28 s** by merge 13, then a tail decline as the last windows land in
the untouched original CTAS files. CTAS 87.3 s; 50 merges 1,297.7 s (mean 25.95 s); leg wall
34.0 min.

### 4.2 Copy-on-write leg — the zero-delete control

| merges | data files | delete files | manifests | manifest-list B | `COUNT(*)` p50 | partition p50 / p99 | point p50 / p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 96 | 0 | 1 | 1,719 | 3,017 ms | 432 / 445 ms | 862 / 905 ms |
| 10 | 101 | 0 | 4 | 2,460 | 3,035 ms | 495 / 518 ms | 946 / 960 ms |
| 20 | 99 | 0 | 3 | 2,214 | 3,044 ms | 465 / 485 ms | 950 / 969 ms |
| 30 | 99 | 0 | 4 | 2,451 | 3,040 ms | 464 / 516 ms | 933 / 949 ms |
| 40 | 102 | 0 | 4 | 2,457 | 3,036 ms | 478 / 504 ms | 945 / 962 ms |
| 50 | 104 | 0 | 5 | 2,696 | 3,071 ms | 511 / 516 ms | 929 / 963 ms |

**Flat.** Over 50 MERGEs that rewrite every row in the table exactly once, `COUNT(*)` moves
1.02x, the partition probe 1.18x and the point probe 1.08x. Data files stay near 100 and live
bytes near 307 MB, because copy-on-write compacts as a side effect of rewriting.

**What the control isolates, stated precisely.** It is tempting to call the MOR/COW gap "the
delete files and nothing else", and that is wrong. At merge 50 the merge-on-read table carries
**1,696 data files against the control's 104 (16.3x)** and **560 MB of live data against 306 MB
(1.83x)**, because every MERGE appends the updated rows as new small files instead of rewriting
in place. So the gap is **the delete files PLUS the data-file fan-out merge-on-read leaves
behind** — two costs with the same cause and different remedies (`rewrite_position_delete_files`
for one, `rewrite_data_files` for the other). This unit does not separate them; a unit that wants
the split needs a third leg with the deletes compacted at every checkpoint.

MERGE wall seconds: 31 53 74 79 80 91 98 111 109 114 114 113 114 113 114 113 114 113 114 114
114 115 114 114 114 114 113 113 113 112 112 112 111 113 112 113 113 112 113 114 113 113 113
112 112 113 113 113 113 113 — a ramp to a **plateau of ~113 s**. CTAS 88.6 s; 50 merges
5,363.9 s (mean 107.28 s); leg wall 95.5 min. **COW costs 4.1x MOR per MERGE** (107.3 s vs
25.9 s mean) and pays nothing on read; that is the whole trade, measured.

The warehouse is the other half of it: **14,782 MB on disk for a 342 MB table** before
`expire_snapshots` — **43x** — because every merge's rewritten files stay reachable from the
snapshot that wrote them.

### 4.3 MOR against the COW control — what merge-on-read costs on read

| merges | point p50 MOR | point p50 COW | MOR/COW | partition MOR/COW | `COUNT(*)` MOR/COW | delete records read per row the point probe returns |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 858 ms | 862 ms | 1.00x | 0.98x | 1.01x | 0 |
| 10 | 643 ms | 946 ms | 0.68x | 0.86x | 0.59x | 1,000 |
| 20 | 1,581 ms | 950 ms | **1.67x** | **2.05x** | 1.36x | 2,000 |
| 30 | 2,350 ms | 933 ms | 2.52x | 3.07x | 2.21x | 3,000 |
| 40 | 3,107 ms | 945 ms | 3.29x | 3.89x | 3.23x | 4,000 |
| 50 | 3,878 ms | 929 ms | **4.18x** | **4.58x** | 4.10x | 5,000 |
| after maintenance | 1,945 ms | 961 ms | **2.02x** | **2.45x** | 0.63x | — |

The ratios above are **not** a delete-file cost alone (§4.2): at every row the merge-on-read
leg is also carrying 16.3x the data files and 1.83x the live bytes of the control.

Both legs return byte-identical answers at every checkpoint — the invariant is the **cross-leg
identity**, on two entirely different write paths, not any particular literal. The values move
as merges land; at merge 50 and after maintenance they are `{n: 625669, s: 3128924193}` for the
partition probe and `{n: 2000, s: 9865011}` for the point probe. That agreement is the
correctness control under every timing above.

### 4.4 The maintenance sequence at 50 merges of debt

Merge-on-read leg, census after each step. **Total 142.34 s.**

| step | wall s | result | data files | delete files | manifests | manifest-list B |
|---|---:|---|---:|---:|---:|---:|
| before | — | — | 1,696 | 400 | 101 | 25,665 |
| `rewrite_position_delete_files` | 44.3 | rewrote 400 -> 8; 28,552,042 B -> 37,377,416 B | 1,696 | 8 | 109 | 27,651 |
| `rewrite_data_files` | 92.4 | rewrote 1,646 -> 120 files, 400,210,608 B; `removed_delete_files_count` **0** | 170 | 8 | 67 | 17,693 |
| `rewrite_manifests` | 0.4 | 59 -> 1 | 170 | 8 | 9 | 3,659 |
| `expire_snapshots` | 5.2 | 1,646 data + 400 delete + 917 manifests + 67 manifest lists deleted | 170 | 8 | 9 | 3,659 |
| `remove_orphan_files` | 0.1 | 0 rows | 170 | 8 | 9 | 3,659 |

Copy-on-write leg, **total 21.2 s**: position-delete compaction 0.1 s (nothing to do),
`rewrite_data_files` 15.0 s (31 -> 24 files), `rewrite_manifests` 0.1 s (8 -> 1),
`expire_snapshots` 6.0 s (**4,783 data files deleted**), orphan dry run 0.0 s / 0 rows.
Manifest list 2,696 B -> 1,734 B. Warehouse 14,782 MB -> 342 MB.

**The orphan dry run listing zero files is not evidence of a clean warehouse.** Spark's
24-hour floor (MW-3) applies, and this warehouse is minutes old, so nothing is eligible by
construction. What the row is worth is its cost: **0.1 s** at 1,696 data files and 400 delete
files, so it can run every cycle.

Scans after the full sequence, MOR: `COUNT(*)` **2,064 ms** (0.68x its own merge-0 figure,
0.16x the merge-50 figure), partition **1,034 ms**, point **1,945 ms**. Manifest list
25,665 B -> 3,659 B (**7.0x**), delete files 400 -> 8 (**50x**), data files 1,696 -> 170
(**10x**). Warehouse 592 MB -> 687 MB: compaction added more bytes than expire reclaimed on
this leg, because after 50 merges every row carries the longer merged `name` and the merge
order has destroyed the seed's id clustering. That is fixture shape, recorded not diagnosed.

**And the sequence does not close the gap.** After every procedure has run, the merge-on-read
table still reads at **2.45x** (partition) and **2.02x** (point) the copy-on-write control — and
it is still carrying **1.90x the live bytes** (647 MB against 340 MB) and 1.75x the data files.
So the residual, like the gap during the run, is delete files *and* retained data. Finding
F-MW7-1 below is the mechanism behind both halves.

```yaml
FINDING:
  id: F-MW7-1
  severity: S2
  category: AT-6
  clause: C-004, C-011
  disposition: OPEN
  title: rewrite_data_files never selects a delete-laden data file, so its dead rows and the delete file covering it are retained without bound
  superseded_first_draft: >
    The first draft of this finding (2026-08-23) said the surviving delete files were DANGLING,
    blamed removed_delete_files_count / the remove-dangling-deletes option, and treated the
    residue as a property of merge-on-read at v2. The Critic refuted all three on 2026-08-24 by
    measurement and this text replaces it. The record is kept rather than quietly rewritten,
    because the wrong mechanism is the interesting part: the count that WAS measured (0) fit
    three different stories and the ledger picked the wrong one without testing it.
  refuted: >
    (1) Not dangling — the surviving delete files reference data files that are still LIVE
    (16/16 references live on the Critic's tiling shape; the 2,500-row pin here asserts
    references are a subset of the live set). (2) Not the missing option — Spark ends the SAME
    sequence with ZERO delete files and zero delete records with remove-dangling-deletes OFF
    (jar default false, javap-verified) and removed_delete_files_count still reported 0.
    (3) Not write.delete.granularity — Spark reaches zero at BOTH granularity settings, so this
    is not MOR-2 under another name.
  mechanism: >
    Java's BinPackRewriteFilePlanner has three candidate clauses; the fork at 5e7b2e4 wires two.
    A file is selected when it is outside the size band, or when it carries at least
    delete_file_threshold delete files — and that threshold defaults to usize::MAX
    (DELETE_FILE_THRESHOLD_DEFAULT, crates/iceberg/src/maintenance/rewrite_data_files.rs:177).
    The third clause, tooHighDeleteRatio at DELETE_RATIO_THRESHOLD_DEFAULT = 0.3, is DEFERRED:
    the module doc at :66-67 and :138-140 states "the delete-RATIO candidate clause is not
    exposed (it needs per-file known-deleted-record accounting) … The ratio clause never fires
    here". In Java that clause makes a delete-laden file a candidate regardless of size, the
    rewrite physically drops its deleted rows, and the delete files covering it die in the
    rewrite commit. Here a correctly sized file that is 100 % dead is invisible to compaction,
    and nothing else will ever remove it.
  evidence: >
    Reproduced as a gate-scale pin (C-011): one 68,523 B data file, inside the bin-pack band for
    a 64 KiB target, and one MERGE deleting all 2,500 of its rows. After the COMPLETE maintenance
    sequence the file is still live carrying 2,500 dead rows, one 8,240 B delete file still names
    it, that name is in the live data-file set, and rewrite_data_files reported
    removed_delete_files_count 0 while rewriting 4 other files. At 1e7 x 50 the same shape ended
    the sequence with 8 delete files holding 10,000,000 delete records, the table reading at
    2.45x (partition) and 2.02x (point) the copy-on-write control and holding 1.90x its live
    bytes. The answers are correct at every point, which is why this needs a registry row rather
    than a refusal: nothing goes wrong loudly, it just never gets better.
  consequence_for_mw8: >
    The runbook as MW-8 will document it cannot reclaim delete-laden data files on this engine.
    Cadence bounds how far the scan degrades between passes; it does not bound the retained dead
    rows, which grow without limit until the fork ask lands.
  registry: RDF-1 in docs/spark-sql-iceberg-parity.md (BACKLOG, fork work)
  fork_ask: F-16 in task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md; the pin above is written to go RED when it lands
FINDING:
  id: F-MW7-2
  severity: S3
  category: AT-7
  clause: C-001
  disposition: OPEN
  title: position-delete compaction cuts the file count 50x and grows the delete BYTES 31%
  evidence: >
    rewrite_position_delete_files reported rewritten_bytes_count 28,552,042 and
    added_bytes_count 37,377,416 — 400 files to 8, and 8.8 MB more delete data than it
    started with. Fewer, larger partition-scoped delete files hold a wider spread of
    file_path values than many small ones, so the column compresses worse. The file-count
    win is real and is what the scan cares about; the byte total moving the other way is
    worth disclosing to anyone sizing storage from the file count.
  registry_candidate: disclosure only; no behaviour claim is wrong
```

## 5. The verdict the charter asked for: is MW-9 urgent?

**Yes. The deciding number is the point probe: 858 ms -> 3,878 ms as 50 MERGEs land, a 4.5x
degradation on a predicate that returns 2,000 rows — 0.02 % of the table.** That probe is a
narrow id window Iceberg can prune hard on the data side, and it touches all 8 partitions. This
engine writes one position-delete file per `(spec, partition)` per commit — Iceberg `partition`
granularity, registry `MOR-2` — so at merge 50 the probe must open every delete file in every
partition it touches: **all 400 of them, 10,000,000 delete records, for 2,000 rows returned.**
Under Spark's default `file` granularity a delete file names the one data file it belongs to,
and delete reads prune alongside data reads instead of staying whole. The degradation is linear
and it starts early — against the copy-on-write control the point probe is at 1.67x by merge 20,
on a table where only 40 % of the rows have been rewritten once. F-MW7-1 makes it worse rather
than better: running the entire maintenance runbook still leaves the table at 2.0-2.5x, so
cadence alone cannot buy the operator out of this.

Two things this verdict deliberately does **not** claim. It does not claim the partition probe
as corroboration — that probe reads a whole partition, so its delete reads would not prune much
under either granularity, and its similar ratio is not independent evidence. And it does not
claim to know what `file` granularity would cost **here**: this engine cannot write that layout,
so the counterfactual is unmeasured. **MW-9 must carry its own before/after on these same three
probes** rather than inherit this verdict as proof of its own fix.

## 6. What the numbers set as MW-8's runbook defaults

1. **Cadence: run the sequence every 10 MERGEs. The ceiling is merge 20, which already
   measures 2.05x — a ceiling that tolerates ~2x, not one that holds under it.** Interpolating
   the control-relative curve puts the 2x crossing at **19.6 merges** on the earliest probe
   (partition: 0.86x at merge 10, 2.05x at merge 20); the point probe crosses at ~24
   (1.67x -> 2.52x between 20 and 30). At merge 10 every probe is still at or below the control.
   **Caveat on the cadence-10 figure:** merge 10 reads better than merge 0 partly because 416
   data files parallelise better than 96 (§4.1), so "at or below the control at merge 10"
   carries a confound this unit did not separate. It is a safe recommendation because it errs
   early, not because the confound was ruled out.
   In table-relative terms the 2x line sits at **delete records = 39.2 % of live rows**
   (19.6 x 200,000 / 10,000,000). State the trigger that way for tables whose merges are not
   2 % — **but note the crossing may track the delete-FILE count (partitions x merges = 157 at
   the crossing) rather than the record fraction.** This unit varied only the merge count, so
   the two are collinear in it and it cannot say which drives the cost. MW-8 should prefer the
   file-count trigger if it must pick one, because that is what a scan opens.
2. **Order is the charter's, and the cost says why.** Position-delete compaction ran 44.3 s
   over 400 files and left 8; data compaction then ran 92.4 s reading those 8 instead of 400.
   Reversing the two makes the expensive step read 50x the delete files.
3. **`expire_snapshots` is the step no one may skip.** The copy-on-write warehouse held
   **14,782 MB for a 342 MB table (43x)** until it ran, and it deleted 4,783 data files in
   6.0 s. On the merge-on-read leg it deleted 1,646 data files, 400 delete files, 917
   manifests and 67 manifest lists in 5.2 s.
4. **`rewrite_manifests` and the orphan dry run are free — run both every cycle.** 0.4 s and
   0.1 s at 50 merges of debt. `rewrite_manifests` cut the manifest list **25,665 B ->
   3,659 B (7.0x)**, which every reader pays on every scan. **The orphan step is a lagging net,
   by construction:** its 24-hour floor means a cycle never sees the orphans that the same
   cycle's `expire_snapshots` just created. At a 10-merge cadence it is catching the previous
   day's cycle, not this one. Document it as a safety net with a day of latency, never as
   confirmation that the cycle just run left nothing behind.
5. **Budget the whole sequence at ~2.5 minutes per 1e7-row merge-on-read table with 50
   merges of debt** (142.34 s measured), and 21.19 s for the copy-on-write equivalent. At the
   recommended cadence of 10 merges the debt is a fifth of that.
6. **Tell the operator the write/read trade in numbers.** Merge-on-read MERGE plateaus at
   ~28 s where copy-on-write plateaus at ~113 s (**4.1x cheaper to write**), and
   copy-on-write scans stay flat where merge-on-read reaches 4.2x by merge 50. Merge-on-read
   plus the runbook is the right default; merge-on-read without it is not.
7. **The orphan dry run answering zero rows means nothing on a young warehouse** (24-hour
   floor). Do not document it as a clean bill of health.
8. **State the limit of the runbook honestly.** On this engine the sequence cannot reclaim
   delete-laden data files at all (F-MW7-1 / registry `RDF-1` / fork ask F-16). Cadence bounds
   how far the SCAN degrades between passes; it does not bound the dead rows retained, which
   grow without limit until the fork carries Java's delete-ratio clause. An MW-8 runbook that
   promises a table returns to baseline after a pass would be promising something this engine
   does not do.

## 7. Provocation proofs (pin liveness)

Each mutation was applied to the driver, the smoke pin was watched go RED, and the mutation
was reverted. Nothing below is committed.

| # | Mutation | Pin that reds | Output (verbatim) |
|---|---|---|---|
| P1 | `file_census` counts position deletes as data files | `test_census_matches_the_metadata_tables` (C-001) | `assert 12 == 10` — `FileCensus(data_files=12, …, delete_files=0)` against 10 real data files |
| P2 | every MERGE window confined to one partition (`int_range` strided by `partitions`) | `test_delete_files_grow_one_per_partition_per_merge` (C-002) | `AssertionError: after 3 MERGEs: 3 delete files, expected 6` — `assert 3 == (3 * 2)` |
| P3 | `remove_orphan_files` moved to the FRONT of the sequence | `test_maintenance_is_the_charters_sequence` (C-006) | `At index 0 diff: 'remove_orphan_files' != 'rewrite_position_delete_files'` |
| P4 | `p99_ms` reported from the MINIMUM sample | `test_timings_carry_their_answer_and_are_ordered` (C-007) | `AssertionError: assert 59.81151096057147 <= 50.62916700262576` on `p50 <= p99` |
| P5 | the probe partition changes between calls (`int(time.time()) % partitions`) | `test_scan_battery_is_fixed_across_checkpoints` (C-008) | `assert {'count_star'…} == {'count_star'…}` — `Differing items` on `predicate_partition` |
| P6 | `generation` dropped from every mutable column of the merge source | `test_generated_frames_are_deterministic` (C-010) | `assert not source.equals(overlap)` — `assert not True`; generations 3 and 4 build the same 50 rows, `m_149` in both |

| P7a | C-011's target file size dropped to 8 KiB, putting the seeded file OUT of the bin-pack band | `test_delete_laden_in_band_file_survives_the_runbook` (C-011) | `AssertionError: seeded file 68523 B is outside the bin-pack band for 8192` — `assert 68523 <= (1.8 * 8192)` |
| P7b | the same, with C-011's band precondition removed so the run continues | same | `AssertionError: the 100 %-dead seeded file is still live: it was never a rewrite candidate` — the out-of-band file WAS rewritten |

P7a and P7b are the red-green pair that identifies the mechanism rather than just asserting the
outcome. In band, the 100 %-dead file is kept forever; out of band, the very same file is
rewritten and its delete file goes dangling. Size is the only thing that changed, which is the
candidate filter and nothing else.

C-003, C-005 and C-009 are held by the same battery and were not separately mutated: each
asserts a shape the driver reports rather than a branch it chooses.

## 8. Lockstep

- Driver: [../../../python/repark-parity/bench/mw7/map.md](../../../python/repark-parity/bench/mw7/map.md),
  linked from [../../../python/repark-parity/bench/map.md](../../../python/repark-parity/bench/map.md).
- Pin: `python/repark/tests/test_mw7_scale_smoke.py`, rowed in
  [../../../python/repark/tests/map.md](../../../python/repark/tests/map.md).
- STATUS: a dated MW-7 addendum to the MW scorecard, ratios not absolutes; the sequenced
  remainder becomes MW-8 -> V3-2.
- Slate: MW-7 leaves [../../../briefs/next-sequence.md](../../../briefs/next-sequence.md);
  MW-8 is next and takes its defaults from §6.
- Registry: **`RDF-1`** is written into
  [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) for
  F-MW7-1 (2026-08-24, after the Critic established the mechanism and the Spark oracle).
  F-MW7-2 stays a ledger finding — it is a disclosure about a count, not a behaviour claim.
- Fork: ask **F-16** in
  [../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md),
  with C-011's pin named as the engine pin that flips when it lands.
- Design: a dated errata on
  [../../../docs/design/format-v3-track.md](../../../docs/design/format-v3-track.md) §3b — its
  v2 sentence held for a 9 %-deleted fixture and is not general.
- Scratch: the warehouses and Parquet trees under the run's `--scratch` root were deleted
  after the numbers were read. Nothing generated by this unit is committed.
