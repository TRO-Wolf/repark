# Charter ledger — SCALE-v3 · the MW-7 `10^7 x 50` scale workload on a format-v3 table

**Date:** 2026-09-02 · **Branch:** `feat/scale-v3-mw7` · **Base:** `origin/main` `cda526e` ·
**Model:** claude-opus-5 (medium) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md) ·
**Path:** STANDARD.

**Retired:** moved to `../completed/` in this unit's last commit.

**Why now.** North star §3 row "Scale" is the last ⚠ that needs no fork work: MW-7 measured
`1e7 x 50` on format v2 (2026-08-24), and v1.0 requires the same measurement on v3.

**Not in this unit:** any engine change under `crates/`; a new probe; a third compacting leg;
`.github/`; dependency files.

## PROPOSITION LEDGER — SCALE-v3 — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The driver takes a `format_version` threaded from `run_mw7.py --format-version {2,3}`, default 2 (every prior invocation unchanged); `3` arms `repark.sql.allowCreateFormatVersion3` on the session. On v3 the MoR leg writes file-scoped Puffin deletion vectors and the COW leg keeps `_row_id`; `rewrite_position_delete_files` refuses on live DVs (`B-MOR-3`) and the driver records that one refusal instead of raising — keyed on the procedure name, so any other refusal still aborts — and the other four procedures are still measured. | Seven pins in `test_mw7_scale_smoke.py`, one of them the live oracle at matched layout; mutation N red of M. | **PROVEN** | §1, §2. Citation: `python/repark/tests/map.md`, `python/repark-parity/bench/mw7/map.md`. |
| C-002 | The `1e7 x 50` workload runs on v3 at the v2 knobs (8 partitions, `--touch-fraction 0.02`, checkpoints every 10, 7 reps, 4 MiB target) on a quiet box, both legs, and the v3-vs-v2 ratios are recorded from counts. | The run JSON, the census and timing tables, the ratio table. | **PROVEN** | §3. `/usr/bin/time -v` exit 0, 2:42:36. Citation: `python/repark-parity/bench/mw7/map.md`. |
| C-003 | North star §3 "Scale" carries the dated v3 numbers, with the write-side ratios labelled cross-run and the read-side ones controlled; `docs/design/format-v3-track.md` §5 Step 6, `docs/guide/iceberg-guide.md`'s runbook section (its format version stated before any number, plus the two false v3 sentences this unit refutes) and both `map.md` files are in lockstep; this ledger moves to `completed/` last. | `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction`. | **PROVEN** | §4. Citation: `python/repark-parity/bench/mw7/map.md`. |

## 1. The knob, measured at smoke scale (20,000 rows x 6 MERGEs, 2 partitions, 256 KiB target)

| Leg | Checkpoint | v2 data / delete / delete records | v3 data / delete / delete records |
|---|---:|---|---|
| MoR | 0 | 2 / 0 / 0 | 2 / 0 / 0 |
| MoR | 3 | 8 / 6 / 1,200 | 8 / 2 / 1,200 |
| MoR | 6 | 14 / 12 / 2,400 | 14 / 2 / 2,400 |
| COW | 6 | 4 / 0 / 0 | 4 / 0 / 0 |

**The v3 shape.** Delete files hold at the seeded data-file count (one DV per data file that
carries deletes) where v2 grows `partitions x merges`; the delete RECORDS grow at the same
rate on both. Every v3 delete file is content 1, `PUFFIN`, and names exactly one live data
file through `referenced_data_file`.

**The maintenance divergence, recorded not tuned.** On the v3 MoR leg
`rewrite_position_delete_files` refuses:

> `CALL rewrite_position_delete_files found 2 live Puffin deletion vector(s) on ns.t and will
> not report a partial result … B-MOR-3 stays.`

That is registry row `B-MOR-3`, already dated. The driver records the refusal on the step
(`refusal` field) and runs the remaining four procedures. The capture is keyed on
`procedure == "rewrite_position_delete_files" and format_version >= 3` — the one refusal that
is a measurement. Any other refusal, and every refusal on v2, still aborts the run.

| v3 MoR maintenance step | result | data / delete / delete records after |
|---|---|---|
| `rewrite_position_delete_files` | REFUSED (`B-MOR-3`) | 14 / 2 / 2,400 |
| `rewrite_data_files` | rewrote 12 → 2, `removed_delete_files_count` 0 | 4 / 2 / 2,400 |
| `rewrite_manifests` | 9 → 1 | 4 / 2 / 2,400 |
| `expire_snapshots` | 12 data + 5 position-delete + 31 manifests + 9 manifest lists | 4 / 2 / 2,400 |
| `remove_orphan_files` | 0 rows (24-hour floor) | 4 / 2 / 2,400 |

The two surviving DVs cover the two seeded data files, which are in the bin-pack band and
12 % deleted — below Java's 0.3 delete-ratio clause, so `rewrite_data_files` correctly leaves
them. `COUNT(*)` is 20,000 at every row above.

## 2. The live oracle at matched layout (4,000 rows x 3 MERGEs, 2 partitions, 256 KiB target)

PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `REPARK_PARITY_LIVE=1`.

| Reading | RePark | PySpark 4.1.2 |
|---|---|---|
| delete files `(content, file_format, record_count)` | `[(1, PUFFIN, 120), (1, PUFFIN, 120)]` | `[(1, PUFFIN, 120), (1, PUFFIN, 120)]` |
| data files | 8 | 8 |
| `COUNT(*)` | 4,000 | 4,000 |

Equal on every cell. Pinned as `test_v3_delete_file_layout_matches_live_spark`.

## 3. MEASUREMENTS — 1e7 rows x 50 MERGEs on format v3 — measured 2026-09-02

**One host, two runs eight days apart.** Wall clock is not a CI pin, and a v3-against-v2 ratio
is a CROSS-RUN ratio: the two runs share knobs, not a day. **Counts and the COW-controlled read
ratios are the claim; every write-side ratio below is labelled uncontrolled**, because the
copy-on-write control — the same 50 MERGEs, the same knobs, no delete files — itself moved
1.22x between the runs, and the quiet-box check sampled the box only up to the start.

Run: `run_mw7.py --rows 10000000 --merges 50 --partitions 8 --touch-fraction 0.02
--checkpoint-every 10 --reps 7 --target-file-size-bytes 4194304 --modes mor,cow
--format-version 3`. Started 2026-09-02T13:28:38-04:00, finished 16:11:14.
`wall_seconds` **9,755.6** (2:42:36 by `/usr/bin/time -v`, exit 0). Base `cda526e`; nothing
under `crates/` changed. The run JSON is a scratch artefact and was not committed, as MW-7 did
with its own; **the tables in this section are the evidence** — every ratio published anywhere
in the tree is recomputable from them.

**Quiet-box wait:** 9 busy checks at 5-minute intervals, **2,701 s waited**, quiet at
13:28:38. Scratch peaked at **16 G**; free disk never fell below **75 G**.

**Peak RSS 4,792 MiB** (`resource.getrusage` 4,907,292 kB = `/usr/bin/time -v` "Maximum
resident set size", exactly equal — C-009's cross-check). MoR's own peak was 2,790 MiB;
the run peak belongs to the COW leg. v2 measured 4,461 MiB (1.07x).

### 3.1 Merge-on-read leg (v3), every 10 merges

| merges | data files | delete files | delete records | manifests | manifest-list B | data MB | delete MB | `COUNT(*)` p50 | partition p50 / p99 | point p50 / p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 96 | 0 | 0 | 1 | 1,727 | 259 | 0.0 | 3,027 ms | 438 / 453 ms | 876 / 946 ms |
| 10 | 176 | 80 | 2,000,000 | 16 | 4,116 | 327 | 0.5 | 1,768 ms | 612 / 657 ms | 846 / 877 ms |
| 20 | 256 | 96 | 4,000,000 | 27 | 5,857 | 394 | 0.6 | 1,645 ms | 784 / 843 ms | 1,106 / 1,143 ms |
| 30 | 336 | 96 | 6,000,000 | 38 | 7,607 | 461 | 0.6 | 2,157 ms | 974 / 979 ms | 1,602 / 1,727 ms |
| 40 | 416 | 96 | 8,000,000 | 48 | 9,189 | 529 | 0.6 | 3,045 ms | 1,105 / 1,171 ms | 2,114 / 2,162 ms |
| 50 | 496 | 96 | 10,000,000 | 56 | 10,466 | 596 | 0.6 | 3,513 ms | 1,297 / 1,357 ms | 2,493 / 2,560 ms |

The exact per-merge rates: **+8 data files, +200,000 delete records** (v2: +32 and +200,000).
**Delete FILES stop growing at 96** — the seeded data-file count — because a v3 delete is a
Puffin deletion vector bound to one data file, rewritten in place, where v2 wrote one
position-delete file per `(spec, partition)` per commit and reached 400. Merge 10 is the
one row still climbing (80 of 96 seeded files have a DV by then). Delete BYTES hold at
0.6 MB against v2's 28.6 MB, because a DV is a bitmap, not a row per deleted position.
`COUNT(*)` answers 10,000,000 at every row.

MERGE wall seconds, in order: 11 16 19 24 25 26 29 31 28 34 32 30 35 38 35 36 41 41 42 45 47
45 41 50 43 53 44 50 51 46 49 55 77 64 41 52 33 55 53 55 63 60 42 63 26 52 30 27 56 20.
CTAS 96.6 s; 50 merges 2,059.6 s (mean **41.19 s**, v2 25.95 s — cross-run, uncontrolled);
leg wall 46.4 min.

### 3.2 Copy-on-write leg (v3) — the zero-delete control

| merges | data files | delete files | manifests | manifest-list B | data MB | `COUNT(*)` p50 | partition p50 / p99 | point p50 / p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 96 | 0 | 1 | 1,728 | 260 | 3,007 ms | 424 / 438 ms | 883 / 955 ms |
| 10 | 129 | 0 | 3 | 2,076 | 341 | 2,942 ms | 477 / 488 ms | 774 / 796 ms |
| 20 | 128 | 0 | 3 | 2,081 | 346 | 2,977 ms | 499 / 513 ms | 804 / 817 ms |
| 30 | 125 | 0 | 4 | 2,245 | 347 | 3,063 ms | 519 / 536 ms | 853 / 894 ms |
| 40 | 123 | 0 | 5 | 2,405 | 345 | 3,122 ms | 507 / 539 ms | 877 / 899 ms |
| 50 | 115 | 0 | 5 | 2,407 | 343 | 3,396 ms | 528 / 565 ms | 926 / 956 ms |

Flat, as on v2. CTAS 95.4 s; 50 merges 6,550.5 s (mean **131.01 s**, v2 107.28 s); leg wall
116.1 min. Warehouse **16,464 MB for a 343 MB table (48x)** before `expire_snapshots`.
**This leg is the control for the cross-run comparison**: it writes no delete files on either
format, so its 1.22x is what eight days and a shared box cost, not what v3 costs. COW costs
**3.2x MoR per MERGE** within this run (131.0 s against 41.2 s); within the v2 run it was 4.1x.

### 3.3 v3 against v2 at the same knobs

Every row of this table is **cross-run and uncontrolled**. The COW row is the control: read
each MoR figure against 1.22x, not against 1.00x.

| Term | v2 (2026-08-24) | v3 (2026-09-02) | v3/v2 |
|---|---:|---:|---:|
| **COW 50 merges — the control** | 5,363.9 s | 6,550.5 s | **1.22x** |
| run wall | 7,768.9 s | 9,755.6 s | 1.26x |
| MoR CTAS / 50 merges / leg wall | 87.3 s / 1,297.7 s / 34.0 min | 96.6 s / 2,059.6 s / 46.4 min | 1.11x / 1.59x / 1.37x |
| COW CTAS / leg wall | 88.6 s / 95.5 min | 95.4 s / 116.1 min | 1.08x / 1.22x |
| MoR maintenance total | 142.34 s | 353.94 s | 2.49x |
| COW maintenance total | 21.19 s | 65.74 s | 3.10x |
| peak RSS | 4,461 MiB | 4,792 MiB | 1.07x |

MoR census and scans at each checkpoint:

| merges | data files v2 → v3 | delete files v2 → v3 | `COUNT(*)` p50 | partition p50 | point p50 |
|---:|---|---|---:|---:|---:|
| 0 | 96 → 96 (1.00x) | 0 → 0 | 0.99x | 1.04x | 1.02x |
| 10 | 416 → 176 (0.42x) | 80 → 80 (1.00x) | 0.99x | 1.44x | 1.32x |
| 20 | 736 → 256 (0.35x) | 160 → 96 (0.60x) | 0.40x | 0.82x | 0.70x |
| 30 | 1,056 → 336 (0.32x) | 240 → 96 (0.40x) | 0.32x | 0.68x | 0.68x |
| 40 | 1,376 → 416 (0.30x) | 320 → 96 (0.30x) | 0.31x | 0.59x | 0.68x |
| 50 | 1,696 → 496 (**0.29x**) | 400 → 96 (**0.24x**) | **0.28x** | **0.55x** | **0.64x** |

**Merge 10 is the one checkpoint where v3 reads worse** (partition 1.44x, point 1.32x): v2's
416 data files parallelise better than v3's 176, and v3 already carries 80 DVs. From merge 20
the delete-file count decides, and every probe crosses under v2 and stays there.

**What the control does on the same cells**, which is how much of the MoR movement is the box
rather than the format. Cross-run COW v3/v2, per checkpoint (merges 10 / 20 / 30 / 40 / 50):
data files **1.28x / 1.29x / 1.26x / 1.21x / 1.11x**, point p50 **0.82x / 0.85x / 0.91x /
0.93x / 1.00x**, partition p50 0.96x / 1.07x / 1.12x / 1.06x / 1.03x, `COUNT(*)` p50 0.97x /
0.98x / 1.01x / 1.03x / 1.11x. The read cells that carry the verdict are the ones where the
control is nearest 1.00x — point p50 at merge 50, control **1.00x**, MoR **0.64x**.

### 3.4 The maintenance sequence at 50 merges of debt (v3)

Merge-on-read leg, census after each step. **Total 353.94 s** (v2 142.34 s).

| step | wall s | result | data files | delete files | delete records | manifests | manifest-list B |
|---|---:|---|---:|---:|---:|---:|---:|
| before | — | — | 496 | 96 | 10,000,000 | 56 | 10,466 |
| `rewrite_position_delete_files` | 0.0 | **REFUSED** — `B-MOR-3`, 96 live Puffin DVs | 496 | 96 | 10,000,000 | 56 | 10,466 |
| `rewrite_data_files` | 350.2 | rewrote 496 → 144, 595,819,342 B; `removed_delete_files_count` **96** | 144 | 0 | 0 | 60 | 11,139 |
| `rewrite_manifests` | 0.3 | 59 → 1 | 144 | 0 | 0 | 2 | 1,911 |
| `expire_snapshots` | 3.3 | 496 data + 203 delete + 641 manifests + 59 manifest lists deleted | 144 | 0 | 0 | 2 | 1,911 |
| `remove_orphan_files` | 0.0 | 0 rows (24-hour floor) | 144 | 0 | 0 | 2 | 1,911 |

**The runbook now finishes the job.** On v2 the same sequence ended with **8 delete files
holding 10,000,000 delete records** and a table reading 2.02x the control; on v3 it ends at
**zero delete files, zero delete records**, 144 data files and 10,000,000 rows. The
`rewrite_position_delete_files` step is inert by refusal, and `rewrite_data_files` does the
whole reclaim in one pass — which is why the v2 ordering rationale ("fold the delete files
first so data compaction reads 8 instead of 400") has no v3 analogue.

Copy-on-write leg, total 65.74 s: position-delete compaction 0.1 s (four zeros, nothing to
do), `rewrite_data_files` 58.5 s (66 → 78 files), `rewrite_manifests` 0.1 s (10 → 1),
`expire_snapshots` 7.0 s (**5,824 data files deleted**), orphan dry run 0.0 s / 0 rows.
Warehouse 16,464 MB → 476 MB.

### 3.5 MoR against the COW control (v3)

| merges | point p50 MoR | point p50 COW | MoR/COW | partition MoR/COW | `COUNT(*)` MoR/COW | v2 point MoR/COW |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 876 ms | 883 ms | 0.99x | 1.03x | 1.01x | 1.00x |
| 10 | 846 ms | 774 ms | 1.09x | 1.28x | 0.60x | 0.68x |
| 20 | 1,106 ms | 804 ms | 1.38x | 1.57x | 0.55x | 1.67x |
| 30 | 1,602 ms | 853 ms | 1.88x | 1.88x | 0.70x | 2.52x |
| 40 | 2,114 ms | 877 ms | 2.41x | 2.18x | 0.98x | 3.29x |
| 50 | 2,493 ms | 926 ms | **2.69x** | 2.46x | 1.03x | **4.18x** |
| after maintenance | 543 ms | 889 ms | **0.61x** | 0.99x | 0.57x | **2.02x** |

Both legs return byte-identical answers at every checkpoint and after maintenance —
`{n: 625669, s: 3128924193}` (partition) and `{n: 2000, s: 9865011}` (point), the same values
the v2 run recorded. That cross-leg identity is the correctness control under every timing.

**The verdict the north star needs.** Merge-on-read on v3 costs **1.59x more to write** and
**0.64x as much to read** at 50 merges of debt, carries a quarter of v2's delete files and
under a third of its data files, and — unlike v2 — comes back to **0.61x the copy-on-write
control** after the runbook instead of 2.02x. MW-7 §6.8 ("cadence bounds how far the scan
degrades; it does not bound the retained dead rows") is a **v2** statement: on v3 the
delete-laden files are reclaimed and the residue is zero.

```yaml
FINDING:
  id: F-SCALE-V3-1
  severity: S3
  category: AT-9
  clause: C-002
  disposition: REMEDIATED
  title: RunResult.started_at was stamped when the result is BUILT, so it recorded the END of the run
  evidence: >
    run_scale_measurement filled started_at in the return statement, after every leg had run.
    The 1e7 run's JSON says 2026-09-02T16:11:14-0400; the run started 13:28:38 and
    /usr/bin/time -v agrees on 2:42:36 of wall clock. MW-7 quoted the same field as a start
    time. FIXED in this unit: the timestamp is now taken beside started_wall at the top of
    run_scale_measurement, so the field means what its name says. The 1e7 numbers above are
    unaffected — the run's real start is stated in the section text.
  registry_candidate: none — a driver disclosure, no engine behaviour claim is wrong
FINDING:
  id: F-SCALE-V3-2
  severity: S3
  category: AT-6
  clause: C-002, C-003
  disposition: REMEDIATED
  title: the MW-8 runbook defaults were fitted to v2 and three of them do not transfer to v3
  evidence: >
    (1) Order — v2 §6.2 justifies folding delete files first so rewrite_data_files reads 8
    instead of 400; on v3 that step REFUSES (B-MOR-3) and rewrite_data_files reclaims all 96
    DVs by itself (removed_delete_files_count 96). (2) Cadence — v2 §6.1 put the 2x
    control-relative crossing at 19.6 merges; on v3 the partition probe crosses 2x between
    merge 30 (1.88x) and merge 40 (2.18x), and the point probe between 40 (2.41x) and 50
    (2.69x). (3) The limit statement — v2 §6.8 says the sequence cannot reclaim delete-laden
    data files; on v3 it does, ending at zero delete files and zero delete records. An MW-8
    runbook must state its format version before it states a number.
  remediation: >
    Discharged in this unit, in the document that carries the runbook for users:
    docs/guide/iceberg-guide.md "The maintenance runbook" now states its format version
    before any number, tells a v3 reader to drop the first CALL and why, and pairs every v2
    figure (400 -> 8, the 19.6-merge crossing, the ~157-delete-file trigger, 2.5 minutes,
    2.02x / 2.45x / 1.90x) with the v3 measurement beside it. Two false sentences in the same
    guide were corrected on the way: it claimed the engine cannot create a v3 table, and that
    repark writes no v3 delete files.
  registry_candidate: disclosure only; B-MOR-3 and RDF-1 already carry the engine claims
```

## 4. Lockstep

| Home | Edit |
|---|---|
| north star §3 row "Scale" | ✅ with the dated v3 numbers and the evidence path |
| `docs/design/format-v3-track.md` §5 Step 6 | one line: the scale workload is measured |
| STATUS | **untouched, deliberately.** STATUS names no scale row — its v3 bullet points at the north-star matrix, which is the row's home and now carries the numbers. STATUS is at 24,995 B against a 25,000 B ceiling, and the only sentences in the v3 bullet long enough to make room are pinned strings owned by V3-9 (`test_plan_1_northstar_fnp_sequence.py`) and LIVE-v3. Rewriting another unit's pinned claim, or raising the ceiling, to add one line here is not worth either. |
| `python/repark-parity/bench/mw7/map.md` | the `--format-version` section, the v3 run row, the v3 numbers row |
| `python/repark/tests/map.md` | the v3 pins as a table |
| `docs/guide/iceberg-guide.md` | the runbook section states its format version first, gives the v3 cycle and the v3 counterpart of every v2 figure; two false v3 sentences corrected and dated (F-SCALE-V3-2) |
| `docs/guide/map.md` | the runbook row names the two formats |
| this ledger | `move`d to `completed/` in the last commit |

Scratch: the warehouses and Parquet trees the run wrote were deleted after the numbers were
read, as MW-7 did. Nothing generated by this unit is committed, and no committed document
cites a scratch path — §3's tables are the record.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: scale-v3-mw7
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Every committed number is recomputed from full-v3.json by a script rather than read
        off the driver's printed tables, including all 30 v3-vs-v2 ratios and all 21
        MoR-vs-COW ratios. The two engines' delete-file census was measured against live
        PySpark 4.1.2 at a matched layout and agrees cell for cell. Peak RSS agrees exactly
        between getrusage and /usr/bin/time -v. The cross-leg answer identity holds at every
        checkpoint and equals the v2 run's values.
      artifacts: ["sections 1, 2, 3.1-3.5"]
    - id: AT-2
      status: ATTACKED
      evidence: >
        The knob is driven at both values in one gate run (two driver fixtures), at the CLI
        boundary (default 2, explicit 3, and 4 refused by argparse), and at the maintenance
        boundary that only v3 reaches — a refusing first procedure. The DV census is checked
        at merge 0 (no DVs), at the growing edge (merge 10, 80 of 96) and at the plateau.
      artifacts: [python/repark/tests/test_mw7_scale_smoke.py]
    - id: AT-3
      status: ATTACKED
      evidence: >
        The one new failure path is the captured refusal. It is armed only at
        format_version >= 3 and catches only UnsupportedOperationException, so a refusal on a
        v2 run still propagates; the v2 fixture proves the four other procedures still return
        results and the v3 fixture proves the refusal is recorded with its text and that the
        remaining four steps carry an empty refusal.
      artifacts: [python/repark-parity/bench/mw7/measure.py, python/repark/tests/test_mw7_scale_smoke.py]
    - id: AT-4
      status: N/A
      justification: One process, one leg at a time, no shared mutable state, no concurrency.
    - id: AT-5
      status: N/A
      justification: >
        Local filesystem and a memory catalog; no AWS, no credential, no network, no
        privileged action. Re-scanned every file this unit touched for absolute home paths,
        scratchpad paths and session ids: none.
    - id: AT-6
      status: ATTACKED
      evidence: >
        Two divergences from the v2 measurement were found and are reported rather than
        tuned: rewrite_position_delete_files refuses on live DVs (B-MOR-3, already dated) and
        the MW-8 runbook defaults do not transfer (F-SCALE-V3-2). The one number that reads
        worse on v3 (merge 10) is stated with its confound rather than dropped.
      artifacts: ["section 3.4", "F-SCALE-V3-2"]
    - id: AT-7
      status: ATTACKED
      evidence: >
        The unbounded-growth shape MW-7 filed as F-MW7-1 is measured closed on v3: the
        sequence ends at zero delete files and zero delete records where v2 ended at 8 files
        and 10,000,000 records, and the table returns to 0.61x the control instead of 2.02x.
        The growth that remains on v3 is the data-file count between maintenance passes,
        +8 per merge against v2's +32.
      artifacts: ["section 3.4", "section 3.5"]
    - id: AT-8
      status: ATTACKED
      evidence: >
        The live oracle is the upstream check: PySpark 4.1.2 + Iceberg 1.11.0 writes the same
        two PUFFIN DVs of 120 records over the same 8 data files at the matched layout, so
        the v3 delete-file arithmetic this ledger reports is Spark's, not RePark's alone.
      artifacts: [python/repark/tests/test_mw7_scale_smoke.py, "section 2"]
    - id: AT-9
      status: ATTACKED
      evidence: >
        The driver's one observability defect found on the way is filed rather than left
        (F-SCALE-V3-1, started_at records the end of the run), and both dated ledger sections
        state the real start time. The refusal is recorded on the step it belongs to, so a
        reader of the JSON sees which procedure did not run.
      artifacts: ["F-SCALE-V3-1", python/repark-parity/bench/mw7/measure.py]
    - id: AT-10
      status: ATTACKED
      evidence: >
        20 tests in the module green (19 plus the live oracle under REPARK_PARITY_LIVE=1),
        and nine mutations run and reverted. 8 red of 9: the CTAS ignoring format_version, the
        session skipping the create opt-in, the refusal text dropped, the default flipped to 3,
        the COW leg taking the merge-on-read properties, an armed step re-raising anyway,
        capture_refusal defaulting to True, and started_at stamped at build time. The ninth —
        arming the capture for EVERY procedure on v3 rather than only
        rewrite_position_delete_files — stays GREEN and is reported rather than hidden: no
        second procedure refuses on the v3 maintenance path, so the narrowing has no observable
        behaviour at gate scale. What is pinned is the mechanism it narrows
        (test_a_refusal_is_recorded_only_when_the_step_is_armed, both directions).
      artifacts: [python/repark/tests/test_mw7_scale_smoke.py]
  complete: true
```
