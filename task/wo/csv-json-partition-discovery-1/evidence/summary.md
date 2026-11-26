# CSV-JSON-PARTITION-DISCOVERY-1 evidence summary

Every count below is printed by `render_step0.py` or the corpora comparator; nothing is hand-counted.
Spark 4.1.2 (`local[2]`, zone `UTC`); main is `pkg-d5-bae11841` (#1435 merged), head as named in the hand-back.

## Step 0 (`probe_step0.py`, 1146 cells, ANSI on and off)

| group | cells | main = Spark | head = Spark | regressed | TARGET | PROTECTED | KNOWN_DIVERGENCE | EXPLORATORY |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `C/csvh` | 50 | 2 | 22 | 0 | 20 | 2 | 22 | 6 |
| `C/csvn` | 50 | 2 | 22 | 0 | 20 | 2 | 22 | 6 |
| `C/json` | 44 | 2 | 26 | 0 | 24 | 2 | 18 | 0 |
| `P/csvh` | 310 | 112 | 230 | 0 | 116 | 112 | 82 | 0 |
| `P/csvn` | 310 | 112 | 242 | 0 | 122 | 112 | 76 | 0 |
| `P/json` | 310 | 112 | 236 | 0 | 122 | 112 | 76 | 0 |
| `TB/csvh` | 24 | 2 | 24 | 0 | 22 | 2 | 0 | 0 |
| `TB/csvn` | 24 | 2 | 24 | 0 | 22 | 2 | 0 | 0 |
| `TB/json` | 24 | 2 | 24 | 0 | 22 | 2 | 0 | 0 |
| **total** | 1146 | 348 | 850 | 0 | 490 | 348 | 296 | 12 |

TARGET equal to Spark on head: 490 of 490; PROTECTED equal on head: 348 of 348; KNOWN_DIVERGENCE and EXPLORATORY answering as main: 256 of 308; regressed: 0

Known-divergence and exploratory cells whose head answer differs from main's (none was equal to Spark on main):

| layout or canary | query | head = Spark | cells |
|---|---|---|---:|
| `case_mixed` | `read` | no | 4 |
| `case_mixed` | `read` | yes | 2 |
| `case_mixed` | `rt` | no | 4 |
| `case_mixed` | `rt` | yes | 2 |
| `case_mixed` | `where_p2` | no | 4 |
| `case_mixed` | `where_p2` | yes | 2 |
| `escaped` | `where_p2` | no | 6 |
| `infer_schema` | `read` | no | 4 |
| `str_vals` | `where_p2` | no | 6 |
| `stray` | `read` | no | 6 |
| `stray` | `rt` | no | 6 |
| `stray` | `where_p2` | yes | 6 |

## #1430 verifier corpora (Parquet), on main and head

| corpus | cells | Spark | main = Spark | head = Spark | head changed vs main | regressed |
|---|---:|---:|---:|---:|---:|---:|
| `probe_d2.py` | 522 | 522 | 510 | 510 | 0 | 0 |
| `repro_d1_findings.py` | 33 | 33 | 29 | 29 | 0 | 0 |
| `atk_opus.py` | 898 | 898 | 808 | 808 | 0 | 0 |

## Delta 1: the verifier's suites (`atk.py`, `atk2.py`, `repro.py`), replayed on Spark, main and head

Main is `pkg-d5-bae11841`; the comparator is the verifier's `cmp.py` (two refusals compare equal).

| suite | cells | main = Spark | head = Spark | REGRESSED | MOVED_BUT_OFF | FIXED |
|---|---:|---:|---:|---:|---:|---:|
| `atk.py` | 1830 | 562 | 1504 | 0 | 54 | 942 |
| `atk2.py` | 334 | 36 | 94 | 0 | 0 | 58 |
| `repro.py` | 24 | 4 | 10 | 0 | 0 | 6 |

## Facade pins (`python/repark/tests/io/test_csv_json_partition_discovery_1.py`)

Expectations recorded from Spark by `record_pins.py` over the layouts in `_csv_json_partition_layouts.py`.

| package | TARGET pass | TARGET fail | PROTECTED pass | PROTECTED fail |
|---|---:|---:|---:|---:|
| main `pkg-d5-bae11841` | 0 | 146 | 77 | 0 |
| head `pkg-head-b82ce609` (delta 1) | 146 | 0 | 77 | 0 |
| A1 mutant (a JSON user schema naming a partition column casts the inferred value), `pkg-head-b82ce609` with `reader.py` patched | 146 | 0 | 76 | 1 |
| first round: main-risk mutant (an unpartitioned read takes the discovery path) `pkg-mutant-m1-9a8a1538` | 144 | 0 | 6 | 60 |

The delta 1 pins add `rootmix` and `under_eq` `inputFiles` (PROTECTED: main lists the root file, as Spark does), the
`rootmix` rows (TARGET), and a `DATE` user schema over string directories with ANSI off (PROTECTED: NULL, as Spark
and main); the JSON user-schema cases that name `p` left the pins (now a known divergence).

Whole facade (`run-facade`) on `pkg-head-b82ce609` (delta 1, at `b82ce6091`): `facade: cases 48269 skipped 998
failed-or-error 4 pytest rc 1`, `allow-listed failures seen 4 of 4`, `verdict PASS`. The first round's run on
`pkg-head-8fafb29e` gave 47062 cases and the same verdict.

## Rust mutant proofs (`cargo test -p repark-core --lib spark_nullable::partition_layout_tests`, 7 tests)

| mutant | killed by |
|---|---|
| M1 an unpartitioned CSV or JSON read takes the discovery path | `csv_and_json_unpartitioned_reads_keep_the_listing_read` |
| M2 a CSV file column named like a partition is dropped from the scan | `csv_and_json_unpartitioned_reads_keep_the_listing_read` |
| M3 no listing suffix for CSV and JSON | `csv_and_json_partitioned_reads_share_discovery` |
| M4 a CSV user schema (`positions`) discovers | `csv_and_json_unpartitioned_reads_keep_the_listing_read` |
| M5 no `_cN` names without a header | `csv_and_json_partitioned_reads_share_discovery` |
| M6 the `utf8_columns` re-read keeps the partition fields | `csv_and_json_unpartitioned_reads_keep_the_listing_read` |
| M7 the JSON door ignores `caseSensitive` | `json_overlap_takes_the_directory_value_and_case_rule` |
| M8 CSV `nullValue` discovers | `csv_and_json_unpartitioned_reads_keep_the_listing_read` |
| M9 a 0-byte CSV file leaves the scan | `csv_and_json_partitioned_reads_share_discovery` |
| M10 root files beside partitions unlisted (delta 1) | `root_files_beside_partitions_are_listed_but_not_read` |
| M11 Parquet lists root files too (delta 1) | `root_files_beside_partitions_are_listed_but_not_read` |

## Delta 2: an unreadable root file beside partitions (N3)

Head `pkg-head-5626c3c8`; main `pkg-d5-bae11841`; the delta 1 head `pkg-d1head-df7eb1ba`. The step 0 probes, fixtures and raw
answers stay in the lane kit bundle (`step0/d2/`); the cells and their classes are in its `step0.json` (`D2/`, `D2B/`,
`D2T/`). TARGET and PROTECTED are classed against the delta 1 head; `head = Spark` is this head.

| group | cells | main = Spark | delta 1 head = Spark | head = Spark | regressed | TARGET (head = Spark) | PROTECTED (head = Spark) | KNOWN_DIVERGENCE |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `D2/csv_us` | 120 | 28 | 28 | 28 | 0 | 0 (0) | 28 (28) | 92 |
| `D2/csv_usp` | 120 | 46 | 46 | 46 | 0 | 0 (0) | 46 (46) | 74 |
| `D2/csvh` | 120 | 40 | 80 | 110 | 0 | 30 (30) | 80 (80) | 10 |
| `D2/csvh_inf` | 120 | 86 | 82 | 92 | 0 | 10 (10) | 82 (82) | 28 |
| `D2/csvn` | 120 | 40 | 80 | 110 | 0 | 30 (30) | 80 (80) | 10 |
| `D2/json_inf` | 120 | 88 | 50 | 120 | 0 | 70 (70) | 50 (50) | 0 |
| `D2/json_us` | 120 | 18 | 120 | 120 | 0 | 0 (0) | 120 (120) | 0 |
| `D2/json_usp` | 120 | 24 | 76 | 76 | 0 | 0 (0) | 76 (76) | 44 |
| `D2B/csvh/cores=16` | 3 | 2 | 1 | 1 | 0 | 2 (0) | 1 (1) | 0 |
| `D2B/csvh/cores=2` | 3 | 1 | 2 | 2 | 0 | 1 (0) | 2 (2) | 0 |
| `D2B/csvh_inf/cores=16` | 3 | 3 | 3 | 3 | 0 | 0 (0) | 3 (3) | 0 |
| `D2B/csvh_inf/cores=2` | 3 | 3 | 3 | 3 | 0 | 0 (0) | 3 (3) | 0 |
| `D2T/csvh/cores=16` | 24 | 8 | 12 | 12 | 0 | 0 (0) | 12 (12) | 12 |
| `D2T/csvh/cores=2` | 24 | 8 | 12 | 12 | 0 | 0 (0) | 12 (12) | 12 |
| `D2T/csvh_inf/cores=16` | 24 | 24 | 12 | 24 | 0 | 12 (12) | 12 (12) | 0 |
| `D2T/csvh_inf/cores=2` | 24 | 24 | 12 | 24 | 0 | 12 (12) | 12 (12) | 0 |
| `D2T/csvn/cores=16` | 24 | 8 | 12 | 12 | 0 | 0 (0) | 12 (12) | 12 |
| `D2T/csvn/cores=2` | 24 | 8 | 12 | 12 | 0 | 0 (0) | 12 (12) | 12 |
| `D2T/json_inf/cores=16` | 24 | 24 | 0 | 24 | 0 | 24 (24) | 0 (0) | 0 |
| `D2T/json_inf/cores=2` | 24 | 24 | 0 | 24 | 0 | 24 (24) | 0 (0) | 0 |
| **total** | 1164 | 507 | 643 | 855 | 0 | 215 (212) | 643 (643) | 306 |

- `D2B` (CSV files above 4 MB) and the `D2T` CSV ties (two equal largest root files, one unreadable) are the owner's
  waiver: Spark's packing and core count, or its file-system listing order, decide; head answers. On the card.
- Step 0 of round 1 and delta 1 on this head: TARGET equal to Spark on head: 490 of 490; PROTECTED equal on head: 348 of 348; KNOWN_DIVERGENCE and EXPLORATORY answering as main: 256 of 308; regressed: 0.

Replays (two refusals compare equal; `lo.py` reuses the re-check's recorded Spark and main answers on a fixture rebuilt
by its `make` step):

| suite | cells | main = Spark | head = Spark | REGRESSED | MOVED_BUT_OFF |
|---|---:|---:|---:|---:|---:|
| `atk.py` | 1830 | 562 | 1504 | 0 | 54 |
| `atk2.py` | 334 | 36 | 94 | 0 | 0 |
| `repro.py` | 24 | 4 | 10 | 0 | 0 |
| `lo.py` | 432 | 210 | 396 | 0 | 10 |

- Against the delta 1 head, the cells that changed: `atk` 0, `atk2` 0, `repro` 0.
- `lo.py`: the 10 `MOVED_BUT_OFF` cells are `json/lo_garbage` (Spark's `_corrupt_record`, N2 on the card).
- #1430 corpora: d2 head eq_spark 510 of 522; d2 regressed []; d2 changed_vs_main 0; repro head eq_spark 29 of 33; repro regressed []; repro changed_vs_main 0; atk head eq_spark 808 of 898; atk regressed []; atk changed_vs_main 0.
- Partitioned Parquet `EXPLAIN` against main: 78 of 78 plans identical (the random temp-view names
  normalised).

Cost: JSON read with no user schema over 200 non-empty root files (`lo_many`): about +1.2 ms per read (delta 1 head 4.9 ms → 6.2 ms, pooled over six rounds on two builds; 1.19× to 1.32×), recorded as an absolute line by the owner on 2026-11-26; CSV 1.09×; `count()` equal.

Final-code rounds on `lo_many` (200 root files), best of 30 after 5 warm-ups, three rounds alternating the packages,
the best round:

| reader | step | delta 1 head (ms) | head (ms) | ratio |
|---|---|---:|---:|---:|
| `csvh` | `read` | 5.67 | 5.51 | 0.97 |
| `csvh` | `count` | 1.65 | 1.64 | 0.99 |
| `json` | `read` | 4.36 | 5.66 | 1.30 |
| `json` | `count` | 1.58 | 1.67 | 1.06 |

Pins: 28 Spark-recorded `refusal` cases in `test_csv_json_partition_discovery_1.py` (both ANSI modes; JSON and CSV
with an unreadable root file larger or smaller than the partition files; `inferSchema`; both JSON user-schema shapes;
the `_tmp/` and `.x=1/` hidden controls). They skip where a mode-0 file stays readable (root). A CSV header read over
an unreadable smaller root file is not pinned: head's header scan opens the first file in directory order (N1's
mechanism, on the card).

Rust: `root_file_opens_follow_what_spark_reads_at_inference` and
`unreadable_root_files_refuse_json_and_inferred_csv_reads` (9 tests in the module). Mutants, each killed:

| mutant | killed by |
|---|---|
| M12 `All` opens no root file | both tests |
| M13 a readable tied root file does not stop the text scan | `root_file_opens_follow_what_spark_reads_at_inference` |
| M14 no 4 MB bound | `root_file_opens_follow_what_spark_reads_at_inference` |
| M15 the JSON `user_schema` flag ignored | `unreadable_root_files_refuse_json_and_inferred_csv_reads` |
