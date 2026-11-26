# CSV-JSON-PARTITION-DISCOVERY-1 evidence summary

Every count below is printed by `render_step0.py` or the corpora comparator; nothing is hand-counted.
Spark 4.1.2 (`local[2]`, zone `UTC`), main `fd5c3fcb9`, head as named in the hand-back.

## Step 0 (`probe_step0.py`, 1146 cells, ANSI on and off)

| group | cells | main = Spark | head = Spark | regressed | TARGET | PROTECTED | KNOWN_DIVERGENCE | EXPLORATORY |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `C/csvh` | 50 | 2 | 22 | 0 | 20 | 2 | 22 | 6 |
| `C/csvn` | 50 | 2 | 22 | 0 | 20 | 2 | 22 | 6 |
| `C/json` | 44 | 2 | 38 | 0 | 36 | 2 | 6 | 0 |
| `P/csvh` | 310 | 112 | 230 | 0 | 116 | 112 | 82 | 0 |
| `P/csvn` | 310 | 112 | 242 | 0 | 122 | 112 | 76 | 0 |
| `P/json` | 310 | 112 | 236 | 0 | 122 | 112 | 76 | 0 |
| `TB/csvh` | 24 | 2 | 24 | 0 | 22 | 2 | 0 | 0 |
| `TB/csvn` | 24 | 2 | 24 | 0 | 22 | 2 | 0 | 0 |
| `TB/json` | 24 | 2 | 24 | 0 | 22 | 2 | 0 | 0 |
| **total** | 1146 | 348 | 862 | 0 | 502 | 348 | 284 | 12 |

TARGET equal to Spark on head: 502 of 502; PROTECTED equal on head: 348 of 348; KNOWN_DIVERGENCE and EXPLORATORY answering as main: 244 of 296; regressed: 0

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

## Facade pins (`python/repark/tests/io/test_csv_json_partition_discovery_1.py`)

Expectations recorded from Spark by `record_pins.py` over the layouts in `_csv_json_partition_layouts.py`.

| package | TARGET pass | TARGET fail | PROTECTED pass | PROTECTED fail |
|---|---:|---:|---:|---:|
| main `pkg-m1rf1-fd5c3fcb` | 0 | 144 | 66 | 0 |
| head `pkg-head-f52e438b` | 144 | 0 | 66 | 0 |
| head `pkg-head-8fafb29e` (inside the whole-facade run) | 144 | 0 | 66 | 0 |
| main-risk mutant (an unpartitioned read takes the discovery path) `pkg-mutant-m1-9a8a1538` | 144 | 0 | 6 | 60 |

Whole facade (`run-facade`), on `pkg-head-f52e438b` and again on `pkg-head-8fafb29e` (the final product tree): each
`facade: cases 47062 skipped 998 failed-or-error 4 pytest rc 1`, `allow-listed failures seen 4 of 4`, `verdict PASS`.

## Rust mutant proofs (`cargo test -p repark-core --lib spark_nullable::partition_layout_tests`)

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
