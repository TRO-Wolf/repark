# map — fixtures/torture/data/ice_write_options_rp_1 (Spark oracle: caller summary on replace commits)

## Purpose

The measured Spark 4.1.2 answers for ICE-WRITE-OPTIONS-RP-1: a caller-supplied
`snapshot-property.replace-partitions` value on a replace-partitions commit (dynamic
`insertInto` overwrite, `writeTo(t).overwritePartitions()`). Recorded 2026-09-18 on
PySpark 4.1.2 + `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`, Hadoop
catalog, `local[2]`. The facade pins in
`python/repark/tests/test_ice_write_options_rp_1.py` read every expectation from the
oracle file; no summary value is a literal in a test body.

## Contents

- `spark_rp_oracle.json` — `oracle` plus 5 `cells` keyed by door and option value:
  `insertInto_dynamic_false` / `insertInto_dynamic_true` (`insertInto` overwrite under
  `partitionOverwriteMode=dynamic` with the option `false` / `true`),
  `overwritePartitions_true` / `overwritePartitions_false`
  (`writeTo(t).overwritePartitions()` with the option `true` / `false`),
  `overwritePartitions_control_other` (`overwritePartitions()` with
  `snapshot-property.k=v` and no `replace-partitions` option). Each cell carries
  `door`, `statement` (the write with a `<t>` table placeholder), `option_key`,
  `option_value`, `outcome`, `committed` (a snapshot was added), and the newest
  snapshot summary's `last_summary_replace_partitions` and `last_summary_k`. The
  document also carries the shared `table_ddl`, `seed` and `source`.
- `map.md` — this file.

## Provenance

Never hand-edited: re-run `python/repark/tests/_record_ice_write_options_rp_1.py` on
live PySpark to re-record. Raw-recording SHA-256: `spark_rp_oracle.json`
`2d67f856b20d38876ae24bf57379fede1b25efe8f8ae563845fc9616f4e3f4a6`
(byte-identical to the recording).

## Reading the cells

- Every cell commits (`committed` true): Spark lands the caller's value instead of
  refusing — this row is not a collision.
- `insertInto_dynamic_false` → summary `replace-partitions=false`,
  `insertInto_dynamic_true` → `true`; `overwritePartitions_true` → `true`,
  `overwritePartitions_false` → `false`.
- `overwritePartitions_control_other` → summary `replace-partitions=true` (the
  operation's own marker, no caller value to win) with `k=v`.
- The recorder seeds one fresh table per cell (`(1, 'a'), (2, 'b')`, source
  `(3, 'a')`); the orchestrator's `probe_rp.py` ran the five writes sequentially on
  one table. The pinned fields (commit plus the two summary values) agree cell for
  cell with the probe's `RESULT` line of 2026-09-18 (run 22b).

pins: ice-write-options-rp-1/C-001, C-002

## Pointers

- Up: [../map.md](../map.md)
- Consumer: [../../../../../repark/tests/test_ice_write_options_rp_1.py](../../../../../repark/tests/test_ice_write_options_rp_1.py)
- Driver: [../../../../../repark/tests/_record_ice_write_options_rp_1.py](../../../../../repark/tests/_record_ice_write_options_rp_1.py)
- Ledger: `task/ledgers/staging/ice-write-options-rp-1-ledger.md`
