# map — python/repark/tests/parity

## Purpose

Cross-subsystem parity: the acceptance harness and its live AWS legs, the cutover battery, the live PySpark oracle tier, dogfood and revalidation records, and the EX-* PySpark-example sweep.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_acceptance_cross_read_local.py](test_acceptance_cross_read_local.py) — Local cross-read leg: live Spark reads RePark-written acceptance tables.
- [test_acceptance_helpers.py](test_acceptance_helpers.py) — AWS-free unit tests for the acceptance-harness helpers — these run EVERYWHERE (no gate).
- [test_acceptance_replace_offline.py](test_acceptance_replace_offline.py) — Offline pin for the CREATE OR REPLACE twice acceptance helper on a memory catalog.
- [test_acceptance_v3_helpers.py](test_acceptance_v3_helpers.py) — AWS-free structural pins for the LIVE-v3 leg body and its two live legs.
- [test_aws_acceptance.py](test_aws_acceptance.py) — Env-gated real-AWS acceptance harness — the source publish job's path against Glue.
- [test_aws_acceptance_cross_read.py](test_aws_acceptance_cross_read.py) — AWS cross-read leg: live Spark reads the AWS acceptance tables.
- [test_catalog_flow.py](test_catalog_flow.py) — The source publish job's path, end to end from Python — the acceptance kernel.
- [test_cutover_schema_1.py](test_cutover_schema_1.py) — Nullability derived the way Spark derives it on the cutover shapes.
- [test_dogfood_gaps.py](test_dogfood_gaps.py) — Group F — production-job gaps from the dogfood run.
- [test_examples_column_a.py](test_examples_column_a.py) — Divergence pins for the EX-17 Column-a example batch (registry §7 EX-COL-1..2).
- [test_examples_dataframe_a.py](test_examples_dataframe_a.py) — Divergence pins for the EX-15 DataFrame-a example batch (registry §7 EX-DF-1..6).
- [test_examples_dataframe_b.py](test_examples_dataframe_b.py) — Divergence pins for the EX-16 DataFrame-b example batch (registry §7 EX-DF-7..10).
- [test_examples_dataframe_c.py](test_examples_dataframe_c.py) — Divergence pins for the EX-18 DataFrame-c example batch (registry §7 EX-DF-11..17).
- [test_examples_dataframe_d.py](test_examples_dataframe_d.py) — Divergence pins for the EX-19 DataFrame-d and EX-29 class-remainder batches.
- [test_examples_functions_a.py](test_examples_functions_a.py) — Divergence pins for the EX-25 F.* long-tail (a) batch.
- [test_examples_functions_b.py](test_examples_functions_b.py) — Divergence pins for the EX-28 F.* scalar remainder and the EX-30 F.* remainder.
- [test_examples_io_session.py](test_examples_io_session.py) — Divergence pins for the EX-26 reader/writer/session batch.
- [test_examples_ml.py](test_examples_ml.py) — Divergence pins for the EX-27 ml example batch.
- [test_examples_window_catalog.py](test_examples_window_catalog.py) — Divergence pins for the EX-20 window/catalog, EX-21 session and EX-22 types/WriterV2 batches.
- [test_parity_live.py](test_parity_live.py) — The live PySpark oracle tier (L1) + its detector proofs (L6a).
- [test_parity_live_dynflatten.py](test_parity_live_dynflatten.py) — Live dynamicFlatten legs against the shared PySpark oracle; the contracts are cited in the root `map.md`.
- [test_parity_live_fnp8.py](test_parity_live_fnp8.py) — Live Spark and RePark indexed-transform Arrow-shape detector for FNP-8.
- [test_parity_live_fnp9.py](test_parity_live_fnp9.py) — The FNP-9/10 live-oracle leg: collections and JSON goldens re-derived from Spark.
- [test_pr_245_revalidation.py](test_pr_245_revalidation.py) — PR-245 public-door revalidation against the pinned PySpark 4.1.2 measurements.
- [test_pyspark_compat_smoke.py](test_pyspark_compat_smoke.py) — C2 compat smoke — subprocess wrapper.
- [test_v3_acceptance_local.py](test_v3_acceptance_local.py) — LIVE-v3: the Glue / S3 Tables v3 leg body, pinned against the local catalog.
- [_acceptance_cross_read.py](_acceptance_cross_read.py) — Spark reads RePark-written tables and answers row-equal.
- [_acceptance_replace.py](_acceptance_replace.py) — CREATE OR REPLACE TABLE … AS twice leg body for the real-AWS acceptance harness.
- [_acceptance_v3.py](_acceptance_v3.py) — Format-version-3 leg body for the real-AWS acceptance harness (LIVE-v3).

## Pointers

- Up: [../map.md](../map.md)
