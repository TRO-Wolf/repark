# map — python/repark/tests/parity

## Purpose

Cross-subsystem parity: the acceptance harness and its live AWS legs, the cutover battery, the live PySpark oracle tier, dogfood and revalidation records, and the EX-* PySpark-example sweep.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_acceptance_commit_resolver.py](test_acceptance_commit_resolver.py) — ICE-INTEGRITY-PINS-1 (2026-10-22): leg U's resolver, AWS-free on every pull request: two agreeing reads resolve, a late landing is waited for, a failed read is "unresolved" (never "not committed"), reads that never agree are unresolved at the deadline, the held metadata file names exactly one attempt, and a missing status-check property is named.
- [test_acceptance_cross_read_local.py](test_acceptance_cross_read_local.py) — Local cross-read leg: live Spark reads RePark-written acceptance tables.
- [test_acceptance_helpers.py](test_acceptance_helpers.py) — AWS-free unit tests for the acceptance-harness helpers — these run EVERYWHERE (no gate).
- [test_acceptance_replace_offline.py](test_acceptance_replace_offline.py) — Offline pin for the CREATE OR REPLACE twice acceptance helper on a memory catalog.
- [test_acceptance_v3_helpers.py](test_acceptance_v3_helpers.py) — AWS-free structural pins for the LIVE-v3 leg body and its two live legs.
- [test_aws_acceptance.py](test_aws_acceptance.py) — Env-gated real-AWS acceptance harness — the source publish job's path against Glue.
- [test_aws_acceptance_cross_read.py](test_aws_acceptance_cross_read.py) — AWS cross-read leg: live Spark reads the AWS acceptance tables.
- [test_acceptance_commit_gate.py](test_acceptance_commit_gate.py) — ICE-INTEGRITY-PINS-1 (2026-10-22): the commit gate and its SigV4 signer, AWS-free on every pull request: the 26 published AWS vectors, the regional allow list, commit recognition on both catalogs, the scratch-namespace refusal, every script against a loopback fake endpoint, the second signature, no header logged, and one RePark session reaching Glue through the gate.
- [test_aws_acceptance_commit_unknown.py](test_aws_acceptance_commit_unknown.py) — ICE-INTEGRITY-PINS-1 leg U (2026-10-22): 96 live statements, U-1 to U-5, U-5r, U-C0 and U-C1 × three doors × v2 and v3 × Glue and S3 Tables, through the commit gate. Skipped without `REPARK_AWS_ACCEPTANCE=1`; the S3 Tables cells skip without `TABLE_BUCKET_ARN`.
- [test_aws_acceptance_cross_release.py](test_aws_acceptance_cross_release.py) — ICE-INTEGRITY-PINS-1 leg X (2026-10-22): 40 live cells, the hash-pinned 1.5.5 and the head reading each other's tables on Glue and S3 Tables. Skipped without `REPARK_AWS_ACCEPTANCE=1`; it fails, never skips, when `REPARK_PREVIOUS_RELEASE_PYTHON` is unset with the gate on.
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
- [test_cross_release_local.py](test_cross_release_local.py) — ICE-INTEGRITY-PINS-1 (2026-10-22): leg X's local control on a `type=hadoop` warehouse. X-L (20 cells) and X-NS (printed) run only when `REPARK_PREVIOUS_RELEASE_PYTHON` names the 1.5.5 interpreter; X-C0 (head writes, head reads) and the two X-C1 refusals run on every pull request.
- [test_v3_acceptance_local.py](test_v3_acceptance_local.py) — LIVE-v3: the Glue / S3 Tables v3 leg body, pinned against the local catalog.
- [_acceptance_commit_gate.py](_acceptance_commit_gate.py) — ICE-INTEGRITY-PINS-1 (2026-10-22): the catalog endpoint gate of pin U. A loopback HTTP server the writer session's `uri` (Glue) or `endpoint_url` (S3 Tables) names; it recognises the commit (`AWSGlue.UpdateTable`; `PUT …/metadata-location`), forwards only an allow list of the operations the drills send (fix round 1, 2026-10-22; fix round 2, 2026-10-23: Glue `GetDatabases` and read-only `GetDatabase`, `GetTables` and `GetTable` for any database, while `CreateTable` and `UpdateTable` name the scratch database, with no `CatalogId` and no repeated JSON key; S3 Tables `get-table`, `metadata-location` GET and PUT, `ListTables` in the scratch namespace, table PUT, and the namespace list, GET and PUT with the body checked), refuses everything else before signing, signs every request again for the region's Glue or S3 Tables endpoint (the only upstreams it accepts), never logs a header, and runs one script per cell: forward, drop or land the commit, reset its reply or answer a throttle, cut later requests, or hold.
- [_acceptance_cross_release.py](_acceptance_cross_release.py) — ICE-INTEGRITY-PINS-1 (2026-10-22): leg X's driver: one side per release (role, interpreter, version, install origin), the runner as a child, and the comparison of the writer's read-back with the reader's read (the shared schema and row-set helpers plus exact Arrow types, the current snapshot, `total-records`, and the delete files).
- [_acceptance_cross_release_runner.py](_acceptance_cross_release_runner.py) — ICE-INTEGRITY-PINS-1 (2026-10-22): the one script both releases run in leg X, with calls 1.5.5 has (the builder, `sql`, `to_arrow`): it refuses unless its version and install origin (`index-wheel` or `local-build`) are its role's, adopts a local table's newest metadata file, creates one of the X kinds, appends, or reads, and writes the read to Arrow IPC with the current snapshot's id, `total-records` and delete files.
- [_acceptance_sigv4.py](_acceptance_sigv4.py) — ICE-INTEGRITY-PINS-1 (2026-10-22): SigV4 header signing in the standard library (ruling 88 and Q10), test-only.
- [_acceptance_commit_resolver.py](_acceptance_commit_resolver.py) — ICE-INTEGRITY-PINS-1 (2026-10-22): leg U's resolver rules: the stable snapshot-id read (two reads 2 s apart agree within 60 s, else `UnresolvedError`), the attempted snapshot from the held metadata file, and the missing status-check properties.
- [_acceptance_cross_read.py](_acceptance_cross_read.py) — Spark reads RePark-written tables and answers row-equal.
- [_acceptance_replace.py](_acceptance_replace.py) — CREATE OR REPLACE TABLE … AS twice leg body for the real-AWS acceptance harness.
- [_acceptance_v3.py](_acceptance_v3.py) — Format-version-3 leg body for the real-AWS acceptance harness (LIVE-v3).

## ICE-INTEGRITY-PINS-1 pins U and X (2026-10-22)

The live legs go here beside `test_aws_acceptance.py` (FACADE-TESTS-LAYOUT-1 moved the facade tests
after the sketch named `python/repark/tests/`). Data files:

- [sigv4_v4_vectors.json](sigv4_v4_vectors.json) — 28 header-mode vectors of the published AWS SigV4 suite (aws-c-auth v0.9.0 `aws-signing-test-suite/v4`, Apache-2.0, as vendored in the `aws-sigv4` 1.5.1 crate): request, context, canonical request, string to sign and signature; `double-encode-path` carries the canonical request only. The two `x-www-form-urlencoded` vectors are the only ones with a body, so they hold the payload hash the gate takes on every commit; their `X-Amz-Content-Sha256` is given as an input header, as the suite's context signs it (fix round 1, 2026-10-22: the earlier text left them out with a wrong reason). `get-space-normalized` is left out because it hides no gate path: the SDK sends percent-encoded paths, and the gate's double encoding is pinned by `double-encode-path`.
- [previous_release_requirements.txt](previous_release_requirements.txt) — the previous release for leg X, compiled with `uv pip compile --generate-hashes --only-binary :all:` from `repark==1.5.5`. Its five wheel hashes equal PyPI's for 1.5.5. The workflow installs it with `--require-hashes` into `.venv-previous` before any credential exists. One release-checklist line moves it to the newest previous release at each tag (ruling 81).

The gate's unit test, the vectors and the botocore cross-check (once, in a throwaway moto 5.2.3 venv built offline from the uv cache; recorded in the ledger) pins: ice-integrity-pins-1/C-032, C-033. The requirements file pins: ice-integrity-pins-1/C-034

Leg U and leg X were built on 2026-10-22; their first live run is after the owner wires #1070.
Locally, X-L read 20 of 20 cells equal both ways, X-C0 and X-C1 held, the two live modules collect
136 tests that skip without the gate, and leg X fails without the previous interpreter.
pins: ice-integrity-pins-1/C-035, C-036, C-039

The resolver's unit test and X's type-aware comparison (`test_x_compare_*`), red under the step-4 mutants:
pins: ice-integrity-pins-1/C-040, C-041

Fix round 1 (2026-10-22, verifier round 1 S1-1): the gate's deny list became an allow list. `test_glue_refuses_every_request_the_drills_do_not_send` and `test_s3tables_refuses_every_request_the_drills_do_not_send` carry each request the verifier got forwarded, red on `c6d74054` (14 cells), and the `forwards_the_operations_the_drills_send` tests keep the drill operations open.
pins: ice-integrity-pins-1/C-042

Fix round 1 (2026-10-22, verifier round 1 S1-2): the vector set grows to 28 with the two body vectors, so a signer that hashes an empty payload reds them. pins: ice-integrity-pins-1/C-043

Fix round 1 (2026-10-22, verifier round 1 S1-4): the status-check test now gives a wrong value (`num-retries = 3`) and requires it named, and `test_x_compare_sees_a_string_read_as_a_large_string` is the X cell where `compare_schemas` is silent and only the exact Arrow type check fires. pins: ice-integrity-pins-1/C-045

Fix round 2 (2026-10-23, first live run F1): the gate forwards read-only Glue operations for any database while `CreateTable` and `UpdateTable` still name the scratch namespace; the twin's moved and added cells hold both sides. pins: ice-integrity-pins-1/C-046

Fix round 2 (2026-10-23, first live run F2): the legs' MERGE and INSERT source is the `(SELECT * FROM VALUES … AS v(id, name)) AS s` form, and `test_the_leg_source_resolves_on_the_local_catalog` runs both doors against the local catalog; the short form, which Spark refuses at parse and RePark fails at resolution, is carded as VALUES-ALIAS-COLUMNS-1. pins: ice-integrity-pins-1/C-047

Fix round 2 (2026-10-23, first live run F3): the cross-release runner is cut off at 300 s, and `test_a_cut_off_runner_reports_its_stderr` pins the exit-124 report with the stderr tail. pins: ice-integrity-pins-1/C-048

Fix round 3 (2026-10-23, second live run G1): a gate refusal of a request with no X-Amz-Target names the method, path and lower-cased sorted header names, and every refusal is recorded as an exchange; the live leg's refusal message lists the exchanges too. pins: ice-integrity-pins-1/C-050

Fix round 3 (2026-10-23, second live run G2): the partitioned cross-release table is `months(ts), bucket(4, id)`, and the pin reads the head-written table's file count (at most 160) and its spec back. pins: ice-integrity-pins-1/C-051

Fix round 3 (2026-10-23, second live run G3): the cross-read step reads a metadata location through the filesystem its URI names, so `s3://` locations no longer collapse to `s3:/`. pins: ice-integrity-pins-1/C-052

Fix round 4 (2026-10-23, third live run H1): the gated Glue session names its S3 endpoint, so the fork's Glue catalog file I/O no longer reaches the gate; the two `gate_session_extra` cells in `test_acceptance_commit_resolver.py` pin the Glue pair and the S3 Tables single key. Leg X holds live at 40 of 40. pins: ice-integrity-pins-1/C-054, C-055

## Path expressions

The move put every file here one level below the old flat directory. These files had a
path expression repointed in the same unit and no other line changed:

| file | change |
|---|---|
| `test_acceptance_helpers.py` | reads the root `_acceptance.py` source one level up. |
| `test_parity_live.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_parity_live_dynflatten.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_pyspark_compat_smoke.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |

## Pointers

- Up: [../map.md](../map.md)
