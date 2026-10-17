# P2 report for #1075, PA-LOCAL-FS-POLICY-1 (U-5), 2026-10-16

## Verifier verdicts, by time

- 2026-10-15 16:26 EDT — first verifier (head `506fe9ec`): verdict FAIL. Two S1 findings (V-01,
  V-02), one S2 (V-03) and three S3 (V-04 to V-06).
- 2026-10-16 03:49 EDT — re-verifier (head `75d9caec`): verdict FAIL. RV-01 (S1): a `[`, `?` or
  `*` in the warehouse path let a write land outside it. RV-02 (S2) and RV-03 (S4) refused writes
  that land inside. Owner ruling 116 (a) gave one scoped round, because all three trace to how
  the path is read.
- 2026-10-16 13:01 EDT — final re-verifier (head `d38ac00b`): verdict PASS.
- 2026-10-16 17:28 EDT — merged as `14ad3c6d` (squash), under owner ruling 126 (a): verifier PASS
  and CI green.

## Acceptance run

Run id [38969694348](https://github.com/TRO-Wolf/repark/actions/runs/38969694348). Head sha
`14ad3c6d0c7e13de88659b6ddc0ada601b8054af` (the #1075 squash on main). Start
`2026-10-16T21:29:11Z` (2026-10-16 17:29:11 EDT). End `2026-10-16T21:49:59Z` (2026-10-16
17:49:59 EDT). Conclusion `success`.

Jobs:

- `tier-2 live AWS acceptance (Glue + S3 Tables, scratch-only)` — `success`
- `ICE-READ-PERF bench (S3 Tables + Glue, dispatch-only, scratch-only)` — `skipped`

Cell count: 12. The acceptance module has 11 and the dbt gold module has 1. All 12 PASSED
(0 FAILED, 0 SKIPPED). The skipped bench job is a job conclusion and has no PASSED, FAILED, or
SKIPPED line.

### Acceptance module

Summary: `11 passed in 415.51s (0:06:55)`.

- `python/repark/tests/test_aws_acceptance.py::test_process_silver_acceptance_against_glue` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_process_silver_acceptance_against_s3tables` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_mor_merge_compact_expire_against_glue` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_mor_merge_compact_expire_against_s3tables` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_v3_dv_dml_maintenance_against_glue` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_v3_dv_dml_maintenance_against_s3tables` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_sql_harden_cutover_against_glue` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_sql_harden_cutover_against_s3tables` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_create_or_replace_twice_against_glue` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_create_or_replace_twice_against_s3tables` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_u12_s3_path_write_against_scratch_prefix` — PASSED

### dbt gold module

Summary: `1 passed, 97 warnings in 55.60s`.

- `python/dbt-repark/tests/test_aws_acceptance_gold.py::test_gold_stage_on_glue` — PASSED

## What changed in the write path

#1075 puts one local-file write policy in `repark-core` (`local_fs_policy`). The Spark door,
the ANSI door and `repark.sql()` all use it, in place of the two doors' private copies. It
covers audit rows PA-02, PA-03 and PA-04.

- **It checks every write node at any depth.** That means every `Copy` and every
  `CreateExternalTable`, including inside `EXPLAIN ANALYZE` and `PREPARE`. A plain `EXPLAIN` still
  answers and writes nothing.
- **It judges where the bytes land** (owner ruling 114). It takes the followed path of the
  nearest existing ancestor, adds the remaining components, and compares the result with the
  canonical warehouse root. It refuses:
  - a `..` in a missing tail;
  - a dangling symlink;
  - a metadata error other than not-found;
  - an empty location.

  It reads the root with no glob split, which closes RV-01.
- **`repark.sql()` fails closed, with no opt-in** (owner ruling of 2026-10-15). Its guard
  `LocalFsGuardedDialect` wraps the dialect the session is handed (ruling 113). It forwards all
  five `SqlDialect` methods, and a refused write never reaches the inner dialect.
- **The opt-in is read from the `repark.sql` config extension only.** Main's ANSI opt-in never
  opened, because DataFusion 54.1 reports extension fields without their prefix.

The final re-verifier measured these:
- **The earlier findings are closed.** RV-01, RV-02 and RV-03 are closed, and V-01 to V-06
  still hold. A glob character in the warehouse path refuses on all three doors, where main
  writes outside.
- **The six ruling-114 cells write inside, on both doors, at the path the guard computed.** One
  of them, `<wh>/deep/../m1/dd_in2`, was over-refused by main's Spark door.
- **Its own adversarial shapes refuse or land inside, with no panic.** It tried eleven: a
  symlink loop, a symlink to a higher directory with a long tail, a double-encoded `%252e%252e`,
  a `file:` URL with another host, NFC against NFD roots, paths over 900 and 5000 characters,
  sibling string prefixes, control characters, and others.
- **Ruling 113's wrap holds.** U-1's Q6 cell, the Q6 cell under the guard, and the local-file
  cell all pass.
- **Never worse.** `cargo test --no-fail-fast -p repark-core -p repark-spark -p repark-sql
  -p repark-python`: head 5,161 passed and 0 failed, main 5,088 passed and 0 failed.
- **Mutants.** 13 of 13 killed.

## Follow-ups

- **Ruling 105's findings are carded on main, and out of this unit's scope.** They are `CREATE
  TABLE … USING parquet LOCATION '<outside>'` with an `INSERT`, and its CTAS twin, on the Spark
  door. Both write outside every warehouse with the opt-in off, identically on main and head.
- **The verifier also recorded these out of scope:**
  - Every `df.write` path is registered as a local warehouse root, which widens the allow-list.
  - `register_memory_catalog` opens in-warehouse `repark.sql()` writes through an underscore
    attribute, not a public opt-in.
- **The symlink-change race between the check and the write** stays the dated row, as ruled.
- **106 (#1090) composes with this guard.** Its refusal sits inside `DataFusionDialect`, which
  this guard wraps. Under ruling 126 (a), 106 merges main and re-runs the Q6 cell and the
  local-file cell. The EXPLAIN-DML-WRITE-1 card (ruling 131) opens after this report.
