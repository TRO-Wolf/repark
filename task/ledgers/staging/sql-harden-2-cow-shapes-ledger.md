# Unit ledger — SQL-HARDEN-2 · copy-on-write cutover shapes

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

**Unit:** SQL-HARDEN-2 · **Date:** 2026-09-04 · **Model:** opus-5 ·
**Branch:** `feat/sql-harden-2-cow-shapes` · **Base:** `origin/main` `c70a306`
**Registry:** [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
cited `CUTOVER-CTAS-REQ-1`, `V3-COV-7`; no new `CUTOVER-COW-*` row.
**Matrix:** [docs/design/sql-harden-cutover-matrix.md](../../../docs/design/sql-harden-cutover-matrix.md).

**Rubric:** STANDARD. `risk_tier: standard`.

## 1. Scope, as checkable propositions

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | S8 = S1/S2/S4 at v2 CoW and S9 = the same three at v3 CoW, on live PySpark 4.1.2 + Iceberg 1.11.0 and repark (memory catalog): row set, schema, snapshot operations, `delete_files` empty both engines, data-file count after the second MERGE, v3 `next-row-id`. Existing S1–S7 verdicts unchanged. | `test_sql_harden_cutover.py` + `_sql_harden_cutover_{programs,repark,spark,golden}.py`; `cow_properties` | **PROVEN** |
| C-002 | The S8/S9 rows through Glue (`testing_repark_acceptance` under the env warehouse) and S3 Tables (env `TABLE_BUCKET_ARN`). Skip without `REPARK_AWS_ACCEPTANCE=1`. Spark AWS cells not required. | `test_sql_harden_cutover_against_glue` / `_s3tables` | **PROVEN** |
| C-003 | A contained `crates/repark-*` / facade CoW fix is flipped here with a mutation; anything larger is a registry row `CUTOVER-COW-<n>` + queue line. HALT if a fix moves another Spark-measured pin. | 0 FIXED; 0 new CoW rows; `CUTOVER-CTAS-REQ-1` / `V3-COV-7` cited | **PROVEN** |
| C-004 | Matrix doc gains S8/S9, registry pins, STATUS one line under h2, this ledger, maps lockstep. | paths below | **PROVEN** |

`LOGIC_SCORE` = **4/4 `PROVEN`**.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: sql-harden-2-cow-shapes
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Six CoW programs on repark and live Spark; values, schema, snaps, empty delete_files, data-file count, next-row-id, MERGE idempotence.
      artifacts: [python/repark/tests/test_sql_harden_cutover.py, python/repark/tests/_sql_harden_cutover_golden.py]
    - id: AT-2
      status: ATTACKED
      evidence: CoW MERGE delete_files empty is an S1 defect if present; always-run pin test_cow_merge_writes_no_delete_files. S2/S5 still use the S3 dedup transform.
      artifacts: [python/repark/tests/test_sql_harden_cutover.py]
    - id: AT-3
      status: ATTACKED
      evidence: No Spark-measured pin flipped. No CUTOVER-COW row; CUTOVER-CTAS-REQ-1 and V3-COV-7 cited.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-4
      status: N/A
      justification: No new concurrency; one Spark JVM, module-scoped live session.
    - id: AT-5
      status: ATTACKED
      evidence: AWS legs scratch namespace only; no DROP TABLE in test_aws_acceptance.py; no .github, no Cargo pin, no secrets.
      artifacts: [python/repark/tests/test_aws_acceptance.py]
    - id: AT-6
      status: ATTACKED
      evidence: No public API change; measurement harness plus cow_properties.
      artifacts: [python/repark/tests/_sql_harden_cutover_programs.py]
    - id: AT-7
      status: ATTACKED
      evidence: S1-S7 goldens and verdicts untouched; S8/S9 are additive.
      artifacts: [python/repark/tests/_sql_harden_cutover_golden.py]
    - id: AT-8
      status: ATTACKED
      evidence: Always-run repark half; Spark behind REPARK_PARITY_LIVE=1; AWS behind REPARK_AWS_ACCEPTANCE=1.
      artifacts: [python/repark/tests/test_sql_harden_cutover.py]
    - id: AT-9
      status: ATTACKED
      evidence: Mutation table below.
      artifacts: [task/ledgers/staging/sql-harden-2-cow-shapes-ledger.md]
    - id: AT-10
      status: ATTACKED
      evidence: STATUS one line under h2; maps lockstep; existing registry rows cited.
      artifacts: [STATUS.md, docs/design/sql-harden-cutover-matrix.md, docs/spark-sql-iceberg-parity.md]
  complete: true
```

## 2. Oracle table (C-001)

| Engine | Pin |
|---|---|
| live PySpark 4.1.2 + Iceberg 1.11.0 | `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC`, `REPARK_PARITY_LIVE=1`, catalog `sqlh1`, Iceberg runtime jar `/tmp/iceberg-spark-runtime-4.1_2.13-1.11.0.jar` |
| repark 1.0.1 (`c70a306` + this unit) | memory catalog `ice`, `repark.sql.allowCreateFormatVersion3=true` for S9 |

Measured 2026-09-04. Single-file bronze parquet. Live-cell rules 1–7: `getActiveSession()` first, stop only an owned session, module-private catalog, `PYSPARK_SUBMIT_ARGS` untouched in the cutover module, no `spark.jars.ivy`.

## 3. The measured matrix

| Row | repark vs Spark | Registry |
|---|---|---|
| `s8-ctas-cow` | rows EQUAL; Arrow/Iceberg requiredness DIVERGES; codec `V3-COV-7`; `delete_files` empty both; snaps `append` | `CUTOVER-CTAS-REQ-1` |
| `s8-merge-idempotent-cow` | rows EQUAL, second MERGE row-idempotent, snaps `append,overwrite,overwrite`; `delete_files` empty both; data-file count 1 both | `CUTOVER-CTAS-REQ-1` |
| `s8-overwrite-partitions-cow` | rows EQUAL `[(B,y,20),(C,z,10)]`; snaps EQUAL; `delete_files` empty; only META codec | `V3-COV-7` |
| `s9-ctas-cow` | as S8 CTAS; format-version 3; `next-row-id` 3 both | `CUTOVER-CTAS-REQ-1` |
| `s9-merge-idempotent-cow` | as S8 MERGE; `next-row-id` 6 both; data-file count 1 both | `CUTOVER-CTAS-REQ-1` |
| `s9-overwrite-partitions-cow` | as S8 overwrite; `next-row-id` 4 both | `V3-COV-7` |

Across all six CoW rows exactly two cell classes diverge and both are pre-existing rows:
Arrow/Iceberg requiredness on the CTAS and MERGE rows (`CUTOVER-CTAS-REQ-1`) and Spark's stamped
`write.parquet.compression-codec = zstd` on all six (`V3-COV-7`). Row set, snapshot operations,
`delete_files` (empty), data-file count, MERGE idempotence and v3 `next-row-id` are Spark-equal
on every CoW row.

S1–S7 committed verdicts unchanged: the live matrix re-ran all 15 rows on both engines —
50 tests green. Totals: 15 programs, 0 EQUAL, 15 DIVERGES, 0 FIXED, 0 `CUTOVER-COW-*`.

## 4. What was not fixed (C-003)

| Row | Why not a contained fix |
|---|---|
| `CUTOVER-CTAS-REQ-1` | Create-path policy; same SE-1 tighten-derived refusal. CoW CTAS/MERGE reproduce it. |
| `V3-COV-7` | Already BACKLOG; stamping `zstd` is create-path policy. CoW overwrite's only remaining cell. |
| CoW `delete_files` / data-file count / snaps | Spark-equal. A delete file under CoW would have been an S1 defect; none observed. |

No HALT: no Spark-measured pin was moved.

## 5. AWS legs (C-002)

One run, both legs, 2026-09-04: `REPARK_AWS_ACCEPTANCE=1` with `REPARK_ACCEPT_WAREHOUSE`,
`REPARK_ACCEPT_BRONZE_BUCKET` and `TABLE_BUCKET_ARN` supplied from the runtime environment
(never committed). **2 passed, 6 deselected in 268 s, exit 0.**

| Leg | Cells | Result |
|---|---|---|
| Glue (`glue_catalog`, namespace `testing_repark_acceptance`) | all 15 rows replayed; row probes equal the memory half; CoW MERGE `delete_files` 0 and data-file count 1 on `s8-merge-idempotent-cow` and `s9-merge-idempotent-cow`; S8/S9 CTAS and overwrite statements OK; S6 gold tables land in the acceptance namespace | **PASS** |
| S3 Tables (`s3tables_catalog`, same namespace) | the same core assertions | **PASS** |

`_assert_s6_aws_date_refusal` was replaced by `_assert_s6_aws_namespace`: DATE-FN-1 landed after
SQL-HARDEN-1, so the `DATE()` refusal it asserted no longer happens and the leg could not pass
until the stale assertion was truthed up. The namespace half of the old assertion is kept.

Skip without the flag. No `DROP TABLE` in `test_aws_acceptance.py`. Spark AWS cells not required.

## 6. Mutation table

| # | Knob | Pins expected red | Result |
|---|---|---|---|
| M1 | `cow_properties` returns the merge-on-read block | 6 `…reproduces_the_measured_repark_answer[s8/s9-*]`, 2 `test_cow_merge_writes_no_delete_files`, 2 `test_cow_merge_data_file_count_matches_golden`, 6 `test_cow_write_properties_are_copy_on_write` | **16 red of 16** |
| M2 | REPARK `s8-merge-idempotent-cow` snapshot ops drop one `overwrite` | `…[s8-merge-idempotent-cow]`, `test_cow_merge_writes_no_delete_files[s8-merge-idempotent-cow]` | **2 red of 2** |
| M3 | REPARK `s9-merge-idempotent-cow` `FILES` 1 → 2 | `…[s9-merge-idempotent-cow]`, `test_cow_merge_writes_no_delete_files[s9…]`, `test_cow_merge_data_file_count_matches_golden[s9…]` | **3 red of 3** |
| M4 | `VERDICTS['s8-ctas-cow'] = 'EQUAL'` | `test_sql_harden_verdicts_match_the_committed_halves` | **1 red of 1** |
| M5 | REPARK `s8-merge-idempotent-cow` `DEL` gains a `POSITION_DELETES/PARQUET` row | `…[s8-merge-idempotent-cow]`, `test_cow_merge_writes_no_delete_files[s8…]` | **2 red of 2** |
| M6 | SPARK `s9-ctas-cow` `next-row-id` 3 → 4 (live) | `test_sql_harden_row_matches_the_live_spark_oracle[s9-ctas-cow]` | **1 red of 1** |

Mutation battery: **25 red of 25**. Every new pin family is covered: the six repark-half rows
(M1), the live-oracle half (M6), the verdict join (M4), empty `delete_files` (M1, M5), the
data-file count (M1, M3) and the copy-on-write property block (M1). The AWS-leg assertions are
proven by the leg runs themselves; the brief allows one run per leg, so no AWS mutation was
taken.

## 7. Docs (C-004)

| Path | What |
|---|---|
| `docs/design/sql-harden-cutover-matrix.md` | S8/S9 rows |
| `docs/spark-sql-iceberg-parity.md` | `CUTOVER-CTAS-REQ-1` pins extended to S8/S9 |
| `STATUS.md` | one line under h2 |
| lockstep `map.md` | `python/repark/tests`, `docs/design`, `task/ledgers/staging` |

## 8. Gates

| Gate | Command | Exit |
|---|---|---|
| develop | `make develop` | 0 |
| Rust + CI | `make verify` | 0 |
| facade suite | `pytest python/repark/tests -q -x --deselect …test_pyspark_compat_smoke.py -k "not test_cross_validator_live_pyspark_shape"` | 0 (4490 passed, 186 skipped, 1 deselected in 336 s) |
| parity suite | `pytest python/repark-parity/tests -q` | 0 (555 passed) |
| live matrix | `REPARK_PARITY_LIVE=1 … pytest test_sql_harden_cutover.py -q` | 0 (50 passed) |
| AWS legs | `REPARK_AWS_ACCEPTANCE=1 … pytest test_aws_acceptance.py -k sql_harden_cutover_against` | 0 (2 passed, 6 deselected in 268 s) |
| map links | `make check-map-sync` | 0 (174 maps clean) |
| ledger grammar | `make check-ledger-grammar` | 0 (25 live ledgers) |
| ledger lifecycle | `make check-ledgers`, `ledger_lifecycle.py check --base origin/main` | 0 |
| docs compaction | `make check-docs-compaction` | 0 (STATUS.md 21415 B) |
| typos | `typos .` | 0 |
| ruff | `ruff check python`, `ruff format --check python` | 0 |

## 9. Delivery template

```yaml
DELIVERY_SIGNOFF:
  pr_unit: sql-harden-2-cow-shapes
  artifacts_verified:
    ledger: PASS (C-001..C-004 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10)
    findings_ledger: PASS (none open)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS
  status_update: STATUS.md h2 one line; S8/S9 matrix; existing registry rows cited
  verdict: ACCEPTED
  rejection_route: N/A
SHIPPED_FLAG_REGISTER:
  pr_unit: sql-harden-2-cow-shapes
  flags: []
  count: 0
```
