# Unit ledger — SQL-HARDEN-1 · the cutover pipeline cutover Iceberg SQL shapes

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

**Unit:** SQL-HARDEN-1 · **Date:** 2026-09-04 · **Model:** grok-4.6 ·
**Branch:** `feat/sql-harden-1-cutover-shapes` · **Base:** `origin/main` `e6ebd40`
**Registry:** [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
`CUTOVER-CTAS-REQ-1`, `CUTOVER-MERGE-FILES-1`, `CUTOVER-DEDUP-SCHEMA-1`, `CUTOVER-DATE-1`;
cited `V3-COV-7`.
**Matrix:** [docs/design/sql-harden-cutover-matrix.md](../../../docs/design/sql-harden-cutover-matrix.md).

**Rubric:** STANDARD. `risk_tier: standard`.

## 1. Scope, as checkable propositions

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | For every shape S1–S7, one program on live PySpark 4.1.2 + Iceberg 1.11.0 and on repark (memory catalog): row set, schema, snapshot summary counts, delete-file kinds, v3 `next-row-id`, second-MERGE idempotence. Golden like V3-COV. | `test_sql_harden_cutover.py` + `_sql_harden_cutover_{programs,repark,spark,golden}.py` | **PROVEN** |
| C-002 | The same programs through Glue (`testing_repark_acceptance` under `s3://repark-acceptance-warehouse-bucket-v1/`) and S3 Tables (`TABLE_BUCKET_ARN=…/repark-acceptance-table-bucket-v1`). Skip without `REPARK_AWS_ACCEPTANCE=1`. Spark AWS cells not required. | `test_sql_harden_cutover_against_glue` / `_s3tables` | **PROVEN** |
| C-003 | A contained `crates/repark-*` / facade fix is flipped in this unit with a mutation; anything larger is a registry row + queue entry with the measured pair. HALT if a fix moves another Spark-measured pin. | 0 FIXED; 4 rows filed; `V3-COV-7` cited | **PROVEN** |
| C-004 | Matrix doc, registry rows, STATUS one line under h2, this ledger, maps lockstep. | paths below | **PROVEN** |

`LOGIC_SCORE` = **4/4 `PROVEN`**.

## ERRATA (2026-09-04, critic round 2)

| # | Sev | Finding | Fix |
|---|---|---|---|
| R2-1 | S1 | S6 gold table names interpolated `_NAMESPACE` (`cut`) so AWS legs addressed `<catalog>.cut.<stem>_…` | `make_names` uses the `namespace` argument for all seven stems; `test_rendered_sql_uses_only_the_passed_namespace` greps rendered SQL |
| R2-2 | S2 | AWS S6 assertion was `assert actual['statements']` | `_assert_s6_aws_date_refusal`: `Invalid function 'date'`, pre-fct tables exist only in the acceptance namespace, fct/agg absent |
| R2-3 | S2 | five-line prose under the matrix | a three-row table |
| R2-4 | S2 | CUTOVER-DATE-1 lacked `to_date` / `CAST AS DATE` / `unix_timestamp` controls | always-run pins; row text records both refusals (DATE-FN-1) |

## ERRATA (2026-09-04, CI round 3)

| # | Sev | Finding | Fix |
|---|---|---|---|
| R3-1 | CI | MERGE goldens pinned delete-file count (3 PARQUET / 3 PUFFIN on a 64-core box); CI writes a different count | `as_golden` collapses DEL to kinds; always-run `count >= Spark's 2`; 3 recorded as host-dependent. Proof this box: default 3; `repark.write.max-concurrent-files=1` + `spark.sql.shuffle.partitions=1` → 2; golden stays green |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: sql-harden-1-cutover-shapes
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Nine programs on repark and live Spark; values, schema, snaps, delete files, next-row-id, MERGE idempotence.
      artifacts: [python/repark/tests/test_sql_harden_cutover.py, python/repark/tests/_sql_harden_cutover_golden.py]
    - id: AT-2
      status: ATTACKED
      evidence: Duplicate-key MERGE fixture rejected; S2/S5 use the S3 dedup transform. Gold DATE() kept as production SQL.
      artifacts: [python/repark/tests/_sql_harden_cutover_run.py]
    - id: AT-3
      status: ATTACKED
      evidence: No Spark-measured pin flipped. V3-COV-7 cited, not restated as a new codec row.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-4
      status: N/A
      justification: No new concurrency; one Spark JVM, module-scoped live session.
    - id: AT-5
      status: ATTACKED
      evidence: AWS legs scratch namespace only; no DROP TABLE in test_aws_acceptance.py (existing structural pin); no .github, no Cargo pin, no secrets.
      artifacts: [python/repark/tests/test_aws_acceptance.py]
    - id: AT-6
      status: ATTACKED
      evidence: No public API change; measurement harness only.
      artifacts: [python/repark/tests/test_sql_harden_cutover.py]
    - id: AT-7
      status: ATTACKED
      evidence: V3-COV goldens untouched; new matrix is a sibling.
      artifacts: [python/repark/tests/test_v3_statement_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: Always-run repark half; Spark behind REPARK_PARITY_LIVE=1; AWS behind REPARK_AWS_ACCEPTANCE=1.
      artifacts: [python/repark/tests/test_sql_harden_cutover.py]
    - id: AT-9
      status: ATTACKED
      evidence: Mutation table below.
      artifacts: [task/ledgers/staging/sql-harden-1-cutover-shapes-ledger.md]
    - id: AT-10
      status: ATTACKED
      evidence: STATUS one line under h2; maps lockstep; four registry rows.
      artifacts: [STATUS.md, docs/design/sql-harden-cutover-matrix.md, docs/spark-sql-iceberg-parity.md]
  complete: true
```

## 2. Oracle table (C-001)

| Engine | Pin |
|---|---|
| live PySpark 4.1.2 + Iceberg 1.11.0 | `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC`, `REPARK_PARITY_LIVE=1`, catalog `sqlh1`, Iceberg runtime jar `/tmp/iceberg-spark-runtime-4.1_2.13-1.11.0.jar` |
| repark 1.0.1 (`e6ebd40` + this unit) | memory catalog `ice`, `repark.sql.allowCreateFormatVersion3=true` for S7 |

Measured 2026-09-04. Single-file bronze parquet. Live-cell rules 1–7: `getActiveSession()` first, stop only an owned session, module-private catalog, `PYSPARK_SUBMIT_ARGS` untouched, no `spark.jars.ivy`.

## 3. The measured matrix

| Row | repark vs Spark | Registry |
|---|---|---|
| `s1-ctas-if-fresh` | rows EQUAL; Arrow/Iceberg requiredness DIVERGES; codec `V3-COV-7` | `CUTOVER-CTAS-REQ-1` |
| `s2-merge-idempotent` | rows EQUAL, second MERGE row-idempotent, snaps `append,overwrite,overwrite`; delete-file kinds PARQUET; count host-dependent (3 on a 64-core box; ≥ Spark's 2) | `CUTOVER-MERGE-FILES-1` |
| `s3-dedup-coalesce-cast` | values EQUAL; `string_view` vs `string`; id/amount/part nullability | `CUTOVER-DEDUP-SCHEMA-1` |
| `s4-overwrite-partitions` | rows EQUAL `[(B,y,20),(C,z,10)]`; snaps EQUAL; only META codec | `V3-COV-7` |
| `s5-maintenance-calls` | CALL tuples EQUAL; rows EQUAL; schema: every field required after the deduped CTAS where Spark makes them optional; codec as V3-COV-7 | `CUTOVER-DEDUP-SCHEMA-1` |
| `s6-gold-incremental` | repark `Invalid function 'date'`; Spark fct `(s1,10,15),(s2,20,40),(s3,10,15)` | `CUTOVER-DATE-1` |
| `s7-ctas-if-fresh` | as S1; format-version 3; `next-row-id` 3 both | `CUTOVER-CTAS-REQ-1` |
| `s7-merge-idempotent` | as S2; PUFFIN kinds; count host-dependent (3 on a 64-core box; ≥ Spark's 2); `next-row-id` 6 both | `CUTOVER-MERGE-FILES-1` |
| `s7-overwrite-partitions` | as S4; `next-row-id` 4 both | `V3-COV-7` |

Totals: 9 programs, 0 EQUAL, 9 DIVERGES, 0 FIXED.

## 4. What was not fixed (C-003)

| Row | Why not a contained fix |
|---|---|
| `CUTOVER-CTAS-REQ-1` | Create-path policy; SE-1 tighten-derived refusal (V3-COV-8 sibling) |
| `CUTOVER-MERGE-FILES-1` | Write-path packing (count host-dependent); row contract already Spark-equal |
| `CUTOVER-DEDUP-SCHEMA-1` | Utf8View + analyzer nullability; values Spark-equal |
| `CUTOVER-DATE-1` | `date(ts)` spelling; next error would be `unix_timestamp` (R-FN-BATCH1) |
| `V3-COV-7` | Already BACKLOG; stamping `zstd` is create-path policy |

No HALT: no Spark-measured pin was moved.

## 5. AWS legs (C-002)

| Leg | Env | Result |
|---|---|---|
| Glue | `REPARK_AWS_ACCEPTANCE=1`, `REPARK_ACCEPT_WAREHOUSE=s3://repark-acceptance-warehouse-bucket-v1/`, namespace `testing_repark_acceptance` | **PASS** 2026-09-04 — row probes of S1–S5/S7 match memory repark; S6 catalog-path only (`DATE()` still refused) |
| S3 Tables | `TABLE_BUCKET_ARN=<the repark-acceptance table-bucket ARN>` | **PASS** 2026-09-04 — same core as Glue; 2 passed in 123 s |

Skip without the flag. No `DROP TABLE` in `test_aws_acceptance.py` (structural pin in `test_acceptance_helpers.py`); tables use `testing_sqlh1_*_<uuid>`. Spark AWS cells not required.

## 6. Mutation table

| Knob | Expected | Result |
|---|---|---|
| `VERDICTS['s1-ctas-if-fresh'] = 'EQUAL'` | `test_sql_harden_verdicts_match_the_committed_halves` red | **1 red of 1** |
| `CUTOVER-DATE-1 —` heading renamed | `test_every_diverging_row_names_a_registry_row_that_exists` red | **1 red of 1** |

| `make_names` survey stem uses `_NAMESPACE` | `test_rendered_sql_uses_only_the_passed_namespace` red | **1 red of 1** (round 2) |
| `_SPARK_DELETE_FILE_FLOOR = 4` | `test_merge_delete_file_count_meets_spark_floor` red | **2 red of 2** (round 3) |

Mutation battery: **5 red of 5**.

## 7. Docs (C-004)

| Path | What |
|---|---|
| `docs/design/sql-harden-cutover-matrix.md` | matrix |
| `docs/spark-sql-iceberg-parity.md` | four rows + `V3-COV-7` cited |
| `STATUS.md` | one line under h2 (24,787 B) |
| lockstep `map.md` | `python/repark/tests`, `docs/design`, `task/ledgers/staging` |

## 8. Gates

| Gate | Exit |
|---|---|
| `make develop` | 0 |
| `make verify` | 0 |
| facade pytest (deselect smoke) | 0 on the matrix; host `test_cross_validator_live_pyspark_shape` SemLock PermissionError (known) |
| parity pytest | 0 (555 passed) |
| live matrix + disclosure co-collect | 0 (22 passed) |
| AWS Glue + S3 Tables legs | 0 (2 passed, 142 s after namespace fix) |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 (STATUS 24787 B) |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
| ruff check/format | 0 |

## 9. Delivery template

```yaml
DELIVERY_SIGNOFF:
  pr_unit: sql-harden-1-cutover-shapes
  artifacts_verified:
    ledger: PASS (C-001..C-004 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10)
    findings_ledger: PASS (none open)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS
  status_update: STATUS.md h2 one line; four registry rows; matrix filed
  verdict: ACCEPTED
  rejection_route: N/A
SHIPPED_FLAG_REGISTER:
  pr_unit: sql-harden-1-cutover-shapes
  flags: []
  count: 0
```
