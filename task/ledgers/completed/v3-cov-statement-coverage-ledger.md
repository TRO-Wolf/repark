# Unit ledger — V3-COV · full v3 statement coverage against PySpark

**Date:** 2026-09-03 · **Branch:** `feat/v3-cov-statement-coverage` · **Base:** `origin/main` `a0cd39e` ·
**Model:** claude-opus-5 (medium) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md) ·
**Registry:** [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
`V3-COV-1`…`V3-COV-6` · **Path:** STANDARD (`risk_tier: standard`; two small Rust repairs, one new
live harness, docs).
**Gate:** [../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
§2 pillar 4 — discharged here.
**Matrix:** [../../../docs/design/v3-statement-coverage.md](../../../docs/design/v3-statement-coverage.md).

**Retired:** filed here in this unit's last commit.

## 1. Scope, as checkable propositions

| ID | Proposition | Verdict | Evidence |
|---|---|---|---|
| C-001 | An inventory document holds one row per served statement class and per `CALL system.*` procedure, derived from the grammar maps | **PROVEN** | `docs/design/v3-statement-coverage.md` §3, 80 rows; `test_v3_cov_docs.py` counts them from §3 |
| C-002 | A parametrised harness runs each row on repark and on the live oracle against the same v3 seed | **PROVEN** | `python/repark/tests/test_v3_statement_coverage.py` + `_v3_statement_coverage_golden.py`; 160 parametrised tests (80 always-run, 80 live) plus 3 fixed cells |
| C-003 | Every cell is measured on both engines before anything is pinned | **PROVEN** | measured 2026-09-03 on live PySpark 4.1.2 + Iceberg 1.11.0; the golden is that measurement, and §4's two fixes were watched red first |
| C-004 | Every DIVERGES cell is a registry row with a pin | **PROVEN** | 3 cited (`DML-1`, `G3-E8` ×2, `B-MOR-3`), 4 filed (`V3-COV-3`…`V3-COV-6`), 2 FIXED (`V3-COV-1`, `V3-COV-2`) |
| C-005 | The north star, the v3 track and STATUS carry the dated discharge | **PROVEN** | §2 pillar 4 discharged; Step 6 state line dated 2026-09-03 (V3-COV); STATUS 24,882 B; `test_v3_cov_docs.py` + the re-pinned `test_v1_gate_docs.py` |
| C-006 | Maps in lockstep; this ledger files last | **PROVEN** | every touched `map.md` moved in the same commit; this ledger filed into `completed/` in the last commit |

## 2. Method

One `_Program` per inventory row: a v3 seed, the statement(s) under test, and the probes compared.
Seeds are single-file per partition on both engines (repark `INSERT … VALUES`; Spark
`createDataFrame(…).coalesce(1).writeTo().append()`), so a file-shape probe is comparable under the
shared `local[2]` session. Both engines run the *same* SQL text; the repark half is always-run
against the committed golden, the Spark half is `REPARK_PARITY_LIVE=1` and re-asserts the verdict.
Live-cell rules 1–7 all hold: `getActiveSession()` recorded before `getOrCreate()` and only a
self-created session stopped, single-file seeds, the module-private catalog `v3cov`,
`PYSPARK_SUBMIT_ARGS` untouched, no per-call `spark.jars.ivy`, co-collected proof below, and the
repark half in an always-run test.

## 3. The measured matrix

| Totals | |
|---|---|
| Statement programs | 80 across 12 groups (create · insert · delete · update · merge · alter · lifecycle · metadata · lineage · time travel · refs · call) |
| Comparison cells | 255 (statements + probes) |
| EQUAL | 72 |
| REFUSED (both engines refuse) | 1 — `create-v3-write-order`, a parse error on both |
| DIVERGES | 7 |
| Statement classes unmeasured | 0 |
| Runtime | repark 24 s; live Spark 75 s; co-collected with the nightly live legs 2 min 25 s |

Every row, its fixture, its probes and both engines' answers are the matrix in
[../../../docs/design/v3-statement-coverage.md](../../../docs/design/v3-statement-coverage.md) §3;
the measured halves are the committed golden. The seven divergences:

| Row | Statement | repark | Apache Spark | Registry | Class |
|---|---|---|---|---|---|
| `insert-overwrite-partition-dynamic` | `INSERT OVERWRITE t PARTITION (part) SELECT …` | replaces `part = 10` only | default-STATIC wipes the table | `DML-1` | DECLARED residue, stood before this unit |
| `update-not-in-subquery-mor` | `UPDATE … WHERE id NOT IN (SELECT …)` | refuses at the G3-E8 valve | updates ids 1, 3, 4 | `G3-E8` | DEFECT, partial fix, stood before |
| `update-exists-subquery-mor` | `UPDATE … WHERE EXISTS (…)` | refuses at the G3-E8 valve | updates id 2 | `G3-E8` | DEFECT, partial fix, stood before |
| `call-rewrite-position-delete-files` | `CALL system.rewrite_position_delete_files` | refuses a live Puffin DV | returns `0, 0` | `B-MOR-3` | DECLARED by analogy, owner line pending |
| partitioned `INSERT INTO` | v3 `INSERT … VALUES` over two identity partitions | `_row_id` mapping unstable — 7 / 12 ascending, 5 / 12 reversed | `{1:0, 2:1, 3:2, 4:3}` | `V3-COV-3` | **DECLARED 2026-09-03, fork TRIGGER** |
| `delete-all-rows-mor` | `DELETE FROM t WHERE id > 0` (MoR) | one PUFFIN DV, `record_count = 4`, data file live | drops the data file, no delete file | `V3-COV-4` | **BACKLOG** |
| `alter-write-ordered-by` | `ALTER TABLE t WRITE ORDERED BY id` | refuses `NotImplemented` | sets the write order | `V3-COV-5` | **BACKLOG** |
| `meta-position-deletes` | `SELECT pos FROM t.position_deletes` | refuses `FeatureUnsupported` (schema-only port) | one `pos` row | `V3-COV-6` | **DECLARED 2026-09-03, fork TRIGGER** |

## 4. The two defects fixed here, red first

| Row | Measured red on `a0cd39e` | Fix | Green |
|---|---|---|---|
| `V3-COV-1` | `INSERT OVERWRITE t PARTITION (part = 10) SELECT …` → `column types must match schema types, expected Utf8 but found Utf8View`; the `VALUES` spelling of the same statement worked, which is why DML-B never saw it | `crates/repark-iceberg/src/write/partition_overwrite.rs::store_assign_source_column` — `refuse_unless_write_store_assignable`, then a strict (`safe: false`) cast | pin red before, green after (reverted the hunk, rebuilt, watched both pins fail, restored) |
| `V3-COV-2` | `ALTER TABLE t ALTER COLUMN id TYPE BIGINT` then a `_row_id` projection → `lineage scan could not rebuild batch: expected Int64 but found Int32`, while `SELECT id, name` on the same table promoted correctly | `crates/repark-iceberg/src/catalog/lineage_columns.rs::conform_batch` — strict cast when the scan type differs from the declared field | same red-first proof |

Neither fix widens a contract: both apply the store-assignment / conform rule the sibling path
already applied.

## 5. What is deliberately not pinned

`_row_id` on a partitioned seed. `V3-COV-3` makes it unstable, so the partitioned programs pin
`_last_updated_sequence_number` (deterministic and Spark-equal on every measured run) and the
instability has its own two cells —
`test_v3_partitioned_insert_row_id_mapping_is_one_of_two_measured_orders` (the two measured
permutations plus the invariant that the block is `[0, 1, 2, 3]`) and the incidental control
`test_v3_ctas_partitioned_row_id_mapping_is_stable_and_spark_ordered`, which shows the
RePark-owned CTAS writer stable and Spark-ordered on every run. Pinning an unstable value would be
the false green the registry exists to prevent.

## 6. Mutation battery — `test_v3_cov_docs.py`, 9 red of 9

| Test | Mutation applied alone, then restored |
|---|---|
| `…_matrix_row_count_and_verdicts_match_the_stated_totals` | delete the `ctas-v3` matrix row |
| `…_every_stated_total_appears_in_the_totals_table` | change the program total from 80 to 81 |
| `…_every_diverging_row_names_a_registry_row_that_exists` | blank `V3-COV-4`'s registry cell |
| `…_the_rows_this_unit_filed_carry_a_class_a_date_and_a_pin` | strip `V3-COV-1`'s pin lines |
| `…_the_fork_routed_rows_name_a_trigger` | drop `TRIGGER:` from `V3-COV-6` |
| `…_the_north_star_carries_the_measured_discharge` | soften "Nothing in §2 pillar 4 is now owed." |
| `…_the_v3_track_step_6_carries_the_v3_cov_state_line` | soften "Step 6 now owes **no engineering item**" |
| `…_the_harness_and_the_golden_carry_every_matrix_row` | rename `merge-mixed-arms` in the doc only |
| `…_the_rows_an_existing_registry_row_covers_are_cited_not_refiled` | renumber `V3-COV-4` to `V3-COV-7` |

## 7. Gates

| Gate | Exit |
|---|---|
| `make preflight` | 0 |
| `make verify` | 0 |
| `make py-test` | 0 |
| `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction check-manifest` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base a0cd39e` | 0 |
| live, co-collected: `test_live_disclosure_still_diverges` + `test_v3_statement_coverage.py` + `test_live_scenario_matches_repark_golden_and_spark` | 0 — 218 passed in 145.85 s |

## 8. The question this unit hands back

**RULING — `V3-COV-3`.** The registry's `V3-FILEORDER-1` states the engine's file-order rule
unqualified: *ascending partition value … applied once per commit*. V3-11 pinned that on the
writers RePark owns (MERGE, CTAS) and it holds there. It does **not** hold on a delegated
partitioned `INSERT`, which runs inside the fork's `iceberg_datafusion::IcebergTableProvider` and
which V3-11 did not measure: twelve runs of one statement on one seed produced two different
`_row_id` mappings. No row appears or disappears and no other probe moves, so by the unit brief's
definition this is not the row-set wrong answer that HALTs; it is filed DECLARED with a dated
reason and a fork TRIGGER, and it is raised here rather than left as a quiet row because it
narrows a claim a FIXED row already makes. **Lean:** keep `V3-COV-3` DECLARED, add the fork item
beside F-20 (`F-v3-10-partition-file-order` is the adjacent order question), and do not block the
v1.0 tag on it — `_row_id` stability inside one engine is not a §3 gate row and the value is
spec-valid either way. Countervailing view, stated so the owner can take it: a v1.0 that promises
v3 row lineage arguably owes a *stable* `_row_id` on its most common write path, in which case
this becomes a fork blocker rather than a residual.

```
COVERAGE_ATTESTATION:
  pr_unit: v3-cov-statement-coverage
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The inventory was derived from the SQL-door grammar maps and the CALL map, not invented; every statement class those maps list has at least one program, and the seven procedures each have one.
      artifacts: [docs/design/v3-statement-coverage.md, python/repark/tests/test_v3_statement_coverage.py]
    - id: AT-2
      status: ATTACKED
      evidence: All 80 programs were run on both engines and all 255 cells compared before any value was pinned; the 8 cells that first diverged were each re-read to separate a harness artefact (4, repaired) from an engine divergence (7 kept).
      artifacts: [python/repark/tests/_v3_statement_coverage_golden.py, task/ledgers/staging/v3-cov-statement-coverage-ledger.md]
    - id: AT-3
      status: ATTACKED
      evidence: The two repairs were watched red first — the hunks were reverted, the extension rebuilt, both pins observed failing, then the hunks restored and the pins observed green.
      artifacts: [crates/repark-iceberg/src/write/partition_overwrite.rs, crates/repark-iceberg/src/catalog/lineage_columns.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The live session is module-scoped, records getActiveSession() before getOrCreate() and stops only a session it created; the catalog name is module-private; PYSPARK_SUBMIT_ARGS is untouched and no per-call Ivy cache is made.
      artifacts: [python/repark/tests/test_v3_statement_coverage.py]
    - id: AT-5
      status: ATTACKED
      evidence: No .github, IAM, secret or dependency change. Warehouses are per-test temp directories removed in a finally block, and the orphan-sweep program runs against a namespace with an explicit location so the shared CTAS fallback guard is not the thing being measured.
      artifacts: [python/repark/tests/test_v3_statement_coverage.py]
    - id: AT-6
      status: ATTACKED
      evidence: An unstable value is refused a pin rather than pinned at one observation — partitioned rows pin _last_updated_sequence_number and the _row_id instability is filed as V3-COV-3 with its own cell and a stable CTAS control.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_v3_statement_coverage.py]
    - id: AT-7
      status: ATTACKED
      evidence: Both repairs are single-column conforms on paths that already ran per column; the matrix's live half runs in 75 s of Spark time and the co-collected suite in 2 min 25 s.
      artifacts: [crates/repark-iceberg/src/write/partition_overwrite.rs, crates/repark-iceberg/src/catalog/lineage_columns.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml or lockfile change; the fork pin ff4764d3 was read, not moved, and the two fork-routed rows carry TRIGGERs instead of a repin.
      artifacts: [Cargo.toml, docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: Divergence semantics went to the registry and state to STATUS; V3-COV-3 narrows V3-FILEORDER-1 in place by naming the scope that row states unqualified rather than contradicting it silently.
      artifacts: [docs/spark-sql-iceberg-parity.md, STATUS.md]
    - id: AT-10
      status: ATTACKED
      evidence: STATUS stayed under the ceiling at 24,882 B by replacing the owed-item line rather than appending; every touched map.md moved in the same commit and this ledger files last.
      artifacts: [STATUS.md, task/ledgers/staging/map.md, python/repark/tests/map.md]
```
