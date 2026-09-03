# Unit ledger — V3-COV · full v3 statement coverage against PySpark

**Date:** 2026-09-03 · **Branch:** `feat/v3-cov-statement-coverage` · **Base:** `origin/main` `a0cd39e` ·
**Model:** claude-opus-5 (medium) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md) ·
**Registry:** [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
`V3-COV-1`…`V3-COV-6` · **Path:** STANDARD (`risk_tier: standard`; two small Rust repairs, one new
live harness, docs).
**Gate:** [../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
§2 pillar 4 — discharged here.
**Matrix:** [../../../docs/design/v3-statement-coverage.md](../../../docs/design/v3-statement-coverage.md).

**Retired:** filed here by `ledger_lifecycle move` in commit `a6901c0`.

## ERRATA 2 (2026-09-03, critic remediation round 2 — this block wins over ERRATA 1 and §1–§8)

Every finding below was re-measured on the live oracle before it was written. `V3-COV-8` was
stated backwards in six places; the goldens were right all along and no golden value moved.

| # | Sev | Finding | Fix |
|---|---|---|---|
| E-7 | S1 | `V3-COV-8`'s direction was inverted everywhere prose stated it — the registry heading, its `repark` / `Apache Spark` bullets and its Rationale, `docs/design/v3-statement-coverage.md` §3 and §4, `E-3` above and §3's divergence table, and the north star's surface-residuals row 6 all read "repark optional / Spark required". **Re-measured on the raw metadata JSON, both engines, 2026-09-03:** repark's CTAS writes `{"name": "id", "required": true, "type": "long"}` and `{"name": "name", "required": true, "type": "string"}`; Spark's writes `{"name": "id", "required": false, "type": "int"}` and `{"name": "name", "required": false, "type": "string"}`. repark derives **wider and required**; Spark derives **the literal's width and optional** | all six places corrected to the measured direction. The Rationale is re-derived rather than negated: the open question is which default the create path keeps — Spark's optional-by-default derivation or repark's required — and SE-1's `ctas.rs` R-D refusal already refuses an Iceberg CREATE carrying a non-nullable field from a tighten-derived source, so `required` is the state that path treats as load-bearing and relaxing the derivation moves what that guard sees. The width half (`long` vs `int`, the `TY-4` root) is unchanged. The `[create-v3-flat]` control is unaffected and still measures `("id", "int", False)` on **both** engines, so the divergence is the CTAS derivation, not the type mapping |
| E-8 | S2 | the surface-residuals row 6 justification invented a broader requires cell ("the cell asks that a v3 table be creatable behind the opt-in and read back …"); §3's row 6 cell reads, verbatim, "stays opt-in until V3-3; default remains v2". Row 9's justification paraphrased its cell the same way | both restated from the cells' actual words. Row 6: the cell reads "stays opt-in until V3-3; default remains v2" — a stamped engine default and the CTAS schema derivation touch neither the opt-in gate nor the default format version. Row 9: the cell reads "full DML including UPDATE/MERGE, round-tripped" — this `DELETE` does round-trip on both engines and only the storage shape differs. `test_v1_gate_docs.py` now pins both quoted cell texts as `_RESIDUAL_JUSTIFICATIONS`, so the invented ask cannot come back |
| E-9 | S2 | stale `80` from before `alter-replace-partition-field` was added (E-4): `format-v3-track.md`'s dated Step 6 discharge line, `C-001` ("80 rows"), `C-002` ("160 parametrised tests (80 always-run, 80 live)") and three lines of `python/repark/tests/map.md` | all corrected to the measured 81 programs — `C-001` 81 rows, `C-002` 162 parametrised tests (81 always-run, 81 live). `test_v3_cov_docs::test_the_v3_track_step_6_carries_the_v3_cov_state_line` now asserts the program total from `_TOTALS` appears in the track line, so the two cannot drift apart again. `P-1`'s "80 sessions and 255 cells" is left as written: it states the pre-remediation tree, which had 80 | **Re-sweep (round 3, 2026-09-03):** three sites were missed and are now 81 as well — `task/roadmap/epic-term/map.md` (§2 pillar 4 line), `docs/design/map.md` (the matrix row) and the inventory module docstring `python/repark/tests/_v3_statement_coverage_programs.py:1`; the Step-6 count pin now also reads both maps.
| E-10 | S2 | `V3-COV-7`'s control claim was false as written — Spark stamps `write.parquet.compression-codec = zstd` on **every** create including the three named controls, whose `META` tuples do not read the property set at all | the claim is narrowed to what the controls actually probe: they agree on format version, current schema and partition spec, and are explicitly NOT a control on the codec key. The row's scope is restated as every `CREATE`, with the measured evidence: `[ctas-v3]`'s metadata carries `write.parquet.compression-codec = zstd` on Spark and no `write.*` key at all on repark |
| E-11 | S2 | two multi-line docstrings landed in `4b731ee` against the no-comment ruling — `_agrees` and `test_v3_statement_row_matches_the_live_spark_oracle` | each reduced to one line; the rationale for both already lives in `python/repark/tests/map.md` |
| E-12 | S3 | three weak spots in the new pins and one in the harness | `test_every_diverging_row_names_a_registry_row_that_exists` now rejects an EMPTY registry cell (it passed before, because `"" != "—"` and `" —"` is in the registry) and anchors on the row's own `^#+ <row> —` heading; the `_SURFACE_RESIDUALS` row-id match is a word-boundary regex, so `V3-COV-8` no longer matches inside a hypothetical `V3-COV-88`; `_latest_metadata` sorts on `(st_mtime, name)` so two pointers written inside one mtime tick order deterministically |
| E-13 | S3 | the streaming static-partition injection (`R-1`) moved where a bad batch fails — mid-stream instead of before anything is staged | recorded in `crates/repark-iceberg/src/write/map.md`: a refused batch now leaves staged data files no commit references. The `OverwriteFiles` commit is still all-or-nothing and the table state is untouched either way, so the residue is orphan files for `remove_orphan_files`, not a partial overwrite |
| E-14 | S3 | STATUS headroom | STATUS.md is 24,882 B against the 25,000 B ceiling in `scripts/check_docs_compaction.py` — 118 B. Nothing was added to it in this round, and nothing should be added to it without removing at least as much |

**Mutation battery, round 2 — 18 red of 18.** The nine `test_v3_cov_docs.py` mutations and the
four `test_v1_gate_docs.py` residual mutations of §6 were re-run against the corrected documents,
and five were added for the new pins: an EMPTY `V3-COV-8` registry cell (the case that used to
pass), a cited row with no `#+ <row> —` heading, the Step 6 program count put back to 80,
`V3-COV-88` in the row 6 residual cell (the word-boundary case), and the invented row 6 ask and
the paraphrased row 9 ask restored. Each was applied alone and reverted; every one was red.

**Gates, round 2 — all 0.** `make preflight`, `make verify`, `make py-test` (554 passed),
`make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction check-manifest`,
`python3 scripts/ledger_lifecycle.py check --base a0cd39e`, the always-run half (84 passed,
81 skipped) and the live sandwich co-collected — 220 passed in 119.36 s. No golden value moved
in this round: the matrix totals of §3 stand as measured.

## ERRATA (2026-09-03, critic remediation — where this block contradicts §1–§8 below, it wins)

The Critic verdict was FAIL on bookkeeping and test adequacy; every matrix value reproduced and
both red-first proofs were confirmed. Each finding is fixed on the branch and the totals below are
re-measured, not adjusted.

| # | Sev | Finding | Fix |
|---|---|---|---|
| E-1 | S1 | `V3-COV-4` and `V3-COV-5` were BACKLOG rows named only in the discharge prose and sat in neither north-star audit bucket | both are rows in §3.1's surface-residuals table with the measured reason each is outside its requires cell (delete-all MoR: both engines return `[]` rows and identical time travel, only storage shape differs; `WRITE ORDERED BY`: no §3 row covers sort-order evolution), and `_SURFACE_RESIDUALS` in `test_v1_gate_docs.py` holds all three new keys |
| E-1b | S2 | the first cut of E-1's pin was itself vacuous: `_SURFACE_RESIDUALS` matched `\| <row> \|` anywhere in §3.1, and §3's own audit table carries rows named `9 · Write: MoR DML via deletion vectors` and `6 · Write: create v3`, so deleting the residual row left the pin green | the pin now extracts the surface-residuals table and requires exactly one matching row inside it; found by mutating the new keys rather than by reading |
| E-2 | S2 | `delete-all-rows-mor` did not probe `t.files`, the table carrying its own central claim | `_P_FILES` added; measured repark `[(0, 4), (1, 4)]` against Spark `[]` — the data files RePark keeps live and Spark drops |
| E-3 | S2 | the four `create` rows compared nothing about the table they created | every create row now carries a metadata-facts probe (format version, current schema, partition fields, and the `write.*` properties for the properties row) read from the table's own metadata JSON. **This found two divergences the row had been hiding:** `V3-COV-7` (Spark stamps `write.parquet.compression-codec = zstd`) and `V3-COV-8` (repark's CTAS derives `id: long, required` where Spark derives `id: int, optional`; the direction was stated backwards here and is corrected in ERRATA 2 / E-7) |
| E-3b | S2 | `_agrees` compared values only for the `OK` kind, so the new metadata cells would have been accepted unread | the comparator now exempts **only** a mutual refusal from the value check; every other cell kind is compared, so a kind cannot be added and silently never checked |
| E-4 | S2 | completeness overreach — `branch-replace-and-drop` ran no REPLACE, `REPLACE PARTITION FIELD` was absent, and one bare arm per `CALL` was not stated | the branch row runs `CREATE OR REPLACE BRANCH`, `alter-replace-partition-field` is a new program, and §6 of the matrix document states the one-arm scope with a table naming where each unrun argument IS covered (and that `expire_snapshots`'s options are covered nowhere) |
| E-5 | S3 | `V3-COV-3`'s TRIGGER did not name the fork item | it names fork F-20 / `F-v3-10-partition-file-order` at the next RP-8 repin, as STATUS already did |
| E-6 | S3 | "filed in the last commit" was false | corrected above: filed by `a6901c0`; every later commit on the branch amends documents, not the filing |
| P-1 | perf | the live cell re-ran the whole repark half — 80 sessions and 255 cells for an answer the always-run sibling already pins | it compares `REPARK[name]` against the live Spark half, so the duplicate pass (a full repark pass measures 12.7 s standalone) is gone. **No speedup is claimed from the wall clock**: the post-remediation sandwich measured 144.54 s and then 117.40 s on the same 267-cell tree against 142.18 s pre-remediation at 255 cells, so the run-to-run spread is wider than the saving and attributing one to the other would be an unmeasured claim |
| P-2 | perf | `_snapshot_marks` and `_latest_metadata` ran for every program (74 metadata scans per engine for 3 consumers; an `rglob` over the shared Spark warehouse) | both are gated on `NEEDS_SNAPSHOT_MARKS` / `NEEDS_METADATA_PATH`, computed once from the program text at import, and the pointer lookup is a `glob` of `*/<stem>/metadata/*.metadata.json` |
| P-3 | perf | `CREATE NAMESPACE IF NOT EXISTS` per program; uncached document reads in the docs meta-pin | the namespace moves into the `coverage_session` fixture; `_read` and `_matrix_rows` are `functools.cache`d |
| R-1 | perf | the two repairs had no Rust-level test, and `write::conform::conform_batch` — the bulk-append hot path — still cast on every column | five unit tests added across the two touched modules (the two behaviour pins watched red against the pre-fix bodies), the identity fast path is backported to `conform_batch`, the static-partition bindings are hoisted into a `StaticPartitionPlan` built once per commit and the injection streams, and the lineage projection is resolved once per scan schema instead of a name scan per field per batch |


## 1. Scope, as checkable propositions

| ID | Proposition | Verdict | Evidence |
|---|---|---|---|
| C-001 | An inventory document holds one row per served statement class and per `CALL system.*` procedure, derived from the grammar maps | **PROVEN** | `docs/design/v3-statement-coverage.md` §3, 81 rows; `test_v3_cov_docs.py` counts them from §3 |
| C-002 | A parametrised harness runs each row on repark and on the live oracle against the same v3 seed | **PROVEN** | `python/repark/tests/test_v3_statement_coverage.py` + `_v3_statement_coverage_golden.py`; 162 parametrised tests (81 always-run, 81 live) plus 3 fixed cells |
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
| Statement programs | 81 across 12 groups (create · insert · delete · update · merge · alter · lifecycle · metadata · lineage · time travel · refs · call) |
| Comparison cells | 267 (statements + probes) |
| EQUAL | 71 |
| REFUSED (both engines refuse) | 1 — `create-v3-write-order`, a parse error on both |
| DIVERGES | 9 |
| Statement classes unmeasured | 0 |
| Runtime | repark 13 s; live Spark 70 s; co-collected with the nightly live legs 1 min 57 s |

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
| `create-v3-properties` | `CREATE TABLE … TBLPROPERTIES (…)` | stores the three `write.*` keys the DDL set | adds `write.parquet.compression-codec = zstd` | `V3-COV-7` | **BACKLOG** |
| `ctas-v3` | `CREATE TABLE … AS SELECT 1 AS id, 'a' AS name` | derives `id: long, required` | derives `id: int, optional` | `V3-COV-8` | **BACKLOG** |

## 4. The two defects fixed here, red first

| Row | Measured red on `a0cd39e` | Fix | Green |
|---|---|---|---|
| `V3-COV-1` | `INSERT OVERWRITE t PARTITION (part = 10) SELECT …` → `column types must match schema types, expected Utf8 but found Utf8View`; the `VALUES` spelling of the same statement worked, which is why DML-B never saw it | `crates/repark-iceberg/src/write/partition_overwrite.rs::store_assign_source_column` — `refuse_unless_write_store_assignable`, then a strict (`safe: false`) cast | pin red before, green after (reverted the hunk, rebuilt, watched both pins fail, restored) |
| `V3-COV-2` | `ALTER TABLE t ALTER COLUMN id TYPE BIGINT` then a `_row_id` projection → `lineage scan could not rebuild batch: expected Int64 but found Int32`, while `SELECT id, name` on the same table promoted correctly | `crates/repark-iceberg/src/catalog/lineage_columns.rs::conform_batch` — strict cast when the scan type differs from the declared field | same red-first proof |

Neither fix widens a contract: both apply the store-assignment / conform rule the sibling path
already applied. Each now carries Rust-level pins in its own module —
`partition_overwrite::tests::store_assign_conforms_a_view_string_source_to_its_utf8_target` and
`…::store_assign_is_identity_on_a_match_and_refuses_a_non_assignable_pair`;
`lineage_columns::tests::conform_batch_promotes_a_narrower_scan_column_to_the_declared_type`,
`…::conform_batch_reuses_the_projection_across_batches_of_one_schema` and
`…::conform_batch_names_a_column_the_scan_did_not_return` — so the repairs are not held by the
Python matrix alone.

## 5. What is deliberately not pinned, and what is outside the matrix

`_row_id` on a partitioned seed. `V3-COV-3` makes it unstable, so the partitioned programs pin
`_last_updated_sequence_number` (deterministic and Spark-equal on every measured run) and the
instability has its own two cells —
`test_v3_partitioned_insert_row_id_mapping_is_one_of_two_measured_orders` (the two measured
permutations plus the invariant that the block is `[0, 1, 2, 3]`) and the incidental control
`test_v3_ctas_partitioned_row_id_mapping_is_stable_and_spark_ordered`, which shows the
RePark-owned CTAS writer stable and Spark-ordered on every run. Pinning an unstable value would be
the false green the registry exists to prevent.

**One arm per row.** Each `CALL system.*` program runs the procedure's bare form. The optional
arguments are outside this matrix and the coverage document's §6 names, per procedure, where each
IS covered — `rewrite_data_files` `where` / `strategy` / `sort_order` by MAINT-rewrite-data-files-options
and `RDF-SORT-1`; `rewrite_manifests` `spec_id` / `rewrite_if` / `use_caching` by MW-6 and
`MANIFEST-1/2/3`; `remove_orphan_files`'s real sweep by `ORPHAN-1/2` — and where one is covered
**nowhere**: `expire_snapshots`'s `stream_results` / `snapshot_ids` / `max_concurrent_deletes`,
recorded as owed rather than implied. `RENAME TO`, `REPLACE TABLE` and `ALTER TABLE … EXECUTE`
are likewise outside the matrix and said to be.

## 6. Mutation battery — 13 red of 13

Nine on `test_v3_cov_docs.py`, four on the extended `test_v1_gate_docs.py` residual pin. Each
applied alone and restored.

| Test | Mutation |
|---|---|
| `…_matrix_row_count_and_verdicts_match_the_stated_totals` | delete the `ctas-v3` matrix row |
| `…_every_stated_total_appears_in_the_totals_table` | change the program total from 81 to 82 |
| `…_every_diverging_row_names_a_registry_row_that_exists` | blank `V3-COV-8`'s registry cell |
| `…_the_rows_this_unit_filed_carry_a_class_a_date_and_a_pin` | strip `V3-COV-1`'s pin lines |
| `…_the_fork_routed_rows_name_a_trigger` | drop `TRIGGER:` from `V3-COV-6` |
| `…_the_north_star_carries_the_measured_discharge` | soften "Nothing in §2 pillar 4 is now owed." |
| `…_the_v3_track_step_6_carries_the_v3_cov_state_line` | soften "Step 6 now owes **no engineering item**" |
| `…_the_harness_and_the_golden_carry_every_matrix_row` | rename `merge-mixed-arms` in the doc only |
| `…_the_rows_an_existing_registry_row_covers_are_cited_not_refiled` | renumber `V3-COV-4` to `V3-COV-9` |
| `test_v1_gate_docs::…_audit_is_scoped_to_the_v1_0_requires_cells` | delete the row 9 residual row |
| " | delete the row 6 residual row |
| " | delete the sort-order residual row |
| " | rename `V3-COV-8` inside the row 6 residual cell |

The two Rust behaviour pins were watched red the same way, against the pre-fix bodies rather than
against a deleted function: `store_assign_source_column` reduced to `Ok(Arc::clone(source))` and
the lineage cast branch reduced to pushing the column unchanged — both new tests failed, both
passed on restore.

## 7. Gates

| Gate | Exit |
|---|---|
| `make preflight` | 0 |
| `make verify` | 0 |
| `make py-test` | 0 |
| `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction check-manifest` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base a0cd39e` | 0 |
| live, co-collected: `test_live_disclosure_still_diverges` + `test_v3_statement_coverage.py` + `test_live_scenario_matches_repark_golden_and_spark` | 0 — 220 passed in 117.40 s |

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
      evidence: All 81 programs were run on both engines and all 267 cells compared before any value was pinned; the 14 programs that first diverged were each re-read rather than accepted - 2 defects with a local fix, 3 that were the one V3-COV-3 instability, 2 harness artefacts (a two-file Spark seed and a content-derived snapshot id) and 7 engine divergences kept.
      artifacts: [python/repark/tests/_v3_statement_coverage_repark.py, python/repark/tests/_v3_statement_coverage_spark.py]
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
      evidence: Both repairs are single-column conforms on paths that already ran per column; the matrix's live half runs in 70 s of Spark time and the co-collected suite in 1 min 57 s.
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
