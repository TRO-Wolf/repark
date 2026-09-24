# Unit ledger — IPI-19 + IPI-56 + IPI-37 · schema evolution on write

**Date:** 2026-09-20 · **Branch:** `fix/ipi-19-56-37-schema-evo-write` · **Base:** `e4160a58`
(`origin/main`) **Model:** Claude Opus 5 (max) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Amended by U6-WRITE-REFUSALS:** branch `feat/u6-write-refusals`, Claude Opus 5.5 (`claude-opus-5-5`), 2026-09-24. That unit moved C-013 to REJECTED (SUPERSEDED by u6-write-refusals/C-009) under the owner ruling of 2026-09-24 and Spark's measurement. The header fields above describe this unit's own branch and base.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Owner direction 2026-09-18 is 1:1 parity with Spark's Iceberg integration, and
2026-09-19 makes full Iceberg support the v1.5.0 gate. Twelve inventory cells answer in Spark and
refuse or silently mis-answer in RePark, all on one question: *what happens when the incoming
data's schema differs from the table's?* Three of them are silent-to-the-user refusals on the
DataFrame `mergeInto` builder that have nothing to do with schema evolution — Spark's own
condition form qualifies the target by its **short table name** and RePark answered
`No field named <table>.id`.

**What it is.** One decision seam in Rust: `write.spark.accept-any-schema` gates every evolving
write except `MERGE WITH SCHEMA EVOLUTION`; the incoming schema is merged into the table's with
the fork's `UpdateSchemaAction::union_by_name_with` (Java `unionByNameWith`); the schema evolves
first and the data files are written against the **evolved** schema.

**Commit shape (corrected 2026-09-20, round 2).** The schema update and the data commit ride
**two** transactions, not one: the fork's `TransactionAction` trait is `pub(crate)`, so an
external engine cannot preview the evolved schema for the data files inside a single
`Transaction`. Nothing observable changes — a schema update emits `AddSchema` + `SetCurrentSchema`
and never `AddSnapshot`, so the data commit is still the only new snapshot (measured: snapshots
grow by exactly one on both doors), which is what Spark records. The residue is atomicity between
the two commits, which Spark shares.

**Round 2 (2026-09-20).** The stopped `wip` became three commits (the A-8 spec move, §6 step 8,
§6 step 9) plus a map-sync unblocker; the snapshot-delta pins, this completion, and the `EX-DF-9`
narrowing landed here.
Model: Muse Spark 1.3 contributor. The attestation below is actor-filed; the verification critic
re-attests per the packet.

**Not in this unit:** positional `INSERT … VALUES` and `INSERT INTO t SELECT *` with extra
columns (IPI-17's `W-ACCEPT-ANY-INSERT-VALUES` / `W-INSERT-MERGE-SCHEMA-CONF`, awaiting their own
ruling); `write.spark.accept-any-schema` anywhere but as this gate (owner decision 8);
`STATUS.md`; `Cargo.toml` / `Cargo.lock`; the fork; any raised size ceiling.

**Writable paths:** `crates/repark-spark/src/`, `crates/repark-iceberg/src/write/`,
`crates/repark-python/src/session_runtime.rs`, `python/repark/src/repark/spark/`,
`python/repark/tests/`, `docs/spark-sql-iceberg-parity.md`, this ledger, touched `map.md` files.

## Measured

Spark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0` over an `InMemoryCatalog`, recorded by run
26e into `/tmp/oc-worker/qe/probe/p2.json` (keys `D.*`) and by the parity inventory into
`/tmp/oc-worker/nc-inventory/out/*.json` (the twelve cells). Every expected value in
`python/repark/tests/test_ice_merge_schema_1.py` is one of those recordings.

| # | Fact | Evidence |
|---|---|---|
| M-1 | `mergeSchema=true` REQUIRES `write.spark.accept-any-schema='true'`; without it the write fails with `[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]` / `21S01` and the table is unchanged | `D.mergeSchema.ms_plain` |
| M-2 | With the property the new column lands **last, optional**; existing rows read NULL | `D.mergeSchema` (the accept-any-schema row), `.rows` |
| M-3 | `writeTo(t).option("mergeSchema","true").append()` behaves identically, and fails identically without the property | `D.writeTo.mergeSchema` |
| M-4 | A frame **missing** a table column succeeds and writes NULL; the schema is unchanged — `mergeSchema` is *union by name*, not *replace* | `D.mergeSchema.missing` |
| M-5 | Type widening does NOT narrow the table: an INT source into a BIGINT column leaves `LongType` | `D.mergeSchema.widen` |
| M-6 | `MERGE WITH SCHEMA EVOLUTION` does NOT need the property; it adds the column with the **source's** type, unwidened (`IntegerType`), optional, last | `D.mse` |
| M-7 | `MERGE WITH SCHEMA EVOLUTION` with no new column is an ordinary upsert; the schema is unchanged | `D.mse_nonew` |
| M-8 | A plain `MERGE INTO` whose source carries an extra column **succeeds**; the extra column is ignored | `D.mse_plain_no_evo` |
| M-9 | `spark.sql.iceberg.merge-schema` only matters when the table carries the property. (no property, conf false) and (no property, conf true) → the arity error; (property, conf false) → `IllegalArgumentException: Field extra not found in source schema`; (property, conf true) → ok, `extra` `IntegerType` last | `D.sql_by_name.*` and its accept-any-schema twin |

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The DataFrame write option `mergeSchema` (and its Iceberg spelling `merge-schema`) adds the frame's extra columns to the table in **one** snapshot: optional, last, carrying the frame's own type, with existing rows reading NULL — on `saveAsTable(mode="append")` and on `writeTo(t).append()` alike. | The three DataFrame cells, schema and rows and snapshot count asserted. | PROVEN | `test_df_merge_schema_option_adds_the_column_last` (both spellings), `test_df_write_to_merge_schema_adds_the_column_last`, and the delta pin `test_df_merge_schema_commits_exactly_one_snapshot`: `EVOLVED_LONG`, `MERGE_SCHEMA_ROWS`, snapshots +1. |
| C-002 | Without `write.spark.accept-any-schema='true'` on the table an extra column refuses with Spark's exact `[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]` text and SQLSTATE `21S01`, whether `mergeSchema` is set or not, and the table is left unchanged. | M-1's pin plus the no-option control. | PROVEN | `test_merge_schema_without_accept_any_schema_raises` (exact text, `21S01`, schema/rows/snapshots unchanged), `test_extra_column_with_the_property_but_no_flag_refuses_like_java`, `test_extra_column_without_the_property_is_the_arity_error`, `test_write_to_extra_column_without_the_property_is_the_arity_error`. |
| C-003 | `mergeSchema` is union-by-name, not replace: a column missing from the frame stays in the schema and is written NULL, and a source column narrower than the table's does not narrow it. | M-4 and M-5's pins, each entering `evolve_schema` via a frame-carried extra column. | PROVEN | `test_merge_schema_missing_column_writes_null` (frame omits `cat` and adds `extra`: `cat` survives, new row `[7,g,None,14]`, schema `EVOLVED_LONG`), `test_merge_schema_does_not_narrow_a_wider_column` (INT `id` plus `extra`: `id` stays `LongType`, schema `EVOLVED_LONG`). |
| C-004 | Precedence is per-write option > session conf `spark.sql.iceberg.merge-schema` > false: an explicit `option("mergeSchema","false")` refuses even with the conf true, and the conf alone evolves. | The two precedence pins. | PROVEN | `test_merge_schema_false_with_conf_true_still_raises` (option false wins, `Field extra not found in source schema`), `test_df_merge_schema_from_the_session_conf` (conf alone evolves to `EVOLVED_LONG`). |
| C-005 | `MERGE WITH SCHEMA EVOLUTION` parses, and adds the source's new column with the **source's** type unwidened, optional, last; with no new column it is an ordinary upsert leaving the schema untouched. Both commit exactly one new snapshot. | The two cells plus a whitespace/case variant. | PROVEN | `test_merge_with_schema_evolution_bigint` (`extra` `int32`, evolved rows, 2 snapshots), `test_merge_with_schema_evolution_no_new_column` (schema unchanged), `test_merge_with_schema_evolution_is_case_insensitive_in_the_clause`, and the delta pin `test_schema_evolution_commits_one_snapshot` (snapshots +1). |
| C-006 | `MERGE WITH SCHEMA EVOLUTION` needs no table property — the asymmetry with C-002 is Spark's. | The no-property pin. | PROVEN | `test_merge_with_schema_evolution_needs_no_property` (DataFrame door refuses arity on the same plain table, MERGE evolves to `EVOLVED_INT`). |
| C-007 | A plain `MERGE INTO` is untouched by the clause sniffer: an extra source column is still ignored rather than refused, and a row whose data is literally `evolution` is not eaten. | M-8's pin and the token-boundary pin. | PROVEN | `test_plain_merge_with_extra_source_column_still_works` (M-8 upsert, schema unchanged), `test_plain_merge_named_schema_evolution_column_is_not_swallowed`, plus the sniffer unit tests (`a_plain_merge_is_left_alone`, `the_words_only_count_immediately_after_merge`, `a_column_or_value_named_evolution_survives`, `a_partial_clause_is_not_stripped`). |
| C-008 | `DataFrame.mergeInto` binds Spark's own condition form — the target by its **short table name**, the source by the frame's alias — on all five non-evolution cells. | The five cells. | PROVEN | `test_df_merge_into_upsert/delete/update_cols/not_matched_by_source/conditional_with_the_spark_qualifier`, each driving `{short}.id = s.id` through the public `mergeInto` door. |
| C-009 | `MergeIntoWriter.withSchemaEvolution()` evolves instead of refusing, and needs no table property (it lowers to `MERGE WITH SCHEMA EVOLUTION`). | The cell. | PROVEN | `test_df_merge_into_with_schema_evolution` (`EVOLVED_INT`, evolved rows, 2 snapshots on a table without the property) and `test_merge_into_with_schema_evolution_adds_the_source_column`. Owner decision 9: the no-property half is a ruling, unmeasured. |
| C-010 | RePark keeps answering the qualifiers Spark rejects: `target.` / `source.` and the bare-key sugar still resolve (owner decision 22 — narrow `EX-DF-9`, do not retire it). | The two legacy pins plus the shipped `test_examples_dataframe_b.py` batch. | PROVEN | `test_df_merge_into_keeps_the_target_source_qualifiers`, `test_df_merge_into_bare_key_sugar_still_upserts`; shipped `test_examples_dataframe_b.py` and `test_merge_semantics_audit` unchanged and green. |
| C-011 | `INSERT … BY NAME` with an extra column answers M-9's four combinations with their three distinct outcomes and **two** error classes, and the `EXTRA_COLUMNS` arm still fires for a misnamed column at or below target arity. | The matrix pin and the `EXTRA_COLUMNS` control. | PROVEN | `test_sql_insert_by_name_merge_schema_matrix` (all four combinations, exact class + SQLSTATE + message) and `test_sql_insert_by_name_extra_column_keeps_the_extra_columns_class`. *Correction (2026-09-24, u6-write-refusals critic r1):* the evolving arm added the column under its case-folded name (`1 AS NewC` added `newc`), and Spark adds `NewC`. The matrix pins a lowercase alias and never saw it. The added column now keeps the source spelling, pinned in u6-write-refusals/C-010. The overwrite door now evolves too, pinned in u6-write-refusals/C-011. |
| C-012 | The session conf reaches the write decision through both spellings — `spark.conf.set` and SQL `SET spark.sql.iceberg.merge-schema = true` — and `unset` clears it. | The SQL `SET` pin plus a post-unset refusal. | PROVEN | `test_sql_set_carries_the_merge_schema_conf` (`SET` then `conf.get`, evolution runs, explicit `unset`, post-unset `conf.get` is not `"true"`, a second `BY NAME` carrying `extra2` refuses with `Field extra2 not found in source schema` and the schema stays `EVOLVED_INT`) and every matrix pin that drives the conf through `conf.set`. |
| C-013 | Positional `INSERT … VALUES` consults neither the conf nor the property: it neither starts adding `colN` nor starts refusing with `Field col1 not found in source schema` (IPI-17 stays where it is). | The IPI-17 boundary pin. | REJECTED (SUPERSEDED by u6-write-refusals/C-009) | `test_positional_insert_values_never_evolves` (conf true + property set, still the arity path, schema unchanged). Superseded 2026-09-24: Spark 4.1.2 resolves a positional `VALUES` by name on an accept-any-schema table, refusing `Field col1 not found in source schema` without the conf and adding `col1..colN` with it (`W-ACCEPT-ANY-INSERT-VALUES`, `W-INSERT-MERGE-SCHEMA-CONF`). U6-WRITE-REFUSALS replaced the test with `test_positional_insert_values_follows_the_property`. |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ipi-19-56-37-schema-evolution-write
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause was checked against Spark's recording, not a paraphrase. The twelve cell expectations and M-1..M-9 come from run 26e's probe and the inventory harness, and every pin asserts schema names, Arrow types, row values, and snapshot counts on the collect path.
      artifacts: [python/repark/tests/test_ice_merge_schema_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: A missing source column, narrower and wider source types, a no-new-column MERGE, both option spellings, both conf spellings, unset, mixed-case and comment-split clause text, partial clauses, CTE-leading WITH, non-MERGE statements carrying the words, and a data value of literally evolution are all exercised.
      artifacts: [python/repark/tests/test_ice_merge_schema_1.py, crates/repark-spark/src/merge/schema_evolution/tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The three error shapes come from three places and are pinned by exact class, condition, and SQLSTATE. Every failure pin also asserts the table is unchanged. The schema-tx-then-data-tx gap is named above as a shared-with-Spark atomicity residue.
      artifacts: [python/repark/tests/test_ice_merge_schema_1.py, crates/repark-spark/src/insert_by_name/evolution.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The executor unions the schema first and expands stars against the evolved schema, so data files carry evolved field ids, and the evolved-row pins fail if that order flips. Conf and option reads are per-statement with no shared mutable state. No new concurrency surface.
      artifacts: [crates/repark-iceberg/src/write/merge/mod.rs, python/repark/tests/test_ice_merge_schema_1.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, no credential or secret handling, no deserialization, no path traversal. The sniffer strips four fixed keywords and aliases flow through the existing identifier quoting.
    - id: AT-6
      status: ATTACKED
      evidence: Rows, Arrow types, and snapshot counts are pinned per cell. The new column lands last and optional with the source's own type. Legacy mergeInto spellings keep working and positional INSERT VALUES is fenced off so IPI-17 cannot flip.
      artifacts: [python/repark/tests/test_ice_merge_schema_1.py, python/repark/tests/test_merge_into.py]
    - id: AT-7
      status: N/A
      justification: No performance claim. An evolving write adds one LIMIT 0 source probe and one schema transaction, and the MERGE read and write paths are unchanged.
    - id: AT-8
      status: ATTACKED
      evidence: The merge rule is the fork's union_by_name_with, not a second rule. Unknown write options stay ignored per ICE-WRITE-OPTIONS-1. No new dependency, no new crate edge, size baselines ratcheted in-commit, no code comments added.
      artifacts: [crates/repark-iceberg/src/write/schema_evolution.rs, crates/repark-spark/src/write_options.rs, scripts/check_rust_file_size.py]
    - id: AT-9
      status: ATTACKED
      evidence: The three refusal shapes carry Spark's exact class and SQLSTATE so each is diagnosable at the call site. The registry rows for the answered cells and the narrowed EX-DF-9 read true after this unit.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_ice_merge_schema_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first at the pins commit (18 failed, 9 passed controls). Every packet mutation has a named pin, including the evolved-file order, the sniffer boundaries, and the unknown-option arm. Every new branch has a nameable input across the gate combinations, the alias shapes, and the clause-strip outcomes.
      artifacts: [python/repark/tests/test_ice_merge_schema_1.py, python/repark/tests/test_merge_into.py, crates/repark-spark/src/merge/schema_evolution/tests.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | CI-1 round (2026-09-20): the `saveAsTable` by-name append refuses an extra column with the arity-mismatch code because the lowering is a named `INSERT INTO … BY NAME SELECT`, while the `writeTo(...).append()` surface is oracle-recorded as `EXTRA_COLUMNS`; whether real Spark's `saveAsTable` byName path prints the same code as `writeTo` is UNMEASURED and needs a live-Spark cell. The replaced pin carried an unmeasured oracle claim ("column number ... doesn't match the data schema"); no record for the saveAsTable surface exists in `python/repark/tests/*_spark_oracle.json` or `python/repark-parity/fixtures/**` (the `ice_rtas_byname_1` codes are SQL-surface-specific and cannot be borrowed), and no COMMON.md recorder was available in this lane, so `test_save_as_table_append_extra_column_raises` pins RePark's current `INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS` emission until that cell is measured. |
