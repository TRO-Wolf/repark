# Unit ledger — U6 WRITE-REFUSALS PR1 · refusal parity on accept-any-schema tables

**Date:** 2026-09-24 · **Branch:** `feat/u6-write-refusals` · **Base:** `72a1b8c0`
(`origin/main`) **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Owner ruling 2026-09-24 (override): the seven refusal cells get refusal parity — the
same class and message shape as Spark 4.1.2 + Iceberg 1.11 — not the removal of the capability
behind a flag. Seven scoreboard cells answered in RePark where Spark refuses, and
`W-INSERT-MERGE-SCHEMA-CONF` answered with the wrong schema.

**What Spark does (measured 2026-09-24, `target/probe-u6/spark-probe.json`, 102 probes).** A table
with `write.spark.accept-any-schema=true` skips Spark's output resolution, so Iceberg resolves every
append and overwrite **by name**, on every door: SQL `INSERT … VALUES`, `INSERT … SELECT`,
`DataFrame.write.insertInto` and `saveAsTable`. A source column with no table column of that name
refuses `IllegalArgumentException: Field <name> not found in source schema` (no condition, no
SQLSTATE), naming the **first unknown source column** in source order — `col1` for a `VALUES`
tuple, `9` / `z` for unaliased literals. With `spark.sql.iceberg.merge-schema=true` the write
instead unions the source schema into the table (new columns last and optional) and writes by
name. `MERGE WITH SCHEMA EVOLUTION` proposes a type change to the source's type for every
matched column whose type differs, and Iceberg refuses a non-promotable change with
`Cannot change column type: <col>: <from> -> <to>`; a MERGE with only `DELETE` clauses does not
evolve.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | A positional `INSERT … VALUES` (append, `TABLE` keyword, short tuple, `INSERT OVERWRITE`) into an accept-any-schema table refuses `IllegalArgumentException: Field col1 not found in source schema` with no condition and no SQLSTATE, and writes nothing. | Exact class + condition + SQLSTATE + message pins on both the Rust door and the facade. | PROVEN | `test_positional_values_seed_refuses_with_the_first_values_column`; Rust `positional_values_into_accept_any_refuses_with_the_values_column_name`. Cells `W-ACCEPT-ANY-INSERT-VALUES`, `W-MERGE-SCHEMA-EVOLUTION-PROP`, `W-MERGE-SCHEMA-EVOLUTION-PROP-BIGINT`, `W-DF-OPT-MERGE-SCHEMA`, `W-DF-OPT-MERGE-SCHEMA-ICEBERG`, `TP-ACCEPT-ANY-SCHEMA-DF` replay EQUAL. |
| C-002 | A positional `INSERT … SELECT` refuses naming the first source output column the table lacks, spelled as Spark spells it: an unaliased integer or string literal is named by its text (`Field 9 …`, `Field z …`). | Pins for the literal, mixed, wrong-name and extra-column shapes. | PROVEN | `test_positional_select_refuses_with_the_first_unknown_output_name`; Rust `positional_select_into_accept_any_refuses_with_the_literal_name`, `syntactic_names_use_the_spark_literal_spelling`. |
| C-003 | The DataFrame door resolves by name on the same tables: `insertInto` of a frame with other names refuses `Field a …`, `saveAsTable` refuses `Field d …`, and a matching or column-short frame appends (missing column NULL). | Facade pins on both writers plus the Rust view shape `INSERT … SELECT * FROM view`. | PROVEN | `test_dataframe_writes_resolve_by_name_on_accept_any`; Rust `a_view_with_other_names_refuses_on_its_first_unknown_column`. |
| C-004 | The near misses keep working: matching aliases (case-insensitive, a subset filled NULL), an explicit column list, a `VALUES … AS v(cols)` source, and every positional write on a table without the property. | Row and schema pins. | PROVEN | `test_matching_names_and_property_less_tables_keep_writing`; Rust `matching_names_still_write_by_name_on_accept_any`, `positional_values_without_the_property_stay_positional`. |
| C-005 | With the property and `spark.sql.iceberg.merge-schema=true`, positional `VALUES` adds `col1 int`, `col2 string`, `col3 string` after the table's columns and writes the tuple there; the table's own columns read NULL. | The `W-INSERT-MERGE-SCHEMA-CONF` schema, rows and snapshot count. | PROVEN | `test_merge_schema_conf_adds_the_values_columns`; Rust `merge_schema_conf_adds_the_values_columns_after_the_table_columns`. The cell replays EQUAL on the whole observation (rows, schema, snapshots, layout). |
| C-006 | Under the conf the union always runs, not only when a column is new: a wider source widens (`col1 int → long`), a narrower one leaves the table alone, a partial source adds its new column, `INSERT OVERWRITE` evolves then replaces, and a non-promotable type refuses `Cannot change column type: id: long -> string` as `IllegalArgumentException`. | One pin per measured probe (g3, g4, g5, g6, g9). | PROVEN | `test_merge_schema_conf_widens_overwrites_and_refuses_like_spark`; Rust `merge_schema_conf_widens_and_overwrites_like_spark`, `merge_schema_conf_refuses_an_incompatible_type_change`; iceberg `union_type_conflict_refuses_as_an_illegal_argument`. |
| C-007 | `MERGE WITH SCHEMA EVOLUTION` whose source column type cannot be promoted from the target's refuses `IllegalArgumentException: Cannot change column type: <col>: <from> -> <to>` — an INT source key into a BIGINT key (`id: long -> int`, also with an explicit `UPDATE SET`), an INT into a STRING column (`data: string -> int`) — and leaves schema and rows unchanged. | Exact pins at the Rust seam, the Rust door and the facade. | PROVEN | `test_merge_schema_evolution_refuses_an_incompatible_type_change`; Rust `merge_schema_evolution_refuses_narrowing_the_target_type`, `merge_schema_evolution_refuses_a_non_promotable_type`; iceberg `merge_evolution_refuses_narrowing_with_the_java_message`, `merge_evolution_refuses_a_non_promotable_change`. Cell `W-MERGE-SCHEMA-EVOLUTION` replays EQUAL. |
| C-008 | The MERGE near misses hold: a BIGINT source adds `extra int`; an INT target key is widened to BIGINT by a BIGINT source; a `DELETE`-only evolving MERGE does not evolve; a plain `MERGE INTO` with an INT source still casts and succeeds. | Schema and row pins per near miss. | PROVEN | `test_merge_schema_evolution_near_misses_still_succeed`; Rust `merge_schema_evolution_with_a_matching_source_still_adds_the_column`, `merge_schema_evolution_with_only_a_delete_clause_does_not_evolve`; iceberg `merge_evolution_widens_a_promotable_column_and_adds_the_new_one`. |
| C-009 | Positional `INSERT … VALUES` follows the property: with it (and the conf) the write evolves by name; without it the IPI-17 arity path still answers `INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS` / `21S01`. Supersedes `ipi-19-56-37-schema-evolution-write/C-013`, which Spark's measurement contradicts. | The re-pinned IPI-17 boundary test. | PROVEN | `test_positional_insert_values_follows_the_property` (replaces `test_positional_insert_values_never_evolves`). |

VERDICT (2026-09-24): 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

## Mutation record (2026-09-24)

Each load-bearing line was broken, the named tests ran red, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | `routes_positional_by_name` always false | 7 Rust door pins, including both C-001/C-002 refusal pins |
| M2 | the `Field … not found` arm never fires | `positional_values_…`, `positional_select_…`, `a_view_with_other_names_…` |
| M3 | integer literals get no Spark name | `syntactic_names_use_the_spark_literal_spelling`, `positional_select_…` |
| M4 | no column aliases on the by-name source | `merge_schema_conf_adds_the_values_columns_…`, `merge_schema_conf_widens_…` |
| M5 | the conf evolves only when a column is new | `merge_schema_conf_refuses_…`, `merge_schema_conf_widens_…` |
| M6 | MERGE issues no type change | `merge_evolution_refuses_narrowing_…`, `merge_evolution_refuses_a_non_promotable_change`, `merge_schema_evolution_refuses_narrowing_…` |
| M7 | the type-change error keeps the fork's class | three iceberg pins and three Rust door pins |
| M8 | a `DELETE`-only MERGE evolves | `merge_schema_evolution_with_only_a_delete_clause_does_not_evolve` |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: u6-write-refusals
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause comes from the 102-probe Spark measurement or the scoreboard record. The eight cells replay EQUAL, and the probe matrix moves from 52 divergences to 26 with none new.
      artifacts: [python/repark/tests/test_ice_write_refusals_1.py, crates/repark-spark/src/tests/accept_any_refusals.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Literal, mixed, short, extra, renamed, case-changed, column-list, VALUES-alias, overwrite, TABLE-keyword and view-star sources are exercised, with and without the property and the conf. MERGE runs with star, explicit, insert-only, update-only, delete-only and reordered clauses.
      artifacts: [python/repark/tests/test_ice_write_refusals_1.py, crates/repark-spark/src/insert_by_name/tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal pins class, condition, SQLSTATE and message by equality and asserts the table is unchanged. The MERGE type change fails inside the schema transaction before any data file is written.
      artifacts: [crates/repark-iceberg/src/write/schema_evolution/tests.rs, python/repark/tests/test_ice_write_refusals_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: The evolving write commits the schema, then the data, which is the existing two-transaction shape. The routing check loads the table once per positional INSERT and holds no state.
      artifacts: [crates/repark-spark/src/insert_by_name/evolution.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling. Source names flow through the existing backtick quoting.
    - id: AT-6
      status: ATTACKED
      evidence: Rows, Arrow types and snapshot counts are pinned for every evolving case. Positional writes on tables without the property are pinned unchanged, and the IPI-19 suite stays green apart from the superseded C-013.
      artifacts: [python/repark/tests/test_ice_merge_schema_1.py, python/repark/tests/test_ice_write_refusals_1.py]
    - id: AT-7
      status: N/A
      justification: No performance claim. A positional INSERT into an Iceberg table adds one table load for the property read.
    - id: AT-8
      status: ATTACKED
      evidence: The type-change rule is the fork's update_column and union_by_name_with, not a second rule. The dead append_with_evolution is gone. No new dependency or crate edge, no code comments, file sizes within the ratchet.
      artifacts: [crates/repark-iceberg/src/write/schema_evolution.rs, crates/repark-spark/src/insert_by_name/evolution.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The refusals carry Spark's class and exact text, so a Spark user reads the same diagnosis. The superseded IPI-19 clause is marked in its ledger and map.
      artifacts: [task/ledgers/staging/ipi-19-56-37-schema-evolution-write-ledger.md, python/repark/tests/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: Eight mutations, each red on a named pin (table above). Every new branch has a nameable input.
      artifacts: [crates/repark-spark/src/tests/accept_any_refusals.rs, crates/repark-iceberg/src/write/schema_evolution/tests.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Spark's other accept-any refusal family is not ported: Iceberg's `validateWriteSchema` refuses an out-of-order or non-promotable by-name source with `Cannot write incompatible dataset to table with schema: … Problems: * <col> is out of order, before <col>` (probes b2, b13, c1, d3, g8). RePark reorders and writes, or refuses with its store-assignment text. A reordered named `SELECT` refused before this unit (the positional cast failed) and now succeeds. |
| R-2 | On an accept-any table Spark 4.1.2 refuses every `MERGE` with `UNRESOLVED_COLUMN.WITH_SUGGESTION` on the target alias (probes f1-acc..f13-acc); RePark merges. |
| R-3 | A positional INSERT that carries write options other than the merge-schema keys stays on the positional path, because the by-name append refuses those options. |
| R-4 | Pre-existing on tables without the property: a wrong-arity `INSERT … SELECT` answers `Column count doesn't match insert query!`, not Spark's `INSERT_COLUMN_ARITY_MISMATCH`, and a string-into-bigint `INSERT` answers the store-assignment text, not `CANNOT_SAFELY_CAST` (probes b3, b4, b6, b13, b17, g3, g5 plain). |
| R-5 | The evolving write commits the schema before the data, so a data-side failure leaves the evolved schema. Spark validates before it commits the schema. This was already true of the IPI-19 append. |
