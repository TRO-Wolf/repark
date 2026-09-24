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
| C-006 | Under the conf the union always runs, not only when a column is new: a wider source widens (`col1 int → long`), a narrower one leaves the table alone, the partial source `SELECT 9 AS id, 'z' AS newc` adds `newc string`, a positional `INSERT OVERWRITE … VALUES` evolves then replaces, and a non-promotable type refuses `Cannot change column type: id: long -> string` as `IllegalArgumentException`. The spelling of an added column is C-010; the `BY NAME` overwrite door is C-011. | One pin per measured probe (g3, g4, g5, g6, g9). | PROVEN | `test_merge_schema_conf_widens_overwrites_and_refuses_like_spark`; Rust `merge_schema_conf_widens_and_overwrites_like_spark`, `merge_schema_conf_refuses_an_incompatible_type_change`; iceberg `union_type_conflict_refuses_as_an_illegal_argument`. |
| C-007 | `MERGE WITH SCHEMA EVOLUTION` whose source column type cannot be promoted from the target's refuses `IllegalArgumentException: Cannot change column type: <col>: <from> -> <to>` — an INT source key into a BIGINT key (`id: long -> int`, also with an explicit `UPDATE SET`), an INT into a STRING column (`data: string -> int`) — and leaves schema and rows unchanged. | Exact pins at the Rust seam, the Rust door and the facade. | PROVEN | `test_merge_schema_evolution_refuses_an_incompatible_type_change`; Rust `merge_schema_evolution_refuses_narrowing_the_target_type`, `merge_schema_evolution_refuses_a_non_promotable_type`; iceberg `merge_evolution_refuses_narrowing_with_the_java_message`, `merge_evolution_refuses_a_non_promotable_change`. Cell `W-MERGE-SCHEMA-EVOLUTION` replays EQUAL. |
| C-008 | The MERGE near misses hold: a BIGINT source adds `extra int`; an INT target key is widened to BIGINT by a BIGINT source; a `DELETE`-only evolving MERGE does not evolve; a plain `MERGE INTO` with an INT source still casts and succeeds. | Schema and row pins per near miss. | PROVEN | `test_merge_schema_evolution_near_misses_still_succeed`; Rust `merge_schema_evolution_with_a_matching_source_still_adds_the_column`, `merge_schema_evolution_with_only_a_delete_clause_does_not_evolve`; iceberg `merge_evolution_widens_a_promotable_column_and_adds_the_new_one`. |
| C-009 | Positional `INSERT … VALUES` follows the property: with it (and the conf) the write evolves by name; without it the IPI-17 arity path still answers `INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS` / `21S01`. Supersedes `ipi-19-56-37-schema-evolution-write/C-013`, which Spark's measurement contradicts. | The re-pinned IPI-17 boundary test. | PROVEN | `test_positional_insert_values_follows_the_property` (replaces `test_positional_insert_values_never_evolves`). |
| C-010 | Under the conf, a column the write adds keeps the source's own spelling, and a source name matches a table column case-insensitively. Measured and pinned shapes: a positional `SELECT 9 AS id, 'z' AS NewC` (plain and backquoted alias) adds `NewC`; `BY NAME SELECT 9 AS id, 'z' AS data, 1 AS NewC` and `BY NAME SELECT 9 AS ID, 'q' AS Cat, 1 AS NewC` add `NewC int`; the first arm of `SELECT 9 AS id, 'z' AS NewC UNION ALL SELECT 8, 'y'` names `NewC`; the reference `SELECT id, NEWC FROM v` adds `NEWC`; the positional and `BY NAME` `INSERT OVERWRITE … SELECT 9 AS id, 'z' AS NewC` add `NewC`; `SELECT 9, 'z'` adds `9 int`, `z string`; a frame with a `NewC INT` column adds `NewC int` through `writeTo(...).option("mergeSchema","true").append()`, `saveAsTable` with `mergeSchema`, and `insertInto` under the conf. `SELECT 9 AS ID, 'z' AS Data` (with or without the conf) and a frame `ID, Data` add nothing and write into `id`, `data`. | Metadata schema names pinned per shape on the Rust door and the facade. | PROVEN | Rust `merge_schema_conf_adds_a_new_column_in_the_source_spelling`, `a_mixed_case_source_name_matches_the_existing_column`, `source_from_clause_aliases_the_leftmost_select_with_the_resolved_names`; facade `test_merge_schema_conf_adds_a_new_column_in_the_source_spelling`, `test_dataframe_writes_add_a_new_column_in_the_frame_spelling`. Probes a1–a5, a6b, a6c, a7–a9, d1–d4, g8, g11 (`target/probe-u6-r1fix/`). |
| C-011 | `INSERT OVERWRITE t BY NAME SELECT 9 AS id, 'z' AS data, 'q' AS cat, 1 AS extra` on an accept-any table refuses `IllegalArgumentException: Field extra not found in source schema` without the conf and leaves the rows alone; with the conf it adds `extra int` and replaces the rows with `[9, z, q, 1]`. | Rust + facade pins, both arms. | PROVEN | Rust `overwrite_by_name_with_an_extra_column_follows_the_conf`; facade `test_overwrite_by_name_with_an_extra_column_follows_the_conf`. Probes b1, b2 (the critic's x3, x4). |
| C-012 | The refusal names the unknown column as the source spells it: `… 1 AS EXTRA` answers `Field EXTRA not found in source schema` on `INSERT INTO … BY NAME`, `INSERT OVERWRITE … BY NAME`, positional `INSERT INTO … SELECT` and positional `INSERT OVERWRITE … SELECT`. | One upper-case pin per door. | PROVEN | Rust `an_unknown_upper_case_alias_is_named_in_its_source_spelling`; facade `test_an_unknown_upper_case_alias_keeps_its_spelling`. Probes c1–c4. |
| C-013 | On an accept-any table, with or without the conf, a repeated source name refuses before any evolution with Iceberg's `Invalid schema: multiple fields for name <name>: <i> and <j>` (first repeat in source order, 0-based positions; `id: 0 and 1`, `n: 1 and 2`, `data: 1 and 3`, `9: 0 and 1`), on append and overwrite, positional and `BY NAME`. A name repeated in another case that matches a table column refuses `IllegalArgumentException: Multiple entries with same key: <field id>=<later> and <field id>=<earlier>` (`1=ID and 1=id`, `2=DATA and 2=data`). Two new names equal but for case refuse `Cannot build lower case index: n and N collide` with the conf, and `Field n not found in source schema` without it. A table without the property keeps `INCOMPATIBLE_DATA_FOR_TABLE.AMBIGUOUS_COLUMN_NAME`. | Message pins per shape; the exception class is residue R-7. | PROVEN | Rust `a_repeated_source_name_refuses_as_an_invalid_schema`, `a_source_name_repeated_in_another_case_refuses_like_iceberg`, `a_repeated_name_without_the_property_stays_ambiguous`; facade `test_repeated_source_names_refuse_like_iceberg`. Probes f1–f9, g1–g7, g9, g12. |
| C-014 | The routing check does not hide a failed table load: a load failure other than table- or namespace-not-found surfaces as the load error, and nothing is written. A missing table keeps the positional path's answer. `REPLACE INTO` on an accept-any table never reaches the by-name path: the facade refuses `ParseException` (Spark: `ParseException` `PARSE_SYNTAX_ERROR`, with and without the conf), and the table is unchanged. | A catalog whose next load fails once; the missing-table and `REPLACE INTO` pins. | PROVEN | Rust `a_target_that_fails_to_load_refuses_instead_of_writing_positionally` (under the old `.ok()` the positional path reloaded and wrote the row), `a_missing_target_keeps_the_positional_answer`, `replace_into_an_accept_any_table_is_never_written`; facade `test_replace_into_an_accept_any_table_refuses_at_the_parser`. Probes g10, r1, r2. |

VERDICT (2026-09-24, critic r1 remediation): 14 clauses, 14 PROVEN, 0 OPEN, 0 REJECTED.

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
| M9 | an added column takes the case-folded name (`name.resolved`) | `merge_schema_conf_adds_a_new_column_in_the_source_spelling` (`newc` where `NewC` was expected) |
| M10 | the routing check treats every load failure as "no property" | `a_target_that_fails_to_load_refuses_instead_of_writing_positionally` |
| M11 | the refusal names the case-folded name | `an_unknown_upper_case_alias_is_named_in_its_source_spelling` |
| M12 | the duplicate-name check is skipped | `a_repeated_source_name_refuses_as_an_invalid_schema`, `a_source_name_repeated_in_another_case_refuses_like_iceberg` |

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
      evidence: Literal, mixed, short, extra, renamed, case-changed, column-list, VALUES-alias, overwrite, TABLE-keyword and view-star sources are exercised, with and without the property and the conf. MERGE runs with star, explicit, insert-only, update-only, delete-only and reordered clauses. Round 2 adds mixed-case, quoted, UNION-arm, reference and literal names; the three DataFrame writers; repeated names, exact and case-folded; the BY NAME overwrite; a failing table load and a missing table; REPLACE INTO; and the PARTITION shapes (measured, listed in R-3).
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
      evidence: Twelve mutations, each red on a named pin (table above). Every new branch has a nameable input.
      artifacts: [crates/repark-spark/src/tests/accept_any_refusals.rs, crates/repark-iceberg/src/write/schema_evolution/tests.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Spark's other accept-any refusal family is not ported: Iceberg's `validateWriteSchema` refuses an out-of-order or non-promotable by-name source with `Cannot write incompatible dataset to table with schema: … Problems: * <col> is out of order, before <col>` (probes b2, b13, c1, d3, g8). RePark reorders and writes, or refuses with its store-assignment text. A reordered named `SELECT` refused before this unit (the positional cast failed) and now succeeds. The same holds for `INSERT OVERWRITE … BY NAME` with a reordered source (critic r1 probe x5: Spark refuses `… cat is out of order, before data`; RePark overwrites). |
| R-2 | On an accept-any table Spark 4.1.2 refuses every `MERGE` with `UNRESOLVED_COLUMN.WITH_SUGGESTION` on the target alias (probes f1-acc..f13-acc); RePark merges. |
| R-3 | Three shapes stay off the by-name routing. (a) A positional INSERT that carries write options other than the merge-schema keys, because the by-name append refuses those options. (b) A positional INSERT with a `PARTITION (…)` clause (measured 2026-09-24, probes e1–e10, partitioned by `cat`), where Spark gives several answers: `INSERT INTO t PARTITION (cat='x') SELECT 9 AS id, 'z' AS data` writes `[9, z, x]`; `… SELECT 9, 'z'` refuses `Field 9 not found in source schema` (also on `INSERT OVERWRITE`), `… VALUES (9, 'z')` refuses `Field col1 not found in source schema`, and `… 1 AS extra` refuses `Field extra not found in source schema`; with the conf, `… 1 AS extra` adds `extra int`, while the literal and `VALUES` sources refuse `Cannot write incompatible dataset to table with schema: … Problems: * 9 is out of order, before cat` (`col1` for `VALUES`), which is the R-1 family. RePark answers `Partitioned inserts not yet supported` for every positional `INSERT INTO … PARTITION`, even without the property (e10: Spark writes), and a positional `INSERT OVERWRITE … PARTITION` overwrites positionally. (c) `REPLACE INTO` never parses on the Spark door: Spark refuses `[PARSE_SYNTAX_ERROR] Syntax error at or near 'INTO': missing 'TABLE'`, and RePark refuses `ParseException` with the sqlparser text (C-014 pins the class). |
| R-4 | Pre-existing on tables without the property: a wrong-arity `INSERT … SELECT` answers `Column count doesn't match insert query!`, not Spark's `INSERT_COLUMN_ARITY_MISMATCH`, and a string-into-bigint `INSERT` answers the store-assignment text, not `CANNOT_SAFELY_CAST` (probes b3, b4, b6, b13, b17, g3, g5 plain). |
| R-5 | The evolving write commits the schema before the data, so a data-side failure leaves the evolved schema. Spark validates before it commits the schema. This was already true of the IPI-19 append. |
| R-6 | Measured 2026-09-24 (probes a6, a10). RePark's planner lowercases unquoted aliases in every query output (`SELECT 'z' AS NewC` reads back as `newc`), so when the name comes from a planned source, the added column is lowercase. Spark adds `NewC` for `INSERT INTO t SELECT * FROM v`, where `v` is `SELECT 9 AS id, 'z' AS NewC`, and for `SELECT * FROM VALUES (9, 'z') AS v(id, NewC)`; RePark adds `newc`. Reading the view shows the same difference outside this door. C-010 covers the shapes whose names come from the statement's text. |
| R-7 | The exception class for Iceberg's `ValidationException` (C-013): Spark surfaces `Py4JJavaError` wrapping `org.apache.iceberg.exceptions.ValidationException: Invalid schema: …`. RePark raises `PySparkException` with `DataInvalid => Invalid schema: …`, the same message core. This matches residue 1 of ice-replace-columns-1. |
| R-8 | With `spark.sql.caseSensitive=true`, the spelling and duplicate shapes of C-010 and C-013 are unmeasured. The case-insensitive checks (`Multiple entries with same key`, `Cannot build lower case index`) are skipped under it. |
