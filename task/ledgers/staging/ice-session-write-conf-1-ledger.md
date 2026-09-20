# Charter ledger — ICE-SESSION-WRITE-CONF-1 · session `spark.sql.iceberg.*` write confs reach every Iceberg write

**Date:** 2026-09-19 · **Branch:** `fix/ice-session-write-conf-1` · **Base:** `origin/main`
`2c232c59` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).

**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Owner direction 2026-09-18: 1:1 parity with Spark's Iceberg
integration. On main both session confs are SILENTLY IGNORED: the writer
options and the table property work, the session conf does not. Pipelines
stamp `snapshot-property.*` idempotency markers and set the session codec per
environment; a dropped conf is a silent wrong answer on every write the
session commits.

**Not in this unit:** `rewrite_data_files` session-conf honour (declared
residue `F-RDF-SESSION-CONF-1`, fork has no hook — strict xfails, not pins);
`STATUS.md` (never edited); `Cargo.toml` / `Cargo.lock` (never edited); fork
pin moves (rule: pin moves only via their own PR).

## Rulings recorded

- Q-24c-5 (orchestrator, 2026-09-19): an option-less plain INSERT reroutes
  through the positional stage branch (empty column list means positional over
  all columns, Spark's semantics); the by-name append branch stays for
  option-carrying form. The positional `VALUES` case that failed is pinned.
- Q-24c-6 (orchestrator, 2026-09-19): `rewrite_data_files` is a DECLARED
  residue — the fork's `RewriteDataFiles` takes no writer/snapshot properties.
  `SP-CALL-RDF` and `CZ-CONF-RDF` stay strict xfails with the dated reason,
  named in the registry row. No table-property overlay.
- R-24c-1 (this lane, 2026-09-19): every decision lands in Rust
  (`write/session_write_conf.rs` resolver, `StatementWriteOptions` merge, the
  router fold). Python forwards key strings only (`is_iceberg_session_write_key`
  is a spelling predicate, not a decision).
- R-24c-2 (this lane, 2026-09-19): ICE-WRITE-OPTIONS-1 needs no correction —
  its SQL-door sentence names the non-existent
  `spark.sql.iceberg.write.snapshot-property.*` spelling, which stays absent on
  both engines. This unit's conf is `spark.sql.iceberg.snapshot-property.*`.

## PROPOSITION LEDGER — ICE-SESSION-WRITE-CONF-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The 25-cell Spark 4.1.2 + Iceberg 1.11.0 oracle is recorded with provenance and SHA-256. | Fixture file + recorder. | PROVEN | `ice_session_write_conf_1_spark_oracle.json` (provenance: `spark-pc1.json` SHA-256 `f1e732ca…`, fixture SHA-256 `bf96b4b9…`); recorder `_record_ice_session_write_conf_1_oracle.py`. |
| C-002 | The recorder re-derives the fixture on live Spark (`record` prints JSON, `check` compares). | Live-leg pin. | PROVEN | `test_live_oracle_fixture_reproduces` (C-032 live leg runs the recorder `check`, rc 0). |
| C-003 | SQL INSERT stamps `team=a` on its append snapshot. | SP-INSERT pin vs fixture. | PROVEN | `test_sp_insert_stamps_session_team` — ops+keys and rows equal the cell. |
| C-004 | Two conf keys both land on the snapshot. | SP-TWO-KEYS pin vs fixture. | PROVEN | `test_sp_two_keys_stamps_both` — `team` and `run` both stamped. |
| C-005 | `writeTo(t).append()` stamps `team=a`. | SP-DF-APPEND pin vs fixture. | PROVEN | `test_sp_df_append_stamps_session_team`. |
| C-006 | `saveAsTable` append stamps `team=a`. | SP-DF-SAVEASTABLE pin vs fixture. | PROVEN | `test_sp_df_saveastable_stamps_session_team`. |
| C-007 | Writer option `snapshot-property.team=opt` wins over conf `team=a`. | SP-DF-OPTION-WINS pin vs fixture. | PROVEN | `test_sp_df_option_wins_over_conf` — summary carries `opt`. |
| C-008 | COW DELETE stamps `team=a` on append and overwrite. | SP-DELETE-COW pin vs fixture. | PROVEN | `test_sp_delete_cow_stamps_overwrite` — both snapshots stamped. |
| C-009 | MoR DELETE stamps `team=a` on append and delete. | SP-DELETE-MOR pin vs fixture. | PROVEN | `test_sp_delete_mor_stamps_delete`. |
| C-010 | UPDATE stamps `team=a` on append and overwrite. | SP-UPDATE pin vs fixture. | PROVEN | `test_sp_update_stamps_overwrite`. |
| C-011 | MERGE stamps `team=a` on append and overwrite. | SP-MERGE pin vs fixture. | PROVEN | `test_sp_merge_stamps_overwrite`. |
| C-012 | INSERT OVERWRITE stamps `team=a` on both snapshots. | SP-OVERWRITE pin vs fixture. | PROVEN | `test_sp_overwrite_stamps_overwrite`. |
| C-013 | CTAS stamps `team=a` on the new-table snapshot. | SP-CTAS pin vs fixture. | PROVEN | `test_sp_ctas_stamps_session_team`. |
| C-014 | `rewrite_data_files` takes no session confs (residue F-RDF-SESSION-CONF-1); the pin is a strict xfail that reds if the fork ever honours them. | SP-CALL-RDF strict xfail + registry residue. | PROVEN | `test_sp_call_rdf_keeps_stamps` xfail-strict with the dated reason; registry row names the residue. |
| C-015 | `rollback_to_snapshot` makes no new snapshot. | SP-ROLLBACK pin vs fixture. | PROVEN | `test_sp_rollback_makes_no_snapshot` — snapshot count unchanged. |
| C-016 | `snapshot-property.operation` never overrides the engine operation. | SP-OPERATION pin vs fixture. | PROVEN | `test_sp_reserved_operation_untouched`. |
| C-017 | Bare-dotted SQL SET applies the conf to a later INSERT (RePark door; Spark refuses it `INVALID_SET_SYNTAX`). | SP-SET-SQL pin vs fixture. | PROVEN | `test_sp_set_sql_applies_team`. |
| C-018 | An empty conf value stamps `""`. | SP-EMPTY pin vs fixture. | PROVEN | `test_sp_empty_value_stamps_empty`. |
| C-019 | INSERT on v3 stamps `team=a` like v2. | SP-INSERT-V3 pin vs fixture. | PROVEN | `test_sp_insert_v3_stamps_session_team`. |
| C-020 | `writeTo` append on v3 stamps `team=a` like v2. | SP-DF-APPEND-V3 pin vs fixture. | PROVEN | `test_sp_df_append_v3_stamps_session_team`. |
| C-021 | COW DELETE on v3 stamps `team=a` like v2. | SP-DELETE-COW-V3 pin vs fixture. | PROVEN | `test_sp_delete_cow_v3_stamps_overwrite`. |
| C-022 | MoR DELETE on v3 stamps `team=a` like v2. | SP-DELETE-MOR-V3 pin vs fixture. | PROVEN | `test_sp_delete_mor_v3_stamps_delete`. |
| C-023 | Conf `gzip` sets the DataFrame append codec. | CZ-CONF-GZIP-DF pin vs fixture. | PROVEN | `test_cz_conf_gzip_df_writes_gzip` — footer codec GZIP. |
| C-024 | Conf `gzip` sets the SQL INSERT codec. | CZ-CONF-GZIP-SQL pin vs fixture. | PROVEN | `test_cz_conf_gzip_sql_writes_gzip`. |
| C-025 | Conf `snappy` sets the SQL INSERT codec. | CZ-CONF-SNAPPY pin vs fixture. | PROVEN | `test_cz_conf_snappy_sql_writes_snappy`. |
| C-026 | Upper-case `GZIP` conf sets the codec. | CZ-CONF-UPPER pin vs fixture. | PROVEN | `test_cz_conf_upper_writes_gzip`. |
| C-027 | Conf `snappy` wins over table property `gzip`. | CZ-CONF-BEATS-PROP pin vs fixture. | PROVEN | `test_cz_conf_beats_table_property` — footer SNAPPY. |
| C-028 | Writer option `snappy` wins over conf `gzip`. | CZ-OPTION-BEATS-CONF pin vs fixture. | PROVEN | `test_cz_option_beats_conf`. |
| C-029 | Bogus codec refuses naming the codec with no snapshot committed. | CZ-BOGUS pin vs fixture. | PROVEN | `test_cz_conf_bogus_refuses_naming_codec` — error names `bogus`, snapshot count unchanged. |
| C-030 | Conf `gzip` sets the rewritten-file codec on COW DELETE. | CZ-CONF-DELETE pin vs fixture. | PROVEN | `test_cz_conf_delete_cow_rewrites_gzip`. |
| C-031 | `rewrite_data_files` output codec takes no session conf (residue F-RDF-SESSION-CONF-1); the pin is a strict xfail that reds if the fork ever honours it. | CZ-CONF-RDF strict xfail + registry residue. | PROVEN | `test_cz_conf_rdf_writes_gzip` xfail-strict with the dated reason; registry row names the residue. |
| C-032 | The live tier re-derives the fixture under `REPARK_PARITY_LIVE=1`. | Live recorder `check`, rc 0. | PROVEN | `test_live_oracle_fixture_reproduces` (live-only; skipped offline). |
| C-033 | The Rust resolver layers writer option over session conf over table property; a bogus session codec refuses naming the codec; unset clears each shape. | Carrier unit battery. | PROVEN | `crates/repark-iceberg/src/tests/session_write_conf.rs` — 11 tests green (`statement_snapshot_wins_over_session`, `bogus_session_codec_refuses_naming_the_codec`, `unset_clears_each_shape`, `builder_map_installs_carrier`). |
| C-034 | The Spark door stamps the session conf on plain INSERT (positional `VALUES`), COW DELETE overwrite, refuses a bogus session codec, and unstamps after unset. | Spark-door battery. | PROVEN | `crates/repark-spark/src/tests/session_write_conf.rs` — 4 tests green (`session_team_stamps_plain_insert`, `session_team_stamps_cow_delete_overwrite`, `bogus_session_codec_refuses_naming_the_codec`, `unset_session_conf_restores_unstamped_writes`). |
| C-035 | (orchestrator local gate, 2026-09-19) The live recorder re-derives all 25 cells, and the fork-committed `UPDATE` is a registered strict xfail. | `_record_ice_session_write_conf_1_oracle.py check` clean on PySpark 4.1.2; `test_sp_update_stamps_overwrite` `xfail(strict)` under `F-RDF-SESSION-CONF-1`. | PROVEN | The gate found the recorder reading `operation` from the summary map (Spark keeps it in its own column), a `_df_one_row` arity bug, an empty rdf short name and order-sensitive `data`; all fixed. `SP-UPDATE` failed offline: a plain UPDATE commits through the fork's DataFusion DML without session properties. |
| C-036 | The 38 `QS-*` / `QZ-*` / `QR-*` path cells recorded on Spark 4.1.2 + Iceberg 1.11.0 (run 25c, 2026-09-19) join the unit fixture with provenance and SHA-256, so the fixture is 63 cells. | Fixture file + the recorder module that holds the statement tuples. | PROVEN | `ice_session_write_conf_1_spark_oracle.json` (63 cells; provenance and SHA-256 in `python/repark/tests/map.md`); statements in `_record_ice_session_write_conf_1_paths.py`, which both the recorder and the pins import, so a pin and its re-recording cannot drift. |
| C-037 | The live recorder re-derives ALL 63 cells, including the one path cell that has no SQL spelling. | `check` rc 0 naming the cell count. | PROVEN | `_record_ice_session_write_conf_1_oracle.py … check` prints `ice-session-write-conf-1 oracle check clean (63 cells)` on PySpark 4.1.2. Round 1 left `QS-BRANCH-DF-APPEND` (a DataFrame `writeTo("t.branch_b").append()`) out of `derive_path_cells`, so `check` failed 62 vs 63 before comparing anything; round 2's `_run_branch_df_append_cell` derives it as the pin drives it. `test_live_oracle_fixture_reproduces` green live. |
| C-038 | Critic P1-1 and the plain-`UPDATE` residue: with the session conf set every remaining snapshot-property write head is owned and stamps like Spark — branch `INSERT` / `DELETE` / `UPDATE` / `MERGE` in copy-on-write and merge-on-read, merge-on-read `UPDATE` / `MERGE`, and a plain `UPDATE … SET … WHERE <scalar comparison>`. | Each `QS-*` cell vs the fixture, plus `SP-UPDATE`. | PROVEN | `test_session_property_path_matches_spark` over every `QS-*` id (`QS-DELETE-PART-META` a dated IPI-08 xfail, named in the registry rationale); `test_sp_update_stamps_overwrite` is a pin again, not the round-1 xfail. Routing: `write_to_branch.rs` keeps the ref-qualified name, `PredicateDmlSpec` carries the branch, `spark_ast.rs::plain_identity_or_update` claims the plain UPDATE. |
| C-039 | A DataFrame `writeTo("<table>.branch_b").append()` reaches the engine and stamps the conf on the branch head. | `QS-BRANCH-DF-APPEND` pin vs the fixture. | PROVEN | `test_branch_dataframe_append_stamps_session_team` — both snapshots, both refs, main data and branch data equal the Spark cell. `writer_layout.table_of_ref_target` strips the selector for the writer's existence probe (spelling only; the Rust router still decides, and refuses a tag). |
| C-040 | Critic P1-3: a v2 position-delete file takes its codec in Iceberg's `SparkWriteConf` order — writer option > session conf > `write.delete.parquet.compression-codec` > `write.parquet.compression-codec` > default — and a v3 deletion vector is puffin and takes no codec. | Each `QZ-*` cell vs the fixture. | PROVEN | `test_session_codec_path_matches_spark` over every `QZ-*` id (`QZ-POSDEL-*`, `QZ-BRANCH-*`, `QZ-TRUNCATE-NOFILES`), comparing delete-file codecs, all-file codecs and summaries. Before the round `position_delete_writer_properties_for` read the DATA-file property only, so the delete-codec property was silently ignored too. |
| C-041 | Critic P1-6: every owned commit site builds the engine summary the fork's producer would build, refuses an ACTUAL collision with Spark's `IllegalArgumentException` text, and stamps a key the engine did not produce — feeding the totals as Java's producer does. | `QR-*` cells plus the `RV-*` replay. | PROVEN | `test_summary_key_collision_matches_spark` over every `QR-*` id; harness replay of `cells_pc4.py` (`RV-INSERT`, `RV-MERGE`, `RV-DELETE-MOR`) equal 3 of 3. The blanket metric-key refusal is the WRONG rule and stays reverted (`549074ce`): it broke ICE-WRITE-OPTIONS-1's recorded `COLL-*` cells. |
| C-042 | Critic P1-2: the native `repark.sql` door answers the session write conf exactly as the facade door does. | Native-door Rust battery. | PROVEN | `crates/repark-sql/src/session_write_conf.rs` — 8 tests green (INSERT / CTAS / MERGE / DELETE / merge-on-read DELETE stamping, the collision refusal with no snapshot committed, the free-key totals, the bogus-codec refusal). The native session carries its own `ConfigOptions`, which the Python facade cannot reach, so this claim can only be pinned in Rust. |
| C-043 | A four-part `<catalog>.<ns>.<table>.branch_<name>` DML target is identity DML on that branch; a `tag_<name>` selector and an empty `branch_` both decline, so peeling a selector cannot widen into a tag write. | Parser battery. | PROVEN | `crates/repark-iceberg/src/write/predicate_dml/tests/plain.rs` — `branch_selector_delete_is_identity_dml_on_the_branch` (asserts `branch == Some("b")`), `tag_selector_delete_is_not_plain_identity`, `empty_branch_selector_delete_is_not_plain_identity`. Round 1 changed this behaviour and left the pre-round assertion red; round 2 replaced it with the claim the round makes. |
| C-044 | Critic P2-4: dropping the summary extras at EITHER `merge/snapshot_commit.rs` commit arm reds a Rust pin. | Run the mutation. | PROVEN | Both arms mutated to `summary_with_extras(&[], &engine)`: `session_team_stamps_cow_delete_overwrite` (repark-spark) plus `native_merge_*` and `native_delete_*` (repark-sql) fail. The row-delta arm alone reddened NOTHING after round 1, so round 2 added `native_merge_on_read_delete_stamps_the_session_snapshot_property` on a `write.delete.mode=merge-on-read` table; that mutation now reds it and nothing else. |

## 1. Red-first record (base `2c232c59`, 2026-09-19)

`test_ice_session_write_conf_1.py` step-1 commit `9f343679`: 30 pins — 26 red
where the session confs are ignored, 3 green controls (already-equal cells),
C-032 live-only. First failure shape: `KeyError: 'team'` on the stamped
summary read. The carrier batteries (`C-033`, 11 tests) and the Spark-door
battery (`C-034`, 4 tests) were written against the new resolver alongside the
fix; the positional `VALUES` pin failed first with the by-name resolve error
(`append batch is missing column id`) and drove ruling Q-24c-5.

## 2. Spark oracle recording (PySpark 4.1.2, Iceberg 1.11.0, 2026-09-18)

`ice_session_write_conf_1_spark_oracle.json`, 25 cells (16 SP + 9 CZ),
`local[1]`, InMemoryCatalog `sc`, HadoopCatalog `hc` for the footer cells,
copied from the orchestrator recording `spark-pc1.json`; provenance and
SHA-256 in the fixture and in `python/repark/tests/map.md`. Measured:

- C-003..C-013: `spark.sql.iceberg.snapshot-property.team=a` stamps `team=a`
  on EVERY snapshot the session commits (INSERT, `writeTo` append,
  `saveAsTable` append, COW DELETE/UPDATE/MERGE overwrites, MoR DELETE delete,
  INSERT OVERWRITE, CTAS). Two keys both land; empty value stamps `""`.
- C-007/C-028: writer option wins over conf; C-027: conf wins over table
  property. C-016: `snapshot-property.operation` does not override `operation`.
  C-015: `rollback_to_snapshot` makes no snapshot.
- C-023..C-026/C-030: `spark.sql.iceberg.compression-codec=gzip` (also
  `snappy`, also upper-case `GZIP`) sets the parquet codec of every data file
  the session writes, including a COW DELETE's rewritten file. C-029: `bogus`
  fails the write with Spark's `Unsupported compression codec: bogus`.
- SP-CALL-RDF / CZ-CONF-RDF: the `rewrite_data_files` `replace` commit and its
  output files ignore both confs — the declared residue (Q-24c-6).
- Format-version 3 twins carry v3 expectations equal to the v2 Spark answers.

## 3. Implementation (2026-09-19)

- `crates/repark-iceberg/src/write/session_write_conf.rs` (new):
  `SessionWriteView` (session codec/level plus the snapshot-property map),
  `resolve_write_for_session` (writer option over session conf over table
  property; bogus codec refuses naming the codec), the `from_config_map` /
  `from_ctx` / `from_options` readers, `with_session_write_conf`.
- `crates/repark-iceberg/src/write/merge/session_staging.rs` (new, split out
  of `mod.rs` 1761 → 1701): the session-conf-aware staged-write entry every
  MERGE writer site stages through; `snapshot_commit.rs` stamps the resolved
  write; `row_lineage.rs` honours it on the fanout.
- `crates/repark-iceberg/src/write/predicate_dml/cow_commit.rs` (new, split
  out of `predicate_dml.rs` 1139 → 1034): the COW identity commits resolve the
  session write at the commit site.
- `crates/repark-spark`: `StatementWriteOptions` merges the live session conf;
  `execute_inner` folds it into every Iceberg write arm; append, overwrite,
  CTAS, by-name and plain INSERT resolve the merged write at their commits.
- `crates/repark-core`: the builder installs the carrier from its config map;
  the statement funnel merges the session conf into the statement options.
- Facade (`builder_conf.py`, `session_configuration.py`,
  `session_runtime.rs`): the `spark.sql.iceberg.*` keys forward through the
  native setter — Rust validates and applies; Python parses nothing.

## 4. Round-1 fix (Q-24c-5, Q-24c-6, 2026-09-19)

- Q-24c-5: the session-conf reroute sent option-less plain INSERT through the
  by-name append, which refused positional `VALUES` (`append batch is missing
  column id`). The empty-column-list arm now stages positionally
  (`stage_overwrite_files_with` with an empty list) when the options map is
  empty; the by-name arm stays for the option-carrying form. Pin:
  `session_team_stamps_plain_insert` (was red, now green); the `insert`
  batteries stay green (101 passed).
- Q-24c-6: no table-property overlay for `rewrite_data_files`. `SP-CALL-RDF`
  and `CZ-CONF-RDF` are strict xfails with the dated
  `F-RDF-SESSION-CONF-1` reason; the registry row names the residue.

## 5. Round 1 — the verification critic's four P1s and two P2s (run 25c, 2026-09-19)

A Grok verification critic on draft PR #733 found five silent gaps on paths the
first pass never reached. All are closed; the 38 `QS-*` / `QZ-*` / `QR-*` cells
recorded on Spark 4.1.2 (run 25c) pin each closure (C-036 … C-042, C-044).

- **P1-1 branch writes** (`984ca08c`). A branch `INSERT` / `DELETE` / `UPDATE`
  was rewritten onto the fork's temp provider, which takes neither snapshot
  properties nor a codec, so every branch snapshot lost the conf. With the conf
  set those heads stay owned: the statement keeps its ref-qualified name,
  `PredicateDmlSpec` carries the branch, the identity `DELETE` / `UPDATE` scan
  the ref's snapshot and commit `to_branch`, the ref-selector scan stops pinning
  a `DELETE` target as a read, and the DataFrame writer probes the table a ref
  hangs off. C-038, C-039, C-043.
- **P1-3 position-delete codecs** (`5820e536`). The writer resolved its codec
  from the DATA-file property only, so both the session conf AND Iceberg's own
  `write.delete.parquet.compression-codec` were silently ignored on every
  merge-on-read delete file. Resolution is now `SparkWriteConf`'s order, with the
  staging threaded from the commit site into the row-delta preparation; a v3
  deletion vector is puffin and takes no codec. C-040.
- **P1-6 the summary-key collision rule** (`619ca5d9`). The MERGE / DML commits
  now build the engine summary the fork's producer would build and refuse an
  ACTUAL collision instead of silently replacing the engine value; a free key is
  stamped and feeds the totals as Java does. The refusal is
  `IllegalArgumentException`-class with Spark's message verbatim. C-041.
- **P1-2 the native `repark.sql` door** (`f8276b12`). Its plain `INSERT` reached
  the fork's DataFusion `insert_into` and its CTAS committed through the staged
  transaction, so neither carried the session write while the same session's
  `MERGE` and `DELETE` did — two doors, two answers on one conf. C-042.
- **The plain-`UPDATE` residue** (`c1b11764`). `try_allowed_plain_update` reuses
  the identity-UPDATE body with a plain scalar-comparison predicate, so a plain
  `UPDATE` is owned when the conf is set. `SP-UPDATE` stops being an xfail. C-038.
- **P2-5 TRUNCATE / metadata-only delete** — resolved by MEASUREMENT, not code:
  Spark stamps neither and refuses a colliding key at neither, and RePark already
  matched. The one exception is `QS-DELETE-PART-META`, where RePark's
  whole-partition `DELETE` rewrites data files and therefore stamps: a
  DELETE-routing divergence owned by IPI-08, carried as a dated xfail.

## 6. Round 2 — finish the unit (2026-09-19)

Round 1 ran out of turns mid-step and left three things red that no one had run.

- **The uncommitted edit.** `commit_overwrite` and
  `commit_row_delta_kind_with_partitions` lost their last production caller when
  step 2a threaded `branch` through every commit site. They take `#[cfg(test)]`,
  which `commit_row_delta_kind` and `CommitScope::unscoped` already carry in that
  file, so the shipped lib stops building two unreachable wrappers.
- **A red Rust pin.** `branch_selector_delete_is_not_plain_identity` asserted the
  pre-round contract that a four-part branch target is NOT identity DML — which
  is exactly what round 1 changed. Replaced by the claim the round makes, with
  two negatives (a `tag_` selector, an empty `branch_`) so peeling a selector
  cannot widen into a tag write. C-043.
- **A red live leg.** `derive_path_cells` walks statement tuples, and
  `QS-BRANCH-DF-APPEND` is the one path cell with no SQL spelling, so the recorder
  re-derived 62 of 63 cells and `check` failed on the count. C-037.
- **P2-4, measured rather than assumed.** Mutating both `snapshot_commit.rs`
  commit arms reds three pins; mutating the row-delta arm ALONE reddened nothing,
  so every merge-on-read stamping claim lived in the Python facade pins.
  `native_merge_on_read_delete_stamps_the_session_snapshot_property` closes that.
  C-044.

**Out of scope, measured and reported, not fixed here:** replaying the original
`SP-*` cells showed that after a copy-on-write `DELETE` RePark's table metadata
JSON lists the `snapshots` array out of creation order (`overwrite` before
`append`) where Java appends in order. The `snapshot-log` IS ordered and the
`<table>.snapshots` metadata table reads correctly ordered by `committed_at`, so
no pin in this unit sees it — only a consumer reading the raw array positionally
would. It belongs to the fork's `TableMetadata` builder and wants its own registry
row; it is recorded in this unit's registry row because this is where it was
measured.

## 7. Registry

Row `ICE-SESSION-WRITE-CONF-1` in `docs/spark-sql-iceberg-parity.md`, beside
ICE-WRITE-OPTIONS-1: **FIXED 2026-09-19**, with every critic P1 and P2 recorded
closed and three named residues (`F-RDF-SESSION-CONF-1`, `IPI-08`, the
`engine-name` / `engine-version` reading). No correction to ICE-WRITE-OPTIONS-1
was needed (R-24c-2).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-session-write-conf-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every pin expectation reads from the committed Spark 4.1.2
        oracle cells, per enumerated door (facade SQL, DataFrame, native
        repark.sql, v2 and v3 twins); the live tier re-derives all 63 cells and
        the recorder check is clean. No RePark-only answer was written into a
        pin. Round 1 added the 38 QS / QZ / QR path cells recorded on run 25c;
        round 2 closed the one cell the recorder could not re-derive.
      artifacts: [python/repark/tests/ice_session_write_conf_1_spark_oracle.json, python/repark/tests/test_ice_session_write_conf_1.py, python/repark/tests/test_ice_session_write_conf_1_paths.py, python/repark/tests/_record_ice_session_write_conf_1_paths.py]
    - id: AT-2
      status: ATTACKED
      evidence: Empty conf value, two keys at once, upper-case codec, bogus
        codec, unset-restores-unstamped, and the reserved operation key are all
        pinned. The pins were red on the base where the confs were ignored.
        Round 1 adds the edges the critic found: a branch head, a v3 deletion
        vector that takes no codec at all, a TRUNCATE and a metadata-only delete
        that neither engine stamps, a summary key the engine also produced
        (refuse) beside one it did not (stamp and feed the totals), and a free
        key whose total would go negative (dropped, not written). Round 2 adds a
        tag selector and an empty branch_ selector, which must decline.
      artifacts: [python/repark/tests/test_ice_session_write_conf_1.py, python/repark/tests/test_ice_session_write_conf_1_paths.py, crates/repark-iceberg/src/tests/session_write_conf.rs, crates/repark-iceberg/src/write/predicate_dml/tests/plain.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The bogus codec refuses before any file is written with no
        snapshot committed, on both SQL doors. A colliding summary key refuses
        with Spark's message and commits NO snapshot. The two RDF cells are
        strict xfails, so the suite reds if the fork ever honours the confs
        there, and QS-DELETE-PART-META is a dated IPI-08 xfail that flips to a
        pin the day RePark's whole-partition DELETE routes as Spark's does.
      artifacts: [python/repark/tests/test_ice_session_write_conf_1.py, python/repark/tests/test_ice_session_write_conf_1_paths.py, crates/repark-sql/src/session_write_conf.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The conf is read per statement from the live session context;
        set applies to later writes only and unset restores unstamped writes.
        Each pin cell sets and unsets its own confs in a finally block, so cells
        cannot leak state into each other. The native repark.sql door carries its
        OWN ConfigOptions, which the facade cannot reach, so its pins build the
        door session directly rather than assuming one shared carrier.
      artifacts: [crates/repark-spark/src/tests/session_write_conf.rs, crates/repark-sql/src/session_write_conf.rs, python/repark/tests/test_ice_session_write_conf_1.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no deserialization and
        no path handling — the conf keys and values are plain strings stamped
        into snapshot summaries.
    - id: AT-6
      status: ATTACKED
      evidence: Every pin asserts the committed rows equal the recorded Spark
        rows alongside the summaries and codecs; the v3 twins assert the v2
        Spark answers on v3 tables. The branch cells additionally assert the
        BRANCH data and the ref-to-snapshot map, so a write that stamps
        correctly but lands on the wrong head still reds.
      artifacts: [python/repark/tests/test_ice_session_write_conf_1.py, python/repark/tests/test_ice_session_write_conf_1_paths.py]
    - id: AT-7
      status: N/A
      justification: The per-statement conf merge walks the set keys only; no
        loop, cache, or growth scales with data size.
    - id: AT-8
      status: ATTACKED
      evidence: The fork's RewriteDataFiles takes no writer or snapshot
        properties, so honour there is not presumed — it is a declared residue
        with strict xfails and a written fork ask. The bogus-codec refusal names
        the codec with Spark's text. Where RePark's summaries cannot carry a JVM
        engine identity, the QR-ENGINE-NAME reading is stated in the registry
        rather than papered over, and the metadata snapshots-array ordering
        divergence round 2 measured is reported rather than silently fixed
        inside this unit.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_ice_session_write_conf_1.py, python/repark/tests/test_ice_session_write_conf_1_paths.py]
    - id: AT-9
      status: ATTACKED
      evidence: The refusal names the codec, so the failure is diagnosable
        from the error alone; stamped keys are readable on every snapshot
        summary the session commits.
      artifacts: [crates/repark-iceberg/src/write/session_write_conf.rs]
    - id: AT-10
      status: ATTACKED
      evidence: The Q-24c-5 reroute flipped the positional VALUES pin from red
        to green while the insert batteries stayed green, so the suite catches
        both the by-name misroute and a positional regression. Both new router
        arms have nameable inputs: list-free option-less VALUES takes the
        positional stage, option-carrying INSERT keeps the by-name append.
        Round 2 RAN the mutation the critic's P2-4 asks about rather than
        asserting it: dropping the summary extras at both merge/snapshot_commit.rs
        commit arms reds session_team_stamps_cow_delete_overwrite, native_merge_*
        and native_delete_*, and mutating the row-delta arm alone reddened
        nothing until native_merge_on_read_delete_stamps_the_session_snapshot_property
        was added — which that mutation now reds.
      artifacts: [crates/repark-spark/src/tests/session_write_conf.rs, crates/repark-spark/src/append_with_options.rs, crates/repark-sql/src/session_write_conf.rs]
  reattested: [AT-1, AT-2, AT-3, AT-4, AT-6, AT-8, AT-10]
  complete: true
```
