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
| C-045 | Second verification critic, P1-OWNERSHIP-FILE-COUNT: no session write conf decides which route a statement takes, so with a conf set a plain UPDATE, a plain INSERT and a DELETE commit the same live data-file count, the same per-file record counts and the same full snapshot summaries they commit with no conf — minus only the stamp, the file sizes a codec is allowed to move, and the per-commit `engine.operation-id` nonce. | Run each statement twice over two warehouses and compare. | PROVEN | `crates/repark-spark/src/tests/session_write_conf.rs` — `a_session_conf_keeps_a_plain_{update,insert,delete}_layout`. `plain_identity_or_update` no longer takes the context; `commit_identity_update_cow` drives ONE rolling writer, so the `survivors UNION ALL new-values` batch count stops choosing the layout (2 data files → 1, which is also Spark's answer: `v3_subquery_dml.rs`'s named artefact `F_V3_8_UPDATE_FILES = 2` becomes `V3_8_UPDATE_FILES = 1`); owned appends take the fork insert exec's `parquet.enable.dictionary` rule so the owned and unowned INSERT routes write the same bytes. Mutation: restoring the `session_write_conf_is_set` gate reds the UPDATE pin on `added-files-size` and nothing else. |
| C-046 | Critic P1-NATIVE-UPDATE and P1-NATIVE-PARTITION-OVERWRITE: the native `repark.sql` door answers a plain UPDATE and an `INSERT OVERWRITE … PARTITION` exactly as the facade door does. | Native-door Rust pins, each red on the pre-round tree. | PROVEN | `crates/repark-sql/src/session_write_conf.rs` — `native_plain_update_stamps_the_session_snapshot_property`, `native_partition_overwrite_stamps_the_session_snapshot_property`, `native_partition_overwrite_takes_the_session_codec` (the last reads the written footer through DataFusion's `parquet` re-export). Both doors now ask one predicate, `plain::try_allowed_plain_identity_or_update`; `execute_partition_overwrite` takes the `*_with_summary` commits and the resolved staging. Pre-round: UPDATE stamped `None`, overwrite stamped `None`, overwrite wrote `ZSTD` where the session named `gzip`. |
| C-047 | Critic P1-REPLACE-PARTITIONS-COLLISION and P2-REPLACE-PARTITIONS-COLLISION-MESSAGE: a dynamic `overwritePartitions` builds its collision oracle from the partitions it actually replaces, so a `snapshot-property.deleted-records` into a NEW partition is a free key that stamps, and into an EXISTING one the refusal names Spark's computed value. | Measure Spark, then pin. | PROVEN | Six cells recorded on Spark 4.1.2 + Iceberg 1.11.0 (run 25c, 2026-09-19): new partition + `deleted-records=5` COMMITS and drops the negative `total-records`; existing partition refuses `Multiple entries with same key: deleted-records=2 and deleted-records=5`; an empty overwrite commits no snapshot either way. `summary_collision::replaced_data_files` filters the live files by partition-field NAME so an evolved spec still matches. Replay equal 5 of 6 — the sixth differs only in `removed-files-size` (906 against 843, two writers' bytes). Pins `replace_partitions_into_a_new_partition_stamps_deleted_records` and `replace_partitions_into_an_existing_partition_names_the_engine_value`, both red on `EngineSummary::for_overwrite`. |
| C-048 | Critic P2-CONF-KEY-CASE is refuted by measurement, and the divergence beside it is closed: Spark's Iceberg session-conf PREFIX is case-sensitive, and the `snapshot-property.` SUFFIX is carried verbatim. | Measure Spark for a mixed-case key, then pin. | PROVEN | Six `QK-*` cells (run 25c, 2026-09-19): `Spark.sql.iceberg.snapshot-property.team`, `SPARK.SQL.ICEBERG.SNAPSHOT-PROPERTY.team` and `Spark.SQL.Iceberg.Compression-Codec` are SILENTLY IGNORED by Spark, so RePark's exact match is the right rule and the Python predicate keeps forwarding nothing else; `spark.sql.iceberg.snapshot-property.TEAM` stamps `TEAM`, so the carrier stopped lowercasing the suffix. `crates/repark-iceberg/src/tests/session_write_conf.rs` — `snapshot_suffix_keeps_its_case_and_last_write_wins`, `a_mixed_case_prefix_is_not_a_session_write_key`. The WRITER OPTION suffix still lowercases: a different rule Spark reaches through a case-insensitive option map, measured separately by ICE-WRITE-OPTIONS-1. |
| C-049 | Critic P2-REWRITE-MANIFESTS-UNPINNED: `CALL rewrite_manifests` commits a replace snapshot that carries no session property and refuses no colliding key — and RePark already answers that. | Measure Spark, then pin. | PROVEN | Cells `QM-REWRITE-MANIFESTS` and `QM-REWRITE-MANIFESTS-COLLIDE` (run 25c, 2026-09-19): the replace snapshot carries no `team`, and a session `total-records=77` neither refuses nor lands — the snapshot commits the engine's `2`. Pins `rewrite_manifests_does_not_stamp_the_session_snapshot_property` and `rewrite_manifests_does_not_refuse_a_colliding_session_key`. No code changed; the residue list does not grow. |
| C-050 | Critic P2-POSDEL-CODEC-PYTHON-ONLY: reverting the delete-file codec to the data-file property alone reds a Rust pin. | Run the mutation. | PROVEN | `write::writer_props::tests::position_delete_codec_resolves_over_the_data_file_property` walks three rows where only the resolved order answers — a delete-codec property beating a data-codec property, a staging override beating both, an override beating a gzip data property. The round-1 footer pin set only `write.parquet.compression-codec` and stayed green on the old rule. Mutating `delete_compression_with` back to the data property reds the new pin. |
| C-051 | Critic P2-BRANCH-EXTRAS-PYTHON-ONLY: dropping the session extras on the BRANCH commit arms reds a Rust pin. | Run the mutation, narrowed to the branch. | PROVEN | `a_branch_insert_stamps_the_session_snapshot_property` and `a_branch_delete_stamps_the_session_snapshot_property` read the branch head's summary, not the table's current snapshot. Mutation (a) narrowed to `branch.is_some()` at `commit_append_with_summary` and at `snapshot_commit`'s on-ref arms reds exactly these two of the fourteen in the file. The INSERT pin also asserts `added-records=1`, so a reroute off the append arm reds it too. |
| C-052 | Critic P2-TRUNCATE-STAMP-PYTHON-ONLY: making TRUNCATE stamp reds a Rust pin. | Run the mutation. | PROVEN | `truncate_does_not_stamp_and_does_not_refuse_a_colliding_key` sets BOTH `team` and a colliding `deleted-records=5` and asserts the delete snapshot carries neither. Routing TRUNCATE through `commit_overwrite_replace_all_with_summary` (mutation (h)) reds it on the refusal. |
| C-053 | Critic P3-QR-SPARK-APP-ID-HOLLOW: every `QR-*` cell observes the key it is about, so a RePark that dropped the extra cannot pass. | New observation + re-record on live Spark. | PROVEN | `stamped_rows` records, per snapshot, whether the cell's OWN key landed and what `team` is. The landing is a COMPARISON, not a value, because Spark's own writer puts a per-run `spark.app.id` on every snapshot it commits — the first re-recording came back carrying `local-1789872738260`. Re-recorded on live PySpark 4.1.2 + Iceberg 1.11.0: the ONLY delta against the committed fixture is this observation on the seven `QR-*` cells that commit a snapshot; the other fifty-six are byte-identical. Fixture SHA-256 `181a56c8…`. Spark's `QR-SPARK-APP-ID` answer is `[["append", false, null], ["append", true, "a"]]` — the session property overrides Spark's own `spark.app.id` rather than colliding with it. |
| C-054 | Third verification critic, P1-STATIC-PARTITION-OVERWRITE-COLLISION: a static `INSERT OVERWRITE … PARTITION (k = v)` builds its collision oracle from the live files its row filter actually removes, so a removal-fed key is free where the partition was never written and refuses naming Spark's computed value where it was. | Measure Spark, then pin on both doors. | PROVEN | Eight `QO-*` cells recorded on Spark 4.1.2 + Iceberg 1.11.0 (run 25c, 2026-09-20): overwriting the live partition refuses `deleted-records=2`, `deleted-data-files=1` and `total-records=2`; overwriting a never-written one stamps `deleted-records=5` and `deleted-data-files=9` and only the total collides (`total-records=4`), with the stamped value feeding the totals so a negative one is dropped. `commit_overwrite_by_row_filter_with_summary` takes the plan's `StaticPartitionOverwrite` and resolves the removed set through `summary_collision::row_filter_removed_files` — the live files whose partition value equals the PARTITION equalities, matched by field name. It cannot come from the staged files: an empty source stages nothing and still clears the partition. Pins `static_partition_overwrite_of_a_new_partition_stamps_deleted_records`, `static_partition_overwrite_of_a_live_partition_names_the_engine_value`, `static_partition_overwrite_names_the_total_it_would_have_written`, `native_static_partition_overwrite_names_the_engine_value`, `native_static_partition_overwrite_stamps_a_free_removal_key`. Mutation: restoring `EngineSummary::for_overwrite` on this arm reds the three Spark-door pins and nothing else. |
| C-055 | Third verification critic, P1-INSERT-OWNERSHIP-TYPING: a session write conf changes what a commit STAMPS, never how its source types — the owned INSERT carries the delegated route's target-schema coercion on both doors. | Pin the three shapes the critic measured drifting, each red under the old materialisation. | PROVEN | The owned append planned a reconstructed `SELECT * FROM (<source>)`, which has no target, so a VALUES list unified against itself (`Inconsistent data type across values list … Was Timestamp(ns) but found Utf8`), a compound NULL lost its type and `DEFAULT` in an outer select came back a `SchemaError` instead of Spark's `UNRESOLVED_COLUMN … 42703`. The fork's insert exec cannot take the resolved overrides at the pinned rev — its commit builds the snapshot properties from a hardcoded map — so the route cannot be the delegated one; the owned route carries the delegated typing instead. `spark_ast::execute_insert_source` is `execute_passthrough` stopped one step short (same parse, same rewrites, same eager analysis, `fill_insert_plan`, the timestamp-ns passes) and executes the planned `Dml` node's INPUT; the native door does the same through `router::delegate_plan`. A branch target is planned against the base table, since DataFusion cannot resolve a 4-part name, and the commit still goes to the ref. Pins `a_session_conf_keeps_the_values_list_typing`, `a_session_conf_keeps_a_compound_null_insert`, `a_session_conf_keeps_the_default_keyword_refusal`, `native_insert_values_type_against_the_target_column`. Measured beyond the pins: with the conf gate dropped so EVERY plain Iceberg INSERT is owned, the whole repark-spark battery is green (1227 passed), including the five typing pins round 3 recorded as red under that same mutation — the routes now answer alike. The gate stays only because the owned writers still name files with a random v4 UUID where the fork uses v7 (the open item of §7), and unifying would spread that instability to every INSERT. |
| C-056 | Third verification critic, P2-COLLISION-FOLDS-VERBATIM-SUFFIX: a summary-metric name in a different case is a DIFFERENT key — it stamps beside the engine's own, and only the exact spelling refuses. | Measure Spark, then pin. | PROVEN | Three `QC-*` cells (run 25c, 2026-09-20): `snapshot-property.Deleted-Records=5` on a copy-on-write DELETE COMMITS, and the snapshot carries both `Deleted-Records=5` and the engine's `deleted-records=3`; `DELETED-RECORDS` likewise; `deleted-records` refuses. C-048 had already made the stamped suffix verbatim, but `summary_with_extras` still asked `refuse_collision` about an ascii-lowered copy, so a spelling Spark stamps was refused. It asks about the spelling it inserts. Folding stays the WRITER-OPTION rule, applied at `StatementWriteOptions::validate`. Pins `a_mixed_case_metric_suffix_is_a_different_key_and_stamps` and `the_exact_metric_suffix_still_refuses`; the folded lookup reds the first and nothing else. |
| C-057 | Third verification critic, P2-ONE-WRITER-UNPINNED-IN-UNIT: the layout battery asserts Spark's OWN committed file count, not only equality across conf on and off. | Measure Spark, then pin absolutely, then run the mutation. | PROVEN | Six `QU-*` cells (run 25c, 2026-09-20): Spark commits ONE live data file and `added-data-files=1` after the identity copy-on-write UPDATE and after the DELETE, and two live files after a second INSERT — with a session conf and without. Both runs of each layout pin now assert those numbers through `assert_spark_layout`. Mutation: restoring `concurrency_from_ctx` on `commit_identity_update_cow` (per-batch writers) moves BOTH sides of the comparison together and left the pin green before; it now reds `a_session_conf_keeps_a_plain_update_layout` and nothing else. |
| C-058 | Third verification critic, P2-ROUND3-CELLS-ABSENT-FROM-FIXTURE: every Spark cell this unit claims is in the unit fixture is in it, and the live recorder re-derives all of them. | Recorder `check` rc 0 naming the cell count. | PROVEN | Rounds 3 and 4 measured six families but left them in the orchestrator recordings, so the fixture held 63 cells while the ledger and registry claimed fifteen more. `_record_ice_session_write_conf_1_rounds::derive_round_cells` derives all 32 — `QK-*`, `QM-*`, `QP-*`, `QO-*`, `QC-*`, `QU-*` — and the oracle recorder appends them. Recorded on PySpark 4.1.2 + Iceberg 1.11.0: the 63 committed cells re-derive byte-identical, the 32 new ones match the orchestrator recordings cell for cell, and `check` prints `ice-session-write-conf-1 oracle check clean (95 cells)`. Fixture SHA-256 `b32404c71f9c964ad31206317d0823b87a2bdbca7f5718aa75051ae30f0522b6`. What RePark must answer for each is pinned in Rust, per the owner's Rust-first ruling. |
| C-059 | Fourth verification critic, P1-MOR-ROW-FILTER-REMOVED-SET-OMITS-DELETE-FILES: a static `INSERT OVERWRITE … PARTITION` on a merge-on-read table resolves the DELETE files the commit removes as well as the data files, so a session snapshot property naming a delete-side removal key refuses on the engine's value. | Five new Spark cells + refusal pins + the data-only mutation. | PROVEN | Measured first (`QD-MOR-*`, Spark 4.1.2 + Iceberg 1.11.0, run 25c 2026-09-20): a `cat`-partitioned merge-on-read table seeded (1,x) (2,x) (3,y) with `DELETE … WHERE id = 1` leaves one position-delete file in partition `x`; the static overwrite of `x` commits `removed-delete-files=1`, `removed-position-deletes=1`, `total-delete-files=0`, `total-position-deletes=0` and `deleted-records=2`, and REFUSES a session extra naming any of the three delete-side keys (`removed-delete-files=1 and removed-delete-files=9`, `removed-position-deletes=1 and 9`, `total-delete-files=0 and 1`). `live_data_files` kept only `ManifestContentType::Data`, so RePark stamped each extra beside the engine's own key. `live_files` is now the one manifest walk parameterised by content type, `live_delete_files` its delete-manifest case, and `row_filter_removed_files` runs the same partition-tuple filter over both sides. `deleted-records` stays `2`, the record count of the removed data file: a position delete does not lower it, and Spark counts it the same way. Pins `a_merge_on_read_overwrite_names_the_delete_file_it_removes`, `…names_the_positions_it_removes`, `…subtracts_the_delete_file_from_the_total`, `…names_the_records_of_the_data_file_it_removes`. Mutation: the removed set from the live DATA files alone reds the first three and nothing else in the 1237-test battery. |
| C-060 | Fourth verification critic, P2-DECIMAL-IDENTITY-PARTITION-CAST-MISSING: a static `INSERT OVERWRITE … PARTITION (k = '<literal>')` on an identity DECIMAL, DOUBLE or BOOLEAN column is planned, not refused, so the removed-set resolver runs on those Spark-legal shapes. | Six new Spark cells + refusal pins + the dropped-arm mutation. | PROVEN | Measured first (`QD-TYPE-*`): Spark 4.1.2 takes all three column types — a free key stamps `team=a`, and `snapshot-property.deleted-records=5` refuses `deleted-records=2 and deleted-records=5`, the two live rows of the partition cleared. `cast_datum` matched Boolean/Int/Long/Float/Double/Date32/Utf8/Timestamp and `_ => None`, so `PARTITION (amt = '1.50')` on `DECIMAL(10,2)` failed loud with `literal … is not assignable to … (decimal(10,2))` and C-054's resolver never ran there. `decimal_datum` takes the mantissa the arrow cast has already landed at the column's own precision and scale and builds the datum through `Datum::try_from_bytes` with the column's `PrimitiveType`, so the row-filter predicate and the partition-tuple lookup compare one typed value. DOUBLE and BOOLEAN needed no code and are pinned so the measurement is not lost. Mutation: dropping the `Decimal128` arm reds `an_identity_decimal_partition_overwrite_names_the_engine_value` and nothing else. |
| C-061 | Fourth verification critic, P2-STATIC-OVERWRITE-PINS-MISS-STAGED-FILES-REVERT: the static-overwrite battery reds when the removed set is taken from the staged files instead of the PARTITION equalities. | A pin whose source stages no file + the staged-files mutation. | PROVEN | The critic measured that `engine_summary_for_row_filter` on `replaced_data_files` left every C-054 pin green: `VALUES (9)` and `SELECT 9` both stage a file in the named partition, so the staged-file partition keys happen to match. The case that motivated the equalities resolver is the discriminator — an EMPTY source stages nothing and still clears a live partition, so a staged-files removed set resolves nothing and the session `deleted-records=5` is stamped where Spark refuses `deleted-records=2`. `an_empty_source_static_overwrite_names_the_partition_it_clears` pins the refusal. Mutation: the staged-files revert now reds this pin and the three `QD-MOR-*` delete-side pins, and nothing else; before this round it reddened nothing. |
| C-062 | Every Spark cell round 5 cites is in the unit fixture and re-derived by the live recorder. | Recorder `check` rc 0 naming the cell count. | PROVEN | `_record_ice_session_write_conf_1_rounds` derives the eleven `QD-*` cells on the live session and the oracle recorder appends them. Recorded on PySpark 4.1.2 + Iceberg 1.11.0: all 95 committed cells re-derive equal under the check leg's own rules (`CZ-CONF-BOGUS` by its needle, the error family by its first message line), and the eleven new ones match the orchestrator recording `spark-qc12.json` cell for cell. `check` prints `ice-session-write-conf-1 oracle check clean (106 cells)`. Fixture SHA-256 `56a2ca565dda8a43546f4142c6ec142ea741fd4b6e40ebd925443113c98502c4`, mirrored in `python/repark/tests/map.md`. |

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

## 7. Round 3 — the second verification critic's four P1s, six P2s and one P3 (2026-09-19)

A second Grok critic attacked the remediated head. Every finding is closed; two
of them were closed by measurement showing the critic's premise was wrong, and
one measurement narrowed a residue this unit had already declared.

- **The worst one was real and wider than filed.** `P1-OWNERSHIP-FILE-COUNT`: a
  session write conf — a codec-only one included — decided whether a plain
  UPDATE was owned, and the reroute changed the committed FILE COUNT. Ownership
  no longer reads the conf anywhere it decided a plain UPDATE, and the owned
  copy-on-write rewrite stopped letting its plan's batch count choose the
  layout. That second half also closes `F_V3_8_UPDATE_FILES`, the artefact
  V3-8 named and pinned in 2026-09-02: the UPDATE cell wrote 2 data files
  against Spark's 1, and now writes 1. C-045.
- **What did NOT work.** Routing every Iceberg plain INSERT through the owned
  append — the other way to make the conf irrelevant to the route — reds five
  pinned Spark-visible typing answers (`v3_timestamp_ns_door` VALUES widening,
  `list_null_compound` map-value widening, `write_defaults`' unresolved-DEFAULT
  error class). The owned append's materialisation does not carry the
  target-schema coercion the delegated path has. Reverted; the INSERT arm is
  closed the other way, by making the owned append write the same bytes.
  Reported, not papered over.
- **Two premises were wrong.** Spark does NOT fold an Iceberg session-conf key's
  prefix (`P2-CONF-KEY-CASE`), and Spark does NOT stamp its `rewrite_manifests`
  replace snapshot (`P2-REWRITE-MANIFESTS-UNPINNED`). Both were measured before
  anything was written. The key-case measurement did surface a real divergence
  next door: the property SUFFIX is verbatim in Spark and RePark was lowering it.
  C-048, C-049.
- **A residue narrowed.** `QM-REWRITE-DATA-FILES` shows Spark does not stamp the
  `rewrite_data_files` replace snapshot either, so only the CODEC half of
  `F-RDF-SESSION-CONF-1` is a gap. `SP-CALL-RDF`, the cell the property half
  leaned on, records a run where the rewrite was a no-op and committed no
  replace snapshot at all.

**Open, reported, not fixed here:** RePark's owned data-file writers name files
with a random UUID v4 where the fork's insert exec uses a time-ordered v7, so
the scan order of a multi-file table is not stable run to run on the owned
route. The one-line fix wants `uuid`'s `v7` feature declared in the workspace
manifest, and this brief forbids editing `Cargo.toml`. No pin in this unit
compares row order in list order across two runs, and the unit's own `_data_rows`
sorts.

## 8. Round 4 — the third verification critic's two P1s and two P2s (2026-09-20)

A third Grok critic ran every mutation red but filed four findings. All four are
closed, each measured on live Spark before anything was written.

- **`P1-STATIC-PARTITION-OVERWRITE-COLLISION` was real.** Round 3 fixed the
  collision oracle for the DYNAMIC `overwritePartitions` arm and left the STATIC
  `INSERT OVERWRITE … PARTITION` arm on `EngineSummary::for_overwrite`, where
  every removal key reads `<resolved at commit>`. Eight new `QO-*` cells show
  Spark draws the same line on both arms. The static arm resolves its removed set
  from the PARTITION equalities rather than from the staged files, because an
  empty source stages nothing and still clears the partition. C-054.
- **`P1-INSERT-OWNERSHIP-TYPING` was real, and the brief's preferred close is not
  available at the pinned rev.** The fork's insert exec builds its snapshot
  properties from a hardcoded map, so the session conf cannot ride the delegated
  path; the owned path carries the delegated TYPING instead, by planning the whole
  insert and executing the planned node's input. That also retires round 3's
  "approach that does not work": with every plain INSERT owned, the five typing
  pins that reddened then are green now, and the full repark-spark battery passes.
  The conf gate stays for one reason only — the v4/v7 file-naming item below.
  C-055.
- **Both P2s were real.** The collision lookup was still folding a suffix that
  stamping had stopped folding (C-056), and the layout battery compared conf
  against no-conf without ever asserting Spark's own number, so per-batch writers
  moved both sides together (C-057).
- **The fixture claim was false and is now true.** Fifteen round-3 cells were
  claimed to be in the unit fixture and were not. All fifteen, plus this round's
  seventeen, are in it and re-derived by the live recorder; the fixture is 95
  cells. C-058.

**Still open, unchanged from §7:** the owned writers' random v4 UUID file names
against the fork's time-ordered v7. The one-word fix wants `uuid`'s `v7` feature
declared in the workspace manifest, which this brief forbids; it is also the only
reason the INSERT route still depends on the conf.

## 9. Round 5 — the fourth verification critic's one P1 and two P2s (2026-09-20)

A fourth Grok critic ran every round-4 mutation red and filed three findings. All
three are closed, each measured on live Spark before anything was written.

- **`P1-MOR-ROW-FILTER-REMOVED-SET-OMITS-DELETE-FILES` was real.** C-054 taught
  the static arm to resolve the DATA files its row filter removes and stopped
  there. On a merge-on-read table the same commit also drops the partition's
  position-delete files, and Spark's producer counts them: `removed-delete-files`,
  `removed-position-deletes` and `total-delete-files` are engine-produced there,
  so a session property naming one must REFUSE. Five `QD-MOR-*` cells measure it;
  the resolver now walks the delete manifests beside the data manifests. C-059.
- **`P2-DECIMAL-IDENTITY-PARTITION-CAST-MISSING` was real, and the measurement
  widened it to a class.** Spark takes an identity DECIMAL, DOUBLE *and* BOOLEAN
  partition column; RePark refused only DECIMAL, and refused it loud, so C-054's
  resolver never ran on that shape. The other two are pinned beside it so the
  measurement outlives this round. C-060.
- **`P2-STATIC-OVERWRITE-PINS-MISS-STAGED-FILES-REVERT` was real and was purely a
  coverage hole.** The production path already answered the empty-source case
  correctly; nothing in the battery would have caught a revert to
  `replaced_data_files`. The pin that reds under it is the empty source that still
  clears a live partition — the case the equalities resolver exists for. C-061.

**Measured but NOT changed, and therefore stated:** the dynamic
`overwritePartitions` arm (`replaced_data_files`) and the whole-table replace-all
arm (`live_data_files`) have the SAME delete-file shape as the static arm, and
this round did not measure Spark's answer for them. They keep their data-only
removed set. The fix is the same two lines the static arm took; it is not taken
here because no cell was recorded for it, and this unit does not write an
expectation it has not measured. Round 6 or a follow-on unit should record
`QD-DYN-*` / `QD-ALL-*` and close it.

**New and reported — the oracle is ahead of the commit.** With C-059 in, all four
`QD-MOR-*` refusals match Spark. The fifth cell, the one that COMMITS, does not:
RePark's overwrite snapshot carries `total-delete-files=1` and no
`removed-delete-files`, where Spark carries `removed-delete-files=1` and
`total-delete-files=0` (harness replay: 10 of 11 equal, data rows equal). The
fork's overwrite action does not remove delete files at all — its summary
collector says as much in its own `remove_file` — so a static partition overwrite
of a merge-on-read partition leaves that partition's position-delete file live
against a data file that is gone. Reads stay correct; the delete-file count is
overstated and the file leaks. The oracle answers SPARK's number anyway, because
the refusal is the user-visible contract and stamping an extra Spark refuses is
the louder divergence. Fork ask filed in the registry row.

**Still open, unchanged from §8:** the owned writers' random v4 UUID file names
against the fork's time-ordered v7, which wants a workspace manifest edit this
brief forbids.

## 7a. Registry

Row `ICE-SESSION-WRITE-CONF-1` in `docs/spark-sql-iceberg-parity.md`, beside
ICE-WRITE-OPTIONS-1: **FIXED 2026-09-19, rounds 4 and 5 2026-09-20**, with every critic
P1 and P2 of all four rounds recorded closed and three named residues
(`F-RDF-SESSION-CONF-1`, `IPI-08`, the `engine-name` / `engine-version` reading).
No correction to ICE-WRITE-OPTIONS-1 was needed (R-24c-2).

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
  round_3_reattested:
    - id: AT-2
      status: ATTACKED
      evidence: Round 3 adds the edges the SECOND critic found. Ownership
        invariance under a conf is attacked with three statements over two
        warehouses each, comparing file counts, per-file record counts and full
        summaries. The replace-partitions family is attacked on both sides of
        the line Spark draws - a partition the overwrite does NOT replace (free
        key, stamps, negative total dropped) and one it does (refuse, naming the
        engine's own 2) - plus the empty overwrite that commits nothing. The
        conf key is attacked on the prefix AND the suffix, in four spellings.
      artifacts: [crates/repark-spark/src/tests/session_write_conf.rs, crates/repark-iceberg/src/tests/session_write_conf.rs, crates/repark-sql/src/session_write_conf.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Round 3 measured before it wrote, twice against its own
        interest. Two of the critic's findings turned out to rest on a premise
        Spark does not hold, and both are recorded that way rather than
        implemented; a third measurement narrowed a residue this unit had
        already declared. The one approach that did not work - owning every
        plain INSERT - is reported with the five pins it reds rather than left
        unsaid, and the UUID-v4 file-naming divergence it surfaced is reported
        with the manifest edit it needs and this brief forbids.
      artifacts: [task/ledgers/staging/ice-session-write-conf-1-ledger.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Every round-3 pin was run against the mutation that names it and
        confirmed to red under it AND to be the only thing that reds. The branch
        mutation was narrowed to `branch.is_some()` rather than dropping the
        extras everywhere, because the broad mutation reds four pins and proves
        nothing about branch coverage; narrowed, it reds exactly the two branch
        pins. The layout pins are checked against the restored conf gate, the
        native pins against the pre-round files, the replace-partitions pins
        against `EngineSummary::for_overwrite`, the delete-codec pin against the
        data-property-only rule, and the TRUNCATE pin against a stamping
        TRUNCATE.
      artifacts: [crates/repark-spark/src/tests/session_write_conf.rs, crates/repark-iceberg/src/write/writer_props.rs, crates/repark-sql/src/session_write_conf.rs]
  round_4_reattested:
    - id: AT-1
      status: ATTACKED
      evidence: The claim that a measurement is "recorded" is now checkable for
        every cell this unit cites. The thirty-two round-3 and round-4 cells
        (QK / QM / QP / QO / QC / QU) entered the fixture and the live recorder,
        which re-derives all ninety-five and re-derived the sixty-three older
        ones byte-identically in the same run. No round-4 expectation was
        written from reasoning: each was recorded on live Spark 4.1.2 first.
      artifacts: [python/repark/tests/ice_session_write_conf_1_spark_oracle.json, python/repark/tests/_record_ice_session_write_conf_1_rounds.py, python/repark/tests/_record_ice_session_write_conf_1_oracle.py]
    - id: AT-2
      status: ATTACKED
      evidence: Round 4 attacks the edges the THIRD critic found. The static
        partition overwrite is attacked on both sides of the line Spark draws
        (a never-written partition where the removal key is free, a live one
        where it refuses) and on the total, which always collides. The summary
        metric name is attacked in three spellings, two of which must STAMP
        beside the engine's own key. The owned INSERT is attacked on the three
        typing shapes the critic measured drifting - a VALUES list mixing a
        TIMESTAMP literal with a string, a compound NULL, and DEFAULT in an
        outer select under WITH - on both doors.
      artifacts: [crates/repark-spark/src/tests/session_write_conf.rs, crates/repark-sql/src/session_write_conf.rs]
    - id: AT-8
      status: ATTACKED
      evidence: The brief's preferred close for the INSERT P1 - carry the
        resolved overrides down the fork's insert - is not available at the
        pinned rev, and that is stated with the reason (the fork's commit exec
        builds its snapshot properties from a hardcoded map) rather than
        approximated. The gate that still lets a conf pick the INSERT route is
        reported as a deliberate choice with its single reason, the v4/v7
        file-naming item, together with the measurement showing the two routes
        now answer alike. The false fixture claim was corrected by making it
        true, not by deleting the sentence.
      artifacts: [task/ledgers/staging/ice-session-write-conf-1-ledger.md, docs/spark-sql-iceberg-parity.md, python/repark/tests/ice_session_write_conf_1_spark_oracle.json]
    - id: AT-10
      status: ATTACKED
      evidence: Every round-4 pin was run against the mutation that names it.
        Restoring EngineSummary::for_overwrite on the static arm reds the three
        static-overwrite pins and nothing else; planning the bare source again
        reds the three typing pins and the native one and nothing else; the
        ascii-folded collision lookup reds the mixed-case pin alone; per-batch
        writers on the identity copy-on-write rewrite red the UPDATE layout pin
        alone - the finding the critic filed was precisely that this last one
        reddened nothing before.
      artifacts: [crates/repark-spark/src/tests/session_write_conf.rs, crates/repark-iceberg/src/write/write_options.rs, crates/repark-iceberg/src/write/predicate_dml/cow_commit.rs]
  round_5_reattested:
    - id: AT-1
      status: ATTACKED
      evidence: Nothing round 5 asserts was reasoned. The delete-side removal
        keys, the three identity partition column types and the record counts
        were all recorded on live Spark 4.1.2 first (eleven QD-* cells), then
        pinned, then entered into the unit fixture and the live recorder, which
        re-derives all 106 and re-derived the 95 older ones equal in the same
        run.
      artifacts: [python/repark/tests/ice_session_write_conf_1_spark_oracle.json, python/repark/tests/_record_ice_session_write_conf_1_rounds.py]
    - id: AT-2
      status: ATTACKED
      evidence: Round 5 attacks the removal side the earlier rounds only
        attacked on its data half. The merge-on-read overwrite is attacked on
        all three delete-side keys AND on deleted-records, which must NOT move
        (a position delete does not lower the removed data file's record count).
        The identity partition column is attacked on three types, not just the
        one the critic named, because the JVM was up and a measurement is
        cheaper than a guess. The static battery is attacked with the one source
        shape that stages no file.
      artifacts: [crates/repark-spark/src/tests/session_write_conf_removals.rs]
    - id: AT-8
      status: ATTACKED
      evidence: What round 5 did NOT do is stated where it can be read. The
        dynamic and replace-all arms have the same delete-file hole as the
        static arm; no Spark cell was recorded for them in this round, so they
        were left alone rather than fixed on an unmeasured expectation, and the
        gap is named in section 9 and in the registry row. The DECIMAL finding
        was widened to DOUBLE and BOOLEAN by measurement, and the two that
        needed no code are pinned as such rather than claimed as fixes.
      artifacts: [task/ledgers/staging/ice-session-write-conf-1-ledger.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Every round-5 pin was run against the mutation that names it and
        confirmed to red under it AND to be the only thing that reds, over the
        whole 1237-test repark-spark battery. The data-only removed set reds the
        three delete-side pins alone; dropping the Decimal128 arm reds the
        DECIMAL pin alone; the staged-files revert reds the empty-source pin and
        the three delete-side pins and nothing else - and reddened NOTHING
        before this round, which is the finding the critic filed.
      artifacts: [crates/repark-spark/src/tests/session_write_conf_removals.rs, crates/repark-iceberg/src/write/summary_collision.rs, crates/repark-iceberg/src/write/static_value.rs]
  complete: true
```
