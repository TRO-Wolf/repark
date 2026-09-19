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
| C-036 | (verification critic F-RESERVED-SUMMARY-OVERWRITE) A snapshot property naming an Iceberg summary metric refuses before any write, on every path, as Spark does. | `refuse_summary_metric_collision` in `resolve_write_for_session`; `a_snapshot_property_naming_a_summary_metric_refuses_like_spark`. | PROVEN | Spark measured by the orchestrator (cells RV-INSERT / RV-MERGE / RV-DELETE-MOR): `Multiple entries with same key` on every shape; the critic's claim that Spark keeps its own metrics was wrong. The branch had let a colliding key overwrite the engine metric on MERGE/DML (silent). The other P1s (branch writes, native door, delete-file codec) stay open for the next run and the PR stays a draft. |

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

## 5. Registry

Row `ICE-SESSION-WRITE-CONF-1` in `docs/spark-sql-iceberg-parity.md`, beside
ICE-WRITE-OPTIONS-1: **FIXED 2026-09-19**. No correction to ICE-WRITE-OPTIONS-1
was needed (R-24c-2).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-session-write-conf-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every pin expectation reads from the committed Spark 4.1.2
        oracle cells, per enumerated door (facade SQL, DataFrame, v2 and v3
        twins); the live tier re-derives the fixture. No RePark-only answer
        was written into a pin.
      artifacts: [python/repark/tests/ice_session_write_conf_1_spark_oracle.json, python/repark/tests/test_ice_session_write_conf_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Empty conf value, two keys at once, upper-case codec, bogus
        codec, unset-restores-unstamped, and the reserved operation key are all
        pinned. The pins were red on the base where the confs were ignored.
      artifacts: [python/repark/tests/test_ice_session_write_conf_1.py, crates/repark-iceberg/src/tests/session_write_conf.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The bogus codec refuses before any file is written with no
        snapshot committed. The two RDF cells are strict xfails, so the suite
        reds if the fork ever honours the confs there.
      artifacts: [python/repark/tests/test_ice_session_write_conf_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: The conf is read per statement from the live session context;
        set applies to later writes only and unset restores unstamped writes.
        Each pin cell sets and unsets its own confs, so cells cannot leak
        state into each other.
      artifacts: [crates/repark-spark/src/tests/session_write_conf.rs, python/repark/tests/test_ice_session_write_conf_1.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no deserialization and
        no path handling — the conf keys and values are plain strings stamped
        into snapshot summaries.
    - id: AT-6
      status: ATTACKED
      evidence: Every pin asserts the committed rows equal the recorded Spark
        rows alongside the summaries and codecs; the v3 twins assert the v2
        Spark answers on v3 tables.
      artifacts: [python/repark/tests/test_ice_session_write_conf_1.py]
    - id: AT-7
      status: N/A
      justification: The per-statement conf merge walks the set keys only; no
        loop, cache, or growth scales with data size.
    - id: AT-8
      status: ATTACKED
      evidence: The fork's RewriteDataFiles takes no writer or snapshot
        properties, so honour there is not presumed — it is a declared residue
        with strict xfails. The bogus-codec refusal names the codec with
        Spark's text.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_ice_session_write_conf_1.py]
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
      artifacts: [crates/repark-spark/src/tests/session_write_conf.rs, crates/repark-spark/src/append_with_options.rs]
  reattested: [AT-8, AT-10]
  complete: true
```
