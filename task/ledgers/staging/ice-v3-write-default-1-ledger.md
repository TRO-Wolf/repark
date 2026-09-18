# Unit ledger — ICE-V3-WRITE-DEFAULT-1 · omitted columns fill from `write_default` on every write path

**Unit:** ICE-V3-WRITE-DEFAULT-1 round 1 · **Date:** 2026-09-17 · **Branch:** `fix/ice-v3-write-default-1` · **Base:** `chore/fork-pin-ice-19b` head (fork pin `75da2b58`, RP-21 / PR #665)
**Model:** claude-opus-5 (round 5, run 21b, 2026-09-17; round 6 = run 21b round 2, 2026-09-18); rounds 1–4 muse-spark-1.3-contributor
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** A format-v3 table created by Spark, with column `c INT` added through
the Java API carrying `initial-default: 5` and `write-default: 5`, fills 5 on
every Spark write door when the column is omitted. RePark writes NULL (INSERT /
MERGE column lists) or refuses (`DataFrame` append: `missing from the DataFrame:
['c']`). Every RePark write path that builds rows for an Iceberg table must fill
an omitted column from the current schema field's `write_default` in Rust, in the
shared write-projection step both SQL doors and the DataFrame writers reach.

**Retires:** this ledger moves to `../completed/` when the unit's last commit
lands.

**Not in this step:** conflict detection, the DML planner's scan (run 19a owns
those); any fork pin move; `STATUS.md`; any JVM outside the one fixture-recording
step.

## Rulings carried in

- Q-19b-4: fix scope is every RePark write path that builds rows for an Iceberg
  table, filling from the field's `write_default`, in Rust, in the shared
  write-projection step.
- **Q-21b-3 (run 21b, 2026-09-17) — L-01 is NOT re-scoped away.** Spark 4.1.2 parses
  all three `INSERT OVERWRITE … PARTITION` shapes with a named column list and fills
  the write-default (cells `ow_dynamic_partition_named_list_short` `[[null, 'x', 5]]`,
  `ow_dynamic_partition_named_list_full` `[[10, 'x', 5]]`,
  `ow_static_partition_named_list` `[[10, 'x', 5]]`,
  `ow_partitioned_no_partition_clause_named_list` `[[10, 'x', 5]]`,
  `ow_partitions_api` `[[10, 'x', 5], [11, 'y', 9]]`). The round-4 "unreachable by
  SQL" note was RePark's parser. Disposition: FIXED on both doors in Rust (C-015).
- **Q-21b-4 — L-03 is a real divergence.** Spark fills `DEFAULT` on `INSERT
  OVERWRITE` in VALUES and named-list position (`ow_values_default_kw`
  `[[15, 'o', 5]]`, `ow_named_list_default_kw` `[[18, 'r', 5]]`), as on `INSERT INTO`
  (`insert_values_default_kw`). Disposition: FIXED (C-016).
- **Q-21b-5 — L-04 is a DECLARED row, not a fill.** Spark `saveAsTable(overwrite)`
  on an Iceberg table REPLACES it and narrows the schema to the frame
  (`saveastable_overwrite_missing_defaulted` `[[30, 's']]`, `schema_after.dfltsat`
  `[id int, name string]`). Disposition: RePark's by-name `INSERT OVERWRITE`
  pinned; dated DECLARED row beside F-002 (C-017).
- **Q-21b-6 — the roll-call merge condition says ACCEPT-AND-NULL.** Spark accepts a
  missing nullable no-default column on `writeTo().append()` and
  `saveAsTable(append)` and writes NULL (`nodef_writeto_append_missing`,
  `nodef_saveas_append_missing`; control `nodef_writeto_append_full`).
  Disposition: pinned offline and live (C-018).
- **Q-21b-8 (run 21b round 2, 2026-09-18) — V-01 is real.** Spark fills an omitted
  defaulted column on `writeTo(t).overwritePartitions()`: `df(12, 'y')` over `pdflt`
  → `[[1,'a',5],[12,'y',5],[2,'b',5]]` (`V01_overwrite_partitions_missing_defaulted_column`).
  RePark refused on arity because the writer threw its by-name column list away.
  Disposition: FIXED, pinned offline and live (C-020).
- **Q-21b-9 — V-02 is NOT a gap; the critic's control premise was the reverse of
  Spark.** Spark refuses `DEFAULT` inside a CTE body (`V02_default_inside_cte_body`),
  inside a derived table (`V02_default_inside_subquery`), and in the outer SELECT of a
  query carrying `WITH` (`V02_control_default_outer_select_with_cte`), all
  `UNRESOLVED_COLUMN` SQLSTATE 42703. RePark refused the first two and FILLED the
  third. Disposition: the outer-SELECT-under-`WITH` shape refuses on INSERT INTO and
  INSERT OVERWRITE, both doors; all three cells pinned as refusals (C-021).
- **Q-21b-10 — mixed static+dynamic PARTITION is out of scope.** Spark accepts
  `PARTITION (id=1, cat)` positional (`MIX_static_dynamic_positional`
  `[[1,'west','p',9],[2,'west','w2',5]]`) and with a named list omitting the default
  (`MIX_static_dynamic_named_list_omits_default` `[[1,'west','p',9],[2,'west','q',5]]`).
  RePark refuses loud. Disposition: OPEN registry row
  `ICE-V3-WRITE-DEFAULT-1-MIX-PARTITION`, refusal pinned (C-022).
- Round 6 cells (Q-21b-8 … Q-21b-10) were measured by the orchestrator
  (`/tmp/oc-worker/kb-oracle/probe_wd2.py`, truth `wd-truth-2.json`) and copied
  verbatim into `truth.json`; not re-derived here.
- The critic's pin-strength note: the ANSI L-03 pin cannot see the Spark-door
  `rewrite_overwrite_default_markers` hunk; a permanent Spark-door Rust pin now goes
  red when that call is removed (C-023).
- Orchestrator addendum (round-1 gate run): the C-009 setter guard and
  `make rust-clippy` must be green, fixed at the source without weakening either
  (C-024).
- All four rulings' cells are recorded by this unit's `record.py` into the
  checked-in `truth.json` (re-recorded live 2026-09-17), matching the
  orchestrator's independent `/tmp/oc-worker/kb-oracle/wd-truth.json`
  value-for-value.
- Owner, jb-common.md: no comments in code; Rust first; branch commits carry
  `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>` as the
  last line; work COMMITTED by 13:30 EDT 2026-09-17; JVM only through
  `/tmp/oc-worker/jb-jvm.sh`, one at a time.

## PROPOSITION LEDGER — ICE-V3-WRITE-DEFAULT-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `repro_before_fix`: `p_write_default.py` on the unfixed tree shows INSERT / MERGE column lists writing NULL for omitted `c` and `DataFrame` append refusing `missing from the DataFrame: ['c']`. | Regenerate the source table with `p_v3_types_defaults.py` via `/tmp/ib-scratch/run-probe.sh`; run `p_write_default.py`; paste the output. | **PROVEN** | Source table regenerated (`/tmp/oc-worker/jb-jvm.sh /tmp/ib-scratch/run-probe.sh p_v3_types_defaults.py`, exit 0; schema carries `initial-default: 5, write-default: 5` on `c`; Spark reads pre-add rows as 5; RePark omitted-`c` insert reads back `(6, 'f', None)`). Repro on this tree's release native (`.venv/bin/python /tmp/ib-scratch/probes/p_write_default.py`, no JVM): `INSERT (id, name)` writes `(7, 'g', None)`; MERGE NOT MATCHED writes `(13, 'm', None)`; `writeTo`/`saveAsTable` append refuse `missing from the DataFrame: ['c']`; positional-short and SELECT-short refuse at planning (`Inconsistent data length`, `Column count doesn't match`); `DEFAULT` keyword refuses (`No field named default`). pins: ice-v3-write-default-1/C-001. |
| C-002 | `spark_oracle_recorded`: the Java-API-created v3 table ships as a checked-in fixture with relative-path-safe adoption, plus a truth JSON of Spark's answer for every cell, recorded by a script checked in beside it. | Fixture directory + truth JSON + recording script, following the `ice_spark_table_1` / `test_ice_spark_table_1.py` copy-and-register pattern. | **PROVEN** | `python/repark-parity/fixtures/torture/data/ice_v3_write_default_1/`: seven v3 tables (225,094 bytes, `.crc` stripped), `truth.json` (banner `spark=4.1.2 tz=UTC`, per-table schema plus seed outcome, 22 cells), `record.py` (Spark-only, pins C-002), `map.md`. Recorded 2026-09-17 on live PySpark 4.1.2 + Iceberg 1.11.0; adoption copies to the baked-in canonical `/tmp/repark-ice-v3-write-default-1/ns/<table>` under a lock. pins: ice-v3-write-default-1/C-002. |
| C-003 | `pins_red_first`: `python/repark/tests/test_ice_v3_write_default_1.py` (offline tier against the fixture, live tier under `REPARK_PARITY_LIVE=1`) and the Rust fill unit tests fail on the unfixed tree. | Run the new pins on the unfixed tree; paste failures. | **PROVEN** | `## Red first` below (8 failed on the unfixed tree); the live cell never ran red-first (it was skipped offline) and its hand-written two-row expectation was repaired to the measured three rows in round 2 (audit class: stale pin, `150a820a`). |
| C-004 | `insert_column_list_fills`: `INSERT INTO t (id, name)` on both SQL doors fills omitted `c` from `write_default` (NULL only when the field has none); a required column with no write-default keeps Spark's error. | Offline + live pins green; Rust unit tests for the fill green. | **PROVEN** | Offline `14 passed, 1 skipped`; live `15 passed` (round 2, release native); ANSI-door Rust pins `crates/repark-sql/tests/ansi_write_defaults.rs` `2 passed` (the ANSI fill had code but no pin until round 2 — found in step 5). Required-missing refuses `non-nullable but contains null` on both doors. |
| C-005 | `merge_not_matched_fills`: `MERGE … WHEN NOT MATCHED THEN INSERT (id, name)` fills omitted `c` from `write_default` on both SQL doors. | Offline + live pins green. | **PROVEN** | Offline + live green (same runs as C-004); Rust `insert_fill.rs` fill + required pins green (`cargo test -p repark-iceberg --lib` in step 7). |
| C-006 | `dataframe_writers_fill`: `writeTo(t).append()` and `saveAsTable(append)` with `c` missing fill from `write_default` instead of refusing; `insertInto` stays positional (no column list) and refuses a short frame, as Spark does — corrected 2026-09-17 per the logic review (L-02): the registry and this clause wrongly listed `insertInto` as a fill door while Spark (`insertInto_missing`: `INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS`) and the pin (`test_insert_into_and_extra_column_refuse`: `Column count doesn't match`) refuse. | Offline + live pins green. | **PROVEN** | Offline + live green (same runs as C-004); `check_lib_py.py` ratchets `writer_readwriter.py` 1101 → 1095. |
| C-007 | `other_shapes_match_spark`: positional-short `INSERT INTO t VALUES (8, 'h')`, `INSERT INTO t SELECT 9, 'i'`, `VALUES (10, 'j', DEFAULT)`, `saveAsTable` append, and `df.write.insertInto` each give Spark's measured answer (fill or error). | Truth JSON cell per shape; pins assert each. | **PROVEN** | Offline + live green (same runs as C-004), including the Spark-door `overwrite_column_list` cell. |
| C-008 | `type_fidelity`: the default literal casts to the field type exactly; coverage is int, string, and one decimal (plus date, timestamp, binary defaults if the spec allows them). | Pins per covered type. | **PROVEN** | `string_default_insert`, `decimal_default_insert`, `temporal_default_insert` cells green offline (no binary-default cell exists — the registry NESTED row claims int/string/decimal/temporal only). |
| C-009 | `nested_struct_default`: one nested struct-field default measured against Spark; filled, or DECLARED with a registry row if the fix is not small. | Measurement pasted; fix or DECLARED row. | **PROVEN** | DECLARED arm taken: `ICE-V3-WRITE-DEFAULT-1-NESTED` row (2026-09-17) — only primitive literals fill; struct-field defaults unpinned and unmeasured on both engines. pins: ice-v3-write-default-1/C-009. |
| C-010 | `no_default_unchanged`: a table without any defaults behaves exactly as before. | Pin green. | **PROVEN** | Offline green (same run as C-004). |
| C-011 | `v2_unchanged`: a v2 table behaves exactly as before. | Pin green. | **PROVEN** | Green in the offline run, the live run, and the whole-facade run (no `test_ice_v3_write_default_1` failure there). |
| C-012 | `write_default_differs`: write-default 7 with initial-default 5 (Java API `updateColumnDefault` / `UpdateSchema`) — old rows read 5, new omitted writes fill 7. | Pin green. | **PROVEN** | Green in the offline run, the live run, and the whole-facade run (no `test_ice_v3_write_default_1` failure there). |
| C-013 | `registry_rewritten`: the V3-6 / write-default rows in `docs/spark-sql-iceberg-parity.md` state the measured truth (dated 2026-09-16, ICE-V3-WRITE-DEFAULT-1, FIXED with the pins; DECLARED rows for anything left, with Spark's shape); `test_rp3_c009_write_default.py` still describes the contract. | Registry diff; guard test disposition recorded. | **PROVEN** | Four rows landed 2026-09-17 (§7): FIXED `ICE-V3-WRITE-DEFAULT-1`, DECLARED `ICE-V3-WRITE-DEFAULT-1-OVERWRITE-PART` (with Spark's measured partition shapes) and `ICE-V3-WRITE-DEFAULT-1-NESTED`, BACKLOG `F-001`. No V3-6/write-default row existed before — these are new. The C-009 guard still describes the contract: the new code only READS defaults (setter needles `with_write_default` / `write_default(` absent from all touched files, grepped 2026-09-17); full guard run in step 7. |
| C-014 | `gates_green`: the new test file offline and live, `cargo test -p repark-iceberg --lib`, `cargo test -p repark-spark --lib`, `make verify`, the whole facade suite, and the whole parity suite are green on the release native. | Counts in this ledger. | **PROVEN** | Unit pins offline `14 passed, 1 skipped`, live `15 passed` (release native); `repark-iceberg --lib` 442 passed; `repark-spark --lib` 1051 passed, 4 ignored; `repark-sql` all targets exit 0 (342 lib + integration incl. 2 new ANSI pins); `insert_fill` struct pin green; `uvx ruff check .` + `format --check .` clean; `make verify` exit 0. Whole facade (`/tmp/oc-worker/jb-wd/facade-r2.log`): 9342 passed, 369 skipped, 26 xfailed, 3 failed — each dispositioned: the stale saveAsTable-missing-column refusal retired to the measured NULL fill (`0d273199`, green on rerun), the insertInto-missing-table `TableNotFound` leak fixed at the root (`rewrite_insert_markers` passthrough, `test_missing_table_text` green on rerun), the sort-pool OOM is load-induced (passes alone and 18/18 as a file). Whole parity (`/tmp/oc-worker/jb-wd/parity-r2.log`): 756 passed, 2 skipped, 12 xfailed, 1 failed — the CAP-1 mirror row for `writer_readwriter.py` ratcheted 1101 → 1095, `23 passed` on rerun. |

| C-015 | `partition_overwrite_fills` (Q-21b-3 / L-01): dynamic `PARTITION (k) (cols)`, static `PARTITION (k=v) (cols)` and the partitioned whole-table column list fill an omitted defaulted column from `write_default` on BOTH doors — never NULL. | Red-first pins on the recorded partitioned fixture, then green; Rust ANSI-door pins. | **PROVEN** | Red first below (2 offline ParserErrors, ANSI dynamic `(10, 'x', 0)` = NULL, ANSI static `NOT_ENOUGH_DATA_COLUMNS`). Green after `8dfa984a`: offline `test_dynamic_partition_named_list_fills_write_default`, `test_static_partition_named_list_fills_write_default`, `test_partitioned_whole_table_named_list_fills_write_default`, `test_overwrite_partitions_api_replaces_source_partitions`; ANSI `ansi_dynamic_…`, `ansi_static_…`, plus branch pins `ansi_static_partition_value_wins_over_its_write_default` (mutation-checked: dropping the reserved list turns it red) and `ansi_static_partition_column_list_refusals`; parser swap `spark_dialect.rs` 2 tests. Retained-rows semantics of a name-only `PARTITION (k)` stay the DML-1 DECLARED residue. pins: ice-v3-write-default-1/C-015 |
| C-016 | `overwrite_default_keyword_fills` (Q-21b-4 / L-03): `INSERT OVERWRITE … VALUES (…, DEFAULT)` and `INSERT OVERWRITE t (id, name, c) SELECT …, DEFAULT` fill from `write_default`, exactly as `INSERT INTO` does. | Red-first pins, then green, both doors. | **PROVEN** | Red first below (`No field named default`). Green after `1bea3998`: offline `test_overwrite_default_keyword_fills_write_default` (both forms, whole table `[(15, 'o', 5)]` then `[(18, 'r', 5)]`); ANSI `ansi_partition_overwrite_default_keyword_fills_write_default` (ANSI whole-table overwrite stays the Q9 refusal, row DML-1). pins: ice-v3-write-default-1/C-016 |
| C-017 | `saveastable_overwrite_declared` (Q-21b-5 / L-04): RePark's `saveAsTable(overwrite)` is by-name `INSERT OVERWRITE` (schema kept, defaulted column filled); the measured Spark answer is a REPLACE that narrows the schema to the frame. Pinned on the RePark side and DECLARED beside F-002. | Pin of RePark's behaviour asserting the recorded Spark replace cell; dated DECLARED registry row. | **PROVEN** | `test_saveastable_overwrite_is_insert_overwrite_not_replace` green (RePark `[(30, 's', 5)]` beside the recorded Spark `[[30, 's']]` and `schema_after`); registry `ICE-V3-WRITE-DEFAULT-1-SAVEAS-OVERWRITE` DECLARED 2026-09-17 (`c5ea01d3`). pins: ice-v3-write-default-1/C-017 |
| C-018 | `rollcall_accept_and_null` (Q-21b-6): a missing NULLABLE column with NO default is accepted on `writeTo().append()` and `saveAsTable(append)` and written NULL, matching Spark — the removed "missing from the DataFrame" refusal is correct. | Offline and live pins on both writer surfaces. | **PROVEN** | Offline `test_missing_nullable_no_default_accepts_and_nulls` green; live `_live_rollcall` green (`22 passed in 115.19s`, `REPARK_PARITY_LIVE=1`). Roll-call section below. pins: ice-v3-write-default-1/C-018 |
| C-019 | `no_default_write_cost` (R-03 / R-04): a MERGE and a column-list `INSERT OVERWRITE` against a table with no write-defaults convert the Iceberg schema to Arrow at most once and never build a `ColumnDefaults` map they discard. | Code shape plus a before/after measurement on a DEFAULT-profile release native, both numbers in this ledger. | **PROVEN** | Code shape in `9a9f49e6` / `8dfa984a`; DEFAULT-profile timings recorded below — noise-dominated under load 70–167, no speed claim made. pins: ice-v3-write-default-1/C-019 |

| C-020 | `overwrite_partitions_fills` (Q-21b-8 / V-01): `writeTo(t).overwritePartitions()` with a defaulted column missing from the frame fills it from `write_default`, matching Spark's `V01` cell. | Red-first pin on the recorded `pdflt` fixture, then green offline and live. | **PROVEN** | Red first (`## Red first — round 6`): `INSERT OVERWRITE column count mismatch: source has 2 columns, target table has 3`. Fixed in `2baebd4d` (the writer passes its by-name list: `INSERT OVERWRITE t (cols) PARTITION (…) SELECT …`). Green offline `test_overwrite_partitions_api_fills_missing_defaulted_column` (seed + `(12, 'y', 5)`, cell asserted); live `_live_overwrite_partitions` (Spark answer == the recorded V01 rows, RePark replays `(13, 'z')` on the adopted table and fills 5) — gate counts in `## Round 6 gates`. `writer_readwriter.py` 1095 → 1094 with the CAP-1 mirror. pins: ice-v3-write-default-1/C-020 |
| C-021 | `default_outside_insert_list_refuses` (Q-21b-9 / V-02): `DEFAULT` inside a CTE body, inside a derived table, or in the outer SELECT of a query carrying `WITH` refuses on INSERT INTO and INSERT OVERWRITE, both doors, and writes nothing; the `WITH` shape names the unresolved `DEFAULT` column with SQLSTATE 42703. | Red-first pins on both doors, then green; all three V02 cells asserted as recorded refusals. | **PROVEN** | Red first: Spark-door Rust `spark_default_in_outer_select_under_with_refuses_unresolved` and ANSI `ansi_default_in_outer_select_under_with_refuses_unresolved` "must refuse"; Python `test_default_outside_the_insert_list_refuses` `DID NOT RAISE`. Fixed in `31c1ca4d` (`insert_defaults::refuse_default_marker_under_with`, called before any load in `rewrite_insert_markers` and at the top of `rewrite_markers_with_table`). Green: Rust Spark door 3/3, ANSI 8/8, offline 24 passed. Text delta, recorded: Spark says `UNRESOLVED_COLUMN.WITH_SUGGESTION … [id, name]` (the CTE's columns); RePark says `WITHOUT_SUGGESTION` — same class, column and SQLSTATE. The CTE-body / subquery shapes refuse `No field named default` (not the Spark text; class of outcome matches — refusal, no write). pins: ice-v3-write-default-1/C-021 |
| C-022 | `mixed_partition_open` (Q-21b-10): mixed static+dynamic `PARTITION (id=1, cat)` refuses loud on both MIX shapes, writes nothing, and carries an OPEN registry row with Spark's measured accept. | Pin + dated OPEN row. | **PROVEN** | `test_mixed_static_dynamic_partition_refuses` green (refusal `cannot mix static assignments`, rows unchanged, both recorded Spark cells asserted); registry `ICE-V3-WRITE-DEFAULT-1-MIX-PARTITION` OPEN 2026-09-18 (`6b6ad1bf`). Not implemented (ruling). pins: ice-v3-write-default-1/C-022 |
| C-023 | `spark_door_default_pin_strength` (critic note on L-03): a Spark-door Rust pin goes red when the `rewrite_overwrite_default_markers` call in `crates/repark-spark/src/insert_overwrite.rs` is removed. | Mutation run, red output pasted. | **PROVEN** | `crates/repark-spark/src/tests/write_defaults.rs::spark_overwrite_default_keyword_fills_write_default`; call replaced by `None` → red `column 'default' not found` (`## Red first — round 6`); restored → green. pins: ice-v3-write-default-1/C-023 |
| C-024 | `guards_green` (orchestrator addendum): `test_rp3_c009_write_default.py::test_engine_sources_do_not_set_write_default` and `make rust-clippy` are green without weakening either. | Guard run; clippy run. | **PROVEN** | Guard: helper renamed `schema_has_primitive_fill`, `insert_defaults` unit tests moved to `insert_defaults/tests/mod.rs` (the guard's own `tests` exemption; guard untouched) — `2 passed` (`fe4e3c08`). Clippy: `match_same_arms` arms merged in `spark_rewrites.rs`; the 135 `large_futures` errors came from round-1 inline awaits in `spark_ast.rs::execute_passthrough_inner` (a preloaded `Option<Table>` held across awaits plus the marker/fill futures) and `insert_overwrite.rs::execute_insert_overwrite` (the rewritten `(String, Insert)`); both `Box::pin`-ed at source with the held values boxed (`8fd8036c`) — `cargo clippy --locked -p repark-spark --all-targets -- -D warnings -A clippy::disallowed_methods` clean; whole-workspace `make rust-clippy` in `## Round 6 gates`. pins: ice-v3-write-default-1/C-024 |

VERDICT: 24 clauses, 24 PROVEN, 0 OPEN, 0 REJECTED.

```
COVERAGE_ATTESTATION:
  pr_unit: ice-v3-write-default-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause C-001 … C-024 walked against its proposition; rulings Q-21b-3 … Q-21b-6 map to C-015 … C-018 and Q-21b-8 … Q-21b-10 to C-020 … C-022, the critic's pin-strength note to C-023 and the addendum guards to C-024; each clause's pins assert the recorded Spark cell, not a paraphrase.
      artifacts: [python/repark/tests/test_ice_v3_write_default_1.py, python/repark-parity/fixtures/torture/data/ice_v3_write_default_1/truth.json, crates/repark-sql/tests/ansi_write_defaults.rs, crates/repark-spark/src/tests/write_defaults.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised — DEFAULT in a CTE body, a derived table and the outer SELECT under WITH (refuse, nothing written); overwritePartitions with the defaulted column omitted; mixed static+dynamic PARTITION (loud refusal, OPEN); NULL partition value from an omitted dynamic partition column, static value on a column that itself carries a write-default, listed static column, arity mismatch, duplicate list entry, explicit NULL, required column without default, v2 and no-default tables, quote-free SQL on the parser fast path.
      artifacts: [crates/repark-sql/tests/ansi_write_defaults.rs, crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/test_ice_v3_write_default_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal on the static by-name path fails before any file is staged or committed (the refusal pin reads the table back empty); the DEFAULT rewrite on overwrite leaves the statement untouched when the target is not an Iceberg table or holds no marker.
      artifacts: [crates/repark-iceberg/src/write/partition_overwrite.rs, crates/repark-spark/src/insert_overwrite.rs]
    - id: AT-4
      status: N/A
      justification: No shared mutable state, lock or ordering assumption added; the fill is a pure function of the loaded schema and the statement, and commits go through the existing overwrite commit paths unchanged.
    - id: AT-5
      status: ATTACKED
      evidence: The parser swap only reorders two existing token spans of the caller's own statement; fill fragments are generated from typed Iceberg literals and identifiers are quoted through `quote_ident_spark`. No new input reaches SQL text unquoted; no network, credential or path handling added.
      artifacts: [crates/repark-spark/src/spark_rewrites.rs, crates/repark-iceberg/src/write/insert_defaults.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Fills read the CURRENT schema's `write_default` (never `initial_default`); the fixture was re-recorded and every pre-existing cell came back identical, so the checked-in tables and truth stay a faithful Spark record.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_v3_write_default_1/record.py, python/repark-parity/fixtures/torture/data/ice_v3_write_default_1/truth.json]
    - id: AT-7
      status: ATTACKED
      evidence: R-03/R-04 remove per-statement Arrow conversions on the no-default path; the DEFAULT-profile before/after timing is recorded and honestly reported as noise-dominated (load 70–167), with no claim made either way.
      artifacts: [crates/repark-iceberg/src/write/merge/insert.rs, crates/repark-iceberg/src/write/insert_defaults.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml, Cargo.lock, pyproject or fork-pin change; the fork API is only read (`field.write_default`); error text reuses Spark's classes (`STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST` SQLSTATE 42713, `INSERT_COLUMN_ARITY_MISMATCH`, `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA`).
      artifacts: [crates/repark-iceberg/src/write/partition_overwrite.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Registry rows state the measured truth — OVERWRITE-PART moved to FIXED, SAVEAS-OVERWRITE DECLARED beside F-002 — and every touched directory's map.md records the change with its pins line.
      artifacts: [docs/spark-sql-iceberg-parity.md, crates/repark-iceberg/src/write/map.md, crates/repark-spark/src/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: Red first recorded for C-015/C-016 and C-020/C-021 on the unfixed tree; the Spark-door DEFAULT hunk mutation-proved red (C-023); the new static by-name branches each have a nameable input and pin, and a real mutation (dropping the reserved static columns) turned `ansi_static_partition_value_wins_over_its_write_default` red before revert.
      artifacts: [crates/repark-sql/tests/ansi_write_defaults.rs, task/ledgers/staging/ice-v3-write-default-1-ledger.md]
  complete: true
```

## Red first

C-001 red is the matrix in `## Evidence` below (unfixed tree, release native
`.venv`, 2026-09-17): every FILL cell reads NULL or refuses where Spark fills.

C-003 red (2026-09-17, unfixed tree, `.venv/bin/python -m pytest
python/repark/tests/test_ice_v3_write_default_1.py -q -p no:cacheprovider`):
8 failed, 6 passed, 1 skipped in 1.22s. The 8 failures are exactly the FILL
cells (`insert_column_list`, `merge_not_matched`, `dataframe_writers`,
`default_keyword`, `no_default` DEFAULT-keyword leg, `string_temporal_differ`,
`decimal`, `overwrite_column_list` — NULL or `No field named default` /
`missing from the DataFrame` where Spark fills). The 6 passes are the
already-correct shapes (seed, short-insert refusals, `insertInto`/extra-column
refusals, explicit NULL, required refusal, v2 control). Live skipped
(`REPARK_PARITY_LIVE` unset). pins: ice-v3-write-default-1/C-003.

## Red first — round 5 (run 21b, 2026-09-17)

The round-5 cells are recorded by this unit's own fixture recorder
(`python/repark-parity/fixtures/torture/data/ice_v3_write_default_1/record.py`,
re-run live on PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`, Java 17,
hadoop catalog, format-version 3, exit 0, 33 cells) against four new tables —
`pdflt` (partitioned by `id`, `c INT` write-default 5), `dfltow` and `dfltsat`
(unpartitioned, same default) and `nodef` (`id, name, c INT`, NO default). Every
new cell reproduces the orchestrator's independently measured oracle
(`/tmp/oc-worker/kb-oracle/wd-truth.json`) value-for-value, and every one of the
22 pre-existing cells re-recorded byte-identical apart from a Py4J gateway object
id inside one recorded error message (`o485` → `o579`, `decimal_default_insert`),
so the recorder is deterministic in rows, schema and error class.

Red first, unfixed tree, release native, `.venv/bin/python -m pytest
python/repark/tests/test_ice_v3_write_default_1.py -q -p no:cacheprovider` —
**3 failed, 18 passed, 1 skipped**:

```
E  repark.errors.ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: name at Line: 1, Column: 72")
E  repark.errors.ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: name at Line: 1, Column: 77")
E  repark.errors.AnalysisException: Schema error: No field named default.
```

- `test_dynamic_partition_named_list_fills_write_default` — RePark's Spark door
  refuses `INSERT OVERWRITE t PARTITION (id) (name) VALUES ('x')` at the parser.
  Spark parses it and answers `[[null, 'x', 5]]`
  (cell `ow_dynamic_partition_named_list_short`), so the round-4 re-scoping note
  ("a ParserError on the Spark door, so the path is unreachable by SQL") was
  RePark's parser, not Spark's. Ruling Q-21b-3 stands L-01 up as filed.
- `test_static_partition_named_list_fills_write_default` — same parser refusal on
  `PARTITION (id = 10) (name)`; Spark answers `[[10, 'x', 5]]`.
- `test_overwrite_default_keyword_fills_write_default` — `No field named default`
  on both `INSERT OVERWRITE dfltow VALUES (15, 'o', DEFAULT)` and the named-list
  form; Spark fills 5 on both (`ow_values_default_kw`,
  `ow_named_list_default_kw`).

Red first, ANSI door, `cargo test -p repark-sql --test ansi_write_defaults` —
**2 passed, 2 failed**:

```
---- ansi_static_partition_column_list_fills_write_default stdout ----
`INSERT OVERWRITE ice.sales.p (name) PARTITION (id = 10) SELECT 'x'` must succeed:
Error during planning: [INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS]
Cannot write to the target: table has 3 columns, static PARTITION injects 1, source has 1

---- ansi_dynamic_partition_column_list_fills_write_default stdout ----
assertion `left == right` failed
  left: (10, "x", 0)
 right: (10, "x", 5)
```

The dynamic row is the V3-03b silent-wrong the logic critic found: the arm writes
`c = NULL` (read back as `0` through `Int32Array::value`) where Spark writes 5.
The static row is loud but wrong-shaped against Spark's answer.

Four round-5 pins were **already green** on the unfixed tree and are pinned so
they cannot regress: the partitioned whole-table column list
(`ow_partitioned_no_partition_clause_named_list`), `overwritePartitions()` with
full-width values (`ow_partitions_api`), `saveAsTable(overwrite)`
(C-017 — RePark's answer, beside the recorded Spark replace), and the whole
roll-call cell (C-018 — accept-and-NULL on both writer surfaces). Their green is
evidence for Q-21b-5 and Q-21b-6, not a fix.

## Red first — round 6 (run 21b round 2, 2026-09-18)

Spark answers for V-01, V-02 and the mixed-PARTITION cells were measured by the
orchestrator (`/tmp/oc-worker/kb-oracle/probe_wd2.py`, PySpark 4.1.2 +
`iceberg-spark-runtime-4.1_2.13:1.11.0`, hadoop catalog, format-version 3) and
copied verbatim from `/tmp/oc-worker/kb-oracle/wd-truth-2.json` into this unit's
`truth.json` `cells` (six keys, `V01_*`, `V02_*`, `MIX_*`; not re-derived).

Unfixed tree (`7a088085` product code), release native, `.venv/bin/python -m pytest
python/repark/tests/test_ice_v3_write_default_1.py -q -p no:cacheprovider -k
"fills_missing_defaulted or outside_the_insert or mixed_static"` — **2 failed, 1 passed**:

```
E  repark.errors.AnalysisException: Error during planning: INSERT OVERWRITE column count mismatch: source has 2 columns, target table has 3 (SQL INSERT is positional — OV1 D9)
E          Failed: DID NOT RAISE AnalysisException
FAILED python/repark/tests/test_ice_v3_write_default_1.py::test_overwrite_partitions_api_fills_missing_defaulted_column
FAILED python/repark/tests/test_ice_v3_write_default_1.py::test_default_outside_the_insert_list_refuses
```

- `test_overwrite_partitions_api_fills_missing_defaulted_column` (V-01): the
  arity refusal where Spark fills `[[1,'a',5],[12,'y',5],[2,'b',5]]`.
- `test_default_outside_the_insert_list_refuses` (V-02): the CTE-body and
  subquery shapes already refuse (`No field named default`); the control
  `INSERT OVERWRITE dfltow WITH x AS (SELECT 20 AS id, 'z' AS name) SELECT id,
  name, DEFAULT FROM x` does NOT raise — RePark fills 5 where Spark refuses
  `UNRESOLVED_COLUMN.WITH_SUGGESTION` 42703.
- `test_mixed_static_dynamic_partition_refuses` (MIX) is green by design: it pins
  RePark's current loud refusal beside Spark's recorded accept (ruling Q-21b-10,
  OPEN registry row, not implemented this round).

Spark door, Rust, `cargo test -p repark-spark --lib tests::write_defaults` —
**1 failed, 2 passed**:

```
---- tests::write_defaults::spark_default_in_outer_select_under_with_refuses_unresolved stdout ----
`INSERT OVERWRITE ice.sales.d WITH x AS (SELECT 20 AS id, 'z' AS name) SELECT id, name, DEFAULT FROM x` must refuse
test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 1131 filtered out
```

ANSI door, `cargo test -p repark-sql --test ansi_write_defaults` — **1 failed, 7 passed**:

```
---- ansi_default_in_outer_select_under_with_refuses_unresolved stdout ----
`INSERT INTO ice.sales.t WITH x AS (SELECT 20 AS id, 'z' AS name) SELECT id, name, DEFAULT FROM x` must fail
test result: FAILED. 7 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out
```

Spark-door L-03 pin, mutation-proved (the critic's pin-strength note): with the
`rewrite_overwrite_default_markers(ctx, catalogs, table_name, insert)` call in
`crates/repark-spark/src/insert_overwrite.rs::execute_insert_overwrite` replaced
by `None`, `cargo test -p repark-spark --lib
tests::write_defaults::spark_overwrite_default` goes **red**; restored, green:

```
---- tests::write_defaults::spark_overwrite_default_keyword_fills_write_default stdout ----
called `Result::unwrap()` on an `Err` value: Diagnostic(Diagnostic { kind: Error, message: "column 'default' not found", ... SchemaError(FieldNotFound { field: Column { relation: None, name: "default" }, valid_fields: [] }, Some("")))
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1133 filtered out
```

## Round-5 L-02 and L-04 (step 4)

- L-02 re-verified 2026-09-17: the correction landed in round 4 (`987b1d09`) and
  survives the rebase — the registry FIXED row names `insertInto` as positional and
  refusing a short frame with Spark's `insertInto_missing` refusal
  (`INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS`), and C-006 says the same.
  `git grep -n insertInto -- ':!*.py' ':!*.rs' | grep -i fill` finds no document
  listing `insertInto` as a fill door. No further edit needed.
- L-04 (ruling Q-21b-5): registry row `ICE-V3-WRITE-DEFAULT-1-SAVEAS-OVERWRITE`,
  DECLARED 2026-09-17, beside finding F-002, naming the measured Spark cell
  (`saveastable_overwrite_missing_defaulted`: rows `[[30, 's']]`, `schema_after`
  `[id int, name string]` — REPLACE, `c` dropped). RePark's answer
  (`[(30, 's', 5)]`, schema kept) is pinned by
  `test_saveastable_overwrite_is_insert_overwrite_not_replace`, which also asserts
  the recorded Spark cell and schema so it flips red when replace semantics land.
- L-01's registry row `ICE-V3-WRITE-DEFAULT-1-OVERWRITE-PART` moved DECLARED → FIXED
  with the round-5 pins. Which rows a name-only dynamic `PARTITION (k)` keeps is the
  already-DECLARED DML-1 residue ("repark always takes the dynamic path"; Spark's
  default STATIC mode replaces the whole table) — the pins assert RePark's retained
  rows and the Spark cell's written row, and the written row, filled `c` included,
  matches Spark.

## Roll-call — the moved "missing from the DataFrame" decision (ruling Q-21b-6)

Earlier rounds removed the facade's Python refusal of a DataFrame that omits a
target column (`missing from the DataFrame: ['c']`) and let the Rust fill decide.
For a defaulted column that fill writes the default. For a NULLABLE column with NO
default the refusal was removed **because Spark accepts**: measured tonight,
Spark 4.1.2 + Iceberg 1.11.0 writes NULL on both writer surfaces —
`df(40, 't').writeTo(nodef).append()` → `[[1, 'a', 1], [40, 't', null]]`
(`nodef_writeto_append_missing`) and
`df(41, 'u').write.mode("append").format("iceberg").saveAsTable(nodef)` →
`… [41, 'u', null]` (`nodef_saveas_append_missing`), with a full-width control
keeping its value (`nodef_writeto_append_full`, `[42, 'v', 7]`) and the schema
unchanged (`schema_after.nodef`). The removal matches Spark; it is not a
silent loosening.

Pinned on both surfaces: offline
`test_missing_nullable_no_default_accepts_and_nulls` replays all three cells
against the recorded `nodef` fixture; live `_live_rollcall` (inside
`test_live_write_default_parity`) builds `nodef` on live Spark, runs both
writers there, adopts the table into RePark and runs both writers again —
`REPARK_PARITY_LIVE=1 … /tmp/oc-worker/jb-jvm.sh .venv/bin/python -m pytest
python/repark/tests/test_ice_v3_write_default_1.py -q -p no:cacheprovider` →
**22 passed in 115.19s** (2026-09-17, release native). These pins were green on
the unfixed round-5 tree — the decision had already moved; the pins prove it.

## R-03 / R-04 — no-default write cost (step 6)

Code shape (the proof of C-019):

- R-03: `execute_merge` already builds the Arrow `write_schema`; `MergeSql::insert_sql`
  now takes it and `table_projection` uses it instead of calling
  `schema_to_arrow_schema` again, and it builds `ColumnDefaults` only when
  `schema_has_write_default` finds a primitive `write_default` — otherwise an empty
  map, no second Arrow conversion. Before: two Arrow conversions plus a HashMap build
  per NOT MATCHED clause. After: zero Arrow conversions and no map on a no-default
  table. `merge/mod.rs` held at its exact 1792-line baseline, `tests/merge.rs` at 1065.
- R-04: `insert_defaults::overwrite_source_with_defaults` (the one overwrite fill,
  step 2) returns the plain `SELECT * FROM (…)` source before any Arrow conversion
  when `schema_has_write_default` is false. Before: `column_defaults` (one Arrow
  conversion plus the map) plus a listed-name scan per field on every column-list
  overwrite. After: one pass over the fields' `write_default` options.

Measurement, DEFAULT release profile on both sides (no
`CARGO_PROFILE_RELEASE_CODEGEN_UNITS`): **before** = round-4 product code
(`36e722f1`, scratch worktree, own target dir and venv, `Finished release in
12m 50s`); **after** = this round's tree (`Finished release in 10m 20s`). Script
`/tmp/oc-worker/kb-wd/bench_nodefault.py` (memory catalog, v2 tables without
defaults, 20 warm-up statements, then N timed `session.sql(…).collect()` calls):
`MERGE INTO m … WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)` and
`INSERT OVERWRITE o (id, name) SELECT i, 'x'`. Logs:
`/tmp/oc-worker/kb-wd/bench.log`, `/tmp/oc-worker/kb-wd/bench-alternating.log`.

| Pass (load avg, 64 cores) | MERGE median before → after (ms) | OVERWRITE median before → after (ms) |
|---|---|---|
| r1, N=200 (~70–90) | 1420.38 → 180.29 | 920.30 → 75.95 |
| r2, N=200 (~80) | 78.76 → 111.53 | 38.91 → 62.31 |
| r3, N=100 (~89 → 139) | 129.88 → 1655.98 | 89.98 → 1615.54 |
| r4, N=100 (~167) | 1978.54 → (not run) | 1752.24 → (not run) |

Verdict: **no end-to-end difference is measurable on this box tonight.** Other
lanes held the load average between 70 and 167 on 64 cores, and the same binary
swung up to 18× between passes (before-MERGE 1420 ms in r1, 79 ms in r2). The
work removed is plan-time and O(columns) — two Arrow schema conversions and one
HashMap of a 3-column schema per statement, microseconds — far below that noise.
I stopped the alternating passes at r4 to protect the clock. The numbers above
are every sample taken, reported as measured; none supports a speed claim either
way. C-019 is PROVEN on the code shape and its pins (MERGE fill and no-fill paths
in `tests/insert_fill.rs` and `tests/merge.rs`; the no-default overwrite branch by
the 68 existing Spark-door overwrite tests, `cargo test -p repark-spark --lib
overwrite`, whose tables carry no defaults, and the defaulted branch by the
round-5 pins), not on a timing.

## Evidence

Spark oracle (live PySpark 4.1.2 + Iceberg 1.11.0, banner `spark=4.1.2 tz=UTC`,
`/tmp/ib-scratch/wh/p_wd_oracle` + `p_wd_oracle3.py`, truth at
`/tmp/ib-scratch/wh/wd_oracle/truth.json`, 20 cells). RePark column is this
tree unfixed (`/tmp/wd_repark_matrix.py`). `FILL(x)` = writes x; both refuse =
parity of refusal with different text.

| Shape | Spark 4.1.2 | RePark unfixed |
|---|---|---|
| INSERT column list omits `c` | FILL(5): `(3, 'c', 5)` | NULL: `(103, 'rc', None)` |
| MERGE NOT MATCHED omits `c` | FILL(5): `(4, 'd', 5)` | NULL: `(104, 'rd', None)` |
| `writeTo(t).append()` missing `c` | FILL(5): `(5, 'e', 5)` | refuses `missing from the DataFrame: ['c']` |
| `saveAsTable` append missing `c` | FILL(5): `(12, 'l', 5)` | refuses `missing from the DataFrame: ['c']` |
| `df.write.insertInto` missing `c` | refuses `INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS` | refuses (`Column count doesn't match`) |
| positional-short `VALUES (8, 'h')` | refuses `NOT_ENOUGH_DATA_COLUMNS` | refuses (`Inconsistent data length`) |
| `INSERT SELECT 9, 'i'` short | refuses `NOT_ENOUGH_DATA_COLUMNS` | refuses (`Column count doesn't match`) |
| `VALUES (10, 'j', DEFAULT)` | FILL(5) | refuses (`No field named default`) |
| explicit NULL full-width | NULL (`(14, 'n', None)`) | NULL |
| no-default nullable omitted (SQL + writeTo) | NULL | SQL NULL; writeTo refuses `missing` |
| string default `'hi'` | FILL(`'hi'`, incl. pre-add row) | NULL on new row |
| decimal default `3.14` | write OK; Spark read of the filled row ERRORS (`Cannot cast default value to long: 3.14`; physical parquet carries `3.14`) | NULL on new row |
| date `2024-10-04` + timestamptz defaults | FILL both | NULL, NULL on new row |
| write-default 7 / initial-default 5 | old row reads 5, new omitted write 7 | new row NULL |
| required `req INT NOT NULL`, no default | refuses both doors (`INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA`, `Cannot find data for the output column`) | INSERT refuses (Arrow `non-nullable but contains null`, pre-commit); writeTo refuses `missing` |
| v2 table + Java `addColumn` default | refused (`Invalid schema for v2: non-null default not supported until v3`) | n/a (no v2 table can carry a default) |
| Java `addRequiredColumn` no default | refused (`cannot add required column without a default value`) | n/a |

Plan ground truth (EXPLAIN on this tree): column-list omit plans
`Projection: column1 AS id, column2 AS name, Int32(NULL) AS c`; explicit
full-width NULL plans `column3 AS c` (source ref); full-width
`SELECT …, NULL` plans a bare `Int32(NULL) AS c` — identical to the
synthesized fill, so the fix must know the statement column list (AST), a bare
plan literal is not enough. Plain INSERT executes fork `IcebergWriteExec`
(DataFusion `insert_to_plan` → fork `insert_into`); the fork's
`apply_write_defaults` fills only MISSING columns, so an explicit NULL never
fills there. MERGE NOT MATCHED null-fills in RePark-owned
`write/merge/insert.rs::insert_projection` (`NULL AS c`).

## Round 6 gates

Run in the foreground on the round-6 head, release native
(`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`); full log in
`/tmp/oc-worker/kb-wd/handback-2.md`.

- `cargo test -p repark-iceberg --lib` — 459 passed, 0 failed.
- `cargo test -p repark-sql` — 20 test binaries, 443 passed, 0 failed (ANSI pins 8/8).
- `cargo test -p repark-spark --lib` — 1130 passed, 0 failed, 4 ignored (Spark-door pins 3/3).
- `test_ice_v3_write_default_1.py` offline — 24 passed, 1 skipped (the live cell).
- Live leg (`jb-jvm.sh`, `REPARK_PARITY_LIVE=1`, zulu-17, driver 2g) — 25 passed in 28.73 s,
  including `_live_overwrite_partitions` (C-020).
- `uvx ruff@0.15.22 check .` — all checks passed; `format --check .` — 1038 files formatted.
- `make rust-clippy` — exit 0 (C-024).
- C-009 guard + CAP-1 mirror — 25 passed.
- `comment_ban.py /tmp/kb-wd origin/main` — hits=0.
- `make verify` — see the hand-back (run last).

## Open questions

—

## Findings (out of scope, observed)

- F-001 (2026-09-17, read path, fork-or-adoption attribution open): a table
  whose head is a schema-only commit (Java `addColumn` with defaults, no
  snapshot after) reads pre-add rows as NULL on this engine while Spark 4.1.2
  reads the initial default. One RePark write (new snapshot on the post-add
  schema) flips the old rows to the default. Measured on the `defaults`
  fixture: adopted at v3 (snapshot schema 0) reads `(1, 'a', None)`; after
  `INSERT INTO t VALUES (99, 'z', 7)` the same rows read `(1, 'a', 5)`. The
  fixture works around it with one post-add Spark seed row per defaulted
  table. Untouched by this unit (write paths only); needs attribution before
  any fix. pins: none (finding, no clause).
- F-002 (2026-09-17, Spark behavior): `df.write.mode("overwrite").saveAsTable`
  on an existing table REPLACES it (2-column result), while this engine
  generates `INSERT OVERWRITE` keeping the schema — pre-existing divergence,
  untouched. pins: none (finding, no clause).
- F-003 (2026-09-17, round 2 step 5, DECLARED as
  `ICE-V3-WRITE-DEFAULT-1-OVERWRITE-PART`): partition-overwrite shapes do not
  fill omitted defaulted columns. Spark measured (banner `spark=4.1.2`,
  partitioned v3 table, Java-API default 5, probe `/tmp/ib-scratch/probes/p_write_default_ow.py`):
  `INSERT OVERWRITE t (id, name) VALUES (10, 'x')` replaces the table with
  `(10, 'x', 5)`; `df.select("id", "name").writeTo(t).overwritePartitions()`
  replaces only source partitions, filling `c = 5`
  (`[[10, 'x', 5], [20, 'y', 5]]`). RePark measured (release native, adopted
  fixture): `overwritePartitions` raises `ParseException: Expected: an
  expression, found: )` from the generated text — loud but wrong-shaped; both
  doors' PARTITION arms stage positionally with no fill (`*_overwrite.rs`
  `execute_partition_overwrite` has no `insert_defaults` call). ANSI
  whole-table `INSERT OVERWRITE` stays Q9-omitted (row DML-1, by ruling).
  Filling needs all four partition arms plus a partitioned defaulted fixture:
  not small, so DECLARED, not fixed.
- F-004 (2026-09-17, round 2 step 5): the ANSI-door `INSERT` fill had code
  (`router.rs` `delegate` → `fill_insert_plan`) but no pin on either level —
  found by reading the dispatch, fixed the same morning with
  `crates/repark-sql/tests/ansi_write_defaults.rs` (`2 passed`: fill
  `(4, 'd', 5)` plus the Spark-door-identical required refusal). The facade
  pin file drives the Spark door only (`ReparkSession.sql` is the Spark door;
  the ANSI door is top-level `repark.sql()` on the native session, which
  cannot adopt a defaulted table in-probe).

## Round-4 notes (2026-09-17, run 20b)

- Ruling Q-20b-7: this unit goes up as a DRAFT PR today; the branch stays in
  the cleanest committable state (one slice per commit, gates green).
- Remediated this round: the 14 added `///` lines are gone (exported `Result`
  entry points carry `#[allow(clippy::missing_errors_doc)]`, error contract in
  `write/map.md`); L-02 corrected (registry FIXED row + C-006 name `insertInto`
  with Spark's `insertInto_missing` refusal); R-01/R-02 fixed
  (`query_has_default_marker` probes the AST before any catalog load,
  `MarkerRewrite` threads the loaded table into `fill_insert_plan`, both doors
  updated, load-count pins in `insert_defaults/tests.rs`, tests split to
  `insert_defaults/tests.rs` + `insert_defaults/map.md` for the file-size
  ceiling).
- Open review items with dispositions (no code change this round):
  - (Superseded in round 5 by rulings Q-21b-3 … Q-21b-6: L-01, L-03, L-04, R-03
    and R-04 are closed as C-015 … C-019; L-02 was re-verified. The dispositions
    below are the round-4 record.)
  - L-01 OPEN: the dynamic PARTITION arm with a column list maps by name and
    null-fills unlisted nullable fields where Spark fills the write-default;
    the Spark door shape is a `ParserError` from the generated text.
    Disposition: needs re-scoping — parser support with a Spark cell
    (F-003 shapes), or a declared row.
  - L-03 OPEN: Spark-door `INSERT OVERWRITE … DEFAULT` never reaches
    `rewrite_insert_markers` (overwrite intercepts first) and the overwrite
    fill only appends omitted columns, so a present `DEFAULT` token fails.
    Disposition: fold `DEFAULT` into `overwrite_source_with_default_fills`
    (or call the marker pass from the overwrite path) and pin both shapes.
  - L-04 OPEN: `saveAsTable(mode=overwrite)` now fills omitted defaults with
    no Spark cell; the unit's own F-002 measurement says Spark replaces.
    Disposition: record a Spark Iceberg cell (fill vs replace) and pin it, or
    DECLARED next to F-002.
  - R-03 OPEN (P3): MERGE `table_projection` reconverts a schema
    `execute_merge` already holds. Disposition: pass the existing
    `write_schema` plus a once-per-MERGE `ColumnDefaults` into
    `insert_projection_with_defaults`.
  - R-04 OPEN (P3): `overwrite_source_with_default_fills` walks the schema on
    every column-list OVERWRITE even when it adds no fills. Disposition: a
    `write_default.is_none()` pre-scan before the Arrow conversion, as R-02.
- Round-4 gates (release native rebuilt after the Rust edits): `cargo test -p
  repark-iceberg --lib` 445 passed; `cargo test -p repark-sql` exit 0 (342 lib);
  unit file offline 14 passed, 1 skipped (live cell); `uvx ruff@0.15.22 check
  .` clean; workspace clippy `-D warnings` green; `make verify` red on ONE
  pre-existing finding only — `check-ledger-grammar` wants a
  `COVERAGE_ATTESTATION` block this ledger never carried on this branch (every
  branch version greps 0); all verify gates before it are clean. Writing that
  attestation is orchestrator-side SEPMO ceremony, out of this round.

## Round-2 notes (2026-09-17, run 20b)

- Step-1 comment grep over `git diff --cached` shows only the gate-required
  `/// # Errors` docstrings on the new `pub` fns (workspace clippy pedantic
  `missing_errors_doc`; AGENTS.md keeps required docstrings). Zero
  explanatory comments. Kept by contract precedence, disclosed here.
- Live-cell repair (`150a820a`, audit class stale pin): the live Spark table
  carries three rows after its own column-list insert fills `c = 5`; the
  adoption assertion now expects all three and the RePark replay inserts
  `(4, 'd')` for the fill. The two-row text was hand-written and never ran
  live (the cell skips without `REPARK_PARITY_LIVE=1`).
- Rebase onto `origin/main` (`225f68ee`): clean, `a003f9f5` skipped as
  predicted, no conflicts, no markers.
- Step-5 probes: `/tmp/ib-scratch/probes/p_write_default_ow.py` (live Spark)
  and `/tmp/p_repark_ow_probe.py` (release native, offline). Fixture metadata
  embeds absolute canonical paths — a probe must materialize at
  `/tmp/repark-ice-v3-write-default-1/ns`, never a private path.

## Build notes (2026-09-17, implementation round)

- Recovered the pre-compaction design from the working-tree diff after the
  compacted summary's file text proved stale: `insert_defaults` exposes
  `column_defaults`/`ColumnDefaults` (MERGE), `insert_column_list`,
  `rewrite_insert_markers` (DEFAULT keyword), `fill_insert_plan` (NULL-pad
  replacement). Reimplemented against DF54 (`Expr::Literal(_, None)`,
  `insert_to_plan` pads omitted columns as `Cast(Literal(Null))`) and the fork
  at `75da2b58` (`PrimitiveLiteral`, `write_default: Option<Literal>`).
- DAG adaptation: `repark-iceberg` cannot take `repark-core` (`check_crate_dag`
  allows core→iceberg only), so the doors resolve `(catalog, TableIdent)` via
  new pure helpers `insert_target` / `dml_target` and pass them in. ANSI door
  plans from SQL text, so `rewrite_insert_markers` returns the rewritten SQL
  for `delegate`; the Spark door plans the mutated statement.
- DataFrame writers funnel through generated SQL: `_by_name_projection` now
  emits a target column list omitting missing columns, so
  `INSERT INTO t (id, name) SELECT ...` reaches `fill_insert_plan`; `insertInto`
  stays positional and refuses; extra columns still refuse in Python.
- C-009 guard (`test_rp3_c009_write_default.py`) scans non-test `.rs` for
  `with_write_default` / `write_default(`: product code reads the field via
  `match &field.write_default` (no needle); schema-building tests live in
  `merge/tests/` (exempt path). Disposition: guard untouched, still green.
- Size gates: `merge/mod.rs` 1792 and `merge/tests/merge.rs` 1065 held exact by
  keeping the `insert_sql(index, table)` shape and moving the fill pins to new
  `merge/tests/insert_fill.rs`; `writer_readwriter.py` 1101→1095 via ordinary
  ratchet-down (dead missing-refusal detail removed).
