# Unit ledger — ICE-DYN-OVERWRITE-1 · dynamic partition-overwrite routing + overwrite race

**Date:** 2026-09-17 · **Branch:** `fix/ice-dyn-overwrite-1` · **Base:** `83c9b215`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** BUILD. Rust-first: the conf-value branch lives in `crates/repark-spark/src/insert_overwrite.rs`;
the facade only forwards the conf and marks `saveAsTable`.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Report row V2-24b (MISSING): with
`spark.sql.sources.partitionOverwriteMode=dynamic`, PARTITION-less `INSERT OVERWRITE` and
`write.mode("overwrite").insertInto` replaced the whole table while Spark replaces only the
touched partitions. K4 residue (V2-20a): under the default `snapshot` isolation an
`INSERT OVERWRITE` committing after a concurrent append silently removed the appended row
(`p_followup`: `deleted-records = 11`); Spark's rule in the same race was unmeasured.
Registry DML-1 (`docs/spark-sql-iceberg-parity.md:315-347`) left the conf out of its unit.

**Sources.** `/tmp/oc-worker/ice-rating/report.md` row V2-24b + §7 item 5;
`/tmp/oc-worker/ice-rating/worker-findings.md` §§V2-24b, K4/`p_followup`, #59, #62.
Probes copied to `/tmp/oc-worker/ja-dyn/probes/` (prefix rewritten to
`/tmp/oc-worker/ice-rating/scratch`; originals untouched). Repro:
`/tmp/oc-worker/ja-dyn/repro/repro_owmode.log`. Oracle recorder:
`/tmp/oc-worker/ja-dyn/record_spark_oracle.py` (+ `record_spark_race{2,3,4}.py`);
fixture `python/repark-parity/fixtures/torture/data/ice_dyn_overwrite_1/spark_oracle.json`.

**Build note (rules §14 vs brief).** The run rules name a frozen fork override
(`/tmp/oc-worker/run20a/fork-override.toml`, source at `e8db2ac`); the unit brief
explicitly orders this unit onto the PINNED fork (`75da2b58`, no `--config`). The frozen
source is a different rev, so the two orders conflict; the brief's explicit unit carve-out
wins over the run-generic rule. All builds and commits here use the pinned fork: the staged diff shows no
override stanza and no frozen-source path before every commit, and `Cargo.lock`
is untouched.

**Not in this unit:** fork edits (none needed — §C-014); `STATUS.md`; push; PRs; `gh`;
`insertInto` + `isolation-level` write option on the V1 door (unmeasured, Q2);
per-write serializable plumbing (refused surface stays table-property-only, Q3).

## PROPOSITION LEDGER — ICE-DYN-OVERWRITE-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Repro on the release native shows the defect: builder-conf dynamic + PARTITION-less overwrite yields `[(20,'b')]`; `PARTITION (p)` stays correct. | Repro log lines. | **PROVEN** | `/tmp/oc-worker/ja-dyn/repro/repro_owmode.log`: `builder rows: [(20, 'b')]` (whole table replaced), `builder PARTITION (p) rows: [(1,'a'),(3,'c'),(20,'b')]` (correct). `conf.set`/`SET` paths on the mixed tree (new facade, old native) refuse `unknown key` and keep `STATIC` — the 2026-09-16 `p_owmode_builder.log` already pins those paths as store-dynamic-but-replace-whole. |
| C-002 | Spark oracle matrix (one JVM, 4.1.2 + runtime 1.11.0): static replaces whole on all three doors; dynamic scopes SQL + `insertInto`, `overwritePartitions`/`PARTITION (p)` already scoped; v2 and v3. | Fixture cells. | **PROVEN** | `spark_oracle.json`: `static_sql/insertInto → [[20,'b']]/[[30,'c']]`; `dynamic_sql → [[1,'a'],[3,'c'],[20,'b']]`; `dynamic_insertInto → [[1,'a'],[2,'b'],[30,'c']]`; `dynamic_sql_v3`, `dynamic_insertInto_v3` same shapes; `dynamic_sql_summary` stamps `operation=overwrite`, `replace-partitions=true`, `changed-partition-count=1`, `deleted-records=1`. |
| C-003 | Unpartitioned table under dynamic: Spark replaces the whole table (both doors). | Fixture cells. | **PROVEN** | `dynamic_sql_unpartitioned → [[20,'b']]`, `dynamic_insertInto_unpartitioned → [[30,'c']]`. |
| C-004 | Empty source: Spark dynamic touches no partition (partitioned AND unpartitioned); static empty wipes. | Fixture cells. | **PROVEN** | `dynamic_sql_empty` + `dynamic_insertInto_empty` + `dynamic_sql_empty_unpartitioned` → `[[1,'a'],[2,'b'],[3,'c']]`; `static_sql_empty` + `static_sql_empty_unpartitioned` → `[]`. Matches the runtime bytecode: `DynamicOverwrite.commit` logs `Dynamic overwrite is empty, skipping commit` and returns with no commit. |
| C-005 | Evolved spec (unpartitioned → `ADD PARTITION FIELD p`) under dynamic: Spark keeps old files, adds the new partition. | Fixture cell. | **PROVEN** | `dynamic_sql_evolved_spec`: `evolved=true`, rows `[[1,'a'],[2,'b'],[3,'c'],[20,'b']]` — old unpartitioned files match no new-spec partition and survive. |
| C-006 | `saveAsTable(overwrite)` under dynamic: Spark replaces the WHOLE table (conf ignored). | Fixture cell. | **PROVEN** | `dynamic_saveAsTable → [[40,'c']]`. Consequence: RePark's `saveAsTable` (which lowers to `INSERT OVERWRITE` SQL) must pin static, or this unit would regress it — the `REPARK_STATIC_OVERWRITE` marker (registry row documents it). |
| C-007 | Conf plumbing matches Spark: builder/SET/`conf.set` all honored; value case-insensitive; bogus refuses at set with Spark's class and keeps the old value. | Fixture cells. | **PROVEN** | `builder_conf.get=dynamic` + scoped rows; `set_syntax.get=dynamic`; `conf_case`: `mixed_get=DyNaMiC` + scoped rows; `bogus_set=IllegalArgumentException: [INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS] ... It should be one of 'STATIC, DYNAMIC'. SQLSTATE: 22022`, `bogus_get=DyNaMiC`, later overwrite still dynamic. RePark's parse message mirrors that class verbatim. |
| C-008 | Spark default race (stale-base dynamic overwrite vs concurrent append): same-partition row silently removed, other-partition row survives, no error. | Disk-verified snapshot order + fresh-session read. | **PROVEN** | `race_default` (`oracle/race4.json`, warehouse `spark-wh-race4/ns/dflt`): disk `[seed 3, append 2, overwrite 2M added / 2 deleted]`; fresh read `neg=[(-1,'a')] total=2000003`. The (-2,'b') row is gone with no error — exactly the `p_followup` shape (`deleted-records = 11`). |
| C-009 | Spark serializable race (`.option("isolation-level","serializable")`): loud `ValidationException` naming the conflicting p=b files; table keeps seed + append. | Disk + error text. | **PROVEN** | `race_serializable`: disk `[seed 3, append 2]`, no overwrite snapshot; `ValidationException: Found conflicting files that can contain records matching partitions [p=b]: [.../p=b/....parquet, .../p=b/....parquet]` via `MergingSnapshotProducer.validateAddedDataFiles ← BaseReplacePartitions.validate`; fresh read `neg=[(-2,'b'),(-1,'a')] total=5`. |
| C-010 | Spark snapshot-option race behaves like the default. | Disk + fresh read. | **PROVEN** | `race_snapshot`: disk `[seed 3, append 2, overwrite 2M/2]`; fresh read `neg=[(-1,'a')] total=2000003`. |
| C-011 | Java rule recorded from the pinned runtime bytecode (no behavior invented). | `javap` excerpts in ledger. | **PROVEN** | `SparkWrite$DynamicOverwrite.commit`: empty → skip; else `newReplacePartitions` + `validateFromSnapshot` only when a write option supplies it; SERIALIZABLE → `validateNoConflictingData` + `validateNoConflictingDeletes`, SNAPSHOT → deletes only, absent (plain SQL) → neither. `isolationLevel()` reads only the `isolation-level` write option; `TableProperties` in 1.11.0 has `write.{delete,update,merge}.isolation-level` and no overwrite key. Same shape in `OverwriteByFilter`. |
| C-012 | Red first (Rust): the 3 dynamic routing tests fail on the unrouted tree with the defect's exact symptoms; controls pass. | Quoted output. | **PROVEN** | With only `insert_overwrite.rs` stashed: `dynamic_partition_less... FAILED left [(20,'b')] right [(1,'a'),(3,'c'),(20,'b')]`; both `dynamic_empty... FAILED left [] right [(1,'a'),(2,'b'),(3,'c')]`; static/unpartitioned-static/hint/race pins pass. See §Evidence. |
| C-013 | Red first (Python): the new battery fails on the pre-fix native; static controls pass. | Quoted output. | **PROVEN** | `3 failed, 3 passed, 1 deselected, 10 errors`: dynamic cells ERROR (`conf.set` refuses unknown key on the old native), mixed/bogus/builder FAIL (incl. `assert [(20,'b')] == [(1,'a'),(3,...),(20,'b')]`), static/saveAsTable pass. See §Evidence. |
| C-014 | Fix: PARTITION-less overwrite routes through `commit_replace_partitions_to` when the session conf is dynamic (case-insensitive), empty-dynamic is a no-op, unpartitioned-dynamic stays replace-all, `saveAsTable` pins static via marker. | Diff + green runs. | **PROVEN** | `crates/repark-spark/src/insert_overwrite.rs` (conf+hint read once in `execute_insert_overwrite`, threaded as `dynamic` into the stage-then-commit); carrier `crates/repark-core/src/partition_overwrite_mode.rs` (key const, parse, `PartitionOverwriteModeConfig`, build-map + ctx readers); `SparkExtension::configure` installs it; `set_runtime_config` serves it; facade forwards on set/unset/builder-reuse; `saveAsTable` appends `/* REPARK_STATIC_OVERWRITE */`. `tests::dyn_partition_overwrite` 8/8 green; pytest 16/16 offline green + live green (see §Gates). |
| C-015 | RePark default race == Spark default race (deterministic pin, no threads). | Rust test. | **PROVEN** | `snapshot_race_replaces_concurrent_same_partition_append`: stale handle + SQL append of `(9,'b')` + staged `(20,'b')` → commit ok, rows `[(1,a),(3,c),(20,b)]` (appended b-row gone, a-row kept). Mirrors fixture `race_default`. |
| C-016 | RePark serializable (table property) refuses the same conflict Spark refuses (deterministic pin). | Rust test. | **PROVEN** | `serializable_race_refuses_concurrent_same_partition_append`: `write.overwrite.isolation-level=serializable` + same staging → `expect_err` with `conflicting files`; rows keep seed + `(9,'b')`. Mirrors fixture `race_serializable`. No fork change: the fork already validates; the adapter already invokes it. Difference from Spark (per-write option vs table property) is a dated registry note, not a behavior gap on the SQL door (which has no isolation option on either engine). |
| C-017 | Registry: DML-1 gains a dated note pointing at the new DML-1B row; DML-1B FIXED with pins; race row DML-1C FIXED (default) with the serializable surface documented. | Row text + line numbers. | **PROVEN** | `docs/spark-sql-iceberg-parity.md` (lines in the registry commit). |
| C-018 | Gates green and clean greps. | Quoted outputs. | **PROVEN** | See §Gates. Comment grep and override grep print nothing on the branch diff. |
| C-019 | Round 3: `INSERT OVERWRITE … BY NAME`, the explicit column list and the positional shape match Spark's measured answer under both modes (v2 + v3, empty source, unpartitioned): 20 cells, rows and snapshot operations. | Fixture `spark_byname_dyn_oracle.json` replayed cell-by-cell by `test_ice_dyn_overwrite_1_by_name.py`; red on the pre-fix tree for exactly the dynamic `BY NAME` cells. | **PROVEN** | Red (§Round 3): `4 failed` — `v2/v3_dynamic_byname_touch_one` `[[9,'a','x']] != [[2,'b','old'],[9,'a','x']]`, `v2/v3_dynamic_byname_empty` `[] != [[1,'a','old'],[2,'b','old']]`; the other 16 cells (column list, positional, static, unpartitioned) green. |
| C-020 | One decision: the dynamic-vs-static read (`partitionOverwriteMode` conf and `force_static_overwrite`) lives in one function in `insert_overwrite.rs`, called by both `execute_insert_overwrite` and `execute_insert_by_name`; `BY NAME` passes its answer into `insert_overwrite_from_staged_source`. | Diff; no second conf parse. | **PROVEN** | `insert_overwrite.rs` `overwrite_is_dynamic(ctx, force_static_overwrite)`; `execute_insert_overwrite` and `execute_insert_by_name` each call it once; `router.rs` passes the typed flag into `execute_insert_by_name`; `grep partition_overwrite_mode_from_ctx crates/repark-spark/src` hits only that function. |
| C-021 | Rust: `BY NAME` under dynamic keeps sibling partitions with `replace-partitions=true`; `BY NAME` with an empty source commits no snapshot under dynamic and wipes under static; the static entry keeps `BY NAME` whole-table under a dynamic conf. | `tests::dyn_by_name_overwrite`, red then green. | **PROVEN** | Red (§Round 3): `2 failed` — touch-one `left [(20,'b')]`, empty `left []`; 4 controls ok. Green: 7/7 (the seventh, empty `BY NAME` on an unpartitioned table under dynamic, added with the fix per Q-21a-1). |
| C-022 | Rebase repair: `RuntimeConfig.unset` of `spark.sql.sources.partitionOverwriteMode` resets the native carrier to STATIC (the round-3 rebase kept `spark.sql.caseSensitive` in the native-restore tuple and dropped this key). | `test_unset_restores_static_on_the_live_session`, red then green. | **PROVEN** | Red (§Round 3): after `set dynamic` + `unset`, a positional overwrite still kept `(2,'b','old')`. Green after `builder_conf.py` puts the key back in the tuple. |
| C-023 | Registry DML-1B and `crates/repark-spark/src/map.md` name the `BY NAME` and column-list shapes and drop the "stays whole-table replace-all" claim. | Row text. | **PROVEN** | `docs/spark-sql-iceberg-parity.md` DML-1B round-3 sentences (repark, Spark, Pin); `crates/repark-spark/src/map.md` `insert_by_name.rs` row (the round-2 "stays whole-table replace-all" clause replaced) and `insert_overwrite.rs` row. |

## Open questions

- Q1 (RULES vs BRIEF, decided): run rules §14 orders the frozen fork override; the brief
  orders the pinned fork for this unit. Decided for the brief (explicit unit carve-out beats
  the run-generic rule; frozen source is a different rev). No approval needed; recorded here.
- Q2 (residue, not this unit): V1-door `insertInto`/`saveAsTable` with
  `.option("isolation-level","serializable")` — Spark honoring unmeasured, RePark V1 ignores
  Iceberg write options. Left to the V2-29 options surface; no invention.
- Q3 (documented, not asked): Spark serializable arrives per-write (DataFrame option);
  RePark serializable arrives per-table (`write.overwrite.isolation-level`). The SQL door has
  no isolation surface on either engine, so the two regimes never meet in one statement.
  Registry DML-1C dates this.
- Q4 (recommendation): if a driver later needs per-write serializable from Python, thread it
  as a second statement marker next to `REPARK_STATIC_OVERWRITE` into the commit's isolation
  level (the commit already branches on it). Not built here: no driver, no invented surface.

## Gates

```text
cargo test -p repark-spark --lib tests::dyn_partition_overwrite
  8 passed; 0 failed (routing, empty arms, unpartitioned arms, static hint, race twins)
cargo test -p repark-core --lib partition_overwrite_mode
  6 passed; 0 failed (parse/carrier/map/ctx unit tests)
cargo test -p repark-iceberg --lib
  434 passed; 0 failed (untouched crate, ordered by the brief)
pytest test_ice_dyn_overwrite_1.py -k "not live" (fixed release native)
  16 passed, 1 deselected
REPARK_PARITY_LIVE=1 pytest test_ice_dyn_overwrite_1.py::test_live_spark_matches_fixture
  1 passed (92s; repark == fixture == live Spark 4.1.2 + Iceberg 1.11.0)
pytest test_dml_b_partition_overwrite.py test_dml_c_truncate.py test_dml_subquery_parity.py
  test_insert_store_assign.py test_merge_insert_scope.py test_sql_dml_eager.py
  81 passed
pytest test_e2_readwriter.py test_writer.py test_writer_v2.py
  95 passed
pytest test_v3_acceptance_local.py test_v3_cow_dml.py test_v3_create_opt_in.py
  test_v3_dv_compaction.py test_v3_dv_container_close.py test_v3_legacy_delete_merge.py
  test_v3_lineage_columns.py test_v3_live_file_order.py test_v3_live_oracle.py
  test_v3_statement_coverage.py test_v3_upgrade.py
  121 passed, 94 skipped (live-gated skips)
make ci
  green (clippy -D warnings, fmt, crate-dag, lib-rs, file-size, lib-py,
  python-conventions, docstring-presence, manifest, ledgers, ledger-grammar,
  compaction, links, owner-ruling, parity dual-wire, matrix liveness, cargo check,
  py-lint, py-format-check, lock, toml, spell)
cargo test --locked --workspace
  green (56 test-result ok lines, 0 failed; exit 0)
comment grep on branch diffs: no output
override-name grep on every commit diff: no output
Cargo.lock: untouched (pinned fork throughout)
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-dyn-overwrite-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every proposition C-001…C-018 carries a quoted artifact above or a registry row; the Spark oracle is the spec and the repark pins read it from the checked-in fixture, never from hand computation. Red pasted on the unfixed tree on both tiers (Rust 3 route-cells, pytest 3 failed + 10 errors), green after.
      artifacts: [task/ledgers/staging/ice-dyn-overwrite-1-ledger.md, python/repark-parity/fixtures/torture/data/ice_dyn_overwrite_1/spark_oracle.json, python/repark/tests/test_ice_dyn_overwrite_1.py, crates/repark-spark/src/tests/dyn_partition_overwrite.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised on both engines: empty source (partitioned and unpartitioned), unpartitioned table, v2 + v3, evolved spec, all three conf paths, mixed-case and bogus conf values, saveAsTable under dynamic, and the race in three isolation regimes. No max/overflow surface exists (row counts, not arithmetic).
      artifacts: [python/repark/tests/test_ice_dyn_overwrite_1.py, /tmp/oc-worker/ja-dyn/oracle/race4.json]
    - id: AT-3
      status: ATTACKED
      evidence: Failure paths pinned loud: bogus conf refuses at set with Spark's OUT_OF_RANGE_OF_OPTIONS class and keeps the old value; serializable race refuses with the conflicting-files ValidationException on both engines; static-empty still wipes (Spark-equal) while dynamic-empty is a no-op; the empty-dynamic arm still runs the type check first, so a mistyped empty source refuses instead of silently passing.
      artifacts: [crates/repark-core/src/partition_overwrite_mode.rs, crates/repark-spark/src/tests/dyn_partition_overwrite.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The race is the unit: three disk-verified Spark interleaves (snapshot order read from metadata files, final rows re-read from a fresh session to dodge catalog-cache staleness, which bit twice during staging) plus deterministic stale-handle twins on the repark side (no threads — the handle is loaded before the concurrent SQL append by construction).
      artifacts: [/tmp/oc-worker/ja-dyn/oracle/race4.json, crates/repark-spark/src/tests/dyn_partition_overwrite.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, no credential, no network path: local memory catalogs and local warehouse dirs on both engines; the REPARK_STATIC_OVERWRITE mark is inert SQL-comment text matched verbatim, never interpolated.
    - id: AT-6
      status: ATTACKED
      evidence: Sibling data is byte-stable by construction (ReplacePartitions only rewrites touched partitions) and asserted by value on every cell; snapshot stamps asserted (operation=overwrite, replace-partitions=true); the evolved-spec cell pins that pre-evolution files survive on both engines.
      artifacts: [crates/repark-spark/src/tests/dyn_partition_overwrite.rs, python/repark/tests/test_ice_dyn_overwrite_1.py]
    - id: AT-7
      status: N/A
      justification: No new work per row and no new retained state: the routing reads one cached conf value per statement; staging still writes the source once. The 2M-row oracle overwrites ran bounded on local[4].
    - id: AT-8
      status: ATTACKED
      evidence: Error and contract surfaces mirrored, not presumed: the bogus-conf class copied verbatim from the live Spark message; the live test asserts the fixture GAV equals the pinned ICEBERG_SPARK_RUNTIME_GAV; the fork is untouched at its pinned rev (Cargo.lock identical, override grep clean); the Java validation rule quoted from the pinned runtime's own bytecode.
      artifacts: [python/repark/tests/test_ice_dyn_overwrite_1.py, task/ledgers/staging/ice-dyn-overwrite-1-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: Every new behavior is diagnosable from its output: the conf refusal names the key and the two legal values with Spark's class; the race refusal names the conflicting files; the registry rows DML-1B/DML-1C record the saveAsTable marker and the per-table vs per-write serializable difference with dates.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_ice_dyn_overwrite_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation spot-check by construction: stashing only insert_overwrite.rs flips exactly the 3 dynamic routing tests red with the defect's symptoms while the 5 characterization pins stay green; the pytest battery flips 3 failed + 10 errors on the pre-fix native. Branch liveness: dynamic, static, empty-dynamic, empty-static, unpartitioned-dynamic, and hint-marked arms each have a nameable input changing the committed output (one Rust test each).
      artifacts: [task/ledgers/staging/ice-dyn-overwrite-1-ledger.md, crates/repark-spark/src/tests/dyn_partition_overwrite.rs]
  complete: true
```

## Late errata (2026-09-17, before close)

- The static pin marker changed spelling during `make verify`: the trailing
  `REPARK_STATIC_OVERWRITE` form broke `py-format-check` against the exact-1101
  `writer_readwriter.py` baseline, so `saveAsTable` now embeds `/*RSOW*/` (Repark
  Static OverWrite) inside its verb — line-neutral, format-clean, and position-free
  for the Rust `contains` check. Earlier prose above naming the long form means this
  spelling; the registry and maps carry the final form.
- The new pytest names are lowercase (`test_insert_into_*`,
  `test_overwrite_partitions_*`, `test_save_as_table_*`) per `N802`; pins are
  unchanged (they live in docstrings).

## Evidence

Rust red (routing stashed, `cargo test -p repark-spark --lib tests::dyn_partition_overwrite`):

```text
test ...::dynamic_empty_overwrite_on_unpartitioned_table_leaves_table_unchanged ... FAILED
test ...::dynamic_overwrite_on_unpartitioned_table_replaces_whole_table ... ok
test ...::dynamic_empty_overwrite_leaves_table_unchanged ... FAILED
test ...::dynamic_partition_less_overwrite_replaces_touched_partition_only ... FAILED
test ...::static_partition_less_overwrite_replaces_whole_table ... ok
test ...::static_hint_pins_whole_table_replace_under_dynamic_conf ... ok
test ...::serializable_race_refuses_concurrent_same_partition_append ... ok
test ...::snapshot_race_replaces_concurrent_same_partition_append ... ok
  left: []
 right: [(1, "a"), (2, "b"), (3, "c")]
  left: [(20, "b")]
 right: [(1, "a"), (3, "c"), (20, "b")]
test result: FAILED. 5 passed; 3 failed
```

Python red (pre-fix native, `pytest test_ice_dyn_overwrite_1.py -k "not live"`):

```text
FAILED test_conf_mixed_case_dynamic_honored - repark.errors.IllegalArgumentException
FAILED test_conf_bogus_refuses_with_spark_class
FAILED test_builder_conf_dynamic_matches_oracle - assert [(20, 'b')] == [(1, 'a'), (3, ...), (20, 'b')]
ERROR (x10, dynamic fixture): conf.set refuses unknown key "spark.sql.sources.partitionOverwriteMode"
3 failed, 3 passed, 1 deselected, 10 errors
```

Oracle heads (full cells in `spark_oracle.json` + `oracle/race4.json`):

```text
CELL dynamic_sql: {"rows": [[1, "a"], [3, "c"], [20, "b"]]}
CELL dynamic_sql_summary: operation=overwrite replace-partitions=true changed-partition-count=1 deleted-records=1
CELL dynamic_saveAsTable: {"rows": [[40, "c"]]}
CELL conf_case: bogus_set=IllegalArgumentException: [INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS] ... one of 'STATIC, DYNAMIC'. SQLSTATE: 22022; bogus_get=DyNaMiC
CELL dflt: disk=[seed 3, append 2, overwrite 2M/2]; fresh neg=[(-1,'a')] total=2000003
CELL ser: disk=[seed 3, append 2]; ValidationException: Found conflicting files ... matching partitions [p=b]
```

## Round 2 (2026-09-17, ruling Q-20a-6, critic L-001 P1 + L-002 P2)

Ruling adopts both findings. L-001: `STATIC_OVERWRITE_HINT` removed from
`partition_overwrite_mode.rs` (+ root re-export); the `sql.contains` check in
`execute_insert_overwrite` replaced by a `force_static_overwrite: bool` carried
on a dedicated path — `saveAsTable` calls `_run_through_temp_view(...,
static_overwrite=True)`, which calls the native free `sql_static_overwrite`
(new `repark-python/src/static_overwrite.rs`; free function, EAGER-BUDGET
precedent, since `session.rs` sits on its exact CAP-1 baseline — the writer's
`session` handle is the native session, so a facade `SparkSession.sql` keyword
was tried and reverted) → `ReparkSession::sql_static_overwrite` (new
`repark-core/src/static_overwrite.rs` holding the shared SQL body, since
`session.rs` would cross its default ceiling) → `EngineContext::
force_static_overwrite` (default false in `new`) → `SparkDialect::execute` →
new `execute_static_overwrite` router entry → `execute_routed` →
`execute_time_travelled` → `execute_inner` → the overwrite arm. `execute` /
`execute_with_read_only` signatures unchanged. Size accounting: core
`session.rs` 995 → 973 (body moved out); `writer_readwriter.py` 1101 → 1109
with SSOT + CAP-1 mirror bumps (`session/session_core.py` holds 2290).

Red-first (commit `2ec780ad`): `dynamic_marker_inside_string_literal_stays_partition_scoped`
and `dynamic_trailing_line_comment_stays_partition_scoped` fail on the old
tree (marker text flips dynamic to static); green after the fix alongside the
replacement `static_entry_pins_whole_table_replace_under_dynamic_conf` and the
unchanged saveAsTable pytest cell.

L-002: `snapshot_race_replaces_concurrent_same_partition_append` now appends
`(9,'b')` and `(8,'a')` on the stale handle and asserts `(8,'a')` survives
(oracle `fresh_neg_rows`). Teeth shown by throwaway mutation (not committed):
committing the same staged state through `commit_overwrite_replace_all_to`
goes red with `left: []` — the pin catches any apply that drops more than
Spark. Per the critic's own trace, `ReplacePartitions` resolves deletes
against current live files, so whole-snapshot orphaning is the only apply
shape that loses `(8,'a')`.

Ledger moved back staging → completed in round 1; round 2 moves it back to
staging (a ledger reaches completed only at merge).

## Round 3 (2026-09-17, rebase onto `71482620`, ICE-RTAS-BYNAME-1 on main)

ICE-RTAS-BYNAME-1 (merged while this unit waited) added `insert_by_name.rs`, whose
overwrite arm calls `insert_overwrite_from_staged_source` with six arguments and whose
empty-source arm calls `commit_overwrite_replace_all_to` directly. On the rebased tree
that is E0061 at `insert_by_name.rs:128`; with the call site passing `false` the tree
compiles and `BY NAME` always replaces the whole table, and its empty arm wipes, whatever
the mode. Spark's measured answer (fixture `spark_byname_dyn_oracle.json`, recorded by
`record_spark_byname_dyn.py`, Spark 4.1.2 + Iceberg 1.11.0, v2 and v3 identical):

| statement | dynamic | static |
|---|---|---|
| `BY NAME SELECT 'x' AS v, 9 AS id, 'a' AS k` | `[(2,b,old),(9,a,x)]`, `append, overwrite` | `[(9,a,x)]`, `append, overwrite` |
| same, `WHERE false` | `[(1,a,old),(2,b,old)]`, no new snapshot | `[]`, `append, delete` |
| `(k, id, v) SELECT 'a', 9, 'x'` | `[(2,b,old),(9,a,x)]` | `[(9,a,x)]` |
| positional `SELECT 9, 'a', 'x'` | `[(2,b,old),(9,a,x)]` | `[(9,a,x)]` |
| `BY NAME`, unpartitioned table | `[(9,a,x)]` | `[(9,a,x)]` |

Rebase-resolution audit: `session_runtime.rs` serves both keys (case-sensitive then
overwrite mode, each with its own carrier writer); `extension.rs` installs both carriers
in `configure`; `builder_conf.py` `set` forwards both. `unset` did not: the resolved tuple
lists `SESSION_TIME_ZONE_KEY`, `SPARK_SQL_ANSI_ENABLED_KEY`, `SPARK_SQL_CASE_SENSITIVE_KEY`
and lost `PARTITION_OVERWRITE_MODE_KEY`, so `conf.unset` only tombstoned the Python store
and the live session stayed dynamic (C-022). The four map.md unions read correctly.

Red, Rust (call site passing `false`; `cargo test -p repark-spark --lib tests::dyn_by_name_overwrite`):

```text
test ...::static_by_name_empty_overwrite_wipes_table ... ok
test ...::static_entry_by_name_empty_wipes_under_dynamic_conf ... ok
test ...::dynamic_by_name_empty_overwrite_commits_nothing ... FAILED
test ...::dynamic_by_name_overwrite_replaces_touched_partition_only ... FAILED
test ...::static_entry_by_name_pins_whole_table_replace_under_dynamic_conf ... ok
test ...::dynamic_column_list_overwrite_replaces_touched_partition_only ... ok
  left: []
 right: [(1, "a"), (2, "b"), (3, "c")]
  left: [(20, "b")]
 right: [(1, "a"), (3, "c"), (20, "b")]
test result: FAILED. 4 passed; 2 failed
```

Red, Python (release native built from the same tree;
`pytest test_ice_dyn_overwrite_1_by_name.py -n 8`):

```text
FAILED ...::test_by_name_overwrite_matches_oracle[v2_dynamic_byname_touch_one]
FAILED ...::test_by_name_overwrite_matches_oracle[v3_dynamic_byname_touch_one]
FAILED ...::test_by_name_overwrite_matches_oracle[v3_dynamic_byname_empty]
FAILED ...::test_by_name_overwrite_matches_oracle[v2_dynamic_byname_empty]
FAILED ...::test_unset_restores_static_on_the_live_session
  assert [[9, 'a', 'x']] == [[2, 'b', 'old'], [9, 'a', 'x']]
  assert [] == [[1, 'a', 'old'], [2, 'b', 'old']]
  assert [[2, 'b', 'old'], [9, 'a', 'x']] == [[9, 'a', 'x']]
5 failed, 16 passed
```

The column-list and positional cells were already green: both reach
`execute_insert_overwrite`, which reads the mode. Only the `BY NAME` module skipped it.
The DataFrame door needs no new cell: `writeTo(t).overwritePartitions()` and
`insertInto(overwrite=True)` are already pinned (C-002, C-003, C-006) and neither lowers
to `BY NAME`.

Green (after the fix, `2b57cd25`):

```text
cargo test -p repark-spark --lib tests::dyn_
  17 passed; 0 failed (7 dyn_by_name_overwrite + 10 dyn_partition_overwrite)
pytest test_ice_dyn_overwrite_1_by_name.py test_ice_dyn_overwrite_1.py -n 8 (release native)
  37 passed, 1 skipped (live-gated)
```

Oracle rows, repark vs Spark after the fix (v2 and v3 each):

| row | dynamic | static |
|---|---|---|
| `BY NAME` touch one | MATCHES (rows + `append, overwrite`) | MATCHES |
| `BY NAME` empty | MATCHES (rows, no new snapshot) | MATCHES (`[]`, `append, delete`) |
| column list | MATCHES | MATCHES |
| positional | MATCHES | MATCHES |
| `BY NAME` unpartitioned | MATCHES | MATCHES |

Rulings:

- **Q-21a-DYN-1 (empty `BY NAME` under dynamic on an unpartitioned table).** The brief
  names the no-op for a partitioned table. The oracle holds no unpartitioned-empty
  `BY NAME` cell, but C-004 measured the positional shape
  (`dynamic_sql_empty_unpartitioned` unchanged), and the runtime's
  `DynamicOverwrite.commit` skips an empty commit without looking at the spec; Spark
  resolves `BY NAME` onto the same V2 dynamic overwrite. Decided: dynamic-empty is a
  no-op on every table, the same arm `execute_insert_overwrite` already has, so one
  decision covers both modules. Pinned in Rust
  (`dynamic_by_name_empty_overwrite_on_unpartitioned_table_commits_nothing`).
- **Q-21a-DYN-2 (DataFrame door).** No new DataFrame cell: `insertInto` and
  `writeTo().overwritePartitions()` never lower to `BY NAME` (no `BY NAME` text in
  the facade), and their mode behavior is already pinned by C-002, C-003 and C-006.
- **Q-21a-DYN-3 (`unset` tuple).** The rebase repair (C-022) is in scope: it is part of the
  conflict resolution this round was asked to check, and the existing `dynamic`
  fixture in `test_ice_dyn_overwrite_1.py` leans on `unset` to restore STATIC.

Round-3 gates (HEAD after the `wipe_by_name_target` split):

```text
comment_ban origin/main..HEAD                      hits=0
make rust-clippy (all-targets, -D warnings)        clean
cargo clippy --all-targets --all-features -D warnings (bare, clippy.toml disallowed-methods live
  on test code): red before reaching this branch's crates — 36 hits in repark-ml tests, and
  ~4650 across repark-core/functions/iceberg tests with repark-ml excluded; none in a file
  this branch touches. The repo splits that list into rust-panic-ban (lib+bins).
cargo test -p repark-spark --lib                   1142 passed; 0 failed; 4 ignored
make verify                                        exit 0 (57 test-result ok lines, 0 failed)
pytest python/repark/tests -n 8 (release native, pre-split tree)
  9719 passed, 5 failed, 399 skipped, 47 xfailed. Failures: q14 current_date x2 (known,
  after 20:00 EDT); test_stack_is_linear_in_columns (timing under load avg ~75, passes
  alone); test_production_file_size x2 (branch drift since round 1, fixed in 90a5a0d2)
pytest unit files + rtas_byname + production_file_size + dml_b + writer (release native at HEAD)
  117 passed, 2 skipped, 5 xfailed
```

## Run 21a close-out (2026-09-18)

The round-3 rulings above are renumbered Q-21a-DYN-1…3 so they do not collide with the run's
orchestrator rulings. Q-21a-DYN-1 (a dynamic empty `BY NAME` source commits nothing on an
unpartitioned table too) was measured afterwards on Spark 4.1.2 + Iceberg 1.11.0: v2 and v3,
dynamic keeps both rows with no new snapshot, static empties the table with `append, delete`.
The four cells are in `spark_byname_dyn_oracle.json` and replayed by
`test_ice_dyn_overwrite_1_by_name.py` (25 of 25).

Verification critic (Grok 4.6, on `4414c184`): **PASS**, no P1 or P2. The typed
`force_static_overwrite` flag, the `BY NAME` dynamic arm, the empty-dynamic no-op and the
`unset` repair each have a pin that fails when the fix is reverted, and no statement-text
marker is left on the decision path. Its five P3s are recorded here, not fixed:

- V-01 — parts of the ledger above still describe the in-band marker as the mechanism; the
  history is kept as written, and ruling Q-20a-6 records the typed flag that replaced it.
- V-02 — SQL `SET` / `RESET` of `spark.sql.sources.partitionOverwriteMode` has no dedicated
  pin; the conf path is pinned through `conf.set` / `conf.unset`.
- V-03 — no dynamic-overwrite × branch-target pin.
- V-04 — the race pins act at the commit layer; Spark's `race_snapshot` cell is not replayed
  as a RePark test.
- V-05 — the `insertInto` docstring predates the typed flag.

Orchestrator gates on the rebased squash (`main` `e980d945`, release native with
codegen-units 16): comment ban `hits=0`; facade `9726 passed, 2 failed` (only the q14
`current_date` pair, which fails locally between 20:00 and 24:00 EDT); parity `756 passed`;
`cargo test -p repark-spark --lib` green; `make verify` rc 0.
