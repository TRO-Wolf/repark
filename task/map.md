# map — task/

## Purpose

Working state for **current** work: the rules in force, the ledger of each unit in flight, and the
acceptance inputs that gates still read. Finished **phases and campaigns** do not accumulate here —
they are archived under [../docs/history/](../docs/history/map.md) once their rules have been
promoted to a current document (mid-campaign phase promotions are allowed; see hardening-h1).

Current state (release, delivered surface, what happens next) is **[../STATUS.md](../STATUS.md)**,
not this directory.

## Contents

- [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) — **the Spark function parity campaign's
  scope audit and approval gate (2026-08-20):** the twelve-clause proposition ledger, the spike
  evidence behind it, and the one `OPEN` clause (C-007 — whether the four sub-project families
  are built or declared absent-and-loud). No campaign unit opens until this gate passes. Design:
  [../docs/design/spark-function-parity.md](../docs/design/spark-function-parity.md); slate:
  [../briefs/spark-function-parity.md](../briefs/spark-function-parity.md).
- [fnp-0-census/](fnp-0-census/map.md) — the measured evidence that gate rests on: the facade
  classification of all 345 functions, the PySpark 4.1.2 gap partition, the higher-order-function
  spec, and the kernel ownership map.
- [df1-rust-flatten-ledger.md](df1-rust-flatten-ledger.md) — **DF1 native
  `dynamic_flatten`:** port of the Python Spark-facade planner into
  `repark_core::dynamic_flatten` (plan rewrite, no new physical operator). Thin
  PyO3 + type-gate facade; `_dynamic_flatten_unnest_structs` deleted.
- [c25-bugfix-ledger.md](c25-bugfix-ledger.md) — **conductor-25 (2026-08-17):**
  DS-4 / F-4 bug-candidate fix round (B4→B1→B6→B2→B3→B5) plus escaped DF-2.
  Single ledger, appended per PR. B4 (#175) CLOSED. **DF-2:** `explode_outer`
  struct-element CAST + `dynamicFlatten(empty_as_null=True)` default (branch
  `grok/c25-df2-outer-flatten`; SQM #176 V-2: void `make_array(NULL)`).
  **G3b (2026-08-18):** angle-spelling CAST for nested arrays (the postfix `[]`
  mis-bind that refused GA4 `items[].item_params[]`), troubleshooting-doc
  polarity truth-up, audit-ignore rationale re-derivation, and
  `createDataFrame` honoring `NullType`/`array<void>` instead of silently
  substituting string. B1 stays queued behind DF-2.
- [fn-gt1-ledger.md](fn-gt1-ledger.md) — **FN-GT1 retro / GT1-FIX (2026-08-18,
  round-4 2026-08-19):** ColumnOrName wiring (G1–G8), oracle pins (P1–P5),
  fence (F1–F3), `lit_indices` sweep, door-side `regexp_*` / length refuse /
  F-5–F-7, then R4-1 positional mid-surrogate probe. Live PySpark 4.1.2.
  0.4.0 release gate.
- [fn-gt2-ledger.md](fn-gt2-ledger.md) — **FN-GT2 (2026-08-17):** leftover
  datetime/collections/url/bitmap thin-wires + SQM rework (W1–W5 / P1–P5 / R1)
  on `grok/c20-fngt2-thin-wire` (PR #174).
- [f3-docstring-ledger.md](f3-docstring-ledger.md) — **F-3 (2026-08-17):** public-docstring
  backfill across the facade. Carries the AST census before/after per file (1117/1210 →
  1199/1210; 82 docstrings, 101 added lines, 0 deleted) and the measured `repark.polars`
  `.str`/`.dt` divergences that the new text states. The 11 names it deferred (blocked on
  `core.py`'s line ceiling) were **closed in F-4 increment 2 (2026-08-17)** once the SE-1 PR-B
  extract freed headroom: census 1211/1211, with the honest caveat that nine are nested
  rendering closures and two are `@overload` typing stubs, so the *user-facing* API surface was
  already complete. No ceiling was raised at any point.
- [c18-datasets-ledger.md](c18-datasets-ledger.md) — **conductor-18** torture-dataset
  workstream (DS-1..DS-4, complete). Generators under
  `python/repark-parity/datasets/`; data stays in the cache root. Single ledger, appended
  per increment. DS-4 closes it with the facade pins
  (`python/repark/tests/test_datasets_facade.py`) and the tour notebook under
  [../examples/map.md](../examples/map.md).
- [c17-explode-case-ledger.md](c17-explode-case-ledger.md) — **U-DF-1
  (2026-08-16):** string-form `explode` / `explode_outer` (and
  `dynamicFlatten` list pass) rebind a single-ident source through the
  frame schema so createDataFrame mixed-case fields (`Legs`) resolve.
  Helper `_bound_generator_array` in `column.py`; `core.py` net-zero.
- [se1-declared-sorted-ledger.md](se1-declared-sorted-ledger.md) — **SE-1 (2026-08-16):**
  declared-sorted temp views (verify-always → `MemTable::with_sort_order` → window
  `SortExec` elision; probe: 1→0 at tp=1 AND through the hash repartition at tp=default,
  results byte-identical). PR-A engine seam; **PR-B (2026-08-17)** lands the facade
  `declareSorted` door + the `plan_collapse.py` headroom extract that made room for it.
  **PR-D1 (2026-08-17):** `tightenNulls` (c+) — verified-null-free keys flip to
  non-nullable; Iceberg CREATE of a tightened frame refuses until PR-D2.
  **Round 3:** R-A cache remint stamp, R-B subquery walk, R-C right-side
  marker, R-D refuse predicate.
  **Round 4 (post-rebase, Y-1..Y-8 + CEIL-1):** closes the DDL-SINK bypass on both
  doors (`CREATE VIEW cat.ns.v` / `SELECT … INTO cat.ns.t` persisted required keys —
  measured on BASE) via `refuse_iceberg_create_of_tightened_ddl`; makes the
  `get_logical_plan` recurse a live pinned branch; relabels three non-discriminating
  pin claims with MEASURED tables; runs the two NOT-RUN verifiers (P-3 cache cell,
  P-5 List/Map child requiredness); and pays back the `core.py` lib-py ceiling with a
  MOVE-ONLY extract (ceiling NOT raised). Records one payload finding: `CREATE VIEW`
  in an Iceberg catalog persists a TABLE, predating this branch and NOT fixed here.
  **Round 5 (Z-1..Z-7):** the ALTITUDE fix — one shared pre-execute belt
  (`repark-core/src/pre_execute.rs`, `PreExecute` plan → guard → execute) that every door
  passes through, because per-door wiring twice missed the NATIVE door (`DataFusionDialect`
  persisted tightened `CREATE VIEW` / `SELECT … INTO`, measured Ok on BASE); the DDL gate now
  reads the RESOLVED catalog, so `SET datafusion.catalog.default_catalog` cannot launder a
  bare name; the ANSI CTAS derivation plans before executing (its eager `ctx.sql` published an
  inner DDL sink AND returned Ok, measured); the two NOT-RUN verifiers are run (visit-budget
  overflow reachability, nested export strip); and the round-4 node counts / `core.py` line
  numbers are trued to the composed head.
  **Round 6 (R6-1..R6-7):** temp-view WRITES become session-local via the
  `repark-core/src/temp_view.rs` seam (`TempViewHome` = build-time name + provider handle,
  `assert_home_intact`), so a `SET` of the default catalog can no longer persist a "temp" view
  into an Iceberg catalog.
  **Round 7 (R7-1..R7-3):** the READ half — a one-part name that is a session-local view now
  resolves to its HOME-qualified spelling on every facade product path
  (`repark/spark/_temp_views.py` + the `resolve_table_name` temp arm), because a bare reference
  follows the LIVE default catalog and under a `SET` missed views `tableExists` reported present
  (measured on BASE). Raw user-typed SQL bodies on the native door keep DataFusion resolution by
  decision. Two number truth-ups (the round-4 facade count, the `check_rust_file_size.py`
  ratchet-out comment) and one DISCLOSED round-8 residual: the engine crates' own bare scratch
  registrations (`repark-iceberg` MERGE / identity DML, `__repark_tt_*` time travel) stay red
  under that same `SET` — measured equally red on BASE, so unchanged by this round.
- [rsix-rsi-sma-iter-ledger.md](rsix-rsi-sma-iter-ledger.md) — **T5
  (2026-08-15):** measurement-funded safe iterator rewrite of `rsi` and
  `sma` kernels only (P-3 `safe_iter`: RSI +5.15%, SMA +1.16%,
  `f64::to_bits` bit-exact). Loop-form, not math. No `unsafe`.
- [m11f-cardinality-exempt-ledger.md](m11f-cardinality-exempt-ledger.md) — **M11
  (2026-08-15):** skip MERGE cardinality for a lone unconditional MATCHED DELETE.
  Shared-executor fold/consume helpers; native-door pins + differential flip
  `dup_source_keys_unconditional_delete` split → content. BL-3 left for orchestrator.
- [colx-column-extract-ledger.md](colx-column-extract-ledger.md) — **COLX
  (2026-08-15):** extract non-pymethods helpers from `column.rs` →
  `column/{mod,window,expr_build}.rs`; ratchet file-size EXCEPTIONS DOWN.
  `multiple-pymethods` stays off.
- [udfx-udf-split-ledger.md](udfx-udf-split-ledger.md) — **UDFX (2026-08-15):**
  split `crates/repark-ta/src/udf.rs` into `udf/mod.rs` + per-family dispatch
  modules (`overlap`/`momentum`/`volatility`/`volume`/`price`). Zero numeric
  change. EXCEPTIONS key moved and ratcheted DOWN.
- [s1-spill-truth-ledger.md](s1-spill-truth-ledger.md) — **S-1 (2026-08-15):**
  FairSpillPool is the one truth for `datafusion.runtime.memory_limit` (R1
  runtime SET swap + R2 temp_directory + R3 RAM-relative default).
- [p1-ta-kernel-benches-ledger.md](p1-ta-kernel-benches-ledger.md) — **P-1
  (2026-08-15):** criterion kernel baseline for `repark-ta` (`ema`/`sma`/`rsi`/
  `bbands` + volume `ad`/`adosc`/`obv`/`mfi` at n=1e6; BBANDS cold / three-
  sibling / cache-hit shape). Measure-only; no `src/` edits.
- [m14-abort-cleanup-ledger.md](m14-abort-cleanup-ledger.md) — **M14 (2026-08-15):**
  rejected MERGE commit abort-deletes writer-result files (design A). Cleanup
  inside `commit` / `commit_overwrite` / `commit_row_delta` /
  `commit_row_delta_kind`. Pins in `occ_conflict_tests.rs`.
- [fn-w-window-fns-ledger.md](fn-w-window-fns-ledger.md) — **FN-W
  (2026-08-15):** `lag`/`lead`/`nth_value`/`percent_rank`/`cume_dist` facade
  window names. Rust grant = `column.rs` only (`window_udwf`, no i32 cast).
  `_PRE_SPLIT_ALL` 291→296. `ignoreNulls` honest-cut. `column.rs` 2200/2200.
- [bl4-update-store-assign-ledger.md](bl4-update-store-assign-ledger.md) — **BL-4
  (2026-08-15):** MERGE `WHEN MATCHED UPDATE SET` ANSI store-assignment gate.
  Same `ansi_store_assignable` matrix as INSERT. Probe assignments before the
  rewrite `CASE` (CASE unification would hide the needle). Both doors.
- [c19-al1a-mimalloc-ledger.md](c19-al1a-mimalloc-ledger.md) — **AL-1a
  (conductor-19, 2026-08-16):** feature-gated mimalloc global allocator in
  `repark-python` (default off; no wheel wire). File-backed `src/allocator.rs`.
- [c19-bh1-default-conf-ledger.md](c19-bh1-default-conf-ledger.md) — **BH-1
  (conductor-19, 2026-08-16):** TA bench PRIMARY cells run at default conf
  (tp unset; emit `target_partitions=default`). Isolation cells emit
  `isolation=single_core`. Measure-only; no engine edits.
- [p2-ta-pipeline-benches-ledger.md](p2-ta-pipeline-benches-ledger.md) — **P-2
  (2026-08-15):** Python TA pipeline benches (§8.1–8.5, §8.7). Measure-only;
  no engine edits. §8.6 cited as #116 (TA-1), not rebuilt. Scripts in
  `python/repark-parity/bench/ta/`. Numbers stay planning-side.
- [ta1-sql-fusion-ledger.md](ta1-sql-fusion-ledger.md) — **TA-1 (2026-08-15):**
  SQL same-OVER `WindowAggExec` fusion pin on both doors (`ta_window.rs` Spark,
  `ta_toll.rs` ANSI+TaExtension). Named `OVER w` and inline same-spec = 1;
  intervening filter between two live windows = 2. Test-only; no engine edits.
- [occ1-merge-isolation-ledger.md](occ1-merge-isolation-ledger.md) — **OCC-1
  (2026-08-15):** MERGE reads `write.merge.isolation-level` (M13) + M15
  AlwaysTrue doc-truth. Resolver copies DML semantics (no trim). Snapshot
  drops data-conflict validation. Pins in `occ_tests.rs` (M19-A S5 split).
- [m16-posdelete-specid-ledger.md](m16-posdelete-specid-ledger.md) — **M16
  (2026-08-15):** evolved unpartitioned `partition_spec_id` on position
  deletes. Spec 0 partitioned → spec 1 unpartitioned; MoR MERGE delete
  was stamped spec 0 and loud-failed commit. Fix: `.with_partition_spec`
  on the unpartitioned-but-not-spec-0 branch. Region:
  `position_delete.rs` only.
- [ta2-with-indicators-ledger.md](ta2-with-indicators-ledger.md) — **TA-2
  (2026-08-15):** `ta.with_indicators` serving helper. Required keyword-only
  `partition`/`order` (cross-symbol RSI footgun). Fused `over_columns` window +
  optional last-bar-per-partition (`row_number`/`max`). No engine edits.
- [occ2-conflict-batteries-ledger.md](occ2-conflict-batteries-ledger.md) — **OCC-2
  (2026-08-15):** M19/M20 conflict batteries B/C/E/F/G/H/I + DELETE/UPDATE
  isolation-property pins. Test-only; engine frozen at `cd0db4f`. Independent
  of OCC-1 #117.
- [ta4-volume-kernels-ledger.md](ta4-volume-kernels-ledger.md) — **TA-4
  (2026-08-15):** volume-family kernel port (`ad`/`adosc`/`obv`/`mfi`) in
  `crates/repark-ta/src/volume.rs` + SPECS/facade + `to_bits` goldens.
- [ta3-volume-goldens-ledger.md](ta3-volume-goldens-ledger.md) — **TA-3
  (2026-08-15):** volume-family goldens (`ad`/`adosc`/`obv`/`mfi`) + C-source
  recon. Recorder + additive `.bin` only; no kernel port (TA-4).
- [fn-f-try-bitwise-ledger.md](fn-f-try-bitwise-ledger.md) — **FN-F
  (2026-08-15):** 10 try/session/bitwise facade names in
  `functions_bitwise.py` + `functions_session.py` + `test_functions_f.py`.
  `_PRE_SPLIT_ALL` pin moved 253→263. Deferred: remaining try_* /
  to_number/to_binary + assert_true. **FN-GT1 shipped** bit_count/getbit/shift*.
- [q10-timestamptype-ledger.md](q10-timestamptype-ledger.md) — **Q10
  (2026-08-15):** `spark.sql.timestampType` LTZ default + NTZ opt-in. Session
  conf + door type-resolution (literals / CAST / DDL). Existing default-mode
  pins untouched.
- [a11-ansi-ns-reject-ledger.md](a11-ansi-ns-reject-ledger.md) — **A11
  (2026-08-15):** ANSI-door column-def `CREATE TABLE` refuses nanosecond
  timestamps at DDL time (column + precision 9 + `TIMESTAMP(6)`). `TIMESTAMP(6)`
  positive control unchanged. Spark door documented, not changed. CTAS / ALTER
  / write-path / fork closed.
- [mg1-scanprune-hardening-ledger.md](mg1-scanprune-hardening-ledger.md) — **MG-1
  (2026-08-15):** MERGE scan-prune / residual-probe hardening (M1 type-domain
  skip-conjunct, M5 char-boundary scanners, M6 probe-failure continue, M7
  case-insensitive source resolve-then-quote). Region: `scan_prune.rs` + thin
  `residual_join_key_filter`.
- [mg2-lowering-strictness-ledger.md](mg2-lowering-strictness-ledger.md) — **MG-2
  (2026-08-15):** MERGE lowering strictness. Both doors refuse Oracle-style
  action sub-predicates (M2), misqualified / nested assignment targets (M3),
  and non-last unconditional same-kind clauses (M10). Spark door adds the ANSI
  INSERT column-list needle (M8). Door validation only; engine untouched.
- [fn-c-aggregates-ledger.md](fn-c-aggregates-ledger.md) — **FN-C
  (2026-08-15):** 8 aggregate aliases/shims in `functions_agg.py` +
  `test_functions_c.py`. `_PRE_SPLIT_ALL` pin moved 253→261. Deferred:
  lag/lead/nth_value/percent_rank/cume_dist (A8) +
  sum_distinct/approx_count_distinct + charter ENGINE-WORK names.
- [fn-a-ordering-null-math-ledger.md](fn-a-ordering-null-math-ledger.md) — **FN-A
  (2026-08-15):** 25 ordering/null/math facade names in `functions_expr.py` +
  `test_functions_a.py`. `_PRE_SPLIT_ALL` pin moved 207→232. Deferred:
  typeof/bround/conv + asc_nulls_last/desc_nulls_first.
  **FN-GT1 shipped** rint/factorial/bin/hex/unhex.
- [fn-e-collections-ledger.md](fn-e-collections-ledger.md) — **FN-E (2026-08-15):**
  9 collection facade names in `functions_collections.py` + `test_functions_e.py`.
  `_PRE_SPLIT_ALL` pin moved 253→262. Deferred: map_from_entries/shuffle/create_map
  + array_compact/element_at + charter higher-order/JSON/generators.
- [fn-b-strings-ledger.md](fn-b-strings-ledger.md) — **FN-B (2026-08-15):** 21
  string facade names in `functions_expr.py` + `test_functions_b.py`.
  `_PRE_SPLIT_ALL` pin moved 207→228. Deferred: regexp_extract_all/regexp_substr
  + to_char/to_varchar. **FN-GT1 shipped** split_part/regexp_count/regexp_instr/
  bit_length/octet_length.
- [fn-d-datetime-ledger.md](fn-d-datetime-ledger.md) — **FN-D (2026-08-15):** 11
  datetime facade names in `functions_datetime.py` + `test_functions_d.py`.
  `_PRE_SPLIT_ALL` pin moved 253→264. Deferred: make_date/interval/dt_interval/
  unix_micros + date_diff/localtimestamp/to_timestamp_ntz + charter ENGINE-WORK.
- [r6-namespace-guard-ledger.md](r6-namespace-guard-ledger.md) — **R-6 / G-6 Q1
  (2026-08-14):** namespace-create location guard. Core `create_namespace` +
  Spark `IF NOT EXISTS` + ANSI `CREATE SCHEMA IF NOT EXISTS` share
  `refuse_contradictory_namespace_location`. Matching / no-location stay
  idempotent; conflict fails loud naming both paths. Registry/STATUS closed
  (§6 handoff).
- [r3-g8-absences-ledger.md](r3-g8-absences-ledger.md) — **R-3 / G8 absence pins
  (2026-08-14):** the four S-2 pin-absences flip to Tested — `SEMANTICS_JOIN_NULL_KEYS`
  both doors (INNER/LEFT/SEMI/ANTI), ANSI `SEMANTICS_WINDOW_FRAMES` (ROWS/RANGE + DATE
  unit-less months), ANSI `SEMANTICS_FLOAT_DETERMINISM` (G7 `f64::to_bits` twins).
  Vocabulary stays at 50. Registry / STATUS closed (A4; §6 paste-true).
- [r4-tz8-ledger.md](r4-tz8-ledger.md) — **R-4 / TZ-8 (2026-08-14):** `CAST(ts AS DATE)` /
  `to_date` read the session zone (LTZ → session calendar; NTZ → stored wall). `datediff`
  rides CAST. `last_day` / `date_add` over TIMESTAMP stay residual. Does **not** edit
  the registry (R-7 deferred; §6 paste-true).
- [r1-g3e8-pr4-ledger.md](r1-g3e8-pr4-ledger.md) — **R-1 / G3-E8 PR-4:** identity
  `UPDATE … SET <scalar> WHERE col IN (SELECT …)` + correlated
  `DELETE … IN` (recorded ≡ EXISTS) through the A1-identity path. ANY/ALL
  deferred (Spark 4.1.2 parse-fails quantified comparisons). ROW 9 restated as
  the permanent v1 valve. Does **not** edit the registry (R-7 deferred; §6
  paste-true).
- [r2-dec-close-ledger.md](r2-dec-close-ledger.md) — **R-2 / DEC close (2026-08-14):**
  U4b Spark `/` formula (new `decimal_spark.rs` + A5 slot), DEC-8 `ExprPlanner`
  compute-with-clamp, DEC-6 exec overflow raise (ANSI knob), TY-3 still DECLARED
  (UNION `forType(INT)` is a wider `TypeCoercion` seam). Does **not** edit the
  registry (R-7 deferred; §6 paste-true).
- [s5-v-landing-ledger.md](s5-v-landing-ledger.md) — **S-5 V-wave §6 landing
  increment (2026-08-14):** verify-before-paste classification of the merged V-wave
  handoffs (G3-E8 IN+NOT IN+`[NOT] EXISTS` footnote, dbt gate MET, family not
  fixed; DEC-3 FIXED via U4a, `/` EXCEPTED as U4b; DEC-5 width FIXED via U3;
  registry DEC-8 still BACKLOG; TY-3 still DECLARED with UNION `forType(INT)`;
  B-TZ-4 FIXED; TZ-8 one row two citations, not fixed; F-V4-1/2 DECLARED
  fork-wave-routed; TZ-6/TZ-7 not duplicated). Completeness proof for the increment
  PR. Live-mirror both-halves: none.
- [s2-g8-ledger.md](s2-g8-ledger.md) — **S-2 / H-2 G8 (2026-08-14):** the 7-ID
  `SEMANTICS_*` value-semantics family (50-ID vocabulary) × 2 doors = 14 matrix rows
  (each a live `cargo test -- --list` name or an honest `DeliberatelyAbsent`) plus
  the test-name liveness gate (`make check-matrix-test-liveness`, dual-wired into
  `make ci` / `make preflight` and the ci.yml rust-test job). No engine edits; no
  new engine tests; registry is S-5. Completeness table for the 14 rows.
- [s1-ansi-knob-u5-ledger.md](s1-ansi-knob-u5-ledger.md) — **S-1 / U5 (2026-08-14):**
  `spark.sql.ansi.enabled` default TRUE on the Spark door + DEC-7 `/0` and `% 0`
  via shared `guard_zero_divisor`. DEC-6 overflow raise DECLARED (38-nines wrap).
  DEC-9 named residue. Does **not** edit the registry (S-5 owns it; §6 paste-true).
- [s3-rehome-ledger.md](s3-rehome-ledger.md) — **S-3 / Q1 `repark.sql` re-home (2026-08-14):**
  facade at `repark.spark`, alias at `repark.spark.sql`, `repark.sql()` ANSI callable,
  `import repark.sql` fails. Node-id map: [s3-rehome-node-id-map.txt](s3-rehome-node-id-map.txt).
  S-1 three files not rewritten (SQM union).
- [v5-w-landing-ledger.md](v5-w-landing-ledger.md) — **V-5 W-wave §6 landing
  increment (2026-08-13):** verify-before-paste classification of the merged W-wave
  handoffs (TZ-4 PR-2 progress not retired; TZ-6/TZ-7 FIXED notes already in-file
  from #85, not duplicated; DEC-1 FIXED; TY-3 still DECLARED; G3-E8 IN+NOT IN
  footnote, family not fixed, dbt gate not met; G5b-R1/R5 FIXED; R4 still OPEN;
  Q-002 FIXED). Completeness proof for the increment PR. Live-mirror both-halves:
  none.
- [v4-partition-values-ledger.md](v4-partition-values-ledger.md) — **V-4 write-path
  partition-key VALUE audit (2026-08-13):** transform × type matrix as carry-check
  pins (identity / bucket / truncate / Iceberg years-months-days-hours UTC-epoch)
  plus load-bearing SQL `year(ts)` / `date_format` identity under non-UTC sessions
  and TZ-8 CAST/to_date disclose. Read-only against the engine. Does **not** edit
  the registry (V-5 owns it; §6 is paste-true).
- [v3-btz4-ledger.md](v3-btz4-ledger.md) — **V-3 / TZ-4 PR-3 (B-TZ-4):** Spark
  `CAST(TIMESTAMP AS STRING)` session-zone space-separated `Utf8`. Oracle recorded
  2026-08-13 against live PySpark 4.1.2 (fraction trim, LTZ/NTZ, epoch, year shape).
  Does **not** edit the registry (V-5 owns it; §6 paste-true here). TZ-8 date-cast
  is ledger handoff only.
- [v1-g3e8-pr3-ledger.md](v1-g3e8-pr3-ledger.md) — **V-1 / G3-E8 PR-3:** `DELETE … WHERE
  [NOT] EXISTS` ± correlation through the A1-identity path. IN + NOT IN + EXISTS + NOT EXISTS
  execute both doors; dbt-upgrade gate sentence in §6. Residual correlated IN / UPDATE /
  nested / scalar stay refused. Does **not** edit the registry (V-5 owns it).
- [v2-dec-u3u4-ledger.md](v2-dec-u3u4-ledger.md) — **V-2 / DEC U3+U4a (2026-08-13):**
  integer-literal min-precision (`fromLiteral` on `+ − *`) + `SparkDecimalPrecision`
  add/sub/mul 38-clamp (CAST-after). `/` declared U4b. DEC-8 still plan-refuse
  (AnalyzerRule cannot see it). TY-3 still DECLARED `(21,1)` nullable. Does **not**
  edit the registry (V-5 owns it; §6 paste-true).
- [w5-z-landing-ledger.md](w5-z-landing-ledger.md) — **W-5 Z-wave §6 landing
  increment (2026-08-13):** verify-before-paste classification of the merged Z-wave
  handoffs (G3-E8 IN-DELETE footnote, not family-fixed; TZ-4 progress not retired;
  DEC-4 / campaign DEC-5 avg FIXED; DEC-1 still OPEN at W-5 write time — closed by
  #84 / V-5; TY-3 still DECLARED; F.abs D6-adjacent FIXED; R1/R4/R5 still OPEN at
  W-5 write time — R1/R5 closed by #82 / V-5). Completeness proof for the increment
  PR. TZ-6 / TZ-7 sections untouched by W-5 (W-1 / #85 wrote those FIXED notes).
  Live-mirror both-halves: none.
- [w4-z-residuals-ledger.md](w4-z-residuals-ledger.md) — **W-4 / Z-wave residuals
  (2026-08-13):** R1 unquoted `INTERVAL 1 DAY` pre-plan quote; R5 interval-over-int
  numeric-`n` restatement; R4 FOLLOWING-to-FOLLOWING re-verify (still DEFER); window
  µs type pins; A6 Q-002 aggregate origin thread. Does **not** edit the registry.
- [w1-tz4-pr2-ledger.md](w1-tz4-pr2-ledger.md) — **W-1 / TZ-4 PR-2:** zoneless LTZ
  localization + NTZ distinction. Retires registry TZ-6 and TZ-7 (headings only).
  Does **not** edit TZ-4 progress row (W-5). `_live_parity.py` A2 only (Y-4 citation).
- [z5-landing-increment-ledger.md](z5-landing-increment-ledger.md) — **Z-5 Y-wave §6
  landing increment (2026-08-13):** verify-before-paste classification of the merged
  Y-wave handoffs (G5b-R2/R3 FIXED; G15; G11 ruling; G10 SHAPE rows; G4b D6 FIXED;
  FA-2 `getDatabase` note; Y-4 citation refresh). Completeness proof for the increment
  PR. Live-mirror both-halves deferred (none demanded; Z-2 owns `_live_parity.py`).
- [w2-dec-u2-ledger.md](w2-dec-u2-ledger.md) — **W-2 / DEC U2 (#84):** Spark-door
  `parse_float_as_decimal=true` default (DEC-1). Named blast-list flips + wider
  fixture sweep. TY-3 still DECLARED (residual U3). Does **not** edit the registry
  (V-5 lands DEC-1 FIXED text).
- [w3-g3e8-pr2-ledger.md](w3-g3e8-pr2-ledger.md) — **W-3 / G3-E8 PR-2:** uncorrelated
  `DELETE … NOT IN (SELECT …)` + the NULL 3VL trap as one unit. Residual EXISTS / UPDATE /
  nested / scalar stay refused. Does **not** edit the registry (W-5 owns it).
- [z3-dec-u1u2-ledger.md](z3-dec-u1u2-ledger.md) — **Z-3 / DEC U1+U2 (#76):** U1
  un-coerced facade `avg(DECIMAL)` (decimal retract, Spark `(p+4,s+4)`). U2
  (`parse_float_as_decimal=true` Spark-door default) was a **named morning deferral**
  — landed as W-2. Registry §6 landed by W-5.
- [z1-g3e8-pr1-ledger.md](z1-g3e8-pr1-ledger.md) — **Z-1 / G3-E8 PR-1 (#78):**
  A1-identity DELETE path + uncorrelated `DELETE … IN (SELECT …)` product hole. Residual
  spellings stay refused. Registry §6 landed by W-5 (family **not** marked fixed).
- [z2-tz4-pr1-ledger.md](z2-tz4-pr1-ledger.md) — **Z-2 / TZ-4 PR-1 (#79):** µs+UTC instant
  producers + Iceberg `timestamptz` for default SQL `TIMESTAMP`. A7 CREATE probe (Spark
  Iceberg type `timestamptz`). Registry TZ-4 PR-1 progress landed by W-5; TZ-6/TZ-7
  FIXED notes landed by #85; TZ-4 PR-2 progress landed by V-5.
- [y7-collation-refuse-ledger.md](y7-collation-refuse-ledger.md) — **Y-7 / G15 in flight:**
  collation refuses loudly at parse altitude (both SQL doors + facade evaluation). §6
  paste-true registry disclosure (silently-wrong-count history + ruling provenance). Does
  **not** edit `docs/spark-sql-iceberg-parity.md`.
- [l1-landing-truth-ledger.md](l1-landing-truth-ledger.md) — **L-1 landing-truth (2026-08-12):**
  docs of record catch up with merged `main` (`baf6617`). §A classification table of every
  `task/*-ledger.md` §6 handoff; live-tier both-halves; STATUS + registry + G14 + G5 slate
  amendment. Completeness proof for the landing-truth PR.
- [g7b-decimal-rust-ledger.md](g7b-decimal-rust-ledger.md) — **G-7b / W-1 in flight:** 10
  bit-exact `Decimal128` i128 pins on the Spark door + 2 cross-door ANSI/Spark rows (Python
  corpus cited, not edited). Continues archived
  [../docs/history/hardening-h1/g7-decimal-ledger.md](../docs/history/hardening-h1/g7-decimal-ledger.md)
  §9.
- [todo.md](todo.md) — a **pointer only**: the live backlog is [../STATUS.md](../STATUS.md), and a
  unit's working plan is its own ledger. The file keeps its name because live code, docs and one
  runtime error message cite this path.
- [lessons.md](lessons.md) — DO / DO-NOT rules in force (append date-stamped; supersede, don't
  delete). Seeded 2026-08-06 from the private v1 repository.
- [g3e8-guard-ledger.md](g3e8-guard-ledger.md) — unit ledger for **G3-E8 (guard-first half)**:
  the DELETE/UPDATE **subquery-predicate valve**. Localizes a silent-data-loss defect (a subquery
  in a DML `WHERE` was lost at DataFusion's DML planning boundary and matched EVERY row), closes
  the window with a refuse-loud valve in BOTH SQL doors, and records the live-Spark oracle the
  future fix will need (`python/repark/tests/test_dml_subquery_parity.py`, 10 rows). Carries the
  statement-form matrix, the guard decisions (incl. the deliberate over-refusal of uncorrelated
  scalar subqueries), provocation transcripts, and §6 registry rows READY TO PASTE but not landed.
  Recon lives planning-side (it names fork + DataFusion internals).
- [metrics.md](metrics.md) — the **process metrics ledger**: one section per retrospective, the
  eight-metric set the SEPMO retrospective contract fixes (findings per cycle, cycles to
  convergence, noise ratio, coverage misses, escaped defects by origin, LIGHT-path escapes, flags
  shipped, environment drift events). Append a section per campaign; never rewrite an earlier one.
  Created 2026-08-10 with the Front-Door campaign's numbers.
- [w3-joins-ledger.md](w3-joins-ledger.md) — **in flight (W-3 / H-2 gap G4):** joins differential
  corpus vs live Spark (value AND Arrow type AND nullability); record driver; §6 paste-true
  registry rows. Does **not** edit `docs/spark-sql-iceberg-parity.md`.
- [w4-windows-ledger.md](w4-windows-ledger.md) — **live** W-4 window-function corpus (gap G5) unit ledger
- [x5-nested-comparator-ledger.md](x5-nested-comparator-ledger.md) — **in flight (X-5 / H-2 gap G18):**
  nested order-insensitive comparator + 6 nested-container differential rows vs live Spark;
  record driver; §6 paste-true registry rows. Does **not** edit
  `docs/spark-sql-iceberg-parity.md`.
- [y6-boundary-shapes-ledger.md](y6-boundary-shapes-ledger.md) — **in flight (Y-6 / H-2 gap G10):**
  facade-boundary container-shape corpus (8–10 pins) vs live Spark 4.1.2; record driver;
  §6 paste-true registry rows. Census cohorts / `_live_parity.py` / registry file **not**
  edited (A11). Sibling of `test_interchange_parity.py`; does not duplicate X-5 VALUES
  families.
- [x1-cast-failure-ledger.md](x1-cast-failure-ledger.md) — **in flight (X-1 / H-2 gap G6):**
  cast-failure semantics differential corpus vs live Spark 4.1.2 ANSI ON; record driver; §6
  paste-true registry + `Disclosure(...)` handoff (does **not** edit the registry or
  `_live_parity.py` — A3).
- [x2-tvl-ledger.md](x2-tvl-ledger.md) — **in flight (X-2 / H-2 gap G12):** three-valued logic
  differential corpus vs live Spark (value AND Arrow type AND nullability) + 2 cross-door 3VL
  rows; record driver; §6 paste-true registry rows. Does **not** edit
  `docs/spark-sql-iceberg-parity.md`. DML NOT-IN twin cites **PR #54 in flight**.
- [g5b-temporal-range-ledger.md](g5b-temporal-range-ledger.md) — **live (G5b / H-2 gap G5, second
  unit):** temporal `RANGE` window frames. Its section 0 recon **falsified the charter premise** —
  interval-bounded temporal `RANGE` already matched Spark 4.1.2 at the frozen base — and found the
  real defect one level down: a **unit-less** `RANGE` offset over a datetime order key was silently
  read as MONTHS (Spark refuses on `TIMESTAMP`, means days on `DATE`). Ships that fix
  (`crates/repark-spark/src/window_range.rs`) with 5 Spark-door pins, 15 appended differential rows
  and 5 recorded residual divergences handed to the unit queue. Does **not** edit
  `docs/spark-sql-iceberg-parity.md` (section 6 is the paste-true handoff).
- [g5br-range-residuals-ledger.md](g5br-range-residuals-ledger.md) — **live (Y-1 / G5b-R):**
  five window-RANGE residual classes on the `window_range.rs` seam. R3 HIGH closed as a
  Spark-empty-frame fix; Half-B closed the kind-only invert hole (`-2 PRECEDING AND -1
  PRECEDING` no longer wraps) and dropped the `10000 YEAR` pair. R2 closed as a
  `DAY TO SECOND` restatement; R1 / R4 / R5 declared deferred. ANSI-door wrapping is a
  named residual. Does **not** edit the registry / `_live_parity.py` / live pins
  (section 6 is the paste-true handoff).
- [g4b-join-widening-ledger.md](g4b-join-widening-ledger.md) — **in flight (O-1 / unit-queue G4b):**
  the FIX behind W-3's two DataFrame `leftsemi`/`leftanti` refuse splits — engine `how`-token
  widening + facade alias map, the splits flipped to content equalities, 3 Rust binding pins.
  §6 states REG-G4-1/2 are now **FIXED** (land them as fixed entries, never live divergences) and
  queues one new disclosure (conditionless semi/anti refuses) plus a declared-rename follow-up.
  Does **not** edit `docs/spark-sql-iceberg-parity.md`.
- [y5-origin-map-ledger.md](y5-origin-map-ledger.md) — **Y-5 / G4b-R2:** semi/anti origin-map
  join-type awareness. After a leftsemi/leftanti join, right-parent Columns raise Spark 4.1.2
  `MISSING_ATTRIBUTES` on select/filter/withColumn instead of silently binding the left
  column; `drop` of that Column is the probed Spark no-op. Does **not** edit
  `docs/spark-sql-iceberg-parity.md`.
- [z4-residuals-ledger.md](z4-residuals-ledger.md) — **Z-4 / Y-wave residuals (#77):** R1/R4/R5
  re-verified and deferred at Z-4 write time (W-4 / #82 later closed R1/R5); `F.abs`
  post-semi origin-thread FIX (Y-5 SAF-001). G13 handoff text only. Registry §6 landed
  by W-5.
- [port/](port/map.md) — **live acceptance inputs**: the deferred-test manifest and its
  reconciliation rule ([port/deferred-tests.md](port/deferred-tests.md)), the machine-readable
  deferral allowlist ([port/deferred-python-tests.txt](port/deferred-python-tests.txt)) and its
  mirror additions ledger ([port/added-python-tests.txt](port/added-python-tests.txt)). The census
  comparator still subtracts these, so they are not history.
- [census/](census/map.md) — **evidence**: the recorded census runs, `baseline-fc3f48102/` (the port
  pin) and `v2-a5be8a7/` (the acceptance run). Never hand-edited; a re-run replaces a whole
  directory in one commit.

- [wc-check-lib-rs-stale-ledger.md](wc-check-lib-rs-stale-ledger.md) — WC: `check_lib_rs` stale-EXCEPTIONS crate-key fail-closed (G-8 mold backport).
- [xc-product-statements-ledger.md](xc-product-statements-ledger.md) — **XC (docs):** G3-E3/E4/E7
  product statements → [`docs/design/product-contract.md`](../docs/design/product-contract.md)
  + design/docs map lockstep; cite inventory; B6 proposals only.
- [x4-catalog-forwards-ledger.md](x4-catalog-forwards-ledger.md) — **X-4 / G17 in flight:**
  `NamespaceScopedCatalog` explicit forwards or stated omissions for every defaulted
  `Catalog` method (16 re-verified at pin `b009ac1`); HIGH `publish_replace_table` +
  4 wrapper pins.
- [x3-float-agg-ledger.md](x3-float-agg-ledger.md) — **X-3 / H-2 gap G7 in flight:** float
  aggregation determinism — 6 `f64::to_bits` Rust pins (sum/avg × target_partitions 1/2/8) over a
  catastrophic-cancellation fixture + run-to-run stability + cross-count spread disclosure; 2
  differential Python rows (both disclosures: Spark 2.25/0.28125 vs repark 3.75/0.46875). Record
  driver. Does **not** edit `_live_parity.py` or the registry (A4/B6).

- [tz5-cast-seconds-ledger.md](tz5-cast-seconds-ledger.md) — **TZ-5:** `CAST(TIMESTAMP AS
  <numeric>)` returns epoch SECONDS (was nanoseconds — a 10⁹ factor). Live-Spark-4.1.2 probe
  transcripts including the **floor-vs-truncate** verdict (Spark floors: `-0.5 s → -1`), the
  engine fix (`repark_functions::timestamp_cast` + the analyzer's `Expr::Cast` arm), the
  divergence-class flip and its per-entry-point corpus, six declared residuals, and the **§6
  paste-true** registry text. Does **not** edit `docs/spark-sql-iceberg-parity.md`.
- [y4-rename-ledger.md](y4-rename-ledger.md) — **Y-4 / G4b-R1 (2026-08-12):** declared-rename
  unit (ships alone). `df_left_semi_unsupported` → `df_left_semi_on_name`,
  `df_left_anti_unsupported` → `df_left_anti_on_name`,
  `timestamp_to_int_spark_seconds_repark_raises` → `timestamp_to_int_nullability`. Identity
  only. Does **not** edit the registry or `_live_parity.py` (§6 paste-true citations).

- [y10-ansi-door-ledger.md](y10-ansi-door-ledger.md) — **Y-10 / H-2 gap G11 in flight:** ANSI
  door correctness-not-parity (Spark is not the ANSI oracle). §0 two-door inventory, 6
  INTENDED `cross_door.rs` rows, 6 `ansi_door_values.rs` standard-SQL pins, FINDING F-Y10-1
  (integer arithmetic overflow wraps). Does **not** edit the registry or the Y-8 ANSI-door
  timezone/cast files.
- [y3-getdatabase-ledger.md](y3-getdatabase-ledger.md) — **Y-3:** `spark.catalog.getDatabase`
  facade + G-6 location-guard live-leg activation (memory catalog; FA-2 untouched).

## Where the closed campaigns' ledgers went

The seventeen `p1*` / `p2*` / `p3*` unit ledgers, the four phase briefs and the port's `todo.md`
execution log moved to [../docs/history/port-v2/](../docs/history/port-v2/map.md) on **2026-08-09**
(Front-Door FD-4), keeping their basenames. A citation of `task/p3e-facade-ledger.md` — a few
survive in Rust doc comments — means
[../docs/history/port-v2/p3e-facade-ledger.md](../docs/history/port-v2/p3e-facade-ledger.md), and so
on. Nothing was lost in the move; the audit is
[../docs/history/port-v2/promotion-ledger.md](../docs/history/port-v2/promotion-ledger.md).

`fd3-ledger.md` left the same way on **2026-08-10**, at the Front-Door campaign's close-out: it is
[../docs/history/frontdoor/fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md), alongside that
campaign's design, slate and retrospective. Its audit is the retrospective's "Promotion check"
section, and the one rule it stranded — set a repo-local git identity before the first commit — was
promoted into [lessons.md](lessons.md) (2026-08-10) **before** the move.

**H-1 phase ledgers** (and the parallel G/N corpus units delivered through the H-1 close gate,
repark #35–#46) moved on **2026-08-11** by **G-9** — a **mid-campaign** phase promotion, not a
campaign close-out. Basenames kept under
[../docs/history/hardening-h1/](../docs/history/hardening-h1/map.md):

| Former `task/` path | Now |
|---|---|
| `task/h1d-ledger.md` | [../docs/history/hardening-h1/h1d-ledger.md](../docs/history/hardening-h1/h1d-ledger.md) |
| `task/h1a-ledger.md` | [../docs/history/hardening-h1/h1a-ledger.md](../docs/history/hardening-h1/h1a-ledger.md) |
| `task/h1c-ledger.md` | [../docs/history/hardening-h1/h1c-ledger.md](../docs/history/hardening-h1/h1c-ledger.md) |
| `task/h1b-ledger.md` | [../docs/history/hardening-h1/h1b-ledger.md](../docs/history/hardening-h1/h1b-ledger.md) |
| `task/g4-tests-split-ledger.md` | [../docs/history/hardening-h1/g4-tests-split-ledger.md](../docs/history/hardening-h1/g4-tests-split-ledger.md) |
| `task/g4-artifacts/` | [../docs/history/hardening-h1/g4-artifacts/](../docs/history/hardening-h1/g4-artifacts/map.md) |
| `task/g5-sweep-ledger.md` | [../docs/history/hardening-h1/g5-sweep-ledger.md](../docs/history/hardening-h1/g5-sweep-ledger.md) |
| `task/g6-chores-ledger.md` | [../docs/history/hardening-h1/g6-chores-ledger.md](../docs/history/hardening-h1/g6-chores-ledger.md) |
| `task/g7-decimal-ledger.md` | [../docs/history/hardening-h1/g7-decimal-ledger.md](../docs/history/hardening-h1/g7-decimal-ledger.md) |
| `task/n2-merge-ledger.md` | [../docs/history/hardening-h1/n2-merge-ledger.md](../docs/history/hardening-h1/n2-merge-ledger.md) |
| `task/g8-file-size-ledger.md` | [../docs/history/hardening-h1/g8-file-size-ledger.md](../docs/history/hardening-h1/g8-file-size-ledger.md) |

A citation of `task/h1d-ledger.md` (or any row above) means the matching file under
`docs/history/hardening-h1/`. The audit is
[../docs/history/hardening-h1/promotion-ledger.md](../docs/history/hardening-h1/promotion-ledger.md).

## Live unit ledgers

| Ledger | Unit |
|---|---|
| [df1-rust-flatten-ledger.md](df1-rust-flatten-ledger.md) | **DF1** native `dynamic_flatten` plan rewrite + thin facade |
| [rsix-rsi-sma-iter-ledger.md](rsix-rsi-sma-iter-ledger.md) | **T5** conductor-15 iterator-form `rsi`/`sma` (bit-exact) |
| [s1-spill-truth-ledger.md](s1-spill-truth-ledger.md) | **S-1** spill truth and reach (FairSpillPool SET / temp_directory / RAM default) |
| [c19-al1a-mimalloc-ledger.md](c19-al1a-mimalloc-ledger.md) | **AL-1a / conductor-19** feature-gated mimalloc (default off) |
| [c19-bh1-default-conf-ledger.md](c19-bh1-default-conf-ledger.md) | **BH-1 / conductor-19** TA bench default-conf primary (measure-only) |
| [p1-ta-kernel-benches-ledger.md](p1-ta-kernel-benches-ledger.md) | **P-1** criterion TA kernel baseline (measure-only) |
| [m14-abort-cleanup-ledger.md](m14-abort-cleanup-ledger.md) | **M14** rejected MERGE commit abort-deletes written files |
| [bl4-update-store-assign-ledger.md](bl4-update-store-assign-ledger.md) | **BL-4** UPDATE SET ANSI store-assignment gate (shared INSERT matrix) |
| [wi1-insert-store-gate-ledger.md](wi1-insert-store-gate-ledger.md) | **WI-1** ANSI store-assignment gate on the non-MERGE write paths (matrix hoisted to `write/store_assign.rs`; plain `INSERT INTO` named unclosed) |
| [wi2-g6-cast-integrity-ledger.md](wi2-g6-cast-integrity-ledger.md) | **G6-3 / G6-5 + WI-2** — Spark-parity `CAST`/`TRY_CAST` DATE↔INT refusals in `analyzer/cast_legality.rs`, and the plain-INSERT store gate as an `AnalyzerRule` over `Dml(Insert)` (`write/insert_gate.rs`). Names the `VALUES`-literal residual |
| [m16-posdelete-specid-ledger.md](m16-posdelete-specid-ledger.md) | **M16** evolved unpartitioned position-delete `spec_id` |
| [fn-d-datetime-ledger.md](fn-d-datetime-ledger.md) | **FN-D** datetime aliases/shims — 11 shipped, rest honest-cut |
| [fn-e-collections-ledger.md](fn-e-collections-ledger.md) | **FN-E** collections / higher-order alias batch |
| [ta3-volume-goldens-ledger.md](ta3-volume-goldens-ledger.md) | **TA-3** volume-family goldens (`ad`/`adosc`/`obv`/`mfi`) — recorder + C recon, no kernels |
| [fn-f-try-bitwise-ledger.md](fn-f-try-bitwise-ledger.md) | **FN-F** try / session / bitwise — 10 shipped, rest deferred |
| [mg1-scanprune-hardening-ledger.md](mg1-scanprune-hardening-ledger.md) | **MG-1** scan-prune hardening — M1/M5/M6/M7 |
| [r3-g8-absences-ledger.md](r3-g8-absences-ledger.md) | **R-3 / G8** four pin-absences → Tested (JOIN both doors, ANSI WINDOW + FLOAT) |
| [r4-tz8-ledger.md](r4-tz8-ledger.md) | **R-4 / TZ-8** CAST(ts AS DATE) / to_date session-zone dates; datediff residual |
| [r2-dec-close-ledger.md](r2-dec-close-ledger.md) | **R-2** DEC close — U4b `/` + DEC-8 `ExprPlanner` + DEC-6 exec-raise; TY-3 DECLARED |
| [s5-v-landing-ledger.md](s5-v-landing-ledger.md) | **S-5** V-wave §6 landing increment — registry + one STATUS dated note |
| [s2-g8-ledger.md](s2-g8-ledger.md) | **S-2 / G8** capability value-semantics matrix + test-name liveness gate |
| [s1-ansi-knob-u5-ledger.md](s1-ansi-knob-u5-ledger.md) | **S-1 / U5** ANSI knob default TRUE + DEC-7 `/0`/`% 0`; DEC-6 DECLARE; DEC-9 residue |
| [v5-w-landing-ledger.md](v5-w-landing-ledger.md) | **V-5** W-wave §6 landing increment — registry + one STATUS dated note |
| [v4-partition-values-ledger.md](v4-partition-values-ledger.md) | **V-4** write-path partition-key VALUE audit — carry-check + load-bearing + TZ-8 |
| [v2-dec-u3u4-ledger.md](v2-dec-u3u4-ledger.md) | **V-2** DEC U3+U4a — integer-literal min-precision + add/sub/mul 38-clamp |
| [w5-z-landing-ledger.md](w5-z-landing-ledger.md) | **W-5** Z-wave §6 landing increment — registry + one STATUS dated note |
| [z5-landing-increment-ledger.md](z5-landing-increment-ledger.md) | **Z-5** Y-wave §6 landing increment — registry + one STATUS dated note |
| [l1-landing-truth-ledger.md](l1-landing-truth-ledger.md) | **L-1** landing-truth — STATUS + registry + live-mirror both-halves + G14 |
| [y4-rename-ledger.md](y4-rename-ledger.md) | **Y-4 / G4b-R1** declared rename (G4b flipped rows + TZ-5 nullability row) |
| [n2b-merge-followup-ledger.md](n2b-merge-followup-ledger.md) | **N-2b / W-2** MERGE follow-up — items 1+4 in PR #50; items 2+3 (lifecycle live + 13 tz live scenarios) in the second PR. Full N-2b closed only when **both** PRs land. |
| [x5-nested-comparator-ledger.md](x5-nested-comparator-ledger.md) | **X-5 / G18** nested comparator + nested-container corpus (Part 1+2) |
| [x4-catalog-forwards-ledger.md](x4-catalog-forwards-ledger.md) | **X-4 / G17** catalog wrapper explicit forwards (HIGH `publish_replace_table`) |
| [y10-ansi-door-ledger.md](y10-ansi-door-ledger.md) | **Y-10 / G11** ANSI door — correctness, not parity |
| [y3-getdatabase-ledger.md](y3-getdatabase-ledger.md) | **Y-3** `getDatabase` + G-6 live-leg |
| [y5-origin-map-ledger.md](y5-origin-map-ledger.md) | **Y-5 / G4b-R2** semi/anti origin-map join-type awareness |
| [g5br-range-residuals-ledger.md](g5br-range-residuals-ledger.md) | **Y-1 / G5b-R** window-RANGE residuals — R3 empty-frame fix (Half-B: kind-or-magnitude invert, no YEAR pair), R2 DAY TO SECOND, R1/R4/R5 deferred; ANSI wrap residual |

## I want to...

| ...do this | go to |
|---|---|
| See the live backlog / what happens next | [../STATUS.md](../STATUS.md) |
| Read the DF1 native `dynamic_flatten` port | [df1-rust-flatten-ledger.md](df1-rust-flatten-ledger.md) |
| Read the U-DF-1 explode mixed-case bind | [c17-explode-case-ledger.md](c17-explode-case-ledger.md) |
| Read the T5 rsi/sma iterator-form rewrite | [rsix-rsi-sma-iter-ledger.md](rsix-rsi-sma-iter-ledger.md) |
| Read the AL-1a feature-gated mimalloc spike | [c19-al1a-mimalloc-ledger.md](c19-al1a-mimalloc-ledger.md) |
| Read the BH-1 default-conf bench-harness fix | [c19-bh1-default-conf-ledger.md](c19-bh1-default-conf-ledger.md) |
| Read the P-1 criterion TA kernel baseline | [p1-ta-kernel-benches-ledger.md](p1-ta-kernel-benches-ledger.md) |
| Read the M14 rejected-commit abort cleanup | [m14-abort-cleanup-ledger.md](m14-abort-cleanup-ledger.md) |
| Read the BL-4 UPDATE SET store-assignment gate | [bl4-update-store-assign-ledger.md](bl4-update-store-assign-ledger.md) |
| Read the M16 evolved-spec position-delete stamp | [m16-posdelete-specid-ledger.md](m16-posdelete-specid-ledger.md) |
| Read the G8 value-semantics matrix + liveness gate | [s2-g8-ledger.md](s2-g8-ledger.md) |
| Read the R-3 flip of the four G8 pin-absences | [r3-g8-absences-ledger.md](r3-g8-absences-ledger.md) |
| Read the FN-D datetime function batch | [fn-d-datetime-ledger.md](fn-d-datetime-ledger.md) |
| Check a rule before acting | [lessons.md](lessons.md) |
| See how a data-loss defect is localized, valved and oracled before it is fixed | [g3e8-guard-ledger.md](g3e8-guard-ledger.md) |
| Find out why a `DELETE`/`UPDATE` with a subquery `WHERE` is refused | [g3e8-guard-ledger.md](g3e8-guard-ledger.md) §2 (the matrix) + §3 (D-3, the deliberate over-refusal) |
| See which G3-E8 spelling now executes (IN-DELETE) | [z1-g3e8-pr1-ledger.md](z1-g3e8-pr1-ledger.md) |
| See which G3-E8 spelling now executes (NOT IN + NULL trap) | [w3-g3e8-pr2-ledger.md](w3-g3e8-pr2-ledger.md) |
| Start a new unit's ledger | copy the shape of the archived [h1d-ledger.md](../docs/history/hardening-h1/h1d-ledger.md) (or [fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md)); link it from this map in the same commit |
| See how a §6 handoff is classified and landed (or superseded) | [l1-landing-truth-ledger.md](l1-landing-truth-ledger.md) (W/X wave) · [z5-landing-increment-ledger.md](z5-landing-increment-ledger.md) (Y wave) · [w5-z-landing-ledger.md](w5-z-landing-ledger.md) (Z wave) · [v5-w-landing-ledger.md](v5-w-landing-ledger.md) (W wave) · [s5-v-landing-ledger.md](s5-v-landing-ledger.md) (V wave) |
| See how a divergence gets declared, pinned and mirrored | [../docs/history/hardening-h1/h1d-ledger.md](../docs/history/hardening-h1/h1d-ledger.md), then [../docs/spark-sql-iceberg-parity.md](../docs/spark-sql-iceberg-parity.md) §6 |
| Read why the session timezone is a build-time knob with one spelling | [../docs/history/hardening-h1/h1a-ledger.md](../docs/history/hardening-h1/h1a-ledger.md) |
| Read how timestamp extraction came to honor it, and what the fix deliberately did NOT close | [../docs/history/hardening-h1/h1a-ledger.md](../docs/history/hardening-h1/h1a-ledger.md) "§ Split B" |
| See how an open question gets FIXED instead of declared (and why a fixed defect gets no row) | [../docs/history/hardening-h1/h1c-ledger.md](../docs/history/hardening-h1/h1c-ledger.md) + [../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md) |
| Find out why a `__repark_tt_*` name is on a session, and which of its three producers put it there | [../docs/history/hardening-h1/h1b-ledger.md](../docs/history/hardening-h1/h1b-ledger.md), then [../crates/repark-spark/src/map.md](../crates/repark-spark/src/map.md) `## Debug` |
| See what a two-mutation acceptance looks like (and why the second mutation is the one that matters) | [../docs/history/hardening-h1/h1b-ledger.md](../docs/history/hardening-h1/h1b-ledger.md) §7c/§7d |
| Read the MERGE INTO differential corpus (gap G3) ledger + registry paste rows | [../docs/history/hardening-h1/n2-merge-ledger.md](../docs/history/hardening-h1/n2-merge-ledger.md) |
| Read the decimal128 differential corpus ledger (Python half) | [../docs/history/hardening-h1/g7-decimal-ledger.md](../docs/history/hardening-h1/g7-decimal-ledger.md) |
| Read the decimal128 Rust half (G-7b pins + cross-door) | [g7b-decimal-rust-ledger.md](g7b-decimal-rust-ledger.md) |
| Read the window-function differential corpus (gap G5) ledger | [w4-windows-ledger.md](w4-windows-ledger.md) |
| Read the G5b-R window-RANGE residual dispositions (Y-1) | [g5br-range-residuals-ledger.md](g5br-range-residuals-ledger.md) |
| Read the V-4 write-path partition-value audit | [v4-partition-values-ledger.md](v4-partition-values-ledger.md) |
| Read the R-4 TZ-8 CAST/to_date session-zone fix | [r4-tz8-ledger.md](r4-tz8-ledger.md) |
| Read the W-4 Z-wave residual close (R1/R5/Q-002) | [w4-z-residuals-ledger.md](w4-z-residuals-ledger.md) |
| Read the nested comparator + nested-container corpus (gap G18) ledger | [x5-nested-comparator-ledger.md](x5-nested-comparator-ledger.md) |
| Read the G17 catalog-wrapper forwards ledger | [x4-catalog-forwards-ledger.md](x4-catalog-forwards-ledger.md) |
| Read the G11 ANSI-door correctness-not-parity ledger | [y10-ansi-door-ledger.md](y10-ansi-door-ledger.md) |
| Read the getDatabase / G-6 live-leg ledger | [y3-getdatabase-ledger.md](y3-getdatabase-ledger.md) |
| Read the three-valued-logic differential corpus (gap G12) ledger | [x2-tvl-ledger.md](x2-tvl-ledger.md) |
| See how a corpus refuse-split gets FIXED and flipped (and why the row keeps its name) | [g4b-join-widening-ledger.md](g4b-join-widening-ledger.md) §2 D2 / §4 |
| See the declared-rename map that retired those kept names (and the TZ-5 flip-row name) | [y4-rename-ledger.md](y4-rename-ledger.md) |
| See why `select(right["k"])` after a semi join must raise `MISSING_ATTRIBUTES` | [y5-origin-map-ledger.md](y5-origin-map-ledger.md) |
| See why a dependency edge or a manifest field is gated, and the proofs it fires | [../docs/history/frontdoor/fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md) |
| File a retrospective's metrics | [metrics.md](metrics.md) — append a section, never rewrite one |
| See which v1 tests are deferred, and why | [port/deferred-tests.md](port/deferred-tests.md) |
| Feed the census comparator its allowlists | [port/map.md](port/map.md) |
| Run or compare a census | [../docs/port/census.md](../docs/port/census.md) |
| Read the port's record (briefs, unit ledgers, retrospectives) | [../docs/history/port-v2/README.md](../docs/history/port-v2/README.md) |
| Read the H-1 phase archive (mid-campaign) | [../docs/history/hardening-h1/README.md](../docs/history/hardening-h1/README.md) |
| Read the port plan the phases executed | [../docs/port/PLAN.md](../docs/port/PLAN.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../AGENTS.md](../AGENTS.md) (the durable contract) and [../STATUS.md](../STATUS.md)
  (current state); this directory holds the moving parts of work in flight.
- Unit ledgers: one `<unit>-ledger.md` per delivered unit, with gate evidence and provocation proofs
  per [../docs/testing.md](../docs/testing.md), linked from this map in the same commit. When a
  **phase or campaign** closes (or is deliberately mid-campaign-promoted), its ledgers are archived
  under [../docs/history/](../docs/history/map.md) after a promotion audit — never deleted.

## Debug

- `pg-integration-report.md` may appear here untracked: `python/repark/tests/test_pg_acceptance.py`
  writes it (CWD-relative) on every facade run. It is gitignored on purpose — a run output, not a
  record. Do not `git add` it.
- If work and trackers disagree, the code is truth — update the tracker.
- A link into `task/p*-ledger.md` or `task/fd3-ledger.md` fails: see "Where the closed campaigns'
  ledgers went" above — same basename, under [../docs/history/](../docs/history/map.md).
- A link into `task/h1*-ledger.md`, `task/g4-*-ledger.md`, `task/g5-sweep-ledger.md`,
  `task/g6-chores-ledger.md`, `task/g7-decimal-ledger.md`, `task/n2-merge-ledger.md`,
  `task/g8-file-size-ledger.md`, or `task/g4-artifacts/` fails the same way: those moved to
  [../docs/history/hardening-h1/](../docs/history/hardening-h1/map.md) on **2026-08-11** (G-9).
- **H-1 phase ledgers were promoted mid-campaign** to
  [../docs/history/hardening-h1/](../docs/history/hardening-h1/map.md) (2026-08-11). **H-2+ unit
  ledgers re-accumulate here** until the next promotion. Empty-of-ledgers is again a valid steady
  state between units (the campaign continues; only the closed H-1 phase record left).
- Looking for a backlog item that is not in [../STATUS.md](../STATUS.md)? Check
  [../docs/history/port-v2/promotion-ledger.md](../docs/history/port-v2/promotion-ledger.md) or
  [../docs/history/hardening-h1/promotion-ledger.md](../docs/history/hardening-h1/promotion-ledger.md)
  — if it was live at archival, that table says where it went.
