# map — python/repark-parity/tests

REVIEW-FIX-5 (2026-09-10): CAP-1 mirror tuple ratcheted down with the code — catalog_config.rs
1044→1028 after the seventeen `//` reasons moved to `crates/repark-core/src/map.md` under the
owner's no-code-comments ruling (D-5). The exact-baseline gate row moved with it; no row raised.

CC-4 (2026-08-30): CAP-1 mirror tuples ratchet down only
(pins: cc-3-comment-condensation/C-009). analyzer.rs 1194→1161; datetime.rs 1783→1709;
dynamic_flatten/tests.rs 1469→1443→1442; declared_sorted.rs 1381→1348.

CC-3 (2026-08-30): comments condensed to one line; banners removed. CAP-1 mirror tuples ratcheted with each slice, including the Python binding files. D-001 catalog.rs 1845→1843; TA kernels 2284→2098 / 1676→1578 / 1873→1821. Spark CAP-1 rows ratcheted; session_timezone.rs retired at 891. Rust exception count 39→38. Router comment-pin expected string restored byte-exact (pins: cc-3-comment-condensation/C-003, C-004).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

CC-2 slice complete: comments and docstrings condensed; oracle discriminators, pins, mutation payloads, and safety contracts kept byte-exact; history narration deleted.

## Purpose

Unit tests for the parity comparison core **and the dataset generators** (no Spark, no
JVM, no repark required). See [../map.md](../map.md).


Docstrings here are one line each: `check_docstring_presence` (D101/D102/D103/D105/D107)
requires one, and nothing may say more. Reasons live in this map, not in the source.


**PERF-DYNFLATTEN-1 round 4:** `test_dynflatten_bed.py` pins the isolation shapes
(`*_nonull`, `cartesian_legs_only`, `cartesian_tags_only`) as flagged and separate from the
headline set, and pins the ranking contract: candidates are ranked by isolated cost and queued
only above 3x the measured noise floor, so a wide floor queues nothing.
pins: perf-dynflatten-1-measure/C-001, C-003

## Contents

- [spill/](spill/map.md) — **NEVEROOM-1 steps 1–3 (2026-09-10/11):** the spill-coverage
  matrix harness, the full-tier run, and the CI golden: the subprocess-per-cell runner
  with an address-space cap, the in-engine `range()` generators sized to the limit
  multiple, the `EXPLAIN ANALYZE` spill-bytes probe reused from `bench/spill/`, the
  three-outcome classifier (`spilled` / `completed` / `refused`, with `KILLED`
  failing the matrix), the worker subprocess entry, the nine-operator × three-multiple
  roster (`matrix_cells.py` `FULL_CELLS`), the full-tier driver (`matrix_run.py`,
  resumable JSONL → folded CSV), the CI-tier cells (`sort`, `hash_aggregate`,
  `hash_join` at 2× the 64 MB limit), `ci_golden.csv`, and the committed-CSV pin.
  Needs the native module: run through `make py-test-spill-matrix`. Ledger:
  [../../../task/ledgers/staging/neveroom-1-ledger.md](../../../task/ledgers/completed/neveroom-1-ledger.md).
  pins: neveroom-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- [torture/](torture/map.md) — **TORTURE-1 steps 1–4:** the both-door torture
  suite (all eight families — `nested`, `inference`, `extreme_types`, `smartcsv`,
  `temporal`, `decimal_overflow`, `secrets`, and the Spark-written `v3_dv` DV-table
  family; DataFrame read and `spark.sql` over a temp view; row counts, declared
  schemas, per-column inferred types, byte-identical CLI determinism for the file
  families, the manifest reuse rule, the `flag_secret_columns` option pins, and the
  CI-tier 60-second workload pin). Divergent cells are strict xfails naming their
  registry rows. Needs the native module: run through `make py-test-torture`. Ledger:
  [../../../task/ledgers/staging/torture-1-ledger.md](../../../task/ledgers/completed/torture-1-ledger.md).
  pins: torture-1/C-001, C-002, C-003, C-004, C-006
- `test_sepmo_packet.py` — **SEPMO-E2 (2026-09-06, round 3):** compact worker
  packet pins: schema validity, prefix byte-identity across five briefs,
  constraint preservation (dropped prefix rule, dropped sidecar
  `authority.constraints` rule, forged trailer assembled at runtime,
  JSON/markdown disagreement, prefix-negating dynamic, uncaptured unbackticked
  boundary path through `build`), prose or invalid-shell `commands[]` through
  `build`/`check`, icescan Cargo.lock exception, ex25 `covered`/`stayed`
  hand-back keys, dynamic-only `diff`, no home paths, source-hash refresh,
  baseline sizes versus E-0 cached/uncached ratios, and adoption `--brief` /
  `--followup` names checked against wrapper text when present.
  pins: sepmo-e2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- `test_sepmo_usage.py` — **SEPMO-E0E1 (2026-09-06, round 3):** usage collector pins: Muse /
  Grok / OpenCode / Claude fixture shapes, Muse session-store join (both `msp-view-v1` and
  `.msp-view-v1`), live Grok usage keys, minority truncated JSONL as a degraded record,
  clean-cut tail with `exit` and no terminal, remote URLs with `://` refused before
  resolve, missing-data stays null, index table shape including a degraded row, schema
  field roster plus uncached-token descriptions, inventory adapter+strata claims, and live
  reconciliation / live `index` of Muse run dirs when `/tmp/muse-worker` is present.
  pins: sepmo-e0-e1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [fixtures/](fixtures/map.md) — sanitized collector run dirs (no home paths).
- `test_ex_0_example_coverage.py` — **EX-0 (2026-08-31):** the v0.7 example-drift
  gate: five-family enumerator, uncovered / stale-backlog / covered-in-backlog
  reds, backlog and exceptions baselines, COVERS-must-be-used, seed `COVERS`,
  cloud exceptions, nonzero example exit, Makefile `make ci` + ci.yml dual-wire
  + wheels.yml `python -I … --require-execute`; F.* includes installer
  `__all__` mutations (`try_*`, `zip_with`, xpath); backlog pins are
  campaign-true since 2026-09-01 (baseline is a `<=` direction ratchet in
  lockstep with the file — the ex-2 ledger's blocker section records the
  ruling). pins: ex-0-example-drift-gate/C-001,
  C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
  **EX-1 (2026-08-31)** widens the same file: the ten-family enumerator at 913
  names, the `CLASS_SURFACES` / `MODULE_SURFACES` tables, hard-error shape
  drift, the live-`__all__` door roster, `Window.*` on its class root versus
  `WindowSpec.*` / `Column.*` / `Row.*` on a repark-rooted local, a `types.*`
  cover refusing the `ml` door and the reverse, and every new name in the
  backlog at baseline 892. The no-dynamic-registration assertion reads the
  gate's own `*_SOURCE` constants, so a moved facade file reds the test instead
  of leaving it pointed at a path that no longer exists.
  **EX-IDIOM (2026-09-01)** adds the session-root parity pin (`ex-idiom/C-001`,
  cited in prose because no `ex-idiom` ledger exists and grammar rule B reds an
  unresolvable `pins:` line): `ReparkSession` and `SparkSession` bind the
  session covers identically. The gate's `SESSION_BUILDER_HINTS` has carried
  both spellings since EX-0, and the examples construct sessions as
  `repark = ReparkSession.builder…` (owner ruling, 2026-09-01).
  pins: ex-1-class-surfaces/C-001, C-002, C-003, C-004, C-005, C-006, C-007,
  C-008
  **EX-17 (2026-09-04):** `test_ex_1_every_new_name_is_in_the_backlog` accepts a widened name that an
  example now covers (backlog OR covered); the first `Column.*` batch was the first to cover one.
  **EX-31 (2026-09-12):** the seven measured non-PySpark plumbing names (six `Column.*`
  helpers, `F.PythonUDFColumn`) are pinned out of the example inventory, its checked-in
  snapshot and the backlog by the named `INVENTORY_EXCLUSIONS` list, while the raw
  `enumerate_public_surface` walk still ships them (frozen API, still callable); the
  widened-name pin now reads `example_inventory`, the post-exclusion view.
  pins: ex-31-inventory-plumbing/C-001, C-005
- `test_plan_1_northstar_fnp_sequence.py` — **PLAN-1 (2026-08-28; tree pins):** the guarded
  North Star sequence, F-17's measured shared-Puffin closure request, the live slate, the
  per-unit FNP remaining order (FNP-7a/7b delivered 2026-08-31; remaining FNP-9/10 → FNP-8
  → FNP-11/12 → FNP-Z) and delivery boundary, FNP-Z retirement, fork independence, and map
  lockstep, including the archived V3-3 and F-rp3-c7 record. **V3-8:** STATUS Next is
  row-lineage carry complete on every served DML shape.
  (pins: plan-1-northstar-fnp-sequence/C-001, C-002, C-003, C-004, C-005, C-006;
  v3-3-dml/C-003; v3-4-serve-lineage-columns/C-010; fnp-7-try-inversions/C-016;
  v3-5-dv-compaction/C-006; rp-6-fork-repin/C-006; v3-8-subquery-where-lineage/C-003).
  **DOCS-1 (2026-09-02):** this pin and `test_v3r_1_rulings.py` are the gate the post-merge
  truth-up re-reads — STATUS's `Next:` sentence, the north-star §3 rows and the ledger bins
  they cite all moved in that pass and both stayed green
  (pins: docs-1-truth-up/C-001, C-002, C-003, C-004, C-005, C-006).
  **V3-9 (2026-09-02):** the STATUS `Next:` sentence it reads now names lineage carry **and**
  merge-on-read as complete. pins: v3-9-mor-predicate-dml-dv/C-005
  **API-REVIEW (2026-09-02):** the north-star §3 gate paragraph this pin reads is what calls for
  an API review, and [../../../docs/design/v1-0-api-review-2026-09-02.md](../../../docs/design/v1-0-api-review-2026-09-02.md)
  is that packet: 35 surface rows whose inventory and coverage are `scripts/check_example_coverage.py`'s
  own output (913 names, 67 covered), whose door rows are read from `repark_common::surfaces::ALL`
  and both door matrices, whose residual column maps all 81 open divergence-registry rows, whose
  recommendation follows one stated rule set, and whose §5 quotes what a `yes` would bind — the
  gate paragraph itself is untouched by that unit
  (pins: api-review-packet/C-001, C-002, C-003, C-004, C-005).
  **API-FREEZE (2026-09-02):** that same gate paragraph now carries one appended line — "API
  review answered 2026-09-02 (packet); the freeze lands with the tag" — and this pin stayed green
  across it. pins: api-freeze/C-002
- `test_api_freeze.py` — **API-FREEZE (2026-09-02; release 2026-09-03: the STATUS pointer now names the cut tag, not the waiting gate):** the v1.0 freeze pin. Holds three things
  at once: every packet row's `decision` equals its `recommend` (15 YES / 15 YES-except / 5 NO,
  dated 2026-09-02, the owner's rule sentence byte-equal in packet, inventory and
  `docs/release.md`); the checked-in register
  [../../../docs/design/v1-0-api-freeze.json](../../../docs/design/v1-0-api-freeze.json) equals a
  fresh `scripts/build_api_freeze.py` build of the tree, so an unrecorded surface move is red; and
  the additive rule holds in both directions. Ten mutations over a scratch copy of the enumerated
  sources — **8 red** (a frozen member's `def` renamed private; a frozen callable's required
  parameter renamed; a door surface flipped Tested → `absent`; a frozen conf-key literal, error
  class or packaging literal dropped; a packet decision that stops equalling its recommendation; a
  surface id that no packet row claims) and **2 green by design** (a new public member, a new
  optional parameter). The five `NO` rows carry `frozen: false` and no member, so nothing about
  them is checked. Regenerating the register is the sanctioned move for an intended additive
  change: `python3 scripts/build_api_freeze.py --write`, in the same commit.
  The unit's docs half rides the same file: the release policy section, the discharged facade
  ruling, the north-star line and the STATUS line are each asserted here, so a docs rollback
  is red. (pins: api-freeze/C-001, C-002, C-003, C-004)
- `test_pr_247_owner_ruling.py` — **PR #247 revalidation (2026-08-27):** the owner-ruling blocks
  in `AGENTS.md` and `CLAUDE.md` stay byte-exact, unique, at the document start, and in regular
  files; one-byte drift, malformed or missing files, relocation, duplication, and symlink
  redirection fail closed. The review-held enforcement boundary stays exact, unique, and adjacent
  to the ruling. The attribution-blind density gate stays absent. CAP-1's exact-baseline Rust and
  Python source gates remain wired. No source-comment sweep belongs to this unit.
  `pins: pr-247-revalidation/C-001, C-002, C-003, C-004, C-005, C-006, C-007`
- `test_proc_1_tiered_review.py` — **PR-244 revalidation:** current source and map guards, tiered
  SEPMO `review_profile` and `critic_engine` bindings in `binding-manifest.md`, MW-6 evidence,
  disk guidance, clause pins, and ledger lifecycle. B-MOR-3 (2026-09-03): the handoff F-7
  acceptance pin reads the retired refusal pin's zeros replacement.
  pins: b-mor-3-rewrite-position-deletes-v3/C-004
- `test_pr_245_revalidation_record.py` — PR #245 source-size ratchets, frozen SQP-1 artifacts,
  bounded parser guards, exact literal-helper inventory, and lifecycle-aware navigation.
  H3-SPILL-1 (2026-09-05): the literal-helper inventory gains
  `bench/spill/cell_worker.py` `sql_string_literal` 1 — the spill harness escapes its own
  warehouse path into `CREATE NAMESPACE … LOCATION`, so it uses the helper rather than a
  second escape rule. MAINT-POLICY-1 (2026-09-10): the inventory gains
  `spark/session/session_maintenance.py` `sql_string_literal` 2 — the facade wrapper renders the
  table name and any string override into the `CALL … run_maintenance(…)` text through the helper
  rather than an f-string, so the one place a caller's value reaches SQL stays inside the audited
  set. pins: h3-spill-1/C-001, maint-policy-1/C-030
- `test_cap_1_source_file_line_cap.py` — **FN-FIX-2 (2026-09-04):** `analyzer.rs` 1161→1142. PERF-FACADE-1 (2026-09-05): `core.py` row 6368 → 6303 with the script baseline. CUTOVER-SCHEMA-1 (2026-09-05): `session.rs` 1040 → 1039 and `repark-python/src/dataframe.rs` 1171 → 1127 with the script baselines; the REG-1 DEC-9 pin follows the row's narrowed rationale. PERF-ICE-CATALOG-IO-1 (2026-09-05): `session.rs` 1039 → 1002 in both tables. H3-SPILL-RESIDUE-1 (2026-09-06): `repark-python/src/dataframe.rs` 1127 → 1126 in both tables. The approved Rust exception count is 36 since CSV-INFER-PERF-1 retired `session.rs`.
- `test_cap_1_source_file_line_cap.py` — **FN-FIX-2 (2026-09-04):** `analyzer.rs` 1161→1142. PERF-FACADE-1 (2026-09-05): `core.py` row 6368 → 6303 with the script baseline. CUTOVER-SCHEMA-1 (2026-09-05): `session.rs` 1040 → 1039 and `repark-python/src/dataframe.rs` 1171 → 1127 with the script baselines; the REG-1 DEC-9 pin follows the row's narrowed rationale. PERF-ICE-CATALOG-IO-1 (2026-09-05): `session.rs` 1039 → 1002 in both tables. H3-SPILL-RESIDUE-1 (2026-09-06): `repark-python/src/dataframe.rs` 1127 → 1126 in both tables. WRITE-DISTRIBUTION-2 (2026-09-06): `write/append.rs` 1884 → 1883 in both tables. DFCORE-1 (2026-09-07): `dataframe/core.py` row 6302 → 5954 and `dataframe/joins_columns.py` row 1239 → 1238 with the script baseline. pins: dfcore-1/C-007
- `test_cap_1_source_file_line_cap.py` — DFCORE-2 (2026-09-07): `dataframe/core.py` row 5954 → 5263 with the script baseline; the two new UDF projection modules carry no row. pins: dfcore-2/C-006
- `test_cap_1_source_file_line_cap.py` — DFCORE-3 (2026-09-07): `dataframe/core.py` row 5263 → 5060 and `dataframe/writer_readwriter.py` row 1113 → 1111 with the script baseline; the new statistics module carries no row. pins: dfcore-3/C-006
- `test_cap_1_source_file_line_cap.py` — DFCORE-4a (2026-09-07): `dataframe/core.py` row 5060 → 4819 with the script baseline; the new sampling module carries no row. pins: dfcore-4a/C-005
- `test_cap_1_source_file_line_cap.py` — DFCORE-4b (2026-09-07): `dataframe/core.py` row 4819 → 4539 with the script baseline; the new display module carries no row. pins: dfcore-4b/C-005
- `test_cap_1_source_file_line_cap.py` — DF-EXPLAIN-1 (2026-09-08): `dataframe/core.py` row 4539 → 4536 with the script baseline; the new `explain.py` module carries no row. At DF-EXPLAIN-1 this mirror was not reached by `make preflight`; PREFLIGHT-PARITY-1 (2026-09-09) wires the file in alone as `make py-test-parity-cap`, so a ratchet that updates only `scripts/check_lib_py.py` now reds preflight — update both tables in the same commit. pins: df-explain-1/C-003
- `test_cap_1_source_file_line_cap.py` — CFG-1 step 3 (2026-09-09): `session_core.py` row 2411 → 2306 (SAF-006 resolvers to `session_configuration.py`) and `repark-python/src/session.rs` row 1177 → 1128 (ruled `config_path` plus `config_file_pairs` paid for by the `drain_arrow_c_stream` move to `arrow_export.rs`) with the script baselines. pins: cfg-1/C-026, C-027
- `test_preflight_parity_1_wiring.py` — **PREFLIGHT-PARITY-1 (2026-09-09):** the CAP-1 mirror
  joins `make preflight` as `make py-test-parity-cap` — the mirror file alone, `py-test`'s
  isolated `uv run --no-project` recipe verbatim, seated after `py-test-facade` before `audit`,
  `verify` untouched, measured 1.4 s against the 60 s budget. Red-first: both pins failed on the
  base tree (no target; absent from the `preflight` line). `DEVELOPMENT.md`'s gate roster and the
  root map's `make preflight` enumeration name the new member; `AGENTS.md`'s roster sentence is
  left to the owner via the PR body. **Answered 2026-09-09 by ruling R-12:** `AGENTS.md`'s
  roster sentence stays as it is and `DEVELOPMENT.md` plus the root `map.md` are the homes
  that name new gate members — both already do, so the unit closes with no further edit and
  its ledger moved to `completed/`. pins: preflight-parity-1/C-001, C-002, C-003, C-004, C-005
- `test_profiles_bed.py` — **PROFILES-1 steps 1–2 (2026-09-10/12):** engine-free bed
  pins: three datasets (futures name, TPCH SF10, 200 Iceberg files, the shared dbgen
  symbol), five reads + three writes (append 8 files, merge 10 %), CSV header /
  median / row shape, the JVM guard both ways, smoke scale below full scale. Step 2
  adds the `table_property_alter` pin (write.* knobs land as TBLPROPERTIES, session
  knobs and `@default` do not) and the doc-vs-CSV pins: three repetitions per cell in
  every committed CSV, every doc table row equal to its CSV median with its ratio,
  the argmax row per knob, and the no-effect table recomputed under the
  AFFECTED_CELLS / CONTROL_VALUES rule (a knob is flat only where it can reach;
  same-as-default spellings are controls, not evidence). Step 3 (2026-09-12) adds
  the profile pins: the committed `docs/examples/config/*.toml` parse (tomllib,
  conf tables flattened with dot joins as the CFG-1 loader emits them) and their
  knob sets must equal the D-1 derivation recomputed from the step-2 CSVs — a
  value qualifies only on an affected, non-noise cell ≥ 5 % better than
  `@default` and outside that cell's measured noise floor, and only while no
  sibling cell in its class pays a ≥ 5 % regression outside the floor; the
  futures read cells are the named noise set. The same file pins the guide's
  profile rows recompute from the CSVs, the guide no-effect list equals the
  step-2 table, and the step-3 re-measure CSVs reproduce each profile win.
  pins: profiles-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
- `test_cap_1_source_file_line_cap.py` — DISPLAY-POLARS-1 step 3 (2026-09-09): `dataframe/core.py` row 4536 → 4525 with the script baseline; the two `__repr__` / `_repr_html_` docstrings condensed to one line each, their contracts moved to `python/repark/src/repark/spark/dataframe/map.md`. pins: display-polars-1/C-004
- `test_cap_1_source_file_line_cap.py` — DISPLAY-POLARS-1 step 4 (2026-09-09, follow-up): `dataframe/plan_collapse.py` row 1168 → 1057 and `session/session_core.py` row 2411 → 2410 with the script baseline (ratchet DOWN; the spellers live in the new `dataframe/polars_cells.py`, the key plumbing in `session_configuration.py`). pins: display-polars-1/C-005
- `test_cap_1_source_file_line_cap.py` — DF-EAGER-1 step 2 (2026-09-09): `dataframe/core.py` row 4525 → 4487 with the script baseline; `.eager()`/`.compute()`/`.lazy()` and the cache-guard trio live in the new `dataframe/eager.py`, so the row ratchets DOWN. pins: df-eager-1/C-001
- `test_cap_1_source_file_line_cap.py` — SQL-DESCRIBE-1 (2026-09-09): `repark-python/src/dataframe.rs` row 1126 → 1084 with the script baseline; the DDL element spelling moved to `repark-spark`. pins: sql-describe-1/C-003
- `test_ex_0_example_coverage.py` — **DF-EAGER-1 step 3 (2026-09-09):** the enumerated public surface moves 923 → 926 as `DataFrame.eager`, `DataFrame.compute` and `DataFrame.lazy` join the dataframe family (923 is CFG-1 step 3's count, merged first); `docs/examples/dataframe/lazy_and_eager.py` covers all three, so `check-example-coverage` stays clean. This pin is in the parity suite, which `make preflight` does not run — a PR that adds a public name must run the parity suite before pushing, and two such PRs open at once each carry their own count until one merges. pins: df-eager-1/C-001
- `test_ex_0_example_coverage.py` — **FNP-9/10 (2026-09-06):** the enumerated public surface
  moves 913 → 921 as the eight built `F.*` names join `functions.py`'s `__all__` through
  `functions_json.install_into`. pins: fnp-9-collections-json/C-001
- `test_ex_0_example_coverage.py` — **CFG-1 step 3 (2026-09-09):** the enumerated public surface moves 921 → 923 as `Builder.config_file` and its `configFile` camelCase twin join the session family; both are covered by `docs/examples/session/config_file.py`, so `check-example-coverage` stays clean. This pin lives in the parity suite, which `make preflight` does not run — a PR that adds a public name must run it before pushing. pins: cfg-1/C-026
- `test_ex_0_example_coverage.py` — **CFG-1 step 4 (2026-09-10):** the surface stays 923 — the new `repark.config` mirror is not one of the enumerated doors (functions / class surfaces / module `__all__` / `repark.sql`), so no count moves and no covering example is owed. pins: cfg-1/C-031
- `test_ex_0_example_coverage.py` — **MAINT-POLICY-1 audit fix (2026-09-10):** the enumerated public surface moves 926 → 927 as `SparkSession.run_maintenance` joins the session family declared in the class body; `docs/examples/session/run_maintenance.py` covers it, so `check-example-coverage` stays clean. pins: maint-policy-1/C-030
  **PERF-UNPIVOT-1 (2026-09-12):** 927 → 928 as `F.stack` joins the functions family. pins: perf-unpivot-1/C-004
- `test_cap_1_source_file_line_cap.py` — **MAINT-POLICY-1 audit fix (2026-09-10):** `session/session_core.py` row 2305 → 2304 with the script baseline; `_temp_view_home_ref` moves to `catalog_resolution.py` to pay for the `run_maintenance` class-body declaration. pins: maint-policy-1/C-030
- `test_cap_1_source_file_line_cap.py` — REVIEW-FIX-6 (2026-09-10): `dataframe/core.py` row 4487 → 4486 with the script baseline; the four-line both-set guard is paid for by five moved comment lines, their facts moved to `python/repark/src/repark/spark/dataframe/map.md`. pins: review-fix-6/C-004
- `test_cap_1_source_file_line_cap.py` — DISPLAY-LAZY-1 (2026-09-11, measured on the rebased tree after REVIEW-FIX-6 #487 took the row to 4486 by the same comment-funded method): `dataframe/core.py` row 4486 → 4485 with the script baseline; the checkpoint-arm `_eager_shape` record is paid for by deleting the cache-pinned early-return rationale (fact restated on the dataframe map). pins: display-lazy-1/C-007
- `test_cap_1_source_file_line_cap.py` — **FN-FIX-2 (2026-09-04):** `analyzer.rs` 1161→1142. PERF-FACADE-1 (2026-09-05): `core.py` row 6368 → 6303 with the script baseline. CUTOVER-SCHEMA-1 (2026-09-05): `session.rs` 1040 → 1039 and `repark-python/src/dataframe.rs` 1171 → 1127 with the script baselines; the REG-1 DEC-9 pin follows the row's narrowed rationale. PERF-ICE-CATALOG-IO-1 (2026-09-05): `session.rs` 1039 → 1002 in both tables. FNP-9/10 (2026-09-06): `functions_expr.py` 2259 → 2256 in both tables as `arrays_zip` and `schema_of_json` trade a multi-line refusal for a one-line wrapper.
- `test_cap_1_source_file_line_cap.py` — **FN-FIX-2 (2026-09-04):** `analyzer.rs` 1161→1142. PERF-FACADE-1 (2026-09-05): `core.py` row 6368 → 6303 with the script baseline. CUTOVER-SCHEMA-1 (2026-09-05): `session.rs` 1040 → 1039 and `repark-python/src/dataframe.rs` 1171 → 1127 with the script baselines; the REG-1 DEC-9 pin follows the row's narrowed rationale. PERF-ICE-CATALOG-IO-1 (2026-09-05): `session.rs` 1039 → 1002 in both tables. H3-SPILL-RESIDUE-1 (2026-09-06): `repark-python/src/dataframe.rs` 1127 → 1126 in both tables.
- `test_cap_1_source_file_line_cap.py` — **FN-FIX-2 (2026-09-04):** `analyzer.rs` 1161→1142. PERF-FACADE-1 (2026-09-05): `core.py` row 6368 → 6303 with the script baseline. CUTOVER-SCHEMA-1 (2026-09-05): `session.rs` 1040 → 1039 and `repark-python/src/dataframe.rs` 1171 → 1127 with the script baselines; the REG-1 DEC-9 pin follows the row's narrowed rationale. PERF-ICE-CATALOG-IO-1 (2026-09-05): `session.rs` 1039 → 1002 in both tables. NULLABILITY-2
  (2026-09-05): `core.py` row 6303 → 6302 with the script baseline, then the
  `_live_parity.py` row 1877 → 1778 with the script baseline when the three
  converged nullability disclosures retire.
- `test_cap_1_source_file_line_cap.py` — **FN-FIX-2 (2026-09-04):** `analyzer.rs` 1161→1142. PERF-FACADE-1 (2026-09-05): `core.py` row 6368 → 6303 with the script baseline. CUTOVER-SCHEMA-1 (2026-09-05): `session.rs` 1040 → 1039 and `repark-python/src/dataframe.rs` 1171 → 1127 with the script baselines; the REG-1 DEC-9 pin follows the row's narrowed rationale. PERF-ICE-CATALOG-IO-1 (2026-09-05): `session.rs` 1039 → 1002 in both tables. PERF-APPROXPCT-1 (2026-09-05): `repark-python/src/column/mod.rs` 1053 → 1052 with the script baseline.
  **CSV-INFER-PERF-1 (2026-09-06):** `session.rs` exception retired (1002 → 988, under the default); the `_RUST_BASELINES` row is deleted. Round 2: `reader.py` 1026 → 1022 in both tables.
  pins: csv-infer-perf-1/C-006
  DF-PRINTSCHEMA-1 (2026-09-04): the `dataframe/core.py` row ratchets 6371 → 6368 with the gate table.
  FN-REGEXP-EXTRACT-1 (2026-09-04): the `functions_expr.py` row ratchets 2261 → 2259 with the gate table.
  PERF-APPROXPCT-1 round 2 (2026-09-06): the `functions_expr.py` row ratchets 2259 → 2258 with the gate table.
  TYPES-1 (2026-09-05): `dataframe/core.py` 6303 → 6305 (increase — the two `__repark_rn` BIGINT casts) and `test_window_parity.py` 1481 → 1422 with the gate table. TYPES-1 round 4 (2026-09-05): `core.py` 6305 → 6303 with the gate table (one import joined absorbs the increase); `datetime.rs` 1704 → 1709 with the gate table (increase — the year-sign arm, no compressible lines, owner approval at merge). TYPES-1 round 5 (2026-09-05): `datetime.rs` 1709 → 1700 with the gate table (the year arm moves to `spark_year_pad.rs`).
  pins: fn-fix-2-string-rows/C-002
  **FN-FIX-1 (2026-09-03):** `datetime.rs` 1709→1704, `column/mod.rs` 1105→1102, `functions_expr.py` 2265→2261. pins: fn-fix-1-registry-rows/C-002
  RP-7 (2026-09-02) mirrors the two downward ratchets `write/merge/mod.rs` 1889→1795 and `write/predicate_dml.rs` 1164→1142 (pins: rp-7-f18-repin/C-005). V3-10 ratchets `repark-spark/src/alter.rs` 1831→1830 and
  `repark-iceberg/src/write/alter.rs` 1641→1630
  (pins: v3-10-upgrade-v2-to-v3/C-003). **CAP-1 (2026-08-26):** exact Rust and Python source-size RP-6 ratchets merge/mod.rs 1894→1892 and predicate_dml.rs 1227→1226; V3-8 ratchets predicate_dml.rs 1226→1164 after the lineage helpers move to `predicate_dml/lineage.rs` (pins: v3-8-subquery-where-lineage/C-002).
  (**DML-B 2026-08-30:** `insert_overwrite.rs` tests 1249→1233, `writer_readwriter.py` 1117→1113)
  B-MOR-3 (2026-09-03): ratchets `repark-spark/src/tests/call.rs` 1307→1303 after the
  live-DV refusal and its counter helper are deleted
  (pins: b-mor-3-rewrite-position-deletes-v3/C-002).
  exception sets and baselines mirrored from the live guard tables (DML-A:
  `merge/mod.rs` 2131 → 2086; `call.rs` 1404 → 1111 after
  RP-2's `call_args.rs` split; RP-3 1407 → 1361; **REF 2026-09-01:** the
  `repark-spark/src/ref_ddl.rs` row is retired, 38 → 37 Rust rows, after that file's
  in-module tests moved to a file-backed `ref_ddl/tests.rs`); blank-line boundaries;
  growth, shrink, retirement,
  missing-path, unreadable-path, and empty-scan provocations; fixture exclusions; unchanged
  facade no-stub scope; existing Makefile/CI wiring and contract/navigation carriers. The
  owner correction restores `position_delete.rs` to 1,068 lines with model provenance. The
  production file-size refactor removes `session/_funcs.py` when its exception retires. The
  catalog-registration test split ratchets `session/tests/session.rs` from 1,485 to 1,461 lines.
  DML-C ratchets `session.rs` 1178 → 1177 and `repark-sql/src/tests.rs` 1523 → 1520.
  WRITE-ORDER-DIST-1 (2026-09-06) ratchets `repark-spark/src/alter.rs` 1830 → 1821,
  `repark-iceberg/src/write/append.rs` 1884 → 1883, and `write/merge/mod.rs` 1795 → 1792
  with the gate table (pins: write-order-dist-1/C-012).
  The same unit ratchets `repark-spark/src/tests/alter.rs` 1436 → 1397 — the obsolete
  WRITE-refusal blocks are deleted (pins: write-order-dist-1/C-001).
  NIGHTLY-LIVE-1 (2026-09-11) ratchets `test_ml_boost_oracle.py` 2244 → 2241 with the
  gate table — the PySpark teardown `try/finally` retires; `_live_parity.py` stays at
  its 1778 baseline line-neutral (the `catalog` kwarg pays for itself by compressing the
  docstring, and `spark_session_conf` restores never-set keys via `conf.unset`).
  pins: nightly-live-1/C-003
- `test_live_v3_docs.py` — **LIVE-v3-M (2026-09-02; tree pins):** the live v3 legs are documented
  as **measured green** — registry `S3T-V3-1` is FIXED by measurement and carries run
  33635288918, its link, base `8c4bc55`, the `6 passed in 122.13s` line, the accepted branch and
  `exact_commit_counts=False`; the north-star "Live: Glue + S3 Tables v3 legs" row is ✅, names
  the run, both legs and the registry row, and keeps the MW-10 sentence with its links; no
  pending wording ("unmeasured", "nothing has run against AWS", "the first measurement is
  pending", "not yet run") survives in the registry row, the north-star row or the STATUS clause;
  `docs/tier2-aws.md` §6 lists one row per leg, its two v3 rows state the answer, it says the v3
  legs need no new IAM action or workflow variable, and it carries no run id because measured
  state belongs to STATUS; both legs and the local pin exist as real `def`s; STATUS's v3
  **V3-11 (2026-09-02):** the `V3-ROWID-3` meta-pin flips from BACKLOG to FIXED, and three
  more join it — `F-v3-10-partition-file-order` names fork ask **F-20** as RePark's rule rather
  than Spark's, and **RP-8 (2026-09-03)** flips that meta-pin to the closed reading: the ask
  landed as fork `#261`, the residual is FIXED, and the registry must still say the drain buys
  determinism and one rule, **not** parity; `V3-FILEORDER-1` must carry the decoded
  `JavaHashes$StructLikeHash` order, the collision caveat and every measured arm; and the
  retired `DataSourceV2Relation` maintenance-oracle note must appear ONCE (under MOR-1) with
  five pointers, so no row can quietly regrow its own copy of a claim that was false on all
  six.
  pins: rp-8-repin-f21-f22/C-004
  **RP-8 (2026-09-03):** `test_v1_gate_docs.py`'s fork-side meta-pin reads the consumed pin, so The STATUS stamp it pins moved to 2026-09-06 with the v1.1.0 release PR.
  it moves with the repin — the north star's "Fork side, at the consumed pin" heading and
  `Cargo.toml` both name `c1d6c9de`, and R114's dated cell names F-21 and F-22.
  pins: rp-8-repin-f21-f22/C-006
  **RP-12 (2026-09-05):** the meta-pin reads the audited pin `189a73ed` from `docs/fork-sync.md`'s
  pin history instead of `Cargo.toml`, so the V1-GATE audit stays true across later bumps.
  **RP-9 (2026-09-03):** the same meta-pin moves to `594bdbe5`; R114's dated cell names F-23.
  pins: rp-9-repin-f23/C-004
  **RP-10 (2026-09-04):** the same meta-pin moves to `85a4aaf0`; R114's dated cell names F-25.
  pins: rp-10-repin-f25/C-004
  **RP-11 (2026-09-04):** the same meta-pin moves to `189a73ed`; `B-MOR-3-FLOOR-1` FIXED.
  pins: rp-11-repin-f24/C-001, C-003
  Two neighbouring meta-pins were repointed when V3-11 compacted STATUS:
  `test_plan_1_northstar_fnp_sequence.py` reads the shortened V3-6 sentence, and
  `test_v3r_1_rulings.py` reads `F-rp3-c7 consumed` from the north-star COW row — the
  artefact's own home — instead of from a STATUS restatement that no longer exists.
  `test_cap_1_source_file_line_cap.py` mirrors the `append.rs` ceiling at its ratcheted 1884.
  pins: v3-11-row-id-determinism/C-003, C-005, C-008
  workstream names the run, carries the `V3-ROWID-3` line and stays under its dual-pinned
  25,000-byte ceiling; registry `V3-ROWID-3` still carries both engines' measured answers and
  names follow-up unit V3-11; and `docs/design/format-v3-track.md` §7's two "not measured" claims
  each carry a dated correction. Whitespace-normalized reads, so a re-wrap does not red it.
  pins: live-v3-aws-legs/C-004, C-005; live-v3-first-measurement/C-001, C-002, C-003
  **V1-GATE (2026-09-03):** the `S3T-V3-1` half gains the confirmation run — the row must say
  `confirmed live 2026-09-03` and name run 33699342417, base `a0fe83a` and `6 passed in
  230.67s`, and the two phrasings of the old "not re-dispatched" note join the stale set.
  pins: v1-gate-audit/C-007
- `test_v3_cov_docs.py` — **V3-COV (2026-09-03; tree pins):** the coverage document, the registry The Step-6 count pin also reads the program count in `task/roadmap/epic-term/map.md` and `docs/design/map.md`.
  and the discharge lines hold one matrix — §1's totals are counted from §3 rather than asserted
  beside it, every DIVERGES row cites a registry row that exists, the six rows this unit filed
  carry a class, the date and their pin, the two fork-routed rows name a TRIGGER, and the north
  star and the v3 track carry the dated discharge; `_read` and `_matrix_rows` are cached, so the
  nine tests parse the document once. `_TOTALS` is the one place the counts live, so
  a re-measurement that moves a total moves this file too. Mutation battery: 9 red of 9
  (ledger §6).
  **Critic remediation round 2 (2026-09-03):** the DIVERGES-cites-a-row pin refused nothing when
  the cited cell was EMPTY (`"" != "—"`, and `" —"` occurs everywhere in the registry), so it now
  requires a non-empty cell and anchors on that row's own `^#+ <row> —` heading; and the Step 6
  pin asserts `_TOTALS`' program count appears in the track's dated line, which is how the stale
  80 survived the first cut.
  **RP-8 (2026-09-03):** `V3-COV-3` was FIXED by the repin, so the TRIGGER pin covers `V3-COV-6`
  alone and a new pin, `test_v3_cov_3_records_the_repin_that_retired_it`, holds the closed
  reading — the date, the fork PR that closed it, the twelve-of-twelve measurement and the
  renamed cell — so a row cannot be quietly re-opened or its evidence dropped. `_TOTALS` is
  unchanged: the nine partitioned rows were re-measured on both engines with the `_row_id` probe
  restored and every verdict held.
  **B-MOR-3 (2026-09-03):** `_TOTALS` moves to 72 EQUAL / 8 DIVERGES with the CALL row's flip,
  and `_CITED` drops `B-MOR-3` — the row is FIXED, not a covered divergence.
  pins: rp-8-repin-f21-f22/C-007
  pins: v3-cov-statement-coverage/C-001, C-004, C-005
  pins: b-mor-3-rewrite-position-deletes-v3/C-004
- `test_v1_gate_docs.py` — **V1-GATE (2026-09-03; tree pins):** the v1.0 gate audit is written
  and true. The north star's §3.1 must carry twenty numbered audit rows, every one glyphed ✅
  and none naming a BACKLOG residual; each of the seven rows that has a residual must name its
  registry row, that row's class word and its date, and every `crates/` or `python/` path in a
  pin cell must exist. The three rows the audit re-glyphed (types, encryption keys, DV / delete
  file maintenance) must read `✅ by dated DECLARED …` with their ruling dates, and the
  `rewrite_manifests` row must carry the SCALE-v3 v3 exercise rather than the v2 wiring alone.
  The gate paragraph must carry exactly ONE dated audit line, must say the tag is the owner's
  step and must never claim it. The fork half reads the five 🟡 `GAP_MATRIX.md` rows the gate
  leans on and the pin rev they were read at, which must still be the one in `Cargo.toml`.
  STATUS must carry the SCALE-v3 numbers, the V3-10 / RDF-1 / LOG1P-1 lines, the audit line and
  its 25,000-byte ceiling, and the published gate board must be filed under `docs/artifacts/`
  with a map row naming its sources.
  **Critic remediation (2026-09-03):** the audit is scoped to each row's §3 v1.0-requires cell,
  so the pin no longer forbids the word BACKLOG outright — a row may name one only beside
  `outside the requires cell` — and it now holds the surface-residual table (`RDF-1`,
  `ORPHAN-1/2`, `MANIFEST-1/3` with their classes), row 13's DELIBERATE-by-analogy-to-OD-2 cell
  with its pending owner line, row 3's queue-entry clause, row 17's undated `S3T-1` clause, the
  note that §2 pillar 4's statement coverage is owed rather than discharged, the narrowed gate
  line, and the Step 6 + slate entries that queue it as V3-COV.
  **Round 2 (2026-09-03):** `_RESIDUAL_JUSTIFICATIONS` holds the verbatim §3 requires cell each of
  the two V3-COV residual rows quotes — row 6 "stays opt-in until V3-3; default remains v2" and
  row 9 "full DML including UPDATE/MERGE, round-tripped" — because a paraphrase there had widened
  row 6's ask into one the cell never made. The residual row-id match is a word-boundary regex,
  so `V3-COV-8` no longer satisfies the pin by matching inside a longer id.
  **B-MOR-3 (2026-09-03):** row 13's cell reads the FIXED ruling with the `B-MOR-3-FLOOR-1`
  residue beside it, the owner paragraph reads BUILD, `_SURFACE_RESIDUALS` and
  `_RESIDUAL_ROWS` follow row 13 out of the residual table, and the EQUAL count is 72.
  **RP-11 (2026-09-04):** that residue is FIXED; the surface-residual class is
  `FIXED 2026-09-04 (RP-11)`.
  pins: v1-gate-audit/C-001, C-002, C-003, C-004, C-005, C-006
  pins: b-mor-3-rewrite-position-deletes-v3/C-004
- `test_reg_1_registry_truth_up.py` — **REG-1 (2026-08-26; tree pins):** the divergence registry
  says what the pins prove — DEC-2 / DEC-6 / DEC-7 / DEC-8 carry dated FIXED notes naming #94 / #99
  and their equality pins (C-001); TZ-8 splits into the FIXED `CAST(ts AS DATE)` / `to_date` /
  `datediff` half (#100) and the `last_day` / `date_add` / `date_sub` residual (C-002); G3-E8
  states the delivered spellings (incl. correlated DELETE IN and uncorrelated UPDATE IN) and the
  true remainder (C-003); the three STATUS bullets match the registry under the ceiling (C-004);
  every cited test resolves and DEC-9 stays BACKLOG (C-005); no row deleted and the maps are in
  lockstep (C-006). NULLABILITY-2 (2026-09-05) flipped C-005 to the DEC-9 FIXED text
  (test name kept per the G2 precedent).
  Cycle 2 (Critic): the DEC notes date by the fix's landing day (2026-08-14), and the TZ-8
  residual names only the pinned spellings (`date_sub` refuses too but is unpinned — not claimed).
  Departure: the `date_sub` window ends at the next heading; DEC rows are asserted by their
  heading or FIXED-note opener, not by a bare id.
  C-006's lockstep half asserts the departed state (the ledger listed by `completed/` or the
  archive map), the way DL-5's slate pin turned over — CI caught the in-flight spelling.
- `test_dl_6_docs_links.py` — **DOCS-LINKS-1 (2026-09-09):** the markdown link gate on a
  scratch tree: a clean fixture counts its files and links (relative links, two GitHub-style
  anchors, one `docs:` evidence cell; externals, code spans and fenced blocks
  out of scope); a missing target, an untracked-on-disk target, a bad anchor and a bad
  `docs:` cell each red with the `path:line: <link> -> <reason>` line; an allowlist entry
  (keyed `path:link` per D-9, so a line shift never reds the gate) drops exactly its own
  finding while a stranger link stays red and a malformed entry fails closed (exit 2); a
  stale allowlist entry that matched no finding reds the gate with its own line (D-8, audit
  round 1); the real tree runs green under the seeded allowlist. **REVIEW-FIX-12
  (2026-09-10):** eight red-first pins for the REVIEW-1 rounds Q-35, Q-36, Q-37, Q-38, Q-46,
  Q-47 — a heading that is a link slugs its rendered text (and the raw-markdown anchor is
  rejected), `Foo` / `Foo` / `Foo-1` anchor `foo` / `foo-1` / `foo-1-1`, a `docs:` token in
  ledger prose is no evidence cell, an unclosed fence is its own finding, an absolute
  target is skipped, a same-file anchor that does not resolve reds, a four-space-indented
  fence hides nothing (while a two-space-indented opener still does, as a guard). The
  clean fixture's same-file fragment now points at its target file, so the count is six
  links. The tests carry no inline pins, so both units' clauses are cited on this line.
  pins: docs-links-1/C-001, C-002, C-003, C-004, C-005
  pins: review-fix-12/C-001, C-002, C-003, C-004, C-005, C-006
- `test_dl_5_contract_compaction.py` — **DL-5 (2026-08-25):** STATUS Current milestone keeps
  the forward path and drops the H-2 wave paste (C-001, C-002); STATUS ceiling ratchets down
  (C-003); engineering-method points at AGENTS.md for invariants and keeps the method
  (C-004, C-005); AGENTS.md keeps the enumerated KEEP set (C-006); no `.agents/roles/`
  (C-007); CEILINGS (d) covers AGENTS.md and the method skill (C-008); DL-4 C-008 still
  holds (C-009); PYC-5 tokens, method how-to, slate #2 (C-010..C-012).
- `test_dl_4_live_doc_compaction.py` — **DL-4 (2026-08-25; 11 tests, incl. the Critic's three
  pinned findings and the two tree pins C-006 / C-008):** the live-document compaction on
  a scratch repository — a merged unit leaves the slate whole and the table renumbers (C-002), a
  closed campaign is cut from STATUS into its history bin with links rewritten (C-003), the
  touched-path set (C-004), the parser's refusals (parametrized) and the coverage check (C-001; a wrapped closed-campaigns
  row is one row; a marker in a code span is prose), and the gate red
  on each of its four classes and green on the compacted tree (C-005).
- `test_v3r_1_rulings.py` — **V3R-1 (2026-08-25; tree pins):** the five owner rulings are
  recorded where the gate reads them — registry `V3-COW-1` (refusal row) and `V3-GEO-1`
  (DECLARED), the queued `V3-VARIANT-SHRED-1`, the north-star matrix rows (COW, types,
  upgrade) and OD-3b, the tier-2 runbook's scoped S3 Tables statement, and the no-obituary
  rule for the unit itself. RP-2 salvage (2026-08-28) retargeted the `V3-COW-1` assertions to
  the narrowed row. RP-3 (2026-08-30) retargeted again: live-DV DELETE merge lifts; UPDATE,
  MERGE, and sequential COW after overwrite stay refused (BACKLOG, 2026-08-25 ruling kept).
  **V3-9 (2026-09-02):** the MOR DML matrix row assertion flipped from "one 🚫, V3-8 measured"
  to "no 🚫, `V3-MOR-1` FIXED plus the dated `V3-DV-1` residual naming fork F-18 and repin
  RP-7", and the STATUS assertions follow the compacted v3 block — including the
  `F-rp3-c7 consumed` substring, which no longer depends on where the line wraps
  (pins: v3-9-mor-predicate-dml-dv/C-005, C-007). **RP-7 (2026-09-02):** the same row now
  asserts `V3-DV-1` FIXED and that STATUS's Known-issues link to it is GONE, so the residual
  cannot be re-opened silently (pins: rp-7-f18-repin/C-003).
  V3-7 (2026-09-02) lifts MERGE; V3-8 (2026-09-02) lifts subquery-`WHERE` DML and the row
  becomes FIXED — the assertions now check the FIXED heading, the discharged ruling, the
  `F-v3-8-update-files` artefact and a 🚫-free north-star COW row
  (pins: v3-8-subquery-where-lineage/C-003; v3-7-merge-lineage/C-003). RP-6 (2026-09-01) lifts UPDATE and sequential
  COW DELETE (pins: rp-6-fork-repin/C-002, C-006). V3-3 (2026-08-30) records the measured keep-refusal: Spark preserves `_row_id`; the engine
  rewrite reassigns (pins: v3-3-dml/C-003). V3-6 (2026-09-01) renames the V3R-1 test to
  `test_v3_geo_1_is_declared_and_shredded_variant_is_rowed_with_v3_6_pins`, retargets the
  `V3-VARIANT-SHRED-1` assertion to the landed §4 row citing the binary-vs-shredded pins,
  and bounds the geo slice before the new section (pins: v3-6-v3-types/C-007);
  ruff-formatted in the same pass.
- `test_dl_2_ledger_grammar.py` — **DL-2 (2026-08-23):** the ledger grammar gate on a scratch
  tree seeded with the script's own `EXCEPTIONS` rows at their ceilings: a clean ledger counts;
  a bad verdict cell, a duplicate id and a row without evidence go red; an unpinned `PROVEN`
  clause and a dead `pins:` citation go red, archived and completed clauses can be cited; the attestation is
  required once no clause is `OPEN` and its shape defects (no artifacts, no justification, a
  missing category, an inconsistent `complete:`) go red; a ledger with no clause table goes
  red; `FINDING:` fields are checked; a raised ceiling or a stale `EXCEPTIONS` row goes red
  against the real tree. Each test cites the DL-2 clause it pins. The DL-1 file's archive-row
  tests likewise cite the DL-3 clauses (the condense rule).
  **LEDGER-READING-1 (2026-09-09):** the same scratch tree gains a reading ledger — its
  `Path` header field set to READING inside the first 40 lines — and three cases: the reading
  ledger's unpinned `PROVEN` clause passes rule B; the identical ledger without the marker reds
  with the standing rule-B message; the reading ledger without an attestation block still reds
  rule C. The new tests carry no inline pins, so the unit's clauses are cited on this line.
  Step 2 (2026-09-09) added C-004, the real-tree flip of BALLISTA-AUDIT-0's twelve clauses;
  no new test was written for it, so its citation rides this line with its evidence cell
  holding the gate runs.
  pins: ledger-reading-1/C-001, C-002, C-003, C-004
  **REVIEW-FIX-10 (2026-09-10):** three new pins on the same scratch tree — a `STANDARD`
  ledger quoting the marker in prose and a `READING-FOO` header both fire rule B, and a
  mid-line `READING.` field stays exempt; the real-field exemption rides the existing test.
  The new tests carry no inline pins, so the unit's clause is cited on this line.
  pins: review-fix-10/C-001
  **REVIEW-FIX-15 (2026-09-11):** the reading fixture's Verdict and Evidence cells are
  the way round the grammar describes (Q-49), and the unit cited LEDGER-READING-1's
  C-001..C-003 from the tests that pin them. Under ruling S2-14 (2026-09-11) a `pins:`
  citation lives in this map, never in code, so the `_pins` code-as-data bindings the
  unit first used are gone and the unit's own clause citations sit on the next line.
  C-004 has no pinning test (its pin is the real-tree gate run in its evidence cell), so
  its citation rides the ledger-reading-1 line only, which keeps all four rows as
  navigation.
  pins: review-fix-15/C-001, C-002
  **REVIEW-FIX-15B (2026-09-11):** the S2-14 move itself — the four `_pins` bindings
  deleted from the tests, the citations they carried re-homed to this bullet.
  pins: review-fix-15b/C-001, C-002
- `test_dl_1_ledger_lifecycle.py` — **DL-1 (2026-08-23):** the ledger lifecycle
  script on a scratch git repository: `archive` moves a `completed/` ledger to
  its dated archive name, rewrites every link to it (fragments kept, code spans
  untouched), re-expresses the ledger's own links, relocates its map row — whole
  into the live bins, condensed to one line (first sentence, `+ `-continuations
  joined) into an archive month map (DL-3) — and stages the lot; idempotent; a ledger not on `main` is left when unnamed (the pickup case)
  and refused when named; `move` to
  `completed`; `archive` is not a `move` target. The provocation proofs of
  `check`: a ledger outside the bins, an archive prefix disagreeing with its
  month, a dead ledger link in a non-map document, and the frozen rule (link
  repair and a prepended errata pass; a prose edit and a deletion fail).
- `test_pyc_6_docstring_presence.py` — **PYC-6 (2026-08-22; the prose-homes pin retargeted to PYC's
  history record by DL-4, 2026-08-25):** five presence
  rules only; style `D` not in py-lint select; tests keep the `D` per-file
  ignore; EXCEPTIONS is 39 keys summing to 136, no `/tests/` path, sorted;
  Ruff pin matches the Makefile; dual-wired `make ci` + ci.yml; on the
  pre-commit hook (conventions stays off).
- `test_pyc_5_close.py` — **PYC-5 (2026-08-22; the prose-homes pin retargeted to PYC's history
  record and its slate copy dropped by DL-4, 2026-08-25):** nested-def EXCEPTIONS empty;
  DATACLASS leftover is dual-wire only; facade tests no longer ignore ANN201;
  conventions guard is not on the pre-commit hook and stays in `make ci` +
  ci.yml.
- `test_pyc_4_parity_harness.py` — **PYC-4 (2026-08-22):** nested-def EXCEPTIONS
  table empty; DATACLASS_EXCEPTIONS is only the dual-wire script; 20 converted
  files import-free of `dataclasses`; every converted `BaseModel` AST-pins
  `extra="forbid"` and not `strict=True`; lifted modules have zero nested `def`s;
  signal-handler / shrink-predicate / spy / dual-wire comparator keep a
  `# nested-def:` pragma *on the def line*; `CensusRow` extra-field refuse + int
  `test_id` refuse; recorded-denominator dummy ids are strings; `repark-parity`
  declares `pydantic>=2.10,<3`; isolated `make py-test` / ci.yml `--with pydantic`
  (C1-Q-001); root Ruff ANN ignores split so parity tests see ANN201/ANN202.
  **PYC-5:** facade ANN201 pin retargeted (ignore dropped).
  Behaviour stays on `test_compare_reports.py` / `test_compat_harness.py`.
- `test_datasets_secrets.py` — **DS-3** secrets fixture: A9 defaults, table-identity
  determinism, manifest class→column coverage in schema order, the needle labels
  re-derived with the `prop_key_is_secret` fold (lowercase, hyphen/dot → underscore,
  underscores stripped for the compact form) **without importing repark**, the
  `bucket_key` `_key` carve-out as a negative control, the hard hygiene fence (every
  value starts with `repark-fake-`; no `AKIA…`/`ghp_…`/`sk-…`/`xoxb-…` shape, no `@`,
  no URL), the one nullable credential column, parquet identity, CSV columns, CLI.
  **Acceptance pin in the module docstring:** reads behave NORMALLY today — the opt-in
  secrets-flagging mechanism is a roadmap feature this fixture predates, so nothing
  here asserts redaction. Facade read pins are DS-4.
- `test_datasets_smartcsv.py` — **DS-3** messy-CSV torture generator: A9 defaults,
  table-identity determinism, both manifest scopes (**column** classes present in
  `small()`; **file** classes provable in the emitted text at 64 rows), the delimiter
  zoo (comma / semicolon / tab / pipe, one file each, byte-equal to `render_csv`),
  BOM + preamble, duplicate header row emitted twice, ragged rows in both directions
  with the short-wins tie (row 137), bool spellings vs yes/no tokens that only look
  boolean, recognized
  vs literal null tokens, currency + decimal width variants, embedded-delimiter
  quoting in every scheme, parquet identity, CLI.
- `test_datasets_manifest_types.py` — **DS-3 rider (from the DS-2 review):** the
  manifest↔schema cross-check over all four labeled families (`schema_inference`,
  `extreme_types`, `secrets`, `smartcsv`). Every manifest-declared type string must
  equal the real Arrow field type after normalizing spacing (`decimal128(24, 21)` vs
  `decimal128(24,21)`) and pyarrow's rendering aliases (`double` → `float64`,
  `date32[day]` → `date32`). Both directions are closed: no manifest row may name a
  column the schema lacks, and no schema field may go unlabeled outside the explicit
  `EXPECTED_UNLABELED` set (`id` in the two DS-2 families). The normalizer is itself
  pinned so a no-op normalizer cannot hide a mismatch, and class ids must be unique.
- `test_datasets_schema_inference.py` — **DS-2** schema-inference generator: manifest
  class→column pin, A9 defaults, `conflict_at` int32→int64 + string/float halves,
  parquet identity, CSV text patterns, CLI `--conflict-at`. **DS-3 rider:** the
  `leading_zero_id` pad width is derived from the requested row count (a fixed `06d`
  loses the leading zero at `row_index >= 1_000_000`, and `MAX_ROWS` is 10M) — pinned
  at the >1M boundary through `leading_zero_width` / `leading_zero_id` with explicit
  widths, never by generating a million rows, plus a helper↔column binding test.
- `test_datasets_extreme_types.py` — **DS-2** extreme-types generator: manifest
  classes, decimal128(24,21), beyond-38 digit strings, uuid5, paragraph length,
  HTML example.com-only, parquet identity, CLI.
- `test_datasets_nested.py` — **DS-1** nested / dynamicFlatten generator: A9 defaults
  (64 / seed 42), table-identity determinism (not raw bytes), parquet + JSON-lines
  re-read under `SCHEMA`, labeled classes (depth ≥ 6, capitalized `Legs`, mixed list
  types, null-typed lists, empty/null list rows), cache symlink + in-repo refuse, CLI
  `--out`. Loads `repark_datasets` via the bench sys.modules loader. Ledger:
  `task/c18-datasets-ledger.md`.
- `test_dynflatten_bed.py` — **PERF-DYNFLATTEN-1** measurement-bed pins: charter
  shapes, dict-encoded capitalized `Name`, 30 % null parents, cartesian sibling
  lists, list widths 1/8/64, parquet round-trip, gate manifest, full-scale skip of
  `list_struct_64`, real-dataset flag/env refuse, in-repo write refuse, the isolation shapes
  kept out of the headline set, and the ranking contract: cost is one fixture's isolated delta
  (never a sum), and a candidate is queued only above 3x the measured noise floor.
  pins: perf-dynflatten-1-measure/C-001, C-002
- `test_compare.py` — equal/unequal frames, order-insensitivity, null handling, schema/row-count
  mismatches, and a **field-nullability difference** (part of the schema signature — a differing
  `nullable` flag with identical name/type/values is a parity failure). **G18 nested invariants:**
  (1) flat-schema `sort_by` path unchanged, (2) nested row-permutation invariance for
  list/struct/map, (3) multiset sensitivity mutation per nested kind, (4) `order_sensitive=True`
  untouched on nested tables; plus list-element-order significance and nested-only schemas.
- `test_compat_harness.py` — C2 / R-PYSPARK-COMPAT unit pins: message-first JVM classification,
  HARNESS narrowness, both denominators (incl. MODULE-TIMEOUT stay-in), `tag_for_pyspark_version`,
  worker env scrub, method-name filter (no prefix steal), `Py4J*` type → NEEDS-JVM,
  `validate_module_short` path-injection refuse, zero-padded tag normalize,
  TimeoutError → MODULE-TIMEOUT (RecordingResult + classify), third-party ImportError → HARNESS
  (incl. site-packages TB + **X1** cache path `repark-pyspark-tests` must not false-FAIL-MISSING;
  **octo C1** site-packages/repark frame + pandas still HARNESS; product `repark.*`
  ModuleNotFoundError + cache path stays FAIL-MISSING),
  unknown status clamp in rank+denominators, module-name dotted IDs, markdown cell escape;
  **C3** `STRETCH_MODULES`/`C3_EXPAND_MODULES` order pin (**U8:** `test_udf` IN);
  **octo C3** `resolve_census_modules` dual-denom pin (`--c3-expand` ignores night-1/`--stretch`);
  C3 series markdown never claims C2 zero-fix /345; ruff-clean import order + format;
  **r20 C4** `C4_EXPAND_MODULES` charter-order pin + `resolve_census_modules(--c4-expand)` dual-denom
  (never blend classic /345 or C3); C4 series/scratch distinct; `_KNOWN_FATAL_TESTS` disjoint
  from C4 cohort + exact-method deselect (not endswith prefix collision); dual-denom markdown %
  pin; filter×fatal dual-denom; testing.utils error rebind pin; PATCH_MAP AssertionError note;
  install_redirect errors-before-factories order; testing.utils rebind via **fake modules**
  (parity isolation — no pyspark/repark); `REPARK_COMPAT_SERIES` worker-only series branding
  pin (no C2 zero-fix on C4 workers; parent ignores leaked env — octo C5); frozen
  `_KNOWN_FATAL_TESTS` exact map pin (octo C6); worker JSON unknown-status clamp.
- **Phase-3 EC-8 additions to `test_compat_harness.py`** (6 tests, new in this repository):
  `CLASSIC_MODULES` charter order + disjointness from both expand cohorts;
  `resolve_census_modules(classic=True)` denominator isolation (hostile `--modules` ignored);
  the fixed precedence `--c4-expand` > `--c3-expand` > `--classic`; the keyword-only default
  that keeps every ported call site unchanged; the **`--stretch` blending pin** (eleven modules,
  not five — the trap `--classic` exists to avoid, documented in executable form); and the CLI
  wiring pin that `--classic` actually reaches the resolver.
  **G-6:** `default_markdown_report_path` pin — markdown defaults under gitignored
  `target/census-reports/` (C-2 alignment), never `task/`.
- `test_compare_reports.py` — the battery for `compat/compare_reports.py`, the census report
  comparator (NEW code; design §6.4). Over synthetic reports: identical → exit 0; each of the
  five delta directions (pass→fail, fail→pass, class-change, appeared, vanished) named and
  exit 1; both denominators re-asserted; deferred subtraction from the baseline side only, with
  the echo, the not-present entry, and the "deferred id appeared in the candidate" finding;
  quarantine excluded both sides; mismatched environment manifests → loud exit 2 **with no diff
  body at all**; `generated_at` / `repark_version` deliberately not gated; sorted-rendering byte
  exactness (a case-only class difference is still a difference); duplicate ids and malformed
  JSON → loud failure; junit mode with skips first-class (skip→pass detected, xfail
  distinguished from skip, manifests required); and the **provoked undeclared subtraction** that
  proves the checked-in ledger file is the only way a row leaves the diff (five plausible
  environment variables set, an `--exclude` flag attempted, the frozen option set asserted).
  Also pins the three properties that keep the manifest gate from being nominal: an external
  `--manifest-*` file may not overwrite a key the report records (the shared-manifest attack that
  makes two different environments render identical); `pandas_version` is refused when it
  **differs** and equally when it is **unrecorded** (a key nobody records compares equal by
  absence); and each report's own recorded denominator block is validated against its own rows —
  including the case where both sides are byte-identical *and* identically wrong, which the
  post-subtraction re-assert alone cannot catch.
- `test_deferred_ledger.py` — **phase-3 PR-5 (EC-4)**: the harness that binds the checked-in
  deferral ledger to the comparator's allowlist. There is exactly one file
  (`task/port/deferred-python-tests.txt`), so byte-identity is pinned by proving the single-file
  property — the comparator's documented acceptance invocation names that path, and its own
  `load_ledger` is what parses it here. Both EC-4 failure directions are closed: every deferred id
  must be a **pin-collected** name (under-subtraction — a row that names nothing removes nothing)
  and must be **absent from the ported tree** (over-subtraction — listed AND ported means a row is
  subtracted from the baseline while still running here). Absence is checked statically via `ast`,
  so the test needs no wheel and runs in the ordinary `make py-test` loop. Also pins that the
  prose ledger names every machine-readable id, so the two halves cannot drift. **PR-5 fixer:**
  a third direction is closed — every deferred id must resolve to a row of the recorded baseline
  JUnit XML *through `load_junit_report`*, the loader the facade cohort's `--junit` gate actually
  uses; the id-space mismatch that assertion catches made the ledger subtract nothing.
- `test_ta_bench_conf.py` — **BH-1 (conductor-19):** default-conf `target_partitions`
  contract for `bench/ta` (omit knob + emit `default`; isolation emits `1` +
  `isolation=single_core`). Helper unit pins + AST scan of the six scripts.
  No engine / numpy / native module.
- `test_w0_window_bench.py` — **W-0:** engine-free pins for the window-shape
  bench (roster equals the charter enumeration, 1e7 unpartitioned constant,
  seeded generator, DuckDB/PySpark pins, thirteen sliding refuses including
  int64 `approx_count_distinct`, fourteen planning-absent names after TYPES-1
  planned `count_if`, result model fields, scratch delete in `finally`, no
  retry on the error path, WIN-SLIDE registry headings).
  **WIN-SLIDE-1 (2026-09-04):** the thirteen moved from `REFUSING_SLIDING_NAMES` (now empty) to
  `RESCANNED_SLIDING_NAMES`; two pins were added —
  `test_every_rescanned_name_has_a_fixed_registry_row` (each name keeps its `WIN-SLIDE-<name>`
  heading, now carrying `FIXED 2026-09-04 (WIN-SLIDE-1)`) and
  `test_the_frozen_sliding_refuse_set_is_empty`.
  pins: w-0-window-bench/C-001, C-002, C-003, C-004, C-007, C-008, C-009, C-010, C-011;
  fnp-7-try-inversions/C-012; win-slide-1/C-008.
- `test_redact.py` — the battery for `compat/redact.py`, the recorded path-redaction transform.
  Its one hard property is that the artifact still parses afterwards, so the two regressions are
  explicit contrasts: a naive text substitution over a traceback-bearing census report emits an
  unescaped quote and stops being JSON, and one over a JUnit XML turns `<scratch>` into an element
  start tag; the parser-based transform cannot do either. Plus key redaction, non-string scalars
  untouched, XML attributes, longest-prefix-wins ordering, malformed-input loud failures, plain
  text passthrough, in-place rewrite idempotence, and the CLI exit codes.
  Stamp pin moved to `_Last updated: 2026-09-10._` with the REVIEW-FIX-4 departure truth-up (2026-09-10). PERF-DESCRIBE-1 (2026-09-12): the helper-call inventory briefly gained `statistics.py` for the literal VALUES grid of the single-pass `describe`; the remediation round moved the unpivot into a lazy `mapInArrow` bridge, so `statistics.py` carries no SQL-literal helper call and the inventory row is gone again.

## Pointers

- Up: [../map.md](../map.md)

### PR-4 orchestrator additions
- `test_duplicate_test_id_loads_when_quarantined` / `…_without_quarantine_still_refuses` —
  both directions of the comparator's one duplicate escape (quarantined ids may repeat,
  first row wins), with the fixture recomputing the recorded denominator block over rows AS
  CARRIED — matching the real v1 expand artifact.

### PR-7: --added tests
- `test_added_cell_present_on_candidate_side_only_passes` / `test_added_does_not_subtract_from_the_baseline_side`
  — both directions of the additions mirror (candidate-side subtraction), plus the frozen-option
  pin now includes `--added`.

## Debug

- `test_sqp_1_record.py` is itself byte-frozen by `test_pr_245_revalidation_record.py`.
  `test_cap_1_source_file_line_cap.py` mirrors both size gates' exception tables and their row
  counts: regenerate the tuples in the commit that ratchets a gate, then run this suite
  (`make py-test` — the suite is not in `preflight`; its CAP-1 file is, as
  `make py-test-parity-cap`, since PREFLIGHT-PARITY-1, 2026-09-09).
| Symptom | First check |
|---|---|
| `test_datasets_manifest_types` reds | A schema field and its `manifest.json` row were edited one-sidedly; the failure names the family and class id |
| `test_datasets_secrets` reds on the hygiene fence | A value stopped starting with `repark-fake-` or picked up a real credential shape — fix the value, never the fence |
| `test_deferred_ledger` reds on "ALSO ported" | A node id is in the txt AND still defined in `python/repark/tests` — excise the test or drop the row |
| `test_deferred_ledger` reds on "absent from the recorded pin collection" | The id does not name a real v1 node; check it against `task/census/baseline-fc3f48102/facade/collected.txt` |
| `test_deferred_ledger` reds on the human summary | `task/port/deferred-tests.md` must name every id in the txt verbatim |
| `test_deferred_ledger` reds on "would subtract nothing" | The id does not survive `junit_node_id` into a row of `…/facade/facade.xml`; check the id form, not the XML (the XML is recorded evidence — never hand-edit it) |

First checks: `PYTHONPATH=python/repark-parity/src pytest python/repark-parity/tests -q`.
Escalate to: [../map.md#debug](../map.md).
