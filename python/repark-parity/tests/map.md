# map — python/repark-parity/tests

ICE-MIXED-CASE-1 round 3 (2026-09-17): the CAP-1 mirror `_RUST_BASELINES` follows the six shrink-only ratchets (merge/mod.rs 1782, merge/tests/merge.rs 1032, streaming_scan.rs 3020, predicate_dml.rs 1141, predicate_dml/tests 1440, cross_door.rs 1254). pins: ice-mixed-case-1/C-012

ICE-MIXED-CASE-1 round 5 (2026-09-17, Q-20b-2): the mirror follows two more ratchets (merge/mod.rs 1780, predicate_dml.rs 1139). pins: ice-mixed-case-1/C-012

DF-PLAN-INTROSPECT-1 follow-up round 3 (2026-09-15, R-11): CAP-1 mirror row
ratcheted down with the code — `dataframe/core.py` 4027 → 4014 (the
`sameSemantics` body moves to `dataframe/plan_introspect.py`). The
`check_lib_py.py` exception row moved in the same commit; no row raised.
pins: df-plan-introspect-1/C-012

TYPES-BASES-1 (2026-09-14): CAP-1 mirror row ratcheted down with the code —
`spark/types.py` 1834→1791 after the abstract bases, spatial types,
`UserDefinedType`, and spatial token helpers moved to `spark/types_bases.py`.
The `check_lib_py.py` exception row moved in the same commit; no row raised.
Follow-up on FACADE-4 step 1: `spark/types.py` 1792→1770 (critic round 1 —
the `_merge_type` spatial arms and `StructField`/`StructType` conversion
delegates added inside the file; the struct conversion bodies live in
`types_bases.py`). The `test_ex_0_example_coverage.py` count moves 930 → 942
and the types family 32 → 44 as the twelve new `types.*` names join the
enumerated surface (both covered by the new `docs/examples/types/` scripts).

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

- [cast/](cast/map.md) — **PERF-CAST-1 step 1 (2026-09-12):** the CAST-cost
  golden CSV pin (nine shape×count rows), the 2500-cast standalone regression
  budget (1.5× the measured median), and the strict-xfail linear-from-50 pin
  on CAST-over-aggregate planning. Needs the native module.
  pins: perf-cast-1/C-001, C-002, C-003, C-004
- [eager_own/](eager_own/map.md) — **EAGER-OWN-1 steps 0+1 (2026-09-13):** the
  bare-`eager()` retention harness — a subprocess worker running N bare
  `eager()` calls on a deterministic TA `withColumns` fixture while recording
  wall / VmRSS / VmHWM and `__repark_cache_*` registration counts, an
  always-on small pin (2,000 rows × 3), and the `REPARK_EAGER_OWN_BENCH=1`-gated
  million-row × 10 pin. Both pins assert the post-fix owned shape — zero
  registrations after each call, post-loop, post-gc and post-clearCache — and
  wrote `docs/perf/eager-own-1-2026-09-13/{base,after}.json`.
  Needs the native module.
  pins: eager-own-1/C-001, C-012
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
- `test_platform_1_wheel_matrix.py` — **PLATFORM-1 (2026-09-12):** the abi3 wheel-matrix
  pins over the two workflows and the release doc — `release.yml` `build-wheel` names
  exactly the five legs (`manylinux-x86_64`/`ubuntu-latest`, `manylinux-aarch64`/
  `ubuntu-24.04-arm`, `macos-arm64`/`macos-latest`, `macos-x86_64`/`macos-13`,
  `windows-x86_64`/`windows-latest`) and `publish-pypi` merges `release-wheel-*`;
  `wheels.yml` `platform-matrix` names the four legs PRs never see behind the
  schedule-or-dispatch `if:` plus the cron/`workflow_dispatch` triggers; the PR `smoke`
  job keeps its gate and host; `docs/release.md` names all five legs. Doctored leg
  lists (dropped, renamed, appended, re-hosted, unmerged, cron removed, `pull_request`
  reachability) each fail. YAML read by indentation-aware regex, no PyYAML.
  pins: platform-1/C-001, C-002, C-003, C-004, C-005
- `test_ice_read_perf_bench_workflow.py` — **ICE-READ-PERF-0 (2026-09-19):** pins over the
  dispatch-only `ice-read-perf-bench` job of `aws-acceptance.yml`. `live-aws` keeps the nightly
  and runs only on the schedule or `leg == acceptance`. The dispatch offers `leg` (choice,
  `acceptance` default) and `purpose`. The bench job runs only on its dispatch, behind
  `environment: aws-acceptance`, the ref guard, job-scoped `id-token: write`,
  `persist-credentials: false` and no `continue-on-error`. It uses the same action SHAs as
  `live-aws` plus the repo's `upload-artifact` SHA. Step order: build, then credentials, then
  S3 Tables create, compaction `disabled` and read back, S3 Tables write, Glue create and write,
  both run loops, the bytes summary, the upload. It runs four modes on both catalogs at
  `BENCH_REPEAT: "1"` (Q-24a-1) under `set -euo pipefail`. No `${{ }}` reaches a `run:` script, and the job
  carries no `#` comment. Doctoring compaction to `enabled` or adding `continue-on-error` reds
  it. Round 3 (F-MUT-4B): no failure can be swallowed. The only `||` in the job's `run:`
  scripts is `|| stop "…"` on the two `aws s3tables` calls, and every `cargo bench` command
  line (backslash continuations joined, `>` scripts folded) carries no `||`, `&&`, `;` or
  pipe, so `|| true`, `&& true` or `; true` after any of the seven bench commands or the two
  compaction calls reds `test_no_bench_or_s3tables_failure_can_be_swallowed`. Round 3
  (F-SEC-REF-ORDER): the bench job's ref guard (`if: github.ref != 'refs/heads/main'`, `exit
  1`) is the first ordered marker. It must be the job's first step, ahead of
  `actions/checkout@` and `aws-actions/configure-aws-credentials@`, so moving it after the
  credentials or after checkout, or dropping its `exit 1`, reds the step-order pin. Round 3
  (F-SEC-PURPOSE, hardening): every `${…}` in the job's `run:` scripts sits inside double
  quotes. A small scanner tracks single quotes, double quotes, `$(…)` and `$((…))` to check it,
  and it is self-pinned on a mixed sample. Unquoting `${PURPOSE:-unstated}`, the `mkdir`
  target, `--mode "${mode}"` or the `--table-bucket-arn` inside the `$(aws …)` reds
  `test_every_variable_expansion_in_the_bench_scripts_is_double_quoted`. Two more pins hold
  every expansion braced (`test_every_variable_expansion_in_the_bench_scripts_is_braced`) and
  errexit never turned off (`test_errexit_is_never_turned_off_in_the_bench_scripts`). The critic's claim
  that `purpose` can run command substitution was measured false (bash 5.2.21, 2026-09-19).
  `PURPOSE='$(touch /tmp/pa-pwn)'; echo "x ${PURPOSE:-unstated}"` prints the text literally and
  creates no file; backticks and the unquoted form behave the same, because bash does not
  re-evaluate an expanded value. YAML read by regex, no PyYAML.
  **ICE-BENCH-BASELINE-1 (2026-09-19):** the dispatch gains a boolean `baseline` input
  (default false, "the before half of a pair on one head"). Both run loops take it through
  `env: BASELINE`, append `--baseline` to all eight `run --mode` invocations only when it
  is `true` (`baseline_flags` array, empty by default — an empty `"${array[@]}"` expands to
  zero arguments under `set -u`, verified on bash 5.2.21), and the summary header names
  `baseline=<value>` beside the purpose. Three pins hold the input, the threading and the
  summary line.
  pins: ice-read-perf-0/C-018, C-019
  pins: ice-bench-baseline-1/C-004
- `test_ex_0_example_coverage.py` — **IO-BUCKET-CLUSTER-1 (2026-09-14):** the raw-walk
  count pin moved 936 → 944 with the eight new writer-layout inventory names
  (`DataFrameWriter.bucketBy`/`bucket_by`/`sortBy`/`sort_by`/`clusterBy`/`cluster_by`,
  `DataFrameWriterV2.clusterBy`/`cluster_by`); backlog and exceptions baselines hold.
  pins: io-bucket-cluster-1/C-003
- `test_ex_0_example_coverage.py` — **FNP-GEN-1 step 2 (2026-09-16):** the
  enumerated public surface moves 1062 → 1064 as `F.inline` and
  `F.inline_outer` join `__all__` through the generator installer;
  `docs/examples/functions/{posexplode,inline}.py` cover the four names and
  the backlog baseline ratchets 112 → 110.
  pins: fnp-gen-1/C-001, C-006
- `test_cap_1_source_file_line_cap.py` — **FNP-GEN-1 step 2 (2026-09-16):**
  mirror rows ratchet `functions_expr.py` 2235 → 2213 and
  `test_explode_rewrite.py` 1135 → 1133; a short-lived
  `scripts/check_example_coverage.py` row was removed when the script was
  compacted back under the default ceiling; python_approved stays 32.
  pins: fnp-gen-1/C-006
- `test_cap_1_source_file_line_cap.py` — **FNP-GEN-1 step 2 (2026-09-16, run 18a):**
  mirror rows ratchet `functions.py` 1984 → 1983 and `functions_expr.py`
  2198 → 2178 with `scripts/check_lib_py.py`.
  pins: fnp-gen-1/C-002, C-003
- `test_cap_1_source_file_line_cap.py` — **ICE-OVERWRITE-MODE-1 round 2 (2026-09-19):**
  mirror row `writer_readwriter.py` 1095 → 1093, the value round 1 set in
  `scripts/check_lib_py.py` without its mirror.
  The verification fix ratchets it again, 1093 → 1091. pins: ice-overwrite-mode-1/C-018
- `test_cap_1_source_file_line_cap.py` — **ICE-REPLACE-COLUMNS-1 (2026-09-19, run 25c):**
  mirror rows ratchet `repark-spark/src/alter.rs` 1813 → 1449 and
  `repark-spark/src/tests/alter.rs` 1379 → 1184 with `scripts/check_rust_file_size.py`,
  after the REPLACE COLUMNS planner moved into its own module.
  pins: ice-replace-columns-1/C-008
- `test_cap_1_source_file_line_cap.py` — **ICE-DROP-NS-1 (2026-09-19, run 24c):**
  mirror row ratchets `repark-sql/src/tests.rs` 1520 → 1513 with `scripts/check_rust_file_size.py`.
  pins: ice-drop-ns-1/C-011
- `test_cap_1_source_file_line_cap.py` — **ICE-TT-RESOLVE-1 (2026-09-19, run 24c):**
  mirror rows ratchet `repark-python/src/session.rs` 1126 → 1122 and `session_core.py`
  2290 → 2287 with `scripts/check_rust_file_size.py` and `scripts/check_lib_py.py`.
  pins: ice-tt-resolve-1/C-012
- `test_cap_1_source_file_line_cap.py` — **ICE-MIXED-CASE-1 (2026-09-18, run 22b):**
  mirror row ratchets `write/merge/mod.rs` to 1761 with `scripts/check_rust_file_size.py`.
  pins: ice-mixed-case-1/C-012
- `test_cap_1_source_file_line_cap.py` — **FNP-GEN-1 step 3 (2026-09-16, run 18a):**
  mirror row ratchets `functions_expr.py` 2178 → 2175 with
  `scripts/check_lib_py.py`.
  pins: fnp-gen-1/C-003, C-004
- `test_cap_1_source_file_line_cap.py` — **ICE-WRITE-OPTIONS-1 round 4 (2026-09-17):**
  mirror rows ratchet `crates/repark-iceberg/src/write/append.rs` 1882 → 1819,
  `dataframe/core.py` 4015 → 3991 and `dataframe/writer_readwriter.py` 1101 → 1093
  in lockstep with `scripts/check_rust_file_size.py` / `scripts/check_lib_py.py`
  (round 3 moved the SSOT rows and left these mirrors behind).
- `test_cap_1_source_file_line_cap.py` — **FNP-GEN-1 step 4b (2026-09-16, run 18a):**
  mirror row ratchets `functions_expr.py` 2175 → 2177 with
  `scripts/check_lib_py.py`.
  pins: fnp-gen-1/C-004
- `test_cap_1_source_file_line_cap.py` — **FNP-GEN-1 step 7 (2026-09-16, run 18a):**
  mirror row ratchets `functions_expr.py` 2177 → 2178 with
  `scripts/check_lib_py.py`.
  pins: fnp-gen-1/C-006
- `test_cap_1_source_file_line_cap.py` — **IO-BUCKET-CLUSTER-1 (2026-09-14):**
  `dataframe/writer_readwriter.py` mirror row 1111 → 1105 with the script baseline
  (the bucketBy/sortBy/clusterBy bindings and action-check calls landed while the five
  write helpers moved to the new `dataframe/writer_layout.py`, 312 lines below the
  default). pins: io-bucket-cluster-1/C-003 **FNP-WIN-1 (2026-09-15, remediation
  round 16a):** mirror rows for `time_window/mod.rs` 1268,
  `spark_time_window.rs` 1125, `test_fnp_win_1.py` 1209, counts 38/32
  (`datetime.rs` keeps 1700 per audit S-2). pins: fnp-win-1/C-008
  **FNP-WIN-1 (2026-09-15, verification round 2, L-002/L-003):** mirror rows
  move to `time_window/mod.rs` 1338, `test_fnp_win_1.py` 1314, counts still
  38/32 (ratchets only, no new row). pins: fnp-win-1/C-004
  **FNP-WIN-1 (2026-09-15, verification round 2, L-001):** mirror rows move
  to `time_window/mod.rs` 1416, `test_fnp_win_1.py` 1415, counts still
  38/32. pins: fnp-win-1/C-003
  **FNP-WIN-1 (2026-09-15, verification round 2, L-004):** mirror rows move
  to `dataframe.rs` 1021, `test_fnp_win_1.py` 1454, counts still 38/32.
  pins: fnp-win-1/C-004
  default). pins: io-bucket-cluster-1/C-003
- `test_cap_1_source_file_line_cap.py` — **FNP-11B step 6 (2026-09-15):**
  `functions.py` mirror row 1960 → 1984 and `functions_expr.py` 2235 → 2237
  with the script baseline. pins: fnp-11b/C-001, C-005
- `test_cap_1_source_file_line_cap.py` — **ICE-V3-WRITE-DEFAULT-1 (2026-09-17):**
  mirror row ratchets `dataframe/writer_readwriter.py` 1101 → 1095 with
  `scripts/check_lib_py.py` (the by-name projection returns the target column
  list and stops refusing missing frame columns).
  pins: ice-v3-write-default-1/C-006
  Run 21b round 2 (2026-09-18): 1095 → 1094 — `overwritePartitions()` passes the
  same column list into its `INSERT OVERWRITE` in two lines instead of three.
  pins: ice-v3-write-default-1/C-020
  **FNP-11B remediation round 1 (2026-09-16):** `functions_expr.py` mirror row
  2237 → 2220 with the script baseline (the `make_timestamp` forwarder becomes a
  direct re-export). pins: fnp-11b/C-007
- `test_cap_1_source_file_line_cap.py` — **ICE-COLUMN-REORDER-1 (2026-09-17):**
  mirror rows ratchet `write/alter.rs` 1630 → 1607, `spark/src/alter.rs` 1821 → 1813
  and `spark/src/tests/alter.rs` 1397 → 1379 with `scripts/check_rust_file_size.py`
  (column-move and partition-spec families split to sibling modules).
  pins: ice-column-reorder-1/C-013
- `test_ex_0_example_coverage.py` — **DF-SURFACE-B-1 (2026-09-14):** the enumerated
  public surface moves 948 → 952 as `DataFrame.foreach`,
  `DataFrame.foreachPartition`, `DataFrame.observe`, and `Observation.get` join
  the dataframe family; `docs/examples/dataframe/foreach_observe.py` covers them.
  pins: df-surface-b-1/C-005
- `test_ex_0_example_coverage.py` — **FNP-11B step 3 (2026-09-15):** the enumerated
  public surface moves 1057 → 1059 as `F.to_timestamp_ltz` / `F.to_timestamp_ntz`
  join the functions family; `docs/examples/functions/timestamp_ltz_ntz.py` covers
  them with `F.try_to_timestamp`.
  pins: fnp-11b/C-007
- `test_ex_0_example_coverage.py` — **FNP-11B step 4 (2026-09-15):** the enumerated
  public surface moves 1061 → 1067 as the TIME family (`F.make_time` /
  `F.to_time` / `F.time_diff` / `F.time_trunc` / `F.current_time` / `F.typeof`)
  joins the functions family; `docs/examples/functions/time_family.py` covers
  them (answers for `current_time` / `typeof`, refusal asserts for the rest).
  pins: fnp-11b/C-007
- `test_ex_0_example_coverage.py` — **FNP-11B step 5 (2026-09-15):** the enumerated
  public surface moves 1067 → 1071 as the to_char family (`F.to_char` /
  `F.to_varchar` / `F.to_number` / `F.to_binary`) joins the functions family;
  `docs/examples/functions/to_char_family.py` covers them (answers plus the
  two strict-twin refusal asserts). pins: fnp-11b/C-007
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
  **FNP-ALIAS-1 (2026-09-15):** 955 → 961 as `approxCountDistinct`, `shiftLeft`, `shiftRight`, `shiftRightUnsigned`, `toDegrees` and `toRadians` join the functions family (walked through `functions_agg.py` / `functions_bitwise.py` / `functions_math.py`, covered by `docs/examples/functions/deprecated_aliases.py`, so the backlog baseline is unchanged). pins: fnp-alias-1/C-001
  **FNP-MISC-1 (2026-09-15):** 1006 → 1010 as `call_function`, `call_udf`, `arrow_udf` and `arrow_udtf` join the functions family (walked through `functions_byname.py` / `functions_arrow_udf.py`, covered by `docs/examples/functions/by_name_and_arrow_udfs.py`, so the backlog baseline is unchanged). pins: fnp-misc-1/C-001
  **FNP-11A (2026-09-15):** 1010 → 1021 as the eleven temporal names installed by `functions_temporal.py` join the walked functions family (covered by `docs/examples/functions/temporal_constructors.py`, so the backlog baseline is unchanged). pins: fnp-11a/C-001
  **FNP-BITMAP-FACADE-1 (2026-09-15):** 1010 → 1013 as `bitmap_construct_agg`, `bitmap_or_agg` and `bitmap_and_agg` join the functions family (walked through `functions_bitwise.py`'s `INSTALL_NAMES`, covered by `docs/examples/functions/bitmap_aggregates.py`, so the backlog baseline is unchanged). pins: fnp-bitmap-facade-1/C-004
  **FNP-BITMAP-FACADE-1 (2026-09-15):** 1018 → 1021 as `bitmap_construct_agg`, `bitmap_or_agg` and `bitmap_and_agg` join the functions family (walked through `functions_bitwise.py`'s `INSTALL_NAMES`, covered by `docs/examples/functions/bitmap_aggregates.py`, so the backlog baseline is unchanged). pins: fnp-bitmap-facade-1/C-004
  **FNP-WIN-1 (2026-09-15):** 1052 → 1055 as `window`, `window_time` and `session_window` installed by `functions_window.py` join the walked functions family (covered by `docs/examples/functions/time_windows.py`, so the backlog baseline is unchanged). pins: fnp-win-1/C-002, C-003, C-004
  **DF-RUST-3 (2026-09-15, rebased onto main after FNP-WIN-1):** 1062 → 1064 as `DataFrame.freqItems` and `DataFrame.transpose` join the dataframe family (both covered by `docs/examples/dataframe/freq_items_transpose.py`; `DataFrameStatFunctions.freqItems` leaves the backlog, so the backlog baseline ratchets 112 → 111). pins: df-rust-3/C-005
- `test_plan_1_northstar_fnp_sequence.py` — **PLAN-1 (2026-08-28; tree pins):** the guarded
  North Star sequence, F-17's measured shared-Puffin closure request, the live slate, the
  per-unit FNP remaining order (FNP-7a/7b delivered 2026-08-31; remaining FNP-9/10 → FNP-8
  → FNP-11/12 → FNP-Z) and delivery boundary, FNP-Z retirement, fork independence, and map
  lockstep, including the archived V3-3 and F-rp3-c7 record. **V3-8:** STATUS Next is
  row-lineage carry on the statement matrix's DML shapes (narrowed 2026-09-17 after the
  2026-09-16 rating measured failures after schema evolution).
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
  **COLUMN-PARITY-1 critic round (2026-09-14):** `build_api_freeze.py` follows
  `alias = _module.func` class bindings to the defining module (instance arg dropped),
  so the sanctioned `column_fields` split keeps `Column.between` / `Column.eqNullSafe`
  param pins exact; binding targets join the scratch-tree sources. pins: column-parity-1/C-007
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
  **FNP-4B (2026-09-15):** the `test_sqp_1_string_literals.py` hash re-baselined for the BL-9/BL-12
  FIXED flips (the old pins documented this red→green). **BL-11 (2026-09-16):** the same hash
  re-baselined for the in-place ANSI-on flip of `test_numeric_to_binary_refuses`.
  H3-SPILL-1 (2026-09-05): the literal-helper inventory gains
  `bench/spill/cell_worker.py` `sql_string_literal` 1 — the spill harness escapes its own
  warehouse path into `CREATE NAMESPACE … LOCATION`, so it uses the helper rather than a
  second escape rule. MAINT-POLICY-1 (2026-09-10): the inventory gains
  `spark/session/session_maintenance.py` `sql_string_literal` 2 — the facade wrapper renders the
  table name and any string override into the `CALL … run_maintenance(…)` text through the helper
  rather than an f-string, so the one place a caller's value reaches SQL stays inside the audited
  set. CATALOG-SURFACE-1 critic round 1 (2026-09-14): the inventory gains
  `spark/catalog_surface.py` `sql_string_literal` 2 — `create_table` renders
  `TBLPROPERTIES` keys and values through the helper. pins: h3-spill-1/C-001,
  maint-policy-1/C-030, catalog-surface-1/C-009
- `test_cap_1_source_file_line_cap.py` — **CATALOG-SURFACE-1 (2026-09-14):** `spark/session/session_core.py` row 2304 → 2291 with the script baseline (`table()` delegates to `catalog_surface.session_table`; the orphaned `_sql_table_ref_resolved` helper removed). pins: catalog-surface-1/C-004 **Critic round 1 (2026-09-14):** `spark/dataframe/core.py` row 4044 → 4043 with the script baseline (`create_or_replace_temp_view` delegates registration to `catalog_surface._register_temp_view`; the frame-token note lives in `cache_handle.bind_registered_view`). pins: catalog-surface-1/C-009 **SESSION-SURFACE-1 (2026-09-15, rebase onto #602):** `spark/session/session_core.py` row 2291 → 2290; EX-0 counts recounted after the Catalog merge. pins: session-surface-1/C-001 **DF-SURFACE-B-1 (2026-09-15, rebase onto #609):** `create_or_replace_temp_view` delegates to `surface_b.register_view_without_fill`, which wraps `catalog_surface._register_temp_view` in the Observation fill suppression; `dataframe/core.py` ratchets down. pins: df-surface-b-1/C-008 **IO-DECLARED-1 (2026-09-15, rebase onto #603):** the mirror drops `spark/session/reader.py` again — the unit retired that exception (954 lines, under the default ceiling) and the rebase had restored main's row. pins: io-declared-1/C-006 **ICE-DYN-OVERWRITE-1 round 2, ruling Q-20a-6 (2026-09-17):** `spark/dataframe/writer_readwriter.py` row 1101 → 1109 (`spark/session/session_core.py` holds 2290) with the script baselines. pins: ice-dyn-overwrite-1/L-001
- `test_ex_0_example_coverage.py` — **CATALOG-SURFACE-1 (2026-09-14):** measured counts move with the 26 new Catalog names — `len(rows)` 930 → 956 and `families["catalog"]` 28 → 54; the four new `docs/examples/catalog/` scripts cover every name. pins: catalog-surface-1/C-007
  set. pins: h3-spill-1/C-001, maint-policy-1/C-030
- `test_cap_1_source_file_line_cap.py` — **GROUPED-SURFACE-1 step 1 (2026-09-14):** `dataframe/joins_columns.py` mirror row 1238 → 1169 with the script baseline (the Arrow-batch apply bridge moved byte-identical into the new `dataframe/grouped_arrow.py`; the grouped-surface bindings are one-line class aliases). pins: grouped-surface-1/C-007
- `test_ex_0_example_coverage.py` — **GROUPED-SURFACE-1 step 1 (2026-09-14):** the enumerated public surface moves 948 → 954 as `GroupedData.apply`, `GroupedData.applyInArrow`, `GroupedData.cogroup`, `GroupedData.applyInPandasWithState`, `GroupedData.transformWithState`, `GroupedData.transformWithStateInPandas`, `PandasCogroupedOps.applyInPandas` and `PandasCogroupedOps.applyInArrow` join the dataframe family; `docs/examples/dataframe/grouped_udfs.py` covers all eight, so `check-example-coverage` stays clean. pins: grouped-surface-1/C-007
  COLUMN-PARITY-1 critic round (2026-09-14): the inventory gains
  `spark/column.py` `_sql_string_literal` 1 — struct field access renders its join-ON
  bracket key through the helper rather than an f-string. pins: column-parity-1/C-007
- `test_cap_1_source_file_line_cap.py` — **CAST-MAP-SPELL-1 (2026-09-19):** `repark-python/src/session.rs` row 1127 → 1126 and `spark/column.py` row 1532 → 1529 with the script baselines (the map-cast type token forwards to the native parser).
- `test_cap_1_source_file_line_cap.py` — **COLUMN-PARITY-1 critic round (2026-09-14):** `spark/column.py` row 1548 → 1532, `dataframe/core.py` row 4044 → 4040 and `dataframe/plan_collapse.py` row 1057 → 1054 with the script baselines (the deferred struct-edit machinery is deleted for the native `update_fields` design; struct field access gains a join-ON bracket fragment). pins: column-parity-1/C-007
- `test_cap_1_source_file_line_cap.py` — **FNP-11A (2026-09-15):** the `functions_expr.py` row ratchets 1010 → 1021 with the script baseline (the destubbed `make_timestamp` forwarder keeps the frozen 1.0 signature, D-10). pins: fnp-11a/C-001
- `test_cap_1_source_file_line_cap.py` — **SET-ANSI-RUNTIME-1 (2026-09-15):** the `tests/test_session_timezone_parity.py` mirror row 1328 → 1318 with the script baseline (the applied-contract flips are net-negative). pins: set-ansi-runtime-1/C-005
- `test_cap_1_source_file_line_cap.py` — **FNP-4B (2026-09-15):** the three Rust rows
1068 → 1065 / 1052 → 1040 / 1084 → 1082 and the `_live_parity.py` row 1778 → 1763 in
both tables with the script baselines (backtick-disclosure retire). pins: fnp-4b/C-009, C-010
- `test_cap_1_source_file_line_cap.py` — **ICE-COMMIT-UNKNOWN-1 (2026-09-14):** `repark-core/src/session/tests/session.rs` row 1412 → 1407 in both tables (the ambiguous-commit classification pins moved to `session/tests/commit_unknown.rs`). pins: ice-commit-unknown-1/C-001
- `test_cap_1_source_file_line_cap.py` — **IO-DECLARED-1 (2026-09-14):** `dataframe/writer_readwriter.py` mirror row 1111 → 1110 with the script baseline and the `session/reader.py` row retires (1022 → 954, under the default; the orc/xml/jdbc refusals bind from `io_declared.py`); `test_ex_0_example_coverage.py` pins the 955 → 961 raw walk and `EXCEPTIONS_BASELINE` 2 → 1 (`DataFrameReader.jdbc` now covered). pins: io-declared-1/C-005, C-006
- `test_cap_1_source_file_line_cap.py` — **DOOR-CONVERGE-2 (2026-09-15, G-2 Q1 one-time grant R-1 under Q-15c-4):** `repark-functions/src/analyzer.rs` row 1142 → 1150 with the script baseline (the `array_concat` → `concat` analyzer arm for Q12-16 outer nullability). pins: door-converge-2/C-001
- `test_cap_1_source_file_line_cap.py` — **DF-SURFACE-A-1 critic round 1 (2026-09-14):** `dataframe/core.py` row 4041 → 4035 with the script baseline (rulings R-5/R-6 removed the `inputFiles`/`semanticHash` bindings and the `_schema_override` slot). pins: df-surface-a-1/C-008
- `test_cap_1_source_file_line_cap.py` — **DF-SURFACE-A-1 step 1 (2026-09-14):** `dataframe/core.py` row 4044 → 4041 with the script baseline (the `localCheckpoint` body moved to `dataframe/surface_a.py`; the nine surface-a names bind one line each and `_schema_override` lands for `to`/`withMetadata`). pins: df-surface-a-1/C-006
- `test_cap_1_source_file_line_cap.py` — **FACADE-4 step-1 remediation round 4 (2026-09-14, L-007..L-009):** `spark/types.py` mirror row 1772 → 1792 with the script baseline (base's container `simpleString`/`_engine_type`/`jsonValue` dispatch bodies and `_SIMPLE_STRING_FAST` returned for byte-identical MRO/override answers; still below main's 1834 ceiling). pins: facade-4/C-029, C-031
- `test_cap_1_source_file_line_cap.py` — **FACADE-4 step-1 remediation round 3 (2026-09-14, ruling R14b-D-2):** `spark/types.py` mirror row 1610 → 1772 with the script baseline (the per-class literal `simpleString`/`_engine_type` methods returned for the `dtypes` surface bar; still below main's 1834 ceiling). pins: facade-4/C-016
- `test_cap_1_source_file_line_cap.py` — **FACADE-4 step-1 remediation round 2 (2026-09-14):** `spark/types.py` mirror row 1834 → 1610 with the script baseline (round-2 remediation moved `_parse_datatype_string` and the conversion fallbacks into `spark/_type_table.py`; the P1-DTYPES ratchet had missed its mirror row). `repark-python/src/dataframe.rs` mirror row 1084 → 1019 fills the step-1 ratchet that commit skipped. pins: facade-4/C-020
- `test_cap_1_source_file_line_cap.py` — **REPLACE-LINEAR-1 step 1 critic round (2026-09-14):** `dataframe/core.py` row 4054 → 4044 with the script baseline (the `_join_qualifiers` slot plus minimal call sites for P2-3 multi-name equi-join `replace`; the join-side aliasing, qualifier assignment, and plan-metadata propagation all live in `dataframe/replace_expr.py`). pins: replace-linear-1/C-004
- `test_cap_1_source_file_line_cap.py` — **REPLACE-LINEAR-1 step 1 (2026-09-14):** `dataframe/core.py` row 4089 → 4054 with the script baseline (the `DataFrame.replace` body moved to `dataframe/replace_expr.py`). pins: replace-linear-1/C-002
- `test_cap_1_source_file_line_cap.py` — **EAGER-BUDGET-1 step 2 (2026-09-13):** `dataframe/core.py` row 4094 → 4089 with the script baseline (the budget resolver consolidated into `eager.py` and the `cache()`/`persist()` docstrings trimmed); `repark-python/src/session.rs` holds its 1128 row. pins: eager-budget-1/C-005
- `test_cap_1_source_file_line_cap.py` — **ABS-EXPR-1 (2026-09-13):** `spark/functions.py` row 1985 → 1962 and `spark/functions_expr.py` row 2255 → 2247 with the script baselines (the facade `when(...)` bodies deleted for native `abs`/`cbrt`/`nullif`). pins: abs-expr-1/C-005
- `test_cap_1_source_file_line_cap.py` — **COMMENT-CORE-1 (2026-09-13):** `dataframe/core.py` 4468 → 4117 with the script baseline (comments removed, no code change). pins: comment-core-1/C-004
- `test_cap_1_source_file_line_cap.py` — **FACADE-2 step 2b (2026-09-12):** `spark/column.py` 1549 → 1548 with the script baseline. pins: facade-2/C-013
- `test_cap_1_source_file_line_cap.py` — **FACADE-2 step 2 (2026-09-12):** `spark/column.py` 1589 → 1549 with the script baseline. pins: facade-2/C-008, C-009
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
- `test_ex_0_example_coverage.py` — **COLUMN-PARITY-1 critic round (2026-09-14):** the enumerated public surface moves 930 → 937 and the column family 40 → 47 as the seven `Column` methods (`isin`, `isNaN`, `astype`, `name`, `outer`, `withField`, `dropFields`) join the surface; `docs/examples/column/struct_fields.py` covers all seven, so `check-example-coverage` stays clean. pins: column-parity-1/C-007
- `test_ex_0_example_coverage.py` — **COLUMN-PARITY-1 re-check round 2 (2026-09-15):** the `origin/main` #600 rebase moved the surface 948 → 955 (seven `DataFrame` names); the merged count is 955 → 962 with the column family already at 47. pins: column-parity-1/C-009
- `test_ex_0_example_coverage.py` — **DF-EAGER-1 step 3 (2026-09-09):** the enumerated public surface moves 923 → 926 as `DataFrame.eager`, `DataFrame.compute` and `DataFrame.lazy` join the dataframe family (923 is CFG-1 step 3's count, merged first); `docs/examples/dataframe/lazy_and_eager.py` covers all three, so `check-example-coverage` stays clean. This pin is in the parity suite, which `make preflight` does not run — a PR that adds a public name must run the parity suite before pushing, and two such PRs open at once each carry their own count until one merges. pins: df-eager-1/C-001
- `test_ex_0_example_coverage.py` — **FNP-9/10 (2026-09-06):** the enumerated public surface
  moves 913 → 921 as the eight built `F.*` names join `functions.py`'s `__all__` through
  `functions_json.install_into`. pins: fnp-9-collections-json/C-001
- `test_ex_0_example_coverage.py` — **CFG-1 step 3 (2026-09-09):** the enumerated public surface moves 921 → 923 as `Builder.config_file` and its `configFile` camelCase twin join the session family; both are covered by `docs/examples/session/config_file.py`, so `check-example-coverage` stays clean. This pin lives in the parity suite, which `make preflight` does not run — a PR that adds a public name must run it before pushing. pins: cfg-1/C-026
- `test_ex_0_example_coverage.py` — **CFG-1 step 4 (2026-09-10):** the surface stays 923 — the new `repark.config` mirror is not one of the enumerated doors (functions / class surfaces / module `__all__` / `repark.sql`), so no count moves and no covering example is owed. pins: cfg-1/C-031
- `test_ex_0_example_coverage.py` — **MAINT-POLICY-1 audit fix (2026-09-10):** the enumerated public surface moves 926 → 927 as `SparkSession.run_maintenance` joins the session family declared in the class body; `docs/examples/session/run_maintenance.py` covers it, so `check-example-coverage` stays clean. pins: maint-policy-1/C-030
- `test_ex_0_example_coverage.py` — **IO-DECLARED-1 (2026-09-15, rebase onto main after #604–#609):** the enumerated public surface moves 1035 → 1041 with the orc / xml reader-writer bindings, `DataFrameWriter.jdbc` and `DataFrameNaFunctions.replace` (measured on the rebased tree). pins: io-declared-1/C-006
- `test_ex_0_example_coverage.py` — **IO-TEXT-1 (2026-09-15, rebase onto IO-DECLARED-1):** the enumerated public surface moves 1035 → 1043 with `DataFrameReader.text` and `DataFrameWriter.text` (measured on the rebased tree). `test_cap_1_source_file_line_cap.py` — `dataframe/writer_readwriter.py` 1104 → 1101 with the script baseline. pins: io-text-1/C-003, C-004
  **PERF-UNPIVOT-1 (2026-09-12):** 927 → 928 as `F.stack` joins the functions family. pins: perf-unpivot-1/C-004
  **CFG-2 step 2 (2026-09-13):** 928 → 930 as `SparkSession.source` and `SparkSession.sources` join the session family (both covered by `docs/examples/session/named_sources.py`, so the backlog baseline is unchanged). pins: cfg-2/C-013
  **ROW-TUPLE-1 (2026-09-14):** 930 → 932 and the types family 32 → 34 as `Row.count` and `Row.index` join the enumerated surface (both covered by `docs/examples/dataframe/row_tuple.py`; backlog baseline unchanged). pins: row-tuple-1/C-003
  **DF-STREAM-BATCH-1 (2026-09-14):** 930 → 934 as `DataFrame.withWatermark`, `DataFrame.with_watermark`, `DataFrame.dropDuplicatesWithinWatermark` and `DataFrame.drop_duplicates_within_watermark` join the dataframe family (all four covered by `docs/examples/dataframe/batch_streaming_names.py`, so the backlog baseline is unchanged; `rdd`/`plot`/`writeStream`/`pandas_api` bind through a tuple assignment the AST walk does not enumerate). pins: df-stream-batch-1/C-007
  **TYPES-BASES-1 follow-up (2026-09-14):** 930 → 942 and the types family 32 → 44 as the twelve new `types.*` names (the eight abstract bases, `GeographyType`, `GeometryType`, `UserDefinedType`, `types.Row`) join the enumerated surface — all covered by `docs/examples/types/abstract_bases.py` and `docs/examples/types/spatial_and_udt.py`. pins: types-bases-1/C-006
  **DF-SURFACE-A-1 (2026-09-14):** 932 → 939 and the dataframe family 153 → 160 as `DataFrame.to`, `withMetadata`, `registerTempTable`, `checkpoint`, `sparkSession`, `isLocal` and `executionInfo` join the enumerated surface (all covered by `docs/examples/dataframe/schema_reconcile.py` and `docs/examples/dataframe/session_and_checkpoint.py`; `inputFiles` and `semanticHash` left the unit under R-5 to the future Rust unit DF-PLAN-INTROSPECT-1; backlog baseline unchanged). pins: df-surface-a-1/C-008
  **SESSION-SURFACE-1 step 1 (2026-09-14):** 930 → 949 as the 19 card names join the session family — the tag quartet, the interrupt trio, the five Connect-only names, `readStream`/`streams`/`dataSource`, `addArtifact`/`addArtifacts`, `profile`, `tvf` — all covered by the three new `docs/examples/session/` scripts, so the backlog baseline is unchanged. pins: session-surface-1/C-009
  **SESSION-SURFACE-1 step 1 (2026-09-14):** 948 → 967 as the 19 card names join the session family — the tag quartet, the interrupt trio, the five Connect-only names, `readStream`/`streams`/`dataSource`, `addArtifact`/`addArtifacts`, `profile`, `tvf` — all covered by the three new `docs/examples/session/` scripts, so the backlog baseline is unchanged. pins: session-surface-1/C-009
- `test_ex_0_example_coverage.py` — **DF-PLAN-INTROSPECT-1 (2026-09-14):** the enumerated public surface moves 961 → 963 and the dataframe family 164 → 166 as `DataFrame.inputFiles` and `DataFrame.semanticHash` join the enumerated surface through the one-line class-body delegations (both covered by `docs/examples/dataframe/plan_introspect.py`, so the backlog baseline is unchanged). pins: df-plan-introspect-1/C-003
- `test_ex_0_example_coverage.py` — **DF-PLAN-INTROSPECT-1 (2026-09-15, rebase onto main after #605–#609):** the enumerated public surface moves 1035 → 1037 (1041 → 1043 after #610) with `DataFrame.inputFiles` and `DataFrame.semanticHash` (measured on the rebased tree). pins: df-plan-introspect-1/C-004
- `test_ex_0_example_coverage.py` — **DF-SUBQUERY-1 (2026-09-15, on `feat/df-subquery-1` after #611):** the enumerated public surface moves 1059 → 1063 and the dataframe family 177 → 181 as `DataFrame.scalar`, `DataFrame.exists`, `DataFrame.lateralJoin` and `DataFrame.asTable` join the surface through four class-body bindings in `core.py` (line-neutral against the exact-baseline ceiling); `docs/examples/dataframe/subquery.py` covers all four, so `check-example-coverage` stays clean and the backlog baseline is unchanged. pins: df-subquery-1/C-007, C-008
- `test_cap_1_source_file_line_cap.py` — **MAINT-POLICY-1 audit fix (2026-09-10):** `session/session_core.py` row 2305 → 2304 with the script baseline; `_temp_view_home_ref` moves to `catalog_resolution.py` to pay for the `run_maintenance` class-body declaration. pins: maint-policy-1/C-030
- `test_cap_1_source_file_line_cap.py` — **FNP-ALIAS-1 (2026-09-15):** the `functions_expr.py` row ratchets 955 → 961 and the `functions.py` row 1962 → 1960 with the script baselines (`degrees`/`radians` move to `functions_math.py`, which sits under the default ceiling; the tail gains a third module-handle line for the new `install_into` modules, paid by one narration comment). pins: fnp-alias-1/C-001, C-004, C-006
- `test_cap_1_source_file_line_cap.py` — REVIEW-FIX-6 (2026-09-10): `dataframe/core.py` row 4487 → 4486 with the script baseline; the four-line both-set guard is paid for by five moved comment lines, their facts moved to `python/repark/src/repark/spark/dataframe/map.md`. pins: review-fix-6/C-004
- `test_cap_1_source_file_line_cap.py` — DISPLAY-LAZY-1 (2026-09-11, measured on the rebased tree after REVIEW-FIX-6 #487 took the row to 4486 by the same comment-funded method): `dataframe/core.py` row 4486 → 4485 with the script baseline; the checkpoint-arm `_eager_shape` record is paid for by deleting the cache-pinned early-return rationale (fact restated on the dataframe map). pins: display-lazy-1/C-007
  FACADE-1 (2026-09-12): `dataframe/core.py` 4485 → 4473 → 4470 with the script baseline.
  pins: facade-1/C-001, C-006
- `test_cap_1_source_file_line_cap.py` — **FN-FIX-2 (2026-09-04):** `analyzer.rs` 1161→1142. PERF-FACADE-1 (2026-09-05): `core.py` row 6368 → 6303 with the script baseline. CUTOVER-SCHEMA-1 (2026-09-05): `session.rs` 1040 → 1039 and `repark-python/src/dataframe.rs` 1171 → 1127 with the script baselines; the REG-1 DEC-9 pin follows the row's narrowed rationale. PERF-ICE-CATALOG-IO-1 (2026-09-05): `session.rs` 1039 → 1002 in both tables. FNP-9/10 (2026-09-06): `functions_expr.py` 2259 → 2256 in both tables as `arrays_zip` and `schema_of_json` trade a multi-line refusal for a one-line wrapper.
- `test_cap_1_source_file_line_cap.py` — **FN-FIX-2 (2026-09-04):** `analyzer.rs` 1161→1142. PERF-FACADE-1 (2026-09-05): `core.py` row 6368 → 6303 with the script baseline. CUTOVER-SCHEMA-1 (2026-09-05): `session.rs` 1040 → 1039 and `repark-python/src/dataframe.rs` 1171 → 1127 with the script baselines; the REG-1 DEC-9 pin follows the row's narrowed rationale. PERF-ICE-CATALOG-IO-1 (2026-09-05): `session.rs` 1039 → 1002 in both tables. H3-SPILL-RESIDUE-1 (2026-09-06): `repark-python/src/dataframe.rs` 1127 → 1126 in both tables.
- `test_cap_1_source_file_line_cap.py` — **FN-FIX-2 (2026-09-04):** `analyzer.rs` 1161→1142. PERF-FACADE-1 (2026-09-05): `core.py` row 6368 → 6303 with the script baseline. CUTOVER-SCHEMA-1 (2026-09-05): `session.rs` 1040 → 1039 and `repark-python/src/dataframe.rs` 1171 → 1127 with the script baselines; the REG-1 DEC-9 pin follows the row's narrowed rationale. PERF-ICE-CATALOG-IO-1 (2026-09-05): `session.rs` 1039 → 1002 in both tables. NULLABILITY-2
  (2026-09-05): `core.py` row 6303 → 6302 with the script baseline, then the
  `_live_parity.py` row 1877 → 1778 with the script baseline when the three
  converged nullability disclosures retire.
- `test_cap_1_source_file_line_cap.py` — **FN-FIX-2 (2026-09-04):** `analyzer.rs` 1161→1142. PERF-FACADE-1 (2026-09-05): `core.py` row 6368 → 6303 with the script baseline. CUTOVER-SCHEMA-1 (2026-09-05): `session.rs` 1040 → 1039 and `repark-python/src/dataframe.rs` 1171 → 1127 with the script baselines; the REG-1 DEC-9 pin follows the row's narrowed rationale. PERF-ICE-CATALOG-IO-1 (2026-09-05): `session.rs` 1039 → 1002 in both tables. PERF-APPROXPCT-1 (2026-09-05): `repark-python/src/column/mod.rs` 1053 → 1052 with the script baseline.
- `test_cap_1_source_file_line_cap.py` — **DF-PLAN-INTROSPECT-1 (2026-09-15, rebase onto #610/#612):** `dataframe/core.py` row 4014 → 4015 with the script baseline (the wrapped import gains `replace_expr`). pins: df-plan-introspect-1/C-004
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
  ICE-OCC-SCOPED-1 (2026-09-17) ratchets `write/merge/mod.rs` 1792 → 1773 with the gate
  table (pins: ice-occ-scoped-1/C-005).
  The same unit ratchets `repark-spark/src/tests/alter.rs` 1436 → 1397 — the obsolete
  WRITE-refusal blocks are deleted (pins: write-order-dist-1/C-001).
  NIGHTLY-LIVE-1 (2026-09-11) ratchets `test_ml_boost_oracle.py` 2244 → 2241 with the
  gate table — the PySpark teardown `try/finally` retires; `_live_parity.py` stays at
  its 1778 baseline line-neutral (the `catalog` kwarg pays for itself by compressing the
  docstring, and `spark_session_conf` restores never-set keys via `conf.unset`).
  pins: nightly-live-1/C-003
  **DF-SURFACE-B-1 rebase (2026-09-15):** `dataframe/core.py` row → 4034 with the script baseline after the rebase over DF-SURFACE-A-1 (#600) merged both import lines.
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
  alone. **ICE-REGISTRY-SWEEP-1A (2026-09-17):** the 2026-09-16 rating re-measured the row and
  it stands OPEN, so the pin is renamed to `test_v3_cov_3_records_the_measured_reopening` and
  holds the corrected reading — the OPEN disposition with the measured date, the 1-of-12
  `INSERT … SELECT` fact, the 12-of-12 VALUES/CTAS fact and the `p_rowid_order` probe — so a
  row cannot be quietly re-closed on the retired evidence (its docstring is wrapped to Ruff's
  100-column limit). `_TOTALS` is
  unchanged: the nine partitioned rows were re-measured on both engines with the `_row_id` probe
  restored and every verdict held.
  **B-MOR-3 (2026-09-03):** `_TOTALS` moves to 72 EQUAL / 8 DIVERGES with the CALL row's flip,
  and `_CITED` drops `B-MOR-3` — the row is FIXED, not a covered divergence.
  pins: rp-8-repin-f21-f22/C-007
  **RP-31 (2026-09-18):** the V3-COV-3 cell reads the row's close (`FIXED 2026-09-18 (RP-31, fork #300)`,
  the 6-distinct reopening, the hash-order residual, both pin files).
  pins: v3-cov-statement-coverage/C-001, C-004, C-005
  pins: b-mor-3-rewrite-position-deletes-v3/C-004
  **ICE-OVERWRITE-MODE-1 (2026-09-19):** the stated totals follow the flipped verdict (EQUAL
  73, DIVERGES 7). pins: ice-overwrite-mode-1/C-018
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
  **UNRESOLVED-ROUTINE-1 (2026-09-16):** the classifier pin also covers Spark's
  `UNRESOLVED_ROUTINE` absence shape. pins: unresolved-routine-1/C-006
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
  **IO-BUCKET-CLUSTER-1 (2026-09-15):** one `escape_sql_single_quotes` call moved with `_sql_option_escape` from `dataframe/writer_readwriter.py` (2 → 1) to the new `dataframe/writer_layout.py` (1); the inventory follows.
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
- **FNP-11A (2026-09-15, on 440b2773):** the CAP-1 table mirrors the `functions_expr.py` ratchet to 2235 lines.
- `test_cap_1_source_file_line_cap.py` — **DOOR-CONVERGE-2 (#622, 2026-09-15):** the `crates/repark-python/src/column/mod.rs` mirror row 1052 → 1036 matches the script baseline (ratchet down). The `analyzer.rs` row 1142 → 1150 is the one-time grant R-1 (Q-15c-4).
- **FNP-4B remediation (2026-09-15):** CAP-1 mirror ratcheted with the scripts: `column/mod.rs` 1038, `cross_door.rs` 1258, `_live_parity.py` 1753.
- `test_cap_1_source_file_line_cap.py` — **FNP-4B (#611, 2026-09-15, orchestrator):** the `crates/repark-python/src/column/mod.rs` mirror row 1022 → 1014 matches the script baseline after the rebase onto #613 (ratchet down).
- `test_cap_1_source_file_line_cap.py` — **UNRESOLVED-ROUTINE-1 (2026-09-16):** the `crates/repark-python/src/column/mod.rs` mirror row 1014 → 1013 matches the script baseline (the `SELECT` wrapper move, ratchet down). pins: unresolved-routine-1/C-003
- `test_ex_0_example_coverage.py` — **FNP-WIN-1 rebase onto 230468c5 (2026-09-15, run 16a):** #613 and #618 each moved the count 1054 → 1057 on the same line, which a merge keeps at 1057; the true combined count is main's 1057 plus this unit's three names, 1060. pins: fnp-win-1/C-007
- `test_cap_1_source_file_line_cap.py` — **FNP-WIN-1 orchestrator fix-up (2026-09-15, run 16a):** the `crates/repark-python/src/dataframe.rs` mirror row returns to 1019 with the script baseline. pins: fnp-win-1/C-007
- `test_ex_0_example_coverage.py` — **FNP-11B rebase onto 0355ef5e (2026-09-15, run 17a):** the branch and main each moved the count 1057 → 1059 on the same line, which the replay keeps at 1059; the true combined count is main's 1059 plus this unit's two examples, 1061. pins: fnp-11b/C-007
- `test_pr_245_revalidation_record.py` — **FNP-11B (2026-09-16, run 17a):** the shipped
  `sql_string_literal` call inventory moves `functions.py` 4 → 5. The new call is the decimal
  literal this unit's `to_char` / `to_number` family builds — `CAST(<literal> AS DECIMAL(p,s))` —
  routed through the escaping helper, which is the direction this census exists to enforce.
  pins: fnp-11b/C-007
- **FNP-GEN-1 rebase onto #640 (2026-09-16, run 17a):** EX-0 and the example backlog are set from
  **measurement**, not arithmetic — `len(rows)` 1078 read from the failing assert, backlog 108 read
  from `check_example_coverage.py`'s own report. Both sides of the rebase had moved both counts, and
  a measured count is exact where an arithmetic one only usually is. pins: fnp-gen-1/C-007
- **FNP-GEN-1 rebase onto #647 (2026-09-16, orchestrating session):** the same recipe a second
  time — `len(rows)` 1082 read from the failing assert on a fresh release native, backlog 108 read
  from `check_example_coverage.py`'s report (unchanged). pins: fnp-gen-1/C-007

ICE-V3-WRITE-DEFAULT-1 rebase onto ICE-DYN-OVERWRITE-1 (2026-09-18, run 21b): `test_cap_1_source_file_line_cap.py` mirrors `check_lib_py.py`: `writer_readwriter.py` 1109 → 1102 (the merged column-list and `static_overwrite` writer; the CAP-1 mirror moves with it). pins: ice-v3-write-default-1/C-024
