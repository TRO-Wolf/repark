# ICE-PROCS-ROUTE-1 — route `ancestors_of`, `compute_table_stats`, `compute_partition_stats`, `rewrite_table_path` (2026-09-19)

- Date: 2026-09-19
- Branch: `fix/ice-procs-route-1`
- Base: `ff5a13c3` (`origin/main`)
- Model: Muse Spark (`muse-spark-1.3-contributor`)
- Policy: SEPMO Actor–Critic = single-session
- Path: P (pure-Rust + py-facade; no JVM in routine CI)
- Risk tier: standard
- Retires: this file closes when the round's last commit merges; the registry row
  `ICE-PROCS-ROUTE-1` in `docs/spark-sql-iceberg-parity.md` carries the durable record.

## Why now

Owner direction 2026-09-18: 1:1 parity with Spark's Iceberg integration — a
refusal is not an end state where Spark answers. On main the four
`CALL <catalog>.system.<proc>` procedures refuse (`CALL … is not supported`)
while the pinned owned fork already implements the three maintenance actions.
This unit is routing (plumbing): the fork's
`iceberg::maintenance::{ComputeTableStats, ComputePartitionStats, RewriteTablePath}`
plus the snapshot parent chain for `ancestors_of`, shaped into Spark's result
schemas, rows, and refusal texts. No table-format semantics were re-implemented
locally; where the fork's answer differs from Spark's (incremental version
range), the clause stops, pins what is equal, and records the fork ask below.

## Not in this step (explicitly out of scope)

- `start_version` / `end_version` incremental rewrite (fork has no version range;
  strict-xfail pair plus fork ask R-001, not silent absorption).
- Staging one rewritten metadata file per version like Spark (fork stages the
  current one; the file list carries the staged file either way — R-002).
- Any change to `STATUS.md`, `Cargo.toml` / `Cargo.lock`, `.github/`, or the fork.
- Commit or push (owner actions; the brief's per-step commits are the unit's own).

## Contract (actor: PROVE every row with a tree pin BEFORE writing code)

| # | Clause | Verdict | Evidence |
|---|--------|---------|----------|
| C-001 | The 27 measured Spark cells ship as a committed fixture with provenance and hash | PROVEN | `python/repark/tests/ice_procs_route_1_spark_oracle.json` (SHA-256 `cfffc500…b9eb7`, provenance `spark-qc3.json` / `cells_qc3.py`, re-recorded by the round-2 recorder); step-1 commit `7c58d046` |
| C-002 | A tree recorder re-derives the cells on live PySpark | PROVEN | `python/repark/tests/_record_ice_procs_route_1_oracle.py` replays every cell shape against live Spark 4.1.2 + Iceberg 1.11.0 |
| C-003 | The live tier re-runs the recorder against the fixture | PROVEN | `test_live_oracle_matches_recorded` (`REPARK_PARITY_LIVE=1`); round 2 normalizes every run-varying token at record time to its exact writer shape, so two fresh-warehouse derivations are byte-identical and `check` compares minus `secs` with no second pass |
| C-004 | `ancestors_of` answers Spark's columns, newest-first chains, and timestamps | PROVEN | `crates/repark-spark/src/call/ancestors_of.rs`; facade pins default/id/rollback/branch/positional; Rust `call_ancestors_of_walks_newest_first_with_snapshot_timestamps` |
| C-005 | `ancestors_of` refusals carry Spark's class and text | PROVEN | `Cannot find snapshot: -1` / `Cannot find snapshot: 12345` as `IllegalArgumentException`; facade `test_ancestors_empty`, `test_ancestors_missing_id`; Rust `call_ancestors_of_names_missing_snapshots_like_spark` |
| C-006 | `compute_table_stats` default/snapshot/twice register exact ndv blobs | PROVEN | `crates/repark-spark/src/call/compute_table_stats.rs` over fork `ComputeTableStats`; facade `test_table_stats_default`, `test_table_stats_snapshot`, `test_table_stats_twice` assert the registered path and the full statistics entries |
| C-007 | `compute_table_stats columns` keeps schema order | PROVEN | Router resolves the requested names and registers them in schema position (reversed input still yields `[id]` before `[data]`); facade `test_table_stats_columns`, `test_table_stats_columns_two`; Rust `call_compute_table_stats_registers_one_blob_per_column_in_schema_order` |
| C-008 | Unknown `columns` entry refuses Spark's text | PROVEN | `Can't find column nope in table <schema>` as `IllegalArgumentException`; facade `test_table_stats_unknown_column`; Rust refusal case |
| C-009 | Every-primitive-type and struct-default cells match with exact ndv | PROVEN | Facade `test_table_stats_types` (8 columns incl. decimal/date/timestamp) and `test_table_stats_nested` (only `id` registered); fork theta sketches agree with Spark exactly at this size |
| C-010 | `compute_table_stats` on an empty table answers zero rows | PROVEN | Router returns the empty single-column frame and commits nothing when there is no current snapshot and no `snapshot_id`; facade `test_table_stats_empty`; Rust empty case |
| C-011 | `compute_partition_stats` registers per-partition counts incl. v3 `dv_count` | PROVEN | `crates/repark-spark/src/call/compute_partition_stats.rs` over fork `ComputePartitionStats`; facade default/snapshot/v3 pins assert the registered path, the entry, and the parquet contents minus volatile columns; Rust registration case |
| C-012 | Unpartitioned table refuses `Table must be partitioned` | PROVEN | Router checks the default spec before the fork runs; facade `test_partition_stats_unpartitioned`; Rust refusal case |
| C-013 | `rewrite_table_path` full rewrite stages, counts, and lists like Spark | PROVEN | `crates/repark-spark/src/call/rewrite_table_path.rs` over fork `RewriteTablePath` plus `repark_iceberg::catalog::write_text_file` for the `file-list` CSV; facade default/staging/MoR pins assert latest version, file list, staged metadata location, and counts (3,0 / 2,0 / 4,1); Rust `call_rewrite_table_path_stages_manifests_lists_and_answers_sparks_counts` |
| C-014 | `create_file_list => false` answers `N/A` | PROVEN | Router skips the list write; facade `test_rewrite_path_no_file_list`; Rust `N/A` case |
| C-015 | Wrong `source_prefix` refuses Spark's text | PROVEN | `Path …/ does not start with …/` as `IllegalArgumentException`; facade `test_rewrite_path_missing_prefix`; Rust prefix case |
| C-016 | `start_version` / `end_version` refuse loudly and stay strict-xfail | PROVEN | Router returns `NotImplemented` naming the fork's missing incremental range; facade `test_rewrite_path_end_version`, `test_rewrite_path_start_version` are `xfail(strict)`; Rust `call_rewrite_table_path_version_range_refuses_naming_the_fork_gap`; fork ask R-001 |

## Evidence

### E-001 — the oracle (what Spark answers)

27 cells measured on Spark 4.1.2 + Iceberg 1.11.0 (`local[1]`; InMemoryCatalog
`sc`, HadoopCatalog `hc`): 7 `ancestors_of`, 9 `compute_table_stats`, 4
`compute_partition_stats`, 7 `rewrite_table_path`. Seed
`(id BIGINT, data STRING, cat STRING)` partitioned by `cat`, three INSERT
snapshots S0..S2. Columns, rows (snapshot ids normalized to commit-order
positions), metadata entries (`statistics` / `partition-statistics` with blob
properties and ndv), partition-stats parquet contents, rewrite rows plus the
file-list CSV and staged metadata location, and for refusals the exception
class (`IllegalArgumentException` throughout) with Spark's message.

### E-002 — error-class map (measured, not invented)

Every procedure-layer validation Spark raises as `IllegalArgumentException`
routes through the shared `call.rs::illegal_argument`
(`DataFusionError::Configuration` → `Error::IllegalArgument` →
`IllegalArgumentException`, the `branch_ops` precedent). Unknown procedures,
missing arguments, and wrong types keep the existing `NotImplemented` /
`AnalysisException` shapes. The native `repark.sql` door has no CALL surface
by design; every facade pin also pins its documented
`Unsupported SQL statement` refusal.

### E-003 — layering rule (what the router may and may not do)

The router shapes Spark's surface (schemas, argument spellings, guards with
Spark's texts, output framing) and never computes table-format state: stats
and partition-stats bytes come from the fork actions; the ancestor chain is a
parent-pointer walk over committed metadata; the rewrite copies the fork's
staging plan and adds the staged-metadata entry the fork stages but plans no
copy for (Spark's list carries staged metadata files). Counts are read off
fork facts (covered snapshots; staged parquet position-delete files), never
re-derived. The one new cross-crate helper,
`repark_iceberg::catalog::write_text_file`, exists only because the Spark door
must not gain a buffer-type dependency for a single CSV write.

### E-004 — red first

Step-1 commit `7c58d046` ran the 28-test file against main: 25 failed on the
not-supported refusal, 1 skipped (live), 2 xfailed (version range). Seeds,
rollback, branch DDL, and the setup paths all worked before routing, so the
red was exactly the four missing procedures.

### E-005 — gates (measured 2026-09-19)

- New Python file offline `-n 4`: 25 passed, 1 skipped, 2 xfailed.
- Live leg (`REPARK_PARITY_LIVE=1`, zulu-17, pinned 1.11.0 runtime from the
  local ivy cache): 1 passed on fresh randomness.
- New Rust battery `cargo test -p repark-spark --lib call_procs_route_1`:
  8 passed. Full `call` filter: 175 passed. `repark-iceberg --lib catalog::`:
  102 passed.
- `cargo fmt --all --check` clean; `cargo clippy` on the touched crates with
  the Makefile's flag pair (`-D warnings -A clippy::disallowed_methods`):
  zero errors. (The brief's bare clippy line omits the Makefile's `-A` flag
  and fails on 3669 pre-existing test-`expect`s; the repo-canonical form is
  what CI runs.)
- `.venv/bin/ruff check .` and `ruff format --check` clean on the touched
  files; `check_rust_file_size`, `check_lib_rs`, `check_lib_py`,
  `check_python_conventions`, `check_docstring_presence` clean; comment-ban 0;
  typos clean (new `ANC`/`anc` domain words in `.typos.toml`).
- The orchestrator harness replay (`harness.py --engine repark`) was NOT run:
  round Rule 1 forbids the parity harness on this lane; the committed pytest
  pins plus the live re-derivation cover the same ground and CI runs the rest.

## Residue and fork asks (dated 2026-09-19)

- R-001 (fork ask): `RewriteTablePath` has no incremental version range, so
  `start_version` / `end_version` refuse. Spark answers: END-VERSION rows
  `v3.metadata.json, 2, 0`; START-VERSION rows `v4.metadata.json, 2, 0` with
  per-version file lists. Ask: a fork `RewriteTablePath` (or successor action)
  that rewrites only the metadata versions in `(start, end]`, stages one entry
  per covered version, and reports per-version counts. Until then the two
  cells stay `xfail(strict)`.
- R-002 (observed, not pinned): the fork stages one rewritten metadata file
  (`<uuid>-rewritten.metadata.json`) where Spark stages every covered version
  (`v1…v4.metadata.json`). The file list carries the staged file either way
  and the staged document's location equals the target; no test pins the
  cardinality.
- R-003 (decision): default staging names use wall-clock nanos plus pid
  (`copy-table-staging-<nanos>-<pid>`) instead of a uuid because adding the
  `uuid` dependency to `repark-spark` needs owner approval (round Rule 2).
  Only the `copy-table-staging-` prefix is pinned.
- R-004 (assumption): output columns are non-nullable by the rewrite-family
  precedent; the fixture records no nullability. If live Spark ever shows a
  nullable procedure column, adjust the schema constructors.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-procs-route-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-016 walked one by one against behavior — the 27-cell oracle was measured on live PySpark 4.1.2 + Iceberg 1.11.0, the recorder re-derives it, the facade pins ran red-first on the base tree, and the routing answers were verified against the oracle cell for cell; every clause is PROVEN and cited from the maps.
      artifacts: [task/ledgers/staging/ice-procs-route-1-ledger.md, python/repark/tests/test_ice_procs_route_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary cells actually exercised — empty tables (ancestors refusal, stats zero rows), unknown snapshot id, unknown column, unpartitioned table, wrong prefix, explicit staging, no-file-list, MoR position deletes, branch head, rollback plus commit, reversed columns, struct default, second run on one snapshot, v3 format.
      artifacts: [python/repark/tests/test_ice_procs_route_1.py, crates/repark-spark/src/tests/call_procs_route_1.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every recorded refusal raises IllegalArgumentException with Spark's text (Cannot find snapshot, Can't find column, Table must be partitioned, does-not-start-with); missing/wrong-typed arguments keep AnalysisException; the version range refuses NotImplemented naming the fork gap; the base-tree red was the not-supported refusal on all four procedures.
      artifacts: [python/repark/tests/test_ice_procs_route_1.py, crates/repark-spark/src/tests/call_procs_route_1.rs]
    - id: AT-4
      status: ATTACKED
      evidence: No privileged action, no destructive catalog operation, no secret — rewrite_table_path stages files under the table/staging locations the caller names and commits nothing; the procedure surface is four literal CALL names over a closed match.
      artifacts: [crates/repark-spark/src/call/rewrite_table_path.rs, crates/repark-spark/src/call.rs]
    - id: AT-5
      status: ATTACKED
      evidence: The version-range gap is a loud NotImplemented plus two strict xfails with fork ask R-001, never a narrowed assertion; no divergence absorbed silently — R-002..R-004 are dated observations in this ledger.
      artifacts: [task/ledgers/staging/ice-procs-route-1-ledger.md, python/repark/tests/test_ice_procs_route_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Arrow value AND type pinned per oracle cell on the facade door (snapshot ids resolved to commit-order positions, ndv asserted exactly, partition counts asserted per partition, file lists asserted line by line); the native-door CALL refusal pinned per cell.
      artifacts: [python/repark/tests/test_ice_procs_route_1.py]
    - id: AT-7
      status: N/A
      justification: No performance or memory-path change — routing adds one metadata walk or one fork action per CALL; no hot loop, no cache, no spill surface.
    - id: AT-8
      status: ATTACKED
      evidence: File-size baselines held (new Rust modules under the default ceiling, call.rs grows within it, Python files under 1000 lines); the one new cross-crate helper lives in repark-iceberg because only that crate carries the buffer-type dependency; no Cargo.toml touched.
      artifacts: [scripts/check_rust_file_size.py, scripts/check_lib_py.py, crates/repark-iceberg/src/catalog/files.rs]
    - id: AT-9
      status: N/A
      justification: No log-format or diagnosis-path change; every failure path raises the same typed exception family as before (IllegalArgumentException for procedure validations, AnalysisException for argument shape, UnsupportedOperationException for unknown names and the native door).
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held — the 28-test file failed 25-strong on the base tree with the not-supported refusal and goes 25 passed plus 2 strict xfails after routing; the live re-derivation passes on fresh randomness, so no dead branch ships.
      artifacts: [python/repark/tests/test_ice_procs_route_1.py, python/repark/tests/_record_ice_procs_route_1_oracle.py]
```

Up: [map.md](map.md)
