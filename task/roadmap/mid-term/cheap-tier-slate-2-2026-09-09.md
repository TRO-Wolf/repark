# Cheap-tier slate 2 — maintenance policy and hardening, cut for mechanical-tier workers

**Date:** 2026-09-09 · **Author:** Claude Fable 5.1 (orchestrator) · **Base read:** `origin/main`
`44774d9b` · **Status:** owner-chartered in conversation 2026-09-09 ("hit up 1 and 2, with an Opus
orchestrator, run Spark 1.3 Max agents on contributor"); each card earns its ledger under
`task/ledgers/staging/` when it starts. The process, preamble, tier legend and gates are
[cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) §1 and the
[overnight runbook](overnight-orchestrator-runbook-2026-09-08.md); this file adds only cards.

**What this is.** Roadmap items pulled forward by the owner on 2026-09-09: **2.1 maintenance
policy** with the adaptive-partitioning epic as its analysis engine, and the two hardening items
**1.2 torture-test dataset suite** and **1.3 Never-OOM truth**. The ruled design lives in
[../epic-term/release-roadmap-2026-08-29.md](../epic-term/release-roadmap-2026-08-29.md) (rows
v1.2, v1.3, 2.1) and [../epic-term/roadmap-design-plan-2026-08-29.md](../epic-term/roadmap-design-plan-2026-08-29.md)
(v0.8, v0.9); the cards below split that design into rounds and pre-make the decisions those
sections left open.

## 0. Rulings folded in (2026-09-09)

| # | Ruling | Where it binds |
|---|---|---|
| S2-1 | Slate 2 = roadmap 2.1 (with ADAPT-PART underneath) + 1.2 + 1.3; Ballista Milestone 1 waits for a separate go. | §2, §3 |
| S2-2 | Muse rounds run `muse-spark-1.3-contributor` at `--effort max` (slate 1 R-17); the orchestrator is Opus at medium. | every I step |
| S2-3 | Slate-1 leftovers (DF-EAGER-1 steps 2–3, CFG-1 steps 3–4, DISPLAY-BRIDGE-1, PROFILES-1 steps 1–3, CONF-UNREAD-1, AP-0) finish first, one lane, before slate 2 opens its second lane. | runbook §7 |
| S2-4 | No scheduler in this slate: `CALL run_maintenance()` is the only trigger; the server (roadmap 1.12) adds the schedule later. | MAINT-POLICY-1 D-5 |
| S2-5 | Torture data is generated, never committed: the CI tier is 10k rows per family, the full tier ≥ 1M rows on this box only. | TORTURE-1 D-1 |
| S2-6 | Never-OOM is documentation and pins, no operator change (the ruled v1.3 text); a cell that cannot spill names its upstream issue and stops there. | NEVEROOM-1 |

## 1. Cards

---

### Card MAINT-POLICY-1 — `[<profile>.maintenance]` and `CALL run_maintenance()` (roadmap 2.1)

**Facts on `44774d9b`.** The five procedures exist and route through `crates/repark-spark/src/call.rs`
(`expire_snapshots(older_than, retain_last, max_concurrent_deletes)`, `rewrite_data_files(table,
options, where)`, `rewrite_manifests`, `remove_orphan_files(older_than, location, dry_run,
max_concurrent_deletes)`, `rewrite_position_delete_files`); each answers Spark's result frame.
`repark.toml` loads through `crates/repark-core/src/config_file.rs` (`ConfigFile { profiles }`,
`Profile { display, session, conf, catalog, database }`, all `toml::Table`s) with
`config_file/{discovery,profile,interpolate,redact,sources}.rs`; CFG-1 steps 3–4 (builder wiring,
docs) may still be landing when this card starts — step 1 here does not depend on them.

**Home.** `crates/repark-core/src/config_file.rs` (`maintenance: Option<toml::Table>` on
`Profile`), `crates/repark-core/src/config_file/maintenance.rs` (new: the typed policy),
`crates/repark-spark/src/call.rs` (the sixth procedure name), `crates/repark-spark/src/call/run_maintenance.rs`
(new), `crates/repark-spark/src/tests/run_maintenance.rs` (new), Python facade
`python/repark/src/repark/spark/session/session_core.py` (`run_maintenance`),
`python/repark/tests/test_maintenance_policy_1.py` (new), `docs/guide/maintenance-policy.md` (new),
`docs/guide/repark-toml.md` (a section), maps.

**Decisions.**

- **D-1 The TOML shape**, profile-scoped like every other table, with per-table overrides:
  ```toml
  [default.maintenance]
  target_file_size_bytes = 536870912
  snapshot_retain_last = 5
  snapshot_older_than = "7d"
  orphan_older_than = "3d"
  rewrite_manifests = true
  position_delete_ratio = 0.3

  [default.maintenance.tables."glue.silver.orders"]
  target_file_size_bytes = 268435456
  snapshot_retain_last = 20
  ```
  Unknown keys refuse loud with the key path; every key is optional; a profile with no
  `maintenance` table means "no policy".
- **D-2 Durations** are strings `"<n>d" | "<n>h" | "<n>m"` parsed by one function in
  `maintenance.rs`; anything else refuses loud naming the key. `older_than` arguments are computed
  at run time as now minus the duration.
- **D-3 The procedure.** `CALL <catalog>.system.run_maintenance(table => 'db.t' [, dry_run =>
  true|false] [, <any D-1 key> => value])`. `dry_run` defaults to **true**; inline keys override
  the file's per-table and profile values in that order. The result frame has one row per step:
  `step` (int), `procedure` (string), `arguments` (string, the CALL as it would be issued),
  `status` (`planned` on a dry run; `ran` | `failed` | `skipped` on apply), `result` (string, the
  procedure's own result frame rendered as JSON, or the error text).
- **D-4 Step order and gating.** 1 `rewrite_position_delete_files` only when the table's delete
  ratio (delete files bytes over data bytes from the `files` and `delete_files` metadata tables)
  is at or above `position_delete_ratio`; 2 `rewrite_data_files` with `target-file-size-bytes`
  from the policy and the existing binpack defaults; 3 `rewrite_manifests` when
  `rewrite_manifests = true`; 4 `expire_snapshots(older_than, retain_last)`; 5
  `remove_orphan_files(older_than)`. Each step is its own commit. A failing step stops the chain;
  its row is `failed` with the error text and the rows after it are `skipped`.
- **D-5 No scheduler** (S2-4). The Python wrapper is
  `session.run_maintenance(table, dry_run=True, **overrides) -> DataFrame`, a thin call to the
  SQL form.
- **D-6 No policy, no inline keys** refuses loud: `run_maintenance: no [<profile>.maintenance]
  table and no inline keys for <table>`.
- **D-7 Extension point for ADAPT-PART.** The step list is built by one function
  `plan_steps(policy, table_stats) -> Vec<Step>`; AP-2 later inserts the partition-spec steps
  before step 2 when a policy key `adaptive_partitioning = true` is present. This card rejects
  that key with "not yet supported" so the name is reserved.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | I (Muse) | `maintenance.rs`: typed `MaintenancePolicy` + `TablePolicy`, the duration parser, per-table override resolution, unknown-key refusals; red-first unit tests in `config_file/tests.rs`; the `maintenance` field on `Profile`; no procedure yet. `make verify`. |
| 2 | I (Muse) | The procedure name, argument parsing (D-3), `plan_steps` over the metadata-table statistics (D-4's gate), the dry-run frame; Rust tests on a memory-catalog table with 20 small files and 3 snapshots: the planned rows, the delete-ratio gate both sides, the override order file → table → inline. |
| 3 | I (Muse) | Apply path: each step issues the existing procedure through the same router entry the CALL door uses; `ran` / `failed` / `skipped` semantics; pins: file count drops to the binpack expectation, snapshots expire to `retain_last`, a stray file placed under the table location is removed, a forced failure in step 2 (a bad option) yields `failed` + `skipped` rows and leaves the table readable. |
| 4 | M (GLM) | Python `run_maintenance` wrapper + facade pins (dry run default, override kwargs, the D-6 refusal by class), `docs/guide/maintenance-policy.md` with the D-1 example and the result frame, the `repark-toml.md` section, maps, ledger. |

**Pins.** `policy_parses_all_keys`, `policy_unknown_key_refuses`, `duration_parses_d_h_m`,
`duration_bad_refuses`, `table_override_wins_over_profile`, `inline_wins_over_table`,
`dry_run_default_true`, `delete_ratio_gate_below_skips_step_1`, `delete_ratio_gate_at_runs_step_1`,
`apply_binpacks_to_expected_count`, `apply_expires_to_retain_last`, `apply_removes_orphan`,
`failed_step_stops_chain`, `no_policy_refuses`, `adaptive_partitioning_key_reserved`,
`test_session_run_maintenance_frame_shape`.

**Done when.** A table with a policy in `repark.toml` is maintained end to end by one CALL, the
dry run prints the plan first, and every pin is green with `make verify` and the facade file.

**Hand back when.** The metadata tables do not expose delete-file bytes (then the gate needs a
different statistic — say which); a procedure cannot be invoked from Rust without going through
SQL text (then step 3 issues SQL text through the router and says so in the ledger).

**Rounds.** 4 (I, I, I, M).

---

### Card AP-1 — `CALL plan_partitioning()` (ADAPT-PART, after AP-0 merges)

The epic's binding rules P-1…P-5 are in slate 1 §3. This card cuts AP-1 only; AP-2 (apply) and
AP-3 (the `adaptive_partitioning` hook into MAINT-POLICY-1 D-7) follow once AP-1's plans have
been read against three real tables.

**Home.** `crates/repark-spark/src/call/plan_partitioning.rs` (new), `call.rs` (the name),
`crates/repark-spark/src/tests/plan_partitioning.rs`, the AP-0 script's statistics module (reuse
its SQL over the `files` metadata table), `docs/guide/maintenance-policy.md` (a section), maps.

**Decisions.**

- **D-1** `CALL <catalog>.system.plan_partitioning(table => 'db.t', target_file_size_bytes =>
  n)` returns one row per candidate spec, best first: `candidate` (the spec as Spark DDL,
  `days(ts)` / `bucket(16, id)` / `identity(region)` / `unpartitioned`), `score` (P-3),
  `projected_partitions`, `projected_files_at_target`, `ddl` (the ALTER statements), `calls`
  (the maintenance CALLs that would follow), `plan_id` (a hash of table snapshot id + candidate).
- **D-2** Statistics come from AP-0's SQL over `t.files` (`readable_metrics` bounds, record
  counts, sizes); no data scan in this card; a column without bounds is not a candidate and the
  result frame's last row says so in a `notes` column.
- **D-3** Candidate generation is exactly P-2; scoring exactly P-3; the penalties are constants
  in one place with their values in the map.md.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | I (Muse) | The procedure, candidates, scoring, the frame; Rust tests on a synthetic memory-catalog table whose bounds make the winner unambiguous (a `ts` column spanning 90 days at ~target/day → `days(ts)` first). |
| 2 | M (GLM) | Run it on the three AP-0 tables (release build), paste the top three rows per table into `docs/perf/adapt-part-ap1-<date>.md` beside AP-0's numbers, the guide section, maps, ledger. |

**Rounds.** 2 (I, M). **Blocked by** AP-0's PR merging.

---

### Card TORTURE-1 — the torture-test dataset suite (roadmap 1.2)

**Home.** `python/repark-parity/fixtures/torture/` (generators as a package
`repark_parity.torture`, seeded, data never committed), `python/repark-parity/tests/torture/`
(the suite), `crates/repark-core/src/read_options.rs` (the secrets flag, step 3), the Python read
option plumbing beside the existing read options, `Makefile` (`py-test-torture`, tiered; **not**
in `preflight`), `docs/perf/torture-1-<date>.md` (full-tier results), `docs/testing.md` (one
paragraph), maps.

**Decisions.**

- **D-1 Tiers and generation.** `python -m repark_parity.torture generate <family> --rows N
  --seed 7 --out DIR` writes Parquet and CSV per family. `TORTURE_TIER=ci` (default) generates
  10k rows into a temp dir at test time; `TORTURE_TIER=full` generates ≥ 1M rows once under
  `/tmp/torture/` and reuses them. Generators are deterministic for a seed.
- **D-2 Families**, one module each, with the exact shapes the roadmap lists: `nested`
  (deep struct/list nesting, mixed element types, lists of structs, capitalised field names,
  null-typed lists); `inference` (int32 → int64 at row 500k in full tier, row 5k in CI;
  string-vs-float halves; bool-looking ints; date-looking strings); `extreme_types`
  (high-precision decimals, UUIDs, paragraph strings, embedded HTML); `smartcsv` (header
  normalisation, blank cells, currency and decimal widths, bool spellings); `secrets` (credential-
  shaped column names carrying fake plaintext, names from `prop_key_is_secret`'s needle set);
  `temporal` (Duration at the ±i64-microsecond bound and the whole-day edge 106751991,
  MonthDayNano mixed and zero-unit, DATE ± interval promotion boundaries, epoch extremes);
  `decimal_overflow` (`decimal(38)` sums and averages that overflow at the aggregate boundary);
  `v3_dv` (many-file format-v3 tables with live Puffin deletion vectors, step 4).
- **D-3 What the suite asserts**, both doors (DataFrame read and `spark.sql` over a temp view),
  per family: the read completes or refuses loud (no panic, no silent truncation), the row count
  equals the generated count, the schema equals the generator's declared expectation, and for
  `inference` the inferred type per column equals the expected one. A cell that fails is **not**
  an xfail: the worker files a registry row in `docs/spark-sql-iceberg-parity.md` with the
  reproduction command and marks the test `xfail(strict=True, reason="<row id>")`.
- **D-4 The secrets flag** (step 3): read option `flag_secret_columns = "off" | "warn" | "refuse"`,
  default `off`, exact-name match only against the existing needle set; `warn` logs one WARNING
  naming the columns; `refuse` raises `AnalysisException` naming them. Data columns only; config
  secrets are CFG-1's redaction.
- **D-5 `v3_dv`** needs live Spark to write the deletion vectors: the generator runs only under
  `REPARK_PARITY_LIVE=1` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` and the JVM alone on the
  box; CI keeps one checked-in fixture under 1 MB (the only committed data in this card, ruled
  by the roadmap's own text).
- **D-6 Measurement bed.** The `nested` family is the bed roadmap v1.6's `dynamicFlatten` work
  will measure on; its generator takes `--depth` and `--width` so the perf units can scale it.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M (GLM) | The package skeleton (`generate` CLI, seed, tiers, one `Family` protocol), the `nested` and `inference` families, the suite skeleton running both doors at CI tier, the Makefile target, maps. Red-first: the suite fails before the families exist. |
| 2 | M (GLM) | `extreme_types`, `smartcsv`, `temporal`, `decimal_overflow` families and their suite cells; any failing cell filed per D-3. |
| 3 | I (Muse) | The secrets family and the D-4 flag in `read_options.rs` + the Python read option; pins for `off`/`warn`/`refuse` on both doors. |
| 4 | I (Muse, JVM alone) | `v3_dv` generator under live Spark, the ≤ 1 MB CI fixture, the cells (true result counts under DVs on both doors). |
| 5 | M (GLM) | Full tier on this box (release build): the results table in `docs/perf/torture-1-<date>.md`, one row per family × door with rows, seconds, outcome; the `docs/testing.md` paragraph; ledger. |

**Pins.** One suite file per family (`test_torture_<family>.py`) plus
`test_secret_flag_off_warn_refuse`, `test_generate_is_deterministic`, `test_ci_tier_under_60s`.

**Done when.** Eight families generate and read on both doors at CI tier under a minute, the
full tier has a dated results table, every failing cell has a registry row, and the secrets flag
is pinned on both doors.

**Hand back when.** A family needs a reader feature that does not exist (record the gap as a
registry row and continue; the ruled card says do not build the reader here); the needle set
lives somewhere other than `catalog_config.rs` / `_secrets.py`.

**Rounds.** 5 (M, M, I, I, M).

---

### Card NEVEROOM-1 — the spill-coverage matrix (roadmap 1.3)

**Facts on `44774d9b`.** `repark-core/src/session/spill.rs` holds the fair spill pool, the temp
dir and the `SET` path; `repark.memory.limit.gb` is the facade key; H3-SPILL-1 and its residue
unit (ledgers in `task/ledgers/staging/h3-spill-*`) measured sliding windows, the nested-loop
join refusal and `collect()` under an address-space limit, and their harness is the starting
point.

**Home.** `python/repark-parity/tests/spill/` (the matrix harness and the CI-tier golden),
`docs/perf/spill-coverage-matrix-<date>.md` and `docs/perf/spill-coverage-matrix-<date>.csv`,
`PROJECT.md` (one pointer line; the orchestrator writes it at departure, not the worker),
`Makefile` (`py-test-spill-matrix`, CI tier only), maps.

**Decisions.**

- **D-1 Rows and columns.** Operators: `sort`, `hash_aggregate`, `hash_join`,
  `sort_merge_join`, `window_sliding` (the W-3 row), `window_unbounded`, `dynamic_flatten`,
  `nested_loop_join`, `collect`. Input sizes: 2×, 4×, 8× the memory limit. The limit is 1 GB
  (`repark.memory.limit.gb = 1`) and inputs are generated in-engine from `range()` with a
  string payload column sized so the Arrow bytes hit the target; no files.
- **D-2 Three outcomes per cell**, and only three: `spilled` (completed, spill bytes > 0 from the
  runtime metrics), `completed` (completed, no spill), `refused` (a loud `MemoryError` /
  `AnalysisException` naming the operator). Each cell runs in a **subprocess** under
  `RLIMIT_AS = 3 × limit` so a silent OOM kill is observable; a killed or non-zero-exit
  subprocess without a refusal is `KILLED`, which fails the matrix.
- **D-3 Release build and an idle box**: `maturin develop --release`, no other lane, `pgrep -x
  cargo` empty, `pgrep -f java` empty; three repetitions, the outcome must agree across all three
  or the cell is `UNSTABLE` and fails.
- **D-4 Cannot-spill cells** name the upstream DataFusion issue URL in the doc's notes column and
  stop there (S2-6); no operator change in this card.
- **D-5 CI tier.** `py-test-spill-matrix` runs `sort`, `hash_aggregate`, `hash_join` at 2× with a
  64 MB limit and compares outcomes to the golden CSV; the full matrix is this box only.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M (GLM) | The harness: subprocess runner with `RLIMIT_AS`, in-engine generators, the spill-bytes probe from the runtime metrics, one cell (`sort` at 2×) end to end at CI tier, the Makefile target; red-first on the outcome classifier. |
| 2 | I (Muse) | The full matrix (D-1 × D-3) on this box, the CSV and the document with one row per cell and the D-4 notes; the H3-SPILL rows (`nested_loop_join`, `collect`) must reproduce their FIXED outcomes. |
| 3 | M (GLM) | The CI golden and pins, `docs/testing.md` pointer, ledger; hand the PROJECT.md pointer line to the orchestrator. |

**Pins.** `test_classifier_three_outcomes_only`, `test_killed_subprocess_fails_matrix`,
`test_ci_tier_matches_golden`, `test_full_matrix_csv_has_27_cells` (full tier, skipped in CI).

**Done when.** Every one of the 27 cells is one of the three states in the dated document, no
`KILLED` or `UNSTABLE` cell remains, each cannot-spill cell names its upstream issue, and the CI
tier guards the three cheap cells.

**Hand back when.** A cell needs an operator change (the ruled text: that is W-3 or upstream);
the runtime metrics expose no spill-bytes counter (then the probe watches the spill temp dir and
the ledger says so).

**Rounds.** 3 (M, I, M).

## 2. Sequence

| # | Unit | Depends on | Tiers | Rounds |
|---|---|---|---|---|
| 0 | Slate-1 leftovers (S2-3) | — | as carded | as carded |
| 1 | MAINT-POLICY-1 | CFG-1 step 1 (merged) | I, I, I, M | 4 |
| 2 | TORTURE-1 | — | M, M, I, I, M | 5 |
| 3 | NEVEROOM-1 | — (box alone for step 2) | M, I, M | 3 |
| 4 | AP-1 | AP-0 merged | I, M | 2 |

Lanes 1 and 2 run together (2's first two steps are GLM and build natives once; 1's steps are
Muse on Rust). NEVEROOM-1 step 2 runs alone on the box. AP-1 opens when AP-0 is on `main`.

## Pointers
- Up: [map.md](map.md) · Slate 1 and the process: [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) · The procedure: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
- The ruled design: [../epic-term/release-roadmap-2026-08-29.md](../epic-term/release-roadmap-2026-08-29.md), [../epic-term/roadmap-design-plan-2026-08-29.md](../epic-term/roadmap-design-plan-2026-08-29.md)
