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
| S2-7 | **Grok lane (2026-09-09 evening, "hit it"):** Grok 4.6 (`grok-worker`, ~82 % weekly quota left) takes two groups as a third lane: Ballista Milestone 1 as actor on the isolated `repark-distributed` crate, and a read-only critic sweep over the units merged since 2026-09-08. Grok launches go through `systemd-run`; every fast hand-back is checked for the turn-1 stall and the fabrication pattern (`num_turns`, `git log origin/main..HEAD`). | §1 BALLISTA-M1-A…D, REVIEW-1; runbook G-4, §7 |
| S2-8 | **NEVEROOM-1 D-2 headroom (2026-09-10, run 5 Q-1):** `RLIMIT_AS` is applied after the session builds as `VmSize_at_apply + 3 × limit`, verified by read-back — the absolute `3 × limit` cannot import pyarrow (measured 2.93 GB of address space). Step 2 runs the full matrix that way. | NEVEROOM-1 D-2 |
| S2-9 | **NEVEROOM-1 refusal family (run 5 Q-2):** `refused` = `MemoryError`, or any `PySparkException`-family exception whose message names one of the row's measured `refusal_names`; everything else folds to `KILLED`, never to `refused`. Confirmed as implemented in #475. | NEVEROOM-1 D-2 |
| S2-10 | **AP-0-R-001 byte model (run 5 §3b-1):** adopted before AP-2. AP-1 step 2 replaces the pre-rewrite byte sum with a ratio measured from the table's own parquet footers (compressed vs uncompressed column-chunk sizes), falling back to the measured 0.55 when footers are unreadable; the `notes` caveat stays until the measured ratio is in the frame. | AP-1 step 2, AP-2 |
| S2-11 | **PROFILES-1 steps 2–3 (run 5 §3b-2):** a dedicated run of their own — release build, no other lane, no JVM — scheduled as its own overnight window, not beside a slate. | PROFILES-1, runbook §7 |
| S2-12 | **`datasets/` vs `fixtures/torture` (run 5 §3b-3):** both trees stay until TORTURE-1 step 5; that step measures which DS-1…DS-3 families the torture generators cover and retires only those, in one M round. | TORTURE-1 step 5 |
| S2-13 | **The `fixtures/` graft (run 5 §3b-4):** accepted as written, with its contract in `python/repark-parity/map.md`; no packaging ruling until a second package needs the spelling. | TORTURE-1 |
| S2-14 | **Citations live in `map.md`, never in code (2026-09-11, run 6 "your call"):** REVIEW-FIX-15's `_pins = "pins: …"` dead-variable strings in three parity tests are the comment ban routed through a variable; the 2026-08-26 adjustment already names the directory's `map.md` as the home of a `pins:` citation. Card **REVIEW-FIX-15b** (M, one Devin round): delete the `_pins` variables, carry the same citations in `python/repark-parity/tests/map.md`, ledger-grammar gate green. | REVIEW-FIX-15b |
| S2-15 | **Tiers after 2026-09-10 (owner):** Muse is METERED (54 % of its period after 115 rounds ≈ two rounds per point; run 6 alone spent 24) — it is the budgeted I-tier, at most one lane, counted in every report. **Devin SWE-2 (free, `devin-worker`) is the default for M rounds and the first try for I rounds**; a misbehaving Devin round (no commits on a build card, fabricated CONCLUDED, launcher error, second failed remediation, `ask_user_question` stall) moves that card and every later one to **Grok** (`grok-worker`, sepmo-actor, stall + fabrication checks), never to Muse for an M round. GLM stays the clerk. R-15 (document rounds to Muse) is suspended until Devin fails a document round. | runbook G-4, every brief |
| S2-16 | **`INSERT INTO` writes uncompressed parquet (run 7, AP-1-R-001):** the fork's task writer ignores the `write.parquet.compression-codec` table property, which is why AP-1's footer-measured ratio still projects 76–88 % high. This is a **fork defect**, not an AP-1 residue to absorb: fork card **F-WRITE-COMPRESS-1** (Devin first, Grok fallback, on the iceberg-rust fork lane: red-first pin that an `INSERT INTO` data file's column chunks carry the table's codec; then RP repin). AP-1-R-001 stays open until the repin lands and the ratio is re-measured. | fork lane, AP-1 |
| S2-17 | **Ballista M2 typed scan rebuild (run 7 decision):** `repark-distributed` takes a direct `iceberg-datafusion` dependency (the fork pin the workspace already carries) so the codec rebuilds `IcebergTableScan` through typed accessors instead of the rebuild-and-compare guard; orchestrator seed under G-5 (dependency-policy row, `cargo build --features cluster` green), then card **BALLISTA-M2-B** (I: the typed rebuild, the string-filter pin that the guard caught, the guard itself retired). | BALLISTA-M2-B |
| S2-18 | **Never-OOM at v1.3 (run 7 measurement):** 24 of 27 cells stable, `hash_join` at 4× KILLED in all three runs, two cells unstable — causes upstream (datafusion#24768, #22758). Per S2-6 the v1.3 text is documentation and pins, so **NEVEROOM-1 step 3 writes the matrix as measured** (the three cells named with their upstream issues and no operator change) and v1.3 cuts on that; a datafusion bump that closes an issue re-runs the matrix as its pin. | NEVEROOM-1 step 3, v1.3 |
| S2-19 | **Compaction and the rewrite paths write uncompressed parquet (run 8, AP-1 re-measure #514):** `#276` fixed only `IcebergWriteExec`; `rewrite_data_files` inflated a 3.07 MB zstd bed to 7.93 MB, and three sibling writer sites build default `WriterProperties`. Fork card **F-WRITE-COMPRESS-2** is OPEN (Devin on the fork lane, Grok fallback; card below); then **RP-17** (its own PR) and AP-1's 20 % check a third time. **Q-1 (`byte_ratio` over the footers' uncompressed sum) is deferred** until that re-measure — ruling it against a polluted actual would tune the model to a defect. | fork lane, RP-17, AP-1 |
| S2-20 | **STATUS sentences for BALLISTA-M2-B, AP-2 and RP-16** ride the v1.4 release PR (they landed after the v1.3.0 cut; the file sits at 22 959 B under its 25 000 B ceiling, so the release rewrite pays for them). No standalone STATUS PR. | v1.4 release PR |
| S2-21 | **Perf review agents (2026-09-02 rule) under the critic-tier rule:** they return as **Grok critic rounds**, read-only, only on a unit branch whose diff touches `crates/` or `python/repark/src/` beyond pins (docs, ledger and example rounds skip them). One Rust reviewer and one Python reviewer per such branch, findings back to the actor before the PR, as REVIEW-1 ran. | every product unit branch |
| S2-22 | **EX-29 Q1 — six `Column.*` engine-plumbing names** (`for_select`, `join_sql_part`, `spark_display_part`, `spark_wrap_display_part`, `sql_expr_part`, `sql_expr_without_alias`), measured absent from `pyspark.sql.Column` on 4.1.2: **the example inventory narrows to drop them** — they are not Spark surface and no honest example can teach them. Card **EX-31** (Devin, one M round, after EX-30 merges so the baselines move once): the enumerator gains an explicit named exclusion list with the measurement as its reason, the six names leave `backlog.txt`, plus **`F.PythonUDFColumn`** (EX-30 Q1, 2026-09-11: the marker class `F.udf(f)("n")` returns, measured absent from `pyspark.sql.functions` and `pyspark.sql.column` on 4.1.2 — same class of name), so seven names; `BACKLOG_BASELINE` 119 → 112 after EX-30, `inventory.txt` regenerated, a pin that the seven are neither public nor on the backlog. | EX-31 |
| S2-23 | **Q-1 ruled (RP-17's third AP-1 re-measure, 2026-09-12, `docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md`):** `plan_partitioning`'s `projected_files_at_target` multiplies the **stored** (already-compressed) file bytes by the footer `byte_ratio`, so it applies compression twice — under one codec it projects 1 156 376 B against a live rewrite of 4 474 081 B (−74 %). The formula changes to **Σ uncompressed footer bytes × byte_ratio** (= the input's compressed bytes, the honest floor for a rewrite of the same rows), which reads −37 % / −32 % on the same beds. Card **AP-3** (one M round, Rust, red-first on the AP-1 beds). The remaining gap is S2-24's, so AP-1-R-001 stays OPEN until both land and the 20 % check runs a fourth time. | AP-3, AP-1 |
| S2-24 | **The rewrite writes 1.5× its input's compressed bytes under the same codec** (RP-17 re-measure: 206 zstd INSERT files, Σ compressed 2 831 692 B → 20 zstd rewrite files, 4 474 081 B; skewed 4 136 632 B). Same rows, same codec, 45–58 % larger — an encoding difference between `IcebergWriteExec` and the maintenance rewrite writer (dictionary encoding, page/row-group shape, statistics, zstd level, or the sort order the INSERT path preserved and the rewrite lost). Fork card **F-REWRITE-SIZE-1** (measure-first on the fork: one bed, both writers, every parquet-level property diffed; then the fix and RP-18). Until it lands the maintenance policy's compaction is a net-size **loss** on zstd tables — say so in the maintenance guide's known-issues line (rides AP-3). | fork lane, RP-18, AP-1 |
| S2-25 | **F-REWRITE-SIZE-1 step 1 measured (fork #279, 2026-09-12): the cause is dead dictionary pages.** On ~22k-row rewrite chunks parquet-rs's dictionary overflows its page limit mid-chunk and the dead ~144 KB dictionary page is still written per high-cardinality column chunk; dictionary off alone → 1.009× (baseline 1.47×). Secondary: the fork's default zstd level is 1 where Java writes 3 (level 3 alone → 1.13×; both → 0.67×). Row order is not the cause. **Step 2 ruled:** per-column dictionary decided from the input files' footers (a column whose input chunks fell back from dictionary writes without one; low-cardinality columns keep it, pinned on an 8-value bed), and the unset-level default becomes zstd 3 to match Java (pinned by bytes). `rewrite_size_pin` un-ignored at ≤ 1.05×. Then RP-18 and AP-1's fourth 20 % check. | fork lane, RP-18, AP-1 |
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

### Card BALLISTA-M1-A — the `repark-distributed` crate and the local executor (Grok lane)

**Ground.** [../epic-term/ballista-audit-2026-09-08.md](../epic-term/ballista-audit-2026-09-08.md)
§26.F–§26.H: depend on `ballista-core`, `ballista` (client), `ballista-scheduler`,
`ballista-executor` at tag `54.1.0` (DataFusion 54 matches the workspace pin); override seats are
the single-slot codec wrapper, `SessionProvider`, `override_config_producer` /
`override_session_builder`; writes and commits stay coordinator-side (ADR-0004 disposition).
Nothing upstream is vendored.

**Home.** NEW `crates/repark-distributed/` (`Cargo.toml`, `src/lib.rs`, `src/executor.rs`,
`src/local.rs`, `src/map.md`), `Cargo.toml` (workspace members + the four pinned deps, seed under
G-5), `repo-manifest.toml` (a `[components.repark-distributed]` row, `layer = "runtime"`,
`status = "delivered"` when the crate lands), `scripts/check_crate_dag.py` (`ROLES` + one
`ALLOWED_EDGES` row `("repark-distributed", "repark-core")` with the reason), `ARCHITECTURE.md`
(one paragraph, the crate DAG figure), `crates/map.md`, docs pointers.

**Decisions.**

- **D-1 Placement.** `repark-distributed` is tier 3, role `runtime`: it depends on `repark-core`
  (the session, the Iceberg provider) and `repark-common`; nothing below tier 3 may depend on
  it; `repark-python` and the planned `repark-server*` crates may. This is a crate-DAG ruling
  the orchestrator records in the ledger; the owner sees it in the PR body.
- **D-2 Feature flag.** Everything Ballista sits behind `features = ["cluster"]` (off by
  default) so the workspace's default build, `make verify` and the wheel are untouched; `local`
  always builds.
- **D-3 The trait**, from the owner's plan §20, in `src/executor.rs`:
  `DistributedExecutor { async fn execute(&self, plan: Arc<dyn ExecutionPlan>) -> Result<JobHandle>;
  async fn status(&self, job: JobId) -> Result<JobStatus>; async fn cancel(&self, job: JobId) -> Result<()> }`
  with `JobHandle { id, stream() -> SendableRecordBatchStream }` and
  `JobStatus { Queued | Running { completed_stages, total_stages } | Completed | Failed(String) | Cancelled }`.
- **D-4 `LocalDataFusionExecutor`** runs the plan on the session's `SessionContext` in-process;
  `status` reports `Completed`/`Failed` after the stream drains; `cancel` aborts the task.
  It is the reference implementation every M1 test runs against first.
- **D-5 Seed commit (G-5):** workspace members gain the crate; `[workspace.dependencies]` gains
  `ballista-core = "=54.1.0"`, `ballista = "=54.1.0"`, `ballista-scheduler = "=54.1.0"`,
  `ballista-executor = "=54.1.0"`; `cargo fetch` must succeed on this box (network is available
  to the orchestrator; workers do not touch the lockfile). If the tag is not on crates.io the
  seed pins the git rev `f4e66525` instead and the ledger says so.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 0 | O (G-5) | The seed: crate skeleton with an empty `lib.rs`, manifest row, DAG rows, workspace deps; `make verify` green; `cargo build -p repark-distributed --features cluster` green. |
| 1 | Grok | `executor.rs` (D-3), `local.rs` (D-4), red-first tests: a `range(1000)` sum through the local executor equals the direct answer; `status` transitions; `cancel` on a long `range` returns within 1 s and the status is `Cancelled`; the `cluster` feature compiles with the four crates linked and no code yet. |
| 2 | Grok | `ARCHITECTURE.md` paragraph + figure row, `crates/map.md`, the crate `map.md` with D-1 and D-2 as its design note, ledger. May chain from step 1. |

**Rounds.** 2 Grok after the seed.

---

### Card BALLISTA-M1-B — the cluster executor: scheduler plus two executors in one process (Grok lane)

**Home.** `crates/repark-distributed/src/cluster.rs` (new), `src/session_provider.rs` (new),
`src/codec.rs` (new), `tests/cluster_two_executors.rs` (new, `#[cfg(feature = "cluster")]`),
maps.

**Decisions.**

- **D-1** `ReparkClusterExecutor` embeds Ballista's standalone shape (audit §26.F: the client
  crate's standalone feature): one scheduler and N executors started in-process on ephemeral
  ports; `N` and the scheduler bind address are constructor arguments; a later unit adds the
  remote form, same trait.
- **D-2** The `SessionProvider` seat is filled by a RePark session builder so every executor
  builds its `SessionState` from the same RePark configuration (analyzer rules, UDFs, the
  Spark-door settings) as the coordinator; the test proves a RePark UDF resolves on an executor.
- **D-3** The codec seat is a delegating wrapper (audit R-1): it forwards the five Ballista
  shuffle node types to the Ballista defaults and registers no RePark write or commit node; a
  round-trip serde test over every default node is the pin that survives an upgrade.
- **D-4** No Iceberg yet: the test data is an in-memory table registered through the session
  provider on every executor.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | Grok | `cluster.rs` + `session_provider.rs` + `codec.rs`; the two-executor test: submit a `SELECT sum(x) FROM t` plan, assert the answer, assert both executors ran at least one task (from the scheduler's job metrics), assert `status` walks Queued → Running → Completed. |
| 2 | Grok | D-2's UDF-on-executor pin, D-3's round-trip pin, the cancel path (a long query cancelled mid-flight: status `Cancelled`, no executor left running a task after 5 s), maps, ledger. |

**Rounds.** 2 Grok.

---

### Card BALLISTA-M1-C — multi-stage queries, shuffle, retry, metrics (Grok lane)

**Home.** `crates/repark-distributed/tests/multi_stage.rs` (new), `src/cluster.rs` (retry and
metrics surface), `docs/design/distributed-m1.md` (new: what M1 delivers, the success list from
the owner's plan §12 with a check per line), maps.

**Decisions.**

- **D-1** Three shapes, each asserted equal to the local executor's answer on the same plan:
  hash aggregate over 4 partitions (two stages), a hash join of two tables (three stages), a
  sort-merge join with `prefer_hash_join = false` (a repartition on both sides).
- **D-2** Retry: kill one executor's task mid-shuffle (Ballista's own fault-injection hook, or
  stopping one executor between stages); the job completes on the remaining executor and the
  status carries the retried stage count.
- **D-3** Metrics: `JobStatus::Completed` carries per-stage rows, bytes shuffled, and wall time,
  read from the scheduler's metrics; the test asserts shuffle bytes > 0 on the two-stage shape.
- **D-4** Shuffle data lands under the session's spill temp dir and is removed when the job
  completes or is cancelled; the test checks the directory.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | Grok | D-1's three shapes against the local answers. |
| 2 | Grok | D-2 retry, D-3 metrics, D-4 cleanup; the design doc with the §12 success list checked line by line, maps, ledger. |

**Rounds.** 2 Grok.

---

### Card BALLISTA-M1-D — Iceberg reads through the executors (Grok lane)

**Home.** `crates/repark-distributed/src/iceberg_provider.rs` (new: the DataFusion-level
provider codec the audit's R-4 records), `tests/iceberg_scan.rs` (new), `docs/design/distributed-m1.md`
(a section), maps.

**Decisions.**

- **D-1** Reads only. The Iceberg `TableProvider` from `repark-core` is serialised as
  `(catalog config, table identifier, snapshot id, projection, filters)` and rebuilt on each
  executor through the session provider; scans distribute by file group. Writes are out of
  scope and the doc says the commit coordinator is a later unit (ADR-0004).
- **D-2** Credentials (audit R-5): the executor resolves the catalog through the same session
  configuration the coordinator used, never ambient authority; the memory catalog is the test
  bed and an S3 leg is a disclosed residue until the cutover credential design lands.
- **D-3** The pin: a memory-catalog Iceberg table with 8 files scanned by two executors answers
  the same `count(*)`, `sum`, and a filtered scan as the local executor, and each executor
  opened at least one file (from the scan metrics).

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | Grok | The provider codec and its round-trip test; the two-executor Iceberg scan pins. |
| 2 | Grok | The design-doc section (what M1 delivers, what the next milestone owns: the runtime abstraction is now in place, Iceberg writes and the commit coordinator are Milestone 3), maps, ledger. |

**Rounds.** 2 Grok. **Blocked by** M1-B.

---

### Card REVIEW-1 — the Grok critic sweep over the merges since 2026-09-08 (Grok lane, read-only)

**Why.** Twenty-plus units merged in 24 hours under orchestrator audits; the SEPMO Critic stage
did not run on them. Grok is the proven critic tier and critics never run on Opus.

**Home.** `/tmp/grok-worker/review1/findings/<unit>.md` (one file per reviewed unit, written by
the critic; the orchestrator copies the ones with findings into
`task/roadmap/mid-term/review-1-findings-<date>.md`), a fix card per confirmed finding appended to
this slate by the orchestrator, maps.

**Decisions.**

- **D-1 Scope**: every PR merged to `main` from #426 through the newest at launch, grouped by
  unit (SQL-DESCRIBE-1, DF-EXPLAIN-1, DISPLAY-POLARS-1, CFG-1, DF-EAGER-1, LEDGER-READING-1,
  PREFLIGHT-PARITY-1, DOCS-LINKS-1, PROFILES-1, BALLISTA-AUDIT-0 as a document).
- **D-2 Roles**: one `critic-quality` and one `critic-logic` round per unit on a **fresh clone
  of `main`** under `--sandbox read-only`; `critic-security` once over CFG-1 (secrets,
  interpolation) and SQL-DESCRIBE-1 (property redaction). Each round reads the unit's ledger and
  its diff (`git log --grep`, `git show`), runs the unit's own pins, and attacks the claims:
  pins that were never red, oracle assertions that pin a divergence as parity, comments in code,
  map lockstep, public names outside the freeze, silent behaviour changes to `show()`/`repr`.
- **D-3 Output contract**: the findings file, not the JSON, carries the report (the grok
  truncation gotcha); the JSON summary is one line per finding with severity and file:line. A
  finding is `CONFIRMED` only with a reproduction command that the orchestrator re-runs.
- **D-4 No edits**: the critic never patches; confirmed findings become fix cards for GLM or
  Muse, and a finding that contradicts an owner ruling is filed as a question, not a fix.

**Steps.** One Grok round per unit and role (about 22 rounds, ~$0.10–0.40 each from the CC-3
history); the orchestrator batches them two at a time between its other lanes, re-runs every
reproduction, and appends the fix cards.

**Done when.** Every unit in D-1 has its quality and logic files, the two security files exist,
every `CONFIRMED` finding has a fix card or a filed question, and the findings document is on
`main`.

---

### Card FACADE-AUDIT-0 — the Rust-backed facade audit (owner, 2026-09-10)

**Release target:** v1.5 (owner ruling 2026-09-10, recorded in the release roadmap's Q&A log), together with FACADE-1…5 and CFG-2 named sources.

**Facts on `2fad813`.** The Python facade is ~52k lines (`core.py` 4,487; ML transformers 2,717;
`session_core.py` 2,305; `functions_expr.py` 2,255; `functions.py` 1,985; `types.py` 1,834;
`ta.py` 1,818; `column.py` 1,589) over **two** pyo3 classes (`PyReparkSession`, `PyDataFrame`,
54 methods). Arrow crosses the boundary as IPC bytes; 23 modules import pyarrow
(`create_dataframe_inference.py` 101 references, `types.py` 43, `core.py` 37); a `Column` is a
Python object that renders SQL text for the engine to re-parse. Three moves to Rust already
happened piecemeal: `dynamicFlatten` (DF1), `collect()` rows (PERF-FACADE-1), column-wise
`createDataFrame` (PERF-FACADE-CDF-1). The target shape is py-polars': Rust owns DataFrame,
Expr, Series-equivalents and DataType; Python is a docstring-and-typing wrapper; pyarrow is
optional; Arrow crosses zero-copy through the C Data Interface at the edges.

**Home.** `task/roadmap/epic-term/facade-audit-<date>.md` (new, a READING ledger), maps.

**Decisions.**

- **D-1 Two halves.** Half A (M, GLM): per module under `python/repark/src/repark/`, a row with
  lines, pyo3 calls, pyarrow references, and a class `delegate` (argument checks + one binding
  call), `logic` (work in Python), or `pyarrow` (transforms Arrow tables in Python), from grep
  and a short AST walk; the IPC crossing sites; every place a `Column` renders SQL. Half B
  (I, Muse): weigh the `logic`/`pyarrow` modules by the perf report's measured walls
  (`docs/perf/`, PERF-ANALYSIS-1) and the API freeze (`docs/design/v1-0-api-freeze.json`: names
  never change; `isinstance` on `Column`, `Row`, the type classes must keep working), then
  write the sequence with a pin list per unit.
- **D-2 The candidate sequence the audit confirms or reorders:** FACADE-1 the Arrow C Stream
  boundary (`__arrow_c_stream__` capsules both ways, pyarrow optional, pinned against pyarrow,
  polars and pandas consumers); FACADE-2 `Column` as a pyo3 class over a DataFusion `Expr`
  with Spark display strings rendered in Rust, the two functions modules thinned to builders;
  FACADE-3 `createDataFrame` inference in Rust (rows/tuples/dicts to Arrow with Spark's
  inference rules); FACADE-4 type conversions (Arrow schema ↔ Spark types ↔ DDL) in Rust with
  the Python classes kept; FACADE-5 the display renderer in Rust.
- **D-3 Out of scope for the sequence:** ML transformers and the library-wrapping parts of
  `ta.py`; any public name; pickling behaviour changes without a pin.

**Steps.** 1 (M) Half A tables; 2 (I) Half B judgement + the sequence; 3 (O) map lockstep, PR.

**Rounds.** 2 (M, I).

---

### Card F-WRITE-COMPRESS-2 — the rewrite, delete and audit writers carry the table's codec (fork lane, S2-19)

**Why.** iceberg-rust `#276` (F-WRITE-COMPRESS-1) fixed `IcebergWriteExec` only. Four production writer
sites still build default `WriterProperties` (UNCOMPRESSED), and one of them is measured inflating a real
table 2.6× — run 8's AP-1 re-measure ([#514](https://github.com/TRO-Wolf/repark/pull/514)) compacted a
3.07 MB zstd bed into 7.93 MB through `rewrite_data_files`.

**Home (fork).** `crates/iceberg/src/maintenance/rewrite_data_files_write.rs` (R-002),
`crates/integrations/datafusion/src/physical_plan/row_lineage.rs` (R-001, COW/MoR rewrite data files),
`crates/iceberg/src/maintenance/partition_key_audit.rs` (R-003),
`crates/iceberg/src/writer/base_writer/position_delete_writer.rs` (R-004, the shared
`position_delete_writer_properties` helper — its callers reuse it, so check each), their `map.md` files,
`task/f-write-compress-2-ledger.md`.

**Decisions.** D-1 each site takes the table's codec through the existing
`parquet_compression_from_properties` helper `#276` added — no second parser. D-2 the position-delete helper
keeps its `statistics_truncate_length` behaviour and gains the codec (RePark's own
`position_delete_writer_properties_for` is the reference shape). D-3 red first: a compaction pin that a
`rewrite_data_files` output file's column chunks carry the table's codec, and the same for the MoR/COW rewrite
path and a position-delete file. D-4 no behaviour change other than the codec.

**Consumer.** RP-17 (a pin bump, its own PR), then AP-1's 20 % check a third time — with compaction
compressing, the projection and the actual are finally measured under one codec, which is the state Q-1
(S2-19) is ruled in.

**Steps.** 1 (I, fork lane). **Rounds.** 1 (I); RP-17 is one M round on the engine.

---

### Card EX-31 — the example inventory drops the seven plumbing names (S2-22)

**Why.** EX-29 measured the six `Column.*` names absent from `pyspark.sql.Column` (`inspect.getattr_static` finds no
member; `hasattr` answers True only through `Column.__getattr__` item fabrication). On repark they are bound
plumbing methods. They sat on the example backlog since EX-1 widened the inventory to class surfaces.

**Home.** `scripts/check_example_coverage.py` (the enumerator: a named exclusion list with the measured reason
per name, the `BACKLOG_BASELINE` move), `docs/examples/backlog.txt`, `docs/examples/inventory.txt`
(regenerated by whatever produced it — read the script), `python/repark-parity/tests/test_ex_0_example_coverage.py`
(a pin that the six are neither public in the inventory nor on the backlog), lockstep `map.md`, ledger
`task/ledgers/staging/ex-31-inventory-plumbing-ledger.md`.

EX-30 (Q1) added `F.PythonUDFColumn`, the marker class `F.udf(f)("n")` returns, absent from `pyspark.sql.functions` — seven names in all.

**Decisions.** D-1 the exclusion is explicit and named (never a pattern that could swallow a real surface),
with the EX-29 measurement quoted beside it. D-2 the backlog ratchet stays a ratchet: the baseline moves down by
exactly six on top of whatever EX-30 left. D-3 no product file changes; the six methods stay callable.

**Steps.** 1 (M), after EX-30 merges. **Rounds.** 1 (M).

---

### Card PERF-DESCRIBE-1 — one aggregate pass for `describe` / `summary` (from the S2-21 reviewer, 2026-09-11)

**Why.** The first Grok perf review under S2-21 (DF-DESCRIBE-STR-1 branch) measured that `describe()` runs
one full scan per statistic — mean, stddev, min and max are four `DataSourceExec` passes over the frame
(count folds to a placeholder), about 80 ms of a 110 ms collect on 200 k rows. Pre-existing, unchanged by
that unit; the dominant cost of the surface.

**Home.** `python/repark/src/repark/spark/dataframe/statistics.py`, its pins, the dataframe `map.md`,
ledger `task/ledgers/staging/perf-describe-1-ledger.md`. No engine change.

**Decisions.** D-1 one `AggregateExec` computes count / avg / stddev / min / max per column in a single pass,
then five literal summary rows are projected (an unpivot in SQL), the row order carried by construction so
the DF-DESCRIBE-STR-1 ordinal wrapper can go. D-2 measured before and after on the reviewer's harness
(200 k numeric, 50 × 10 k wide, string-bearing frame), three repetitions, medians in the ledger; the unit
lands only if every shape is faster. D-3 every describe / summary pin stays green; Spark's answers are the
spec (the DF-DESCRIBE-STR-1 oracle table).

**Steps.** 1 (I). **Rounds.** 1 (I), after DF-DESCRIBE-STR-1 merges.

---

### Card AP-3 — `projected_files_at_target` multiplies the uncompressed footer sum (S2-23)

**Why.** Three AP-1 measurements agree: the projection multiplies the stored (compressed) file bytes
by the footer `byte_ratio`, compressing twice. RP-17's re-measure under one codec reads −74 % / −72 %
against the live rewrite; the corrected form reads −37 % / −32 % (the remainder is S2-24's).

**Home.** `crates/repark-spark/src/call/plan_partitioning_bytes.rs` (the sums the footers give),
`crates/repark-spark/src/call/plan_partitioning_score.rs` (`accumulate`: `amount` becomes the
uncompressed sum per item), `plan_partitioning.rs` (the residue note and the frame's `notes` spelling
`byte_ratio=… (footers)`), the AP-0/AP-1 pins, the maintenance guide's known-issues line (S2-24),
ledger `task/ledgers/staging/ap-3-ledger.md`.

**Decisions.** D-1 `projected_files_at_target` = ceil(Σ_uncompressed(item) × byte_ratio / target) per
item, where Σ_uncompressed is the footers' `total_uncompressed_size` sum; the fallback (unreadable
footer) keeps its documented ratio but multiplies the uncompressed estimate `stored / ratio`. D-2 red
first: the AP-1 beds' pins move from the stored-bytes projection to the uncompressed one with the
RP-17 numbers as evidence (1 156 376 → 2 831 692 on uniform/skewed); every other plan pin stays green.
D-3 `projected_partitions` is untouched (measured exact). D-4 no fork change.

**Steps.** 1 (M). **Rounds.** 1 (M).

---

### Card F-REWRITE-SIZE-1 — why the rewrite's output is 1.5× its input's compressed bytes (fork lane, S2-24)

**Why.** RP-17's re-measure: 206 zstd `INSERT` files (Σ compressed 2 831 692 B, Σ uncompressed
7 529 566 B) → `rewrite_data_files` → 20 zstd files, 4 474 081 B (uniform) / 4 136 632 B (skewed).
Same rows, same codec. Compaction on a zstd table is a net-size loss until this is understood.

**Home (fork).** Measure first: `crates/iceberg/src/maintenance/rewrite_data_files_write.rs` and
`crates/integrations/datafusion/src/physical_plan/write.rs` (`IcebergWriteExec`) — diff every parquet
writer property both paths end up with (`WriterProperties`: dictionary enabled/page size,
`data_page_size_limit`, `max_row_group_size`, `write_batch_size`, statistics, zstd level, encodings
per column) and the row order each writes (the INSERT path preserves the partition-hash order; a
rewrite that concatenates files in manifest order may destroy run-length and dictionary locality).
Then the fix at the site the measurement names; ledger `task/f-rewrite-size-1-ledger.md`.

**Decisions.** D-1 step 1 is a measurement ledger (one bed, both writers, a table of properties and
per-column encodings read back from the footers with `parquet::file::reader`, plus the output size
with each candidate property flipped one at a time). D-2 the fix is whatever single change brings the
rewrite within 10 % of the input's compressed bytes on that bed, pinned; if it is a sort, it must be
the table's sort order or the input's observed order, never an invented one. D-3 RP-18 consumes it;
AP-1's 20 % check runs a fourth time.

**Steps.** 1 (I, measure — done, fork #279, S2-25 names the cause); 2 (I, fix — per-column dictionary from the input footers + zstd default level 3, S2-25). **Rounds.** 2.

---

## 2. Sequence

| # | Unit | Depends on | Tiers | Rounds |
|---|---|---|---|---|
| 0 | Slate-1 leftovers (S2-3) | — | as carded | as carded |
| 1 | MAINT-POLICY-1 | CFG-1 step 1 (merged) | I, I, I, M | 4 |
| 2 | TORTURE-1 | — | M, M, I, I, M | 5 |
| 3 | NEVEROOM-1 | — (box alone for step 2) | M, I, M | 3 |
| 4 | AP-1 | AP-0 merged | I, M | 2 |
| 5 | BALLISTA-M1-A → D | seed (G-5); D after B | Grok | 8 |
| 6 | REVIEW-1 | — (read-only) | Grok critic | ~22 short |
| 7 | FACADE-AUDIT-0 | — (reading unit) | M, I | 2 |

The Grok lane (5, then 6 interleaved) runs as a **third** lane: M1 builds only its own crate behind a feature flag, REVIEW-1 builds nothing. Lanes 1 and 2 run together (2's first two steps are GLM and build natives once; 1's steps are
Muse on Rust). NEVEROOM-1 step 2 runs alone on the box. AP-1 opens when AP-0 is on `main`.

## Pointers
- Up: [map.md](map.md) · Slate 1 and the process: [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) · The procedure: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
- The ruled design: [../epic-term/release-roadmap-2026-08-29.md](../epic-term/release-roadmap-2026-08-29.md), [../epic-term/roadmap-design-plan-2026-08-29.md](../epic-term/roadmap-design-plan-2026-08-29.md)
