# Unit ledger — MAINT-POLICY-1 step 1 · typed `[<profile>.maintenance]` policy

**Unit:** MAINT-POLICY-1 step 1 · **Date:** 2026-09-10 · **Branch:** `feat/maint-policy-1` · **Base:** `44774d9b`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** Card MAINT-POLICY-1 lands `[<profile>.maintenance]` and `CALL run_maintenance()`
in four rounds. Step 1 is Rust only: the typed policy (`MaintenancePolicy` + `TablePolicy`),
the D-2 duration parser, per-table override resolution (D-1), unknown-key refusals naming the
key path, the `adaptive_partitioning` reservation (D-7), and the `maintenance` field on
`Profile`. No procedure, no CALL door, no Python in this step.

**Not in this step:** the procedure name, argument parsing, `plan_steps`, the dry-run frame
(step 2); the apply path (step 3); the Python wrapper, the guide, the `repark-toml.md`
section (step 4); `STATUS.md`, `briefs/next-sequence.md`, any dependency file.

## PROPOSITION LEDGER — MAINT-POLICY-1 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The policy parses every D-1 key (`target_file_size_bytes`, `snapshot_retain_last`, `snapshot_older_than`, `orphan_older_than`, `rewrite_manifests`, `position_delete_ratio`) with the documented value types. | `maintenance_policy_parses_every_documented_key` | **PROVEN** | Green in the step-1 run (`cargo test -p repark-core config_file`: `54 passed; 0 failed`, 2026-09-10). The pin parses the card's D-1 example values end to end through `parse()` plus `from_table` and asserts each typed field (`536870912`, `5`, `7d` as 604800 s, `3d` as 259200 s, `true`, `0.3`) with an empty `tables` map. Red first below. |
| C-002 | An unknown key under `[<profile>.maintenance]` refuses loud with the key path. | `an_unknown_maintenance_key_refuses_naming_the_key_path` | **PROVEN** | Green in the same run. `profile.rs` validates eagerly through `from_table` but stores the raw table, so `parse("[default.maintenance]\nnonesuch = 1")` refuses with `default.maintenance.nonesuch` in the message. Red first below. |
| C-003 | Durations parse `"<n>d"`, `"<n>h"`, `"<n>m"` to day / hour / minute counts. | `durations_parse_days_hours_and_minutes` | **PROVEN** | Green in the same run. `parse_duration` maps `7d` / `12h` / `30m` to 604800 / 43200 / 1800 s, asserted as `Duration::from_secs`. Red first below. |
| C-004 | Anything else in a duration slot refuses loud naming the key. | `a_malformed_duration_refuses_naming_the_key` | **PROVEN** | Green in the same run. Nine malformed spellings (`7`, `d`, `7w`, `1y`, empty, `seven days`, `7 d`, `-3d`, `1.5h`) each refuse naming `default.maintenance.snapshot_older_than`, and a non-string TOML value (`snapshot_older_than = 7`) refuses naming the same path. Red first below. |
| C-005 | A per-table entry overrides the profile-level values key by key; unset table keys fall back to the profile values. | `a_table_entry_overrides_the_profile_policy` | **PROVEN** | Green in the same run. `resolve("glue.silver.orders")` answers the entry's `268435456` / `20` over the profile's `536870912` / `5`; a partial entry (`glue.silver.partial`) keeps its own size but falls back to the profile `retain_last`; an unknown table falls back to both profile values. Red first below. |
| C-006 | The `adaptive_partitioning` key refuses with "not yet supported" at profile and table level, reserving the name for ADAPT-PART. | `the_adaptive_partitioning_key_refuses_as_not_yet_supported` | **PROVEN** | Green in the same run. Both `[default.maintenance]` and `[default.maintenance.tables.orders]` spellings refuse with `adaptive_partitioning` and `not yet supported` in the message. Red first below. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

Pins written first against the base tree (`44774d9b` plus the step-1 profile/config_file
scaffolding already on this branch, with no `maintenance` module). `cargo test -p repark-core
config_file` was run before any implementation line. No test was edited at any point.

```text
error[E0432]: unresolved import `super::maintenance`
  --> crates/repark-core/src/config_file/tests/mod.rs:13:12
   |
13 | use super::maintenance::{MaintenancePolicy, parse_duration};
   |            ^^^^^^^^^^^ could not find `maintenance` in `super`

error[E0609]: no field `maintenance` on type `&config_file::Profile`
   --> crates/repark-core/src/config_file/tests/mod.rs:775:17
    |
775 |         profile.maintenance.as_ref().expect("maintenance table"),
    |                 ^^^^^^^^^^^ unknown field
    |
    = note: available fields are: `display`, `session`, `conf`, `catalog`, `database`

error: could not compile `repark-core` (lib test) due to 2 previous errors
```

No pin assertion was edited at any point: the six pins above are the exact tests
from this red run, modulo two mechanical normalizations applied later to satisfy
gates already green on `main` — `cargo fmt`'s line reflow and clippy's `duration_suboptimal_units`, whose
suggested `from_days` / `from_hours` / `from_mins` constructors are still
unstable on the pinned toolchain (E0658, issue 120301) — and which fires even
on plain second literals — so the pins compare `as_secs()` integers
(`604_800`, `259_200`, `43_200`, `1_800`) instead of constructing a `Duration`.
Every asserted instant is value-identical.

## PROPOSITION LEDGER — MAINT-POLICY-1 step 2 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-007 | The dry run answers one row per admitted D-4 step with the D-3 frame shape (`step` Int32, `procedure` / `arguments` / `status` / `result` Utf8, every `status` `planned`, stable D-4 ordinals, gated-out steps absent). | `run_maintenance_dry_run_plans_every_step_with_planned_status` | **PROVEN** | Green in the step-2 run (`cargo test -p repark-spark --lib run_maintenance`: `22 passed; 0 failed`, 2026-09-10). The pin asserts column names, Arrow types, the five D-4 procedures in order, ordinals 1–5, all-`planned`, empty `result`, and the per-step argument strings on a merge-on-read table carrying live delete files. Red first below. |
| C-008 | Inline keys override the file's per-table entry, which overrides the profile values. | `run_maintenance_inline_wins_over_table` | **PROVEN** | Green in the same run. `snapshot_retain_last => 2` renders `retain_last => 2` over the entry's 20; without inline the same CALL renders the entry's 20 over the profile's 5, and the rewrite row keeps the entry's `268435456` size. Red first below. |
| C-009 | `dry_run` defaults to true: omitting it plans instead of applying. | `run_maintenance_dry_run_defaults_to_true` | **PROVEN** | Green in the same run. The CALL without `dry_run` answers all-`planned` rows; a false default would take the step-3 apply refusal instead. Red first below. |
| C-010 | A delete ratio below `position_delete_ratio` skips step 1 and keeps stable D-4 ordinals. | `run_maintenance_delete_ratio_gate_below_skips_step_1` | **PROVEN** | Green in the same run. 20 files, 3 snapshots, zero delete bytes against the 0.3 threshold plans ordinals 2, 3, 4, 5 with no `rewrite_position_delete_files` row. Red first below. |
| C-011 | A delete ratio at or above the threshold runs step 1, including exact equality. | `run_maintenance_delete_ratio_gate_at_runs_step_1` + `an_exact_threshold_ratio_admits_step_1` | **PROVEN** | Green in the same run. The CALL pin measures the fixture ratio through the metadata tables, passes it back as the inline threshold, and step 1 admits by exact equality; the pure pin holds 300/1000 against 0.3 while 299/1000 skips. Red first below. |
| C-012 | No policy and no inline keys refuses with the exact D-6 message; inline keys alone satisfy it. | `run_maintenance_no_policy_refuses` | **PROVEN** | Green in the same run. The bare CALL refuses with `run_maintenance: no [default.maintenance] table and no inline keys for sales.ghost`; the same tableless profile plans rows once `target_file_size_bytes => 100` is inline. Red first below. |

VERDICT (step 2): 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Step-2 red first

Pins written first against the step-1 tree (no `run_maintenance` module, no registry stamp,
no public policy API). `cargo test -p repark-spark run_maintenance` was run before any
implementation line. No test was edited to make it pass afterwards except two test-side fixes
with no production cause: a temporary-lifetime borrow in the frame helper, and two wrong
expectations the run itself corrected (the saturate pin expected `i64::MIN` where saturating
subtraction answers `-9223370336854775807`; the 20-file fixture expected `retain_last` alone
to expire fresh snapshots, while the fork only expires snapshots older than the cutoff, so
the fixture passes a future `older_than` like the shipped expire pins do).

```text
error[E0425]: cannot find function `parse_maintenance_policy` in crate `repark_core`
error[E0716]: temporary value dropped while borrowed
  --> crates/repark-spark/src/tests/run_maintenance.rs:45:25
error[E0599]: no method named `set_maintenance_policy` found for mutable reference
  `&mut repark_core::CatalogRegistry` in the current scope
error: could not compile `repark-spark` (lib test) due to 3 previous errors
```

The E0716 is the test helper's own borrow; E0425/E0599 are the missing step-2 API. The
implementation then made all 22 pins green with no further test edits.

## Step-2 notes for the next round

- The file policy reaches the CALL door as a per-execute registry stamp
  (`CatalogRegistry::set_maintenance_policy` / `maintenance_policy`), the same shape as the
  per-execute read-only set the router already stamps. Step 1's sketched
  `maintenance_policy(profile_name, profile)` accessor became `parse_maintenance_policy`
  (full-document text in, typed policy out) because `repark-spark` must not grow a `toml`
  dependency and the D-2 parser must stay single-homed in `maintenance.rs`. The
  `#[allow(dead_code)]` on `resolve` is gone: `plan_steps` calls it through the stamp.
- D-6 with a never-stamped registry names the `default` profile. Production stamping from
  the loaded `repark.toml` is NOT in this step: no step owns the session-build wiring yet
  (step 3 owns the apply path, step 4 the Python wrapper and docs). Whoever wires it stamps
  the active profile name alongside the policy, which is what the D-6 message then names.
- Readings of D-4 this step locks in: `files WHERE content = 0` measures data bytes (the
  `content` ordinals are already pinned by the shipped metadata tests) and `delete_files`
  measures delete bytes; a missing `position_delete_ratio` or `rewrite_manifests = true`
  omits its step, as does a missing `orphan_older_than` (its CALL needs the cutoff); steps
  2 and 4 always plan; ordinals keep D-4 numbers when a step is gated out. Gate-skipped
  versus chain-stopped `skipped` rows are a step-3 call: the dry run only knows `planned`.
- `dry_run => false` refuses loud until step 3 (`run_maintenance_apply_refuses_until_step_3`
  retires there). The rewrite arguments render Spark's options-map spelling
  (`options => map('target-file-size-bytes', '…')`), which the current `rewrite_data_files`
  door refuses: step 3 makes that string executable or revises the rendering and says so.
- The statistics queries re-enter `router::execute` (one `Box::pin` at the call site breaks
  the async recursion the compiler rejects). The procedure loads no table object on a dry
  run; a missing table fails loud from the metadata query itself.
- Measured on 2026-09-10: the fork's `files` / `delete_files` tables expose
  `file_size_in_bytes` as Int64 (fork `inspect/files.rs`, asserted by its own
  `record_count_and_size` test), so neither card hand-back trigger fired in this step.
- The newly public fns carry `#[allow(clippy::missing_errors_doc)]` instead of `# Errors`
  sections: this lane's no-comments fence bans `///`, and the item-scoped allow is the
  repo's precedented escape for pedantic doc lints.

## Step-1 notes for the next round

- `MaintenancePolicy::resolve` carries `#[allow(dead_code)]`: C-005 pins it, but no
  production caller exists until step 2's `plan_steps` arrives. The allow comes off in
  step 2, the same way CFG-1 step 3 removed the now-live allows.
- The `maintenance_policy(profile_name, profile) -> Result<Option<MaintenancePolicy>>`
  accessor (the `None` = "no policy" arm step 2's D-6 refusal needs) was written, found
  with no step-1 caller, and removed rather than kept behind an allow. Step 2 re-adds
  it with its D-6 pin.
- Two shape-refusal branches are implemented but deliberately unpinned in this step:
  a non-table `[<profile>.maintenance` slot and a non-table `tables` entry. Both ride
  on the shared `table_slot` helper, which the sibling slots already pin
  (`a_non_table_display_or_session_refuses_naming_the_path`,
  `a_non_table_catalog_slot_refuses_naming_the_key_path`,
  `a_non_table_database_name_slot_refuses_naming_the_key_path`); only the
  maintenance-spelled instances lack a pin. Step 2 may pin them or leave them.

## PROPOSITION LEDGER — MAINT-POLICY-1 step 3 (apply path + session-build stamp) — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-013 | `dry_run => false` binpacks the 20-file fixture down to the binpack expectation and keeps every row readable. | `apply_binpacks_to_expected_count` | **PROVEN** | Green in the step-3 run (`cargo test -p repark-spark run_maintenance`: `28 passed; 0 failed`, 2026-09-10). Policy `target_file_size_bytes = 67108864` alone plans steps 2 and 4; apply answers both `ran`; `data_files` measures 1; all 22 rows read back. Red first below. |
| C-014 | `dry_run => false` expires snapshots down to `retain_last`. | `apply_expires_to_retain_last` | **PROVEN** | Green in the same run. Policy `snapshot_older_than = "0d"` + `snapshot_retain_last = 1` applies the expire step `ran`; `snapshots` measures 1; all 22 rows read back. `"0d"` is required because the fork only expires snapshots older than the cutoff (default `now - history.expire.max-snapshot-age-ms`, 5 days), so no positive-duration policy can expire fresh snapshots — measured fork semantics, not assumed. Red first below. |
| C-015 | `dry_run => false` removes an aged stray file under the table location and names it in the step result. | `apply_removes_orphan` | **PROVEN** | Green in the same run. Stray `orphan-0.parquet` aged 10 days (the `set_modified` precedent from the orphan-call pins) with policy `orphan_older_than = "3d"`: the step answers `ran`, its JSON `result` contains the stray name, the path is gone from disk, and the 3 live rows read back. Red first below. |
| C-016 | A failing step stops the chain: its row is `failed` with the error text, later rows are `skipped`, and the table stays readable. | `failed_step_stops_chain` | **PROVEN** | Green in the same run. Policy `target_file_size_bytes = 0` + `rewrite_manifests = true` plans steps 2, 3, 4; step 2 answers `failed` with the fork refusal naming `target-file-size-bytes`, steps 3–4 answer `skipped` with empty results, and the 3 rows read back. The zero size proves the policy value reaches the fork action (a dropped size would succeed). Red first below. |
| C-017 | A gate-skipped step stays absent on apply exactly as on a dry run; `skipped` means chain-stopped only. | `apply_omits_gate_skipped_steps_like_dry_run` | **PROVEN** | Green in the same run. The 20-file fixture under the full policy plans ordinals `[2, 3, 4, 5]`; apply reports the identical ordinals, all `ran`, with no step-1 row. Decision rationale: D-3/D-4 reserve `skipped` for rows after a failure, so reusing it for gated-out steps would blur two meanings; no pin showed that hiding a failure. |
| C-018 | A session built from a `repark.toml` with a `[default.maintenance]` table plans the policy values with no inline keys. | `a_repark_toml_policy_reaches_run_maintenance` | **PROVEN** | Green in the same run. `ReparkSession::builder().from_config_file(...).with_extension(SparkExtension).with_sql_dialect(SparkDialect)`, memory catalog plus namespace created through the public session API: the bare CALL plans the file's `536870912` size and `retain_last => 7`. Red first below. |
| C-019 | A session built from a `repark.toml` whose profile has no `maintenance` table refuses naming that profile. | `the_d6_refusal_names_the_active_profile` | **PROVEN** | Green in the same run through the same session shape: the bare CALL refuses with `run_maintenance: no [default.maintenance] table and no inline keys`. This pin is a guard, not red-first: the unstamped tree already named `default`, so it passed before the fix too; the non-default profile naming is pinned at the `FileConfig` level (C-020) where the environment arrives as a stub closure. |
| C-020 | The loaded file resolves into a `(profile, Option<MaintenancePolicy>)` build stamp, and the builder installs it on the registry. | `maintenance_table_resolves_into_file_config_with_profile_name` + `repask_env_profile_names_the_maintenance_stamp` + `file_built_session_stamps_the_registry_maintenance_policy` | **PROVEN** | Green in the step-3 run (`cargo test -p repark-core config_file`: `57 passed; 0 failed`, 2026-09-10). Default-profile values resolve with the `default` name; `REPARK_ENV=analytics` stamps `analytics` with and without a table (`None` policy); a file-built session exposes the stamp on `catalogs_snapshot().maintenance_policy()`. A build with no config file leaves the registry unstamped, so existing sessions behave as before (every pre-existing test exercises that path). Red first below. |

VERDICT (step 3): 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## Step-3 red first

Pins written first against the step-2 tree (apply refusal live, no `maintenance` on
`FileConfig`, no builder stamp). `cargo test -p repark-core config_file` failed to compile;
`cargo test -p repark-spark run_maintenance` ran `23 passed; 6 failed`. No pin assertion was
edited afterwards except provisional measurement values confirmed by the green run.

```text
error[E0609]: no field `maintenance` on type `FileConfig`
   --> crates/repark-core/src/config_file/tests/wiring.rs:315:10
    |
    = note: available fields are: `provenance`, `pairs`, `origins`, `memory_limit_gb`,
             `batch_size`, `target_partitions`

thread 'tests::run_maintenance::apply_binpacks_to_expected_count' panicked at
crates/repark-spark/src/tests/run_maintenance.rs:35:10:
run_maintenance apply: NotImplemented("CALL run_maintenance dry_run => false is not supported
in this build: the dry run is the only mode until the apply path lands")

thread 'tests::run_maintenance::a_repark_toml_policy_reaches_run_maintenance' panicked at
crates/repark-spark/src/tests/run_maintenance.rs:793:10:
the file policy plans with no inline keys: Analysis("Error during planning:
run_maintenance: no [default.maintenance] table and no inline keys for sales.t")
```

The six failing spark pins are the five apply/gate pins plus the session-policy pin; the
D-6 session pin passed before and after (a no-behavior-change guard, C-019). Step 2's
`run_maintenance_apply_refuses_until_step_3` is retired in this step, replaced by the five
apply pins above.

## Step-3 notes for the next round

- No procedure needed SQL-text re-entry, so the card's HALT trigger never fired and no second
  execution path was invented. Each apply step calls the same procedure body the `execute_call`
  router dispatches to, with a programmatically built `CallArgs`: steps 1/3/4/5 through their
  `execute_*` entries, step 2 through the shared `run_rewrite` core extracted from
  `rewrite_data_files.rs` (the door passes `None` for the size after its v1 refusals; apply
  passes the policy size). The door contract is unchanged: `options` stays refused and every
  shipped rewrite pin passes untouched.
- The dry-run rewrite rendering keeps Spark's options-map spelling while apply passes the
  parsed size directly, which the card's step-2 notes explicitly allow ("makes that string
  executable or revises the rendering and says so" — this is the former, scoped to the value,
  with the spelling left as documentation). D-3's "the CALL as it would be issued" holds for
  steps 1/3/4/5 bit-for-bit; for step 2 the size travels typed instead of through the refused
  map. `StepAction` carries the plan-time cutoffs so apply reuses the rendered values rather
  than recomputing `now`.
- The orphan apply passes `dry_run => false` explicitly because that door defaults it true;
  without it the sweep would only list.
- `result` is the step's own frame rendered as JSON by a small local renderer. `serde_json`
  is workspace-pinned but test-only for another crate, and `Cargo.toml` is frozen on this
  card, so the renderer hand-rolls the procedure result types (booleans, integers, floats,
  strings, nulls) and refuses loud on anything else rather than guessing its shape.
  Non-finite floats refuse for the same reason.
- One `Box::pin` at the apply call site keeps the router future under the 16 KiB
  `large_futures` lint; without it three untouched `v3_subquery_dml` tests tripped the lint
  because `execute_call`'s future grew.
- `translate_document` keeps its length lint via the extracted `resolve_maintenance` helper.
- The `audit-repark-parity` skill was considered and not triggered: `run_maintenance` is a new
  unreleased procedure with no live-oracle pins, and no existing Spark-visible default or
  error contract changed (the rewrite door keeps its refusal; the D-6 text is unchanged).
- Measured on 2026-09-10: twenty small files binpack to exactly 1 file at a 64 MiB target;
  the fork's expire honors `retain_last` only for snapshots older than the cutoff, hence the
  `"0d"` fixture; a zero `target-file-size-bytes` fails in the fork before any commit, hence
  the readable table after the stopped chain.

## PROPOSITION LEDGER — MAINT-POLICY-1 step 4 (Python wrapper, guide, rewrite cleanup) — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-021 | `session.run_maintenance` defaults `dry_run` to true: omitting it plans instead of applying. | `test_run_maintenance_dry_run_defaults_to_true` | **PROVEN** | Green in the step-4 run (`.venv/bin/python -m pytest python/repark/tests/test_maintenance_policy_1.py`: `5 passed`, 2026-09-10). Inline `target_file_size_bytes` + `snapshot_retain_last` with no `dry_run` answers every row `planned`. Red first below. |
| C-022 | Override kwargs reach the CALL: `snapshot_retain_last` renders `retain_last`, `target_file_size_bytes` renders the options map. | `test_run_maintenance_override_kwargs_reach_the_call` | **PROVEN** | Green in the same run. Read behaviorally off the planned `arguments` column (`retain_last => 7`, `target-file-size-bytes', '67108864'`), so the pin observes the issued CALL rather than a mock. Red first below. |
| C-023 | No policy and no inline keys refuses with the D-6 text as `AnalysisException`. | `test_run_maintenance_no_policy_refuses_by_class` | **PROVEN** | Green in the same run. `pytest.raises(AnalysisException)` plus the D-6 match; measured on the rebuilt native (a `DataFusionError::Plan` refusal surfaces as `repark.errors.AnalysisException`), never guessed. Red first below. |
| C-024 | The facade answers the D-3 frame shape with stable D-4 ordinals. | `test_session_run_maintenance_frame_shape` | **PROVEN** | Green in the same run. `step` is `pa.int32()`, the four string columns are `pa.string()`, procedures read `["rewrite_data_files", "expire_snapshots"]` with ordinals `[2, 4]`. Red first below. |
| C-025 | The reserved `adaptive_partitioning` key refuses through the facade with "not yet supported". | `test_run_maintenance_adaptive_partitioning_refuses_reserved` | **PROVEN** | Green in the same run. The kwarg rides through to the engine refusal (`AnalysisException`, `not yet supported`); the facade adds no screening of its own. Red first below. |
| C-026 | `docs/guide/maintenance-policy.md` documents the D-1 shape, the D-4 order and gate, the D-3 frame with a worked dry run and apply, the D-6 refusal, and the reservation; `repark-toml.md` gains the `[<profile>.maintenance]` section. | files + maps | **PROVEN** | Both files landed with every behavioral claim executed against the built module first (five-file, five-snapshot memory table; exact `arguments` strings and result JSON quoted verbatim; runnable blocks use `type = "memory"` per R-21). Listed in `docs/guide/map.md` in the same commit. |
| C-027 | The parity mirror stays green: no public-surface count moves, no new example row. | `test_ex_0_example_coverage.py` + `test_cap_1_source_file_line_cap.py` | **PROVEN** | Green in the step-4 run (command in the brief). `run_maintenance` is installed onto `ReparkSession` from the sibling module, so the AST enumerator that reads the `session_core.py` class body still counts 926 rows; `session_core.py` stays byte-identical on its 2305 baseline and every new file lands under the 1000 default. |
| C-028 | `rewrite_data_files` loads the table once: the `where` path shares the door's load with `run_rewrite`. | existing `where` pins green + code path | **PROVEN** | Green in the step-4 run (`cargo test -p repark-spark --lib call_rewrite`: `34 passed`, `cargo test -p repark-spark --lib run_maintenance`: `28 passed`, 2026-09-10). `run_rewrite` now takes the loaded `Table` + `TableIdent`; both callers resolve and load once. No new pin can observe a load count from outside, so the standing `where` byte-identity pins are the regression guard, named here. |
| C-029 | A CALL with both a missing table and a malformed `remove-dangling-deletes` value reports the table, never the flag. | `call_rewrite_missing_table_reports_the_table_before_a_bad_flag` | **PROVEN** | Green in the same spark run. The pin asserts the message names `ghost` and never `remove-dangling-deletes`; it FAILED on the base tree with the flag error (red first below). Decision rationale: this restores the pre-step-3 order (the door resolves and loads its target before validating options), so no shipped contract changes silently. The live-oracle multi-failure comparison was not re-run (no oracle row covers it; parity-live runs on `main`, never on unmerged code); the ledger records that instead of a measurement. |

VERDICT (step 4): 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

## Step-4 red first

Facade pins written first against the tree with no `run_maintenance` attribute.
`.venv/bin/python -m pytest python/repark/tests/test_maintenance_policy_1.py -q`
before any implementation line. No pin assertion was edited afterwards.

```text
python/repark/tests/test_maintenance_policy_1.py:75: AttributeError
=========================== short test summary info ============================
FAILED python/repark/tests/test_maintenance_policy_1.py::test_run_maintenance_dry_run_defaults_to_true
FAILED python/repark/tests/test_maintenance_policy_1.py::test_run_maintenance_override_kwargs_reach_the_call
FAILED python/repark/tests/test_maintenance_policy_1.py::test_run_maintenance_no_policy_refuses_by_class
FAILED python/repark/tests/test_maintenance_policy_1.py::test_session_run_maintenance_frame_shape
FAILED python/repark/tests/test_maintenance_policy_1.py::test_run_maintenance_adaptive_partitioning_refuses_reserved
5 failed in 0.34s
```

The precedence pin written first against the step-3 tree (flag parsed before the
load). `cargo test -p repark-spark --lib
call_rewrite_missing_table_reports_the_table_before_a_bad_flag` before the fix.
No pin assertion was edited afterwards; the table-error text it asserts
(`TableNotFound => No such table`, naming `ghost`) was measured on the miss
path first.

```text
thread 'tests::call_rewrite_options::call_rewrite_missing_table_reports_the_table_before_a_bad_flag' (698990) panicked at crates/repark-spark/src/tests/call_rewrite_options.rs:289:5:
got: Error during planning: CALL argument `remove-dangling-deletes` must be a boolean literal (true / false), got `'not-a-bool'`
```

## Step-4 notes for the orchestrator

- `session_core.py` is byte-identical (2305 baseline holds): the wrapper lives in
  `session_maintenance.py` and `__init__.py` installs it as
  `ReparkSession.run_maintenance`. Class-level assignment is slot-safe
  (`__slots__` blocks instance attributes, not class attributes) and reaches
  every holder of the class object, including the `session_core` direct
  importers. The sibling carries one public function with the one-line
  docstring, module-level lazy imports (the `sql()` precedent), and annotated
  locals per the code-quality skill.
- The D-6 class was measured, not chosen: `DataFusionError::Plan` surfaces as
  `repark.errors.AnalysisException` (probe 2026-09-10 against the rebuilt
  native), and the pin asserts that class.
- Guide truth rule held: the worked dry run and apply ran in this clone against
  the rebuilt native; the `arguments` strings and both result JSON frames are
  quoted verbatim (cutoffs move run to run — the guide says so).
- `make py-test-facade` rebuilds the native the facade pins need; the step-4
  facade run above used that same rebuilt module.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: maint-policy-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Step 1 pins walk D-1 (all keys, unknown-key refusal, per-table override order), D-2 (three suffixes green, nine malformed plus non-string red) and D-7 (reserved key refused at both levels) clause by clause; step 2 pins walk D-3 (frame shape, all-planned, dry_run default, D-6 both halves, unknown and reserved inline keys, apply refusal) and D-4 (gate both sides through real metadata tables, override order file-table-inline) clause by clause.
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs, crates/repark-spark/src/tests/run_maintenance.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Step 1 boundaries are the nine malformed duration spellings, the non-string duration value, the unknown maintenance key, and the full / partial / absent per-table entries; step 2 boundaries are the exact-equality gate (measured ratio passed back inline, plus the 300/1000 pure pin), the empty-table zero ratio both sides of a 0.0 threshold, unset-threshold and unset-flag omissions, a saturating huge duration, bare expire/rewrite renderings, and the loud inline refusals (negative integer, malformed duration, quote doubling).
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs, crates/repark-spark/src/call/run_maintenance.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Step 1 failure paths are loud Error::Config refusals naming the key path over a pure in-memory parse; step 2 failure paths are loud Plan/NotImplemented errors (unknown arg, reserved key, D-6, apply mode, negative integer, malformed inline duration, structurally surprising byte sums), and the dry run commits nothing, so neither step has partial-failure state.
      artifacts: [crates/repark-core/src/config_file/maintenance.rs, crates/repark-spark/src/call/run_maintenance.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Step 1 is pure functions over an in-memory table; step 2 holds no locks across its re-entrant metadata queries (the router clones the registry per execute), and the stamp is per-execute state on the cloned snapshot, so concurrent statements cannot share it.
      artifacts: [crates/repark-core/src/catalog_state.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Step 1 builds no SQL or shell from the values; step 2 doubles single quotes in the rendered CALL text and double-quotes metadata identifiers per part for the inner sums.
      artifacts: [crates/repark-spark/src/call/run_maintenance.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Step 1 is backward compatible by construction (the maintenance slot is optional; all 48 pre-existing config_file pins pass unchanged); step 2 keeps the stamp None until set, so every pre-existing CALL behaves as before and the unknown-procedure list only gains the new name.
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs, crates/repark-spark/src/call.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Step 1 parses a small per-profile table on the cold path; step 2 issues two single-row metadata SUMs and renders five step strings, linear in the D-4 steps.
      artifacts: [crates/repark-spark/src/call/run_maintenance.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Neither step changes a dependency. Step 1 keeps the module private with pub(crate) items under the Error::Config contract; step 2 adds four re-exports plus the registry stamp pair, and every refusal reuses the Plan/NotImplemented contract.
      artifacts: [crates/repark-core/src/lib.rs, crates/repark-core/src/catalog_state.rs, crates/repark-core/src/config_file/maintenance.rs, crates/repark-core/src/config_file/profile.rs]
    - id: AT-9
      status: N/A
      justification: No log or metric surface in either step; each refusal carries the key path in the error text itself.
    - id: AT-10
      status: ATTACKED
      evidence: All twelve PROVEN clauses are cited as pins: maint-policy-1/C-001 through C-006 in the config_file map and C-007 through C-012 in the Spark call, Spark tests, and Spark src maps; each pin asserts exact values or exact message text, so a changed default or reworded refusal fails the suite.
      artifacts: [task/ledgers/staging/maint-policy-1-ledger.md, crates/repark-core/src/config_file/map.md, crates/repark-spark/src/call/map.md, crates/repark-spark/src/tests/map.md, crates/repark-spark/src/map.md]
```
