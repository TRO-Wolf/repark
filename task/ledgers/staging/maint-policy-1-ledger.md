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
