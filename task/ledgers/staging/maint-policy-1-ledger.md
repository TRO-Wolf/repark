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
  pr_unit: maint-policy-1-step-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Six pins walk D-1 (all keys, unknown-key refusal, per-table override order), D-2 (three suffixes green, nine malformed plus non-string red) and D-7 (reserved key refused at both levels) clause by clause.
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised are the nine malformed duration spellings, the non-string duration value, the unknown maintenance key, and the full / partial / absent per-table entries; the two unpinned shape branches are disclosed in Step-1 notes above.
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every failure path is a loud Error::Config naming the key path; the parse is pure over an in-memory TOML table, so there is no partial-failure state to clean up.
      artifacts: [crates/repark-core/src/config_file/maintenance.rs]
    - id: AT-4
      status: N/A
      justification: Pure functions over an in-memory table; no locks, no shared mutable state, no ordering assumptions.
    - id: AT-5
      status: N/A
      justification: Local config-file parsing only; no auth, no secrets, no SQL or shell built from the values.
    - id: AT-6
      status: ATTACKED
      evidence: Backward compatible by construction: the maintenance slot is optional, and all 48 pre-existing config_file pins pass unchanged beside the 6 new ones.
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs]
    - id: AT-7
      status: N/A
      justification: Cold-path parse of a small per-profile table; no hot loop, no unbounded growth.
    - id: AT-8
      status: ATTACKED
      evidence: No dependency change; the module is private with pub(crate) items, and every refusal reuses the existing Error::Config contract.
      artifacts: [crates/repark-core/src/config_file/maintenance.rs, crates/repark-core/src/config_file/profile.rs]
    - id: AT-9
      status: N/A
      justification: No log or metric surface; each refusal carries the key path in the error text itself.
    - id: AT-10
      status: ATTACKED
      evidence: All six PROVEN clauses are cited as pins: maint-policy-1/C-001 through C-006 in the config_file map; each pin asserts exact values or exact message fragments, so a changed default or reworded refusal fails the suite.
      artifacts: [task/ledgers/staging/maint-policy-1-ledger.md, crates/repark-core/src/config_file/map.md]
```
