# map — repark-functions/src/analyzer/time_window

## Purpose

Spark time-bucketing analyzer rules, split at the cohesive boundary so each
file stays under the `check_rust_file_size` ceiling. `SparkSessionWindow`
re-exports through `mod.rs` so its registration path never moved.

## Contents

- `mod.rs` — `SparkTimeWindow` (tumbling/sliding `window` expansion) and
  `SparkWindowTimeGrouping` (`window_time` in-aggregate refusal), plus the
  shared rule tests. Remediation 16a: tumbling projections filter NULL times,
  nested (field-access-wrapped) `window` calls rewrite through their wrappers,
  `window_time` refuses provably non-window structs with the oracle
  `_LEGACY_ERROR_TEMP_3101` text, and touched projections rebase to input
  qualifiers. pins: fnp-win-1/C-002, C-003, C-005, C-006, C-008, C-010, C-012, C-014
- `session_window.rs` — `SparkSessionWindow` (lag / new-session flag /
  running sum / min-max sessionize over staged `session_window` markers).
  Remediation 16a: the dynamic-gap path threads `__repark_session_end__`
  (per-row calendar end, NULL-end drop filter, `ts > previous end` chaining),
  DATE times cast to timestamp, unkeyed plans stay single-partition while keyed
  plans hash-partition on the grouping keys, and display-named session refs
  rebase like the window side.
  pins: fnp-win-1/C-004, C-006, C-008, C-009, C-011, C-012, C-013, C-015

## Pointers

- Up: [../map.md](../map.md) — the analyzer submodules
- Rule registration: [`../../registration.rs`](../../registration.rs)
- UDFs the rule threads: [`../../spark_session_window.rs`](../../spark_session_window.rs)
