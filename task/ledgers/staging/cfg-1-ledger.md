# Unit ledger — CFG-1 step 1 · `repark.toml` discovery, profile merge, `${VAR}` interpolation

**Unit:** CFG-1 step 1 (+ step 1b, R-14) · **Date:** 2026-09-09 · **Branch:** `feat/cfg-1` (step 1b: `feat/cfg-1-step1b`) · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3) — recording round; GLM 5.3 Flash (zai/glm-5.3-flash) — implementation, step 1b (R-14)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** The CFG-1 seed commit left `config_file/{discovery,profile,interpolate}.rs` as empty
placeholders with `load()` answering the empty config. Step 1 lands the ruled steps 1–3 of
[Card CFG-1](../../roadmap/mid-term/cheap-tier-slate-2026-09-08.md): discovery, the
`[default]` + `[<profile>]` merge, and `${VAR}` interpolation. No wiring into `session.rs`;
that is step 3.

**Division of labour.** GLM 5.3 Flash wrote the implementation and its pins and ran both gates
green, then lost its connection twice before writing the ledger or committing. The recording
round (Muse Spark) changed no implementation line: it verified the tree, re-established the
red first, wrote this ledger, updated the two maps, and committed.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** `sources.rs` + `redact.rs` (step 2), `session.rs` wiring
(`from_config_file`, precedence, the dump's `source` column), the Python `config_path` /
`Builder.configFile` arguments (step 3), the Pydantic mirror and `docs/guide/repark-toml.md`
(step 4), `STATUS.md`, `briefs/next-sequence.md`, any dependency file.

## PROPOSITION LEDGER — CFG-1 step 1 — 2026-09-09

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Discovery order is `$REPARK_CONFIG` → `./repark.toml` → `~/.config/repark/repark.toml`, first hit wins; absent everywhere is the empty config, never an error. | `repark_config_beats_local_and_home_repark_toml`, `local_repark_toml_beats_the_home_copy`, `the_home_copy_is_found_when_no_local_file_exists`, `no_file_anywhere_discovers_nothing`, `load_without_a_file_is_the_empty_config` | **PROVEN** | All 24 `config_file` pins green: `test result: ok. 24 passed; 0 failed` (`cargo test -p repark-core config_file`, 2026-09-09). Tempdir fixtures per pin; `discover` takes the environment as a parameter, no pin mutates the process environment. |
| C-002 | `REPARK_CONFIG` set but empty disables discovery for one process. | `test_repark_config_empty_disables_discovery` | **PROVEN** | Green in the same run. The pin writes a local `repark.toml` and still discovers `None` under an empty `REPARK_CONFIG`. |
| C-003 | `REPARK_CONFIG` naming a missing file refuses, naming the path. | `repark_config_naming_a_missing_path_refuses_naming_the_path` | **PROVEN** | Green in the same run. The refusal is `Error::Config` and its message contains the missing path verbatim. |
| C-004 | `[default]` plus `[<profile>]` selected by `REPARK_ENV`, deep-merged; `None` selects the default table alone; an unknown profile refuses naming it. | `profile_merge_deep_merges_the_profile_over_default`, `without_repark_env_the_default_table_stands_alone`, `repark_env_naming_an_absent_profile_refuses_naming_the_profile` | **PROVEN** | Green in the same run. The merge pin asserts the overlay value wins (`/prod/warehouse`), the base-only key survives (`style`), and the sibling key joins (`max_rows`); the refusal names `staging` and lists the known profiles. |
| C-005 | Unknown keys refuse with the key path. | `an_unknown_top_level_key_refuses_as_a_config_error`, `an_unknown_key_inside_a_profile_refuses_with_the_key_path`, `an_unknown_display_key_refuses_with_the_key_path`, `an_unknown_session_key_refuses_with_the_key_path`, `a_non_table_display_or_session_refuses_naming_the_path` | **PROVEN** | Green in the same run. Paths asserted: `default.nonesuch`, `default.display.nonesuch`, `prod.session.nonesuch`, `default.display` / `prod.session` for non-table values, top-level `nonesuch` at the document root. |
| C-006 | `${VAR}` interpolation expands string values; a missing variable refuses loud, naming the key path and the variable. | `interpolation_expands_variables_in_string_values_only`, `a_missing_variable_refuses_naming_the_path_and_the_variable`, `a_missing_variable_in_an_unselected_profile_does_not_refuse`, `a_dollar_without_a_brace_is_left_alone`, `an_empty_variable_name_refuses_naming_the_path`, `interpolation_reaches_array_elements` (the two `$`-edge pins `an_unterminated_reference_is_left_verbatim` / `a_double_dollar_is_not_an_escape` moved to C-007/C-008 under R-14, 2026-09-09) | **PROVEN** | Green in the same run. The missing-variable refusal names both `WAREHOUSE` and `conf.spark.sql.warehouse.dir`; non-string scalars pass through untouched; interpolation applies to the effective table, so an unselected profile's missing variable does not refuse. |
| C-007 | R-14: `$$` is the escape for a literal `$`; a lone `$$` renders one `$`, and `$${TOTAL}` renders the literal `${TOTAL}` verbatim with no environment lookup. | `a_double_dollar_renders_the_reference_verbatim`, `a_double_dollar_alone_renders_a_single_dollar` | **PROVEN** | Step 1b, 2026-09-09. Red first: both pins failed against the step-1 code (`$42` vs `${TOTAL}`, `$$` vs `$` — see Red first below); green after the `interpolate.rs` change. The verbatim pin runs with `TOTAL=42` in the environment and still gets `${TOTAL}`, so no lookup happens. |
| C-008 | R-14: an unterminated `${` (no closing `}`) refuses loud naming the key path, the same `Error::Config` shape as the missing-variable refusal. | `an_unterminated_reference_refuses_naming_the_path` | **PROVEN** | Step 1b, 2026-09-09. Red first: the pin got the step-1 verbatim pass-through (`unterminated must refuse: {"value": String("${TOTAL")}`) instead of an error — see Red first below; green after the change. The refusal message is `unterminated \`${\` reference at key \`value\``, naming the key path. |
| C-009 | `a_dollar_without_a_brace_is_left_alone` (`cost is $5 and ${TOTAL}` → `cost is $5 and 42`) survives R-14 unchanged. | `a_dollar_without_a_brace_is_left_alone` | **PROVEN** | The pin's text was not edited in step 1b and stayed green through both the red run (2026-09-09) and the final gates. |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED (6 from step 1, C-007/C-008/C-009 from step 1b).

## Red first

The previous worker's red runs were lost with its session, so the recording round
re-established them: the four implementation files were stashed (the new `tests.rs` left in
place) and `cargo test -p repark-core config_file` was run against the seed's placeholders.
No test was edited at any point.

The red is a compile refusal, not an assertion failure: the seed's stage files expose none
of the three names the pins import, so the test target does not build. That is an honest
red — the pins cannot pass without the implementation.

```text
error[E0432]: unresolved import `super::discovery::discover`
 --> crates/repark-core/src/config_file/tests.rs:6:5
  |
6 | use super::discovery::discover;
  |     ^^^^^^^^^^^^^^^^^^^^^^^^^^ no `discover` in `config_file::discovery`

error[E0432]: unresolved import `super::interpolate::interpolate_table`
 --> crates/repark-core/src/config_file/tests.rs:7:5
  |
7 | use super::interpolate::interpolate_table;
  |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ no `interpolate_table` in `config_file::interpolate`

error[E0432]: unresolved import `super::profile::effective_table`
 --> crates/repark-core/src/config_file/tests.rs:8:5
  |
8 | use super::profile::effective_table;
  |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ no `effective_table` in `config_file::profile`

error: could not compile `repark-core` (lib test) due to 3 previous errors
```

After `git stash pop` the same command returns to green:
`test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 237 filtered out`.

## Red first — step 1b (R-14)

The two flipped pins and the new escape pin were written first and run against the untouched
step-1 `interpolate.rs`; only then was the implementation changed. Red run:
`cargo test -p repark-core config_file`, 2026-09-09 — `test result: FAILED. 22 passed; 3 failed`.

```text
---- config_file::tests::a_double_dollar_renders_the_reference_verbatim stdout ----
assertion `left == right` failed
  left: "$42"
 right: "${TOTAL}"

---- config_file::tests::a_double_dollar_alone_renders_a_single_dollar stdout ----
assertion `left == right` failed
  left: "$$"
 right: "$"

---- config_file::tests::an_unterminated_reference_refuses_naming_the_path stdout ----
unterminated must refuse: {"value": String("${TOTAL")}

failures:
    config_file::tests::a_double_dollar_alone_renders_a_single_dollar
    config_file::tests::a_double_dollar_renders_the_reference_verbatim
    config_file::tests::an_unterminated_reference_refuses_naming_the_path
```

All three failures are the old step-1 behaviour asserting itself: the escape renders as an
interpolation (`$42`), the bare `$$` stays doubled, and the unterminated reference passes
through instead of refusing. `a_dollar_without_a_brace_is_left_alone` passed in the same run,
unchanged.

## Gates

| Command | Result |
|---|---|
| `cargo test -p repark-core config_file` | exit 0 — `test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 237 filtered out` |
| `make verify` | exit 0 — 48 × `test result: ok`, 0 FAILED; `All checks passed!` (ruff check); `804 files already formatted` (ruff format) |

The recording round fixed nothing: both gates were green on the inherited tree, and the only
tree change in this round outside the inherited diff is this ledger plus the two map edits.

## Gates — step 1b (R-14)

| Command | Result |
|---|---|
| `cargo test -p repark-core config_file` | exit 0 — `test result: ok. 25 passed; 0 failed; 0 ignored; 0 measured; 237 filtered out` (24 step-1 pins, two renamed under R-14, one escape pin added) |
| `make verify` | exit 0 — 48 × `test result: ok`, 0 FAILED; `All checks passed!` (ruff check); `809 files already formatted` (ruff format) |

## Pins

All pins live in `crates/repark-core/src/config_file/tests.rs`.

| Test | Clause |
|---|---|
| `load_without_a_file_is_the_empty_config` (seed) | C-001 |
| `an_empty_document_parses_to_the_empty_config` (seed) | C-005 |
| `an_unknown_top_level_key_refuses_as_a_config_error` (seed) | C-005 |
| `repark_config_beats_local_and_home_repark_toml` | C-001 |
| `local_repark_toml_beats_the_home_copy` | C-001 |
| `the_home_copy_is_found_when_no_local_file_exists` | C-001 |
| `no_file_anywhere_discovers_nothing` | C-001 |
| `test_repark_config_empty_disables_discovery` | C-002 |
| `repark_config_naming_a_missing_path_refuses_naming_the_path` | C-003 |
| `profile_merge_deep_merges_the_profile_over_default` | C-004 |
| `without_repark_env_the_default_table_stands_alone` | C-004 |
| `repark_env_naming_an_absent_profile_refuses_naming_the_profile` | C-004 |
| `an_unknown_key_inside_a_profile_refuses_with_the_key_path` | C-005 |
| `an_unknown_display_key_refuses_with_the_key_path` | C-005 |
| `an_unknown_session_key_refuses_with_the_key_path` | C-005 |
| `a_non_table_display_or_session_refuses_naming_the_path` | C-005 |
| `interpolation_expands_variables_in_string_values_only` | C-006 |
| `a_missing_variable_refuses_naming_the_path_and_the_variable` | C-006 |
| `a_missing_variable_in_an_unselected_profile_does_not_refuse` | C-006 |
| `a_dollar_without_a_brace_is_left_alone` | C-006, C-009 |
| `an_unterminated_reference_refuses_naming_the_path` (step 1b; renamed from `an_unterminated_reference_is_left_verbatim`, flipped per R-14) | C-008 |
| `a_double_dollar_renders_the_reference_verbatim` (step 1b; renamed from `a_double_dollar_is_not_an_escape`, flipped per R-14) | C-007 |
| `a_double_dollar_alone_renders_a_single_dollar` (step 1b) | C-007 |
| `an_empty_variable_name_refuses_naming_the_path` | C-006 |
| `interpolation_reaches_array_elements` | C-006 |

## Decisions

Orchestrator rulings D-5…D-10 from the round's brief history, and how the tree honours them:

| ID | Ruling | Honoured by |
|---|---|---|
| D-5 | No dependency edits. | No `Cargo.toml`, `Cargo.lock`, `pyproject.toml`, `uv.lock`, or `.github/` line is touched; the seed commit already declared `serde` and `toml`. |
| D-6 | `ConfigFile` grows only step-1 fields. | `ConfigFile` is now `{ profiles: BTreeMap<String, Profile> }` with `Profile { display, session, conf, catalog, database }` — the D-1 tables only, no step-2 specs. |
| D-7 | `load()` keeps its ruled signature. | `pub fn load() -> Result<ConfigFile>` is unchanged; the seed's `#[allow(clippy::unnecessary_wraps)]` is gone because `load()` now fails (unreadable directory, unreadable file). |
| D-8 | Discovery takes its environment as a parameter, so no test mutates the process environment. | `discover(environment: EnvironmentLookup, current_directory, home)`; every pin uses a `stub_environment` closure over a `HashMap`. |
| D-9 | Absent file is the empty config, `REPARK_CONFIG` empty disables, a named-but-missing path refuses. | `discover` returns `Ok(None)` for both absent cases; `load()` maps `None` to `ConfigFile::default()`; the named-but-missing arm is `Err` naming the path (C-002, C-003). |
| D-10 | `${VAR}` only — no nesting or defaults syntax. | `interpolate_string` handles exactly `${NAME}`; see the implementation choices below for the edge behaviour this fixes. |

Choices the implementation took that D-5…D-10 did not fix (read from the code, not invented):

| Choice | What the code does |
|---|---|
| Unknown `REPARK_ENV` profile | `effective_table` refuses with `unknown profile \`<name>\` named by REPARK_ENV; the config file carries profiles: <known>`, where `<known>` is the comma-joined profile list or `none`. No fallback to default. |
| `REPARK_ENV` is not read yet | Nothing in step 1 reads the process `REPARK_ENV`; `effective_table` takes `profile_name: Option<&str>` and the caller (step 3 wiring) will pass it. The name appears only in that refusal message. |
| Tests live in one file | All 24 pins sit in `config_file/tests.rs`, not in per-stage test modules; the stage files carry no `#[cfg(test)]` block. |
| Typed allowlists | `display` accepts exactly `max_cols, max_rows, str_len, style`; `session` accepts exactly `batch_size, memory_limit_gb, target_partitions`; anything else refuses naming `name.table.inner`. `conf`, `catalog`, `database` accept any key but must be tables. |
| Non-table slot or top-level value | `[default] display = 3` refuses naming `default.display` (`must be a table`); a non-table top-level value refuses as an unknown top-level key naming the key. |
| `$` without `{` | Left alone (`cost is $5` survives); only `${` opens a reference. |
| Unterminated `${` | Left verbatim (`${TOTAL` survives), not a refusal. Superseded 2026-09-09 by R-14 (step 1b): refuses naming the key path (C-008). |
| `$$` | Not an escape: the second `$` opens a reference, so `$${TOTAL}` with `TOTAL=42` yields `$42`. Superseded 2026-09-09 by R-14 (step 1b): `$$` escapes a literal `$`, so `$${TOTAL}` yields `${TOTAL}` verbatim (C-007). |
| Empty `${}` | Refuses naming the key path (`empty variable name at key \`<path>\``). |
| Reach | Interpolation walks nested tables (paths joined with `.`) and arrays (paths as `path[index]`); integer, float, boolean, and datetime values pass through untouched. |
| Unselected profiles | Interpolation applies to the effective table after the merge, so a missing variable in a profile `REPARK_ENV` did not select never refuses (pinned). |

## Notes for the orchestrator

**Still OPEN:** steps 2–4 of Card CFG-1. `sources.rs` and `redact.rs` are still the seed's
one-line placeholders; `session.rs` has no `from_config_file`; there is no Python mirror and
no `docs/guide/repark-toml.md`.

**The card did not anticipate:** (a) the merge-then-interpolate order is now pinned —
interpolation sees only the effective table, which silently scopes missing-variable refusals
to the selected profile; step 3 should keep that order when it wires `REPARK_ENV` through;
(b) the `$`-edge table (`$$` is not an escape, unterminated is verbatim) is implementation
choice rather than ruled design — if the owner wants POSIX-style `$$` escape or a refusal on
unterminated references, that is a ruling before step 4 documents the file
**— now ruled: R-14 (2026-09-09, step 1b) adopts the POSIX-style `$$` escape and the
unterminated refusal; the `$`-edge facts above and in the config_file map carry the flip;**
(c) `effective_table` with `profile_name: None` returns the default table even when the file
carries only named profiles, and returns an empty table when `[default]` is absent — both
fall out of the code, neither is pinned beyond the `None`-selects-default test.

## Attestation (orchestrator, step 1)

Step 1's six clauses are all PROVEN, so the block is written now rather than at the unit's
departure; steps 2–4 append their own categories when they land.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cfg-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every ruled behaviour is pinned in both directions, not only the happy path — each of the three discovery sources wins in its own fixture and loses in another, an empty REPARK_CONFIG disables discovery with a local file present, a named-but-missing path refuses, the profile merge is asserted on three axes (overlay wins, base-only key survives, sibling joins), and each refusal pin asserts the key path in the message rather than only the failure.
      artifacts: [crates/repark-core/src/config_file/tests.rs]
    - id: AT-2
      status: N/A
      justification: No numeric or performance claim is made; step 1 is a parser and a lookup order.
    - id: AT-3
      status: ATTACKED
      evidence: The failure paths ARE the subject — a missing file, a missing variable, an unknown key at three depths, a non-table where a table is required, an absent profile named by REPARK_ENV — and each is pinned on its message content, not just its Err-ness.
      artifacts: [crates/repark-core/src/config_file/tests.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Ordering is pinned where it is load-bearing: discovery is first-hit-wins across three sources, and the merge-then-interpolate order is pinned by the test that a missing variable in an UNSELECTED profile does not refuse. No shared state exists — discovery takes its environment as a parameter, so no pin mutates the process environment and the suite is parallel-safe.
      artifacts: [crates/repark-core/src/config_file/discovery.rs, crates/repark-core/src/config_file/tests.rs]
    - id: AT-5
      status: ATTACKED
      evidence: The one privileged-ish action is reading a path from the environment, and it is bounded — a path named by REPARK_CONFIG that does not exist refuses instead of falling through to a different file, so a stale variable cannot silently load the wrong configuration. No secret is read, printed or logged in this step; redaction is step 2's file and is still a placeholder.
      artifacts: [crates/repark-core/src/config_file/discovery.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The hole a config loader opens is silent acceptance, and it is closed at every level — serde's deny_unknown_fields at the document root plus typed allowlists for the display and session tables, each refusing with the full key path. A value in the wrong shape (a non-table where a table is required) refuses rather than being ignored.
      artifacts: [crates/repark-core/src/config_file/profile.rs]
    - id: AT-7
      status: N/A
      justification: No hot path and no resource behaviour; the loader reads one small file once at session construction, and is not wired into the builder until step 3.
    - id: AT-8
      status: ATTACKED
      evidence: The two new upstream dependencies are the orchestrator's seed commit, not the worker's, and are the versions the card names (serde 1.0.229, toml 0.8.23); Cargo.lock was refreshed by the build. The public surface is unchanged — the module is crate-private and its items carry allow(dead_code) precisely because nothing calls them until step 3.
      artifacts: [Cargo.toml, crates/repark-core/Cargo.toml]
    - id: AT-9
      status: ATTACKED
      evidence: Diagnosability is the point of every refusal in this step: each message names the key path, and the missing-variable refusal names both the variable and the path it sat under. The pins assert those strings, so a wording regression is red.
      artifacts: [crates/repark-core/src/config_file/interpolate.rs]
    - id: AT-10
      status: ATTACKED
      evidence: The red was re-established honestly after the implementing session was lost — the four implementation files stashed with the pins left in place, and the recorded red is the compile refusal (E0432 x3) that the seed's placeholders produce, pasted verbatim. No test was edited to manufacture it.
      artifacts: [crates/repark-core/src/config_file/tests.rs]
  complete: true
```

