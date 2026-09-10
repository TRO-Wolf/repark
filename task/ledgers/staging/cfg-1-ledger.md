# Unit ledger — CFG-1 step 1 · `repark.toml` discovery, profile merge, `${VAR}` interpolation

**Unit:** CFG-1 step 1 (+ step 1b, R-14) · **Date:** 2026-09-09 · **Branch:** `feat/cfg-1` (step 1b: `feat/cfg-1-step1b`) · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3) — recording round; GLM 5.3 Flash (zai/glm-5.3-flash) — implementation, step 1b (R-14) and step 2; Muse Spark (muse-spark-1.3-contributor) — implementation, step 3 and step 4
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
| C-010 | A `[<profile>.catalog.<name>]` block parses into the SAME `CatalogSpec` the equivalent `.config()` keys produce — `assert_eq!`-identical against the measured Glue block and against native `memory` / `postgres` blocks vs the flat path. | `a_toml_catalog_block_matches_the_equivalent_config_calls_byte_for_byte`, `native_type_catalog_blocks_match_the_flat_config_path` | **PROVEN** | Red first (step 2 below), green in the step-2 run: `40 passed; 0 failed`. The TOML block is joined into `repark.sql.catalog.<name>.<prop>` keys and parsed by `parse_catalog_specs` itself, so `io-impl` is dropped and `catalog-impl` consumed exactly as on the flat path; the `assert_eq!` covers name, kind and the full props map. |
| C-011 | The Java-class spelling and the native `type` spelling of one catalog produce the same spec (`CatalogKind::Glue`, `CatalogKind::S3Tables`); the S3 Tables warehouse-ARN translation fires on both spellings. | `java_class_and_native_type_spellings_produce_the_same_spec` | **PROVEN** | Red first (step 2 below); green in the step-2 run. Pairs: `catalog-impl = "…GlueCatalog"` ↔ `type = "glue"`, and `catalog-impl = "…S3TablesCatalog"` ↔ `type = "s3tables"` with the ARN as `warehouse` on both sides so the prop sets match; `table_bucket_arn` is asserted present from the ARN translation. |
| C-012 | `[<profile>.database.<kind>.<name>]` parses into the crate-private `SourceSpec` (`postgres \| sqlserver \| trino`), carrying name, kind and connection props as a `BTreeMap<String, String>`. | `database_tables_parse_into_source_specs` | **PROVEN** | Red first (step 2 below); green in the step-2 run. One profile with all three kinds; each source found with its `SourceKind` and props asserted; `catalogs` empty on the same profile. |
| C-013 | An unknown database kind refuses loud naming the key path; any other spelling (including capitalization) refuses. | `an_unknown_database_kind_refuses_naming_the_key_path` | **PROVEN** | Red first (step 2 below); green in the step-2 run. `prod.database.hive` refuses and the message lists `postgres, sqlserver, trino`; `prod.database.Postgres` refuses the same way — exact-spelling match per ruling D-5. |
| C-014 | Names are unique per profile across the catalog and database families; a collision refuses loud naming both key paths — cross-family and inside the database family. | `a_name_collision_across_the_families_refuses_naming_both_keys`, `a_name_collision_inside_the_database_family_refuses_naming_both_keys` | **PROVEN** | Red first (step 2 below); green in the step-2 run. The cross-family refusal names `default.catalog.acme` and `default.database.postgres.acme`; the within-family pin refuses `prod.database.postgres.acme` vs `prod.database.trino.acme`. |
| C-015 | Redaction: `redact_config` masks every key `prop_key_is_secret` matches, an unmasked secret never appears in the redacted output, non-secret values stay verbatim, no key is dropped; `SourceSpec`'s `Debug` masks through the same function. | `a_dump_masks_every_key_the_secret_predicate_matches`, `a_source_spec_debug_masks_secret_props` | **PROVEN** | Red first (step 2 below); green in the step-2 run. The dump pin runs the C1-SEC-002 key matrix (13 secret spellings incl. hyphenated and camelCase) plus a non-secret `url`; the Debug pin asserts the password is absent, `***` present, the url verbatim. `redact.rs` calls `catalog_config::prop_key_is_secret` (widened to `pub(crate)` this step) — the predicate is not re-implemented. |
| C-016 | Wrong shapes refuse loud naming the key path: a non-string catalog prop, a non-string database prop, a non-table catalog slot, a non-table database name slot. | `a_non_string_catalog_prop_refuses_naming_the_key_path`, `a_non_string_database_prop_refuses_naming_the_key_path`, `a_non_table_catalog_slot_refuses_naming_the_key_path`, `a_non_table_database_name_slot_refuses_naming_the_key_path` | **PROVEN** | Red first (step 2 below); green in the step-2 run. Paths asserted: `default.catalog.g.max_connections`, `prod.database.postgres.company_db.port`, `default.catalog.m`, `prod.database.postgres.company_db`. TOML scalars are never silently stringified. |
| C-017 | Loud guards on catalog names: a block carrying no properties refuses, and a dotted catalog name refuses instead of silently re-splitting through the flat-key bridge. | `a_catalog_block_carrying_no_properties_refuses`, `a_catalog_name_with_a_dot_refuses_instead_of_resplitting` | **PROVEN** | Red first (step 2 below); green in the step-2 run. An empty `[default.catalog.ghost]` refuses naming the path (flat config has no equivalent — a silent ignore would vanish the catalog); `[default.catalog."a.b"]` refuses naming `default.catalog.a.b` (flat keys would alias it to catalog `a`). |
| C-018 | `[<profile>.display]` translates to `repark.display.*` pairs (`style`, `max_rows`, `max_cols`, `str_len`); string values verbatim, integers stringified; any other TOML shape refuses naming the key path. | `test_toml_display_table_sets_style` | **PROVEN** | Red first (step 3 below); green in the step-3 run (`48 passed; 0 failed`). `max_rows = 20` arrives as `"20"`; `max_rows = true` refuses naming `default.display.max_rows`. |
| C-019 | `[<profile>.session]` knobs (`memory_limit_gb`, `batch_size`, `target_partitions`) land as typed builder fallbacks (explicit typed setters still win); integers or integer strings, else refuses naming the key path. | `test_toml_session_table_sets_builder_knobs` | **PROVEN** | Red first (step 3 below); green in the step-3 run. A file-built session reports `batch_size 4096` / `target_partitions 3` through the live engine config; `memory_limit_gb = 2` is asserted on the translation; `batch_size = "lots"` refuses naming `default.session.batch_size`. |
| C-020 | `[<profile>.conf]` applies in sorted-key order (deterministic; see C-024 for why not file order); string values verbatim, integers stringified, other shapes refuse naming the key path. TOML-nested tables flatten with dot joins so the natural `spark.sql.x = "v"` spelling works; a quoted-plus-nested collision refuses. | `test_toml_conf_table_applies_in_order`, `conf_nested_tables_flatten_with_dot_joins` | **PROVEN** | Red first (step 3 below); green in the step-3 run. `zebra/apple/mango` in file order apply as `apple/mango/zebra`; `port = 5432.5` refuses naming `default.conf.port`. The flatten pin was red-proven separately against the pre-flatten code (`key \`default.conf.spark\` must be a string`) before going green. |
| C-021 | Precedence is builder `.config()` > `REPARK_ENV` profile > `[default]`, pinned as a parametrised table over one file plus varying builder maps. | `file_builder_precedence_is_builder_then_profile_then_default` | **PROVEN** | Red first (step 3 below); green in the step-3 run. Builder value wins with source `builder`; otherwise the profile value wins with `file:<path>#prod`; profile-absent keys keep the default value with `default`. |
| C-022 | The dump renders `(key, redacted value, source)` with sources `builder`, `file:<path>#<profile>`, `default` — exactly the three precedence levels — and masks every secret key through `config_file::redact`. | `file_dump_reports_origin_and_masks_secrets` | **PROVEN** | Red first (step 3 below); green in the step-3 run. `password` renders `***` with source `default`; a profile-overridden nickname renders verbatim with `file:<path>#prod`; a builder-overridden one renders verbatim with `builder`. |
| C-023 | Done condition: a session built from a file registers the same catalogs the equivalent `.config()` calls would — equal merged pairs modulo the source column, both registrations `Ok`, both probes agree. | `file_built_session_registers_the_same_catalogs_as_config_calls` | **PROVEN** | Red first (step 3 below); green in the step-3 run. Memory catalog `m` over a tempdir warehouse on both sides; the flat side uses the `repark.sql.catalog.*` spelling so the pair maps compare key-for-key. |
| C-024 | D-1 says `[<profile>.conf]` keys apply "in file order"; this build's `toml::Table` is `BTreeMap`-backed (`preserve_order` off), so file order is not recoverable without a dependency-feature change. | (none — open wording question for the owner) | **OPEN** | Measured in step 2, ruled for this round by the orchestrator: apply in sorted-key order, deterministic and pinned (C-020). Ways out: enable `toml`'s `preserve_order` feature, or carry the order some other way. No dependency was touched. |
| C-025 | A non-empty `[<profile>.database]` table refuses loud at load: the sources parse and validate (collisions included) but named-source registration is CFG-2's card, and a silent drop would vanish configured sources. | `database_sources_refuse_until_named_registration_lands` | **PROVEN** | Red first (step 3 below); green in the step-3 run. The refusal names `default.database.postgres.company_db` and the `CFG-2` card. |
| C-026 | `Builder.configFile(path)` forces a file and automatic discovery (`REPARK_CONFIG` → CWD → home) runs at `getOrCreate` without it; the file's translated pairs fold through the existing `.config()` choke point with builder keys winning, so display, session-knob, and `conf` values reach the knob resolvers and `conf.get`. | `test_config_file_folds_pairs_through_config`, `test_repark_config_env_discovers_file` | **PROVEN** | Red first (Python, below: `'Builder' object has no attribute 'configFile'`); green after `make develop` (counts in Gates). The fold asserts `custom.probe.key` in `conf.get` and `display_style == "spark"` from the file; the discovery pin sets `REPARK_CONFIG` by monkeypatch and reads the file value back. |
| C-027 | The forced path reaches the engine through `PyReparkSession::new(config_path=...)`; end to end, an explicit `.config()` beats the file, and a forced-but-missing path refuses naming it. | `test_builder_config_beats_config_file`, `test_config_file_missing_path_refuses`, `config_path_names_the_forced_file_and_missing_refuses` (binding) | **PROVEN** | Red first (Python, below); green after `make develop` (counts in Gates). The missing-path refusal matches `does not exist` from the engine's forced-path check; the binding pin also asserts undiscoverable `config_file_pairs(None)` comes back empty. |
| C-028 | The `repark.config` mirror validates typed construction the way the loader parses: unknown `display` / `session` keys refuse, session knobs keep non-negative integers and digit strings, booleans refuse in every table, database kinds outside `postgres / sqlserver / trino` refuse, dotted catalog names / empty catalog blocks / cross-family name collisions refuse. | `test_unknown_display_key_refuses`, `test_unknown_session_key_refuses`, `test_session_digit_string_accepted`, `test_session_negative_knob_refuses`, `test_boolean_values_refuse`, `test_unknown_database_kind_refuses`, `test_catalog_name_with_dot_refuses`, `test_empty_catalog_block_refuses`, `test_name_collision_across_families_refuses` | **PROVEN** | Red first (step 4, below: `ModuleNotFoundError: No module named 'repark.config'`); green after `config.py` landed: `12 passed` (`.venv/bin/python -m pytest python/repark/tests/test_config_mirror.py -q`, 2026-09-10). The mirror never parses: no discovery, merge, interpolation, or translation code exists in the module. |
| C-029 | The mirror renders TOML the loader accepts: the text parses back to the same tables (dotted conf keys nest and flatten at load), a rendered file builds a session answering the file's conf value and display style, and a rendered database source refuses at load naming `CFG-2`. | `test_rendered_toml_parses_to_same_tables`, `test_rendered_file_builds_session_with_file_values`, `test_rendered_database_source_refuses_at_load_until_cfg_2` | **PROVEN** | Red first (step 4, below); green in the same `12 passed` run. The session pin reads `example.probe.key` and `display_style` off a live session built from `save()` output; the refusal pin matches `CFG-2` from the engine's database-table check. |
| C-030 | `docs/guide/repark-toml.md` carries one complete example file (`[default]`, `[prod]`, `[read]`, `[write]`, one catalog, one database source, the display and session tables), states the database-load refusal and the sorted-`conf`-key order plainly, and quotes only blocks and errors that ran in this clone. | Step-3 pins for discovery/precedence/fold (`test_repark_config_env_discovers_file`, `test_builder_config_beats_config_file`, `test_config_file_folds_pairs_through_config`) plus the C-029 render/refusal pins for the quoted outputs | **PROVEN** | Every quoted transcript re-ran in this clone (probe outputs in Gates — step 4): the mirror render, the `cfg-1` / `polars` build, `from-env` / `from-local` discovery, the empty-disable `Configuration property probe.key is not set.`, the four `repark config error` refusals verbatim, the `'${TOTAL}'` escape with `TOTAL=42`, and the `***` / `plain` / `s3cr3t` redaction triple. The guide is linked from `docs/guide/map.md` (Contents + I-want rows) with a pointer in `session-and-conf.md`. |
| C-031 | `config.py` adds no enumerated public name: the surface stays 923, no size baseline moves, and the parity suite plus `check-example-coverage` stay green with no covering example owed. | The `len(rows) == 923` pin in `test_ex_0_enumerator_emits_five_families_and_repark_sql` | **PROVEN** | The enumerator walks only the function/class/module doors plus `repark.sql` — `repark/config.py` is none of them — so the count pin holds without a bump (parity-suite output in Gates — step 4). `config.py` (253 lines) and the test file carry no baseline row; `check_lib_py` is clean. |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED (6 from step 1, C-007/C-008/C-009 from step 1b).
VERDICT (step 2, 2026-09-09): 17 clauses, 17 PROVEN, 0 OPEN, 0 REJECTED (C-010…C-017 appended this step).
VERDICT (step 3, 2026-09-09): 25 clauses, 24 PROVEN, 1 OPEN, 0 REJECTED (C-018…C-025 appended this step; C-024 is the orchestrator-ruled D-1 wording question, carried OPEN).
VERDICT (step 3 facade, 2026-09-09): 27 clauses, 26 PROVEN, 1 OPEN, 0 REJECTED (C-026, C-027 appended with the Python slice; C-024 still the only OPEN).
VERDICT (step 4, 2026-09-10): 31 clauses, 30 PROVEN, 1 OPEN, 0 REJECTED (C-028…C-031 appended with the mirror + guide slice; C-024 still the only OPEN — the owner's file-order question, untouched by this step).

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

## Red first — step 2 (sources.rs + redact.rs)

The 15 step-2 pins were written into `tests.rs` first and the run was taken against the
seed's one-byte `sources.rs` / `redact.rs` placeholders; only then were the two files
implemented. Red run: `cargo test -p repark-core config_file`, 2026-09-09 — a compile
refusal, the same honest red class as step 1: the placeholders expose none of the names the
pins import.

```text
error[E0432]: unresolved import `super::redact::redact_config`
  --> crates/repark-core/src/config_file/tests.rs:11:5
   |
11 | use super::redact::redact_config;
   |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ no `redact_config` in `config_file::redact`

error[E0432]: unresolved imports `super::sources::SourceKind`, `super::sources::SourceSpec`, `super::sources::profile_sources`
  --> crates/repark-core/src/config_file/tests.rs:12:22
   |
12 | use super::sources::{SourceKind, SourceSpec, profile_sources};
   |                      ^^^^^^^^^^  ^^^^^^^^^^  ^^^^^^^^^^^^^^^ no `profile_sources` in `config_file::sources`
   |                      |           |
   |                      |           no `SourceSpec` in `config_file::sources`
   |                      no `SourceKind` in `config_file::sources`

For more information about this error, try `rustc --explain E0432`.
error: could not compile `repark-core` (lib test) due to 2 previous errors
```

After implementing the two files the same command returns green:
`test result: ok. 40 passed; 0 failed; 0 ignored; 0 measured; 237 filtered out`
(25 inherited pins + 15 step-2 pins).

## Red first — step 3 (wiring)

The 8 step-3 pins were written first (before `wiring.rs`, `from_config_file`, or
`conf_dump` existed) and run against the untouched step-2 tree. The eighth pin
(`conf_nested_tables_flatten_with_dot_joins`) arrived with the facade round, after the
pins exposed that TOML nests dotted `conf` keys; it was red-proven separately (see C-020).
Red run: `cargo test -p repark-core config_file`, 2026-09-09 — a compile refusal, the same
honest red class as steps 1–2: the pins name items that do not exist yet.

```text
error[E0432]: unresolved import `super::wiring`
  --> crates/repark-core/src/config_file/tests.rs:13:12
   |
13 | use super::wiring::{FileConfig, conf_dump_rows, load_file_config};
   |            ^^^^^^ could not find `wiring` in `super`

error[E0599]: no method named `from_config_file` found for struct `ReparkSessionBuilder` in the current scope
   --> crates/repark-core/src/config_file/tests.rs:834:10

error[E0599]: no method named `from_config_file` found for struct `ReparkSessionBuilder` in the current scope
   --> crates/repark-core/src/config_file/tests.rs:980:10

error[E0599]: no method named `conf_dump` found for struct `session::ReparkSession` in the current scope
    --> crates/repark-core/src/config_file/tests.rs:1002:65

error: could not compile `repark-core` (lib test) due to 4 previous errors
```

After implementing `wiring.rs` plus the `session.rs` hook the same command returns green:
`test result: ok. 48 passed; 0 failed; 0 ignored; 0 measured; 237 filtered out`
(40 inherited pins + 8 step-3 pins). No pin was edited at any point; the battery was then
split `tests.rs` → `tests/mod.rs` + `tests/wiring.rs` (identity move, same green after).

## Red first — step 3 facade (Python)

The 4 facade pins were written into `test_builder_config_map.py` first (before
`Builder.configFile`, the fold, or the native `config_path` existed) and run against the
untouched tree. Red run: `.venv/bin/python -m pytest
python/repark/tests/test_builder_config_map.py -q -k "config_file or beats_config_file or
env_discovers"`, 2026-09-09 — `4 failed, 20 deselected`, every failure the missing
surface:

```text
E       AssertionError: Regex pattern did not match.
E         Expected regex: 'does not exist'
E         Actual message: "'Builder' object has no attribute 'configFile'"

=========================== short test summary info ============================
FAILED python/repark/tests/test_builder_config_map.py::test_config_file_folds_pairs_through_config
FAILED python/repark/tests/test_builder_config_map.py::test_builder_config_beats_config_file
FAILED python/repark/tests/test_builder_config_map.py::test_repark_config_env_discovers_file
FAILED python/repark/tests/test_builder_config_map.py::test_config_file_missing_path_refuses
4 failed, 20 deselected in 1.10s
```

No pin was edited at any point; green came from the implementation plus `make develop`.

## Red first — step 4 (mirror)

The 11 step-4 pins were written into `test_config_mirror.py` first (before
`repark/config.py` existed) and run against the untouched step-3 tree. Red run:
`.venv/bin/python -m pytest python/repark/tests/test_config_mirror.py -q`, 2026-09-10 —
a collection refusal: the module under test does not exist yet.

```text
E   ModuleNotFoundError: No module named 'repark.config'
!!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
1 error in 0.18s
```

After implementing `config.py` the same command returns green (`12 passed` — the
boolean-refusal pin arrived with the bool-coercion fix, see Choices step 4). No pin was
edited except the two `prod`/`default` conf-shape assertions, which moved from the flat
quoted-key form to the nested form when the renderer switched dotted conf keys from quoted
to bare (the loader flattens both to the same key; the session pin proves the flat read).

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

## Gates — step 2 (sources.rs + redact.rs)

| Command | Result |
|---|---|
| `cargo test -p repark-core config_file` | exit 0 — `test result: ok. 40 passed; 0 failed; 0 ignored; 0 measured; 237 filtered out` (25 inherited pins + 15 step-2 pins) |
| `make verify` | exit 0 — 48 × `test result: ok`, 0 FAILED; repark-core lib suite `277 passed` (the 40 `config_file` pins included); `All checks passed!` (ruff check); `809 files already formatted` (ruff format) |

## Gates — step 3 (wiring + facade)

| Command | Result |
|---|---|
| `cargo test -p repark-core config_file` | exit 0 — `test result: ok. 48 passed; 0 failed; 0 ignored; 0 measured; 237 filtered out` (40 inherited pins + 8 step-3 pins) |
| `cargo test -p repark-core` | exit 0 — lib `285 passed`, integration `37 passed` + `8 passed`, 0 FAILED |
| `cargo test -p repark-python --test bindings` | exit 0 — `25 passed; 0 failed` (24 inherited + the new `config_path_names_the_forced_file_and_missing_refuses`) |
| `.venv/bin/python -m pytest python/repark/tests/test_builder_config_map.py python/repark/tests/test_session_config_knobs.py python/repark/tests/test_session.py python/repark/tests/test_display_styles.py -q` | exit 0 — `147 passed` (resolver-move blast radius plus the 4 new facade pins) |
| `make develop` | exit 0 — wheel rebuilt and installed editable after every native change |
| `make py-test-facade` | exit 0 — `5841 passed, 369 skipped, 7 xfailed` in 692s |
| `.venv/bin/python docs/examples/session/config_file.py` | exit 0 — the new covering example runs end to end |
| `make verify` | exit 0 — full gate green (clippy, panic-ban, DAG, lib-rs, file sizes, ruff, example coverage, ledgers, ledger grammar, docs, manifest, workspace tests) |

## Gates — audit follow-up (ceiling DOWN ratchet)

| Command | Result |
|---|---|
| `cargo test -p repark-python` | exit 0 — lib `59 passed`, bindings `25 passed`, 0 FAILED (identical counts before and after the `drain_arrow_c_stream` move — the move is pure) |
| `python3 scripts/check_rust_file_size.py` | exit 0 — `442 files clean (default ceiling 1000; 36 exceptions)`; `session.rs` row now 1128 |
| `make verify` | exit 0 — full gate green |
| `make py-test-parity-cap` | exit 0 — `23 passed` |

## Gates — step 4 (mirror + guide)

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_config_mirror.py -q` | exit 0 — `12 passed` (11 red-first pins + the boolean-refusal pin) |
| `make py-test-facade` | exit 0 — `5858 passed, 369 skipped, 7 xfailed` in 702s (maturin develop + full facade suite) |
| `make py-lint` | exit 0 — `All checks passed!` (uvx ruff@0.15.22 check) |
| `python3 scripts/check_lib_py.py` | exit 0 — `600 files clean (default ceiling 1000; 32 exceptions; facade no-stub held)` |
| `make check-docs-links` | exit 0 after `git add` — `703 files, 4541 links checked — clean` (pre-add red was only `repark-toml.md -> exists but is not tracked` ×3) |
| `python3 scripts/check_ledger_grammar.py` | exit 0 — `65 live ledgers clean (419 clauses, 1066 pinned clause ids, 2 exception rows)` |
| `make check-example-coverage` | exit 0 — `923 public names; 793 covered; 128 backlog; 2 exceptions; 210 examples` (count unmoved, C-031) |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q` | `638 passed`, 1 failed (`test_dl_6_docs_links`, the same untracked-file red — green on rerun after `git add`: `35 passed` with `test_ex_0_example_coverage.py`, 923 pin holds) |

Probe outputs quoted in `docs/guide/repark-toml.md` (all ran 2026-09-10 in this clone):

```text
cfg-1
polars
probe: from-env
probe: from-local
Exception: Configuration property probe.key is not set.
IllegalArgumentException repark config error: missing environment variable `NO_SUCH_VAR_DEFINED_ANYWHERE` for key `conf.key`
IllegalArgumentException repark config error: unterminated `${` reference at key `conf.key`
IllegalArgumentException repark config error: unknown profile `staging` named by REPARK_ENV; the config file carries profiles: default, other
IllegalArgumentException repark config error: database sources (default.database.postgres.company_db) are parsed but named-source registration arrives with CFG-2 — drop the `[<profile>.database]` tables until that card lands
'${TOTAL}'
***
plain
s3cr3t
shared: from-builder
shared: from-prod
default.only: d
```

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
| `a_toml_catalog_block_matches_the_equivalent_config_calls_byte_for_byte` (step 2) | C-010 |
| `native_type_catalog_blocks_match_the_flat_config_path` (step 2) | C-010 |
| `java_class_and_native_type_spellings_produce_the_same_spec` (step 2) | C-011 |
| `database_tables_parse_into_source_specs` (step 2) | C-012 |
| `an_unknown_database_kind_refuses_naming_the_key_path` (step 2) | C-013 |
| `a_name_collision_across_the_families_refuses_naming_both_keys` (step 2) | C-014 |
| `a_name_collision_inside_the_database_family_refuses_naming_both_keys` (step 2) | C-014 |
| `a_dump_masks_every_key_the_secret_predicate_matches` (step 2) | C-015 |
| `a_source_spec_debug_masks_secret_props` (step 2) | C-015 |
| `a_non_string_catalog_prop_refuses_naming_the_key_path` (step 2) | C-016 |
| `a_non_string_database_prop_refuses_naming_the_key_path` (step 2) | C-016 |
| `a_non_table_catalog_slot_refuses_naming_the_key_path` (step 2) | C-016 |
| `a_non_table_database_name_slot_refuses_naming_the_key_path` (step 2) | C-016 |
| `a_catalog_block_carrying_no_properties_refuses` (step 2) | C-017 |
| `a_catalog_name_with_a_dot_refuses_instead_of_resplitting` (step 2) | C-017 |
| `test_toml_display_table_sets_style` (step 3) | C-018 |
| `test_toml_session_table_sets_builder_knobs` (step 3) | C-019 |
| `test_toml_conf_table_applies_in_order` (step 3) | C-020 |
| `conf_nested_tables_flatten_with_dot_joins` (step 3) | C-020 |
| `file_builder_precedence_is_builder_then_profile_then_default` (step 3) | C-021 |
| `file_dump_reports_origin_and_masks_secrets` (step 3) | C-022 |
| `file_built_session_registers_the_same_catalogs_as_config_calls` (step 3) | C-023 |
| `database_sources_refuse_until_named_registration_lands` (step 3) | C-025 |
| `test_config_file_folds_pairs_through_config` (step 3 facade) | C-026 |
| `test_repark_config_env_discovers_file` (step 3 facade) | C-026 |
| `test_builder_config_beats_config_file` (step 3 facade) | C-027 |
| `test_config_file_missing_path_refuses` (step 3 facade) | C-027 |
| `config_path_names_the_forced_file_and_missing_refuses` (step 3 binding) | C-027 |
| `test_unknown_display_key_refuses` (step 4) | C-028 |
| `test_unknown_session_key_refuses` (step 4) | C-028 |
| `test_session_digit_string_accepted` (step 4) | C-028 |
| `test_session_negative_knob_refuses` (step 4) | C-028 |
| `test_boolean_values_refuse` (step 4) | C-028 |
| `test_unknown_database_kind_refuses` (step 4) | C-028 |
| `test_catalog_name_with_dot_refuses` (step 4) | C-028 |
| `test_empty_catalog_block_refuses` (step 4) | C-028 |
| `test_name_collision_across_families_refuses` (step 4) | C-028 |
| `test_rendered_toml_parses_to_same_tables` (step 4) | C-029 |
| `test_rendered_file_builds_session_with_file_values` (step 4) | C-029 |
| `test_rendered_database_source_refuses_at_load_until_cfg_2` (step 4) | C-029, C-030 |

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

Choices step 2 took that the card and D-5 did not fix (read from the code, not invented):

| Choice | What the code does |
|---|---|
| Catalog bridge | Each `[<profile>.catalog.<name>]` block is joined into `repark.sql.catalog.<name>.<prop>` keys (the repark-native prefix, "accepted as a synonym for new code") and parsed by `parse_catalog_specs` itself — one code path for both sources, byte-identical specs by construction. |
| Native `type = "rest"` | Refuses loud with `parse_catalog_specs`'s existing unrecognized-value error. `CatalogKind` has no `Rest` variant in this tree; the design plan gives `CatalogKind::Rest` to the REST-catalog card. No variant was invented. |
| C-025 flagged to the owner (orchestrator, 2026-09-09) | The step chose to refuse loud at LOAD when a profile carries a non-empty `[<profile>.database]` table. That is stricter than the ruled CFG-2 card, which says a database source in this era is "parsed, validated, listed, and refuses-loud **on use**". Accepted for this step because no listing surface exists yet, so the only alternatives were a silent drop (worse) or inventing CFG-2's registry here (out of scope) — and refusing at load names the key and the CFG-2 card rather than losing the section. **Consequence the owner should weigh: until CFG-2 lands, a `repark.toml` that declares a database source cannot open a session at all.** Carried to the owner as an open question in the run-4 report. |
| Q-1 answered (orchestrator, 2026-09-09) | The loud refusal STANDS for this unit. `type = "rest"` is refused by `parse_catalog_specs`'s existing unrecognized-value error until a `CatalogKind::Rest` ruling lands with the REST-catalog card; the TOML path adds no kind the flat `.config()` path does not already have. Carried to the owner as an open question in the run-4 report, not as work for CFG-1. |
| `[<profile>.conf]` ordering (step-3 input, 2026-09-09) | `toml::Table` is `BTreeMap`-backed in this build (no `preserve_order` feature), so a `conf` table iterates in sorted-key order, not file order. D-1 says the `conf` keys apply "in file order"; step 3's wiring must either enable `preserve_order` or carry the order itself. Measured in step 2, filed here so step 3 does not discover it late. |
| Database kinds | Exact-spelling match on `postgres / sqlserver / trino` per D-5's "any other spelling" — `Postgres` (capitalized) refuses; no case-folding, unlike the catalog `type` path's lowercase folding, which lives in `catalog_config.rs` and is not this step's code. |
| Non-string props | A non-string TOML value inside a catalog or database block refuses naming the key path; no silent stringification of TOML scalars. |
| Empty catalog block | `[<profile>.catalog.<name>]` with no properties refuses naming the path — flat config has no equivalent of an empty block, so a silent ignore would vanish the catalog without an error. |
| Dotted catalog name | A quoted name containing `.` refuses: the flat-key bridge would silently re-split `a.b` into catalog `a`, prop `b.<prop>`. Database names carry no such bridge and are unrestricted. |
| `SourceSpec` visibility | `pub(crate)` in `config_file/sources.rs` with `#[allow(dead_code)]`, per ruling D-5 — the public API is frozen; step 3 decides what leaves the crate. |
| `SourceSpec` Debug | Manual, masking secret props through `redact_value` — the C1-SEC-002 pattern `CatalogSpec`'s Debug already sets; equality (`PartialEq`/`Eq`) stays on raw values. |
| Collision scope | Uniqueness is enforced over the union of both families per profile: cross-family and within-database collisions both refuse naming both key paths (two catalogs can only collide if TOML allowed duplicate headers, which it does not). |

Choices step 3 took that the card and the round brief did not fix (read from the code, not invented):

| Choice | What the code does |
|---|---|
| Dump source semantics | `builder` = the builder map carried the key (it wins); `file:<path>#<profile>` = the key survived from the SELECTED profile's table; `default` = the key survived from `[default]`. The three labels are exactly the three precedence levels — no engine-default rows are invented. |
| `REPARK_ENV` empty or absent | Both select `[default]` alone. An unknown non-empty `REPARK_ENV` refuses (step-1 behaviour, unchanged). |
| No file discovered | `REPARK_ENV` is ignored and the builder is untouched — a globally exported `REPARK_ENV` with no `repark.toml` anywhere is a no-op, never a refusal, so existing file-less `build()` calls cannot observe the new code path. |
| Session knobs travel typed | The file's `session` knobs set the builder's typed fields only when unset; they do not also enter the config map, so the pre-existing dual-knob refusal cannot fire on file content alone and the dump never shows a knob row the engine did not honour. The `repark.*` knob-key pairs still exist on `FileConfig.pairs` for the facade fold. |
| `display` / `conf` / catalog value shapes | Strings verbatim, integers stringified (`max_rows = 20` → `"20"`); every other TOML shape refuses naming the key path — the same no-silent-stringification rule step 2 set for catalog and database props. |
| Database sources refuse | A non-empty `[<profile>.database]` table refuses loud naming every offending source path and the `CFG-2` card: the sources parse and validate (collisions included) but registration is CFG-2's seam, and a silent drop would vanish configured sources. |
| `TimeTravelOpts` moves | `session.rs` needed 26 lines for the wiring inside the 1,000-line ceiling, so the 57-line `TimeTravelOpts` + `into_spec` moved verbatim to `time_travel.rs` next to the `TimeTravelSpec` it builds (the gate's sanctioned split at a cohesive boundary). Two-line delta from verbatim: the `Result` qualifies as `repark_common::Result` (the module's `Result` is DataFusion's) plus its import; `lib.rs` re-exports the same name from the new home. |
| Test battery split | `tests.rs` (40 pins) → `tests/mod.rs` + `tests/wiring.rs` (7 step-3 pins) when the battery passed the 1,000-line ceiling — stage pins versus wiring pins, no pin moved or edited. |
| Facade fold placement | The fold lives in `session_configuration.py` as `fold_config_file_into_builder` and calls only the public `Builder.config` — the one choke point from the brief. Absent-keys-only (builder wins regardless of call order); `repark.display.style` aliases compare case-insensitively, every other key exactly. The pairs arrive unredacted from the native `config_file_pairs` static because they feed `.config()` like any other pairs; `getAll` redacts on read exactly as for builder-set secrets. |
| Resolver move | `Builder._lookup_int` + the three `_resolve_*` knob resolvers moved to `session_configuration.py` (which already owns the key tuples) as config-dict functions, verbatim bodies. The sanctioned seam for the `session_core.py` ceiling: 2411 → 2306, ratcheted down in both baseline tables plus the CAP-1 mirror. `_resolve_display_style` stays on the Builder (pins call it directly). |
| Baseline DOWN, no approval needed | `crates/repark-python/src/session.rs` 1177 → 1128: the ruled `config_path` argument plus the `config_file_pairs` static are paid for by moving `drain_arrow_c_stream` (with its capsule-name constant and imports) verbatim to `arrow_export.rs` — the sibling module for exactly this, named in the audit. Move-only otherwise: the two `pyo3::types` method traits the prelude glob used to provide are now explicit imports. Same count in both baseline tables plus the CAP-1 mirror (this row replaces the raise recorded before the audit). |
| Clippy repairs (no behavior change) | The true clippy recipe (`-A clippy::disallowed_methods`; my first local run omitted it and showed thousands of test-target false errors on a clean tree) found three real step-3 findings: `build()` over the 100-line function ceiling → the file-merge plus knob-validation preamble extracted to the private `prepare_build_state` helper (validation order and outcomes unchanged); `flatten_conf_into`'s `prefix: String` → `&str`; the externally reachable `config_file_pairs` (core) and the `config_file_pairs` binding static each carry the compiler-mandated three-line `# Errors` doc — the two places the round's no-comment fence yields to a deny-by-default gate, recorded here instead of hidden behind an allow (the fence grep for this slice shows only these six lines plus three re-anchored doc pairs). |

Choices step 4 took that the card and the round brief did not fix (read from the code, not invented):

| Choice | What the code does |
|---|---|
| Mirror validates parse-level shapes only | `config.py` refuses unknown display/session keys, non-integer knobs, booleans, unknown database kinds, dotted catalog names, empty catalog blocks, and cross-family collisions — every refusal the loader makes at parse. Engine-build refusals (unknown profile at `REPARK_ENV`, missing `${VAR}`, the CFG-2 database gate) stay engine-side; the mirror renders them without comment and the loader refuses. |
| Booleans refuse before coercion | Pydantic lax mode would silently coerce `True` to `1`/`"True"`; the loader refuses every TOML boolean. `mode="before"` validators plus `StrictStr` on `CatalogBlock.type` refuse booleans first, so the mirror never renders a value the loader would refuse. |
| Dotted conf keys render bare | `"spark.sql.x"` renders as `spark.sql.x = "v"` (nested TOML, flattened back by the loader to the same key) rather than quoted — the same spelling the repo's own example writes. Catalog, database, and property keys keep strict bare-or-quoted rendering, where a dot would be structural. |
| `conf` is the flattened dotted form | The mirror takes `{"spark.sql.x": "v"}` and does not accept nested dicts — nested TOML tables flatten with dot joins at load, so both spellings reach the same key, and one form keeps the model honest. |
| Style is a string, not an enum | `DisplayConfig.style` accepts any string; the session validates the `spark` / `polars` / `duckdb` vocabulary at build. The mirror refuses shapes, never engine vocabularies it does not own. |
| No new enumerated surface | `repark/config.py` is not one of the example-coverage doors, so its public names move no count and owe no covering example — verified by the unchanged `923` pin, not by reading the gate. |
| Guide shows memory catalogs running | A `type = "glue"` block was measured attempting a live AWS connection at session build, so every runnable guide transcript uses `type = "memory"`; the database table appears in the complete file with its load refusal quoted verbatim. |

## Notes for the orchestrator

**Still OPEN:** steps 3–4 of Card CFG-1 (updated 2026-09-09, step 2: `sources.rs` and
`redact.rs` landed with their 15 pins — step 1's note that they were placeholders is
superseded). `session.rs` has no `from_config_file`; there is no Python mirror and
no `docs/guide/repark-toml.md`. For step 3: the dump's `source` column and the
`conf_dump()` shape are still unwired (`redact_value`/`redact_config` are the pieces to
call), and `profile_sources` expects the effective, interpolated `Profile` (via
`profile_from_table` on `effective_table`'s output) so the merge-then-interpolate order the
step-1 pins hold is preserved.

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
      evidence: Every ruled behaviour is pinned in both directions, not only the happy path — each of the three discovery sources wins in its own fixture and loses in another, an empty REPARK_CONFIG disables discovery with a local file present, a named-but-missing path refuses, the profile merge is asserted on three axes (overlay wins, base-only key survives, sibling joins), and each refusal pin asserts the key path in the message rather than only the failure. Step 2 (2026-09-09) keeps the discipline — the catalog byte-identity is pinned against the measured block under both spellings, all three database kinds are exercised, and every refusal names its key path.
      artifacts: [crates/repark-core/src/config_file/tests.rs, crates/repark-core/src/config_file/sources.rs]
    - id: AT-2
      status: N/A
      justification: No numeric or performance claim is made; step 1 is a parser and a lookup order, and step 2's bridge and redaction add none.
    - id: AT-3
      status: ATTACKED
      evidence: The failure paths ARE the subject — a missing file, a missing variable, an unknown key at three depths, a non-table where a table is required, an absent profile named by REPARK_ENV — and each is pinned on its message content, not just its Err-ness. Step 2 (2026-09-09) adds the same class of pins for the source families — unknown kind, collision across families, non-string prop, non-table slot, empty catalog block, dotted catalog name — each asserting its key path.
      artifacts: [crates/repark-core/src/config_file/tests.rs, crates/repark-core/src/config_file/sources.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Ordering is pinned where it is load-bearing: discovery is first-hit-wins across three sources, and the merge-then-interpolate order is pinned by the test that a missing variable in an UNSELECTED profile does not refuse. No shared state exists — discovery takes its environment as a parameter, so no pin mutates the process environment and the suite is parallel-safe. Step 2 adds no state: profile_sources reads a Profile and returns specs.
      artifacts: [crates/repark-core/src/config_file/discovery.rs, crates/repark-core/src/config_file/tests.rs]
    - id: AT-5
      status: ATTACKED
      evidence: The one privileged-ish action is reading a path from the environment, and it is bounded — a path named by REPARK_CONFIG that does not exist refuses instead of falling through to a different file, so a stale variable cannot silently load the wrong configuration. No secret is read, printed or logged in this step. Step 2 (2026-09-09) makes redaction real — redact_config over prop_key_is_secret masks the C1-SEC-002 key matrix (pinned: an unmasked secret never appears in the redacted output), and SourceSpec's Debug masks through the same function, so connection credentials do not leak through Debug either.
      artifacts: [crates/repark-core/src/config_file/discovery.rs, crates/repark-core/src/config_file/redact.rs, crates/repark-core/src/config_file/sources.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The hole a config loader opens is silent acceptance, and it is closed at every level — serde's deny_unknown_fields at the document root plus typed allowlists for the display and session tables, each refusing with the full key path; a value in the wrong shape refuses rather than being ignored. Step 2 (2026-09-09) closes the new holes the same way — an empty catalog block, a dotted catalog name that the flat-key bridge would silently re-split, a non-string prop that would be silently stringified elsewhere, and an unknown database spelling all refuse naming the key path.
      artifacts: [crates/repark-core/src/config_file/profile.rs, crates/repark-core/src/config_file/sources.rs]
    - id: AT-7
      status: N/A
      justification: No hot path and no resource behaviour; the loader reads one small file once at session construction, and is not wired into the builder until step 3.
    - id: AT-8
      status: ATTACKED
      evidence: The two new upstream dependencies are the orchestrator's seed commit, not the worker's, and are the versions the card names (serde 1.0.229, toml 0.8.23); Cargo.lock was refreshed by the build. The public surface is unchanged — the module is crate-private and its items carry allow(dead_code) precisely because nothing calls them until step 3. Step 2 (2026-09-09) honours the freeze — SourceSpec is pub(crate) per ruling D-5, and the only edit outside the family is the authorized one-word widening of prop_key_is_secret to pub(crate).
      artifacts: [Cargo.toml, crates/repark-core/Cargo.toml]
    - id: AT-9
      status: ATTACKED
      evidence: Diagnosability is the point of every refusal in this step: each message names the key path, and the missing-variable refusal names both the variable and the path it sat under. The pins assert those strings, so a wording regression is red. Step 2 (2026-09-09) follows suit — the collision refusal names BOTH colliding key paths, and the unknown-kind refusal lists the accepted spellings.
      artifacts: [crates/repark-core/src/config_file/interpolate.rs, crates/repark-core/src/config_file/sources.rs]
    - id: AT-10
      status: ATTACKED
      evidence: The red was re-established honestly after the implementing session was lost — the four implementation files stashed with the pins left in place, and the recorded red is the compile refusal (E0432 x3) that the seed's placeholders produce, pasted verbatim. No test was edited to manufacture it. Step 2 (2026-09-09) ran its 15 new pins against the untouched empty placeholders first — the E0432 x2 compile refusal is recorded verbatim below the step-1b red — and only then implemented.
      artifacts: [crates/repark-core/src/config_file/tests.rs]
  complete: true
```

