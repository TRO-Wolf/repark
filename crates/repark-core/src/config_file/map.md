# map — repark-core/src/config_file

## Purpose

The `repark.toml` loader's parts (CFG-1). The parent module `../config_file.rs` owns the public
entry (`load`) and the document type; each file here owns one stage of the ruled design in
[../../../../task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md](../../../../task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md)
"Card CFG-1 — the loader (Rust-owned)". See [../map.md](../map.md).

## Contents

Step 1 (CFG-1) landed `discovery.rs`, `profile.rs` and `interpolate.rs`; step 2 (2026-09-09)
landed `sources.rs` and `redact.rs`. The stages, in the order the ruled design runs them:

- `discovery.rs` — `discover(environment, current_directory, home)`: `$REPARK_CONFIG` →
  `./repark.toml` → `~/.config/repark/repark.toml`, first hit wins; absent everywhere is
  `Ok(None)`; `REPARK_CONFIG` set but empty disables discovery; a named-but-missing path
  refuses naming it. Step 1.
  pins: cfg-1/C-001, C-002, C-003
- `profile.rs` — `profile_from_table` (typed `display` / `session` allowlists, free-form
  `conf` / `catalog` / `database`, unknown keys refuse with the `name.table.key` path) and
  `effective_table` (`[default]` deep-merged under the named profile; `None` selects default
  alone; an unknown `REPARK_ENV` profile refuses naming it and the known list). Nothing here
  reads the process `REPARK_ENV`; step 3 passes it in. Step 1.
  pins: cfg-1/C-004, C-005
- `interpolate.rs` — `${VAR}` expansion over the effective table's strings (nested tables and
  arrays included, scalars untouched); a missing variable refuses naming the key path and the
  variable; `$` without `{` stays verbatim; `$$` escapes a literal `$`, so `$${NAME}` renders
  `${NAME}` with no environment lookup; an unterminated `${` refuses naming the key path.
  Step 1; the two `$`-edge behaviours flipped by ruling R-14 in step 1b.
  pins: cfg-1/C-006, C-007, C-008, C-009
- `sources.rs` — `profile_sources(profile_name, profile)` → `ProfileSources` (`catalogs` +
  `sources`). The catalog family joins each `[<profile>.catalog.<name>]` block into
  `repark.sql.catalog.<name>.<prop>` keys and reuses `../../catalog_config.rs`'s
  `parse_catalog_specs`, so Java-class keys (`catalog-impl = "…GlueCatalog"`), the native
  `type` spellings and every kind/prop refusal behave exactly as the equivalent `.config()`
  calls — the specs compare byte-identical to the flat path's, pinned against the measured
  Glue block. A `type = "rest"` value refuses loud with the engine's unrecognized-value error
  until `CatalogKind::Rest` arrives (the REST-catalog card owns that variant). The database
  family parses `[<profile>.database.<kind>.<name>]` into the crate-private `SourceSpec`
  (`postgres | sqlserver | trino` exact spellings, `BTreeMap<String, String>` props, a manual
  `Debug` that masks through `redact_value`). Names must be unique per profile across both
  families; a collision refuses naming both key paths. Wrong shapes refuse naming the key
  path: a non-string prop, a non-table slot, an empty catalog block, and a dotted catalog
  name that would silently re-split through the flat-key bridge. Step 2.
  pins: cfg-1/C-010, C-011, C-012, C-013, C-014, C-016, C-017
- `redact.rs` — `redact_value` / `redact_config` over `../../catalog_config.rs`'s
  `prop_key_is_secret` (widened to `pub(crate)` this step, the one authorized edit outside
  the family; the predicate is not re-implemented). The `***` mask matches the `CatalogSpec`
  `Debug` placeholder; `SourceSpec`'s `Debug` masks through the same function, so a secret
  value never reaches `Debug`/dump output. Step 3 wires `redact_value` into the dump's
  value column. Step 2.
  pins: cfg-1/C-015
- `wiring.rs` — step-3 builder seam. `load_file_config(forced, environment, current_directory,
  home)` (forced path refuses naming it when absent, else automatic discovery; empty
  `REPARK_CONFIG` disables, empty `REPARK_ENV` selects default) then `REPARK_ENV` profile,
  merge, interpolation, and translation of the effective table into flat pairs:
  `display.*` → `repark.display.*` (string or integer, else refuses), `session` knobs →
  typed fallbacks plus `repark.*` knob-key pairs, `conf` flattened with dot joins
  (TOML nests dotted keys; a quoted-plus-nested collision refuses) in sorted-key order
  (the `toml::Table` here is `BTreeMap`-backed, so file order is not recoverable — pinned
  as sorted, ledger C-024 carries the D-1 wording), `catalog` blocks → the same
  `repark.sql.catalog.*` keys `parse_catalog_specs` reads. `profile_sources` still runs on
  every load (collision and shape refusals stay single-implementation); a non-empty
  `[<profile>.database]` table refuses loud until CFG-2 registers named sources. `KeyOrigin`
  (`Profile` / `Default`) records which table each pair survived from; `conf_dump_rows`
  renders `(key, redacted value, source)` with `builder`, `file:<path>#<profile>`, or
  `default` — the three precedence levels, nothing else. `load_for_build` is the thin
  process-environment wrapper `session.rs` calls. Step 3.
  pins: cfg-1/C-018, C-019, C-020, C-021, C-022, C-023, C-025
- `maintenance.rs` — `MaintenancePolicy` (the six D-1 profile-level keys plus the
  `tables` map of per-table `TablePolicy` entries) with `from_table` (unknown keys refuse
  naming the `name.maintenance.key` path; `adaptive_partitioning` refuses as not yet
  supported at both levels, reserving the name for ADAPT-PART), `parse_duration` (the one
  D-2 `<n>d | <n>h | <n>m` parser; anything else refuses naming the key), and `resolve`
  (per-table entry wins key by key, unset keys fall back to the profile values; step 2's
  entry point, `#[allow(dead_code)]` until the procedure calls it). `profile.rs`
  validates eagerly through `from_table` but still stores the raw table on
  `Profile.maintenance`, so both `parse()` and the typed API refuse; `profile_as_table`
  carries the slot so the `REPARK_ENV` merge keeps it. Step 1 of MAINT-POLICY-1.
  pins: maint-policy-1/C-001, C-002, C-003, C-004, C-005, C-006
- `tests/` — `mod.rs` keeps the 40 stage pins untouched (the seed's three, step-1
  discovery/merge/interpolation, step-1b `$`-edge flips, step 2's catalog/database/redaction
  pins); `wiring.rs` carries the 8 step-3 pins. Split from the single `tests.rs` when the
  battery passed the 1,000-line file ceiling — stage pins versus wiring pins, no pin moved
  or edited in the split. The environment arrives as a stub closure throughout, so no pin
  mutates the process environment (build-level pins use forced temp paths and assume the
  ambient `REPARK_ENV` is unset, the same class of assumption as the seed's no-file pin).
  pins: cfg-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011,
  C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-020, C-021, C-022, C-023, C-025

## Pointers

- Up: [../map.md](../map.md)
- The card: [../../../../task/roadmap/mid-term/cheap-tier-slate-2026-09-08.md](../../../../task/roadmap/mid-term/cheap-tier-slate-2026-09-08.md) "Card CFG-1"
- The builder that will consume the loaded document: `../session.rs`.

## Debug

| Symptom | First check |
|---|---|
| A `repark.toml` key is silently ignored | `ConfigFile` carries `#[serde(deny_unknown_fields)]`, so an unknown key refuses rather than passing. A key that IS in the type but does nothing means its stage file is still the seed's placeholder. |
| Clippy asks to unwrap `load()`'s `Result` | The seed carries `#[allow(clippy::unnecessary_wraps)]`: the ruled signature is `pub fn load() -> Result<ConfigFile>`, and it cannot fail only because discovery, interpolation and the profile merge — every fallible stage — are still placeholders. Step 1 removes the allow, not the `Result`. |
| `load()` returns the empty config although a file exists | Discovery (`discovery.rs`) is not wired yet at the seed; `load()` answers the empty config by construction until step 1. |

First checks: `cargo test -p repark-core config_file`.
