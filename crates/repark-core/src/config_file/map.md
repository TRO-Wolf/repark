# map — repark-core/src/config_file

## Purpose

The `repark.toml` loader's parts (CFG-1). The parent module `../config_file.rs` owns the public
entry (`load`) and the document type; each file here owns one stage of the ruled design in
[../../../../task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md](../../../../task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md)
"Card CFG-1 — the loader (Rust-owned)". See [../map.md](../map.md).

## Contents

Step 1 (CFG-1) landed `discovery.rs`, `profile.rs` and `interpolate.rs`; `sources.rs` and
`redact.rs` are still the seed's empty placeholders until step 2. The stages, in the order the
ruled design runs them:

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
- `sources.rs` — the `[catalogs]` / `[sources]` tables to `CatalogSpec` / `SourceSpec`, whose
  fixtures are `../catalog_config.rs`'s so the specs compare byte-identical. Still an empty
  placeholder. Step 2.
- `redact.rs` — the `conf_dump()` redaction and the `source` column (`builder`,
  `file:<path>#<profile>`, `default`). Still an empty placeholder. Step 2.
- `tests.rs` — all 25 pins in one file (the seed's three plus the discovery, merge and
  interpolation pins; step 1b flipped the two `$`-edge pins under R-14 and added the escape
  pin); the environment arrives as a stub closure, so no pin mutates the process environment.
  pins: cfg-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009

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
