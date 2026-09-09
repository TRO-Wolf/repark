# map — repark-core/src/config_file

## Purpose

The `repark.toml` loader's parts (CFG-1). The parent module `../config_file.rs` owns the public
entry (`load`) and the document type; each file here owns one stage of the ruled design in
[../../../../task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md](../../../../task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md)
"Card CFG-1 — the loader (Rust-owned)". See [../map.md](../map.md).

## Contents

At the CFG-1 seed commit (step 0) every stage file below is an empty placeholder: the family
exists so step 1 and step 2 add stages without touching the module wiring or the manifest, and
`load()` answers the empty config until discovery lands. The stages, in the order the ruled
design runs them:

- `discovery.rs` — which file is read: `$REPARK_CONFIG` → `./repark.toml` →
  `~/.config/repark/repark.toml`, first hit wins; `REPARK_CONFIG` set but empty disables
  discovery for one process (D-2). Step 1.
- `profile.rs` — `[default]` plus `[<profile>]` selected by `REPARK_ENV`, deep-merged, with the
  unknown-key path named in the refusal. Step 1.
- `interpolate.rs` — `${ENV_VAR}` expansion; a missing variable refuses loud and names the key
  path it sat under. Step 1.
- `sources.rs` — the `[catalogs]` / `[sources]` tables to `CatalogSpec` / `SourceSpec`, whose
  fixtures are `../catalog_config.rs`'s so the specs compare byte-identical. Step 2.
- `redact.rs` — the `conf_dump()` redaction and the `source` column (`builder`,
  `file:<path>#<profile>`, `default`). Step 2.
- `tests.rs` — the seed's three pins on the parent module: `load()` without a file is the empty
  config, an empty document parses to it, and an unknown top-level key refuses as a
  `repark config error` naming the key. The stage files carry their own tests as they land.

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
