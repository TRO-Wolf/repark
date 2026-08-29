# map — python/repark-parity/datasets/secrets

CC-2 slice complete: comments and docstrings condensed; oracle discriminators, pins, mutation payloads, and safety contracts kept byte-exact; history narration deleted.

## Purpose

Credential-named-column fixture. Ten secret-shaped column names (`apiKey`,
`api_key`, `api_token`, `access_token`, `password`, `session_token`, `accessKey`,
`client_secret`, `private_key`, `credential_id`) plus two negative controls
(`id`, `bucket_key`). `manifest.json` labels each column with the needle class it
stands for.

**Acceptance:** reads of this family behave **NORMALLY** today. Nothing redacts,
masks, warns about, or refuses a credential-named data column. The opt-in
secrets-flagging mechanism is a roadmap feature this fixture deliberately
predates; the fixture exists so the feature has a corpus when it lands. Facade
read pins are DS-4.

## Hygiene fence (hard)

Every synthetic value starts with the literal `repark-fake-` marker and must
never imitate a real credential format — no `AKIA…`, `ghp_…`, `sk-…`,
`xoxb-…` shapes, no `@`, no URLs. `FORBIDDEN_VALUE_PREFIXES` names the shapes
credential scanners hunt and the test asserts none of them appear. A fixture
that looks like a live key eventually gets reported as a leak.

## Contents

- `datagen.py` — `generate` / `small` / `write_files` / `read_parquet` /
  `fake_secret` / CLI (`--rows` default 1_000_000, `--seed` default 42, `--out`).
- `manifest.json` — class id → column, parquet type, needle, needle form,
  `secret` flag. Tests read this file.
- `__init__.py` — public door.
- `map.md` — this file.

## Classes

See `manifest.json`. The needle inventory is the facade's `prop_key_is_secret`
mirror, but this lane is pure pyarrow and does **not** import repark: the needles
are carried as manifest labels and the test re-derives the same fold (lowercase,
hyphen/dot → underscore, then underscores stripped for the compact form).

`session_token` is the one NULLABLE credential column (every 7th row is null) —
a null secret is still a secret-shaped column. `bucket_key` is the documented
`_key` carve-out: it ends with `_key` yet the `bucket` exclusion means it is not
secret, and its value is an object key rather than a credential.

## I want to…

| I want to… | Go to |
|---|---|
| Build 64 deterministic typed rows | `small(rows=64, seed=42)` |
| Write CSV + parquet | `write_files(...)` or the CLI |
| Add a column | add it to `SECRET_COLUMNS` (or the schema) **and** to `manifest.json` — the type cross-check test binds the two |

## Pointers

- Up: [../map.md](../map.md)
- Tests: [../../tests/map.md](../../tests/map.md) `test_datasets_secrets.py`,
  `test_datasets_manifest_types.py`

## Debug

| Symptom | First check |
|---|---|
| A value does not start with `repark-fake-` | `fake_secret` is the only value factory; never hand-write a literal |
| Secret-scanner alert on this tree | A forbidden prefix leaked in; `FORBIDDEN_VALUE_PREFIXES` is the fence |
| Manifest/schema type mismatch | `test_datasets_manifest_types.py` names the offending class id |
| Expected redaction on read | There is none today — that is the acceptance pin, not a bug |
