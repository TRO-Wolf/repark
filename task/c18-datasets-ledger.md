# Unit ledger — conductor-18 torture-dataset workstream (DS-1..DS-4)

**Unit:** conductor-18 · **Date:** 2026-08-16 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-c18` · **Branch:** per increment (`grok/c18-ds2-schema-inference`) ·
**Base:** origin/main at release SSOT 0.3.2 (#157); DS-1 merged as #153

**Charter:** conductor-18 brief + 2026-08-16 Q&A addendum (A1–A9).
This ledger is the single file for the whole workstream (A4); later increments
append, they do not open a second ledger.

Engine / fork / dbt-repark / `python/repark/src/` are CLOSED. Found engine bugs
are reported, not fixed. Divergence-registry rows are orchestrator-side.

### Proposition ledger (DS-1)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Home is `python/repark-parity/datasets/<family>/`; import name `repark_datasets` via the bench loader. | PROVEN — loader in `test_datasets_nested.py`; hatch/Makefile/ci.yml untouched. |
| C-002 | Shared `_cache.py` uses XDG `repark-datasets/<family>` + symlink refuse + in-repo refuse. | PROVEN — `test_default_root_respects_xdg`, `test_refuse_symlink_cache`, `test_refuse_repository_output`. |
| C-003 | DS-1 creates only `datasets/` + `datasets/nested/` (slugs bound, no empty siblings). | PROVEN — tree listing. |
| C-004 | `small(rows=64, seed=42)` is the in-memory door; CLI default is 1_000_000. | PROVEN — `test_small_defaults_are_a9_64_and_42`; CLI `--rows` default. |
| C-005 | Same seed+rows ⇒ identical pyarrow tables (schema + values), not raw file bytes. | PROVEN — `test_small_same_seed_is_table_identical`; seed change moves values. |
| C-006 | File re-read (parquet + JSON-lines) matches `small()` under `SCHEMA`. | PROVEN — `test_write_and_reread_parquet_matches_small`. |
| C-007 | Nested shape: depth ≥ 6, capitalized `Legs`, mixed list types, null-typed lists, empty and null list rows. | PROVEN — `test_schema_has_required_nested_classes`, `test_small_emits_every_labeled_row_class`. |
| C-008 | Generator tests live in `python/repark-parity/tests/` (`make py-test`). Facade pins wait for DS-4/DS-5. | PROVEN — this increment adds no `python/repark/tests/` files. |
| C-009 | `map.md` lockstep + this ledger + `task/map.md` row in the same change. | PROVEN — listed in §2. |
| C-010 | Lockfiles, `.github/`, crates, facade src, PrimarySync untouched. | PROVEN — diff names. |

---

## 1. DS-1 delivered

- `python/repark-parity/datasets/_cache.py` + `datasets/nested/datagen.py`
- Generator tests: `python/repark-parity/tests/test_datasets_nested.py`
- Maps: `datasets/map.md`, `datasets/nested/map.md`, parent `map.md` rows

## 2. Files (DS-1)

See the increment diff. New directories: `python/repark-parity/datasets/`,
`python/repark-parity/datasets/nested/`.

## 3. DS-2 delivered

`schema_inference` + `extreme_types`. Each family has `datagen.py`, `manifest.json`
(tests read the file), `map.md`. CSV + parquet. CLI default `conflict_at=500_000`;
`small()` defaults `conflict_at=rows//2`. `_cache.py` not re-opened (dangling-symlink
amend from #153 stays). Facade pins still DS-4 (c17 explode fix is on main via #154;
DS-5 rider not required).

### Proposition ledger (DS-2)

| ID | Proposition | Verdict |
|---|---|---|
| C-011 | `schema_inference` emits every manifest class as a column. | PROVEN — `test_manifest_labels_every_class_and_column` |
| C-012 | `int_widens` is int32-range before `conflict_at` and > 2^31-1 after. | PROVEN — `test_int_widens_shifts_at_conflict_at` |
| C-013 | CLI default `conflict_at` is 500_000; `small()` defaults inside the row budget. | PROVEN — `DEFAULT_CONFLICT_AT` pin + `small()` default `rows//2` |
| C-014 | Parquet re-read matches `small()` (table identity, not file bytes). | PROVEN — both families' write/re-read tests |
| C-015 | `extreme_types` has decimal128(24,21), beyond-38 digit strings, uuid5, paragraph, HTML at example.com. | PROVEN — `test_decimal128_scale_and_beyond_38`, `test_uuid_paragraph_html_shapes` |
| C-016 | Tests read the checked-in `manifest.json` (not a hardcoded twin). | PROVEN — `load_manifest()` in both families |
| C-017 | No facade tests, no `_cache.py` edit, no lockfiles / `.github/` / crates. | PROVEN — diff names |

## 4. DS-3 delivered

**Executor (DS-3):** Claude (claude-opus-5) · **Branch:** `opus/c18-ds3-secrets-smartcsv` ·
**Base:** origin/main at the mimalloc allocator increment (#159). Same charter, same
fences; only the executor changed for this increment.

`secrets` + `smartcsv` — the last two bound family slugs. Each has `datagen.py`,
`manifest.json` (tests read the file), `map.md`. `_cache.py` not re-opened. No
facade tests; DS-4 still owns every engine-facing pin.

**secrets** — ten credential-named columns plus two negative controls, every value
prefixed `repark-fake-`. **smartcsv** — messy-CSV torture at generator scale:
parquet typed truth plus one CSV per delimiter scheme, with BOM, preamble, ragged
rows, duplicate headers, bool spellings, null-token variants and currency/decimal
width variants.

Two riders from the DS-2 review land here (both were non-live before this
increment): the manifest↔schema type cross-check, and the `leading_zero_id`
pad-width fix.

### Proposition ledger (DS-3)

| ID | Proposition | Verdict |
|---|---|---|
| C-018 | `secrets` carries the four charter columns plus a labeled credential-shaped set; no Hadoop/S3/JDBC conf-key spellings appear as columns. | PROVEN — `SECRET_COLUMNS` + `test_manifest_labels_every_class_and_column` |
| C-019 | Every secret value is obviously fake: `repark-fake-` prefix, no real credential shape, no `@`, no URL. | PROVEN — `test_every_secret_value_is_obviously_fake` (`FORBIDDEN_VALUE_PREFIXES` is the fence) |
| C-020 | Each secrets column is labeled with the needle class it stands for, and the label matches the column name under the `prop_key_is_secret` fold — derived in the test, never by importing repark. | PROVEN — `test_manifest_needle_labels_match_the_column_names` |
| C-021 | `bucket_key` is a negative control: it ends with `_key` yet the `bucket` carve-out excludes it. | PROVEN — `test_bucket_key_is_the_documented_carve_out` |
| C-022 | Secrets acceptance: reads behave NORMALLY today; the opt-in flagging mechanism is a roadmap feature this fixture predates. | PROVEN — stated in the `test_datasets_secrets.py` module docstring and `secrets/map.md`; no read behavior is asserted |
| C-023 | `smartcsv` labels every torture class with a scope, and `small()` at 64 rows emits every one: column classes in the table, file classes in the emitted CSV text. | PROVEN — `test_manifest_column_classes_are_all_emitted_by_small`, `test_manifest_file_classes_are_all_visible_at_small_scale` |
| C-024 | Delimiter zoo: one CSV per scheme (comma / semicolon / tab / pipe), each byte-equal to `render_csv` and each reconstructing the embedded-delimiter value. | PROVEN — `test_delimiter_zoo_emits_one_file_per_scheme` |
| C-025 | Ragged rows exist in both directions and short wins the tie (first collision row 137); short rows null the two trailing columns in typed truth. | PROVEN — `test_ragged_rows_null_the_trailing_columns` |
| C-026 | A3 determinism holds for both new families: same seed+rows ⇒ identical pyarrow tables from `small()` and from the parquet re-read; a seed change moves values. | PROVEN — both families' identity + seed-change + write/re-read tests |
| C-027 | RIDER a — every manifest-declared type matches the real Arrow field type across all four labeled families, after normalizing spacing and pyarrow's rendering aliases; both directions closed. | PROVEN — `test_datasets_manifest_types.py`; provoked RED by retyping one manifest row |
| C-028 | RIDER b — `leading_zero_id` keeps a leading zero at every emitted index: the pad width is derived from the requested row count (floor 6), pinned at the >1M boundary without generating 1M rows. | PROVEN — `test_leading_zero_width_is_derived_from_the_requested_rows`, `test_leading_zero_id_keeps_a_leading_zero_past_one_million`, `test_leading_zero_helper_matches_the_generated_column` |
| C-029 | Zero new dependencies; `_cache.py`, `uv.lock`, `Cargo.lock`, `.github/`, crates and facade src untouched; no generated data committed. | PROVEN — diff names |
| C-030 | `map.md` lockstep for every touched directory + this ledger appended in the same commit. | PROVEN — §5 |

### Charter deviation (called out)

Addendum **A6** bound the secrets column set to the four charter names plus
`password` / `session_token` / `accessKey`, with values shaped `repark-fake-…`
**or** `sk-repark-fake-…`. Two deliberate departures:

1. **No `sk-` prefixed values.** The DS-3 hygiene fence forbids any value that
   pattern-matches a real credential format, and `sk-` is exactly such a shape.
   Every value is `repark-fake-…`; `FORBIDDEN_VALUE_PREFIXES` pins the exclusion.
2. **Three extra columns** (`client_secret`, `private_key`, `credential_id`) so the
   labeled set covers the remaining needle classes in the `prop_key_is_secret`
   inventory. All three are ordinary column names, not Hadoop/S3/JDBC conf-key
   spellings, so the A6 prohibition is respected. Two negative controls (`id`,
   `bucket_key`) were added for the same reason — a needle set with no non-matches
   proves nothing.

## 5. Files (DS-3)

New: `datasets/secrets/{datagen.py,manifest.json,__init__.py,map.md}`,
`datasets/smartcsv/{datagen.py,manifest.json,__init__.py,map.md}`,
`tests/{test_datasets_secrets.py,test_datasets_smartcsv.py,test_datasets_manifest_types.py}`.

Edited: `datasets/map.md`, `datasets/schema_inference/{datagen.py,__init__.py,manifest.json,map.md}`,
`tests/test_datasets_schema_inference.py`, `tests/map.md`, this ledger.

## 6. Later increments

- DS-4 — facade-scale pins + `examples/notebooks/datasets_tour.ipynb` (capitalized-explode / dynamicFlatten success pins land here; #154 is on main). Both DS-3 families are ready for it: `secrets` needs a NORMAL-read pin (no redaction), `smartcsv` needs the four-scheme read plus the documented sampling / p>38 POLICY pins.
