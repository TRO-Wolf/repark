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

## 6. DS-4 delivered (final increment)

**Executor (DS-4):** Claude (claude-opus-5) · **Branch:** `opus/c18-ds4-facade-pins` ·
**Base:** origin/main with all five families landed (#161). Same charter, same fences.

Engine-facing pins for all five families in `python/repark/tests/test_datasets_facade.py`,
plus the tour notebook in a new top-level `examples/` tree. Every family is generated at
the A9 scale (64 rows, seed 42) into `tmp_path` and read back through the facade; the
generator table is the oracle, so no expectation is hand-typed.

The capitalized-explode / dynamicFlatten SUCCESS pins held from DS-1 (addendum A8) land
here. #154 is on main and was **content**-verified, not merge-state-verified:
`_bound_generator_array` exists in `column.py` and rebinds a single-ident generator
source through the frame schema, and its own pins live in `test_explode_rewrite.py`.

### Proposition ledger (DS-4)

| ID | Proposition | Verdict |
|---|---|---|
| C-031 | Facade pins exist for all five families at seeded `small()` scale; the 1M CLI default never runs in CI. | PROVEN — `ROWS = 64` / `SEED = 42` are the only scales in `test_datasets_facade.py` |
| C-032 | The facade env imports the datasets tree by path (bench loader), with no hatch/Makefile/PYTHONPATH edit. | PROVEN — `_load_datasets()` mirrors `test_tpch_smoke.py`; no packaging change in the diff |
| C-033 | Expected values come from the generator (`small()`), never from hand-typed literals. | PROVEN — every family's pin compares against `_<family>_truth()` |
| C-034 | A8 held pin: string-form `F.explode('Legs')` on a capitalized nested column succeeds through the parquet read path (value AND Arrow type). | PROVEN — `test_nested_explode_string_form_capitalized_legs` |
| C-035 | The same field resolves through the casefold, `F.col` and getitem spellings; `explode_outer` keeps the null/empty-list rows on the scalar-element lists while plain `explode` drops them. | PROVEN — `test_nested_explode_casefold_and_column_forms_agree`, `test_nested_explode_outer_keeps_null_and_empty_list_rows` (`Tags` 75+26, `Scores` 90+21) |
| C-035b | BUG-CANDIDATE: `explode_outer` refuses on `array<struct>` where plain `explode` succeeds on the same column. Reported, not fixed. | PROVEN — `test_nested_explode_outer_on_array_of_struct_refuses_loud` (both the refusal needle and the plain-explode contrast) |
| C-036 | A8 held pin: `dynamicFlatten` flattens struct columns with parent-path prefixes, in place, dropping the `array<void>` column — read through parquet (not dict rows, which infer as maps). | PROVEN — `test_nested_dynamic_flatten_unnests_struct_columns`, `test_nested_dynamic_flatten_full_depth_column_order` |
| C-036b | BUG-CANDIDATE: `count()` on the full-depth flatten plan reds inside `push_down_leaf_projections` while `to_arrow()` on the same plan returns the correct rows; one explode pass counts fine. Reported, not fixed. | PROVEN — `test_nested_dynamic_flatten_count_action_refuses_loud` (export-path count, the refusal needle, and the two narrow controls) |
| C-037 | POLICY (A5): an under-sampled smartCsv read misses the int32→int64 widening that sits past the cap; the full scan sees it. Pinned, not fixed. | PROVEN — `test_schema_inference_sampling_miss_is_policy` (capped `int32` vs full-scan `int64`, with the file's own values as the control) |
| C-037b | POLICY: the under-widened column then fails LOUD on materialisation (`Cannot cast string '2147483648' to value of Int32 type`) instead of truncating or nulling, and the widened read materialises the same column cleanly as int64. | PROVEN — `test_schema_inference_undersampled_cast_refuses_loud` (both halves) |
| C-038 | POLICY: zero-padded identifier strings resolve to `int32` (the padding is lost), and the recognized null-token spellings dominate `empty_or_null`. | PROVEN — `test_schema_inference_labeled_classes_resolve_as_documented` |
| C-039 | `decimal128(24,21)` round-trips through parquet exactly; the same file's >38-digit column is POLICY-demoted to float64 on the CSV ladder. | PROVEN — `test_extreme_types_decimal_round_trips_through_parquet`, `test_extreme_types_beyond_38_digits_demote_to_float64` |
| C-040 | Secrets reads behave NORMALLY: values pass through verbatim, nothing is masked, camelCase headers survive both doors. Flagging of secret-shaped DATA columns is a roadmap feature this fixture predates. | PROVEN — `test_secrets_parquet_read_is_unredacted`, `test_secrets_smart_csv_keeps_camel_case_headers` (stated in the module and test docstrings) |
| C-041 | Each delimiter scheme reads through the facade smart-CSV path with the delimiter declared: BOM stripped, preamble skipped, duplicate header deduped, ragged rows padded into a synthesized overflow column. | PROVEN — `test_smartcsv_delimiter_zoo_reads_every_scheme`, `test_smartcsv_ragged_rows_pad_and_overflow_column` |
| C-042 | Null-token, bool-spelling, embedded-delimiter, duplicate-header and ragged-overflow classes match the generator's typed truth cell for cell; the decimal-width union materialises as `decimal128(15,5)` with exact values. | PROVEN — `test_smartcsv_null_tokens_and_bool_spellings`, `test_smartcsv_decimal_widths_materialize` |
| C-043 | BUG-CANDIDATE / known-limit: delimiter AUTO-detect picks a rival delimiter on the embedded-delimiter corpus and eats one data row as the header. B4 rounds 1–3 tried to close this and each regressed a named counterexample; round 4 (#175) descoped detect back to origin/main and documented `sep=` as the remedy. | PROVEN — `test_smartcsv_delimiter_autodetect_picks_a_rival_delimiter` (both halves: the miss, and the correct read with `sep` declared) |
| C-043b | BUG-CANDIDATE: a euro-comma column infers `decimal128(5,2)` and the cast then refuses the raw comma text, so a whole-frame read of either corpus carrying the class raises. Reported, not fixed. | PROVEN — `test_smartcsv_euro_comma_decimal_cast_refuses_loud` (both `smartcsv` and `schema_inference`, with the resolved type asserted first so the broken promise is visible) |
| C-044 | The tour notebook runs all five families end to end from the cache root at 2 000 rows, and is committed with outputs cleared. | PROVEN — `examples/notebooks/datasets_tour.ipynb` executed headless before commit; `outputs: []` in the committed file |
| C-045 | `datasets/` generators and `_cache.py` untouched; `python/repark/src/` untouched; zero new dependencies; `uv.lock` / `Cargo.lock` / `.github/` untouched. | PROVEN — diff names |
| C-046 | `map.md` lockstep for every touched directory + this ledger in the same commit; `repo-manifest.toml` needs no row (its `[documentation]` index is the spine documents and its components are Cargo members — neither claim covers `examples/`). | PROVEN — the Files subsection below |

### Findings reported (not fixed)

Six, all found by reading the generated corpora through the facade: four marked
BUG-CANDIDATE and two POLICY confirmations. Finding 2 (delimiter auto-detect)
stays a known-limit after B4 (#175) round 4 descope; the other five stay as
this lane left them. Every one is pinned so a later fix reds the pin instead of
landing silently.

1. **A euro-comma decimal column infers `decimal128` and then refuses its own cast
   (BUG-CANDIDATE).** The ladder normalizes `760,35` to a fixed-point value and resolves
   the column to `decimal128(5,2)`, but the generated cast is handed the **raw** cell text
   and the engine refuses: `Arrow error: Cast error: Cannot cast string '760,35' to value
   of Decimal128(38, 10) type` (message quoted verbatim; the lane did not chase why the
   reported target type is the engine default rather than the resolved `(5,2)`). A
   whole-frame read of either corpus carrying the class raises, so the value pins around
   it project the columns they are about. The refusal is honest — no silent corruption —
   but the resolved type promises a value the read cannot deliver. No existing suite
   covered euro commas end to end; the protocol-level rung pin in `test_t4_csv_smart.py`
   stops before the cast.

2. **Delimiter auto-detect loses to an embedded rival delimiter (BUG-CANDIDATE /
   known-limit after B4 #175 round 4).** `detect_delimiter` scores candidates
   by field-count agreement, and `csv.reader` treats a quote that does not START
   a field as literal text. Three detect redesigns each closed the named corpus
   and regressed an unnamed one (including declared-sep value corruption).
   Detect and parse reverted to origin/main. Declaring `sep` (European-locale:
   `sep=';'`) reads the file correctly. The pin asserts the documented miss
   plus the declared-sep control.
3. **`explode_outer` refuses on `array<struct>` where `explode` succeeds (BUG-CANDIDATE).**
   `explode_outer` needs an SQL element type for its null/empty guard and has no spelling
   for a struct element:
   `explode_outer cannot resolve SQL element type for array column '…' (engine type
   'array<struct<leg_id:bigint,…>>'); cast the array or use a supported element type`.
   Plain `explode` on the same column unnests fine. The null/empty-keep behavior is
   therefore pinned on the scalar-element lists (`Tags`, `Scores`), and the asymmetry is
   pinned as its own test.

4. **`count()` on the full-depth flatten plan reds in the optimizer (BUG-CANDIDATE).**
   `frame.dynamicFlatten().to_arrow()` returns the right 140 rows, but `.count()` on the
   same plan raises `Optimizer rule 'push_down_leaf_projections' failed … Schema contains
   qualified field name <explode-alias>."Legs" and unqualified field name "Legs" which
   would be ambiguous`. The multi-pass explode is what triggers it: one explode pass, and
   the shallow one-pass flatten, both count fine on the same corpus. Found by executing
   the tour notebook, which now counts through the export path and says why.

5. **Zero-padded identifiers lose their padding to inference (POLICY).** Documented ladder
   behavior — `000000` is an integer token — but it is exactly the "leading-zero ids"
   torture class, so it is pinned rather than left to be discovered downstream.

6. **The under-sampled cast fails loud (POLICY, working as documented).** With
   `samplingRows` below the conflict row the column resolves `int32` and materialisation
   refuses with `Cannot cast string '2147483648' to value of Int32 type`. That is the
   contract the smartCsv docstring states ("the subsequent cast fails loud rather than
   corrupting"); DS-4 is the first pin of it end to end.

### Files (DS-4)

New: `python/repark/tests/test_datasets_facade.py`, `examples/map.md`,
`examples/notebooks/map.md`, `examples/notebooks/datasets_tour.ipynb`.

Edited: `python/repark/tests/map.md`, root `map.md` (the new `examples/` row), `README.md`
(one pointer sentence), `.gitignore` (`.ipynb_checkpoints/`), `task/map.md`, this ledger.

Untouched by fence: `python/repark-parity/datasets/**` (frozen generators + `_cache.py`),
`python/repark/src/**`, `uv.lock`, `Cargo.lock`, `.github/**`, `repo-manifest.toml`,
`test_metadata_tables.py`, `test_partition_value_audit.py`, `test_dogfood_gaps.py`,
`test_t4_csv_smart.py`.
