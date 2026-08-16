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

## 4. Later increments

- DS-3 — `secrets` + `smartcsv`
- DS-4 — facade-scale pins + `examples/notebooks/datasets_tour.ipynb` (capitalized-explode / dynamicFlatten success pins land here; #154 is on main)
