# Unit ledger — conductor-18 torture-dataset workstream (DS-1..DS-4)

**Unit:** conductor-18 · **Date:** 2026-08-16 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-c18` · **Branch:** `grok/c18-ds1-nested` (per increment) ·
**Base:** origin/main at release SSOT 0.3.1 (#152)

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

## 3. Later increments (append here)

- DS-2 — `schema_inference` + `extreme_types`
- DS-3 — `secrets` + `smartcsv`
- DS-4 — facade-scale pins + `examples/notebooks/datasets_tour.ipynb`
- DS-5 — capitalized-explode / dynamicFlatten success pins, only if conductor-17 is
  still unverified when DS-4 ships (A8)
