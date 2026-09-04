# map — python/repark-parity/datasets

## Purpose

Torture-dataset **generators** (PROJECT.md validation roadmap, workstream 2). Generators
are checked in; data files are never committed. Writes go to
`$XDG_CACHE_HOME/repark-datasets/<family>` or `~/.cache/repark-datasets/<family>`, or
an explicit `--out` **outside** the repository.

This tree is **not** part of the hatch `repark_parity` package. Tests and CLIs load it as
`repark_datasets` via the bench sys.modules loader (`repark_tpch_bench` precedent). Do
not add it to hatch `packages`, Makefile `PYTHONPATH`, or `ci.yml`.

## Contents

- `_cache.py` — cache root, known family slugs, symlink refuse (incl. DANGLING symlinks:
  `is_symlink()` without an `exists()` pre-check, which would follow the link), in-repo
  write refuse.
- `__init__.py` — re-exports `KNOWN_FAMILIES`, `default_datasets_root`, `family_cache_dir`.
- `nested/` — nested / dynamicFlatten family (DS-1); see [nested/map.md](nested/map.md).
- `schema_inference/` — inference-conflict family (DS-2); see
  [schema_inference/map.md](schema_inference/map.md).
- `extreme_types/` — extreme-types family (DS-2); see
  [extreme_types/map.md](extreme_types/map.md).
- `secrets/` — credential-named-column fixture (DS-3); see
  [secrets/map.md](secrets/map.md).
- `smartcsv/` — messy-CSV torture family (DS-3); see
  [smartcsv/map.md](smartcsv/map.md).
- `map.md` — this file.

All five bound family slugs now exist. `nested` labels row classes; the other four
carry a `manifest.json` whose declared types are cross-checked against the real
Arrow schema by `tests/test_datasets_manifest_types.py` — edit a schema field and
its manifest row in the same change, or that test reds.

## I want to…

| I want to… | Go to |
|---|---|
| Generate the nested family (in-memory) | `nested/datagen.py` `small(rows=64, seed=42)` |
| Generate nested files | `python python/repark-parity/datasets/nested/datagen.py --rows N --seed S --out DIR` |
| Generate the dynamicFlatten measurement bed | `python python/repark-parity/datasets/nested/bed.py --scale gate --out /tmp/oc-dynflatten-bed` |
| Generate schema-inference files | `python python/repark-parity/datasets/schema_inference/datagen.py --rows N --seed S --out DIR` |
| Generate extreme-types files | `python python/repark-parity/datasets/extreme_types/datagen.py --rows N --seed S --out DIR` |
| Generate the secrets fixture | `python python/repark-parity/datasets/secrets/datagen.py --rows N --seed S --out DIR` |
| Generate the smartCsv torture corpus (4 delimiter schemes) | `python python/repark-parity/datasets/smartcsv/datagen.py --rows N --seed S --out DIR` |
| Read the cache contract | `_cache.py` |
| Generator tests | [../tests/map.md](../tests/map.md) `test_datasets_*.py` |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../../docs/testing.md](../../../docs/testing.md)

## Debug

| Symptom | First check |
|---|---|
| `ModuleNotFoundError: repark_datasets` | Load via the sys.modules helper in `tests/test_datasets_nested.py`; do not expect hatch/site-packages |
| Wrote data into the checkout | `_cache.refuse_repository_output` — pass `--out` outside the repo or omit it (cache root) |
| Symlink refused | Cache root / out dir / data file must be real directories/files, not symlinks |
| `make py-test` cannot import `repark` | Expected — this tree's tests are pyarrow-only |
| Manifest type does not match the schema | `tests/test_datasets_manifest_types.py` names the family and class id |

## Constraints

- Zero new Python dependencies (pyarrow + stdlib).
- Facade / engine pins land in DS-4 (c17 explode fix is on main; no DS-5 rider).
- Never cite planning/ paths in this tree.
