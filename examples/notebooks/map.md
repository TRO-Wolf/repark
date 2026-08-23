# map — examples/notebooks/

## Purpose

Jupyter notebooks that walk a reader through repark end to end. Each notebook is
executed by hand before it lands and is **committed with its outputs cleared**, so the
diff stays reviewable and the repository carries no rendered data. Execution gating
arrives with the examples-harness workstream; nothing here runs in CI today.

Notebooks **are** linted and format-checked: the pinned Ruff (0.15.22) carries `*.ipynb`
in its default `include`, so `make py-lint` / `make py-format-check` — and therefore
`make ci` — already cover every cell here. No `extend-include` entry was added, and none
is needed; a cell that fails Ruff fails the canonical gate.

## Contents

- `datasets_tour.ipynb` — the conductor-18 torture-dataset tour: all five generated
  families (`nested`, `schema_inference`, `extreme_types`, `secrets`, `smartcsv`)
  generated at 2 000 rows / seed 42 into the cache root, read back through the facade,
  each stopping on its signature behavior — capitalized `explode('Legs')` +
  `dynamicFlatten`, the inference sampling miss, the >38-digit decimal demotion,
  credential-named columns reading normally, and the messy-CSV delimiter zoo. Two
  reported findings are shown honestly rather than hidden: delimiter auto-detect picks a
  rival delimiter on the embedded-delimiter corpus (declare `sep=`; European-locale
  files use `sep=';'`), and the euro-comma column infers `decimal128` but refuses its
  cast — which is why the CSV cells declare `sep=` and project the columns they are
  about. A third finding shown there is now retired: the nested cell counted through the
  export path because `deep.count()` reded in `push_down_leaf_projections`; DEFECT-2
  (2026-08-18) fixed that rule for `Unnest` plans, so the cell counts both ways.
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Run the tour | `make develop`, then open `datasets_tour.ipynb` (or execute it headless, below) |
| Execute a notebook headless | `VIRTUAL_ENV=$PWD/.venv uv run --no-project --with nbclient --with ipykernel python -c "import nbformat; from nbclient import NotebookClient; nb = nbformat.read('examples/notebooks/datasets_tour.ipynb', as_version=4); NotebookClient(nb, timeout=1800, kernel_name='python3').execute()"` |
| See the pins behind the tour | [../../python/repark/tests/test_datasets_facade.py](../../python/repark/tests/test_datasets_facade.py) |
| Read the generators the tour drives | [../../python/repark-parity/datasets/map.md](../../python/repark-parity/datasets/map.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../task/c18-datasets-ledger.md](../../task/ledgers/archive/2026-08/2026-08-16-c18-datasets-ledger.md) — what
  each dataset increment delivered and reported.

## Debug

| Symptom | First check |
|---|---|
| `RuntimeError: run this notebook from inside a repark checkout` | The kernel's cwd is outside the repo; the setup cell walks up for `AGENTS.md` + `Cargo.toml` |
| A cell raises `ModuleNotFoundError: repark` | The kernel is not the environment `maturin develop` installed into |
| Outputs appear in `git status` | Clear them before committing — this tree commits cleared notebooks |
| `.ipynb_checkpoints/` appears | Ignored by `.gitignore`; never commit it |

## Constraints

- Cache-root data only, small-to-mid row counts (minutes, not hours).
- No credentials, no real hosts (`example.com` only), no absolute user paths in cells or
  outputs.
- Every behavior a notebook shows must already be pinned by a test; the notebook links
  the pin rather than becoming one.
