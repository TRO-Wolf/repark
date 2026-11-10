# Provenance: the after-tables

**Build.** The head leg is the release build of `adaa27494` (item 2), made with `maturin develop --release` from a
tree with no tracked change. `_native.abi3.so` sha256 `65adf5915890e623eb657ea9217d490f59316499d6c722e0affe4ede8276530e`. Interpreter:
the branch's own venv (CPython 3.12.3). Spark was not re-run: every Spark leg is the before-table's.

**Runs.** Each leg ran as a `repark.slice` unit, from a scratch copy of the committed probe, with a fresh warehouse:

    <venv>/bin/python drive_cmp.py --leg main --warehouse <warehouse-p1> --cells cells-p1.jsonl --out raw/head-p1.jsonl
    <venv>/bin/python pass2_cmp.py --leg main --mode cells --warehouse <warehouse-p2> --cells cells-p2.jsonl --out raw/head-p2.jsonl
    <venv>/bin/python variant_probe.py --leg base --only P10,P10dml
    <venv>/bin/python probe_cmp_variant.py --leg base

`cells-p1.jsonl` and `cells-p2.jsonl` split `../step0/cells.jsonl` into its pass 1 (4389 cells) and pass 2 (4 cells);
`step0/raw/head.jsonl` is the two outputs joined. The variant probes have no head slot for these plans, so the head
build ran in the probe's `base` slot. The records therefore carry `"leg": "base"`, and the rendered variant tables call
the head leg `base`.

**Reproducibility.** The same legs were measured on the uncommitted item-2 tree (run r4, the predecessor's) and again
here on the committed build (run r5). The step-0 records are byte-identical, and the two variant renders are
byte-identical. The variant records differ only in each cell's `seconds` field.

**Renders.**

- `step0/table.md` and `step0/groups.txt`: `../step0/render2.py`, run from a scratch copy holding `../step0/cells.jsonl`,
  `../step0/raw/spark.jsonl`, and `step0/raw/head.jsonl` as `raw/main.jsonl`. Two lines were then substituted: the
  title (`step 0 table` to `step 0 after-table (head)`), and the `main` column header to `head`.
- `variant-seat-c/table.md` and `variant-ext/table.md`: the probe's `--render`, run beside `raw/spark.jsonl` (the
  before-table's) and `raw/base.jsonl` (this directory's `raw/head.jsonl`). Neither file was edited after rendering.

**Moves.** `moves.md` comes from labelling each cell on both legs with the before-table's own labeller: `drive_cmp.label_of`
and `render2.derived_label` for step 0, and the rendered `label vs Spark` column for the variant tables.

**Scrubbing.** Neither the legs nor the renders hold an absolute path.
