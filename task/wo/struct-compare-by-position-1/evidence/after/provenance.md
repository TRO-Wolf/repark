# Provenance: the after-tables

**Build.** The head leg is the release build of the item-6 head (run r6), made with `maturin develop --release`
from a tree with no uncommitted product change. That build holds:

- the `udf.rs` split;
- the `error_map` peel of this rule's refusal wrapper.

`_native.abi3.so` sha256 `bc3ad71f8a849e861fe6985e6b78f4391aeb5a4e541c1f5e1e932634fb4cabde`. Interpreter: the branch's
own venv (CPython 3.12.3). Spark was not re-run: every Spark leg is the before-table's.

**Runs.** Each leg ran as a `repark.slice` unit, from a scratch copy of the committed probe, with a fresh warehouse:

    <venv>/bin/python drive_cmp.py --leg main --warehouse <warehouse-p1> --cells cells-p1.jsonl --out raw/head-p1.jsonl
    <venv>/bin/python pass2_cmp.py --leg main --mode cells --warehouse <warehouse-p2> --cells cells-p2.jsonl --out raw/head-p2.jsonl
    <venv>/bin/python variant_probe.py --leg base --only P10,P10dml
    <venv>/bin/python probe_cmp_variant.py --leg base

`cells-p1.jsonl` and `cells-p2.jsonl` split `../step0/cells.jsonl` into its pass 1 (4389 cells) and pass 2 (4 cells);
`step0/raw/head.jsonl` is the two outputs joined. The variant probes have no head slot for these plans, so the head
build ran in the probe's `base` slot. The records therefore carry `"leg": "base"`, and the rendered variant tables call
the head leg `base`.

**Reproducibility.** Three runs give the same labels:

- r4: the predecessor's run, on the uncommitted item-2 tree;
- r5: on the release build of `adaa27494` (`_native.abi3.so` sha256 `65adf591…`);
- r6: on this build.

r5 reproduces r4 exactly: the step-0 records are byte-identical, and the two variant renders are byte-identical. r6
gives every cell the same label and the same Spark class as r5. The cells that differ are 1196 step-0 records, 21 ext
rows and 12 seat C rows. They differ only in this rule's refusal text, which now starts at the bracketed class: the
`struct_operands_by_position` / `caused by` / `Error during planning: ` wrapper is peeled. The committed legs are r6.

**Renders.**

- `step0/table.md` and `step0/groups.txt`: `../step0/render2.py`, run from a scratch copy holding `../step0/cells.jsonl`,
  `../step0/raw/spark.jsonl`, and `step0/raw/head.jsonl` as `raw/main.jsonl`. Two lines were then substituted: the
  title (`step 0 table` to `step 0 after-table (head)`), and the `main` column header to `head`.
- `variant-seat-c/table.md` and `variant-ext/table.md`: the probe's `--render`, run beside `raw/spark.jsonl` (the
  before-table's) and `raw/base.jsonl` (this directory's `raw/head.jsonl`). Neither file was edited after rendering.

**Moves.** `moves.md` comes from labelling each cell on both legs with the before-table's own labeller: `drive_cmp.label_of`
and `render2.derived_label` for step 0, and the rendered `label vs Spark` column for the variant tables.

**Scrubbing.** Neither the legs nor the renders hold an absolute path.
