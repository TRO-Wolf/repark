# map — task/ledgers/staging/using-per-side-keys-1-probes/

## Purpose

Step-0 oracle evidence for USING-PER-SIDE-KEYS-1: the probe that recorded the
acceptance grid and the two answer files it produced. The ledger that reads
this evidence is
[using-per-side-keys-1-ledger.md](../using-per-side-keys-1-ledger.md). No
product code lives here; the product rounds pin against these files.

## Contents

- [grid.py](grid.py) — the probe. Usage `grid.py <spark|repark> <out.json>`.
  One script drives both engines through the PySpark surface: `USING` joins of
  six types, aliased and not, with star, the unqualified key, per-side
  qualified keys, getitem and stale references in select, filter, sort and a
  join condition on the DataFrame door; the same on the SQL door over temp
  views, plus group-by; then mixed-type keys, two keys, chained operations,
  `NATURAL` joins and non-`USING` controls. One record per cell: the columns
  and rows (sorted, except sort cells), or the error class, condition and
  first line. Volatile ids in messages are masked. Ruff-clean; no comments by
  the project ban.
- [grid-spark.json](grid-spark.json) — the Spark leg: 1,090 cells on Spark
  4.1.2 (Zulu 17, `local[1]`), recorded 2026-10-07.
- [grid-main.json](grid-main.json) — the repark leg: the same 1,090 cells on
  `main` `3fbcb2ca` (repark 1.5.3), recorded 2026-10-07.
- [grid-head.json](grid-head.json) — the head leg: the same 1,090 cells on
  branch `fix/using-per-side-keys-1` after both doors were built, recorded
  2026-10-08.
- [corpus.py](corpus.py) — fold 1 (2026-10-08): records the differential
  corpus of `python/repark/tests/_using_corpus.py`. Usage
  `corpus.py <spark|repark> <out.json>`; the two recordings the pin reads are
  beside the test (`using_per_side_keys_1_corpus_spark.json`, Spark 4.1.2, and
  `…_main.json`, a `main` `3fbcb2ca` build). Ruff-clean; no comments.
- [mixed.py](mixed.py) — fold 1: the shown key's type and values for 14 pairs
  of key types in both orders on four join types (112 cells). Usage as above;
  the recordings are `using_per_side_keys_1_mixed_spark.json` and
  `…_mixed_main.json` beside the tests. Ruff-clean; no comments.
