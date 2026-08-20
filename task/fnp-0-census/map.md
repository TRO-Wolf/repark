# map — task/fnp-0-census/

## Purpose

The measured evidence behind the Spark function parity campaign's approval gate
([../fnp-0-charter-ledger.md](../fnp-0-charter-ledger.md)). Produced 2026-08-20 by a nine-agent
read-only census; nothing here was hand-estimated. These files are **evidence, not status** — when
a number changes, the campaign re-runs the census and replaces the file; STATUS.md remains the
single source of truth for current state.

## Contents

- [facade-classification.json](facade-classification.json) — every facade function in
  `python/repark/src/repark/spark/functions*.py` (345 rows, 328 of them exported names) with its
  module, line, classification, wire names, `lit_indices`, executable body lines, and a per-row
  statement of how evaluation reaches Rust and what the Python body decides first. The
  classification vocabulary and the counts are in
  [../../docs/design/spark-function-parity.md §2.2](../../docs/design/spark-function-parity.md).
- [pyspark-gap.md](pyspark-gap.md) — the PySpark 4.1.2 → RePark gap: the exact 181 absent names,
  a 25-family partition (verified complete: no duplicates, no orphans), the unreachable set with
  a mechanism per name, a ten-tier implementation order, and the secondary finding that 35
  exported names raise unconditionally.
- [lambda-spec.md](lambda-spec.md) — the eleven Spark higher-order functions: signatures, lambda
  arities, return-type and null rules; the DataFusion 54.1 kernel inventory; the full required
  surface of `HigherOrderUDFImpl`; per-kernel line-count anchors; and a build/alias/rewrite verdict
  per function.
- [ownership-map.md](ownership-map.md) — provenance for all 435 callable spellings on the Spark
  door (REPARK_OWNED / DATAFUSION_SPARK / DATAFUSION_CORE), the 29 deliberate overwrite points,
  the line-count economics of `repark-functions`, and the two-door asymmetry finding that became
  charter clause C-012.

## Pointers

Up: [../map.md](../map.md) (the `task/` container).
Related: [../fnp-0-charter-ledger.md](../fnp-0-charter-ledger.md) (the gate this evidence serves),
[../../docs/design/spark-function-parity.md](../../docs/design/spark-function-parity.md) (the
design that consumes it),
[../../briefs/spark-function-parity.md](../../briefs/spark-function-parity.md) (the slate).

## Debug

- A number here disagrees with STATUS.md → STATUS.md wins; this directory is a dated measurement,
  not a live tracker. Re-run the census rather than editing a file here.
- A classification looks wrong for one function → the row carries `engine_path` and `python_work`
  in prose; check those against the source before treating the row as authoritative.
- The census needs re-running → it is a read-only fan-out over `functions*.py`,
  `crates/repark-python/src/column/`, `crates/repark-functions/src/`, the vendored
  `datafusion*-54.1.0` sources and PySpark 4.1.2's `__init__.py`. No build required.
