# task/wo/signed-zero-normalize-1/evidence/part-b/after/ — part B part 1, the head re-measured

The step-0 and item-1 RePark legs re-run on the part 1 head's release build (`4f2108ae`), labelled against the same
Spark 4.1.2 legs, and compared cell by cell with the base (`276dc503`, item 1's re-measure). The record is
[after.md](after.md).

- [after.md](after.md) — the after-table: moves by label, every non-EQUAL head cell with its disposition, and the
  guard (d) and floatkeys results.
- [raw/](raw/map.md) — the head legs: `repark-4f2108ae-{reads,dml,floatkeys,spill,explain,addendum}.jsonl`.
- [labelled/](labelled/map.md), [new-cells/](new-cells/map.md), [addendum/](addendum/map.md) — the head legs labelled by
  `../step0/label_render.py` (`labels.jsonl`, `table.md`).
- [moves-step0.md](moves-step0.md), [moves-new-cells.md](moves-new-cells.md),
  [moves-addendum.md](moves-addendum.md) — each cell's label on the base and on the head; generated.
- [moves.py](moves.py) — writes a moves table from two `labels.jsonl` files.
- [facade_cells.py](facade_cells.py) — writes the facade pin's cell file
  (`python/repark/tests/sql/data/signed_zero_keys_1_cells.jsonl`) from the head read leg, Spark's view answers and
  both label sets.

- [scan_predicates.py](scan_predicates.py) — runs `EXPLAIN` on every step-0 read cell on both doors and keeps the
  `IcebergTableScan` lines (the pushed predicate); `scan-predicates-276dc503.jsonl` and `scan-predicates-head.jsonl` are
  its legs (guard (d) pin 1).

- [finish.md](finish.md) — the finish on `e710af4f`: the gate table, the two facade pins that moved and their
  dispositions, the `F.collect_set` halt and its resolution (owner, 2026-11-12), and clause item 4's one-wrapper proof.
- [confounded_wrappers.py](confounded_wrappers.py) — runs `EXPLAIN` on every answering confounded float cell and
  counts `__repark_normalize_zero__` per plan; `confounded-wrappers-head.jsonl` is its head leg (clause item 4).
- [zero_side.py](zero_side.py), [zero_order.py](zero_order.py) — the side probes behind the moved facade pins
  (`collect_set`, `countDistinct` over two columns, `dropDuplicates`, and `collect_set` by input order) on Spark and
  RePark; `zero-side-legs.jsonl` holds their legs.
- [schema_batch_repeat.py](schema_batch_repeat.py) — the 50-repeat `UPDATE-SCHEMA-BATCH-MISMATCH-1` probe for part 1:
  part A's `<=>` statement and a key statement (`IN (SELECT …)`) per kind; `schema-batch-repeat-276dc503.json` and
  `schema-batch-repeat-head-66fb7f13.json` are its base and head results.
- [measure.md](measure.md) — the mutation record (order §6) and the 50-repeat result (order §7) on `66fb7f13`, the
  literal-skip pin with the shapes tried on both doors (§1a), and the verifiers' note on P5's OB-4 order (§3);
  [mutations.jsonl](mutations.jsonl) holds the per-mutation records.

Up: [../map.md](../map.md).
