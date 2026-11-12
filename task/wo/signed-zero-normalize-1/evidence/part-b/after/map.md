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

Up: [../map.md](../map.md).
