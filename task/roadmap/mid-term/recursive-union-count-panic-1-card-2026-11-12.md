# Card RECURSIVE-UNION-COUNT-PANIC-1: `count(*)` over a recursive `UNION` CTE panics in DataFusion's `multi_group_by`

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** SIGNED-ZERO-NORMALIZE-1 part B
builder hand-back, 2026-11-12 (unresolved item 3, the predecessor's item 5, and §6 of part B's after-table).

**Status:** filed 2026-11-12, not dispatched. Severity **S2, provisional**. The source gives no severity. It reports a
Rust panic and does not say whether the facade's panic fence caught it, so S2 applies under the panic rule until a
measurement shows the fence turns it into a loud error with no table change; then it is S3. Pre-existing: base equals
head.

**Measured on main `276dc503` (part B's base) and on part B part 1's head `e710af4f`:**

- `WITH RECURSIVE r AS (… UNION …) SELECT count(*) FROM r` panics in DataFusion's `multi_group_by` with
  `index out of bounds: the len is 0`.
- The panic is the same with integer keys, so it is not a float-key defect, and the same with part B's key wrap
  switched off.
- `SELECT x FROM r` over the same CTE answers; only the `count(*)` form panics.
- The source does not record Spark's answer for the cell.

**Fix direction:** none in the source. The cell needs Spark's answer recorded first, then a guard or an upstream fix so
the shape answers or refuses without a panic.

**Close:** the shape answers Spark's count or refuses loud, pinned, or the card is declined by a recorded ruling.

Up: [map.md](map.md).
