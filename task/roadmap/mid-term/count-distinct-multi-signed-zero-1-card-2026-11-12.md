# Card COUNT-DISTINCT-MULTI-SIGNED-ZERO-1: `countDistinct('a', 'b')` keeps signed-zero pairs apart where Spark counts them once

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`), under owner ruling 2 of 2026-11-12 ("Go with
the defaults on 1,2,4"). **Source:** SIGNED-ZERO-NORMALIZE-1 part B builder hand-back, 2026-11-12 (unresolved item 2).

**Status:** filed 2026-11-12, not dispatched. Severity **S2**: a wrong count, no error. Pre-existing: base equals head.

**Measured against Spark 4.1.2:**

- Input: the rows `(a, b)` = `(-0.0, 0.0)`, `(0.0, 0.0)` and `(-0.0, -0.0)`, both columns DOUBLE.
- `agg(F.countDistinct('a', 'b'))` answers **3** on main `276dc503` (part B's base) and on part B part 1's head
  `e710af4f` (release build).
- Spark answers **1**: it normalises `-0.0` to `0.0` in every distinct key, so all three rows are one pair.
- Part B part 1 does not move the cell.

**Cause (from the source):** a multi-argument distinct aggregate's arguments are not one of part B's seat-2 key positions,
and no clause cell covers them, so the float leaves are compared by bit pattern.

**Checked first:** [SCALAR-FLOAT-KEY-SIGNED-ZERO-NAN-1](scalar-float-key-signed-zero-nan-1-card-2026-11-02.md) covers a
scalar DOUBLE map-key lookup and RePark's general `=`. It is a different seat and does not name a distinct aggregate, so
this card is filed separately.

**Fix direction:** treat the arguments of a multi-argument distinct aggregate as seat-2 key positions, so each float leaf
is normalised as the single-argument form already is. Pin the three-row cell above with Spark's 1 on both doors.

**Close:** fixed with that pin, or declined by a recorded ruling.

Up: [map.md](map.md).
