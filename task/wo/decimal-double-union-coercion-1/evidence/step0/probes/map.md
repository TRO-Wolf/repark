# task/wo/decimal-double-union-coercion-1/evidence/step0/probes/ — the step-0 runners

Comment-free Python. The domain-table runners themselves are the sketch's, in [../../probes/](../../probes/map.md),
re-run unchanged; the commands are in [../map.md](../map.md).

- [dml.py](dml.py) — the DML cells: `SELECT`, `DELETE`, `UPDATE` and six `MERGE … ON` forms with `d = v`
  (and `<`, `<=>`, `IN`, `BETWEEN`) over three tables (verifier 2's flip rows at DECIMAL(10,2), the
  DECIMAL(38,18) `ROUND` rows, the signed-zero rows), copy-on-write and merge-on-read, on the Spark door, the
  native door or Spark; the flip `c1`–`c7` selects; two `NOT MATCHED BY SOURCE` controls.
- [coerce.py](coerce.py) — verifier 2's coercion fuzz, copied unchanged: 3,455 doubles × 54 shapes against six
  DECIMAL types (a shape that raises as a whole is one cell), which holds option B's scale-18 family (`d38_18`).
- [family.py](family.py) — verifier 2's structured `d = v` set (60,000 rows), recording the matching ids for
  `=`, `<`, `<=>` and `IN`.
- [fexpr_extra.py](fexpr_extra.py) — twelve `F.expr`, `selectExpr`, string-filter and DataFrame column cells with
  a decimal column or a decimal cast beside a double (inventory H2, H3).
- [coerce_widened.py](coerce_widened.py) — the same fuzz with every DECIMAL operand cast to DOUBLE by hand, the
  form seat E would produce: run on `ff34baf5` it emulates the head.
- [dec_to_double.py](dec_to_double.py) — RePark's DECIMAL→DOUBLE cast against Spark's over 5,455 plain-text values
  at five DECIMAL types (inventory H12).
- [split_roots.py](split_roots.py) — the scale-18 cells the emulated head gets wrong, recomputed with an exact
  DECIMAL→DOUBLE (through STRING) and against `ROUND(v, 18) = v`, to split their two roots.
- [analyze_widened.py](analyze_widened.py) — the emulated head against Spark, main and `ff34baf5`.
- [analyze_step0.py](analyze_step0.py) — joins Spark, main and `ff34baf5` per cell and writes `../cells/`.
- [render_step0.py](render_step0.py) — prints [../table.md](../table.md) from `../cells/`.

Up: [../map.md](../map.md).
