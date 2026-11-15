# task/wo/decimal-double-union-coercion-1/evidence/step0b/probes/ — the step-0′ runners

Comment-free Python. The commands are in [../map.md](../map.md).

The runners:
- [coerce_head.py](coerce_head.py) is verifier 2's coercion fuzz in the form part 1 would plan, in three variants:
  - `seat_e` casts every DECIMAL operand to DOUBLE;
  - `k1` routes the written DOUBLE/FLOAT→DECIMAL casts through SQL text;
  - `k1k2` also routes the DECIMAL→DOUBLE casts through text.
  Its `k1` route is faithful on Spark but not on RePark, because of E-notation text ([../table.md](../table.md)
  section 6). So main's `k1` and `k1k2` use `coerce_head_k1.py`.
- [coerce_head_k1.py](coerce_head_k1.py) gives main's `k1` and `k1k2` heads. K1 is the double's JDK 17 text from
  main's own `CAST(… AS STRING)`, rounded half up at the scale in Python and fed back as exact digits. It also
  writes the route's kernel cells, which match Spark's direct cast on every value.
- [kernels.py](kernels.py) checks K1 (DOUBLE and FLOAT) and K2 against Spark at six DECIMAL types. Each is run
  directly and through text.
- [family_head.py](family_head.py) is the structured `d = v` set with K2 on the widened decimal.
- [dml_head.py](dml_head.py) is step 0's DML cells with every widened decimal through K2 and the flip casts through
  K1 and K2.
- [zero_fold.py](zero_fold.py) is the `zero_eq` shapes with `EXPLAIN VERBOSE`, to name the rule that folds them
  (critic item 7).

The analysis:
- [analyze_step0b.py](analyze_step0b.py) tags each joined cell with its part and its `6f9f2aaa` answer.
- [analyze_head.py](analyze_head.py) compares the emulated heads with Spark, main and `ff34baf5`.
- [render_step0b.py](render_step0b.py) prints [../table.md](../table.md).

Up: [../map.md](../map.md).
