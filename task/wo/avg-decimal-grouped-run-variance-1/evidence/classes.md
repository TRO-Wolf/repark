# Seat A's kept divergence, cell for cell

Every `avg` / `try_avg` (query, ANSI mode) pair on `t`, `big` and `t4` where the exact answer (seat A, [model.py](model.py)) differs from Spark 4.1.2 at `local[1]` or `local[4]` ([verdicts/spark-vs-model.tsv](verdicts/spark-vs-model.tsv)). The over-precision files are left out: Spark refuses to read them.

| query | ANSI | exact (seat A) | Spark `local[1]` | class vs `local[1]` | Spark `local[4]` | class vs `local[4]` |
|---|---|---|---|---|---|---|
| `big/c/g/avg/all` | false | rows=3000 nulls=0 | rows=3000 nulls=3000 | EXACT_VALUE_SPARK_NULL | rows=3000 nulls=3000 | EXACT_VALUE_SPARK_NULL |
| `big/c/g/avg/all` | true | rows=3000 nulls=0 | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `big/c/g/try_avg/all` | false | rows=3000 nulls=0 | rows=3000 nulls=3000 | EXACT_VALUE_SPARK_NULL | rows=3000 nulls=3000 | EXACT_VALUE_SPARK_NULL |
| `big/c/g/try_avg/all` | true | rows=3000 nulls=0 | rows=3000 nulls=3000 | EXACT_VALUE_SPARK_NULL | rows=3000 nulls=3000 | EXACT_VALUE_SPARK_NULL |
| `big/c/u/avg/all` | false | 0.0167 | 0.0167 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `big/c/u/avg/all` | true | 0.0167 | 0.0167 | EQUAL | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `big/c/u/try_avg/all` | false | 0.0167 | 0.0167 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `big/c/u/try_avg/all` | true | 0.0167 | 0.0167 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/distinct/avg/g5` | true | ERR ARITHMETIC_OVERFLOW | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION | ERR ARITHMETIC_OVERFLOW | EQUAL |
| `t/c38_0/expr/avg/g1` | false | 1|0.5556 | 1|NULL | EXACT_VALUE_SPARK_NULL | 1|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/expr/avg/g1` | true | 1|0.5556 | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/expr/avg/g6` | false | 6|0.2 | 6|0.2 | EQUAL | 6|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/expr/avg/g6` | true | 6|0.2 | 6|0.2 | EQUAL | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/expr/avg/g7` | false | 7|0 | 7|NULL | EXACT_VALUE_SPARK_NULL | 7|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/expr/avg/g7` | true | 7|0 | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/expr/avg/g8` | false | 8|0.2727 | 8|NULL | EXACT_VALUE_SPARK_NULL | 8|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/expr/avg/g8` | true | 8|0.2727 | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/expr/avg/g9` | true | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR ARITHMETIC_OVERFLOW | BOTH_RAISE_OTHER_CONDITION | ERR ARITHMETIC_OVERFLOW | BOTH_RAISE_OTHER_CONDITION |
| `t/c38_0/filter/avg/all` | true | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR ARITHMETIC_OVERFLOW | BOTH_RAISE_OTHER_CONDITION | ERR NUMERIC_VALUE_OUT_OF_RANGE | EQUAL |
| `t/c38_0/g/avg/all` | false | [["0", "-0.01"], ["1", "0.5556"], ["10", "0"], ["11", "2.1667"], ["... | [["0", "-0.01"], ["1", null], ["10", "0"], ["11", "2.1667"], ["12",... | EXACT_VALUE_SPARK_NULL | [["0", "-0.01"], ["1", null], ["10", "0"], ["11", "2.1667"], ["12",... | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/avg/all` | true | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW | EQUAL | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION |
| `t/c38_0/g/avg/g1` | false | 1|0.5556 | 1|NULL | EXACT_VALUE_SPARK_NULL | 1|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/avg/g1` | true | 1|0.5556 | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/g/avg/g6` | false | 6|0.2 | 6|0.2 | EQUAL | 6|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/avg/g6` | true | 6|0.2 | 6|0.2 | EQUAL | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/g/avg/g7` | false | 7|0 | 7|NULL | EXACT_VALUE_SPARK_NULL | 7|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/avg/g7` | true | 7|0 | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/g/avg/g8` | false | 8|-0.2727 | 8|NULL | EXACT_VALUE_SPARK_NULL | 8|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/avg/g8` | true | 8|-0.2727 | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/g/avg/g9` | true | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR ARITHMETIC_OVERFLOW | BOTH_RAISE_OTHER_CONDITION | ERR ARITHMETIC_OVERFLOW | BOTH_RAISE_OTHER_CONDITION |
| `t/c38_0/g/try_avg/all` | false | [["0", "-0.01"], ["1", "0.5556"], ["10", "0"], ["11", "2.1667"], ["... | [["0", "-0.01"], ["1", null], ["10", "0"], ["11", "2.1667"], ["12",... | EXACT_VALUE_SPARK_NULL | [["0", "-0.01"], ["1", null], ["10", "0"], ["11", "2.1667"], ["12",... | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/try_avg/all` | true | [["0", "-0.01"], ["1", "0.5556"], ["10", "0"], ["11", "2.1667"], ["... | [["0", "-0.01"], ["1", null], ["10", "0"], ["11", "2.1667"], ["12",... | EXACT_VALUE_SPARK_NULL | [["0", "-0.01"], ["1", null], ["10", "0"], ["11", "2.1667"], ["12",... | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/try_avg/g1` | false | 1|0.5556 | 1|NULL | EXACT_VALUE_SPARK_NULL | 1|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/try_avg/g1` | true | 1|0.5556 | 1|NULL | EXACT_VALUE_SPARK_NULL | 1|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/try_avg/g6` | false | 6|0.2 | 6|0.2 | EQUAL | 6|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/try_avg/g6` | true | 6|0.2 | 6|0.2 | EQUAL | 6|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/try_avg/g7` | false | 7|0 | 7|NULL | EXACT_VALUE_SPARK_NULL | 7|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/try_avg/g7` | true | 7|0 | 7|NULL | EXACT_VALUE_SPARK_NULL | 7|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/try_avg/g8` | false | 8|-0.2727 | 8|NULL | EXACT_VALUE_SPARK_NULL | 8|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/g/try_avg/g8` | true | 8|-0.2727 | 8|NULL | EXACT_VALUE_SPARK_NULL | 8|NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/having/avg/all` | false | [["1", "0.5556"], ["11", "2.1667"], ["6", "0.2"]] | [["11", "2.1667"], ["6", "0.2"]] | EXACT_VALUE_SPARK_OTHER | 11|2.1667 | EXACT_VALUE_SPARK_OTHER |
| `t/c38_0/having/avg/all` | true | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW | EQUAL | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION |
| `t/c38_0/ice/avg/all` | false | stmt ok; [["0", "-0.01"], ["1", "0.5556"], ["10", "0"], ["11", "2.1667"], ["... | stmt ok; [["0", "-0.01"], ["1", null], ["10", "0"], ["11", "2.1667"], ["12",... | EXACT_VALUE_SPARK_NULL | stmt ok; [["0", "-0.01"], ["1", null], ["10", "0"], ["11", "2.1667"], ["12",... | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/ice/avg/all` | true | stmt ERR ARITHMETIC_OVERFLOW; ERR | stmt ERR ARITHMETIC_OVERFLOW; ERR | EQUAL | stmt ERR NUMERIC_VALUE_OUT_OF_RANGE; ERR | BOTH_RAISE_OTHER_CONDITION |
| `t/c38_0/rollup/avg/all` | false | [["0", "-0.01"], ["1", "0.5556"], ["10", "0"], ["11", "2.1667"], ["... | [["0", "-0.01"], ["1", null], ["10", "0"], ["11", "2.1667"], ["12",... | EXACT_VALUE_SPARK_NULL | [["0", "-0.01"], ["1", null], ["10", "0"], ["11", "2.1667"], ["12",... | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/avg/g1` | false | 0.5556 | 0.5556 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/avg/g1` | true | 0.5556 | 0.5556 | EQUAL | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/u/avg/g3` | true | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW | EQUAL | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION |
| `t/c38_0/u/avg/g5` | true | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW | EQUAL | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION |
| `t/c38_0/u/avg/g6` | false | 0.2 | 0.2 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/avg/g6` | true | 0.2 | 0.2 | EQUAL | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/u/avg/g7` | false | 0 | 0 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/avg/g7` | true | 0 | 0 | EQUAL | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/u/avg/g8` | false | -0.2727 | -0.2727 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/avg/g8` | true | -0.2727 | -0.2727 | EQUAL | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t/c38_0/u/avg/g9` | true | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR NUMERIC_VALUE_OUT_OF_RANGE | EQUAL | ERR ARITHMETIC_OVERFLOW | BOTH_RAISE_OTHER_CONDITION |
| `t/c38_0/u/try_avg/g1` | false | 0.5556 | 0.5556 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/try_avg/g1` | true | 0.5556 | 0.5556 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/try_avg/g6` | false | 0.2 | 0.2 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/try_avg/g6` | true | 0.2 | 0.2 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/try_avg/g7` | false | 0 | 0 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/try_avg/g7` | true | 0 | 0 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/try_avg/g8` | false | -0.2727 | -0.2727 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/u/try_avg/g8` | true | -0.2727 | -0.2727 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t/c38_0/win/avg/g2` | true | ERR ARITHMETIC_OVERFLOW | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION |
| `t/c38_0/win/avg/g3` | true | ERR ARITHMETIC_OVERFLOW | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION |
| `t/c38_0/win/avg/g5` | true | ERR ARITHMETIC_OVERFLOW | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION |
| `t4/c38_0/g/avg/g21` | true | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR ARITHMETIC_OVERFLOW | BOTH_RAISE_OTHER_CONDITION | ERR ARITHMETIC_OVERFLOW | BOTH_RAISE_OTHER_CONDITION |
| `t4/c38_0/g/avg/g23` | false | 23|1.6667 | 23|1.6667 | EQUAL | 23|NULL | EXACT_VALUE_SPARK_NULL |
| `t4/c38_0/g/avg/g23` | true | 23|1.6667 | 23|1.6667 | EQUAL | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t4/c38_0/g/avg/g24` | false | 24|1 | 24|NULL | EXACT_VALUE_SPARK_NULL | 24|1 | EQUAL |
| `t4/c38_0/g/avg/g24` | true | 24|1 | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES | 24|1 | EQUAL |
| `t4/c38_0/g/try_avg/g23` | false | 23|1.6667 | 23|1.6667 | EQUAL | 23|NULL | EXACT_VALUE_SPARK_NULL |
| `t4/c38_0/g/try_avg/g23` | true | 23|1.6667 | 23|1.6667 | EQUAL | 23|NULL | EXACT_VALUE_SPARK_NULL |
| `t4/c38_0/g/try_avg/g24` | false | 24|1 | 24|NULL | EXACT_VALUE_SPARK_NULL | 24|1 | EQUAL |
| `t4/c38_0/g/try_avg/g24` | true | 24|1 | 24|NULL | EXACT_VALUE_SPARK_NULL | 24|1 | EQUAL |
| `t4/c38_0/u/avg/g22` | true | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW | EQUAL | ERR NUMERIC_VALUE_OUT_OF_RANGE | BOTH_RAISE_OTHER_CONDITION |
| `t4/c38_0/u/avg/g23` | false | 1.6667 | 1.6667 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t4/c38_0/u/avg/g23` | true | 1.6667 | 1.6667 | EQUAL | ERR ARITHMETIC_OVERFLOW | EXACT_ANSWERS_SPARK_RAISES |
| `t4/c38_0/u/try_avg/g23` | false | 1.6667 | 1.6667 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |
| `t4/c38_0/u/try_avg/g23` | true | 1.6667 | 1.6667 | EQUAL | NULL | EXACT_VALUE_SPARK_NULL |

## Revision r1 (critic round 1): the classes after seat A's revision

The table above is step 0's, against step 0's seat A. Two changes follow from the critic's round 1
([raw-r1/critic-verdict.md](raw-r1/critic-verdict.md)), each measured in [raw-r1/](raw-r1/map.md):

- **The windows leave.** Seat A's window path now checks only the quotient, as Spark's window does (sketch §7,
  item 2). The three window pairs above (`t/c38_0/win/avg/g2`, `g3`, `g5`, ANSI on) become EQUAL: the revised seat A
  and Spark at both masters raise `NUMERIC_VALUE_OUT_OF_RANGE` ([raw-r1/spark.md](raw-r1/spark.md)). The other 78
  pairs stand as listed; the critic re-derived 56 of them from its own exact model and Spark runs.
- **A new class: the exact answer raises or is NULL where Spark answers.** Spark's ungrouped final merge never
  checks the sum under whole-stage codegen, and an `ObjectHashAggregate` never checks it at all. So at `local[N]`
  with N > 1, an ungrouped `avg` whose every partial total fits 38 digits answers the quotient even when the merged
  total does not. The exact model (seat A, Q7 (a)) raises `ARITHMETIC_OVERFLOW` (`avg`, ANSI on) or is NULL there.
  Step 0's tables never held such a total; [r1_tables.py](r1_tables.py) writes five:

| table | files × rows × value | exact total | average | Spark `local[1]` | Spark `local[4]`, codegen on | Spark `local[4]`, codegen off | seat A and main |
|---|---|---|---|---|---|---|---|
| `m_w34` (critic's) | 4 × 5,000 × `9e33`, `decimal(38,0)` | `1.8e38` | `9e33` | NULL / AO | **`9e33`** | NULL / AO | NULL / AO |
| `m_n33` | 4 × 10,000 × `-3e33` | `-1.2e38` | `-3e33` | NULL / AO | **`-3e33`** | NULL / AO | NULL / AO |
| `m_s2` | 4 × 5,000 × `9e31`, `decimal(38,2)` | `1.8e36` (unscaled `1.8e38`) | `9e31` | NULL / AO | **`9e31`** | NULL / AO | NULL / AO |
| `m_mix` | 4 × (2,500 × `9e33` + 2,500 × `7e33`) | `1.6e38` | `8e33` | NULL / AO | **`8e33`** | NULL / AO | NULL / AO |
| `m_w8` | 8 × 2,500 × `9e33` | `1.8e38` | `9e33` | NULL / AO | **`9e33`** | NULL / AO | NULL / AO |

NULL / AO reads: NULL with ANSI off and in `try_avg`, `ARITHMETIC_OVERFLOW` for `avg` with ANSI on. Every cell is
steady: Spark 2 runs per (master, mode, codegen), main 20 runs in each of six configurations (SET 1, 2, 4, 8 at 10
cores; 2 cores; 4 cores with stealing off). The grouped twins of all five tables are NULL / AO in both engines at
every setting (Spark's per-row grouped check), so they are EQUAL. The windows of all five answer the quotient in
Spark at both masters, both modes and both codegen settings; the revised seat A answers the same, main answers
NULL / AO.

**The class, counted** (`avg` and `try_avg`, ungrouped, both modes: 4 pairs per table):

| class (seat A against Spark) | vs `local[1]` | vs `local[4]` |
|---|---|---|
| exact value, Spark NULL | 24 | 46 |
| exact value, Spark raises `ARITHMETIC_OVERFLOW` | 8 | 16 |
| both raise, another condition | 5 (step 0: 8) | 10 (step 0: 13) |
| **the exact answer raises or is NULL where Spark answers** | **0** | **20** (step 0: 0) |
| equal to Spark at that master | 61 | 6 |
| pairs | 98 | 98 |

- At `local[1]` the class is empty: one partial holds every row, and its output row is checked once, which is the
  exact model's check. At `local[4]` it holds all 20 pairs on these tables. The size of the class is not a property
  of the data alone: it is every ungrouped `avg` whose total passes 38 digits while each of Spark's partials fits,
  so it grows with Spark's split count.
- An `ObjectHashAggregate` (a `collect_list` in the same `SELECT`) answers the quotient at `local[4]` with codegen
  off too: `SELECT avg(c), size(collect_list(id)) FROM m_w34` is `9e33` at `local[4]` in both codegen settings and
  NULL at `local[1]`. It is the same class; its count is not added above.
- Q7 (a) keeps this class; Q7 (b) and (c) would trade it for a layout-dependent check or for disagreeing with
  `local[1]` (sketch §6).

Up: [map.md](map.md).
