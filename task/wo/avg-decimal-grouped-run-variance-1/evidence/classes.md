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

Up: [map.md](map.md).
