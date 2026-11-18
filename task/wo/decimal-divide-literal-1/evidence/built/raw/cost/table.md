| row | main (s) | head (s) | head/main | control band (main-aa/main) | bar | runs | answers equal | at the bar |
|---|---|---|---|---|---|---|---|---|
| 1 SQL `d10 / 3` (`DECIMAL(10,2)`) | 0.2695 | 0.2074 | 0.769 | 0.949 to 1.018 | 1.05 | 5 | False | under |
| 1 SQL `d38 / 3` (`DECIMAL(38,2)`) | 0.2633 | 0.2542 | 0.966 | 0.907 to 1.185 | 1.05 | 5 | True | under |
| 1 SQL `mod(d10, 3)` | 0.0771 | 0.0670 | 0.869 | 1.012 to 1.095 | 1.05 | 5 | True | under |
| 1 SQL `pmod(d10, 3)` | 0.1365 | 0.1558 | 1.142 | 0.950 to 1.064 | 1.05 | 10 | True | **over, outside its band** |
| 2 DataFrame `F.expr("d10 / 3")` (a base for row 2 `/`) | 0.2742 | 0.2111 | 0.770 | 0.950 to 1.160 | 1.05 | 5 | False | under |
| 2 DataFrame `F.expr("mod(d10, 3)")` | 0.0817 | 0.0648 | 0.794 | 0.881 to 1.110 | 1.05 | 5 | True | under |
| 2 DataFrame `F.pmod(col(d10), F.lit(3))` | 0.1087 | 0.1076 | 0.990 | 0.927 to 1.113 | 1.05 | 10 | True | under |
| 2 DataFrame `col(d10) % 3` | 0.0665 | 0.0622 | 0.936 | 0.933 to 1.038 | 1.05 | 5 | True | under |
| 2 DataFrame `col(d10) / 3` | 0.0525 | 0.2067 | 3.935 | 0.891 to 1.139 | 1.05 | 10 | False | **over: the owner's row** |
| 3 DataFrame `col(i1) / col(i2)` | 0.0262 | 0.0270 | 1.032 | 0.939 to 1.062 | 1.05 | 5 | True | under |
| 3 DataFrame `col(x1) / col(x2)` | 0.0375 | 0.0379 | 1.011 | 0.879 to 1.008 | 1.05 | 5 | False | under |
| 3 SQL `i1 / i2` (`INT`) | 0.0289 | 0.0300 | 1.040 | 0.899 to 1.062 | 1.05 | 5 | False | under |
| 3 SQL `x1 / x2` (`DOUBLE`) | 0.0374 | 0.0388 | 1.038 | 1.040 to 1.164 | 1.05 | 5 | True | under |
| 4 C-012 `c012p6` | 38.9300 | 39.1300 | 1.005 | 0.952 to 1.004 | 1.05 | 5 | True | under |
| 4 C-012 `c012rest` | 42.3800 | 42.8800 | 1.012 | 0.953 to 1.007 | 1.05 | 5 | True | under |

The owner's row, `col(d10) / 3` on the head, against three bases on main:

| base on main | main (s) | head `col(d10) / 3` (s) | ratio |
|---|---|---|---|
| main's DataFrame `col(d10) / 3` (a `double` division) | 0.0525 | 0.2067 | 3.935 |
| main's SQL `d10 / 3` | 0.2695 | 0.2067 | 0.767 |
| main's `F.expr("d10 / 3")` on the DataFrame door | 0.2742 | 0.2067 | 0.754 |
