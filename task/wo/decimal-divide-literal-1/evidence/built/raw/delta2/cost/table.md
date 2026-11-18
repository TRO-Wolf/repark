| row | main (s) | head (s) | head/main | control band (main-aa/main) | bar | runs | answers equal | at the bar |
|---|---|---|---|---|---|---|---|---|
| 1 SQL `d10 % 3` (a base for the SQL `pmod()` row) | 0.0655 | 0.0679 | 1.035 | 0.857 to 1.088 | 1.05 | 10 | True | under |
| 1 SQL `d10 / 3` (`DECIMAL(10,2)`) | 0.2694 | 0.2092 | 0.777 | 0.934 to 1.074 | 1.05 | 5 | False | under |
| 1 SQL `d38 / 3` (`DECIMAL(38,2)`) | 0.2602 | 0.2572 | 0.988 | 0.934 to 1.172 | 1.05 | 5 | True | under |
| 1 SQL `mod(d10, 3)` | 0.0806 | 0.0644 | 0.799 | 0.929 to 1.121 | 1.05 | 5 | True | under |
| 1 SQL `pmod(d10, 3)` | 0.1383 | 0.1037 | 0.749 | 0.896 to 1.056 | 1.05 | 5 | True | under |
| 2 DataFrame `F.expr("d10 / 3")` (a base for row 2 `/`) | 0.2658 | 0.2028 | 0.763 | 0.948 to 1.078 | 1.05 | 5 | False | under |
| 2 DataFrame `F.expr("mod(d10, 3)")` | 0.0785 | 0.0651 | 0.829 | 0.827 to 1.174 | 1.05 | 5 | True | under |
| 2 DataFrame `F.pmod(col(d10), F.lit(3))` | 0.1083 | 0.0913 | 0.843 | 0.940 to 1.069 | 1.05 | 5 | True | under |
| 2 DataFrame `col(d10) % 3` | 0.0635 | 0.0623 | 0.981 | 0.965 to 1.104 | 1.05 | 5 | True | under |
| 2 DataFrame `col(d10) / 3` | 0.0530 | 0.2070 | 3.907 | 0.849 to 1.175 | 1.05 | 10 | False | **over: the owner's row** |
| 3 DataFrame `col(i1) / col(i2)` | 0.0266 | 0.0276 | 1.038 | 0.946 to 1.095 | 1.05 | 5 | False | under |
| 3 DataFrame `col(x1) / col(x2)` | 0.0372 | 0.0376 | 1.010 | 0.859 to 1.180 | 1.05 | 10 | False | under |
| 3 SQL `i1 / i2` (`INT`) | 0.0299 | 0.0295 | 0.986 | 0.931 to 1.035 | 1.05 | 5 | False | under |
| 3 SQL `x1 / x2` (`DOUBLE`) | 0.0405 | 0.0396 | 0.976 | 0.869 to 1.029 | 1.05 | 5 | False | under |
| 4 C-012 `c012p6` | 40.0700 | 41.2300 | 1.029 | 0.057 to 1.012 | 1.05 | 10 | True | under |
| 4 C-012 `c012rest` | 43.4400 | 43.7650 | 1.007 | 0.071 to 3.000 | 1.05 | 10 | True | under |

The owner's row, `col(d10) / 3` on the head, against three bases on main:

| base on main | main (s) | head `col(d10) / 3` (s) | ratio |
|---|---|---|---|
| main's DataFrame `col(d10) / 3` (a `double` division) | 0.0530 | 0.2070 | 3.907 |
| main's SQL `d10 / 3` | 0.2694 | 0.2070 | 0.768 |
| main's `F.expr("d10 / 3")` on the DataFrame door | 0.2658 | 0.2070 | 0.779 |

The SQL `pmod(d10, 3)` row on the head, against two bases on main:

| base on main | main (s) | head `pmod(d10, 3)` (s) | ratio |
|---|---|---|---|
| main's SQL `pmod(d10, 3)` (`decimal(12,2)`, not Spark's type) | 0.1383 | 0.1037 | 0.749 |
| main's SQL `d10 % 3` | 0.0655 | 0.1037 | 1.582 |
