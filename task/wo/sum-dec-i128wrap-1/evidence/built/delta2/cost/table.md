| row | main (s) | head (s) | head / main | control main-aa / main | runs | answers equal | bar |
|---|---|---|---|---|---|---|---|
| c012_c012p6 | 38.8600 | 39.2900 | 1.011 | 0.976 to 1.012 | 5 |  | held |
| c012_c012rest | 42.3800 | 42.6700 | 1.007 | 0.993 to 1.005 | 5 |  | held |
| k1_sum_d10_off | 0.0941 | 0.0942 | 1.001 | 0.811 to 1.216 | 10 | True | held |
| k1_sum_d10_on | 0.0936 | 0.0926 | 0.989 | 0.890 to 1.059 | 5 | True | held |
| k1_sum_d38_off | 0.1204 | 0.1307 | 1.085 | 0.926 to 1.062 | 5 | True | held (1.12x) |
| k1_sum_d38_on | 0.1205 | 0.1289 | 1.070 | 0.882 to 1.119 | 5 | True | held (1.12x) |
| k3_sum_d38 | 0.0569 | 0.0562 | 0.989 | 0.919 to 1.058 | 5 | True | held |
| k5_try_sum_d10_grouped | 0.8482 | 0.0931 | 0.110 | 0.958 to 1.044 | 5 | True | reported |

The bar column is corrected by hand for the two K1 d38 rows: the owner's 20:05 ruling holds K1 at precision 20 and up to 1.12x; the script applies 1.05x to every row.

K2 scaling, head, d10: ROWS 1000 PRECEDING / ROWS 3 PRECEDING = nan

K2 scaling, head, d38: ROWS 1000 PRECEDING / ROWS 3 PRECEDING = nan
