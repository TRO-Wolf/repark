| row | main (s) | head (s) | head / main | control main-aa / main | runs | answers equal | bar |
|---|---|---|---|---|---|---|---|
| c012_c012p6 | 39.1050 | 39.4500 | 1.009 | 0.993 to 1.003 | 6 |  | 1.05x (held) |
| c012_c012rest | 42.5700 | 42.6600 | 1.002 | 1.000 to 1.070 | 5 |  | 1.05x (held) |
| k1_sum_d10_off | 0.0981 | 0.0924 | 0.942 | 0.919 to 1.054 | 5 | True | 1.05x (held) |
| k1_sum_d10_on | 0.0960 | 0.0932 | 0.970 | 0.916 to 1.112 | 5 | True | 1.05x (held) |
| k1_sum_d38_off | 0.1226 | 0.1274 | 1.038 | 0.947 to 1.127 | 5 | True | 1.12x (held) |
| k1_sum_d38_on | 0.1183 | 0.1267 | 1.072 | 0.907 to 1.029 | 10 | True | 1.12x (held) |
| k3_sum_d10 | 0.0367 | 0.0342 | 0.930 | 0.867 to 1.064 | 5 | True | reported |
| k3_sum_d38 | 0.0572 | 0.0560 | 0.978 | 1.001 to 1.086 | 5 | True | 1.05x (held) |
| k5_try_sum_d10_grouped | 0.8450 | 0.0919 | 0.109 | 0.891 to 0.992 | 5 | True | reported |

Bar column corrected by hand from the script's 1.05x default: `k1_sum_d38_off` and `k1_sum_d38_on` are held to
1.12x; `k3_sum_d10` and `k5_try_sum_d10_grouped` are reported, not held. The script also prints two `K2 scaling`
lines; this delta measured no K2 rows, so both print `nan` and are left out.
