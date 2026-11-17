# AVG-DECIMAL-ROUND-1 — cost (built 2026-11-17)

Head `pkg-avg-27d6149f` (the product commit `27d6149f`) against main `pkg-dct-7bf32859`. Method:
- five alternating runs of main, head, main again (`main-aa`, the control band);
- each sample is one process under `flock -x measure.lock` plus the link and cargo locks, `nice -n 10`,
  `taskset -c 32-47`;
- each timed region repeats its query: 4 times for K1, K2, K4 and the reported row, and 10 times for K3. Every sample
  is at least 200 ms. The table gives the per-query median;
- data: 10,000,000 rows (`k1`: 200,000 groups, `d10 DECIMAL(10,2)`, `d38 DECIMAL(38,0)` below `10^26`, `x DOUBLE`,
  `b BIGINT`) and 1,000,000 rows (`k2`: 100 partitions), written once as Parquet by main's package;
- C-012 is the like-for-like replay set (`replay_work.py`, `c012p6` and `c012rest`), as in DECIMAL-COMMON-TYPE-1.

Raw: [raw/cost/](raw/cost/) (`samples.jsonl`, `raw-avg.jsonl`, `table.md`).

| row | held | main (s) | head (s) | head/main | control band (main-aa/main) | bar | runs |
|---|---|---|---|---|---|---|---|
| C-012 `c012p6` | yes | 39.1800 | 39.3500 | 1.004 | 0.994 to 1.035 | 1.05 | 5 |
| C-012 `c012rest` | yes | 42.8500 | 43.1500 | 1.007 | 0.991 to 1.050 | 1.05 | 5 |
| K1 grouped `avg(d10)`, 200,000 groups | yes | 0.0970 | 0.0978 | 1.008 | 0.914 to 1.126 | 1.05 | 5 |
| K2 window `avg(d10)`, `ROWS 3 PRECEDING` | yes | 0.0759 | 0.0790 | 1.041 | 0.928 to 1.068 | 1.05 | 5 |
| K3 ungrouped `avg(d10)`, 10M rows | yes | 0.0323 | 0.0329 | 1.019 | 0.901 to 1.119 | 1.05 | 5 |
| K3 ungrouped `avg(d38)`, 10M rows | yes | 0.0517 | 0.0531 | 1.026 | 1.014 to 1.047 | 1.05 | 5 |
| K4 grouped `avg(x)` (`DOUBLE`) | yes | 0.0787 | 0.0766 | 0.974 | 1.002 to 1.136 | 1.05 | 5 |
| K4 grouped `mean(x)` (`DOUBLE`) | yes | 0.0744 | 0.0795 | 1.069 | 0.974 to 1.083 | 1.05 | 5 |
| K4 grouped `avg(b)` (`BIGINT`) | yes | 0.0797 | 0.0784 | 0.983 | 0.889 to 1.074 | 1.05 | 5 |
| K4 grouped `mean(b)` (`BIGINT`) | yes | 0.0756 | 0.0780 | 1.031 | 0.969 to 1.061 | 1.05 | 5 |
| grouped `mean(d10)` (reported; DataFusion's `Avg` → RePark's) | no | 0.0938 | 0.0946 | 1.008 | 0.915 to 1.004 | — | 5 |

- Every held row is at or under 1.05x except K4 `mean(x)` at 1.069. Main's own control band reaches 1.083 on that
  row, so it covers the ratio; by the brief's rule this is not a halt (no ruling made here). `mean` of a `DOUBLE` now
  runs RePark's float accumulator in place of DataFusion's `Avg`; `avg(x)` runs it on both builds (0.974).
- Answers: the decimal rows agree between main and head here because the K data's quotients happen to terminate
  (the rounding fix is pinned elsewhere). The K4 `sum(m)` over 200,000 group means is a float sum in output order,
  so it differs in the last digits between any two runs, `main` against `main-aa` included.
- Design history, measured on the way: an `i256` per-group sum cost K1 1.48x; a third `wraps` state column cost K1
  1.19x (DataFusion skips partial aggregation at this cardinality, so every row's state is shipped: 309 MB against
  397 MB of partial output); `i256` arithmetic at each window `evaluate` cost K2 1.07x to 1.13x. The product commit
  keeps a native running sum with a wrap count, ships the declared state types, and averages through the `i128`
  path of `div_half_up`.
