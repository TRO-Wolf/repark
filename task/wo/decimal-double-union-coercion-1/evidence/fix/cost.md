# DECIMAL-DOUBLE-UNION-COERCION-1 part 1 — cost

Measured by the orchestrator (`claude-opus-5-5`) on 2026-11-15, after the builder stopped at its turn budget. The
builds were:
- main `dc6b4c94` (release);
- the head `120e9d8a` (release); the later commits change no product code;
- main again, as the control.

**Method:**
- five alternating runs, rotating the order;
- each sample under the exclusive measure lock, the link lock and both cargo locks;
- 16 cores (32-47);
- the probe is [probes/cost_probe.py](probes/cost_probe.py), over a 100M-row in-memory table (`t`) and a 20M-row
  one (`m`), both from parquet;
- each query is timed after one warm-up run, as the median of three;
- the C-012 rows time the whole replay;
- each ratio is the median of the per-run head/main ratios, and "control" is the range of main-again/main;
- ns per value is the difference of the medians divided by the rows, so with 16 cores it is not a per-core figure.

## Held rows (the brief's 1.05x bar)

| row | main (s) | head (s) | head/main | control | ns/value |
|---|---|---|---|---|---|
| `WHERE d10 = v` (`d10 DECIMAL(10,2)`, `v DOUBLE`), 100M | 0.3727 | 0.2921 | 0.789 | 0.872 to 1.094 | -0.81 |
| `WHERE d10 < v`, 100M | 0.4049 | 0.3300 | 0.771 | 0.831 to 1.049 | -0.75 |
| `UNION ALL` of `d10` and `v`, summed, 200M | 0.3318 | 0.2905 | 0.891 | 0.909 to 1.111 | -0.21 |
| CASE nested 30 deep over `d10` and `v`, 100M | 4.3360 | 4.2684 | 0.982 | 0.960 to 1.013 | -0.68 |
| a flat 50-branch CASE over `d10` and `v`, 100M | 2.8327 | 2.8159 | 1.014 | 0.975 to 1.068 | -0.17 |
| a 200-column decimal-free projection (planning) | 2.7723 | 2.8051 | 1.007 | 0.982 to 1.020 | |
| a wide DECIMAL and DOUBLE plan with no unifying construct (planning) | 2.1898 | 2.2001 | 1.000 | 0.982 to 1.022 | |
| a 200-branch `UNION ALL` (planning) | 5.9405 | 5.4785 | 0.920 | 0.977 to 1.024 | |
| a 10,000-row `VALUES` (planning) | 26.7302 | 26.8174 | 1.004 | 0.996 to 1.006 | |
| a 1,000-column projection (planning) | 3.1335 | 3.1377 | 1.003 | 1.003 to 1.016 | |
| `DELETE … WHERE d10 = v` on a four-file table, 20M | 2.7869 | 2.8109 | 1.017 | 0.992 to 1.050 | +1.20 |
| C-012 `c012p6` | 38.61 | 38.56 | 0.998 | 0.993 to 1.004 | |
| C-012 `c012rest` | 41.80 | 41.91 | 1.003 | 0.995 to 1.007 | |

**The one held shape over 1.05x: a DECIMAL column against a DOUBLE constant.** The builder's row C11 had it
(`prune_d_dbl`) at 20M rows, where the samples were 40 to 70 ms and the control ranged up to 1.586. Re-run at 100M
rows:

| row | main (s) | head (s) | head/main | control | ns/value |
|---|---|---|---|---|---|
| `WHERE d10 = CAST(0.5 AS DOUBLE)`, 100M | 0.0490 | 0.1967 | **3.952** | 0.942 to 0.993 | +1.48 |
| `WHERE d10 = 0.5` (a DECIMAL literal, the shape's control), 100M | 0.0457 | 0.0457 | 1.008 | 0.996 to 1.055 | +0.00 |
| `WHERE d38 = CAST(0.5 AS DOUBLE)` (`d38 DECIMAL(38,18)`), 100M | 0.0484 | 0.5001 | **10.473** | 0.932 to 1.040 | +4.52 |

- **Main** casts the constant to the column's decimal type at planning (`FilterExec: d10 = Some(50),10,2`) and
  compares integers.
- **The head** compares as Spark does, by converting each row's decimal to a double with K2
  (`FilterExec: __repark_decimal_to_double__(d10) = 0.5`).
- **The pruning rows** (`prune_v_declit`, `prune_d_dbl`) read the same 16 files with the same pushed predicate on both
  builds. Neither build pushes the DECIMAL-column form, so file pruning is unchanged.
- **A DataFrame filter** of a DECIMAL column against a Python float (`df.filter(df.d > 10.5)`) is this shape.

## Reported rows (for the owner's ruling at merge)

| row | main (s) | head (s) | head/main | control | ns/value |
|---|---|---|---|---|---|
| `WHERE d38 < v` (K2 at `DECIMAL(38,18)`, the exact refinement), 100M | 0.4078 | 0.6902 | 1.662 | 0.931 to 1.014 | +2.82 |
| written `CAST(v AS DECIMAL(10,2))` (K1), 100M | 0.1947 | 0.2612 | 1.341 | 0.831 to 1.084 | +0.66 |
| written `CAST(v AS DECIMAL(38,18))` (K1), 100M | 0.1840 | 0.6107 | 3.361 | 1.016 to 1.131 | +4.27 |

## Answers

The answers equal between main and the head in every row except where the fix changes the type or the digits:
- the `UNION` and CASE sums are now `double`, as Spark types them;
- `CAST(v AS DECIMAL(38,18))` of `999.99` is `999.990000000000000000` on the head, as on Spark, where main gives
  `999.990000000000065536`.

## Side finding

A CASE nested 50 deep does not parse on main or on the head (`Expected: ), found: WHEN at Line: 1, Column: 17`);
30 deep does. This is pre-existing and not this unit's. The builder's 50-deep row was replaced by the nested-30 and
flat-50 rows above.

Up: [map.md](map.md).
