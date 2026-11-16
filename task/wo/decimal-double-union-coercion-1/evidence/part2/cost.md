# Part 2 cost

## Method

- **The runner:** the build's cost runner, copied for the delta (scratchpad `ddu-work/cost-p2d/run.sh`; the build's
  `ddu-work/cost-p2/` raw files are kept as the halt's evidence). Each run takes a06c, the head and a06c again
  (`a06c-aa`) in a rotating order, per case, one sample at a time under the exclusive measure lock, on cores 32 to 47;
  the `in` group adds dc6b. A row is the median of its runs; the control band is the range of `a06c-aa / a06c`
  over the runs. The probes are `../fix/probes/cost_probe.py` (`held`), `probes/cost_p2.py` (`p2`),
  `../part2-step0/probes/in_cost.py` (`in`) and the C-012 replay (`c012`).
- **The bars** (the build brief): every `held` row, the unmixed `VALUES` rows and the per-row `IN` rows at most
  1.05× against a06c; the mixed `VALUES` rows are reported (each is a fix); C-012 at most 1.05×. A held row above
  1.05× after 5 runs whose control band does not cover it is a halt.
- **Packages:** a06c `pkg-ddu1-7b21bde3` (product tree `a06c1088`), dc6b `pkg-main-dc6b4c94`, the build's head
  `pkg-p2-6be38a8e`, the delta's head `pkg-p2-12101e9b` (`BUILT_FROM` `12101e9b`, built from the committed tree).

## The build's runs: the halt

The build ran 3 alternating runs on `pkg-p2-6be38a8e` before the halt was called:

| row | bar | head/a06c | control band |
|---|---|---|---|
| `plan_values10k` (one double) | held 1.05× | 1.132 | 1.003 to 1.010 |
| `plan_values10k_none` | held 1.05× | 1.115 | 0.999 to 1.001 |
| `plan_values10k_dbl` | held 1.05× | 1.145 | 0.984 to 1.006 |
| `ins_values10k_dec`, unmixed | held 1.05× | 1.153 | 0.981 to 1.001 |
| `ins_values10k_dbl`, unmixed | held 1.05× | 1.089 | 0.982 to 0.999 |
| `merge_values1000`, unmixed | held 1.05× | 1.306 | 0.961 to 1.013 |
| `in_dbl:d38:1000`, per-row | held 1.05× | 1.083 | 0.939 to 1.056 |
| `notin_dbl:d38:1000`, per-row | held 1.05× | 1.134 | 0.950 to 1.046 |
| `ins_values10k_dec_mixed` | reported | 1.504 | |
| `ins_values10k_dbl_mixed` | reported | 1.102 | |
| `merge_values1000_mixed` | reported | 1.293 | |

The old runner was still going when the delta began. The session's permission system refused the delta's request to
stop it, so it finished on its own: **all 5 runs, 380 samples**, ending at 11:36 UTC, before the delta's first
sample. Its 5-run table, [raw/cost-build-table.md](raw/cost-build-table.md), confirms the halt
(`plan_values10k_none` 1.131, `ins_values10k_dec` 1.153, `merge_values1000` 1.298, `in_dbl:d38:1000` 1.088,
`notin_dbl:d38:1000` 1.131). Verifier 2's 10 runs on `pkg-p2-7906b0e3` agree (its F4 and F5).

## Where seat V's time went

- **Spark door, release** (`probes/phase_split.py`, median of 10, 10,000 rows × 2 columns): the build's head spent
  +145 ms (no double) and +160 ms (one double) over a06c in `spark.sql`, which parses, marks, plans and analyzes
  eagerly (0.866 and 0.968 s against 0.721 and 0.804 s). `collect` of a planned frame cost the same as a06c's
  (0.35 s), so the optimizer and execution paid nothing.
- **Split, debug profile** (`values_marker/tests.rs`, `seat_v_phase_timing`, a plain session with the consumer): the
  SQL planner's 20,000 marker calls cost +465 ms (each `get_type` of a function call builds its argument and return
  fields and formats its display name), the analyzer +68 ms (the consumer and coercion over function cells), and the
  delta's AST pass 17.5 ms (no double) to 24.6 ms (one double). About 87% planning the markers, 13% the consumer.
- **The delta's head** (release): `spark.sql` 0.726 to 0.731 s (no double) and 0.834 to 0.835 s (one double),
  against a06c's 0.721 and 0.800 to 0.804 s.
- **The `IN` rows:** the per-row form paid `decimal_in_form` over all 1,000 constants on every optimizer pass before
  deciding per-row; it now stops at the 33rd distinct wide range.

## The delta's runs

5 alternating runs for `held` and `p2`, 3 for `in` and `c012`, on `pkg-p2-12101e9b`; 345 samples, every one rc 0;
every row's answers equal across the builds. The full table: [raw/cost-delta-table.md](raw/cost-delta-table.md).

| row | bar | before | a06c (s) | head (s) | head/a06c | control band | runs | call |
|---|---|---|---|---|---|---|---|---|
| `plan_values10k` (one double) | held 1.05× | 1.132 | 1.3460 | 1.3813 | 1.026 | 1.002 to 1.007 | 5 | pass |
| `plan_values10k_none` | held 1.05× | 1.115 | 1.3221 | 1.3324 | 1.008 | 0.994 to 1.004 | 5 | pass |
| `plan_values10k_dbl` | held 1.05× | 1.145 | 1.3346 | 1.3780 | 1.033 | 0.995 to 1.008 | 5 | pass |
| `ins_values10k_dec`, unmixed | held 1.05× | 1.153 | 1.1690 | 1.1802 | 1.010 | 0.989 to 1.007 | 5 | pass |
| `ins_values10k_dbl`, unmixed | held 1.05× | 1.089 | 1.7246 | 1.7326 | 1.005 | 0.998 to 1.015 | 5 | pass |
| `merge_values1000`, unmixed | held 1.05× | 1.306 | 0.1987 | 0.1995 | 1.004 | 0.977 to 1.016 | 5 | pass |
| `in_dbl:d38:1000`, per-row | held 1.05× | 1.083 | 0.6303 | 0.6478 | 1.028 | 1.002 to 1.027 | 3 | pass |
| `notin_dbl:d38:1000`, per-row | held 1.05× | 1.134 | 0.6292 | 0.6579 | 1.046 | 0.970 to 1.026 | 3 | pass |
| `ins_values10k_dec_mixed` | reported | 1.504 | 1.2494 | 1.7223 | 1.379 | 0.986 to 0.998 | 5 | reported |
| `ins_values10k_dbl_mixed` | reported | 1.102 | 1.7272 | 1.7293 | 1.001 | 0.990 to 1.006 | 5 | reported |
| `merge_values1000_mixed` | reported | 1.293 | 0.2129 | 0.2286 | 1.074 | 0.966 to 1.043 | 5 | reported |
| `prune_d_dbl` | held 1.05× | 1.046 | 0.0553 | 0.0603 | 1.090 | 0.871 to 1.288 | 5 | inside its band |
| `filter_eq_d10`, `filter_lt_d10`, `plan_free200`, `plan_wide_dd`, `plan_union200`, `prune_v_declit`, `delete_d_v` | held 1.05× | | | | 0.897 to 1.017 | | 5 | pass |
| C-012 `c012p6` / `c012rest` | 1.05× | 1.007 / 1.025 | 38.55 / 42.04 | 38.94 / 42.22 | 1.010 / 1.004 | 0.993 to 1.005 / 0.983 to 1.003 | 3 | pass |

- `prune_d_dbl` is a 55 ms sample, under the 200 ms floor; its control band (0.871 to 1.288) covers it, and
  verifier 2's 10 runs read 0.996. It is not a halt.
- `ins_values10k_dec_mixed` now costs one DOUBLE cast and one DOUBLE-to-DECIMAL store cast per cell, which is
  Spark's order (the inline table is `double`, then the store cast); a06c stored a decimal literal directly and
  truncated `1e-20`. It is a fix and reported, not held.
- Every other `in` row is at most 1.064 (`in_dbl:d38:1`, a 47 ms sample inside its band 0.944 to 1.107) or below
  1.0; the precision-16 rows `p16_in_8`, `_16`, `_32` read 0.280, 0.328 and 0.957, and `ice_in20` 0.795.

Up: [map.md](map.md).
