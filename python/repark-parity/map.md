# map — python/repark-parity

## Purpose

The Spark-parity differential test harness: compares repark output against a reference (recorded
Spark goldens in CI; live PySpark when refreshing them) and fails on any divergence. The comparison
core is pure pyarrow — no Spark, no JVM — so it runs in routine CI.

## Contents

- `pyproject.toml` — hatchling package; `pyarrow` dep; `record` extra (`pyspark`) for golden refresh.
- `src/repark_parity/` — `compare.py` (the comparison core), `__init__.py`, `py.typed`.
- `tests/` — unit tests for the comparison core **and the dataset generators**.
- `datasets/` — torture-dataset generators (cache-root outputs, data never committed);
  loaded as `repark_datasets`, not part of the hatch package; see [datasets/map.md](datasets/map.md).
- `bench/` — local performance measurement scripts (R-PERF-MEASURE); see [bench/map.md](bench/map.md).
- `compat/` — the Apache `pyspark.sql.tests` census harness (redirect seam + runner) **and the
  report comparator** that turns two census runs into the port's acceptance verdict; see
  [compat/map.md](compat/map.md).

- `record_ta_goldens.py` — standalone PEP-723 script (NOT part of the package): records the
  `repark-ta` bit-exactness goldens (158 series, incl. the T2 batch-1 `min`/`max`/`sum`
  math-operators, the WG1 overlap-MA family `wma`/`dema`/`tema`/`trima` odd+even/`kama`/`t3`
  at two vfactors/`midpoint`/`midprice`, the WG2 simple-momentum batch `mom`/`roc`/`rocp`/
  `rocr`/`rocr100`/`willr`/`cci`/`cmo`/`bop`/`apo`/`ppo` at matype 0 **and matype 7 (MAMA)**/
  `aroon` split/`aroonosc`/`trix`/`ultosc`, the WG3 directional + MACD families `dx`/`adxr`/
  `plus_di`/`minus_di`/`plus_dm`/`minus_dm`/split `macd`/`macdfix`/`macdext` (matype 0 +
  all-MAMA + mixed 7/0/1)/`ma` at matype 0+1, the WG4 split stochastics
  `stoch_slowk`/`_slowd`/`stochf_fastk`/`_fastd`/`stochrsi_fastk`/`_fastd` at the polars_talib
  defaults **and matype 7** (`stoch_type7_*`/`stoch_mixed_7_0_*`/`stochf_type7_*`/`stochrsi_type7_*`),
  and the WG5 sweep-up `natr`/`beta` + the O/H/L/C price transforms
  `avgprice`/`medprice`/`typprice`/`wclprice` (the last four recorded from `polars_talib`'s own
  native Rust, not C TA-Lib — see the crate's `price_transform.rs`), plus the
  flat-plateau guard series `flat_kama_10`, the WG2
  `flat_cmo`/`flat_willr`/`flat_cci`/`flat_bop`/`flat_ultosc`, the WG3
  `flat_dx`/`flat_plus_di`/`flat_minus_di`, the WG4 `flat_stochf_fastk`/`_fastd`, the WG5
  `flat_beta_5`, and the T3 parked four — `mama_mama`/`mama_fama`/`flat_mama_mama`/`flat_mama_fama`,
  `sar`, `sarext`/`sarext_long_offset`/`sarext_short`, `mavp`/`mavp_ema`, `ma_30_type7`, plus the
  `fixture_periods` input series MAVP consumes, plus the TA-3 volume family
  `ad`/`adosc_3_10`/`obv`/`mfi_14` and `flat_ad`/`flat_adosc_3_10`/`flat_obv`/`flat_mfi_14`
  over the additive `fixture_volume` / `fixture_flat_volume` columns — dedicated RNGs
  seed 4242 / 77, never the OHLC RNGs)
  from C TA-Lib 0.4.0 via `polars-talib` (pinned in its header; asserts the bundled TA-Lib
  version). Run `uv run python/repark-parity/record_ta_goldens.py`; output lands in
  `crates/repark-ta/tests/goldens/`.

## I want to...

| ...do this | go to |
|---|---|
| Add/adjust the comparison logic | `src/repark_parity/compare.py` (and its tests) |
| Add a parity case for a new op | the corpus (Phase 1+) + a recorded golden fixture |
| Record / extend the TA kernel goldens | `record_ta_goldens.py` → [../../crates/repark-ta/map.md](../../crates/repark-ta/map.md) |
| Run the PySpark-suite compatibility census | [compat/map.md](compat/map.md) / `python -m compat.runner --classic` |
| Compare two census runs (the acceptance gate) | `python -m compat.compare_reports` / [../../docs/port/census.md](../../docs/port/census.md) |
| Follow the recorded census procedure | [../../docs/port/census.md](../../docs/port/census.md) |
| Generate / test torture datasets | [datasets/map.md](datasets/map.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: the testing contract is [../../docs/testing.md](../../docs/testing.md).

### PR-6 note
- The `record` extra is PINNED `pyspark==4.1.2` (was `>=3.5` — the live-oracle drift detector's
  own oracle must not float; the goldens are recorded under 4.1.2). Bump deliberately + re-lock.

## Debug

| Symptom | First check |
|---|---|
| Parity fails on row order | Comparison is order-insensitive by default; pass `order_sensitive=True` only for `ORDER BY` |
| Need to refresh goldens | Install the `record` extra (needs a JVM) and run record mode — only for `live-recorded` corpora, see [../../docs/port/census.md](../../docs/port/census.md) §6 |

First checks: `PYTHONPATH=python/repark-parity/src pytest python/repark-parity/tests -q`
(the `make` wrapper targets arrive with the Makefile wiring). Escalate to:
[../map.md#debug](../map.md).
