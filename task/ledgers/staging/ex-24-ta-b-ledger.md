# Unit ledger — EX-24 · v1.1 example backfill, the TA kernels (b)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-24 merges, or when the owner closes the
slate row.

**Unit:** EX-24 · **Date:** 2026-09-04 · **Model:** glm-5.3-flash · **Branch:** `docs/ex-24-ta-b` · **Base:** `188499a6` (= `origin/main` at dispatch; no merge performed — the orchestrator merges)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-24 lane brief (45 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/ta/`, `docs/examples/backlog.txt`,
the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`,
`docs/spark-sql-iceberg-parity.md` §7, `python/repark/tests/test_examples_window_catalog.py`,
lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`,
`python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger,
`briefs/next-sequence.md`.

## Scope

The roster is the remaining 45 `ta.*` backlog names at the base `188499a6` (backlog lines
215–259). The oracle for this family is **not** Spark — Spark has no TA kernels — it is the
recorded C TA-Lib 0.4.0 goldens under `crates/repark-ta/tests/goldens/` (`<kernel>_<params>.bin`,
little-endian `f64`, 5000 rows, the same files `python/repark/tests/test_ta.py` and
`test_ta_volume.py` pin bit-identically against the DataFrame route). Twelve files cover all 45
names: each rebuilds the 5000-row OHLCV fixture as a `createDataFrame`, runs the kernel over
`Window.orderBy("ts")`, and asserts the full 5000-row output bit-for-bit against the golden read
from the `.bin` at run time (`expect_bit_exact`: equal length, NaN rows matched positionally,
every other row by `f64` bit pattern, copied byte-identical from the EX-23 files) — every
asserted number comes from the golden file, none is hand-computed. Parameters are the recorder's
spellings, read call-site by call-site in
[crates/repark-ta/tests/goldens.rs](../../../crates/repark-ta/tests/goldens.rs) before any file
was written (multi-output `stoch_*`/`stochf_*`/`stochrsi_*` and `sarext` take exactly the
recorder's parameters; `mavp` runs over the `fixture_periods` column as the recorder does). All
45 measured bit-identical to their goldens, so none stayed on the backlog, no registry §7 row
was filed, and `python/repark/tests/test_examples_window_catalog.py` is unchanged. No JVM and no
network. The composition helpers are covered per the roster's composition rule: each is a real
fused call whose every produced column is asserted bit-exact against a golden (4 asserts for the
2 COVERS names — the only file in the batch where asserts exceed COVERS, and every extra assert
is a produced column of the helper under test).

**Roster (45):** `ta.mama`, `ta.mavp`, `ta.max`, `ta.medprice`, `ta.mfi`, `ta.midpoint`,
`ta.midprice`, `ta.min`, `ta.minus_di`, `ta.minus_dm`, `ta.mom`, `ta.natr`, `ta.obv`,
`ta.over_columns`, `ta.plus_di`, `ta.plus_dm`, `ta.ppo`, `ta.roc`, `ta.rocp`, `ta.rocr`,
`ta.rocr100`, `ta.rsi`, `ta.sar`, `ta.sarext`, `ta.stddev`, `ta.stoch_slowd`, `ta.stoch_slowk`,
`ta.stochf_fastd`, `ta.stochf_fastk`, `ta.stochrsi_fastd`, `ta.stochrsi_fastk`, `ta.sum`,
`ta.t3`, `ta.tema`, `ta.trange`, `ta.trima`, `ta.trix`, `ta.tsf`, `ta.typprice`, `ta.ultosc`,
`ta.var`, `ta.wclprice`, `ta.willr`, `ta.with_indicators`, `ta.wma`.

**Grouping (12 files, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `ta/ma_variants.py` | `ta.mama`, `ta.mavp`, `ta.midpoint`, `ta.midprice`, `ta.t3`, `ta.tema`, `ta.trima`, `ta.wma` | The overlap-study remainder: the MESA pair (mama output, MAVP over the periods column), the two midpoints, Tillson T3, and the MA variants. |
| `ta/math_operators.py` | `ta.max`, `ta.min`, `ta.sum` | The lowercase spellings of the rolling extremes, at period 21 like the EX-23 uppercase aliases. |
| `ta/rate_of_change.py` | `ta.mom`, `ta.roc`, `ta.rocp`, `ta.rocr`, `ta.rocr100` | The rate-of-change family at period 10. |
| `ta/oscillators.py` | `ta.ppo`, `ta.rsi`, `ta.trix`, `ta.ultosc`, `ta.willr` | The oscillator remainder: PPO at 12/26, RSI(14), TRIX(30), ULTOSC(7/14/28), Williams %R(14). |
| `ta/stochastics.py` | `ta.stoch_slowk`, `ta.stoch_slowd`, `ta.stochf_fastk`, `ta.stochf_fastd`, `ta.stochrsi_fastk`, `ta.stochrsi_fastd` | The three stochastic variants, each selecting its two split outputs in one pass at the recorder's parameters. |
| `ta/directional_movement.py` | `ta.minus_di`, `ta.minus_dm`, `ta.plus_di`, `ta.plus_dm` | The directional-movement quartet at period 14. |
| `ta/sar.py` | `ta.sar`, `ta.sarext` | The parabolic pair: SAR(0.02, 0.2) and SAREXT at the recorder's ten-parameter spelling. |
| `ta/true_range.py` | `ta.natr`, `ta.trange` | The true-range kernels: TRANGE and NATR(14). |
| `ta/volume_flow.py` | `ta.mfi`, `ta.obv` | The volume-flow pair: MFI(14) and OBV. |
| `ta/price_averages.py` | `ta.medprice`, `ta.typprice`, `ta.wclprice` | The remaining no-period OHLC averages. |
| `ta/statistics.py` | `ta.stddev`, `ta.tsf`, `ta.var` | The dispersion pair at 5/1.0 and the time-series forecast TSF(5). |
| `ta/composition.py` | `ta.over_columns`, `ta.with_indicators` | The two composition helpers, each fusing several kernels through one window; every produced column asserted bit-exact (`ema_5`, `trima_5`, `rsi_3`, `min_34` goldens; `with_indicators` runs partitioned by a symbol column, as its contract requires). |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Twelve runnable files under `docs/examples/ta/` land local examples for all 45 roster names, each script rebuilding the 5000-row OHLCV fixture as a `createDataFrame`, running the kernel over `Window.orderBy("ts")`, and asserting the full 5000-row output bit-for-bit against the recorded golden read from `crates/repark-ta/tests/goldens` at run time (`expect_bit_exact`: equal length, NaN rows matched positionally, every other row by `f64` bit pattern) — no hand-computed number in any example; each script exits 0 under `python <path>` with no network and no JVM; no product file is touched. | The shipped examples themselves: the `--require-execute` gate executes every `expect_bit_exact` assert over all 5000 rows on this tree (exit 0, gates table); the oracle table (45 rows, one per roster name) records the golden, the call, and the measured result per name. | **PROVEN** |
| C-002 | All 45 covered names leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 45, 258 → 213, with no other `scripts/` change; the gate's static half and its `--require-execute` leg both exit 0 (698 covered; 213 backlog; 185 examples on this tree). | The gate's own counts line at the base `188499a6` (653/258/173, the unit's before-line) and on the shipped tree (698/213/185), plus the red-first provocation below. | **PROVEN** |
| C-003 | A name whose repark answer differs from its golden is not papered over: the 45-name run measured zero divergences (every `expect_bit_exact` green over the full 5000 rows), so no §7 row is filed, no name stays on the backlog, and `python/repark/tests/test_examples_window_catalog.py` is unchanged; a future divergence would follow the EX-TA-`<n>` row + pin route. | The shipped examples' bit-exact control over the full 5000-row golden — executed by the gate on every run — plus the red-first re-run (45-findings provocation and the control mutation, both exit 1; table in "Red-first") and the unchanged pin file (`git diff --exit-code python/repark/tests/test_examples_window_catalog.py` on this tree). | **PROVEN** |
| C-004 | This ledger records the roster, the grouping, the red-first provocations, and the name-by-name oracle table; `staging/map.md` gains the EX-24 row; `docs/examples/map.md` and `docs/examples/ta/map.md` move in lockstep with the files; `scripts/map.md` carries the baseline-ratchet entry with the pin citations. | The ledger itself and the lockstep map diffs in the same commits. | **PROVEN** |

`LOGIC_SCORE` = **4/4 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

**Provocation 1 — the backlog ratchet (round 1, on this tree):** the twelve example files held
outside `docs/examples/` (in gitignored scratch) while `docs/examples/backlog.txt` kept the 45
roster rows deleted and `BACKLOG_BASELINE` stood at 213 (`258 − 45`, as if the whole roster were
covered): the gate exits **1** with exactly **45 findings**, every one
`public name ta.<name> has no example COVERS row…`, and no other finding. Restoring the twelve
files returns the static gate to **0** (`698 covered; 213 backlog; 185 examples`).
`pins: ex-24-ta-b/C-001, C-002`

**Provocation 2 — the bit-exact control (round 1, on this tree):** one provocation injected into
`ta/statistics.py` (temporary, never committed, reverted before the next run): the first 4995
compared rows of the `ta.tsf` output overwritten with 999.0, tail intact — the class a tail-only
control passes. Full gate `.venv/bin/python scripts/check_example_coverage.py --require-execute`:
exit **1** with exactly one execute finding,
`example …/docs/examples/ta/statistics.py exited 1: … ta.tsf: bit mismatch at row 0: got 999.0
vs golden nan` — the control names the kernel, the first mismatching row, and both values.
Reverting restores the full gate to **0**. `pins: ex-24-ta-b/C-001, C-003`

## Oracle (recorded C TA-Lib 0.4.0 goldens — Spark has no TA kernels)

The measurement instrument is the shipped examples' `expect_bit_exact` control over the full
5000-row golden, executed by `.venv/bin/python scripts/check_example_coverage.py
--require-execute` on this tree (exit 0). Each script was also run standalone from a temp cwd
(the gate's own layout) before any commit: exit 0, all 45. Every asserted value is read from the
`.bin` at run time; parameters are the recorder's spellings from
[crates/repark-ta/tests/goldens.rs](../../../crates/repark-ta/tests/goldens.rs). No Spark JVM was
started; `python3` on this box cannot import `repark._native`, so the `--require-execute` leg
runs under `.venv/bin/python`, which resolves `repark` to the sibling checkout at `671a7144` —
an ancestor of this unit's base `188499a6`; the four commits between them touch docs and a
`repark-core` dynamic-flatten path only (`git diff --stat 671a7144 188499a6 --
crates/repark-ta python/repark/src` is empty), so the TA kernels exercised are the base's.

| Name | Golden | repark call (over `Window.orderBy("ts")`) | Result vs golden | File |
|---|---|---|---|---|
| `ta.mama` | `mama_mama.bin` | `ta.mama("close", fastlimit=0.5, slowlimit=0.05)` | bit-identical, all 5000 rows | `ta/ma_variants.py` |
| `ta.mavp` | `mavp.bin` | `ta.mavp("close", "periods", minperiod=5, maxperiod=20, matype=0)` | bit-identical, all 5000 rows | `ta/ma_variants.py` |
| `ta.max` | `max_21.bin` | `ta.max("close", timeperiod=21)` | bit-identical, all 5000 rows | `ta/math_operators.py` |
| `ta.medprice` | `medprice.bin` | `ta.medprice("high", "low")` | bit-identical, all 5000 rows | `ta/price_averages.py` |
| `ta.mfi` | `mfi_14.bin` | `ta.mfi("high", "low", "close", "volume", timeperiod=14)` | bit-identical, all 5000 rows | `ta/volume_flow.py` |
| `ta.midpoint` | `midpoint_10.bin` | `ta.midpoint("close", timeperiod=10)` | bit-identical, all 5000 rows | `ta/ma_variants.py` |
| `ta.midprice` | `midprice_10.bin` | `ta.midprice("high", "low", timeperiod=10)` | bit-identical, all 5000 rows | `ta/ma_variants.py` |
| `ta.min` | `min_21.bin` | `ta.min("close", timeperiod=21)` | bit-identical, all 5000 rows | `ta/math_operators.py` |
| `ta.minus_di` | `minus_di_14.bin` | `ta.minus_di("high", "low", "close", timeperiod=14)` | bit-identical, all 5000 rows | `ta/directional_movement.py` |
| `ta.minus_dm` | `minus_dm_14.bin` | `ta.minus_dm("high", "low", timeperiod=14)` | bit-identical, all 5000 rows | `ta/directional_movement.py` |
| `ta.mom` | `mom_10.bin` | `ta.mom("close", timeperiod=10)` | bit-identical, all 5000 rows | `ta/rate_of_change.py` |
| `ta.natr` | `natr_14.bin` | `ta.natr("high", "low", "close", timeperiod=14)` | bit-identical, all 5000 rows | `ta/true_range.py` |
| `ta.obv` | `obv.bin` | `ta.obv("close", "volume")` | bit-identical, all 5000 rows | `ta/volume_flow.py` |
| `ta.over_columns` | `ema_5.bin` + `trima_5.bin` (the produced columns) | `ta.over_columns(window, {"ema5": ta.ema("close", timeperiod=5), "trima5": ta.trima("close", timeperiod=5)})` → `withColumns` | bit-identical, all 5000 rows | `ta/composition.py` |
| `ta.plus_di` | `plus_di_14.bin` | `ta.plus_di("high", "low", "close", timeperiod=14)` | bit-identical, all 5000 rows | `ta/directional_movement.py` |
| `ta.plus_dm` | `plus_dm_14.bin` | `ta.plus_dm("high", "low", timeperiod=14)` | bit-identical, all 5000 rows | `ta/directional_movement.py` |
| `ta.ppo` | `ppo_12_26.bin` | `ta.ppo("close", fastperiod=12, slowperiod=26, matype=0)` | bit-identical, all 5000 rows | `ta/oscillators.py` |
| `ta.roc` | `roc_10.bin` | `ta.roc("close", timeperiod=10)` | bit-identical, all 5000 rows | `ta/rate_of_change.py` |
| `ta.rocp` | `rocp_10.bin` | `ta.rocp("close", timeperiod=10)` | bit-identical, all 5000 rows | `ta/rate_of_change.py` |
| `ta.rocr` | `rocr_10.bin` | `ta.rocr("close", timeperiod=10)` | bit-identical, all 5000 rows | `ta/rate_of_change.py` |
| `ta.rocr100` | `rocr100_10.bin` | `ta.rocr100("close", timeperiod=10)` | bit-identical, all 5000 rows | `ta/rate_of_change.py` |
| `ta.rsi` | `rsi_14.bin` | `ta.rsi("close", timeperiod=14)` | bit-identical, all 5000 rows | `ta/oscillators.py` |
| `ta.sar` | `sar.bin` | `ta.sar("high", "low", acceleration=0.02, maximum=0.2)` | bit-identical, all 5000 rows | `ta/sar.py` |
| `ta.sarext` | `sarext.bin` | `ta.sarext("high", "low", startvalue=0.0, offsetonreverse=0.0, accelerationinitlong=0.02, accelerationlong=0.02, accelerationmaxlong=0.2, accelerationinitshort=0.02, accelerationshort=0.02, accelerationmaxshort=0.2)` | bit-identical, all 5000 rows | `ta/sar.py` |
| `ta.stddev` | `stddev_5_nbdev1.bin` | `ta.stddev("close", timeperiod=5, nbdev=1.0)` | bit-identical, all 5000 rows | `ta/statistics.py` |
| `ta.stoch_slowd` | `stoch_slowd.bin` | `ta.stoch_slowd("high", "low", "close", fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)` | bit-identical, all 5000 rows | `ta/stochastics.py` |
| `ta.stoch_slowk` | `stoch_slowk.bin` | `ta.stoch_slowk("high", "low", "close", fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)` | bit-identical, all 5000 rows | `ta/stochastics.py` |
| `ta.stochf_fastd` | `stochf_fastd.bin` | `ta.stochf_fastd("high", "low", "close", fastk_period=5, fastd_period=3, fastd_matype=0)` | bit-identical, all 5000 rows | `ta/stochastics.py` |
| `ta.stochf_fastk` | `stochf_fastk.bin` | `ta.stochf_fastk("high", "low", "close", fastk_period=5, fastd_period=3, fastd_matype=0)` | bit-identical, all 5000 rows | `ta/stochastics.py` |
| `ta.stochrsi_fastd` | `stochrsi_fastd.bin` | `ta.stochrsi_fastd("close", timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)` | bit-identical, all 5000 rows | `ta/stochastics.py` |
| `ta.stochrsi_fastk` | `stochrsi_fastk.bin` | `ta.stochrsi_fastk("close", timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)` | bit-identical, all 5000 rows | `ta/stochastics.py` |
| `ta.sum` | `sum_21.bin` | `ta.sum("close", timeperiod=21)` | bit-identical, all 5000 rows | `ta/math_operators.py` |
| `ta.t3` | `t3_5.bin` | `ta.t3("close", timeperiod=5, vfactor=0.7)` | bit-identical, all 5000 rows | `ta/ma_variants.py` |
| `ta.tema` | `tema_10.bin` | `ta.tema("close", timeperiod=10)` | bit-identical, all 5000 rows | `ta/ma_variants.py` |
| `ta.trange` | `trange.bin` | `ta.trange("high", "low", "close")` | bit-identical, all 5000 rows | `ta/true_range.py` |
| `ta.trima` | `trima_10.bin` | `ta.trima("close", timeperiod=10)` | bit-identical, all 5000 rows | `ta/ma_variants.py` |
| `ta.trix` | `trix_30.bin` | `ta.trix("close", timeperiod=30)` | bit-identical, all 5000 rows | `ta/oscillators.py` |
| `ta.tsf` | `tsf_5.bin` | `ta.tsf("close", timeperiod=5)` | bit-identical, all 5000 rows | `ta/statistics.py` |
| `ta.typprice` | `typprice.bin` | `ta.typprice("high", "low", "close")` | bit-identical, all 5000 rows | `ta/price_averages.py` |
| `ta.ultosc` | `ultosc_7_14_28.bin` | `ta.ultosc("high", "low", "close", timeperiod1=7, timeperiod2=14, timeperiod3=28)` | bit-identical, all 5000 rows | `ta/oscillators.py` |
| `ta.var` | `var_5.bin` | `ta.var("close", timeperiod=5, nbdev=1.0)` | bit-identical, all 5000 rows | `ta/statistics.py` |
| `ta.wclprice` | `wclprice.bin` | `ta.wclprice("high", "low", "close")` | bit-identical, all 5000 rows | `ta/price_averages.py` |
| `ta.willr` | `willr_14.bin` | `ta.willr("high", "low", "close", timeperiod=14)` | bit-identical, all 5000 rows | `ta/oscillators.py` |
| `ta.with_indicators` | `rsi_3.bin` + `min_34.bin` (the produced columns) | `ta.with_indicators(frame, partition="symbol", order="ts", columns={"rsi3": ta.rsi("close", timeperiod=3), "min34": ta.min("close", timeperiod=34)})` | bit-identical, all 5000 rows | `ta/composition.py` |
| `ta.wma` | `wma_10.bin` | `ta.wma("close", timeperiod=10)` | bit-identical, all 5000 rows | `ta/ma_variants.py` |

## Gates (2026-09-04, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** (`698 covered; 213 backlog; 2 exceptions; 185 examples`; every assert executed over the full 5000 rows) |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_window_catalog.py -q` | **0** (13 passed; the file is untouched by this unit) |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `typos .` | **0** |
| `.venv/bin/ruff check docs/examples python/repark/tests` | **0** |
| `.venv/bin/ruff format --check docs/examples python/repark/tests` | **0** |

`python3 scripts/check_example_coverage.py --skip-execute` (system `python3`, no native module)
also exits 0 on the static half. No pin file was created — the pytest leg re-ran the existing
EX-20/EX-21/EX-22 pins beside this unit's tree.

Counts line (execute leg, on this tree; the base `188499a6` run printed `653 covered;
258 backlog; 2 exceptions; 173 examples`):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 698 covered; 213 backlog; 2 exceptions; 185 examples`

Before this unit: `653 covered; 258 backlog; 173 examples` (at `188499a6`, `BACKLOG_BASELINE`
258). On this unit's tree: `698 covered; 213 backlog; 185 examples` (`BACKLOG_BASELINE` 258 →
213) — exactly the 45 roster names, +45 / −45 / +12.

## Review notes (round 1, in-lane)

| Finding | Disposition |
|---|---|
| `ruff format` reflowed 3 of the 12 files (composition, price_averages, volume_flow) after the first lint pass | reformatted and the three re-run green before any commit; `ruff format --check` exits 0 on the shipped tree |
| `ta/composition.py` asserts 4 columns for 2 COVERS names, unlike the 1:1 discipline of the other eleven files | intended, per the roster's composition rule ("every produced column is still asserted bit-exact against a golden"); recorded here and in Scope — no unasserted column exists |

## Cost

The GLM (glm-5.3-flash) leg started 2026-09-04: read the contract, the corpus (the merged EX-23
batch end to end, `sma.py`, the EX-22 ledger, `test_ta.py` / `test_ta_volume.py` for the
golden-loading and window idioms), the gate, and the recorder call sites in `goldens.rs` for
every golden's parameters; wrote the twelve example files, ran each standalone from a temp cwd
against the goldens (JVM-free), ran the two red-first provocations, then the backlog ratchet,
the maps, and this ledger, committing in slices. Base `188499a6`.

## Disk

Pickup: `df -h` 584 GB free of 1.8 TB. The provocation scratch lives under the gitignored
`scratch/ex24-ta-b-probe/` (held-aside example copies, captured red/green logs — all removable
at close). No build artifacts created: no cargo build, `make develop` not run; `.venv` and the
sibling-checkout native module reused.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-24 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-24-ta-b
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 45 roster names are covered by twelve new example files and the oracle table records the golden, the call, and the measured result for all 45 rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/ta/ma_variants.py, docs/examples/ta/math_operators.py, docs/examples/ta/rate_of_change.py, docs/examples/ta/oscillators.py, docs/examples/ta/stochastics.py, docs/examples/ta/directional_movement.py, docs/examples/ta/sar.py, docs/examples/ta/true_range.py, docs/examples/ta/volume_flow.py, docs/examples/ta/price_averages.py, docs/examples/ta/statistics.py, docs/examples/ta/composition.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red-first provocation 1 held the twelve files outside docs/examples with the backlog rows deleted and the baseline at 213; the gate exited 1 with exactly 45 findings and the backlog is an exact baseline 213.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: The gate's use-check binds every ta.* COVERS name on the ta door alias; the examples call each kernel through ta.<name> (mavp through its two-series spelling, the stochastic splits through their split functions, the composition helpers through their fused calls), so a dropped call is an unused-cover red.
      artifacts: [scripts/check_example_coverage.py, docs/examples/ta/ma_variants.py, docs/examples/ta/stochastics.py, docs/examples/ta/composition.py]
    - id: AT-4
      status: N/A
      justification: The gate and the examples are read-only processes over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the twelve local examples; they read repo goldens by a __file__-derived path, drop AWS_* and PYTHONPATH in the gate's child, and touch no network or cloud service.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; provocation 1 ran the AST-only half with exactly 45 findings.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the new examples; the walk adds no import of the facade.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The pins citations for C-001..C-004 live in scripts/map.md beside the prior example batches, the example docstrings cite ex-24-ta-b/C-001, and this ledger cites its clauses in the red-first and oracle sections.
      artifacts: [scripts/map.md, docs/examples/ta/ma_variants.py, task/ledgers/staging/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Goldens: [../../../crates/repark-ta/tests/goldens/map.md](../../../crates/repark-ta/tests/goldens/map.md)
- Pins: [../../../python/repark/tests/test_ta.py](../../../python/repark/tests/test_ta.py) (the bit-exact DataFrame-route pins over the same goldens; no new pin file — zero divergences)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 (no row filed from this unit)
- Siblings: [ex-23-ta-a-ledger.md](ex-23-ta-a-ledger.md), [ex-22-types-writerv2-ledger.md](ex-22-types-writerv2-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-24-ta-b
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-24-ta-b
  artifacts_verified:
    ledger: PASS (C-001..C-004 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review notes carry the in-lane round-1 dispositions)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, TA kernels (b) — 45 covered, zero divergences, no registry row
  verdict: PENDING
  rejection_route: N/A
```
