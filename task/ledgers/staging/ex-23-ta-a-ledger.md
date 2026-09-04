# Unit ledger — EX-23 · v1.1 example backfill, the TA kernels (a)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-23 merges, or when the owner closes the
slate row.

**Unit:** EX-23 · **Date:** 2026-09-04 · **Model:** glm-5.3-flash · **Branch:** `docs/ex-23-ta-a` · **Base:** `bfef4a62` (dispatched at `671a7144`; main re-merged twice, last after EX-22)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-23 lane brief (40 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/ta/`, `docs/examples/backlog.txt`,
the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`,
`docs/spark-sql-iceberg-parity.md` §7, `python/repark/tests/test_examples_window_catalog.py`,
lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`,
`python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger,
`briefs/next-sequence.md`.

## Scope

The roster is the first 40 `ta.*` backlog names at the dispatch base `671a7144` (backlog lines 229–268). The
oracle for this family is **not** Spark — Spark has no TA kernels — it is the recorded C TA-Lib
0.4.0 goldens under `crates/repark-ta/tests/goldens/` (`<kernel>_<params>.bin`, little-endian
`f64`, 5000 rows, the same files `python/repark/tests/test_ta.py` and `test_ta_volume.py` pin
bit-identically against the DataFrame route). Eight files cover all 40 names: each rebuilds the
5000-row OHLCV fixture as a `createDataFrame`, runs the kernel over `Window.orderBy("ts")`, and
asserts the full 5000-row output bit-for-bit against the golden read from the `.bin` at run time
(`expect_bit_exact`: equal length, NaN rows matched positionally, every other row by `f64` bit
pattern) — every asserted number comes from the golden file, none is hand-computed. All 40
measured bit-identical to their goldens (round-1 probe: `maxdev = 0.0`, NaN-prefix aligned,
`f64` bit-equal on the dense suffix; re-proven on the unit tree by the shipped examples'
bit-exact control under the gate), so none stayed on the backlog, no
registry §7 row was filed, and no pin file was created (`test_examples_window_catalog.py` is
unchanged). No JVM and no network; `ta.MAX`/`ta.MIN`/`ta.SUM` are taught through the uppercase
TA-Lib-name aliases themselves.

**Roster (40):** `ta.MAX`, `ta.MIN`, `ta.SUM`, `ta.ad`, `ta.adosc`, `ta.adx`, `ta.adxr`,
`ta.apo`, `ta.aroon_down`, `ta.aroon_up`, `ta.aroonosc`, `ta.atr`, `ta.avgprice`,
`ta.bbands_lower`, `ta.bbands_middle`, `ta.bbands_upper`, `ta.beta`, `ta.bop`, `ta.cci`,
`ta.cmo`, `ta.correl`, `ta.dema`, `ta.dx`, `ta.ema`, `ta.fama`, `ta.kama`, `ta.linearreg`,
`ta.linearreg_angle`, `ta.linearreg_intercept`, `ta.linearreg_slope`, `ta.ma`, `ta.macd`,
`ta.macd_hist`, `ta.macd_signal`, `ta.macdext`, `ta.macdext_hist`, `ta.macdext_signal`,
`ta.macdfix`, `ta.macdfix_hist`, `ta.macdfix_signal`.

**Grouping (8 files, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `ta/overlap_studies.py` | `ta.ema`, `ta.dema`, `ta.kama`, `ta.ma`, `ta.bbands_upper`, `ta.bbands_middle`, `ta.bbands_lower`, `ta.fama` | The overlap studies: the MA family on close, the three Bollinger bands at 20/2.0/2.0, and the MAMA family's fama output. |
| `ta/rolling_extremes.py` | `ta.MAX`, `ta.MIN`, `ta.SUM` | The rolling extremes at period 21, called through the uppercase TA-Lib-name aliases. |
| `ta/momentum.py` | `ta.apo`, `ta.aroon_down`, `ta.aroon_up`, `ta.aroonosc`, `ta.bop`, `ta.cci`, `ta.cmo`, `ta.adx`, `ta.adxr`, `ta.dx` | Momentum plus the directional-movement trio (TA-Lib groups ADX here): Aroon split + oscillator, BOP, CCI, CMO, APO at 12/26 SMA. |
| `ta/macd.py` | `ta.macd`, `ta.macd_signal`, `ta.macd_hist`, `ta.macdext`, `ta.macdext_signal`, `ta.macdext_hist`, `ta.macdfix`, `ta.macdfix_signal`, `ta.macdfix_hist` | The MACD family at the 12/26/9 defaults: each variant selects its three split outputs in one pass over the window. |
| `ta/regression.py` | `ta.linearreg`, `ta.linearreg_angle`, `ta.linearreg_intercept`, `ta.linearreg_slope`, `ta.beta`, `ta.correl` | The statistic functions: the linear-regression family and the two-series rolling beta/correl on high vs low. |
| `ta/volatility.py` | `ta.atr` | ATR(14) over high/low/close. |
| `ta/volume.py` | `ta.ad`, `ta.adosc` | The Chaikin pair: AD and ADOSC at the recorded 3/10 periods. |
| `ta/price_transforms.py` | `ta.avgprice` | The no-period OHLC average. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Eight runnable files under `docs/examples/ta/` land local examples for all 40 roster names, each script rebuilding the 5000-row OHLCV fixture as a `createDataFrame`, running the kernel over `Window.orderBy("ts")`, and asserting the full 5000-row output bit-for-bit against the recorded golden read from `crates/repark-ta/tests/goldens` at run time (`expect_bit_exact`: equal length, NaN rows matched positionally, every other row by `f64` bit pattern) — no hand-computed number in any example; each script exits 0 under `python <path>` with no network and no JVM; no product file is touched. | The shipped examples themselves: the `--require-execute` gate executes every `expect_bit_exact` assert over all 5000 rows on this tree (exit 0, round-2 gates table); the oracle table (40 rows, one per roster name) remains the round-1 per-name measurement record. | **PROVEN** |
| C-002 | All 40 covered names leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 40, 298 → 258 shipped (340 → 300 at the dispatch base `671a7144`), with no other `scripts/` change; the gate's static half and its `--require-execute` leg both exit 0 (653 covered; 258 backlog; 173 examples on the shipped tip; 611 / 300 / 162 at the dispatch base). | The gate's own counts line at the base `bfef4a62` (613/298/165) and on the shipped tree (653/258/173) — 571/340/154 → 611/300/162 at the dispatch base `671a7144` — plus the red-first provocation below. | **PROVEN** |
| C-003 | A name whose repark answer differs from its golden is not papered over: the 40-name probe measured zero divergences (`maxdev = 0.0`, NaN pattern aligned, bit-equal dense suffix on every row), so no §7 row is filed, no name stays on the backlog, and `python/repark/tests/test_examples_window_catalog.py` is unchanged; a future divergence would follow the EX-TA-`<n>` row + pin route. | The shipped examples' bit-exact control over the full 5000-row golden — executed by the gate on every run — plus the round-2 red-first re-run (four mutations, all exit 1; table in "Red-first") and the unchanged pin file (`git diff --exit-code python/repark/tests/test_examples_window_catalog.py` on this tree). | **PROVEN** |
| C-004 | This ledger records the roster, the grouping, the red-first provocation, and the name-by-name oracle table; `staging/map.md` gains the EX-23 row; `docs/examples/map.md` and `docs/examples/ta/map.md` move in lockstep with the files; `scripts/map.md` carries the baseline-ratchet entry with the pin citations. | The ledger itself and the lockstep map diffs in the same commits. | **PROVEN** |

`LOGIC_SCORE` = **4/4 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured on this tree with the eight example files held outside `docs/examples/` and
`docs/examples/backlog.txt` + `scripts/check_example_coverage.py` restored to the base state
(round 1, at the dispatch base `671a7144`, the 40 roster rows still listed, `BACKLOG_BASELINE=340`): the static gate exits **0**
(`571 covered; 340 backlog; 154 examples`). **Provocation:** delete all 40 roster rows and set
`BACKLOG_BASELINE` to 300 (`340 − 40`, as if the whole roster were covered) with no example
files present; the gate exits **1** with exactly **40 findings**, one per roster name and no
others. Restoring the rows, the baseline 300, and the eight files returns the gate to **0**
(`611 covered; 300 backlog; 162 examples` at that base). Re-run on the shipped tip `83d682d3` after the EX-22 merge: base `bfef4a62` prints `613 covered; 298 backlog; 165 examples`; the tip prints `653 covered; 258 backlog; 173 examples` with `BACKLOG_BASELINE` 298 → 258 — the same +40 / −40 / +8. `pins: ex-23-ta-a/C-001, C-002`

### Round-2 red-first re-run — the bit-exact control (2026-09-04)

Round 2 replaced `expect_tail` with `expect_bit_exact`, so the provocations re-ran against the
new control: one provocation at a time injected into `ta/volatility.py` on this tree (temporary,
never committed, restored before the next), full gate
`.venv/bin/python scripts/check_example_coverage.py --require-execute` each time. The gate names
the failing script and the control names the kernel, the first mismatching row and both values.
Clean runs before and after the series: **0**. `pins: ex-23-ta-a/C-001, C-003`

| Provocation (temporary, reverted) | Gate finding (verbatim detail) | Exit |
|---|---|---|
| first 4995 output rows overwritten with 999.0, tail intact (the class the tail-only control passed) | `example /tmp/oc-ex23/docs/examples/ta/volatility.py exited 1: … ta.atr: bit mismatch at row 0: got 999.0 vs golden nan` | **1** |
| NaN prefix shifted by one row (first finite output row set NaN, tail intact) | `… exited 1: … ta.atr: bit mismatch at row 14: got nan vs golden 1.2067907115794319` | **1** |
| last output value perturbed by 1e-9 (inside the old atol=1e-9 tolerance) | `… exited 1: … ta.atr: bit mismatch at row 4999: got 0.3960501768356986 vs golden 0.39605017583569857` | **1** |
| kernel period 14 → 13 | `… exited 1: … ta.atr: bit mismatch at row 13: got 1.2529424998767853 vs golden nan` | **1** |

## Oracle (recorded C TA-Lib 0.4.0 goldens — Spark has no TA kernels)

Measured with `.venv/bin/python`, throwaway probe `scratch/ex23-ta-probe/probe.py` (gitignored,
never committed): one `createDataFrame` of the 5000-row OHLCV fixture
(`fixture_{open,high,low,close,volume}.bin`), each kernel over `Window.orderBy("ts")`, the
collected column compared to the golden `np.frombuffer(..., dtype="<f8")` array index-for-index
(NaN pattern equality, `f64` bit equality and `maxdev` on the dense suffix). Parameters are the
recorded spellings the crate's golden suite and `test_ta.py` pin. The gate's execute leg then
re-ran every assert green. No Spark JVM was started; `python3` on this box cannot import
`repark._native`, so the `--require-execute` leg runs under `.venv/bin/python`, which resolves
`repark` to the sibling checkout of the same base SHA `671a7144` (expected for this lane).
Round 2: the probe is the measurement instrument for the table above only — the durable evidence
for C-001/C-003 is now the shipped examples themselves, whose `expect_bit_exact` control the
`--require-execute` gate executes over all 5000 rows on every run (the probe was gitignored, so
nothing executed at CI time could see it), and the round-2 red-first re-run proves the control
detects the mutation classes the tail-only control missed.
`pins: ex-23-ta-a/C-001, C-003`

| Name | Golden | repark call (over `Window.orderBy("ts")`) | Result vs golden | File |
|---|---|---|---|---|
| `ta.MAX` | `max_21.bin` | `ta.MAX("close", timeperiod=21)` | bit-identical, `maxdev 0.0` | `ta/rolling_extremes.py` |
| `ta.MIN` | `min_21.bin` | `ta.MIN("close", timeperiod=21)` | bit-identical, `maxdev 0.0` | `ta/rolling_extremes.py` |
| `ta.SUM` | `sum_21.bin` | `ta.SUM("close", timeperiod=21)` | bit-identical, `maxdev 0.0` | `ta/rolling_extremes.py` |
| `ta.ad` | `ad.bin` | `ta.ad("high", "low", "close", "volume")` | bit-identical, `maxdev 0.0` | `ta/volume.py` |
| `ta.adosc` | `adosc_3_10.bin` | `ta.adosc("high", "low", "close", "volume", fastperiod=3, slowperiod=10)` | bit-identical, `maxdev 0.0` | `ta/volume.py` |
| `ta.adx` | `adx_14.bin` | `ta.adx("high", "low", "close", timeperiod=14)` | bit-identical, `maxdev 0.0` | `ta/momentum.py` |
| `ta.adxr` | `adxr_14.bin` | `ta.adxr("high", "low", "close", timeperiod=14)` | bit-identical, `maxdev 0.0` | `ta/momentum.py` |
| `ta.apo` | `apo_12_26.bin` | `ta.apo("close", fastperiod=12, slowperiod=26, matype=0)` | bit-identical, `maxdev 0.0` | `ta/momentum.py` |
| `ta.aroon_down` | `aroon_14_down.bin` | `ta.aroon_down("high", "low", timeperiod=14)` | bit-identical, `maxdev 0.0` | `ta/momentum.py` |
| `ta.aroon_up` | `aroon_14_up.bin` | `ta.aroon_up("high", "low", timeperiod=14)` | bit-identical, `maxdev 0.0` | `ta/momentum.py` |
| `ta.aroonosc` | `aroonosc_14.bin` | `ta.aroonosc("high", "low", timeperiod=14)` | bit-identical, `maxdev 0.0` | `ta/momentum.py` |
| `ta.atr` | `atr_14.bin` | `ta.atr("high", "low", "close", timeperiod=14)` | bit-identical, `maxdev 0.0` | `ta/volatility.py` |
| `ta.avgprice` | `avgprice.bin` | `ta.avgprice("open", "high", "low", "close")` | bit-identical, `maxdev 0.0` | `ta/price_transforms.py` |
| `ta.bbands_lower` | `bbands_20_lower.bin` | `ta.bbands_lower("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0)` | bit-identical, `maxdev 0.0` | `ta/overlap_studies.py` |
| `ta.bbands_middle` | `bbands_20_middle.bin` | `ta.bbands_middle("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0)` | bit-identical, `maxdev 0.0` | `ta/overlap_studies.py` |
| `ta.bbands_upper` | `bbands_20_upper.bin` | `ta.bbands_upper("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0)` | bit-identical, `maxdev 0.0` | `ta/overlap_studies.py` |
| `ta.beta` | `beta_5.bin` | `ta.beta("high", "low", timeperiod=5)` | bit-identical, `maxdev 0.0` | `ta/regression.py` |
| `ta.bop` | `bop.bin` | `ta.bop("open", "high", "low", "close")` | bit-identical, `maxdev 0.0` | `ta/momentum.py` |
| `ta.cci` | `cci_14.bin` | `ta.cci("high", "low", "close", timeperiod=14)` | bit-identical, `maxdev 0.0` | `ta/momentum.py` |
| `ta.cmo` | `cmo_14.bin` | `ta.cmo("close", timeperiod=14)` | bit-identical, `maxdev 0.0` | `ta/momentum.py` |
| `ta.correl` | `correl_14.bin` | `ta.correl("high", "low", timeperiod=14)` | bit-identical, `maxdev 0.0` | `ta/regression.py` |
| `ta.dema` | `dema_10.bin` | `ta.dema("close", timeperiod=10)` | bit-identical, `maxdev 0.0` | `ta/overlap_studies.py` |
| `ta.dx` | `dx_14.bin` | `ta.dx("high", "low", "close", timeperiod=14)` | bit-identical, `maxdev 0.0` | `ta/momentum.py` |
| `ta.ema` | `ema_21.bin` | `ta.ema("close", timeperiod=21)` | bit-identical, `maxdev 0.0` | `ta/overlap_studies.py` |
| `ta.fama` | `mama_fama.bin` | `ta.fama("close", fastlimit=0.5, slowlimit=0.05)` | bit-identical, `maxdev 0.0` | `ta/overlap_studies.py` |
| `ta.kama` | `kama_10.bin` | `ta.kama("close", timeperiod=10)` | bit-identical, `maxdev 0.0` | `ta/overlap_studies.py` |
| `ta.linearreg` | `linearreg_5.bin` | `ta.linearreg("close", timeperiod=5)` | bit-identical, `maxdev 0.0` | `ta/regression.py` |
| `ta.linearreg_angle` | `linearreg_angle_14.bin` | `ta.linearreg_angle("close", timeperiod=14)` | bit-identical, `maxdev 0.0` | `ta/regression.py` |
| `ta.linearreg_intercept` | `linearreg_intercept_5.bin` | `ta.linearreg_intercept("close", timeperiod=5)` | bit-identical, `maxdev 0.0` | `ta/regression.py` |
| `ta.linearreg_slope` | `linearreg_slope_5.bin` | `ta.linearreg_slope("close", timeperiod=5)` | bit-identical, `maxdev 0.0` | `ta/regression.py` |
| `ta.ma` | `ma_30_type0.bin` | `ta.ma("close", timeperiod=30, matype=0)` | bit-identical, `maxdev 0.0` | `ta/overlap_studies.py` |
| `ta.macd` | `macd_12_26_9_macd.bin` | `ta.macd("close")` | bit-identical, `maxdev 0.0` | `ta/macd.py` |
| `ta.macd_hist` | `macd_12_26_9_hist.bin` | `ta.macd_hist("close")` | bit-identical, `maxdev 0.0` | `ta/macd.py` |
| `ta.macd_signal` | `macd_12_26_9_signal.bin` | `ta.macd_signal("close")` | bit-identical, `maxdev 0.0` | `ta/macd.py` |
| `ta.macdext` | `macdext_12_26_9_macd.bin` | `ta.macdext("close")` | bit-identical, `maxdev 0.0` | `ta/macd.py` |
| `ta.macdext_hist` | `macdext_12_26_9_hist.bin` | `ta.macdext_hist("close")` | bit-identical, `maxdev 0.0` | `ta/macd.py` |
| `ta.macdext_signal` | `macdext_12_26_9_signal.bin` | `ta.macdext_signal("close")` | bit-identical, `maxdev 0.0` | `ta/macd.py` |
| `ta.macdfix` | `macdfix_9_macd.bin` | `ta.macdfix("close")` | bit-identical, `maxdev 0.0` | `ta/macd.py` |
| `ta.macdfix_hist` | `macdfix_9_hist.bin` | `ta.macdfix_hist("close")` | bit-identical, `maxdev 0.0` | `ta/macd.py` |
| `ta.macdfix_signal` | `macdfix_9_signal.bin` | `ta.macdfix_signal("close")` | bit-identical, `maxdev 0.0` | `ta/macd.py` |

## Gates (2026-09-04, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_window_catalog.py -q` | **0** (9 passed; the file is untouched by this unit) |
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

### Round-2 gates (2026-09-04, critic remediation tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** (`653 covered; 258 backlog; 173 examples` on the shipped tip after the EX-22 merge; `611 / 300 / 162` at the dispatch base; all eight examples re-executed over the bit-exact control) |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_window_catalog.py python/repark/tests/test_qi1_idents.py -q` | **0** |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `typos .` | **0** |
| `.venv/bin/ruff check docs/examples python/repark/tests scripts` | **0** |
| `.venv/bin/ruff format --check docs/examples python/repark/tests scripts` | **0** |
| `git diff origin/main -- '*.py'` scanned for added `#` lines | no added `#` line beyond `# noqa` (the comment ban; grep exits 1 on no match — absence is the pass) |

Counts line (execute leg, shipped tip `83d682d3` after the EX-22 merge; the dispatch-base run printed `611 covered; 300 backlog; 2 exceptions; 162 examples`):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 653 covered; 258 backlog; 2 exceptions; 173 examples`

Before this unit: `613 covered; 298 backlog; 165 examples` (at `bfef4a62`; `571 / 340 / 154` at the dispatch base `671a7144`). On this unit's tree:
`653 covered; 258 backlog; 173 examples` (`BACKLOG_BASELINE` 298 → 258 shipped; 340 → 300 at dispatch) — exactly the 40 roster
names.

## Review-gap table (round-1 findings, resolved in-lane)

| Finding | Disposition |
|---|---|
| the first provocation set `BACKLOG_BASELINE` to 299 instead of 300 (`340 − 40`), producing a 41st baseline-ratchet finding beside the 40 roster findings | redone with 300: exit 1 with exactly 40 findings, one per roster name and no others; the gate's own message ("backlog count is 300, baseline is 299") is what caught it |
| the first `momentum.py` draft exceeded the 100-column lint on one `ta.apo` select line | caught by `ruff check`; `ruff format` reflowed the file before any commit |

### Round 2 (critic findings, resolved in-lane 2026-09-04)

| Finding | Disposition |
|---|---|
| F1 (S2): the durable control was tail-only and blind to the NaN prefix — `expect_tail` asserted only the last five non-NaN values at atol=1e-9 in all eight files; the critic proved an example still exits 0 after overwriting the first 4995 rows with 999.0 or shifting the NaN prefix by one | replaced in all eight files by `expect_bit_exact(name, got, golden)`: equal length (5000), NaN rows matched positionally, every other row by `f64` bit pattern — the same property `assert_bit_exact` holds in `crates/repark-ta/tests/goldens.rs`; raises `SystemExit` naming the kernel, the first mismatching row and both values; the numpy comparison is microseconds, far inside the gate's 120 s per-script timeout. C-001/C-003 evidence re-pointed from the gitignored `scratch/ex23-ta-probe/probe.py` to the shipped examples the gate executes. Red-first re-run recorded with exit codes in "Round-2 red-first re-run": bulk overwrite → exit 1 (row 0); prefix shift by one → exit 1 (row 14); tail 1e-9 → exit 1 (row 4999); period 14 → 13 → exit 1 (row 13) |
| F2 (S2): 24 helper docstrings breached the example house form (`na_surface.py`: one module docstring plus the one-line `main()` docstring; helpers bare) | all 24 deleted in the eight files, nothing written in their place; the module docstring and the `main()` line are the only docstrings left in each file |
| F3 (S3, accepted, no action): the three helpers are duplicated across the eight files — examples are self-contained scripts by design | left duplicated as accepted; after F1/F2 the eight helper copies are byte-identical (verified by checksum over the `golden`/`column`/`expect_bit_exact` block) |

## Cost

The GLM (glm-5.3-flash) leg started 2026-09-04: read the contract, the corpus (`sma.py`, the
merged EX-22 ledger from `FETCH_HEAD`, `test_ta.py` / `test_ta_volume.py` for the golden-loading
and window idioms), and the gate; ran the 40-name probe against the recorded goldens (JVM-free);
wrote the eight example files, the backlog ratchet, the maps, and this ledger, then committed in
slices. Base `671a7144`. Round 2 (same day, critic remediation): replaced the tail-only control
with the full-array bit-exact control in all eight files, stripped the 24 helper docstrings,
re-ran the provocations red-first against the new control, and re-pointed C-001/C-003 evidence
from the gitignored probe to the shipped examples.

## Disk

Pickup: `df -h` 602 GB free of 1.8 TB. The oracle probe and the provocation scratch live under
the gitignored `scratch/ex23-ta-probe/` (probe script, captured red/green logs, held-aside
example copies — all removable at close). No build artifacts created: no cargo build, `make
develop` not run; `.venv` and the sibling-checkout native module reused.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-23 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-23-ta-a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 40 roster names are covered by eight new example files and the oracle table records the golden, the call, and the measured result for all 40 rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/ta/overlap_studies.py, docs/examples/ta/rolling_extremes.py, docs/examples/ta/momentum.py, docs/examples/ta/macd.py, docs/examples/ta/regression.py, docs/examples/ta/volatility.py, docs/examples/ta/volume.py, docs/examples/ta/price_transforms.py]
    - id: AT-2
      status: ATTACKED
      evidence: The red-first provocation deleted all 40 roster rows and set the baseline to 300 with no example files; the gate exited 1 with exactly 40 findings, and the backlog is an exact baseline 300.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: The gate's use-check binds every ta.* COVERS name on the ta door alias; the examples call each kernel through ta.<name> including the uppercase MAX/MIN/SUM aliases, so a dropped call is an unused-cover red.
      artifacts: [scripts/check_example_coverage.py, docs/examples/ta/rolling_extremes.py, docs/examples/ta/macd.py]
    - id: AT-4
      status: N/A
      justification: The gate and the probe are read-only processes over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the eight local examples; they read repo goldens by a __file__-derived path, drop AWS_* and PYTHONPATH in the gate's child, and touch no network or cloud service.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; the red-first provocation ran the AST-only half with exactly 40 findings.
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
      evidence: The pins citations for C-001..C-004 live in scripts/map.md beside the prior example batches, the example docstrings cite ex-23-ta-a/C-001, and this ledger cites its clauses in the red-first and oracle sections.
      artifacts: [scripts/map.md, docs/examples/ta/overlap_studies.py, task/ledgers/staging/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Goldens: [../../../crates/repark-ta/tests/goldens/map.md](../../../crates/repark-ta/tests/goldens/map.md)
- Pins: [../../../python/repark/tests/test_ta.py](../../../python/repark/tests/test_ta.py) (the bit-exact DataFrame-route pins over the same goldens; no new pin file — zero divergences)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 (no row filed from this unit)
- Siblings: [ex-21-catalog-session-ledger.md](ex-21-catalog-session-ledger.md), [ex-20-window-catalog-ledger.md](ex-20-window-catalog-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-23-ta-a
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-23-ta-a
  artifacts_verified:
    ledger: PASS (C-001..C-004 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review-gap table carries the in-lane round-1 resolutions and the round-2 critic remediation F1–F3)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, TA kernels (a) — 40 covered, zero divergences, no registry row
  verdict: PENDING
  rejection_route: N/A
```
