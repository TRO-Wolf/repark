# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "numpy==2.3.2",
#     "polars==1.32.3",
#     "polars-talib==0.1.5",
# ]
# ///
"""Record the C-TA-Lib golden fixtures for the `repark-ta` bit-exactness gate.

The oracle is `polars_talib`, which bundles **C TA-Lib 0.4.0** — the exact library the
pipeline's models were trained against. This script generates two deterministic fixtures — a
5000-row OHLC random walk, and a 600-row series with a 300-bar dead-flat plateau (which drives
the `TA_IS_ZERO` guard branches a smooth walk never reaches) — plus a strictly-positive
`volume` column on both (dedicated RNGs; seeds 4242 / 77 — never the OHLC RNGs) — computes
every indicator/param set the `repark-ta` golden tests cover, and writes raw little-endian
f64 bit patterns (`.bin`, one u64 per row; nulls recorded as NaN) plus a `manifest.json`
into `crates/repark-ta/tests/goldens/`. Writes are temp-file + atomic rename, so an
interrupted run cannot leave a half-written file.

Run from the repo root (dependencies resolve from the PEP-723 header):

    uv run python/repark-parity/record_ta_goldens.py

Re-record ONLY when adding series or deliberately moving the oracle version; both the bundled
TA-Lib version and the polars-talib wrapper version are asserted below so silent oracle drift
fails loudly.
"""

from __future__ import annotations

import importlib.metadata
import json
import logging
import struct
from pathlib import Path

import numpy as np
import polars as pl
import polars_talib as plta

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("record_ta_goldens")

EXPECTED_TALIB_VERSION_PREFIX = "0.4.0"
EXPECTED_WRAPPER_VERSION = "0.1.5"
N_ROWS = 5000
N_ROWS_FLAT = 600
# Dedicated volume RNGs — independent of the OHLC seeds (walk=42, flat=7) so existing
# fixture_{open,high,low,close,periods} and fixture_flat_* bytes stay byte-identical.
WALK_VOLUME_SEED = 4242
FLAT_VOLUME_SEED = 77
# Lognormal ~1e6 geometric mean; always strictly positive.
VOLUME_LOG_MEAN = float(np.log(1_000_000.0))
VOLUME_LOG_SIGMA = 0.35
OUT_DIR = Path(__file__).resolve().parents[2] / "crates" / "repark-ta" / "tests" / "goldens"


def positive_volume(n_rows: int, seed: int) -> np.ndarray:
    """Strictly-positive lognormal volume from a dedicated RNG (never the OHLC RNG)."""
    volume_rng = np.random.default_rng(seed)
    return np.exp(volume_rng.normal(VOLUME_LOG_MEAN, VOLUME_LOG_SIGMA, n_rows))


def series_bits(values: list[float | None]) -> bytes:
    """Encode a value series as little-endian f64 bit patterns (null → NaN)."""
    nan = float("nan")
    return b"".join(struct.pack("<d", nan if v is None else v) for v in values)


def write_atomic(path: Path, data: bytes) -> None:
    """Write via temp file + rename so an interrupted run never leaves a partial file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def walk_fixture() -> pl.DataFrame:
    """The 5000-row lognormal OHLC random walk (numpy `default_rng(42)`)."""
    rng = np.random.default_rng(42)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0.0, 0.01, N_ROWS)))
    spread_hi = np.abs(rng.normal(0.0, 0.004, N_ROWS))
    spread_lo = np.abs(rng.normal(0.0, 0.004, N_ROWS))
    high = close * (1.0 + spread_hi)
    low = close * (1.0 - spread_lo)
    open_ = low + (high - low) * rng.uniform(0.0, 1.0, N_ROWS)
    # Per-row variable period for MAVP, deterministic (cycles 2..30 so a min=5/max=20 clamp fires at
    # both ends). Drawn WITHOUT the RNG so the OHLC bytes above stay byte-for-byte unchanged (the
    # goldens gate is additive-only).
    periods = 2.0 + (np.arange(N_ROWS) % 29).astype(np.float64)
    # Volume: dedicated seed 4242 — does not consume the OHLC RNG (seed 42).
    volume = positive_volume(N_ROWS, WALK_VOLUME_SEED)
    return pl.DataFrame(
        {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "periods": periods,
            "volume": volume,
        }
    )


def flat_fixture() -> pl.DataFrame:
    """600 rows: 150-bar walk → 300-bar dead-flat plateau (high == low == close, unchanged —
    a halted symbol / market holiday) → 150-bar walk. The plateau is long enough for the
    Wilder-decayed accumulators (RSI gain+loss, ADX prevTR) to decay under TA-Lib's 1e-8
    epsilon, so the `TA_IS_ZERO` / `TA_IS_ZERO_OR_NEG` guards genuinely fire."""
    rng = np.random.default_rng(7)
    seg_a = 100.0 * np.exp(np.cumsum(rng.normal(0.0, 0.01, 150)))
    plateau = np.full(300, seg_a[-1])
    seg_c = seg_a[-1] * np.exp(np.cumsum(rng.normal(0.0, 0.01, 150)))
    close = np.concatenate([seg_a, plateau, seg_c])
    spread = np.abs(rng.normal(0.0, 0.004, N_ROWS_FLAT))
    spread[150:450] = 0.0
    high = close * (1.0 + spread)
    low = close * (1.0 - spread)
    # `open` is drawn AFTER the spread so the high/low/close bytes are unchanged (additive). On the
    # dead-flat plateau high == low, so `open` collapses onto them — BOP's zero-range guard fires.
    open_ = low + (high - low) * rng.uniform(0.0, 1.0, N_ROWS_FLAT)
    # Volume: dedicated seed 77 — does not consume the OHLC RNG (seed 7). Stays strictly
    # positive through the price plateau so AD's `tmp > 0.0` skip, OBV's equal-close hold,
    # and MFI's zero-delta / `pos+neg < 1.0` guards fire with real volume.
    volume = positive_volume(N_ROWS_FLAT, FLAT_VOLUME_SEED)
    return pl.DataFrame({"open": open_, "high": high, "low": low, "close": close, "volume": volume})


def walk_cases() -> dict[str, pl.Expr]:
    c, h, lo, o = pl.col("close"), pl.col("high"), pl.col("low"), pl.col("open")
    p = pl.col("periods")
    v = pl.col("volume")
    mama = plta.mama(c, fastlimit=0.5, slowlimit=0.05)
    bb = plta.bbands(c, timeperiod=20, nbdevup=2.0, nbdevdn=2.0)
    aroon = plta.aroon(h, lo, timeperiod=14)
    bb_unit = plta.bbands(c, timeperiod=20, nbdevup=1.0, nbdevdn=1.0)
    bb_up1 = plta.bbands(c, timeperiod=20, nbdevup=1.0, nbdevdn=2.5)
    bb_dn1 = plta.bbands(c, timeperiod=20, nbdevup=2.5, nbdevdn=1.0)
    bb_asym = plta.bbands(c, timeperiod=20, nbdevup=1.5, nbdevdn=2.5)
    macd = plta.macd(c, fastperiod=12, slowperiod=26, signalperiod=9)
    macdfix = plta.macdfix(c, signalperiod=9)
    macdext = plta.macdext(
        c,
        fastperiod=12,
        fastmatype=0,
        slowperiod=26,
        slowmatype=0,
        signalperiod=9,
        signalmatype=0,
    )
    # Matype-7 (MAMA): APO/PPO all-MAMA; MACDEXT all-MAMA + mixed 7/0/1.
    macdext_mama = plta.macdext(
        c,
        fastperiod=12,
        fastmatype=7,
        slowperiod=26,
        slowmatype=7,
        signalperiod=9,
        signalmatype=7,
    )
    macdext_mixed_mama = plta.macdext(
        c,
        fastperiod=12,
        fastmatype=7,
        slowperiod=26,
        slowmatype=0,
        signalperiod=9,
        signalmatype=1,
    )
    stoch = plta.stoch(
        h, lo, c, fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0
    )
    # Matype-7 (MAMA) stochastic smoothing: all-MAMA + mixed slowk=MAMA/slowd=SMA.
    stoch_mama = plta.stoch(
        h, lo, c, fastk_period=5, slowk_period=3, slowk_matype=7, slowd_period=3, slowd_matype=7
    )
    stoch_mixed_mama = plta.stoch(
        h, lo, c, fastk_period=5, slowk_period=3, slowk_matype=7, slowd_period=3, slowd_matype=0
    )
    stochf = plta.stochf(h, lo, c, fastk_period=5, fastd_period=3, fastd_matype=0)
    stochf_mama = plta.stochf(h, lo, c, fastk_period=5, fastd_period=3, fastd_matype=7)
    stochrsi = plta.stochrsi(c, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
    stochrsi_mama = plta.stochrsi(c, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=7)
    return {
        "sma_2": plta.sma(c, timeperiod=2),
        "sma_20": plta.sma(c, timeperiod=20),
        "ema_5": plta.ema(c, timeperiod=5),
        "ema_8": plta.ema(c, timeperiod=8),
        "ema_21": plta.ema(c, timeperiod=21),
        "rsi_3": plta.rsi(c, timeperiod=3),
        "rsi_14": plta.rsi(c, timeperiod=14),
        "trange": plta.trange(h, lo, c),
        "atr_1": plta.atr(h, lo, c, timeperiod=1),
        "atr_14": plta.atr(h, lo, c, timeperiod=14),
        "adx_14": plta.adx(h, lo, c, timeperiod=14),
        "var_5": plta.var(c, timeperiod=5, nbdev=1.0),
        "stddev_5_nbdev1": plta.stddev(c, timeperiod=5, nbdev=1.0),
        "stddev_5_nbdev2": plta.stddev(c, timeperiod=5, nbdev=2.0),
        "linearreg_5": plta.linearreg(c, timeperiod=5),
        "linearreg_slope_5": plta.linearreg_slope(c, timeperiod=5),
        "linearreg_intercept_5": plta.linearreg_intercept(c, timeperiod=5),
        "linearreg_angle_2": plta.linearreg_angle(c, timeperiod=2),
        "linearreg_angle_14": plta.linearreg_angle(c, timeperiod=14),
        "tsf_5": plta.tsf(c, timeperiod=5),
        "min_21": plta.min(c, timeperiod=21),
        "min_34": plta.min(c, timeperiod=34),
        "max_21": plta.max(c, timeperiod=21),
        "sum_21": plta.sum(c, timeperiod=21),
        "correl_14": plta.correl(h, lo, timeperiod=14),
        # WG1 overlap MA family.
        "wma_10": plta.wma(c, timeperiod=10),
        "dema_10": plta.dema(c, timeperiod=10),
        "tema_10": plta.tema(c, timeperiod=10),
        "trima_10": plta.trima(c, timeperiod=10),  # even period branch
        "trima_5": plta.trima(c, timeperiod=5),  # odd period branch
        "kama_10": plta.kama(c, timeperiod=10),
        "t3_5": plta.t3(c, timeperiod=5, vfactor=0.7),  # default vfactor
        "t3_5_vf05": plta.t3(c, timeperiod=5, vfactor=0.5),  # vfactor threading
        "midpoint_10": plta.midpoint(c, timeperiod=10),
        "midprice_10": plta.midprice(h, lo, timeperiod=10),
        "bbands_20_upper": bb.struct.field("upperband"),
        "bbands_20_middle": bb.struct.field("middleband"),
        "bbands_20_lower": bb.struct.field("lowerband"),
        # The other rounding-distinct band branches (middle is nbdev-independent).
        "bbands_20_unit_upper": bb_unit.struct.field("upperband"),
        "bbands_20_unit_lower": bb_unit.struct.field("lowerband"),
        "bbands_20_up1_lower": bb_up1.struct.field("lowerband"),
        "bbands_20_up1_upper": bb_up1.struct.field("upperband"),
        "bbands_20_dn1_upper": bb_dn1.struct.field("upperband"),
        "bbands_20_dn1_lower": bb_dn1.struct.field("lowerband"),
        "bbands_20_asym_upper": bb_asym.struct.field("upperband"),
        "bbands_20_asym_lower": bb_asym.struct.field("lowerband"),
        # WG2 simple-momentum batch (defaults; APO/PPO at matype=0 = SMA).
        "mom_10": plta.mom(c, timeperiod=10),
        "roc_10": plta.roc(c, timeperiod=10),
        "rocp_10": plta.rocp(c, timeperiod=10),
        "rocr_10": plta.rocr(c, timeperiod=10),
        "rocr100_10": plta.rocr100(c, timeperiod=10),
        "willr_14": plta.willr(h, lo, c, timeperiod=14),
        "cci_14": plta.cci(h, lo, c, timeperiod=14),
        "cmo_14": plta.cmo(c, timeperiod=14),
        "bop": plta.bop(o, h, lo, c),
        "apo_12_26": plta.apo(c, fastperiod=12, slowperiod=26, matype=0),
        "ppo_12_26": plta.ppo(c, fastperiod=12, slowperiod=26, matype=0),
        # Matype 7 (MAMA) — APO/PPO route both legs through TA_MA → MAMA(0.5, 0.05).
        "apo_12_26_type7": plta.apo(c, fastperiod=12, slowperiod=26, matype=7),
        "ppo_12_26_type7": plta.ppo(c, fastperiod=12, slowperiod=26, matype=7),
        "aroon_14_down": aroon.struct.field("aroondown"),
        "aroon_14_up": aroon.struct.field("aroonup"),
        "aroonosc_14": plta.aroonosc(h, lo, timeperiod=14),
        "trix_30": plta.trix(c, timeperiod=30),
        "ultosc_7_14_28": plta.ultosc(h, lo, c, timeperiod1=7, timeperiod2=14, timeperiod3=28),
        # WG3 directional family (DI/DM allow period 1; recorded at the canonical 14).
        "dx_14": plta.dx(h, lo, c, timeperiod=14),
        "adxr_14": plta.adxr(h, lo, c, timeperiod=14),
        "plus_di_14": plta.plus_di(h, lo, c, timeperiod=14),
        "minus_di_14": plta.minus_di(h, lo, c, timeperiod=14),
        "plus_dm_14": plta.plus_dm(h, lo, timeperiod=14),
        "minus_dm_14": plta.minus_dm(h, lo, timeperiod=14),
        # WG3 MACD family (split into the three TA-Lib outputs). MACDFIX pins 12/26 with fixed
        # constants; MACDEXT is recorded at the matype-0 (SMA) defaults.
        "macd_12_26_9_macd": macd.struct.field("macd"),
        "macd_12_26_9_signal": macd.struct.field("macdsignal"),
        "macd_12_26_9_hist": macd.struct.field("macdhist"),
        "macdfix_9_macd": macdfix.struct.field("macd"),
        "macdfix_9_signal": macdfix.struct.field("macdsignal"),
        "macdfix_9_hist": macdfix.struct.field("macdhist"),
        "macdext_12_26_9_macd": macdext.struct.field("macd"),
        "macdext_12_26_9_signal": macdext.struct.field("macdsignal"),
        "macdext_12_26_9_hist": macdext.struct.field("macdhist"),
        "macdext_12_26_9_type7_macd": macdext_mama.struct.field("macd"),
        "macdext_12_26_9_type7_signal": macdext_mama.struct.field("macdsignal"),
        "macdext_12_26_9_type7_hist": macdext_mama.struct.field("macdhist"),
        "macdext_mixed_7_0_1_macd": macdext_mixed_mama.struct.field("macd"),
        "macdext_mixed_7_0_1_signal": macdext_mixed_mama.struct.field("macdsignal"),
        "macdext_mixed_7_0_1_hist": macdext_mixed_mama.struct.field("macdhist"),
        # WG3 MA selector: matype 0 (SMA) and matype 1 (EMA) prove the dispatch.
        "ma_30_type0": plta.ma(c, timeperiod=30, matype=0),
        "ma_20_type1": plta.ma(c, timeperiod=20, matype=1),
        # WG4 stochastics (split into the TA-Lib outputs; polars_talib defaults, matype 0 = SMA).
        "stoch_slowk": stoch.struct.field("slowk"),
        "stoch_slowd": stoch.struct.field("slowd"),
        "stochf_fastk": stochf.struct.field("fastk"),
        "stochf_fastd": stochf.struct.field("fastd"),
        "stochrsi_fastk": stochrsi.struct.field("fastk"),
        "stochrsi_fastd": stochrsi.struct.field("fastd"),
        # Group G2 — matype 7 (MAMA) on stochastic smoothing legs (each output leg separate).
        "stoch_type7_slowk": stoch_mama.struct.field("slowk"),
        "stoch_type7_slowd": stoch_mama.struct.field("slowd"),
        "stoch_mixed_7_0_slowk": stoch_mixed_mama.struct.field("slowk"),
        "stoch_mixed_7_0_slowd": stoch_mixed_mama.struct.field("slowd"),
        "stochf_type7_fastk": stochf_mama.struct.field("fastk"),
        "stochf_type7_fastd": stochf_mama.struct.field("fastd"),
        "stochrsi_type7_fastk": stochrsi_mama.struct.field("fastk"),
        "stochrsi_type7_fastd": stochrsi_mama.struct.field("fastd"),
        # WG5 sweep-up: NATR (ATR normalized by close), BETA (two-series rolling covariance slope,
        # high vs low mirroring the CORREL golden pairing), and the four O/H/L/C price transforms
        # (no period, lookback 0).
        "natr_14": plta.natr(h, lo, c, timeperiod=14),
        "beta_5": plta.beta(h, lo, timeperiod=5),
        "avgprice": plta.avgprice(o, h, lo, c),
        "medprice": plta.medprice(h, lo),
        "typprice": plta.typprice(h, lo, c),
        "wclprice": plta.wclprice(h, lo, c),
        # T3 — the parked four. MAMA is split into its two outputs (mama/fama, TA-Lib defaults
        # fastlimit 0.5 / slowlimit 0.05). SAR at the defaults (accel 0.02 / max 0.2). SAREXT three
        # ways: the auto default (startvalue 0 -> the -DM1 direction, symmetric af, no offset -- the
        # NEGATIVE short-side output), a forced-long start with an offset-on-reverse and asymmetric
        # long/short accelerations, and a forced-short start (startvalue < 0 → |value|). MAVP over
        # the per-row `periods` series (clamped to [5, 20]) at SMA and EMA — the EMA case pins C's
        # shifted MA seeding (a full-array port diverges). `ma_30_type7` pins the MA-selector matype
        # 7 = MAMA(0.5, 0.05) extension (period ignored, FAMA discarded).
        "mama_mama": mama.struct.field("mama"),
        "mama_fama": mama.struct.field("fama"),
        "sar": plta.sar(h, lo, acceleration=0.02, maximum=0.2),
        "sarext": plta.sarext(
            h,
            lo,
            startvalue=0.0,
            offsetonreverse=0.0,
            accelerationinitlong=0.02,
            accelerationlong=0.02,
            accelerationmaxlong=0.2,
            accelerationinitshort=0.02,
            accelerationshort=0.02,
            accelerationmaxshort=0.2,
        ),
        "sarext_long_offset": plta.sarext(
            h,
            lo,
            startvalue=100.0,
            offsetonreverse=0.05,
            accelerationinitlong=0.021,
            accelerationlong=0.022,
            accelerationmaxlong=0.25,
            accelerationinitshort=0.019,
            accelerationshort=0.018,
            accelerationmaxshort=0.15,
        ),
        "sarext_short": plta.sarext(
            h,
            lo,
            startvalue=-100.0,
            offsetonreverse=0.0,
            accelerationinitlong=0.02,
            accelerationlong=0.02,
            accelerationmaxlong=0.2,
            accelerationinitshort=0.02,
            accelerationshort=0.02,
            accelerationmaxshort=0.2,
        ),
        "mavp": plta.mavp(c, p, minperiod=5, maxperiod=20, matype=0),
        "mavp_ema": plta.mavp(c, p, minperiod=5, maxperiod=20, matype=1),
        "ma_30_type7": plta.ma(c, timeperiod=30, matype=7),
        # TA-3 volume family (kernels land in TA-4). Canonical TA-Lib / polars_talib defaults.
        "ad": plta.ad(h, lo, c, v),
        "adosc_3_10": plta.adosc(h, lo, c, v, fastperiod=3, slowperiod=10),
        "obv": plta.obv(c, v),
        "mfi_14": plta.mfi(h, lo, c, v, timeperiod=14),
    }


def flat_cases() -> dict[str, pl.Expr]:
    c, h, lo, o = pl.col("close"), pl.col("high"), pl.col("low"), pl.col("open")
    v = pl.col("volume")
    mama = plta.mama(c, fastlimit=0.5, slowlimit=0.05)
    bb = plta.bbands(c, timeperiod=20, nbdevup=2.0, nbdevdn=2.0)
    stochf = plta.stochf(h, lo, c, fastk_period=5, fastd_period=3, fastd_matype=0)
    return {
        "flat_rsi_3": plta.rsi(c, timeperiod=3),
        "flat_adx_14": plta.adx(h, lo, c, timeperiod=14),
        "flat_atr_14": plta.atr(h, lo, c, timeperiod=14),
        "flat_var_5": plta.var(c, timeperiod=5, nbdev=1.0),
        "flat_stddev_5": plta.stddev(c, timeperiod=5, nbdev=1.0),
        "flat_correl_5": plta.correl(h, lo, timeperiod=5),
        # KAMA's efficiency-ratio zero-guard fires on the dead-flat plateau.
        "flat_kama_10": plta.kama(c, timeperiod=10),
        "flat_bbands_20_upper": bb.struct.field("upperband"),
        "flat_bbands_20_middle": bb.struct.field("middleband"),
        "flat_bbands_20_lower": bb.struct.field("lowerband"),
        # WG2 guard-branch coverage: the flat plateau drives each of these to its zero short-circuit
        # (CMO's gain+loss, WILLR's high-low diff, CCI's deviation/MAD, BOP's high-low range,
        # ULTOSC's true-range totals).
        "flat_cmo_14": plta.cmo(c, timeperiod=14),
        "flat_willr_14": plta.willr(h, lo, c, timeperiod=14),
        "flat_cci_14": plta.cci(h, lo, c, timeperiod=14),
        "flat_bop": plta.bop(o, h, lo, c),
        "flat_ultosc_7_14_28": plta.ultosc(h, lo, c, timeperiod1=7, timeperiod2=14, timeperiod3=28),
        # WG3: the flat plateau decays prevTR to zero, firing DX's TA_IS_ZERO re-emit and the
        # DI functions' zero short-circuit.
        "flat_dx_14": plta.dx(h, lo, c, timeperiod=14),
        "flat_plus_di_14": plta.plus_di(h, lo, c, timeperiod=14),
        "flat_minus_di_14": plta.minus_di(h, lo, c, timeperiod=14),
        # WG4: the dead-flat plateau makes highest == lowest, so the raw %K `diff != 0.0` guard
        # short-circuits to 0.0 (then MA-smoothed) — bit-exactly as C.
        "flat_stochf_fastk": stochf.struct.field("fastk"),
        "flat_stochf_fastd": stochf.struct.field("fastd"),
        # WG5: on the dead-flat plateau high == low is constant, so BETA's per-bar returns are 0,
        # firing both the TA_IS_ZERO(prevPrice) return guard and the TA_IS_ZERO denominator guard
        # (→ 0.0) — bit-exactly as C.
        "flat_beta_5": plta.beta(h, lo, timeperiod=5),
        # T3: MAMA on the flat plateau — the Hilbert detrender decays to zero, so the atan phase
        # guard (I1 == 0.0 → 0) and the Re/Im period-adjust guard fire in steady state (not only the
        # warm-up), bit-exactly as C.
        "flat_mama_mama": mama.struct.field("mama"),
        "flat_mama_fama": mama.struct.field("fama"),
        # The price plateau pins zero-change branches: AD skip, OBV hold, MFI zero, and ADOSC over
        # a frozen AD line.
        "flat_ad": plta.ad(h, lo, c, v),
        "flat_adosc_3_10": plta.adosc(h, lo, c, v, fastperiod=3, slowperiod=10),
        "flat_obv": plta.obv(c, v),
        "flat_mfi_14": plta.mfi(h, lo, c, v, timeperiod=14),
    }


def main() -> None:
    talib_version = plta.__talib_version__
    if not talib_version.startswith(EXPECTED_TALIB_VERSION_PREFIX):
        msg = f"oracle drift: bundled TA-Lib is {talib_version!r}, goldens expect 0.4.0"
        raise RuntimeError(msg)
    wrapper_version = importlib.metadata.version("polars_talib")
    if wrapper_version != EXPECTED_WRAPPER_VERSION:
        msg = (
            f"oracle drift: polars_talib wrapper is {wrapper_version!r}, "
            f"goldens expect {EXPECTED_WRAPPER_VERSION}"
        )
        raise RuntimeError(msg)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, int] = {}

    walk = walk_fixture()
    cases = walk_cases()
    result = walk.with_columns(**cases)
    # `periods` rides along as `fixture_periods` (the MAVP per-row period input series).
    # `volume` rides along as `fixture_volume` (the TA-3 volume-family input series).
    for name in ("open", "high", "low", "close", "periods", "volume"):
        write_atomic(OUT_DIR / f"fixture_{name}.bin", series_bits(result[name].to_list()))
        manifest[f"fixture_{name}"] = N_ROWS
    for name in cases:
        write_atomic(OUT_DIR / f"{name}.bin", series_bits(result[name].to_list()))
        manifest[name] = N_ROWS

    flat = flat_fixture()
    fcases = flat_cases()
    fresult = flat.with_columns(**fcases)
    for name in ("open", "high", "low", "close", "volume"):
        write_atomic(OUT_DIR / f"fixture_flat_{name}.bin", series_bits(fresult[name].to_list()))
        manifest[f"fixture_flat_{name}"] = N_ROWS_FLAT
    for name in fcases:
        write_atomic(OUT_DIR / f"{name}.bin", series_bits(fresult[name].to_list()))
        manifest[name] = N_ROWS_FLAT

    manifest_doc = {
        "talib_version": talib_version,
        "polars_talib_version": wrapper_version,
        "series": dict(sorted(manifest.items())),
    }
    write_atomic(OUT_DIR / "manifest.json", (json.dumps(manifest_doc, indent=2) + "\n").encode())
    logger.info(
        "recorded %d series (%d walk + %d flat) from TA-Lib %s / polars_talib %s into %s",
        len(manifest),
        len(cases),
        len(fcases),
        talib_version,
        wrapper_version,
        OUT_DIR,
    )


if __name__ == "__main__":
    main()
