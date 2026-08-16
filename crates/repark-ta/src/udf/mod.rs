//! DataFusion window-UDF wrappers for the TA kernels (feature `datafusion`).
//!
//! Each kernel in this crate is a *stateful, full-series* function: its value at row `i` depends
//! on every earlier row of the ordered partition (Wilder smoothing, running accumulators, …). A
//! per-`RecordBatch` scalar UDF would see only a slice and compute the wrong thing, so the kernels
//! are wrapped as DataFusion **window** UDFs: [`PartitionEvaluator::evaluate_all`] receives the
//! whole ordered partition as one array, which is exactly the kernels' `&[f64]`-in/`Vec<f64>`-out
//! shape. Ordering is the caller's responsibility (`OVER (ORDER BY ts)` / a `PARTITION BY`); with
//! no frame declared, `evaluate_all` still covers the entire partition.
//!
//! Call shape — the series column(s) come first, the scalar parameters (period, `nbdev`) follow as
//! **literal** arguments:
//!
//! ```sql
//! SELECT ta_ema(close, 21)              OVER (ORDER BY ts) FROM t;
//! SELECT ta_adx(high, low, close, 14)   OVER (ORDER BY ts) FROM t;
//! SELECT ta_bbands_upper(close, 20, 2.0, 2.0) OVER (ORDER BY ts) FROM t;
//! ```
//!
//! Every wrapper returns `Float64`; the lookback prefix is emitted as `NaN` (kernel-identical, not
//! SQL `NULL`) so the engine output is `f64::to_bits`-identical to calling the kernel directly on
//! the ordered column. Scalar parameters must be constant literals — a non-literal errors at plan
//! time. The multi-output `BBANDS` is split into three UDFs (`_upper` / `_middle` / `_lower`), one
//! per output band, matching the frozen call-site ergonomics.

use std::cell::RefCell;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Float64Builder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl,
};
use datafusion::physical_expr::expressions::Literal;
use datafusion::prelude::SessionContext;

use crate::{
    beta, correl, linearreg, linearreg_angle, linearreg_intercept, linearreg_slope, max, min,
    stddev, sum, tsf, var,
};

mod momentum;
mod overlap;
mod price;
mod volatility;
mod volume;

// ===========================================================================================
// Scout #8 — partition-local multi-output cache.
//
// Multi-output kernels (BBANDS / MACD* / STOCH* / AROON / MAMA) are exposed as one WindowUDF
// per band. Without a cache, selecting three BBANDS columns re-runs the full kernel three
// times. Sibling UDFs share a single-slot thread-local entry keyed by (family, params bits,
// series buffer identity). DataFusion evaluates window expressions for one partition
// sequentially on one thread, so siblings of the same partition hit; a different partition /
// params / series identity misses and replaces the slot.
//
// Invalidation story (documented, no TTL):
// - Single-slot: any key mismatch drops the previous entry (no cross-partition retention).
// - Key uses the Arrow values-buffer pointer + len + null_count of each series argument (the
//   arrays DataFusion hands `evaluate_all`). Casts that allocate a new buffer change the key
//   (correctness-preserving miss).
// - The entry also **pins** the series `ArrayRef`s: while the slot holds a hit, those buffers
//   cannot be freed and recycled under a later partition's arrays (ABA false-hit guard).
// - Params are `f64::to_bits` equality (NaN bits matter; periods are integral literals).
// - Not process-global: thread_local only — no cross-thread sharing, no locks on the hot path.
// - Correctness on miss: recompute full kernel (same as the pre-cache path).
// ===========================================================================================

/// Multi-output kernel family. Sibling band UDFs share one cached kernel run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultiFamily {
    Bbands,
    Macd,
    Macdfix,
    Macdext,
    Stoch,
    Stochf,
    Stochrsi,
    Aroon,
    Mama,
}

/// Identity of one series argument for the multi-output cache key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeriesId {
    /// Data buffer pointer (`values().as_ptr()` for Float64; cast-buffer ptr otherwise).
    values_ptr: usize,
    len: usize,
    null_count: usize,
}

/// Single cached multi-output evaluation (all bands of one family for one partition).
#[derive(Debug)]
struct MultiOutEntry {
    family: MultiFamily,
    series: [SeriesId; 4],
    n_series: usize,
    /// `f64::to_bits` of each scalar param (length `n_params`).
    param_bits: [u64; 8],
    n_params: usize,
    /// Pins the series `ArrayRef`s so values-buffer pointer identity cannot ABA: while this
    /// entry lives, the allocator cannot recycle those buffers for a later partition's arrays.
    /// Dropped when the single-slot entry is replaced.
    series_pin: Vec<ArrayRef>,
    /// Owned band outputs, index = band (0=upper/macd/slowk/…, 1=middle/signal/…, …).
    bands: Vec<Vec<f64>>,
}

thread_local! {
    static MULTI_OUT_CACHE: RefCell<Option<MultiOutEntry>> = const { RefCell::new(None) };
}

/// ===========================================================================================
/// Build a [`SeriesId`] for multi-output cache keying (pointer identity of the values buffer).
/// ===========================================================================================
fn series_id(array: &ArrayRef) -> SeriesId {
    if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
        return SeriesId {
            values_ptr: floats.values().as_ptr() as usize,
            len: floats.len(),
            null_count: floats.null_count(),
        };
    }
    // Non-Float64: use the array length + null_count + the type-id discriminant and the
    // data-buffer address when present. Siblings that share the same pre-cast column still
    // collide correctly; a per-call cast that allocates will miss (safe fallback).
    let data = array.to_data();
    let values_ptr = data
        .buffers()
        .first()
        .map_or(0, |buffer| buffer.as_ptr() as usize);
    SeriesId {
        values_ptr,
        len: array.len(),
        null_count: array.null_count(),
    }
}

/// ===========================================================================================
/// Look up a band in the thread-local multi-output cache. `None` on miss.
/// ===========================================================================================
fn multi_out_lookup(
    family: MultiFamily,
    series_ids: &[SeriesId],
    params: &[f64],
    band: usize,
) -> Option<Vec<f64>> {
    MULTI_OUT_CACHE.with(|cell| {
        let guard = cell.borrow();
        let entry = guard.as_ref()?;
        if entry.family != family {
            return None;
        }
        if entry.n_series != series_ids.len() || entry.n_params != params.len() {
            return None;
        }
        // series_pin length tracks n_series (kept alive for ABA safety; assert keeps field live).
        if entry.series_pin.len() != entry.n_series {
            return None;
        }
        if entry.series[..series_ids.len()] != series_ids[..] {
            return None;
        }
        for (index, param) in params.iter().enumerate() {
            if entry.param_bits[index] != param.to_bits() {
                return None;
            }
        }
        entry.bands.get(band).cloned()
    })
}

/// ===========================================================================================
/// Store a full multi-output result (replaces any previous single-slot entry).
///
/// `series_pin` must be the live series columns whose identities are in `series_ids` — clones
/// of those `ArrayRef`s keep the buffers alive for the lifetime of the cache entry.
/// ===========================================================================================
fn multi_out_store(
    family: MultiFamily,
    series_ids: &[SeriesId],
    series_pin: &[ArrayRef],
    params: &[f64],
    bands: Vec<Vec<f64>>,
) {
    debug_assert!(series_ids.len() <= 4);
    debug_assert!(params.len() <= 8);
    debug_assert_eq!(series_ids.len(), series_pin.len());
    let mut series = [SeriesId {
        values_ptr: 0,
        len: 0,
        null_count: 0,
    }; 4];
    series[..series_ids.len()].copy_from_slice(series_ids);
    let mut param_bits = [0_u64; 8];
    for (index, param) in params.iter().enumerate() {
        param_bits[index] = param.to_bits();
    }
    let entry = MultiOutEntry {
        family,
        series,
        n_series: series_ids.len(),
        param_bits,
        n_params: params.len(),
        series_pin: series_pin.to_vec(),
        bands,
    };
    MULTI_OUT_CACHE.with(|cell| {
        *cell.borrow_mut() = Some(entry);
    });
}

/// ===========================================================================================
/// Test / bench helper: drop the thread-local multi-output cache entry.
/// ===========================================================================================
#[cfg(test)]
fn multi_out_clear() {
    MULTI_OUT_CACHE.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// The 81 window-UDF names, in registration order: the 17 single-output T1 kernels (incl. the
/// `MIN`/`MAX`/`SUM` math operators), the 8 WG1 overlap-MA kernels (`WMA`, `DEMA`, `TEMA`,
/// `TRIMA`, `KAMA`, `T3`, `MIDPOINT`, `MIDPRICE`), the three split `BBANDS` outputs, the 16
/// WG2 simple-momentum entry points (`MOM`, `ROC`/`ROCP`/`ROCR`/`ROCR100`, `WILLR`, `CCI`, `CMO`,
/// `BOP`, `APO`/`PPO` — each carrying a `matype` literal — the split `AROON` outputs
/// `ta_aroon_down`/`_up`, `AROONOSC`, `TRIX`, `ULTOSC`), the 16 WG3 directional + MACD entry
/// points (`DX`, `ADXR`, `PLUS_DI`/`MINUS_DI`, `PLUS_DM`/`MINUS_DM`, the split `MACD`/`MACDFIX`/
/// `MACDEXT` outputs `ta_macd`/`_signal`/`_hist` etc., and the `MA` selector carrying a `matype`),
/// the 6 WG4 stochastic entry points (the split `STOCH`/`STOCHF`/`STOCHRSI` outputs
/// `ta_stoch_slowk`/`_slowd`, `ta_stochf_fastk`/`_fastd`, `ta_stochrsi_fastk`/`_fastd`), plus the 6
/// WG5 sweep-up entry points (`NATR`, `BETA`, and the no-period O/H/L/C price transforms
/// `ta_avgprice`/`ta_medprice`/`ta_typprice`/`ta_wclprice`), plus the 5 T3 parked-four entry points
/// (`MAMA` split into `ta_mama`/`ta_fama`, `ta_sar`, the 8-scalar `ta_sarext`, and the two-series
/// `ta_mavp`), plus the 4 TA-4 volume entry points (`ta_ad`/`ta_adosc`/`ta_obv`/`ta_mfi`). This
/// is the single source of truth for name → kernel; both [`register_all`] and [`window_udf`]
/// read it, so adding a kernel is one row.
const SPECS: &[(&str, TaFn)] = &[
    ("ta_sma", TaFn::Sma),
    ("ta_ema", TaFn::Ema),
    ("ta_rsi", TaFn::Rsi),
    ("ta_adx", TaFn::Adx),
    ("ta_atr", TaFn::Atr),
    ("ta_trange", TaFn::Trange),
    ("ta_var", TaFn::Var),
    ("ta_stddev", TaFn::Stddev),
    ("ta_linearreg", TaFn::Linearreg),
    ("ta_linearreg_slope", TaFn::LinearregSlope),
    ("ta_linearreg_intercept", TaFn::LinearregIntercept),
    ("ta_linearreg_angle", TaFn::LinearregAngle),
    ("ta_tsf", TaFn::Tsf),
    ("ta_correl", TaFn::Correl),
    ("ta_min", TaFn::Min),
    ("ta_max", TaFn::Max),
    ("ta_sum", TaFn::Sum),
    ("ta_wma", TaFn::Wma),
    ("ta_dema", TaFn::Dema),
    ("ta_tema", TaFn::Tema),
    ("ta_trima", TaFn::Trima),
    ("ta_kama", TaFn::Kama),
    ("ta_t3", TaFn::T3),
    ("ta_midpoint", TaFn::Midpoint),
    ("ta_midprice", TaFn::Midprice),
    ("ta_bbands_upper", TaFn::BbandsUpper),
    ("ta_bbands_middle", TaFn::BbandsMiddle),
    ("ta_bbands_lower", TaFn::BbandsLower),
    ("ta_mom", TaFn::Mom),
    ("ta_roc", TaFn::Roc),
    ("ta_rocp", TaFn::Rocp),
    ("ta_rocr", TaFn::Rocr),
    ("ta_rocr100", TaFn::Rocr100),
    ("ta_willr", TaFn::Willr),
    ("ta_cci", TaFn::Cci),
    ("ta_cmo", TaFn::Cmo),
    ("ta_bop", TaFn::Bop),
    ("ta_apo", TaFn::Apo),
    ("ta_ppo", TaFn::Ppo),
    ("ta_aroon_down", TaFn::AroonDown),
    ("ta_aroon_up", TaFn::AroonUp),
    ("ta_aroonosc", TaFn::Aroonosc),
    ("ta_trix", TaFn::Trix),
    ("ta_ultosc", TaFn::Ultosc),
    ("ta_dx", TaFn::Dx),
    ("ta_adxr", TaFn::Adxr),
    ("ta_plus_di", TaFn::PlusDi),
    ("ta_minus_di", TaFn::MinusDi),
    ("ta_plus_dm", TaFn::PlusDm),
    ("ta_minus_dm", TaFn::MinusDm),
    ("ta_macd", TaFn::Macd),
    ("ta_macd_signal", TaFn::MacdSignal),
    ("ta_macd_hist", TaFn::MacdHist),
    ("ta_macdfix", TaFn::Macdfix),
    ("ta_macdfix_signal", TaFn::MacdfixSignal),
    ("ta_macdfix_hist", TaFn::MacdfixHist),
    ("ta_macdext", TaFn::Macdext),
    ("ta_macdext_signal", TaFn::MacdextSignal),
    ("ta_macdext_hist", TaFn::MacdextHist),
    ("ta_ma", TaFn::Ma),
    ("ta_stoch_slowk", TaFn::StochSlowk),
    ("ta_stoch_slowd", TaFn::StochSlowd),
    ("ta_stochf_fastk", TaFn::StochfFastk),
    ("ta_stochf_fastd", TaFn::StochfFastd),
    ("ta_stochrsi_fastk", TaFn::StochrsiFastk),
    ("ta_stochrsi_fastd", TaFn::StochrsiFastd),
    ("ta_natr", TaFn::Natr),
    ("ta_beta", TaFn::Beta),
    ("ta_avgprice", TaFn::Avgprice),
    ("ta_medprice", TaFn::Medprice),
    ("ta_typprice", TaFn::Typprice),
    ("ta_wclprice", TaFn::Wclprice),
    // T3 — the parked four: MAMA split into its two outputs, SAR, the 8-param SAREXT, and
    // MAVP (whose second series is the per-row periods column, not a scalar).
    ("ta_mama", TaFn::Mama),
    ("ta_fama", TaFn::Fama),
    ("ta_sar", TaFn::Sar),
    ("ta_sarext", TaFn::Sarext),
    ("ta_mavp", TaFn::Mavp),
    // TA-4 volume family (81/81): all single-output. AD/ADOSC/MFI are four-series H/L/C/V;
    // OBV is close+volume.
    ("ta_ad", TaFn::Ad),
    ("ta_adosc", TaFn::Adosc),
    ("ta_obv", TaFn::Obv),
    ("ta_mfi", TaFn::Mfi),
];

/// ===========================================================================================
/// Which kernel a wrapper dispatches to. `Copy` so the wrapper and its per-partition evaluator
/// carry it by value without lifetimes.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TaFn {
    Sma,
    Ema,
    Rsi,
    Adx,
    Atr,
    Trange,
    Var,
    Stddev,
    Linearreg,
    LinearregSlope,
    LinearregIntercept,
    LinearregAngle,
    Tsf,
    Correl,
    Min,
    Max,
    Sum,
    Wma,
    Dema,
    Tema,
    Trima,
    Kama,
    T3,
    Midpoint,
    Midprice,
    BbandsUpper,
    BbandsMiddle,
    BbandsLower,
    Mom,
    Roc,
    Rocp,
    Rocr,
    Rocr100,
    Willr,
    Cci,
    Cmo,
    Bop,
    Apo,
    Ppo,
    AroonDown,
    AroonUp,
    Aroonosc,
    Trix,
    Ultosc,
    Dx,
    Adxr,
    PlusDi,
    MinusDi,
    PlusDm,
    MinusDm,
    Macd,
    MacdSignal,
    MacdHist,
    Macdfix,
    MacdfixSignal,
    MacdfixHist,
    Macdext,
    MacdextSignal,
    MacdextHist,
    Ma,
    StochSlowk,
    StochSlowd,
    StochfFastk,
    StochfFastd,
    StochrsiFastk,
    StochrsiFastd,
    Natr,
    Beta,
    Avgprice,
    Medprice,
    Typprice,
    Wclprice,
    Mama,
    Fama,
    Sar,
    Sarext,
    Mavp,
    Ad,
    Adosc,
    Obv,
    Mfi,
}

impl TaFn {
    /// How many leading arguments are *series* columns (the rest are scalar literal params).
    fn n_series(self) -> usize {
        match self {
            TaFn::Bop | TaFn::Avgprice | TaFn::Ad | TaFn::Adosc | TaFn::Mfi => 4,
            TaFn::Trange
            | TaFn::Atr
            | TaFn::Adx
            | TaFn::Willr
            | TaFn::Cci
            | TaFn::Ultosc
            | TaFn::Dx
            | TaFn::Adxr
            | TaFn::PlusDi
            | TaFn::MinusDi
            | TaFn::StochSlowk
            | TaFn::StochSlowd
            | TaFn::StochfFastk
            | TaFn::StochfFastd
            | TaFn::Natr
            | TaFn::Typprice
            | TaFn::Wclprice => 3,
            TaFn::Correl
            | TaFn::Midprice
            | TaFn::AroonDown
            | TaFn::AroonUp
            | TaFn::Aroonosc
            | TaFn::PlusDm
            | TaFn::MinusDm
            | TaFn::Beta
            | TaFn::Medprice
            // SAR/SAREXT are two-series (high, low); MAVP's second series is the periods column.
            | TaFn::Sar
            | TaFn::Sarext
            | TaFn::Mavp
            | TaFn::Obv => 2,
            _ => 1,
        }
    }

    /// How many trailing arguments are scalar parameters (period(s), `nbdev`s, `vfactor`,
    /// `matype`).
    fn n_scalars(self) -> usize {
        match self {
            TaFn::Trange
            | TaFn::Bop
            | TaFn::Avgprice
            | TaFn::Medprice
            | TaFn::Typprice
            | TaFn::Wclprice
            | TaFn::Ad
            | TaFn::Obv => 0,
            // T3: period + vfactor; MA: period + matype; MACDFIX: signal only (1, the default arm).
            // MAMA: fastlimit + slowlimit; SAR: acceleration + maximum (all real-valued scalars).
            TaFn::Var
            | TaFn::Stddev
            | TaFn::T3
            | TaFn::Ma
            | TaFn::Mama
            | TaFn::Fama
            | TaFn::Sar
            | TaFn::Adosc => 2,
            // BBANDS: period + 2 nbdev; APO/PPO: fast + slow + matype; ULTOSC: three periods;
            // MACD: fast + slow + signal; STOCHF: fastk + fastd + fastd matype; MAVP: min + max +
            // matype (the periods series is a column, not a scalar).
            TaFn::BbandsUpper
            | TaFn::BbandsMiddle
            | TaFn::BbandsLower
            | TaFn::Apo
            | TaFn::Ppo
            | TaFn::Ultosc
            | TaFn::Macd
            | TaFn::MacdSignal
            | TaFn::MacdHist
            | TaFn::StochfFastk
            | TaFn::StochfFastd
            | TaFn::Mavp => 3,
            // STOCHRSI: timeperiod + fastk + fastd + fastd matype.
            TaFn::StochrsiFastk | TaFn::StochrsiFastd => 4,
            // STOCH: fastk + slowk period/type + slowd period/type.
            TaFn::StochSlowk | TaFn::StochSlowd => 5,
            // MACDEXT: fast period/type, slow period/type, signal period/type.
            TaFn::Macdext | TaFn::MacdextSignal | TaFn::MacdextHist => 6,
            // SAREXT: start value, offset-on-reverse, and the six long/short acceleration factors.
            TaFn::Sarext => 8,
            _ => 1,
        }
    }

    /// Total argument count = series columns + scalar params (drives the [`Signature`]).
    fn arity(self) -> usize {
        self.n_series() + self.n_scalars()
    }

    /// Multi-output family this UDF belongs to, if any (scout #8 cache).
    fn multi_family(self) -> Option<MultiFamily> {
        match self {
            TaFn::BbandsUpper | TaFn::BbandsMiddle | TaFn::BbandsLower => Some(MultiFamily::Bbands),
            TaFn::Macd | TaFn::MacdSignal | TaFn::MacdHist => Some(MultiFamily::Macd),
            TaFn::Macdfix | TaFn::MacdfixSignal | TaFn::MacdfixHist => Some(MultiFamily::Macdfix),
            TaFn::Macdext | TaFn::MacdextSignal | TaFn::MacdextHist => Some(MultiFamily::Macdext),
            TaFn::StochSlowk | TaFn::StochSlowd => Some(MultiFamily::Stoch),
            TaFn::StochfFastk | TaFn::StochfFastd => Some(MultiFamily::Stochf),
            TaFn::StochrsiFastk | TaFn::StochrsiFastd => Some(MultiFamily::Stochrsi),
            TaFn::AroonDown | TaFn::AroonUp => Some(MultiFamily::Aroon),
            TaFn::Mama | TaFn::Fama => Some(MultiFamily::Mama),
            _ => None,
        }
    }

    /// Band index within [`Self::multi_family`] (0-based).
    fn multi_band_index(self) -> Option<usize> {
        match self {
            TaFn::BbandsUpper
            | TaFn::Macd
            | TaFn::Macdfix
            | TaFn::Macdext
            | TaFn::StochSlowk
            | TaFn::StochfFastk
            | TaFn::StochrsiFastk
            | TaFn::AroonDown
            | TaFn::Mama => Some(0),
            TaFn::BbandsMiddle
            | TaFn::MacdSignal
            | TaFn::MacdfixSignal
            | TaFn::MacdextSignal
            | TaFn::StochSlowd
            | TaFn::StochfFastd
            | TaFn::StochrsiFastd
            | TaFn::AroonUp
            | TaFn::Fama => Some(1),
            TaFn::BbandsLower | TaFn::MacdHist | TaFn::MacdfixHist | TaFn::MacdextHist => Some(2),
            _ => None,
        }
    }

    /// Run the full multi-output kernel once; returns every band (scout #8).
    ///
    /// Callers only invoke this for multi-output UDFs (`multi_family().is_some()`). A
    /// single-output `TaFn` falls through to a one-element wrap of [`Self::compute`].
    fn compute_all(self, series: &[&[f64]], params: &[f64]) -> crate::Result<Vec<Vec<f64>>> {
        match self.multi_family() {
            Some(family @ (MultiFamily::Bbands | MultiFamily::Mama)) => {
                overlap::compute_all(family, series, params)
            }
            Some(
                family @ (MultiFamily::Macd
                | MultiFamily::Macdfix
                | MultiFamily::Macdext
                | MultiFamily::Stoch
                | MultiFamily::Stochf
                | MultiFamily::Stochrsi
                | MultiFamily::Aroon),
            ) => momentum::compute_all(family, series, params),
            None => self.compute(series, params).map(|band| vec![band]),
        }
    }

    /// Dispatch to the kernel. `series` holds the [`Self::n_series`] input columns (already
    /// `f64`); `params` holds the scalar arguments as `f64` (period fields are cast to `usize`
    /// via [`period`] — the kernel then range-validates them).
    fn compute(self, series: &[&[f64]], params: &[f64]) -> crate::Result<Vec<f64>> {
        match self {
            TaFn::Sma
            | TaFn::Ema
            | TaFn::Wma
            | TaFn::Dema
            | TaFn::Tema
            | TaFn::Trima
            | TaFn::Kama
            | TaFn::T3
            | TaFn::Midpoint
            | TaFn::Midprice
            | TaFn::BbandsUpper
            | TaFn::BbandsMiddle
            | TaFn::BbandsLower
            | TaFn::Ma
            | TaFn::Mama
            | TaFn::Fama
            | TaFn::Sar
            | TaFn::Sarext
            | TaFn::Mavp => overlap::compute(self, series, params),
            TaFn::Rsi
            | TaFn::Adx
            | TaFn::Mom
            | TaFn::Roc
            | TaFn::Rocp
            | TaFn::Rocr
            | TaFn::Rocr100
            | TaFn::Willr
            | TaFn::Cci
            | TaFn::Cmo
            | TaFn::Bop
            | TaFn::Apo
            | TaFn::Ppo
            | TaFn::AroonDown
            | TaFn::AroonUp
            | TaFn::Aroonosc
            | TaFn::Trix
            | TaFn::Ultosc
            | TaFn::Dx
            | TaFn::Adxr
            | TaFn::PlusDi
            | TaFn::MinusDi
            | TaFn::PlusDm
            | TaFn::MinusDm
            | TaFn::Macd
            | TaFn::MacdSignal
            | TaFn::MacdHist
            | TaFn::Macdfix
            | TaFn::MacdfixSignal
            | TaFn::MacdfixHist
            | TaFn::Macdext
            | TaFn::MacdextSignal
            | TaFn::MacdextHist
            | TaFn::StochSlowk
            | TaFn::StochSlowd
            | TaFn::StochfFastk
            | TaFn::StochfFastd
            | TaFn::StochrsiFastk
            | TaFn::StochrsiFastd => momentum::compute(self, series, params),
            TaFn::Atr | TaFn::Trange | TaFn::Natr => volatility::compute(self, series, params),
            TaFn::Ad | TaFn::Adosc | TaFn::Obv | TaFn::Mfi => volume::compute(self, series, params),
            TaFn::Avgprice | TaFn::Medprice | TaFn::Typprice | TaFn::Wclprice => {
                price::compute(self, series, params)
            }
            TaFn::Var => var(series[0], period(params[0])?, params[1]),
            TaFn::Stddev => stddev(series[0], period(params[0])?, params[1]),
            TaFn::Linearreg => linearreg(series[0], period(params[0])?),
            TaFn::LinearregSlope => linearreg_slope(series[0], period(params[0])?),
            TaFn::LinearregIntercept => linearreg_intercept(series[0], period(params[0])?),
            TaFn::LinearregAngle => linearreg_angle(series[0], period(params[0])?),
            TaFn::Tsf => tsf(series[0], period(params[0])?),
            TaFn::Correl => correl(series[0], series[1], period(params[0])?),
            TaFn::Min => min(series[0], period(params[0])?),
            TaFn::Max => max(series[0], period(params[0])?),
            TaFn::Sum => sum(series[0], period(params[0])?),
            TaFn::Beta => beta(series[0], series[1], period(params[0])?),
        }
    }
}

/// ===========================================================================================
/// Loud error when a family `compute` table dropped a variant the router still sends there.
/// Never a kernel-math path.
/// ===========================================================================================
fn family_dispatch_miss(func: TaFn) -> crate::TaError {
    crate::TaError::InvalidRealParam {
        name: "udf family dispatch",
        value: format!("{func:?}"),
        range: "owned family compute arm",
    }
}

/// ===========================================================================================
/// Same miss shape as [`family_dispatch_miss`], keyed by [`MultiFamily`].
/// ===========================================================================================
fn family_dispatch_miss_multi(family: MultiFamily) -> crate::TaError {
    crate::TaError::InvalidRealParam {
        name: "udf family dispatch",
        value: format!("{family:?}"),
        range: "owned family compute arm",
    }
}

/// Coerce a scalar `f64` parameter to the kernel's `usize` period.
///
/// Non-integral finite values (e.g. `21.9`) fail loud — never silent truncation. Non-finite
/// values (`NaN` / ±∞) and negatives still map to a rejectable `usize` so the kernel's
/// `check_period` surfaces `InvalidPeriod` as before.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn period(value: f64) -> crate::Result<usize> {
    if value.is_finite() && value.fract() != 0.0 {
        return Err(crate::TaError::NonIntegralPeriod {
            name: "optInTimePeriod",
            value: value.to_string(),
        });
    }
    // Saturating cast: NaN/negatives → 0; huge positives → usize::MAX; check_period rejects.
    Ok(value as usize)
}

/// ===========================================================================================
/// The window-UDF wrapper for one TA kernel. `PartitionEvaluator` construction extracts the
/// scalar literal params; `evaluate_all` reads the series columns and runs the kernel over the
/// full ordered partition.
/// ===========================================================================================
#[derive(Debug, PartialEq, Eq, Hash)]
struct TaWindowUdf {
    name: &'static str,
    func: TaFn,
    signature: Signature,
}

impl WindowUDFImpl for TaWindowUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let n_series = self.func.n_series();
        let exprs = args.input_exprs();
        let mut params = Vec::with_capacity(exprs.len().saturating_sub(n_series));
        for (index, expr) in exprs.iter().enumerate().skip(n_series) {
            let literal = expr.downcast_ref::<Literal>().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "{}: scalar parameter #{} must be a constant literal",
                    self.name,
                    index - n_series + 1
                ))
            })?;
            params.push(scalar_f64(literal.value())?);
        }
        Ok(Box::new(TaEvaluator {
            func: self.func,
            params,
            densify_scratch: Vec::new(),
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::Float64, true).into())
    }
}

/// ===========================================================================================
/// The per-partition evaluator. `evaluate_all` is handed the entire ordered partition, so the
/// stateful kernel sees the full series exactly once.
///
/// Scout #8: multi-output siblings share a thread-local cache (see module-level docs).
/// Scout #9: null-free `Float64` inputs borrow `values()` (zero densify copy); nullable inputs
/// densify NULL→NaN into reusable scratch buffers; kernel output is written via a
/// [`Float64Builder`].
/// ===========================================================================================
#[derive(Debug)]
struct TaEvaluator {
    func: TaFn,
    /// The scalar parameters (period, `nbdev`s) as `f64`, extracted from the literal arguments.
    params: Vec<f64>,
    /// Per-series densify scratch, reused across partitions on this evaluator (scout #9).
    densify_scratch: Vec<Vec<f64>>,
}

impl TaEvaluator {
    /// Ensure `densify_scratch` has one buffer per series column.
    fn ensure_scratches(&mut self, n_series: usize) {
        if self.densify_scratch.len() < n_series {
            self.densify_scratch.resize_with(n_series, Vec::new);
        }
    }
}

impl PartitionEvaluator for TaEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        let n_series = self.func.n_series();
        if values.len() < n_series {
            return Err(DataFusionError::Execution(format!(
                "TA window function expected {n_series} series argument(s), got {}",
                values.len()
            )));
        }
        let series_arrays = &values[..n_series];

        // Scout #8: multi-output cache hit — no densify, no kernel re-run.
        if let (Some(family), Some(band)) = (self.func.multi_family(), self.func.multi_band_index())
        {
            let series_ids: Vec<SeriesId> = series_arrays.iter().map(series_id).collect();
            if let Some(cached) = multi_out_lookup(family, &series_ids, &self.params, band) {
                return Ok(float64_array_from_values(&cached));
            }
            // Miss: compute all bands once (borrow null-free single Float64 when possible),
            // store, return this band.
            let bands = if n_series == 1
                && let Some(borrowed) = try_borrow_null_free_f64(&series_arrays[0])
            {
                self.func
                    .compute_all(&[borrowed], &self.params)
                    .map_err(|err| DataFusionError::Execution(format!("TA kernel error: {err}")))?
            } else {
                self.ensure_scratches(n_series);
                densify_series_into(series_arrays, &mut self.densify_scratch[..n_series])?;
                let slices: Vec<&[f64]> = self.densify_scratch[..n_series]
                    .iter()
                    .map(Vec::as_slice)
                    .collect();
                self.func
                    .compute_all(&slices, &self.params)
                    .map_err(|err| DataFusionError::Execution(format!("TA kernel error: {err}")))?
            };
            let out = bands
                .get(band)
                .cloned()
                .ok_or_else(|| DataFusionError::Execution("TA multi-output band missing".into()))?;
            multi_out_store(family, &series_ids, series_arrays, &self.params, bands);
            return Ok(float64_array_from_values(&out));
        }

        // Single-output path — scout #9 densify / borrow.
        // Null-free single Float64 series: zero-copy borrow of values() into the kernel.
        if n_series == 1
            && let Some(borrowed) = try_borrow_null_free_f64(&series_arrays[0])
        {
            let out = self
                .func
                .compute(&[borrowed], &self.params)
                .map_err(|err| DataFusionError::Execution(format!("TA kernel error: {err}")))?;
            return Ok(float64_array_from_values(&out));
        }
        self.ensure_scratches(n_series);
        densify_series_into(series_arrays, &mut self.densify_scratch[..n_series])?;
        let slices: Vec<&[f64]> = self.densify_scratch[..n_series]
            .iter()
            .map(Vec::as_slice)
            .collect();
        let out = self
            .func
            .compute(&slices, &self.params)
            .map_err(|err| DataFusionError::Execution(format!("TA kernel error: {err}")))?;
        Ok(float64_array_from_values(&out))
    }
}

/// ===========================================================================================
/// Null-free Float64 → borrow the Arrow values buffer (scout #9). `None` if cast/densify needed.
/// ===========================================================================================
fn try_borrow_null_free_f64(array: &ArrayRef) -> Option<&[f64]> {
    let floats = array.as_any().downcast_ref::<Float64Array>()?;
    if floats.null_count() == 0 {
        Some(floats.values().as_ref())
    } else {
        None
    }
}

/// ===========================================================================================
/// Densify every series column into the corresponding scratch buffer (reused across windows).
///
/// Fast path (null-free Float64): `copy_from_slice` of `values()` — no per-row null checks.
/// Slow path: cast to Float64 then NULL→NaN densify.
/// ===========================================================================================
fn densify_series_into(arrays: &[ArrayRef], scratches: &mut [Vec<f64>]) -> Result<()> {
    debug_assert_eq!(arrays.len(), scratches.len());
    for (array, scratch) in arrays.iter().zip(scratches.iter_mut()) {
        densify_one_into(array, scratch)?;
    }
    Ok(())
}

/// ===========================================================================================
/// Densify one column into `scratch` (cleared/reused).
/// ===========================================================================================
fn densify_one_into(array: &ArrayRef, scratch: &mut Vec<f64>) -> Result<()> {
    if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
        densify_float64_into(floats, scratch);
        return Ok(());
    }
    let casted = cast(array, &DataType::Float64)?;
    let floats = casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("could not cast TA window input to Float64".to_string())
        })?;
    densify_float64_into(floats, scratch);
    Ok(())
}

/// ===========================================================================================
/// NULL-free → memcpy of `values()`; with nulls → NULL→NaN densify into `scratch`.
/// ===========================================================================================
fn densify_float64_into(floats: &Float64Array, scratch: &mut Vec<f64>) {
    let len = floats.len();
    scratch.clear();
    scratch.reserve(len);
    if floats.null_count() == 0 {
        // Scout #9 fast path: bulk copy, no per-index is_null.
        scratch.extend_from_slice(floats.values().as_ref());
        return;
    }
    let values = floats.values();
    for index in 0..len {
        if floats.is_null(index) {
            scratch.push(f64::NAN);
        } else {
            scratch.push(values[index]);
        }
    }
}

/// ===========================================================================================
/// Build a `Float64Array` from a dense `f64` slice via [`Float64Builder`] (scout #9).
///
/// Kernel outputs are dense (NaN lookback, never SQL NULL), so the builder appends values only.
/// ===========================================================================================
fn float64_array_from_values(values: &[f64]) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(values.len());
    builder.append_slice(values);
    Arc::new(builder.finish())
}

/// Read a scalar literal parameter as `f64` (period and `nbdev` alike; periods are `<= 100_000`,
/// so `f64` is lossless). A non-numeric or `NULL` literal is a plan error.
fn scalar_f64(value: &ScalarValue) -> Result<f64> {
    match value.cast_to(&DataType::Float64)? {
        ScalarValue::Float64(Some(number)) => Ok(number),
        other => Err(DataFusionError::Plan(format!(
            "expected a numeric scalar parameter, got {other:?}"
        ))),
    }
}

/// Build the [`WindowUDF`] for one spec-table entry.
fn make_udf(name: &'static str, func: TaFn) -> WindowUDF {
    WindowUDF::new_from_impl(TaWindowUdf {
        name,
        func,
        signature: Signature::any(func.arity(), Volatility::Immutable),
    })
}

/// ===========================================================================================
/// Every TA window UDF, ready to register on a `SessionContext`.
///
/// Exposed separately from [`register_all`] so callers/tests can inspect the set.
/// ===========================================================================================
#[must_use]
pub fn window_udfs() -> Vec<WindowUDF> {
    SPECS
        .iter()
        .map(|&(name, func)| make_udf(name, func))
        .collect()
}

/// ===========================================================================================
/// The TA window UDF for `name` (e.g. `"ta_ema"`), or `None` if unknown.
///
/// Returned as an `Arc<WindowUDF>` so it drops straight into a DataFusion
/// `WindowFunctionDefinition::WindowUDF` — the DataFrame-API builder in `repark-python` uses this.
/// ===========================================================================================
#[must_use]
pub fn window_udf(name: &str) -> Option<Arc<WindowUDF>> {
    SPECS
        .iter()
        .find(|(spec_name, _)| *spec_name == name)
        .map(|&(spec_name, func)| Arc::new(make_udf(spec_name, func)))
}

/// ===========================================================================================
/// Register every TA window UDF on `ctx` (`ta_sma`, `ta_ema`, …, `ta_bbands_lower`).
///
/// Called at `ReparkSession` build so the whole set is SQL- and DataFrame-callable.
/// ===========================================================================================
pub fn register_all(ctx: &SessionContext) {
    for udf in window_udfs() {
        ctx.register_udwf(udf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ad, adosc, aroon, bbands, ema, macdext, macdfix, mama, mfi, obv, sma, stoch, stochf,
        stochrsi,
    };

    #[test]
    fn spec_arity_matches_series_plus_scalars() {
        for &(name, func) in SPECS {
            assert_eq!(
                func.arity(),
                func.n_series() + func.n_scalars(),
                "{name}: arity should be series + scalars"
            );
        }
    }

    #[test]
    fn window_udfs_registers_all_names() {
        let udfs = window_udfs();
        assert_eq!(udfs.len(), 81);
        let names: Vec<&str> = udfs.iter().map(WindowUDF::name).collect();
        for &(spec_name, _) in SPECS {
            assert!(
                names.contains(&spec_name),
                "{spec_name} missing from window_udfs()"
            );
        }
    }

    #[test]
    fn window_udf_lookup_hits_and_misses() {
        assert!(window_udf("ta_ema").is_some());
        assert!(window_udf("ta_bbands_lower").is_some());
        assert!(window_udf("ta_ad").is_some());
        assert!(window_udf("ta_adosc").is_some());
        assert!(window_udf("ta_obv").is_some());
        assert!(window_udf("ta_mfi").is_some());
        assert!(window_udf("ta_not_a_function").is_none());
    }

    #[test]
    fn period_saturates_out_of_range_to_a_rejectable_value() {
        // Negative / NaN saturate to 0 (< every kernel min); the kernel then errors, never
        // reaching the arithmetic with a bogus period.
        assert_eq!(period(-1.0).unwrap(), 0);
        assert_eq!(period(f64::NAN).unwrap(), 0);
        assert_eq!(period(21.0).unwrap(), 21);
    }

    #[test]
    fn period_rejects_non_integral_values() {
        let err = period(21.9).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not a whole number") && msg.contains("21.9"),
            "{msg}"
        );
        assert_eq!(period(3.0).unwrap(), 3);
    }

    #[test]
    fn compute_ema_matches_the_kernel() {
        // The dispatch table calls the same kernel the goldens gate; a spot check on a short
        // series keeps the wrapper honest independent of the engine e2e tests.
        let close: Vec<f64> = (1..=10).map(f64::from).collect();
        let via_udf = TaFn::Ema
            .compute(&[close.as_slice()], &[3.0])
            .expect("compute");
        let direct = ema(&close, 3).expect("ema");
        assert_eq!(via_udf.len(), direct.len());
        for (a, b) in via_udf.iter().zip(&direct) {
            assert!(a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()));
        }
    }

    #[test]
    fn compute_volume_kernels_match_the_kernel() {
        // TA-4 wiring: SPECS dispatch is bit-exact with the public kernels (arity + series order).
        let high: Vec<f64> = (0..20).map(|i| 12.0 + f64::from(i)).collect();
        let low: Vec<f64> = (0..20).map(|i| 8.0 + f64::from(i)).collect();
        let close: Vec<f64> = (0..20).map(|i| 11.0 + f64::from(i)).collect();
        let volume: Vec<f64> = (0..20).map(|i| 100.0 + f64::from(i)).collect();
        let hlcv = [
            high.as_slice(),
            low.as_slice(),
            close.as_slice(),
            volume.as_slice(),
        ];
        let via_ad = TaFn::Ad.compute(&hlcv, &[]).expect("ta_ad");
        assert_f64_series_bit_exact(&via_ad, &ad(&high, &low, &close, &volume).expect("ad"));
        let via_adosc = TaFn::Adosc.compute(&hlcv, &[3.0, 10.0]).expect("ta_adosc");
        assert_f64_series_bit_exact(
            &via_adosc,
            &adosc(&high, &low, &close, &volume, 3, 10).expect("adosc"),
        );
        let via_obv = TaFn::Obv
            .compute(&[close.as_slice(), volume.as_slice()], &[])
            .expect("ta_obv");
        assert_f64_series_bit_exact(&via_obv, &obv(&close, &volume).expect("obv"));
        let via_mfi = TaFn::Mfi.compute(&hlcv, &[14.0]).expect("ta_mfi");
        assert_f64_series_bit_exact(
            &via_mfi,
            &mfi(&high, &low, &close, &volume, 14).expect("mfi"),
        );
    }

    #[test]
    fn multi_family_covers_every_split_entry_point() {
        // Every multi-output UDF maps to a family + band; singles do not.
        assert_eq!(TaFn::BbandsUpper.multi_family(), Some(MultiFamily::Bbands));
        assert_eq!(TaFn::BbandsUpper.multi_band_index(), Some(0));
        assert_eq!(TaFn::BbandsMiddle.multi_band_index(), Some(1));
        assert_eq!(TaFn::BbandsLower.multi_band_index(), Some(2));
        assert_eq!(TaFn::MacdHist.multi_family(), Some(MultiFamily::Macd));
        assert_eq!(TaFn::MacdfixSignal.multi_band_index(), Some(1));
        assert_eq!(TaFn::Macdext.multi_family(), Some(MultiFamily::Macdext));
        assert_eq!(TaFn::StochSlowd.multi_family(), Some(MultiFamily::Stoch));
        assert_eq!(TaFn::StochfFastk.multi_band_index(), Some(0));
        assert_eq!(TaFn::StochrsiFastd.multi_band_index(), Some(1));
        assert_eq!(TaFn::AroonUp.multi_family(), Some(MultiFamily::Aroon));
        assert_eq!(TaFn::Fama.multi_family(), Some(MultiFamily::Mama));
        assert_eq!(TaFn::Fama.multi_band_index(), Some(1));
        assert!(TaFn::Ema.multi_family().is_none());
        assert!(TaFn::Aroonosc.multi_family().is_none()); // oscillator is single-output
    }

    #[test]
    fn every_spec_multi_family_has_band_and_compute_all_width() {
        // C5-Q-001: SPECS loop — multi_family ⇔ multi_band_index; compute_all band count
        // covers the band index for a short synthetic series.
        let close: Vec<f64> = (1..=64).map(|i| 50.0 + f64::from(i)).collect();
        let high: Vec<f64> = close.iter().map(|v| v + 1.0).collect();
        let low: Vec<f64> = close.iter().map(|v| v - 1.0).collect();
        for &(name, func) in SPECS {
            match (func.multi_family(), func.multi_band_index()) {
                (None, None) => {}
                (Some(_), Some(band)) => {
                    let n_series = func.n_series();
                    let series: Vec<&[f64]> = match n_series {
                        1 => vec![close.as_slice()],
                        2 => vec![high.as_slice(), low.as_slice()],
                        3 => vec![high.as_slice(), low.as_slice(), close.as_slice()],
                        _ => panic!("{name}: unexpected n_series {n_series} for multi family"),
                    };
                    // Legal default-ish params per family (integral periods / real MAMA limits).
                    let params: Vec<f64> = match func.multi_family() {
                        Some(MultiFamily::Bbands) => vec![5.0, 2.0, 2.0],
                        Some(MultiFamily::Macd) => vec![12.0, 26.0, 9.0],
                        Some(MultiFamily::Macdfix) => vec![9.0],
                        Some(MultiFamily::Macdext) => vec![12.0, 0.0, 26.0, 0.0, 9.0, 0.0],
                        Some(MultiFamily::Stoch) => vec![5.0, 3.0, 0.0, 3.0, 0.0],
                        Some(MultiFamily::Stochf) => vec![5.0, 3.0, 0.0],
                        Some(MultiFamily::Stochrsi) => vec![14.0, 5.0, 3.0, 0.0],
                        Some(MultiFamily::Aroon) => vec![14.0],
                        Some(MultiFamily::Mama) => vec![0.5, 0.05],
                        None => unreachable!(),
                    };
                    let bands = func
                        .compute_all(&series, &params)
                        .unwrap_or_else(|err| panic!("{name}: compute_all: {err}"));
                    assert!(
                        bands.len() > band,
                        "{name}: compute_all returned {} bands; band index {band}",
                        bands.len()
                    );
                    assert_eq!(bands[band].len(), close.len(), "{name}: band len");
                }
                (family, band) => {
                    panic!("{name}: multi_family/band desync family={family:?} band={band:?}")
                }
            }
        }
    }

    #[test]
    fn multi_out_cache_serves_sibling_bands_bit_exact() {
        multi_out_clear();
        let close: Vec<f64> = (1..=64).map(|i| 100.0 + f64::from(i) * 0.25).collect();
        let array: ArrayRef = Arc::new(Float64Array::from(close.clone()));
        let ids = [series_id(&array)];
        let params = [20.0_f64, 2.0, 2.0];
        let (upper, middle, lower) = bbands(&close, 20, 2.0, 2.0).expect("bbands");
        multi_out_store(
            MultiFamily::Bbands,
            &ids,
            std::slice::from_ref(&array),
            &params,
            vec![upper.clone(), middle.clone(), lower.clone()],
        );
        let hit_u = multi_out_lookup(MultiFamily::Bbands, &ids, &params, 0).expect("upper hit");
        let hit_m = multi_out_lookup(MultiFamily::Bbands, &ids, &params, 1).expect("middle hit");
        let hit_l = multi_out_lookup(MultiFamily::Bbands, &ids, &params, 2).expect("lower hit");
        assert_eq!(hit_u.len(), upper.len());
        for (a, b) in hit_u.iter().zip(&upper) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
        for (a, b) in hit_m.iter().zip(&middle) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
        for (a, b) in hit_l.iter().zip(&lower) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
        // Param mismatch must miss.
        assert!(multi_out_lookup(MultiFamily::Bbands, &ids, &[20.0, 2.0, 2.1], 0).is_none());
        // Family mismatch must miss.
        assert!(multi_out_lookup(MultiFamily::Macd, &ids, &params, 0).is_none());
        multi_out_clear();
    }

    /// Bit-exact helper: NaN payload bits may differ; kernel NaNs are compared via `is_nan`.
    fn assert_f64_series_bit_exact(got: &[f64], expected: &[f64]) {
        assert_eq!(got.len(), expected.len());
        for (a, b) in got.iter().zip(expected) {
            assert!(
                a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()),
                "bits {a:?} vs {b:?}"
            );
        }
    }

    fn array_values(array: &ArrayRef) -> Vec<f64> {
        let floats = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        (0..floats.len()).map(|index| floats.value(index)).collect()
    }

    #[test]
    fn evaluate_all_multi_output_siblings_bit_exact_via_cache() {
        // C1-Q-001: drive PartitionEvaluator::evaluate_all for three BBANDS siblings — first
        // miss stores all bands; subsequent siblings must hit and stay bit-exact to the kernel.
        multi_out_clear();
        let close: Vec<f64> = (1..=80).map(|i| 50.0 + f64::from(i) * 0.5).collect();
        let array: ArrayRef = Arc::new(Float64Array::from(close.clone()));
        let params = vec![5.0_f64, 2.0, 2.0];
        let mut upper_eval = TaEvaluator {
            func: TaFn::BbandsUpper,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut middle_eval = TaEvaluator {
            func: TaFn::BbandsMiddle,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut lower_eval = TaEvaluator {
            func: TaFn::BbandsLower,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let values = [array];
        let out_u = upper_eval
            .evaluate_all(&values, close.len())
            .expect("upper evaluate_all");
        let out_m = middle_eval
            .evaluate_all(&values, close.len())
            .expect("middle evaluate_all");
        let out_l = lower_eval
            .evaluate_all(&values, close.len())
            .expect("lower evaluate_all");
        let (kernel_u, kernel_m, kernel_l) = bbands(&close, 5, 2.0, 2.0).expect("bbands");
        assert_f64_series_bit_exact(&array_values(&out_u), &kernel_u);
        assert_f64_series_bit_exact(&array_values(&out_m), &kernel_m);
        assert_f64_series_bit_exact(&array_values(&out_l), &kernel_l);
        multi_out_clear();
    }

    #[test]
    fn evaluate_all_multi_output_two_partitions_no_cross_talk() {
        // C1-Q-002 / C1-L-001: sequential partitions with same len + params must each match
        // their own kernel (series_pin prevents pointer-reuse false hits).
        multi_out_clear();
        let params = vec![5.0_f64, 2.0, 2.0];
        let close_a: Vec<f64> = (1..=64).map(|i| 100.0 + f64::from(i)).collect();
        let close_b: Vec<f64> = (1..=64).map(|i| 300.0 - f64::from(i) * 0.75).collect();
        assert_ne!(close_a, close_b);

        for close in [&close_a, &close_b] {
            let array: ArrayRef = Arc::new(Float64Array::from(close.clone()));
            let mut upper_eval = TaEvaluator {
                func: TaFn::BbandsUpper,
                params: params.clone(),
                densify_scratch: Vec::new(),
            };
            let mut middle_eval = TaEvaluator {
                func: TaFn::BbandsMiddle,
                params: params.clone(),
                densify_scratch: Vec::new(),
            };
            let mut lower_eval = TaEvaluator {
                func: TaFn::BbandsLower,
                params: params.clone(),
                densify_scratch: Vec::new(),
            };
            let values = [array];
            let out_u = upper_eval
                .evaluate_all(&values, close.len())
                .expect("upper");
            let out_m = middle_eval
                .evaluate_all(&values, close.len())
                .expect("middle");
            let out_l = lower_eval
                .evaluate_all(&values, close.len())
                .expect("lower");
            let (kernel_u, kernel_m, kernel_l) = bbands(close, 5, 2.0, 2.0).expect("bbands");
            assert_f64_series_bit_exact(&array_values(&out_u), &kernel_u);
            assert_f64_series_bit_exact(&array_values(&out_m), &kernel_m);
            assert_f64_series_bit_exact(&array_values(&out_l), &kernel_l);
        }
        multi_out_clear();
    }

    #[test]
    fn multi_out_entry_pin_keeps_series_buffer_alive() {
        // C1-L-001: while the cache holds a pin, the stored SeriesId pointer remains valid
        // identity for the pinned array (strong count ≥ 2: test local + pin).
        multi_out_clear();
        let close: Vec<f64> = (1..=32).map(f64::from).collect();
        let array: ArrayRef = Arc::new(Float64Array::from(close.clone()));
        let ids = [series_id(&array)];
        let params = [5.0_f64, 2.0, 2.0];
        let (upper, middle, lower) = bbands(&close, 5, 2.0, 2.0).expect("bbands");
        multi_out_store(
            MultiFamily::Bbands,
            &ids,
            std::slice::from_ref(&array),
            &params,
            vec![upper, middle, lower],
        );
        let strong = Arc::strong_count(&array);
        assert!(
            strong >= 2,
            "cache series_pin must hold an ArrayRef clone (strong_count={strong})"
        );
        // Drop the test-local ref; pin alone must keep the buffer identity hit-able.
        let ptr_before = ids[0].values_ptr;
        drop(array);
        let hit = multi_out_lookup(MultiFamily::Bbands, &ids, &params, 0);
        assert!(
            hit.is_some(),
            "pin must keep entry hittable after outer drop"
        );
        assert_eq!(ids[0].values_ptr, ptr_before);
        multi_out_clear();
    }

    #[test]
    fn densify_casts_int64_null_free_and_nullable() {
        // C1-Q-003: non-Float64 cast path through densify_one_into.
        use datafusion::arrow::array::Int64Array;
        let ints = Int64Array::from(vec![1_i64, 2, 3]);
        let array: ArrayRef = Arc::new(ints);
        let mut scratch = Vec::new();
        densify_one_into(&array, &mut scratch).expect("cast densify");
        assert_eq!(scratch, vec![1.0, 2.0, 3.0]);

        let nullable = Int64Array::from(vec![Some(10_i64), None, Some(30)]);
        let array_n: ArrayRef = Arc::new(nullable);
        densify_one_into(&array_n, &mut scratch).expect("nullable cast");
        assert_eq!(scratch.len(), 3);
        assert_eq!(scratch[0].to_bits(), 10.0_f64.to_bits());
        assert!(scratch[1].is_nan());
        assert_eq!(scratch[2].to_bits(), 30.0_f64.to_bits());
    }

    #[test]
    fn compute_all_macd_and_aroon_match_split_compute() {
        // C1-Q-004: multi-output families beyond BBANDS.
        let close: Vec<f64> = (1..=80).map(|i| 40.0 + f64::from(i) * 0.3).collect();
        let high: Vec<f64> = close.iter().map(|v| v + 1.0).collect();
        let low: Vec<f64> = close.iter().map(|v| v - 1.0).collect();

        let macd_params = [12.0_f64, 26.0, 9.0];
        let macd_bands = TaFn::Macd
            .compute_all(&[close.as_slice()], &macd_params)
            .expect("macd compute_all");
        assert_eq!(macd_bands.len(), 3);
        let macd_line = TaFn::Macd
            .compute(&[close.as_slice()], &macd_params)
            .expect("macd");
        let signal = TaFn::MacdSignal
            .compute(&[close.as_slice()], &macd_params)
            .expect("signal");
        let hist = TaFn::MacdHist
            .compute(&[close.as_slice()], &macd_params)
            .expect("hist");
        assert_f64_series_bit_exact(&macd_bands[0], &macd_line);
        assert_f64_series_bit_exact(&macd_bands[1], &signal);
        assert_f64_series_bit_exact(&macd_bands[2], &hist);

        let aroon_params = [14.0_f64];
        let aroon_bands = TaFn::AroonDown
            .compute_all(&[high.as_slice(), low.as_slice()], &aroon_params)
            .expect("aroon compute_all");
        assert_eq!(aroon_bands.len(), 2);
        let down = TaFn::AroonDown
            .compute(&[high.as_slice(), low.as_slice()], &aroon_params)
            .expect("down");
        let up = TaFn::AroonUp
            .compute(&[high.as_slice(), low.as_slice()], &aroon_params)
            .expect("up");
        assert_f64_series_bit_exact(&aroon_bands[0], &down);
        assert_f64_series_bit_exact(&aroon_bands[1], &up);
    }

    #[test]
    fn evaluate_all_nullable_float64_densify_matches_kernel_nan() {
        // Nullable single-series path through evaluate_all (densify NULL→NaN).
        multi_out_clear();
        let data = vec![
            Some(1.0_f64),
            None,
            Some(3.0),
            Some(4.0),
            Some(5.0),
            Some(6.0),
        ];
        let array: ArrayRef = Arc::new(Float64Array::from(data));
        let mut densified = Vec::new();
        densify_one_into(&array, &mut densified).expect("densify");
        let mut eval = TaEvaluator {
            func: TaFn::Ema,
            params: vec![3.0],
            densify_scratch: Vec::new(),
        };
        let out = eval
            .evaluate_all(std::slice::from_ref(&array), densified.len())
            .expect("evaluate_all");
        let kernel = ema(&densified, 3).expect("ema");
        assert_f64_series_bit_exact(&array_values(&out), &kernel);
        multi_out_clear();
    }

    #[test]
    fn compute_all_bbands_matches_split_compute() {
        let close: Vec<f64> = (1..=40).map(|i| 50.0 + f64::from(i)).collect();
        let params = [5.0_f64, 2.0, 2.0];
        let bands = TaFn::BbandsUpper
            .compute_all(&[close.as_slice()], &params)
            .expect("compute_all");
        assert_eq!(bands.len(), 3);
        let upper = TaFn::BbandsUpper
            .compute(&[close.as_slice()], &params)
            .expect("upper");
        let middle = TaFn::BbandsMiddle
            .compute(&[close.as_slice()], &params)
            .expect("middle");
        let lower = TaFn::BbandsLower
            .compute(&[close.as_slice()], &params)
            .expect("lower");
        for (a, b) in bands[0].iter().zip(&upper) {
            assert!(a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()));
        }
        for (a, b) in bands[1].iter().zip(&middle) {
            assert!(a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()));
        }
        for (a, b) in bands[2].iter().zip(&lower) {
            assert!(a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()));
        }
    }

    #[test]
    fn densify_null_free_matches_values_slice() {
        let data = vec![1.0_f64, 2.5, -3.0, 4.25];
        let array = Float64Array::from(data.clone());
        let mut scratch = Vec::new();
        densify_float64_into(&array, &mut scratch);
        assert_eq!(scratch, data);
        // Borrow path agrees.
        let array_ref: ArrayRef = Arc::new(Float64Array::from(data.clone()));
        let borrowed = try_borrow_null_free_f64(&array_ref).expect("borrow");
        assert_eq!(borrowed, data.as_slice());
    }

    #[test]
    fn densify_maps_null_to_nan() {
        let array = Float64Array::from(vec![Some(1.0), None, Some(3.0)]);
        let mut scratch = Vec::new();
        densify_float64_into(&array, &mut scratch);
        assert_eq!(scratch.len(), 3);
        assert_eq!(scratch[0].to_bits(), 1.0_f64.to_bits());
        assert!(scratch[1].is_nan());
        assert_eq!(scratch[2].to_bits(), 3.0_f64.to_bits());
        // Nullable arrays do not take the borrow fast path.
        let array_ref: ArrayRef = Arc::new(array);
        assert!(try_borrow_null_free_f64(&array_ref).is_none());
    }

    #[test]
    fn densify_scratch_reused_across_calls() {
        let a = Float64Array::from(vec![1.0, 2.0]);
        let b = Float64Array::from(vec![9.0, 8.0, 7.0, 6.0]);
        let mut scratch = Vec::with_capacity(2);
        densify_float64_into(&a, &mut scratch);
        assert_eq!(scratch, vec![1.0, 2.0]);
        let capacity_after_first = scratch.capacity();
        densify_float64_into(&b, &mut scratch);
        assert_eq!(scratch, vec![9.0, 8.0, 7.0, 6.0]);
        // Reuse should not shrink capacity below the previous high-water mark.
        assert!(scratch.capacity() >= capacity_after_first);
    }

    #[test]
    fn float64_builder_output_is_dense_bit_exact() {
        let values = vec![f64::NAN, 1.5, -2.25, 0.0];
        let array = float64_array_from_values(&values);
        let floats = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        assert_eq!(floats.null_count(), 0);
        assert_eq!(floats.len(), values.len());
        for (index, expected) in values.iter().enumerate() {
            let got = floats.value(index);
            if expected.is_nan() {
                assert!(got.is_nan());
            } else {
                assert_eq!(got.to_bits(), expected.to_bits());
            }
        }
    }

    #[test]
    fn evaluate_all_stoch_multi_series_siblings_bit_exact() {
        // C2-Q-001: multi-series multi-output path (always densifies; cache shares bands).
        multi_out_clear();
        let n = 64_usize;
        let high: Vec<f64> = (0..n)
            .map(|i| 110.0 + f64::from(u32::try_from(i).expect("test n fits u32")) * 0.2)
            .collect();
        let low: Vec<f64> = (0..n)
            .map(|i| 90.0 + f64::from(u32::try_from(i).expect("test n fits u32")) * 0.15)
            .collect();
        let close: Vec<f64> = (0..n)
            .map(|i| 100.0 + f64::from(u32::try_from(i).expect("test n fits u32")) * 0.18)
            .collect();
        let high_a: ArrayRef = Arc::new(Float64Array::from(high.clone()));
        let low_a: ArrayRef = Arc::new(Float64Array::from(low.clone()));
        let close_a: ArrayRef = Arc::new(Float64Array::from(close.clone()));
        // fastk=5, slowk=3, slowk_matype=0, slowd=3, slowd_matype=0
        let params = vec![5.0_f64, 3.0, 0.0, 3.0, 0.0];
        let values = [high_a, low_a, close_a];
        let mut k_eval = TaEvaluator {
            func: TaFn::StochSlowk,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut d_eval = TaEvaluator {
            func: TaFn::StochSlowd,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let out_k = k_eval.evaluate_all(&values, n).expect("slowk");
        let out_d = d_eval.evaluate_all(&values, n).expect("slowd");
        let (kernel_k, kernel_d) = stoch(&high, &low, &close, 5, 3, 0, 3, 0).expect("stoch");
        assert_f64_series_bit_exact(&array_values(&out_k), &kernel_k);
        assert_f64_series_bit_exact(&array_values(&out_d), &kernel_d);
        multi_out_clear();
    }

    #[test]
    fn evaluate_all_aroon_multi_series_siblings_bit_exact() {
        // C2-Q-001: AROON H/L multi-series densify + cache.
        multi_out_clear();
        let n = 48_usize;
        let high: Vec<f64> = (0..n)
            .map(|i| 50.0 + f64::from(u32::try_from(i).expect("test n fits u32")))
            .collect();
        let low: Vec<f64> = (0..n)
            .map(|i| 40.0 + f64::from(u32::try_from(i).expect("test n fits u32")) * 0.5)
            .collect();
        let high_a: ArrayRef = Arc::new(Float64Array::from(high.clone()));
        let low_a: ArrayRef = Arc::new(Float64Array::from(low.clone()));
        let params = vec![14.0_f64];
        let values = [high_a, low_a];
        let mut down_eval = TaEvaluator {
            func: TaFn::AroonDown,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut up_eval = TaEvaluator {
            func: TaFn::AroonUp,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let out_down = down_eval.evaluate_all(&values, n).expect("down");
        let out_up = up_eval.evaluate_all(&values, n).expect("up");
        let (kernel_down, kernel_up) = aroon(&high, &low, 14).expect("aroon");
        assert_f64_series_bit_exact(&array_values(&out_down), &kernel_down);
        assert_f64_series_bit_exact(&array_values(&out_up), &kernel_up);
        multi_out_clear();
    }

    #[test]
    fn evaluate_all_mama_siblings_real_scalars_bit_exact() {
        // C2-Q-003: MAMA/FAMA real limits (not period()) through evaluate_all + cache.
        multi_out_clear();
        let close: Vec<f64> = (1..=80)
            .map(|i| 100.0 + (f64::from(i) * 0.37).sin() * 5.0)
            .collect();
        let array: ArrayRef = Arc::new(Float64Array::from(close.clone()));
        let params = vec![0.5_f64, 0.05];
        let values = [array];
        let mut mama_eval = TaEvaluator {
            func: TaFn::Mama,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut fama_eval = TaEvaluator {
            func: TaFn::Fama,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let out_m = mama_eval.evaluate_all(&values, close.len()).expect("mama");
        let out_f = fama_eval.evaluate_all(&values, close.len()).expect("fama");
        let (kernel_m, kernel_f) = mama(&close, 0.5, 0.05).expect("mama kernel");
        assert_f64_series_bit_exact(&array_values(&out_m), &kernel_m);
        assert_f64_series_bit_exact(&array_values(&out_f), &kernel_f);
        multi_out_clear();
    }

    #[test]
    fn evaluate_all_empty_and_short_series_match_kernel() {
        // C2-Q-002: empty partition + short series (len < period) via evaluate_all.
        multi_out_clear();
        for close in [Vec::<f64>::new(), vec![1.0_f64, 2.0, 3.0]] {
            let array: ArrayRef = Arc::new(Float64Array::from(close.clone()));
            let mut eval = TaEvaluator {
                func: TaFn::Ema,
                params: vec![5.0],
                densify_scratch: Vec::new(),
            };
            let out = eval
                .evaluate_all(std::slice::from_ref(&array), close.len())
                .expect("evaluate_all");
            let kernel = ema(&close, 5).expect("ema");
            assert_f64_series_bit_exact(&array_values(&out), &kernel);
        }
        // Multi-output empty: band aligned with kernel.
        let empty: ArrayRef = Arc::new(Float64Array::from(Vec::<f64>::new()));
        let params = vec![5.0_f64, 2.0, 2.0];
        let mut upper_eval = TaEvaluator {
            func: TaFn::BbandsUpper,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let out_u = upper_eval
            .evaluate_all(std::slice::from_ref(&empty), 0)
            .expect("bbands empty");
        let (kernel_u, _, _) = bbands(&[], 5, 2.0, 2.0).expect("bbands");
        assert_f64_series_bit_exact(&array_values(&out_u), &kernel_u);
        multi_out_clear();
    }

    #[test]
    fn evaluate_all_bbands_int64_series_siblings_bit_exact() {
        // C2-L-002: non-Float64 cast path through multi-output evaluate_all (series_id on
        // pre-cast buffer; densify casts; siblings share the same Int64 ArrayRefs).
        use datafusion::arrow::array::Int64Array;
        multi_out_clear();
        let close_i: Vec<i64> = (1..=40).map(i64::from).collect();
        #[allow(clippy::cast_precision_loss)]
        let close_f: Vec<f64> = close_i.iter().map(|&v| v as f64).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(close_i));
        let params = vec![5.0_f64, 2.0, 2.0];
        let values = [array];
        let mut upper_eval = TaEvaluator {
            func: TaFn::BbandsUpper,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut lower_eval = TaEvaluator {
            func: TaFn::BbandsLower,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let out_u = upper_eval
            .evaluate_all(&values, close_f.len())
            .expect("upper");
        let out_l = lower_eval
            .evaluate_all(&values, close_f.len())
            .expect("lower");
        let (kernel_u, _, kernel_l) = bbands(&close_f, 5, 2.0, 2.0).expect("bbands");
        assert_f64_series_bit_exact(&array_values(&out_u), &kernel_u);
        assert_f64_series_bit_exact(&array_values(&out_l), &kernel_l);
        multi_out_clear();
    }

    #[test]
    fn evaluate_all_macdfix_and_stochf_siblings_bit_exact() {
        // C3-Q-001: remaining multi-output families through evaluate_all + cache.
        multi_out_clear();
        let close: Vec<f64> = (1..=90).map(|i| 80.0 + f64::from(i) * 0.4).collect();
        let array: ArrayRef = Arc::new(Float64Array::from(close.clone()));
        let macdfix_params = vec![9.0_f64];
        let values = [array.clone()];
        let mut macd_eval = TaEvaluator {
            func: TaFn::Macdfix,
            params: macdfix_params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut signal_eval = TaEvaluator {
            func: TaFn::MacdfixSignal,
            params: macdfix_params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut hist_eval = TaEvaluator {
            func: TaFn::MacdfixHist,
            params: macdfix_params,
            densify_scratch: Vec::new(),
        };
        let out_m = macd_eval
            .evaluate_all(&values, close.len())
            .expect("macdfix");
        let out_s = signal_eval
            .evaluate_all(&values, close.len())
            .expect("signal");
        let out_h = hist_eval.evaluate_all(&values, close.len()).expect("hist");
        let (kernel_m, kernel_s, kernel_h) = macdfix(&close, 9).expect("macdfix kernel");
        assert_f64_series_bit_exact(&array_values(&out_m), &kernel_m);
        assert_f64_series_bit_exact(&array_values(&out_s), &kernel_s);
        assert_f64_series_bit_exact(&array_values(&out_h), &kernel_h);

        let n = 64_usize;
        let high: Vec<f64> = (0..n)
            .map(|i| 120.0 + f64::from(u32::try_from(i).expect("n")) * 0.1)
            .collect();
        let low: Vec<f64> = (0..n)
            .map(|i| 100.0 + f64::from(u32::try_from(i).expect("n")) * 0.1)
            .collect();
        let close_s: Vec<f64> = (0..n)
            .map(|i| 110.0 + f64::from(u32::try_from(i).expect("n")) * 0.1)
            .collect();
        let stochf_params = vec![5.0_f64, 3.0, 0.0];
        let stoch_values = [
            Arc::new(Float64Array::from(high.clone())) as ArrayRef,
            Arc::new(Float64Array::from(low.clone())) as ArrayRef,
            Arc::new(Float64Array::from(close_s.clone())) as ArrayRef,
        ];
        let mut line_k = TaEvaluator {
            func: TaFn::StochfFastk,
            params: stochf_params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut line_d = TaEvaluator {
            func: TaFn::StochfFastd,
            params: stochf_params,
            densify_scratch: Vec::new(),
        };
        let got_k = line_k.evaluate_all(&stoch_values, n).expect("fastk");
        let got_d = line_d.evaluate_all(&stoch_values, n).expect("fastd");
        let (expect_k, expect_d) = stochf(&high, &low, &close_s, 5, 3, 0).expect("stochf");
        assert_f64_series_bit_exact(&array_values(&got_k), &expect_k);
        assert_f64_series_bit_exact(&array_values(&got_d), &expect_d);
        multi_out_clear();
    }

    #[test]
    fn evaluate_all_nullable_bbands_siblings_match_nan_densify() {
        // C3-Q-002: multi-output + nullable Float64 densify + cache.
        multi_out_clear();
        let data = vec![
            Some(10.0_f64),
            Some(11.0),
            None,
            Some(13.0),
            Some(14.0),
            Some(15.0),
            Some(16.0),
            Some(17.0),
            Some(18.0),
            Some(19.0),
        ];
        let array: ArrayRef = Arc::new(Float64Array::from(data));
        let mut densified = Vec::new();
        densify_one_into(&array, &mut densified).expect("densify");
        let params = vec![3.0_f64, 2.0, 2.0];
        let values = [array];
        let mut upper_eval = TaEvaluator {
            func: TaFn::BbandsUpper,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut lower_eval = TaEvaluator {
            func: TaFn::BbandsLower,
            params,
            densify_scratch: Vec::new(),
        };
        let out_u = upper_eval
            .evaluate_all(&values, densified.len())
            .expect("upper");
        let out_l = lower_eval
            .evaluate_all(&values, densified.len())
            .expect("lower");
        let (kernel_u, _, kernel_l) = bbands(&densified, 3, 2.0, 2.0).expect("bbands");
        assert_f64_series_bit_exact(&array_values(&out_u), &kernel_u);
        assert_f64_series_bit_exact(&array_values(&out_l), &kernel_l);
        multi_out_clear();
    }

    #[test]
    fn evaluate_all_float32_cast_matches_kernel() {
        // C3-Q-003: Float32 → Float64 cast densify on single-output path.
        use datafusion::arrow::array::Float32Array;
        multi_out_clear();
        let close_f32: Vec<f32> = (1_i16..=20).map(f32::from).collect();
        let close_f64: Vec<f64> = close_f32.iter().map(|&v| f64::from(v)).collect();
        let array: ArrayRef = Arc::new(Float32Array::from(close_f32));
        let mut eval = TaEvaluator {
            func: TaFn::Sma,
            params: vec![3.0],
            densify_scratch: Vec::new(),
        };
        let out = eval
            .evaluate_all(std::slice::from_ref(&array), close_f64.len())
            .expect("sma f32");
        let kernel = sma(&close_f64, 3).expect("sma");
        assert_f64_series_bit_exact(&array_values(&out), &kernel);
        multi_out_clear();
    }

    #[test]
    fn evaluate_all_stochrsi_and_macdext_siblings_bit_exact() {
        // C4-Q-001: last multi-output families through evaluate_all + cache.
        multi_out_clear();
        let close: Vec<f64> = (1..=100).map(|i| 70.0 + f64::from(i) * 0.25).collect();
        let array: ArrayRef = Arc::new(Float64Array::from(close.clone()));
        // STOCHRSI: timeperiod=14, fastk=5, fastd=3, fastd_matype=0
        let stochrsi_params = vec![14.0_f64, 5.0, 3.0, 0.0];
        let values = [array.clone()];
        let mut rsi_k = TaEvaluator {
            func: TaFn::StochrsiFastk,
            params: stochrsi_params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut rsi_d = TaEvaluator {
            func: TaFn::StochrsiFastd,
            params: stochrsi_params,
            densify_scratch: Vec::new(),
        };
        let got_k = rsi_k
            .evaluate_all(&values, close.len())
            .expect("stochrsi k");
        let got_d = rsi_d
            .evaluate_all(&values, close.len())
            .expect("stochrsi d");
        let (expect_k, expect_d) = stochrsi(&close, 14, 5, 3, 0).expect("stochrsi");
        assert_f64_series_bit_exact(&array_values(&got_k), &expect_k);
        assert_f64_series_bit_exact(&array_values(&got_d), &expect_d);

        // MACDEXT: fast=12 type0, slow=26 type0, signal=9 type0
        let macdext_params = vec![12.0_f64, 0.0, 26.0, 0.0, 9.0, 0.0];
        let mut line = TaEvaluator {
            func: TaFn::Macdext,
            params: macdext_params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut signal = TaEvaluator {
            func: TaFn::MacdextSignal,
            params: macdext_params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut hist = TaEvaluator {
            func: TaFn::MacdextHist,
            params: macdext_params,
            densify_scratch: Vec::new(),
        };
        let got_m = line.evaluate_all(&values, close.len()).expect("macdext");
        let got_s = signal
            .evaluate_all(&values, close.len())
            .expect("macdext signal");
        let got_h = hist
            .evaluate_all(&values, close.len())
            .expect("macdext hist");
        let (expect_m, expect_s, expect_h) =
            macdext(&close, 12, 0, 26, 0, 9, 0).expect("macdext kernel");
        assert_f64_series_bit_exact(&array_values(&got_m), &expect_m);
        assert_f64_series_bit_exact(&array_values(&got_s), &expect_s);
        assert_f64_series_bit_exact(&array_values(&got_h), &expect_h);
        multi_out_clear();
    }

    #[test]
    fn multi_out_kernel_error_does_not_pollute_cache() {
        // C4-Q-002: invalid period on miss must not store; a later valid call must miss-then-compute.
        multi_out_clear();
        let close: Vec<f64> = (1..=30).map(f64::from).collect();
        let array: ArrayRef = Arc::new(Float64Array::from(close.clone()));
        let values = [array.clone()];
        let mut bad = TaEvaluator {
            func: TaFn::BbandsUpper,
            params: vec![0.0_f64, 2.0, 2.0], // invalid period
            densify_scratch: Vec::new(),
        };
        let err = bad.evaluate_all(&values, close.len());
        assert!(err.is_err(), "period 0 must fail");
        // Cache must be empty — a valid sibling pair must still compute correctly.
        let params = vec![5.0_f64, 2.0, 2.0];
        let ids = [series_id(&array)];
        assert!(
            multi_out_lookup(MultiFamily::Bbands, &ids, &params, 0).is_none(),
            "failed evaluate_all must not leave a cache entry"
        );
        let mut good = TaEvaluator {
            func: TaFn::BbandsUpper,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let out = good.evaluate_all(&values, close.len()).expect("valid");
        let (kernel_u, _, _) = bbands(&close, 5, 2.0, 2.0).expect("bbands");
        assert_f64_series_bit_exact(&array_values(&out), &kernel_u);
        multi_out_clear();
    }

    #[test]
    fn evaluate_all_too_few_series_errors_loud() {
        // C5-Q-002: arity guard on evaluate_all (e.g. ATR needs 3 series).
        multi_out_clear();
        let close: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0]));
        let mut eval = TaEvaluator {
            func: TaFn::Atr,
            params: vec![3.0],
            densify_scratch: Vec::new(),
        };
        let err = eval
            .evaluate_all(std::slice::from_ref(&close), 3)
            .expect_err("ATR with 1 series must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("expected 3 series") || msg.contains("3 series"),
            "{msg}"
        );
        multi_out_clear();
    }

    #[test]
    fn evaluate_all_sliced_float64_borrow_matches_kernel() {
        // C4-Q-003: sliced array (offset values buffer) via null-free borrow path.
        multi_out_clear();
        let full: Vec<f64> = (0..40).map(|i| f64::from(i) + 1.0).collect();
        let full_array = Float64Array::from(full.clone());
        let sliced = full_array.slice(5, 20);
        let sliced_ref: ArrayRef = Arc::new(sliced);
        let expected_close: Vec<f64> = full[5..25].to_vec();
        assert!(try_borrow_null_free_f64(&sliced_ref).is_some());
        let mut eval = TaEvaluator {
            func: TaFn::Ema,
            params: vec![3.0],
            densify_scratch: Vec::new(),
        };
        let out = eval
            .evaluate_all(std::slice::from_ref(&sliced_ref), expected_close.len())
            .expect("sliced ema");
        let kernel = ema(&expected_close, 3).expect("ema");
        assert_f64_series_bit_exact(&array_values(&out), &kernel);
        // Multi-output on the same slice.
        let params = vec![5.0_f64, 2.0, 2.0];
        let mut upper = TaEvaluator {
            func: TaFn::BbandsUpper,
            params: params.clone(),
            densify_scratch: Vec::new(),
        };
        let mut lower = TaEvaluator {
            func: TaFn::BbandsLower,
            params,
            densify_scratch: Vec::new(),
        };
        let values = [sliced_ref];
        let out_u = upper
            .evaluate_all(&values, expected_close.len())
            .expect("upper");
        let out_l = lower
            .evaluate_all(&values, expected_close.len())
            .expect("lower");
        let (kernel_u, _, kernel_l) = bbands(&expected_close, 5, 2.0, 2.0).expect("bbands");
        assert_f64_series_bit_exact(&array_values(&out_u), &kernel_u);
        assert_f64_series_bit_exact(&array_values(&out_l), &kernel_l);
        multi_out_clear();
    }

    #[test]
    fn compute_routes_every_spec_to_a_family_or_shared_arm() {
        // Split pin: every SPECS entry must hit a family/shared compute arm, never
        // `family_dispatch_miss` (which would mean a router/table drift).
        let close: Vec<f64> = (1..=64).map(|i| 50.0 + f64::from(i)).collect();
        let high: Vec<f64> = close.iter().map(|value| value + 1.0).collect();
        let low: Vec<f64> = close.iter().map(|value| value - 1.0).collect();
        let open: Vec<f64> = close.iter().map(|value| value - 0.25).collect();
        let volume: Vec<f64> = (0..64).map(|i| 100.0 + f64::from(i)).collect();
        for &(name, func) in SPECS {
            let series: Vec<&[f64]> = match func {
                TaFn::Ad | TaFn::Adosc | TaFn::Mfi => {
                    vec![
                        high.as_slice(),
                        low.as_slice(),
                        close.as_slice(),
                        volume.as_slice(),
                    ]
                }
                TaFn::Obv => vec![close.as_slice(), volume.as_slice()],
                _ => match func.n_series() {
                    1 => vec![close.as_slice()],
                    2 => vec![high.as_slice(), low.as_slice()],
                    3 => vec![high.as_slice(), low.as_slice(), close.as_slice()],
                    4 => vec![
                        open.as_slice(),
                        high.as_slice(),
                        low.as_slice(),
                        close.as_slice(),
                    ],
                    n_series => panic!("{name}: unexpected n_series {n_series}"),
                },
            };
            let params: Vec<f64> = match func {
                TaFn::Mama | TaFn::Fama => vec![0.5, 0.05],
                TaFn::Sar => vec![0.02, 0.2],
                TaFn::Sarext => vec![0.0, 0.0, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2],
                _ => vec![14.0; func.n_scalars()],
            };
            match func.compute(&series, &params) {
                Ok(_) => {}
                Err(err) => {
                    let msg = err.to_string();
                    assert!(
                        !msg.contains("udf family dispatch"),
                        "{name}: family dispatch miss: {msg}"
                    );
                }
            }
        }
    }
}
