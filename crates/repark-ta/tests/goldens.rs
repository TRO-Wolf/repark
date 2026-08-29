//! Bit-exactness gate against C TA-Lib 0.4.0 goldens.
//!
//! Compare each element by `f64::to_bits`; NaN payloads may differ. The walk and flat-plateau
//! fixtures cover normal and `TA_IS_ZERO` branches. `manifest.json` must match test coverage.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::LazyLock;

use repark_ta::{
    ad, adosc, adx, adxr, apo, aroon, aroonosc, atr, avgprice, bbands, beta, bop, cci, cmo, correl,
    dema, dx, ema, kama, linearreg, linearreg_angle, linearreg_intercept, linearreg_slope, ma,
    macd, macdext, macdfix, mama, mavp, max, medprice, mfi, midpoint, midprice, min, minus_di,
    minus_dm, mom, natr, obv, plus_di, plus_dm, ppo, roc, rocp, rocr, rocr100, rsi, sar, sarext,
    sma, stddev, stoch, stochf, stochrsi, sum, t3, tema, trange, trima, trix, tsf, typprice,
    ultosc, var, wclprice, willr, wma,
};

/// Every golden series the tests below consume — checked against `manifest.json` so a series
/// recorded-but-untested (or tested-but-unrecorded) fails loudly.
const CONSUMED: &[&str] = &[
    "fixture_open",
    "fixture_high",
    "fixture_low",
    "fixture_close",
    "fixture_flat_high",
    "fixture_flat_low",
    "fixture_flat_close",
    "sma_2",
    "sma_20",
    "ema_5",
    "ema_8",
    "ema_21",
    "rsi_3",
    "rsi_14",
    "trange",
    "atr_1",
    "atr_14",
    "adx_14",
    "var_5",
    "stddev_5_nbdev1",
    "stddev_5_nbdev2",
    "linearreg_5",
    "linearreg_slope_5",
    "linearreg_intercept_5",
    "linearreg_angle_2",
    "linearreg_angle_14",
    "tsf_5",
    "min_21",
    "min_34",
    "max_21",
    "sum_21",
    "correl_14",
    "wma_10",
    "dema_10",
    "tema_10",
    "trima_10",
    "trima_5",
    "kama_10",
    "t3_5",
    "t3_5_vf05",
    "midpoint_10",
    "midprice_10",
    "bbands_20_upper",
    "bbands_20_middle",
    "bbands_20_lower",
    "bbands_20_unit_upper",
    "bbands_20_unit_lower",
    "bbands_20_up1_upper",
    "bbands_20_up1_lower",
    "bbands_20_dn1_upper",
    "bbands_20_dn1_lower",
    "bbands_20_asym_upper",
    "bbands_20_asym_lower",
    "flat_rsi_3",
    "flat_adx_14",
    "flat_atr_14",
    "flat_var_5",
    "flat_stddev_5",
    "flat_correl_5",
    "flat_kama_10",
    "flat_bbands_20_upper",
    "flat_bbands_20_middle",
    "flat_bbands_20_lower",
    "mom_10",
    "roc_10",
    "rocp_10",
    "rocr_10",
    "rocr100_10",
    "willr_14",
    "cci_14",
    "cmo_14",
    "bop",
    "apo_12_26",
    "ppo_12_26",
    "apo_12_26_type7",
    "ppo_12_26_type7",
    "aroon_14_down",
    "aroon_14_up",
    "aroonosc_14",
    "trix_30",
    "ultosc_7_14_28",
    "fixture_flat_open",
    "flat_cmo_14",
    "flat_willr_14",
    "flat_cci_14",
    "flat_bop",
    "flat_ultosc_7_14_28",
    "dx_14",
    "adxr_14",
    "plus_di_14",
    "minus_di_14",
    "plus_dm_14",
    "minus_dm_14",
    "macd_12_26_9_macd",
    "macd_12_26_9_signal",
    "macd_12_26_9_hist",
    "macdfix_9_macd",
    "macdfix_9_signal",
    "macdfix_9_hist",
    "macdext_12_26_9_macd",
    "macdext_12_26_9_signal",
    "macdext_12_26_9_hist",
    "macdext_12_26_9_type7_macd",
    "macdext_12_26_9_type7_signal",
    "macdext_12_26_9_type7_hist",
    "macdext_mixed_7_0_1_macd",
    "macdext_mixed_7_0_1_signal",
    "macdext_mixed_7_0_1_hist",
    "ma_30_type0",
    "ma_20_type1",
    "flat_dx_14",
    "flat_plus_di_14",
    "flat_minus_di_14",
    "stoch_slowk",
    "stoch_slowd",
    "stochf_fastk",
    "stochf_fastd",
    "stochrsi_fastk",
    "stochrsi_fastd",
    "stoch_type7_slowk",
    "stoch_type7_slowd",
    "stoch_mixed_7_0_slowk",
    "stoch_mixed_7_0_slowd",
    "stochf_type7_fastk",
    "stochf_type7_fastd",
    "stochrsi_type7_fastk",
    "stochrsi_type7_fastd",
    "flat_stochf_fastk",
    "flat_stochf_fastd",
    "natr_14",
    "beta_5",
    "avgprice",
    "medprice",
    "typprice",
    "wclprice",
    "flat_beta_5",
    "fixture_periods",
    "mama_mama",
    "mama_fama",
    "flat_mama_mama",
    "flat_mama_fama",
    "sar",
    "sarext",
    "sarext_long_offset",
    "sarext_short",
    "mavp",
    "mavp_ema",
    "ma_30_type7",
    "fixture_volume",
    "fixture_flat_volume",
    "ad",
    "adosc_3_10",
    "obv",
    "mfi_14",
    "flat_ad",
    "flat_adosc_3_10",
    "flat_obv",
    "flat_mfi_14",
];

fn goldens_dir() -> PathBuf {
    // Runtime `CARGO_MANIFEST_DIR` avoids stale fixture paths when a shared target is reused by
    // another worktree. Fall back to the compile-time value outside Cargo.
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").into());
    PathBuf::from(manifest_dir).join("tests/goldens")
}

/// `manifest.json` series → row count, parsed once.
static MANIFEST: LazyLock<serde_json::Value> = LazyLock::new(|| {
    let raw = fs::read_to_string(goldens_dir().join("manifest.json"))
        .expect("manifest.json missing — run record_ta_goldens.py");
    serde_json::from_str(&raw).expect("manifest.json is not valid JSON")
});

fn manifest_rows(name: &str) -> usize {
    let rows = MANIFEST["series"][name]
        .as_u64()
        .unwrap_or_else(|| panic!("{name} not in manifest.json — re-record the goldens"));
    usize::try_from(rows).expect("row count fits usize")
}

fn golden(name: &str) -> Vec<f64> {
    let path = goldens_dir().join(format!("{name}.bin"));
    let bytes = fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "missing golden {} — re-record via record_ta_goldens.py: {e}",
            path.display()
        )
    });
    let rows = manifest_rows(name);
    assert_eq!(
        bytes.len(),
        rows * 8,
        "{name}: size disagrees with manifest.json"
    );
    bytes
        .chunks_exact(8)
        .map(|c| {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(c);
            f64::from_le_bytes(buf)
        })
        .collect()
}

fn assert_bit_exact(name: &str, ours: &[f64]) {
    let expected = golden(name);
    assert_eq!(ours.len(), expected.len(), "{name}: length mismatch");
    for (i, (a, b)) in ours.iter().zip(&expected).enumerate() {
        if a.is_nan() && b.is_nan() {
            continue;
        }
        assert!(
            a.to_bits() == b.to_bits(),
            "{name}: bit mismatch at row {i}: ours {a:?} ({:#018x}) vs C TA-Lib {b:?} ({:#018x})",
            a.to_bits(),
            b.to_bits()
        );
    }
}

struct Fixture {
    high: Vec<f64>,
    low: Vec<f64>,
    close: Vec<f64>,
}

fn fixture() -> Fixture {
    Fixture {
        high: golden("fixture_high"),
        low: golden("fixture_low"),
        close: golden("fixture_close"),
    }
}

fn flat_fixture() -> Fixture {
    Fixture {
        high: golden("fixture_flat_high"),
        low: golden("fixture_flat_low"),
        close: golden("fixture_flat_close"),
    }
}

#[test]
fn goldens_dir_resolves_from_the_runtime_manifest_dir() {
    // A shared target may run this binary from another worktree; runtime Cargo resolution must win.
    let runtime = std::env::var("CARGO_MANIFEST_DIR")
        .expect("cargo sets CARGO_MANIFEST_DIR in the test process environment");
    assert!(
        goldens_dir().starts_with(&runtime),
        "goldens_dir() must be rooted at the runtime manifest dir {runtime:?}, got {:?}",
        goldens_dir()
    );
    assert!(
        goldens_dir().join("manifest.json").is_file(),
        "the resolved goldens dir must actually contain the fixtures"
    );
}

#[test]
fn manifest_and_tests_cover_the_same_series() {
    let consumed: BTreeSet<&str> = CONSUMED.iter().copied().collect();
    let recorded: BTreeSet<&str> = MANIFEST["series"]
        .as_object()
        .expect("manifest series map")
        .keys()
        .map(String::as_str)
        .collect();
    let untested: Vec<&&str> = recorded.difference(&consumed).collect();
    let unrecorded: Vec<&&str> = consumed.difference(&recorded).collect();
    assert!(
        untested.is_empty(),
        "recorded but not consumed by any test: {untested:?}"
    );
    assert!(
        unrecorded.is_empty(),
        "consumed by tests but not recorded: {unrecorded:?}"
    );
}

/// Compare volume-family kernels with their C 0.4.0 goldens by `f64::to_bits`.
#[test]
fn volume_family_matches_c_talib() {
    let f = fixture();
    let walk_volume = golden("fixture_volume");
    assert_eq!(walk_volume.len(), 5000);
    assert!(
        walk_volume.iter().all(|value| *value > 0.0),
        "walk volume must be strictly positive"
    );

    let ad_out = ad(&f.high, &f.low, &f.close, &walk_volume).expect("ad");
    let adosc_out = adosc(&f.high, &f.low, &f.close, &walk_volume, 3, 10).expect("adosc");
    let obv_out = obv(&f.close, &walk_volume).expect("obv");
    let mfi_out = mfi(&f.high, &f.low, &f.close, &walk_volume, 14).expect("mfi");

    assert!(!ad_out[0].is_nan(), "AD lookback is 0");
    assert!(!obv_out[0].is_nan(), "OBV lookback is 0");
    assert_eq!(
        obv_out[0].to_bits(),
        walk_volume[0].to_bits(),
        "OBV[0] is C's prevOBV = inVolume[startIdx]"
    );
    assert!(
        adosc_out[..9].iter().all(|value| value.is_nan()),
        "ADOSC prefix"
    );
    assert!(!adosc_out[9].is_nan(), "ADOSC first live output");
    assert!(
        mfi_out[..14].iter().all(|value| value.is_nan()),
        "MFI prefix"
    );
    assert!(!mfi_out[14].is_nan(), "MFI first live output");

    assert_bit_exact("ad", &ad_out);
    assert_bit_exact("adosc_3_10", &adosc_out);
    assert_bit_exact("obv", &obv_out);
    assert_bit_exact("mfi_14", &mfi_out);
}

#[test]
fn volume_family_flat_guard_branches_match_c_talib() {
    let f = flat_fixture();
    let flat_volume = golden("fixture_flat_volume");
    assert_eq!(flat_volume.len(), 600);
    assert!(
        flat_volume.iter().all(|value| *value > 0.0),
        "flat volume must be strictly positive"
    );

    let ad_out = ad(&f.high, &f.low, &f.close, &flat_volume).expect("flat ad");
    let adosc_out = adosc(&f.high, &f.low, &f.close, &flat_volume, 3, 10).expect("flat adosc");
    let obv_out = obv(&f.close, &flat_volume).expect("flat obv");
    let mfi_out = mfi(&f.high, &f.low, &f.close, &flat_volume, 14).expect("flat mfi");

    assert!(!ad_out[0].is_nan(), "flat AD lookback is 0");
    assert!(!obv_out[0].is_nan(), "flat OBV lookback is 0");
    assert_eq!(
        obv_out[0].to_bits(),
        flat_volume[0].to_bits(),
        "flat OBV[0] is C's prevOBV = inVolume[startIdx]"
    );
    assert!(
        adosc_out[..9].iter().all(|value| value.is_nan()),
        "flat ADOSC prefix"
    );
    assert!(!adosc_out[9].is_nan(), "flat ADOSC first live output");
    assert!(
        mfi_out[..14].iter().all(|value| value.is_nan()),
        "flat MFI prefix"
    );
    assert!(!mfi_out[14].is_nan(), "flat MFI first live output");

    assert_bit_exact("flat_ad", &ad_out);
    assert_bit_exact("flat_adosc_3_10", &adosc_out);
    assert_bit_exact("flat_obv", &obv_out);
    assert_bit_exact("flat_mfi_14", &mfi_out);
}

#[test]
fn sma_matches_c_talib() {
    let f = fixture();
    assert_bit_exact("sma_2", &sma(&f.close, 2).expect("sma_2"));
    assert_bit_exact("sma_20", &sma(&f.close, 20).expect("sma_20"));
}

#[test]
fn ema_matches_c_talib() {
    let f = fixture();
    assert_bit_exact("ema_5", &ema(&f.close, 5).expect("ema_5"));
    assert_bit_exact("ema_8", &ema(&f.close, 8).expect("ema_8"));
    assert_bit_exact("ema_21", &ema(&f.close, 21).expect("ema_21"));
}

#[test]
fn rsi_matches_c_talib() {
    let f = fixture();
    assert_bit_exact("rsi_3", &rsi(&f.close, 3).expect("rsi_3"));
    assert_bit_exact("rsi_14", &rsi(&f.close, 14).expect("rsi_14"));
}

#[test]
fn trange_and_atr_match_c_talib() {
    let f = fixture();
    assert_bit_exact(
        "trange",
        &trange(&f.high, &f.low, &f.close).expect("trange"),
    );
    assert_bit_exact("atr_1", &atr(&f.high, &f.low, &f.close, 1).expect("atr_1"));
    assert_bit_exact(
        "atr_14",
        &atr(&f.high, &f.low, &f.close, 14).expect("atr_14"),
    );
}

#[test]
fn adx_matches_c_talib() {
    let f = fixture();
    assert_bit_exact(
        "adx_14",
        &adx(&f.high, &f.low, &f.close, 14).expect("adx_14"),
    );
}

#[test]
fn var_and_stddev_match_c_talib() {
    let f = fixture();
    assert_bit_exact("var_5", &var(&f.close, 5, 1.0).expect("var_5"));
    assert_bit_exact(
        "stddev_5_nbdev1",
        &stddev(&f.close, 5, 1.0).expect("stddev nbdev1"),
    );
    assert_bit_exact(
        "stddev_5_nbdev2",
        &stddev(&f.close, 5, 2.0).expect("stddev nbdev2"),
    );
}

#[test]
fn linearreg_family_matches_c_talib() {
    let f = fixture();
    assert_bit_exact("linearreg_5", &linearreg(&f.close, 5).expect("linearreg_5"));
    assert_bit_exact(
        "linearreg_slope_5",
        &linearreg_slope(&f.close, 5).expect("linearreg_slope_5"),
    );
    assert_bit_exact(
        "linearreg_intercept_5",
        &linearreg_intercept(&f.close, 5).expect("linearreg_intercept_5"),
    );
    assert_bit_exact(
        "linearreg_angle_2",
        &linearreg_angle(&f.close, 2).expect("linearreg_angle_2"),
    );
    assert_bit_exact(
        "linearreg_angle_14",
        &linearreg_angle(&f.close, 14).expect("linearreg_angle_14"),
    );
    assert_bit_exact("tsf_5", &tsf(&f.close, 5).expect("tsf_5"));
}

#[test]
fn min_max_sum_match_c_talib() {
    let f = fixture();
    assert_bit_exact("min_21", &min(&f.close, 21).expect("min_21"));
    assert_bit_exact("min_34", &min(&f.close, 34).expect("min_34"));
    assert_bit_exact("max_21", &max(&f.close, 21).expect("max_21"));
    assert_bit_exact("sum_21", &sum(&f.close, 21).expect("sum_21"));
}

#[test]
fn correl_matches_c_talib() {
    let f = fixture();
    assert_bit_exact(
        "correl_14",
        &correl(&f.high, &f.low, 14).expect("correl_14"),
    );
}

#[test]
fn overlap_ma_family_matches_c_talib() {
    let f = fixture();
    assert_bit_exact("wma_10", &wma(&f.close, 10).expect("wma_10"));
    assert_bit_exact("dema_10", &dema(&f.close, 10).expect("dema_10"));
    assert_bit_exact("tema_10", &tema(&f.close, 10).expect("tema_10"));
    assert_bit_exact("trima_10", &trima(&f.close, 10).expect("trima_10"));
    assert_bit_exact("trima_5", &trima(&f.close, 5).expect("trima_5"));
    assert_bit_exact("kama_10", &kama(&f.close, 10).expect("kama_10"));
    assert_bit_exact("t3_5", &t3(&f.close, 5, 0.7).expect("t3_5"));
    assert_bit_exact("t3_5_vf05", &t3(&f.close, 5, 0.5).expect("t3_5_vf05"));
    assert_bit_exact("midpoint_10", &midpoint(&f.close, 10).expect("midpoint_10"));
    assert_bit_exact(
        "midprice_10",
        &midprice(&f.high, &f.low, 10).expect("midprice_10"),
    );
}

#[test]
fn bbands_matches_c_talib() {
    let f = fixture();
    let (upper, middle, lower) = bbands(&f.close, 20, 2.0, 2.0).expect("bbands");
    assert_bit_exact("bbands_20_upper", &upper);
    assert_bit_exact("bbands_20_middle", &middle);
    assert_bit_exact("bbands_20_lower", &lower);
}

#[test]
fn bbands_band_branches_match_c_talib() {
    let f = fixture();
    let (upper, _, lower) = bbands(&f.close, 20, 1.0, 1.0).expect("bbands unit");
    assert_bit_exact("bbands_20_unit_upper", &upper);
    assert_bit_exact("bbands_20_unit_lower", &lower);
    let (upper, _, lower) = bbands(&f.close, 20, 1.0, 2.5).expect("bbands up1");
    assert_bit_exact("bbands_20_up1_upper", &upper);
    assert_bit_exact("bbands_20_up1_lower", &lower);
    let (upper, _, lower) = bbands(&f.close, 20, 2.5, 1.0).expect("bbands dn1");
    assert_bit_exact("bbands_20_dn1_upper", &upper);
    assert_bit_exact("bbands_20_dn1_lower", &lower);
    let (upper, _, lower) = bbands(&f.close, 20, 1.5, 2.5).expect("bbands asym");
    assert_bit_exact("bbands_20_asym_upper", &upper);
    assert_bit_exact("bbands_20_asym_lower", &lower);
}

#[test]
fn rate_of_change_family_matches_c_talib() {
    let f = fixture();
    assert_bit_exact("mom_10", &mom(&f.close, 10).expect("mom_10"));
    assert_bit_exact("roc_10", &roc(&f.close, 10).expect("roc_10"));
    assert_bit_exact("rocp_10", &rocp(&f.close, 10).expect("rocp_10"));
    assert_bit_exact("rocr_10", &rocr(&f.close, 10).expect("rocr_10"));
    assert_bit_exact("rocr100_10", &rocr100(&f.close, 10).expect("rocr100_10"));
}

#[test]
fn willr_cci_cmo_match_c_talib() {
    let f = fixture();
    assert_bit_exact(
        "willr_14",
        &willr(&f.high, &f.low, &f.close, 14).expect("willr_14"),
    );
    assert_bit_exact(
        "cci_14",
        &cci(&f.high, &f.low, &f.close, 14).expect("cci_14"),
    );
    assert_bit_exact("cmo_14", &cmo(&f.close, 14).expect("cmo_14"));
}

#[test]
fn bop_matches_c_talib() {
    let f = fixture();
    let open = golden("fixture_open");
    assert_bit_exact("bop", &bop(&open, &f.high, &f.low, &f.close).expect("bop"));
}

#[test]
fn apo_ppo_match_c_talib() {
    let f = fixture();
    assert_bit_exact("apo_12_26", &apo(&f.close, 12, 26, 0).expect("apo_12_26"));
    assert_bit_exact("ppo_12_26", &ppo(&f.close, 12, 26, 0).expect("ppo_12_26"));
}

#[test]
fn apo_ppo_matype7_match_c_talib() {
    let f = fixture();
    assert_bit_exact(
        "apo_12_26_type7",
        &apo(&f.close, 12, 26, 7).expect("apo_12_26_type7"),
    );
    assert_bit_exact(
        "ppo_12_26_type7",
        &ppo(&f.close, 12, 26, 7).expect("ppo_12_26_type7"),
    );
}

#[test]
fn aroon_split_and_oscillator_match_c_talib() {
    let f = fixture();
    let (down, up) = aroon(&f.high, &f.low, 14).expect("aroon_14");
    assert_bit_exact("aroon_14_down", &down);
    assert_bit_exact("aroon_14_up", &up);
    assert_bit_exact(
        "aroonosc_14",
        &aroonosc(&f.high, &f.low, 14).expect("aroonosc_14"),
    );
}

#[test]
fn trix_and_ultosc_match_c_talib() {
    let f = fixture();
    assert_bit_exact("trix_30", &trix(&f.close, 30).expect("trix_30"));
    assert_bit_exact(
        "ultosc_7_14_28",
        &ultosc(&f.high, &f.low, &f.close, 7, 14, 28).expect("ultosc_7_14_28"),
    );
}

#[test]
fn directional_family_matches_c_talib() {
    let f = fixture();
    assert_bit_exact("dx_14", &dx(&f.high, &f.low, &f.close, 14).expect("dx_14"));
    assert_bit_exact(
        "adxr_14",
        &adxr(&f.high, &f.low, &f.close, 14).expect("adxr_14"),
    );
    assert_bit_exact(
        "plus_di_14",
        &plus_di(&f.high, &f.low, &f.close, 14).expect("plus_di_14"),
    );
    assert_bit_exact(
        "minus_di_14",
        &minus_di(&f.high, &f.low, &f.close, 14).expect("minus_di_14"),
    );
    assert_bit_exact(
        "plus_dm_14",
        &plus_dm(&f.high, &f.low, 14).expect("plus_dm_14"),
    );
    assert_bit_exact(
        "minus_dm_14",
        &minus_dm(&f.high, &f.low, 14).expect("minus_dm_14"),
    );
}

#[test]
fn macd_family_matches_c_talib() {
    let f = fixture();
    let (m, s, h) = macd(&f.close, 12, 26, 9).expect("macd");
    assert_bit_exact("macd_12_26_9_macd", &m);
    assert_bit_exact("macd_12_26_9_signal", &s);
    assert_bit_exact("macd_12_26_9_hist", &h);
    let (m, s, h) = macdfix(&f.close, 9).expect("macdfix");
    assert_bit_exact("macdfix_9_macd", &m);
    assert_bit_exact("macdfix_9_signal", &s);
    assert_bit_exact("macdfix_9_hist", &h);
    let (m, s, h) = macdext(&f.close, 12, 0, 26, 0, 9, 0).expect("macdext");
    assert_bit_exact("macdext_12_26_9_macd", &m);
    assert_bit_exact("macdext_12_26_9_signal", &s);
    assert_bit_exact("macdext_12_26_9_hist", &h);
}

#[test]
fn macdext_matype7_match_c_talib() {
    let f = fixture();
    let (m, s, h) = macdext(&f.close, 12, 7, 26, 7, 9, 7).expect("macdext type7");
    assert_bit_exact("macdext_12_26_9_type7_macd", &m);
    assert_bit_exact("macdext_12_26_9_type7_signal", &s);
    assert_bit_exact("macdext_12_26_9_type7_hist", &h);
    let (m, s, h) = macdext(&f.close, 12, 7, 26, 0, 9, 1).expect("macdext mixed");
    assert_bit_exact("macdext_mixed_7_0_1_macd", &m);
    assert_bit_exact("macdext_mixed_7_0_1_signal", &s);
    assert_bit_exact("macdext_mixed_7_0_1_hist", &h);
}

#[test]
fn ma_selector_matches_c_talib() {
    let f = fixture();
    assert_bit_exact("ma_30_type0", &ma(&f.close, 30, 0).expect("ma_30_type0"));
    assert_bit_exact("ma_20_type1", &ma(&f.close, 20, 1).expect("ma_20_type1"));
}

#[test]
#[allow(clippy::similar_names)] // slowk/slowd, fastk/fastd mirror TA-Lib's output names.
fn stochastics_match_c_talib() {
    let f = fixture();
    let (slowk, slowd) = stoch(&f.high, &f.low, &f.close, 5, 3, 0, 3, 0).expect("stoch");
    assert_bit_exact("stoch_slowk", &slowk);
    assert_bit_exact("stoch_slowd", &slowd);
    let (fastk, fastd) = stochf(&f.high, &f.low, &f.close, 5, 3, 0).expect("stochf");
    assert_bit_exact("stochf_fastk", &fastk);
    assert_bit_exact("stochf_fastd", &fastd);
    let (rsi_k, rsi_d) = stochrsi(&f.close, 14, 5, 3, 0).expect("stochrsi");
    assert_bit_exact("stochrsi_fastk", &rsi_k);
    assert_bit_exact("stochrsi_fastd", &rsi_d);
}

#[test]
#[allow(clippy::similar_names)] // slowk/slowd, fastk/fastd mirror TA-Lib's output names.
fn stochastics_matype7_match_c_talib() {
    let f = fixture();
    let (slowk, slowd) = stoch(&f.high, &f.low, &f.close, 5, 3, 7, 3, 7).expect("stoch type7");
    assert_bit_exact("stoch_type7_slowk", &slowk);
    assert_bit_exact("stoch_type7_slowd", &slowd);
    let (slowk, slowd) = stoch(&f.high, &f.low, &f.close, 5, 3, 7, 3, 0).expect("stoch mixed 7/0");
    assert_bit_exact("stoch_mixed_7_0_slowk", &slowk);
    assert_bit_exact("stoch_mixed_7_0_slowd", &slowd);
    let (fastk, fastd) = stochf(&f.high, &f.low, &f.close, 5, 3, 7).expect("stochf type7");
    assert_bit_exact("stochf_type7_fastk", &fastk);
    assert_bit_exact("stochf_type7_fastd", &fastd);
    let (rsi_k, rsi_d) = stochrsi(&f.close, 14, 5, 3, 7).expect("stochrsi type7");
    assert_bit_exact("stochrsi_type7_fastk", &rsi_k);
    assert_bit_exact("stochrsi_type7_fastd", &rsi_d);
}

#[test]
#[allow(clippy::similar_names)] // fastk/fastd mirror TA-Lib's output names.
fn stochastics_flat_guard_branch_matches_c_talib() {
    let f = flat_fixture();
    let (fastk, fastd) = stochf(&f.high, &f.low, &f.close, 5, 3, 0).expect("flat stochf");
    assert_bit_exact("flat_stochf_fastk", &fastk);
    assert_bit_exact("flat_stochf_fastd", &fastd);
}

#[test]
fn wg3_flat_guard_branches_match_c_talib() {
    let f = flat_fixture();
    assert_bit_exact(
        "flat_dx_14",
        &dx(&f.high, &f.low, &f.close, 14).expect("flat dx"),
    );
    assert_bit_exact(
        "flat_plus_di_14",
        &plus_di(&f.high, &f.low, &f.close, 14).expect("flat plus_di"),
    );
    assert_bit_exact(
        "flat_minus_di_14",
        &minus_di(&f.high, &f.low, &f.close, 14).expect("flat minus_di"),
    );
}

#[test]
fn wg2_flat_guard_branches_match_c_talib() {
    let f = flat_fixture();
    let open = golden("fixture_flat_open");
    assert_bit_exact("flat_cmo_14", &cmo(&f.close, 14).expect("flat cmo"));
    assert_bit_exact(
        "flat_willr_14",
        &willr(&f.high, &f.low, &f.close, 14).expect("flat willr"),
    );
    assert_bit_exact(
        "flat_cci_14",
        &cci(&f.high, &f.low, &f.close, 14).expect("flat cci"),
    );
    assert_bit_exact(
        "flat_bop",
        &bop(&open, &f.high, &f.low, &f.close).expect("flat bop"),
    );
    assert_bit_exact(
        "flat_ultosc_7_14_28",
        &ultosc(&f.high, &f.low, &f.close, 7, 14, 28).expect("flat ultosc"),
    );
}

#[test]
fn flat_plateau_guard_branches_match_c_talib() {
    let f = flat_fixture();
    assert_bit_exact("flat_rsi_3", &rsi(&f.close, 3).expect("flat rsi"));
    assert_bit_exact(
        "flat_adx_14",
        &adx(&f.high, &f.low, &f.close, 14).expect("flat adx"),
    );
    assert_bit_exact(
        "flat_atr_14",
        &atr(&f.high, &f.low, &f.close, 14).expect("flat atr"),
    );
    assert_bit_exact("flat_var_5", &var(&f.close, 5, 1.0).expect("flat var"));
    assert_bit_exact(
        "flat_stddev_5",
        &stddev(&f.close, 5, 1.0).expect("flat stddev"),
    );
    assert_bit_exact(
        "flat_correl_5",
        &correl(&f.high, &f.low, 5).expect("flat correl"),
    );
    assert_bit_exact("flat_kama_10", &kama(&f.close, 10).expect("flat kama"));
    let (upper, middle, lower) = bbands(&f.close, 20, 2.0, 2.0).expect("flat bbands");
    assert_bit_exact("flat_bbands_20_upper", &upper);
    assert_bit_exact("flat_bbands_20_middle", &middle);
    assert_bit_exact("flat_bbands_20_lower", &lower);
}

#[test]
fn natr_and_beta_match_c_talib() {
    let f = fixture();
    assert_bit_exact(
        "natr_14",
        &natr(&f.high, &f.low, &f.close, 14).expect("natr_14"),
    );
    assert_bit_exact("beta_5", &beta(&f.high, &f.low, 5).expect("beta_5"));
}

#[test]
fn price_transforms_match_c_talib() {
    let f = fixture();
    let open = golden("fixture_open");
    assert_bit_exact(
        "avgprice",
        &avgprice(&open, &f.high, &f.low, &f.close).expect("avgprice"),
    );
    assert_bit_exact("medprice", &medprice(&f.high, &f.low).expect("medprice"));
    assert_bit_exact(
        "typprice",
        &typprice(&f.high, &f.low, &f.close).expect("typprice"),
    );
    assert_bit_exact(
        "wclprice",
        &wclprice(&f.high, &f.low, &f.close).expect("wclprice"),
    );
}

#[test]
fn beta_flat_guard_branch_matches_c_talib() {
    let f = flat_fixture();
    assert_bit_exact("flat_beta_5", &beta(&f.high, &f.low, 5).expect("flat beta"));
}

#[test]
fn mama_matches_c_talib() {
    let f = fixture();
    let (mama_out, fama_out) = mama(&f.close, 0.5, 0.05).expect("mama");
    assert_bit_exact("mama_mama", &mama_out);
    assert_bit_exact("mama_fama", &fama_out);
}

#[test]
fn mama_flat_guard_branches_match_c_talib() {
    let f = flat_fixture();
    let (mama_out, fama_out) = mama(&f.close, 0.5, 0.05).expect("flat mama");
    assert_bit_exact("flat_mama_mama", &mama_out);
    assert_bit_exact("flat_mama_fama", &fama_out);
}

#[test]
fn sar_and_sarext_match_c_talib() {
    let f = fixture();
    assert_bit_exact("sar", &sar(&f.high, &f.low, 0.02, 0.2).expect("sar"));
    assert_bit_exact(
        "sarext",
        &sarext(&f.high, &f.low, 0.0, 0.0, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2).expect("sarext"),
    );
    assert_bit_exact(
        "sarext_long_offset",
        &sarext(
            &f.high, &f.low, 100.0, 0.05, 0.021, 0.022, 0.25, 0.019, 0.018, 0.15,
        )
        .expect("sarext long offset"),
    );
    assert_bit_exact(
        "sarext_short",
        &sarext(
            &f.high, &f.low, -100.0, 0.0, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2,
        )
        .expect("sarext short"),
    );
}

#[test]
fn mavp_matches_c_talib() {
    let f = fixture();
    let periods = golden("fixture_periods");
    assert_bit_exact(
        "mavp",
        &mavp(&f.close, &periods, 5, 20, 0).expect("mavp sma"),
    );
    assert_bit_exact(
        "mavp_ema",
        &mavp(&f.close, &periods, 5, 20, 1).expect("mavp ema"),
    );
    assert_bit_exact(
        "mama_mama",
        &mavp(&f.close, &periods, 5, 20, 7).expect("mavp mama"),
    );
}

#[test]
fn ma_selector_matype7_is_mama_matches_c_talib() {
    let f = fixture();
    assert_bit_exact("ma_30_type7", &ma(&f.close, 30, 7).expect("ma matype 7"));
    assert_bit_exact(
        "mama_mama",
        &ma(&f.close, 100, 7).expect("ma matype 7, other period"),
    );
}
