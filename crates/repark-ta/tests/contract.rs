//! The crate-wide argument contract, enforced for EVERY kernel (docs/testing.md: every spec
//! invariant gets ≥ 1 test):
//!
//! 1. A period below the TA-Lib minimum → `TaError::InvalidPeriod` (never a panic).
//! 2. A period above [`repark_ta::MAX_PERIOD`] → `TaError::InvalidPeriod` — this is also the
//!    guard that keeps every internal period expression (`period + 1`, `2 * period`, the
//!    linearreg cubic) far away from usize overflow, so it is probed with `usize::MAX`.
//! 3. An input too short for one output → a full-length all-NaN vector, not an error.
//! 4. An empty input → an empty vector.

use repark_ta::{
    MAX_PERIOD, TA_REAL_MAX, TaError, adx, adxr, apo, aroon, aroonosc, atr, avgprice, bbands, beta,
    bop, cci, cmo, correl, dema, dx, ema, kama, linearreg, linearreg_angle, linearreg_intercept,
    linearreg_slope, ma, macd, macdext, macdfix, mama, mavp, max, medprice, midpoint, midprice,
    min, minus_di, minus_dm, mom, natr, plus_di, plus_dm, ppo, roc, rocp, rocr, rocr100, rsi, sar,
    sarext, sma, stddev, stoch, stochf, stochrsi, sum, t3, tema, trange, trima, trix, tsf,
    typprice, ultosc, var, wclprice, willr, wma,
};

type Kernel = (
    &'static str,
    usize,
    Box<dyn Fn(&[f64], usize) -> Result<Vec<f64>, TaError>>,
);

/// Every public kernel with a period parameter, adapted to one shape: (name, TA-Lib documented
/// minimum, runner over a single OHLC-ish input).
fn kernels() -> Vec<Kernel> {
    vec![
        ("sma", 2, Box::new(sma)),
        ("ema", 2, Box::new(ema)),
        ("rsi", 2, Box::new(rsi)),
        ("adx", 2, Box::new(|d, p| adx(d, d, d, p))),
        ("atr", 1, Box::new(|d, p| atr(d, d, d, p))),
        ("var", 1, Box::new(|d, p| var(d, p, 1.0))),
        ("stddev", 2, Box::new(|d, p| stddev(d, p, 1.0))),
        ("linearreg", 2, Box::new(linearreg)),
        ("linearreg_slope", 2, Box::new(linearreg_slope)),
        ("linearreg_intercept", 2, Box::new(linearreg_intercept)),
        ("linearreg_angle", 2, Box::new(linearreg_angle)),
        ("tsf", 2, Box::new(tsf)),
        ("min", 2, Box::new(min)),
        ("max", 2, Box::new(max)),
        ("sum", 2, Box::new(sum)),
        ("correl", 1, Box::new(|d, p| correl(d, d, p))),
        ("wma", 2, Box::new(wma)),
        ("dema", 2, Box::new(dema)),
        ("tema", 2, Box::new(tema)),
        ("trima", 2, Box::new(trima)),
        ("kama", 2, Box::new(kama)),
        ("t3", 2, Box::new(|d, p| t3(d, p, 0.7))),
        ("midpoint", 2, Box::new(midpoint)),
        ("midprice", 2, Box::new(|d, p| midprice(d, d, p))),
        (
            "bbands",
            2,
            Box::new(|d, p| bbands(d, p, 2.0, 2.0).map(|(u, _, _)| u)),
        ),
        // WG2 simple-momentum kernels whose period parameter is `optInTimePeriod` (MOM/ROC family
        // allow period 1). ULTOSC/APO/PPO/BOP use different parameter names / no period, so they
        // get dedicated contract tests below.
        ("mom", 1, Box::new(mom)),
        ("roc", 1, Box::new(roc)),
        ("rocp", 1, Box::new(rocp)),
        ("rocr", 1, Box::new(rocr)),
        ("rocr100", 1, Box::new(rocr100)),
        ("willr", 2, Box::new(|d, p| willr(d, d, d, p))),
        ("cci", 2, Box::new(|d, p| cci(d, d, d, p))),
        ("cmo", 2, Box::new(cmo)),
        (
            "aroon_down",
            2,
            Box::new(|d, p| aroon(d, d, p).map(|(dn, _)| dn)),
        ),
        (
            "aroon_up",
            2,
            Box::new(|d, p| aroon(d, d, p).map(|(_, up)| up)),
        ),
        ("aroonosc", 2, Box::new(|d, p| aroonosc(d, d, p))),
        ("trix", 2, Box::new(trix)),
        // WG3 directional family (H/L/C or H/L collapsed onto one series). DX/ADXR require period
        // 2; the DI/DM variants and the MA selector allow period 1 (so the below-min sweep skips
        // them — their period-0 / MAMA rejections are pinned in `macd_ma_directional_contract`).
        ("dx", 2, Box::new(|d, p| dx(d, d, d, p))),
        ("adxr", 2, Box::new(|d, p| adxr(d, d, d, p))),
        ("plus_di", 1, Box::new(|d, p| plus_di(d, d, d, p))),
        ("minus_di", 1, Box::new(|d, p| minus_di(d, d, d, p))),
        ("plus_dm", 1, Box::new(|d, p| plus_dm(d, d, p))),
        ("minus_dm", 1, Box::new(|d, p| minus_dm(d, d, p))),
        ("ma", 1, Box::new(|d, p| ma(d, p, 0))),
        // WG5 sweep-up: NATR (H/L/C collapsed like ATR) and BETA (two-series like CORREL); both
        // allow period 1. The no-period price transforms get a dedicated test below.
        ("natr", 1, Box::new(|d, p| natr(d, d, d, p))),
        ("beta", 1, Box::new(|d, p| beta(d, d, p))),
    ]
}

#[test]
fn below_minimum_period_errors_for_every_kernel() {
    let data = [1.0, 2.0, 3.0, 4.0];
    for (name, min, run) in kernels() {
        if min == 1 {
            continue; // 0 is below every minimum, but usize can't go below 1's floor via -1.
        }
        let got = run(&data, min - 1);
        assert_eq!(
            got,
            Err(TaError::InvalidPeriod {
                name: "optInTimePeriod",
                value: min - 1,
                min
            }),
            "{name}: period {} must be rejected",
            min - 1
        );
    }
    // The min == 1 kernels reject 0 explicitly.
    assert!(matches!(
        atr(&data, &data, &data, 0),
        Err(TaError::InvalidPeriod { .. })
    ));
    assert!(matches!(
        var(&data, 0, 1.0),
        Err(TaError::InvalidPeriod { .. })
    ));
    assert!(matches!(
        correl(&data, &data, 0),
        Err(TaError::InvalidPeriod { .. })
    ));
    // linearreg's period-1 arithmetic must not run before validation (period 0 once panicked).
    assert!(matches!(
        linearreg(&data, 0),
        Err(TaError::InvalidPeriod { .. })
    ));
}

#[test]
fn above_max_period_errors_not_overflows_for_every_kernel() {
    let data = [1.0, 2.0, 3.0, 4.0];
    for (name, min, run) in kernels() {
        for period in [MAX_PERIOD + 1, usize::MAX] {
            let got = run(&data, period);
            assert_eq!(
                got,
                Err(TaError::InvalidPeriod {
                    name: "optInTimePeriod",
                    value: period,
                    min
                }),
                "{name}: period {period} must be rejected, never wrap/overflow"
            );
        }
    }
}

#[test]
fn short_input_is_all_nan_for_every_kernel() {
    // 3 rows can't produce a single output for any period-8 kernel (max lookback here is
    // adx's 2*8-1 = 15).
    let data = [1.0, 2.0, 3.0];
    for (name, _, run) in kernels() {
        let out = run(&data, 8).unwrap_or_else(|e| panic!("{name}: short input errored: {e}"));
        assert_eq!(
            out.len(),
            data.len(),
            "{name}: output must stay input-length"
        );
        assert!(
            out.iter().all(|v| v.is_nan()),
            "{name}: short input must be all-NaN"
        );
    }
    let out = trange(&data[..1], &data[..1], &data[..1]).expect("trange 1 row");
    assert_eq!(out.len(), 1);
    assert!(out[0].is_nan());
}

#[test]
fn empty_input_yields_empty_output_for_every_kernel() {
    for (name, _, run) in kernels() {
        let out = run(&[], 8).unwrap_or_else(|e| panic!("{name}: empty input errored: {e}"));
        assert!(out.is_empty(), "{name}: empty in, empty out");
    }
    assert!(trange(&[], &[], &[]).expect("trange empty").is_empty());
}

#[test]
fn multi_series_length_mismatch_errors() {
    let a = [1.0, 2.0, 3.0];
    let b = [1.0, 2.0];
    assert_eq!(
        trange(&a, &b, &a),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        atr(&a, &a, &b, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        correl(&a, &b, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        midprice(&a, &b, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        willr(&a, &b, &a, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        cci(&a, &a, &b, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        aroon(&a, &b, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        bop(&a, &b, &a, &a),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        ultosc(&a, &a, &b, 2, 2, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    // WG3 directional family.
    assert_eq!(
        dx(&a, &b, &a, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        adxr(&a, &a, &b, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        plus_di(&a, &b, &a, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        minus_di(&a, &a, &b, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        plus_dm(&a, &b, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        minus_dm(&a, &b, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    // WG5 sweep-up.
    assert_eq!(
        natr(&a, &b, &a, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        beta(&a, &b, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
}

/// The four price transforms carry no period parameter and have lookback 0, so they sit outside the
/// shared `kernels()` sweep — their empty-input, every-bar-output, and length-mismatch contract is
/// pinned here directly (mirroring `trange`, the other no-lookback family member).
#[test]
fn price_transform_argument_contract() {
    let data = [1.0, 2.0, 3.0, 4.0];
    // No lookback → every bar produces a finite value (no all-NaN prefix).
    let avg = avgprice(&data, &data, &data, &data).expect("avgprice");
    assert_eq!(avg.len(), data.len());
    assert!(avg.iter().all(|v| v.is_finite()));
    let med = medprice(&data, &data).expect("medprice");
    assert!(med.iter().all(|v| v.is_finite()));
    let typ = typprice(&data, &data, &data).expect("typprice");
    assert!(typ.iter().all(|v| v.is_finite()));
    let wcl = wclprice(&data, &data, &data).expect("wclprice");
    assert!(wcl.iter().all(|v| v.is_finite()));

    // Empty in → empty out.
    assert!(avgprice(&[], &[], &[], &[]).expect("empty").is_empty());
    assert!(medprice(&[], &[]).expect("empty").is_empty());
    assert!(typprice(&[], &[], &[]).expect("empty").is_empty());
    assert!(wclprice(&[], &[], &[]).expect("empty").is_empty());

    // Length mismatch across the O/H/L/C series.
    let a = [1.0, 2.0, 3.0];
    let b = [1.0, 2.0];
    assert_eq!(
        avgprice(&a, &b, &a, &a),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        medprice(&a, &b),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        typprice(&a, &a, &b),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        wclprice(&a, &b, &a),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
}

/// The MACD family (`optInFast/Slow/SignalPeriod`), the `MA` selector, and the directional DI/DM
/// functions that allow `period == 1` sit outside the shared `kernels()` sweep — their below-min,
/// above-`MAX_PERIOD`, short/empty, and `UnsupportedMaType` contract is pinned here directly.
#[test]
fn macd_ma_directional_contract() {
    let data = [1.0, 2.0, 3.0, 4.0, 5.0];
    // MACD — each period is validated in its own name; signal minimum is 1.
    assert_eq!(
        macd(&data, 1, 26, 9),
        Err(TaError::InvalidPeriod {
            name: "optInFastPeriod",
            value: 1,
            min: 2,
        })
    );
    assert!(matches!(
        macd(&data, 12, usize::MAX, 9),
        Err(TaError::InvalidPeriod {
            name: "optInSlowPeriod",
            ..
        })
    ));
    assert_eq!(
        macd(&data, 12, 26, 0),
        Err(TaError::InvalidPeriod {
            name: "optInSignalPeriod",
            value: 0,
            min: 1,
        })
    );
    assert_eq!(
        macdfix(&data, 0),
        Err(TaError::InvalidPeriod {
            name: "optInSignalPeriod",
            value: 0,
            min: 1,
        })
    );
    assert!(matches!(
        macdext(&data, 1, 0, 26, 0, 9, 0),
        Err(TaError::InvalidPeriod {
            name: "optInFastPeriod",
            ..
        })
    ));
    // MACDEXT accepts MAMA (7) on any leg — short series is all-NaN success; out-of-range fails.
    let (macd_m, sig_m, hist_m) = macdext(&data, 12, 7, 26, 0, 9, 0).expect("macdext mama short");
    assert!(
        macd_m
            .iter()
            .chain(&sig_m)
            .chain(&hist_m)
            .all(|v| v.is_nan())
    );
    assert!(matches!(
        macdext(&data, 12, 9, 26, 0, 9, 0),
        Err(TaError::UnsupportedMaType { matype: 9, .. })
    ));
    // Short input → three full-length all-NaN outputs; empty → three empty.
    let short = [1.0, 2.0, 3.0];
    for (m, s, h) in [
        macd(&short, 12, 26, 9).expect("macd short"),
        macdfix(&short, 9).expect("macdfix short"),
        macdext(&short, 12, 0, 26, 0, 9, 0).expect("macdext short"),
    ] {
        assert_eq!(m.len(), short.len());
        assert!(m.iter().chain(&s).chain(&h).all(|v| v.is_nan()));
    }
    for (m, s, h) in [
        macd(&[], 12, 26, 9).expect("macd empty"),
        macdfix(&[], 9).expect("macdfix empty"),
        macdext(&[], 12, 0, 26, 0, 9, 0).expect("macdext empty"),
    ] {
        assert!(m.is_empty() && s.is_empty() && h.is_empty());
    }
    // The MA selector rejects period 0 and out-of-range matypes (MAMA matype 7 is in-range for MA).
    assert!(matches!(
        ma(&data, 0, 0),
        Err(TaError::InvalidPeriod { .. })
    ));
    assert!(matches!(
        ma(&data, 3, 9),
        Err(TaError::UnsupportedMaType { matype: 9, .. })
    ));
    // Octo C1-Q-006 / C1-L-006: MA@7 routes through mama (non-trivial values; not a zero stub).
    let mama_via_ma = ma(&data, 12, 7).expect("ma matype 7");
    let (mama_line, _) = mama(&data, 0.5, 0.05).expect("mama");
    assert_eq!(mama_via_ma.len(), mama_line.len());
    for (index, (left, right)) in mama_via_ma.iter().zip(mama_line.iter()).enumerate() {
        assert_eq!(
            left.to_bits(),
            right.to_bits(),
            "ma(..., 7) must equal mama at {index}"
        );
    }
    // DI/DM allow period 1; period 0 is still rejected.
    for got in [
        plus_di(&data, &data, &data, 0),
        minus_di(&data, &data, &data, 0),
    ] {
        assert!(matches!(got, Err(TaError::InvalidPeriod { .. })));
    }
    assert!(matches!(
        plus_dm(&data, &data, 0),
        Err(TaError::InvalidPeriod { .. })
    ));
    assert!(matches!(
        minus_dm(&data, &data, 0),
        Err(TaError::InvalidPeriod { .. })
    ));
}

/// ULTOSC/APO/PPO name their period parameters differently (or, for BOP, carry none), so they sit
/// outside the shared `kernels()` sweep — the same below-min/above-MAX/short/empty contract is
/// asserted here directly.
#[test]
fn ultosc_apo_ppo_bop_argument_contract() {
    let data = [1.0, 2.0, 3.0, 4.0];
    // ULTOSC — each period is `optInTimePeriod{1,2,3}`, minimum 1.
    assert_eq!(
        ultosc(&data, &data, &data, 0, 14, 28),
        Err(TaError::InvalidPeriod {
            name: "optInTimePeriod1",
            value: 0,
            min: 1,
        })
    );
    for period in [MAX_PERIOD + 1, usize::MAX] {
        assert!(matches!(
            ultosc(&data, &data, &data, 7, period, 28),
            Err(TaError::InvalidPeriod {
                name: "optInTimePeriod2",
                ..
            })
        ));
    }
    // APO/PPO — `optInFastPeriod`/`optInSlowPeriod`, minimum 2.
    assert_eq!(
        apo(&data, 1, 26, 0),
        Err(TaError::InvalidPeriod {
            name: "optInFastPeriod",
            value: 1,
            min: 2,
        })
    );
    assert!(matches!(
        ppo(&data, 12, usize::MAX, 0),
        Err(TaError::InvalidPeriod {
            name: "optInSlowPeriod",
            ..
        })
    ));

    // Short input → full-length all-NaN (never an error) for the period-bearing kernels.
    let short = [1.0, 2.0, 3.0];
    for out in [
        ultosc(&short, &short, &short, 7, 14, 28).expect("ultosc short"),
        apo(&short, 12, 26, 0).expect("apo short"),
        ppo(&short, 12, 26, 0).expect("ppo short"),
    ] {
        assert_eq!(out.len(), short.len());
        assert!(out.iter().all(|v| v.is_nan()));
    }

    // Empty in → empty out, for all four (BOP has no lookback, so it only needs the empty case).
    assert!(ultosc(&[], &[], &[], 7, 14, 28).expect("empty").is_empty());
    assert!(apo(&[], 12, 26, 0).expect("empty").is_empty());
    assert!(ppo(&[], 12, 26, 0).expect("empty").is_empty());
    assert!(bop(&[], &[], &[], &[]).expect("empty").is_empty());

    // BOP has no lookback — every bar produces a value (no all-NaN short case).
    let bop_out = bop(&data, &data, &data, &data).expect("bop");
    assert_eq!(bop_out.len(), data.len());
    assert!(bop_out.iter().all(|v| v.is_finite()));

    // APO/PPO accept MAMA (matype 7) — short series is all-NaN success; out-of-range still fails.
    let apo_mama = apo(&data, 12, 26, 7).expect("apo mama short");
    assert!(apo_mama.iter().all(|v| v.is_nan()));
    assert!(matches!(
        ppo(&data, 12, 26, 99),
        Err(TaError::UnsupportedMaType { matype: 99, .. })
    ));
}

/// The stochastics (STOCH/STOCHF split into two outputs, STOCHRSI over RSI) are multi-input,
/// multi-output, and carry named period + `matype` parameters, so they sit outside the shared
/// `kernels()` sweep — the same below-min/above-MAX/short/empty/length-mismatch and matype
/// contract is pinned here directly.
#[test]
fn stochastics_argument_contract() {
    let data = [1.0, 2.0, 3.0, 4.0, 5.0];
    // STOCH/STOCHF periods have minimum 1 (so 0 is rejected); STOCHRSI's RSI period has minimum 2.
    assert_eq!(
        stochf(&data, &data, &data, 0, 3, 0),
        Err(TaError::InvalidPeriod {
            name: "optInFastK_Period",
            value: 0,
            min: 1,
        })
    );
    assert!(matches!(
        stoch(&data, &data, &data, 5, 0, 0, 3, 0),
        Err(TaError::InvalidPeriod {
            name: "optInSlowK_Period",
            ..
        })
    ));
    assert_eq!(
        stochrsi(&data, 1, 5, 3, 0),
        Err(TaError::InvalidPeriod {
            name: "optInTimePeriod",
            value: 1,
            min: 2,
        })
    );
    // Above-MAX periods error (never wrap/overflow).
    for period in [MAX_PERIOD + 1, usize::MAX] {
        assert!(matches!(
            stochf(&data, &data, &data, period, 3, 0),
            Err(TaError::InvalidPeriod {
                name: "optInFastK_Period",
                ..
            })
        ));
        assert!(matches!(
            stochrsi(&data, period, 5, 3, 0),
            Err(TaError::InvalidPeriod {
                name: "optInTimePeriod",
                ..
            })
        ));
    }
    // Short input → two full-length all-NaN outputs (never an error); empty → two empty.
    let short = [1.0, 2.0, 3.0];
    for (k, d) in [
        stoch(&short, &short, &short, 5, 3, 0, 3, 0).expect("stoch short"),
        stochf(&short, &short, &short, 5, 3, 0).expect("stochf short"),
    ] {
        assert_eq!(k.len(), short.len());
        assert!(k.iter().chain(&d).all(|v| v.is_nan()));
    }
    let (k, d) = stochrsi(&short, 14, 5, 3, 0).expect("stochrsi short");
    assert_eq!(k.len(), short.len());
    assert!(k.iter().chain(&d).all(|v| v.is_nan()));
    for (k, d) in [
        stoch(&[], &[], &[], 5, 3, 0, 3, 0).expect("stoch empty"),
        stochf(&[], &[], &[], 5, 3, 0).expect("stochf empty"),
    ] {
        assert!(k.is_empty() && d.is_empty());
    }
    let (k, d) = stochrsi(&[], 14, 5, 3, 0).expect("stochrsi empty");
    assert!(k.is_empty() && d.is_empty());
    // Length mismatch across the H/L/C series.
    let a = [1.0, 2.0, 3.0];
    let b = [1.0, 2.0];
    assert_eq!(
        stoch(&a, &b, &a, 5, 3, 0, 3, 0),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        stochf(&a, &a, &b, 5, 3, 0),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    // Matype 7 (MAMA) is accepted on every stochastic smoothing leg — short series is all-NaN
    // success (composed lookback includes MAMA's fixed 32). Out-of-range still fails loud.
    let (fk, fd) = stochf(&data, &data, &data, 5, 3, 7).expect("stochf matype 7 short");
    assert!(fk.iter().chain(&fd).all(|v| v.is_nan()));
    let (sk, sd) = stoch(&data, &data, &data, 5, 3, 7, 3, 7).expect("stoch all-MAMA short");
    assert!(sk.iter().chain(&sd).all(|v| v.is_nan()));
    let (rk, rd) = stochrsi(&data, 14, 5, 3, 7).expect("stochrsi matype 7 short");
    assert!(rk.iter().chain(&rd).all(|v| v.is_nan()));
    assert!(matches!(
        stoch(&data, &data, &data, 5, 3, 0, 3, 9),
        Err(TaError::UnsupportedMaType { matype: 9, .. })
    ));
    assert!(matches!(
        stochf(&data, &data, &data, 5, 3, 9),
        Err(TaError::UnsupportedMaType { matype: 9, .. })
    ));
    assert!(matches!(
        stochrsi(&data, 14, 5, 3, 99),
        Err(TaError::UnsupportedMaType { matype: 99, .. })
    ));
}

/// The parked four (MAMA / SAR / SAREXT / MAVP) carry real-valued or multi-input parameters that
/// sit outside the shared `kernels()` sweep — their range, length-mismatch, and short/empty
/// contract is pinned here directly.
#[test]
#[allow(clippy::too_many_lines)] // one flat table of the four kernels' contracts.
fn parked_four_argument_contract() {
    let data = [1.0, 2.0, 3.0, 4.0, 5.0];

    // --- MAMA: fast/slow limits ∈ [0.01, 0.99]; NaN rejected; short/empty → all-NaN / empty. ---
    assert!(matches!(
        mama(&data, 0.001, 0.05),
        Err(TaError::InvalidRealParam {
            name: "optInFastLimit",
            ..
        })
    ));
    assert!(matches!(
        mama(&data, 0.5, 1.5),
        Err(TaError::InvalidRealParam {
            name: "optInSlowLimit",
            ..
        })
    ));
    assert!(matches!(
        mama(&data, f64::NAN, 0.05),
        Err(TaError::InvalidRealParam {
            name: "optInFastLimit",
            ..
        })
    ));
    // len 5 ≤ lookback 32 → two full-length all-NaN outputs (never an error).
    let (mama_out, fama_out) = mama(&data, 0.5, 0.05).expect("mama short");
    assert_eq!(mama_out.len(), data.len());
    assert!(mama_out.iter().chain(&fama_out).all(|v| v.is_nan()));
    let (mama_out, fama_out) = mama(&[], 0.5, 0.05).expect("mama empty");
    assert!(mama_out.is_empty() && fama_out.is_empty());

    // --- SAR: acceleration/maximum ∈ [0, 3e37]; H/L length mismatch; short (< 2 bars) → all-NaN. ---
    assert!(matches!(
        sar(&data, &data, -0.01, 0.2),
        Err(TaError::InvalidRealParam {
            name: "optInAcceleration",
            ..
        })
    ));
    assert!(matches!(
        sar(&data, &data, 0.02, TA_REAL_MAX * 2.0),
        Err(TaError::InvalidRealParam {
            name: "optInMaximum",
            ..
        })
    ));
    let a = [1.0, 2.0, 3.0];
    let b = [1.0, 2.0];
    assert_eq!(
        sar(&a, &b, 0.02, 0.2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    let short = sar(&[1.0], &[1.0], 0.02, 0.2).expect("sar 1 bar");
    assert_eq!(short.len(), 1);
    assert!(short[0].is_nan());
    assert!(sar(&[], &[], 0.02, 0.2).expect("sar empty").is_empty());

    // --- SAREXT: eight params; start ∈ [-3e37, 3e37] (negative is a legal short start), the rest
    //     ∈ [0, 3e37]; H/L length mismatch; empty → empty. ---
    assert!(matches!(
        sarext(&data, &data, 0.0, -0.01, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2),
        Err(TaError::InvalidRealParam {
            name: "optInOffsetOnReverse",
            ..
        })
    ));
    assert!(matches!(
        sarext(&data, &data, 0.0, 0.0, -1.0, 0.02, 0.2, 0.02, 0.02, 0.2),
        Err(TaError::InvalidRealParam {
            name: "optInAccelerationInitLong",
            ..
        })
    ));
    // A negative start value is NOT rejected — it forces a short start at |start_value|.
    assert!(sarext(&data, &data, -50.0, 0.0, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2).is_ok());
    assert_eq!(
        sarext(&a, &b, 0.0, 0.0, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert!(
        sarext(&[], &[], 0.0, 0.0, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2)
            .expect("sarext empty")
            .is_empty()
    );

    // --- MAVP: min/max ∈ [2, MAX_PERIOD]; matype ∈ 0..=8; periods-series length; short/empty. ---
    let periods = [10.0; 5];
    assert!(matches!(
        mavp(&data, &periods, 1, 20, 0),
        Err(TaError::InvalidPeriod {
            name: "optInMinPeriod",
            value: 1,
            min: 2
        })
    ));
    assert!(matches!(
        mavp(&data, &periods, 5, usize::MAX, 0),
        Err(TaError::InvalidPeriod {
            name: "optInMaxPeriod",
            ..
        })
    ));
    assert!(matches!(
        mavp(&data, &periods, 5, 20, 9),
        Err(TaError::UnsupportedMaType { matype: 9, .. })
    ));
    assert_eq!(
        mavp(&a, &b, 5, 20, 0),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    // len 5 ≤ lookback 19 (SMA, max_period 20) → full-length all-NaN, not an error.
    let short = mavp(&data, &periods, 5, 20, 0).expect("mavp short");
    assert_eq!(short.len(), data.len());
    assert!(short.iter().all(|v| v.is_nan()));
    assert!(mavp(&[], &[], 5, 20, 0).expect("mavp empty").is_empty());
}
