//! Crate-wide argument contracts for every kernel:
//!
//! 1. A period below the TA-Lib minimum → `TaError::InvalidPeriod` (never a panic).
//! 2. A period above [`repark_ta::MAX_PERIOD`] → `TaError::InvalidPeriod`, including `usize::MAX`.
//! 3. An input too short for one output → a full-length all-NaN vector, not an error.
//! 4. An empty input → an empty vector.

use repark_ta::{
    MAX_PERIOD, TA_REAL_MAX, TaError, ad, adosc, adx, adxr, apo, aroon, aroonosc, atr, avgprice,
    bbands, beta, bop, cci, cmo, correl, dema, dx, ema, kama, linearreg, linearreg_angle,
    linearreg_intercept, linearreg_slope, ma, macd, macdext, macdfix, mama, mavp, max, medprice,
    mfi, midpoint, midprice, min, minus_di, minus_dm, mom, natr, obv, plus_di, plus_dm, ppo, roc,
    rocp, rocr, rocr100, rsi, sar, sarext, sma, stddev, stoch, stochf, stochrsi, sum, t3, tema,
    trange, trima, trix, tsf, typprice, ultosc, var, wclprice, willr, wma,
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
        ("dx", 2, Box::new(|d, p| dx(d, d, d, p))),
        ("adxr", 2, Box::new(|d, p| adxr(d, d, d, p))),
        ("plus_di", 1, Box::new(|d, p| plus_di(d, d, d, p))),
        ("minus_di", 1, Box::new(|d, p| minus_di(d, d, d, p))),
        ("plus_dm", 1, Box::new(|d, p| plus_dm(d, d, p))),
        ("minus_dm", 1, Box::new(|d, p| minus_dm(d, d, p))),
        ("ma", 1, Box::new(|d, p| ma(d, p, 0))),
        ("natr", 1, Box::new(|d, p| natr(d, d, d, p))),
        ("beta", 1, Box::new(|d, p| beta(d, d, p))),
        ("mfi", 2, Box::new(|d, p| mfi(d, d, d, d, p))),
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
    assert_eq!(
        natr(&a, &b, &a, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        beta(&a, &b, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
}

/// Check the no-period price transforms' empty, output, and length contracts.
#[test]
fn price_transform_argument_contract() {
    let data = [1.0, 2.0, 3.0, 4.0];
    let avg = avgprice(&data, &data, &data, &data).expect("avgprice");
    assert_eq!(avg.len(), data.len());
    assert!(avg.iter().all(|v| v.is_finite()));
    let med = medprice(&data, &data).expect("medprice");
    assert!(med.iter().all(|v| v.is_finite()));
    let typ = typprice(&data, &data, &data).expect("typprice");
    assert!(typ.iter().all(|v| v.is_finite()));
    let wcl = wclprice(&data, &data, &data).expect("wclprice");
    assert!(wcl.iter().all(|v| v.is_finite()));

    assert!(avgprice(&[], &[], &[], &[]).expect("empty").is_empty());
    assert!(medprice(&[], &[]).expect("empty").is_empty());
    assert!(typprice(&[], &[], &[]).expect("empty").is_empty());
    assert!(wclprice(&[], &[], &[]).expect("empty").is_empty());

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

/// Check MACD, MA, and period-one directional contracts.
#[test]
fn macd_ma_directional_contract() {
    let data = [1.0, 2.0, 3.0, 4.0, 5.0];
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
    assert!(matches!(
        ma(&data, 0, 0),
        Err(TaError::InvalidPeriod { .. })
    ));
    assert!(matches!(
        ma(&data, 3, 9),
        Err(TaError::UnsupportedMaType { matype: 9, .. })
    ));
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

/// Check ULTOSC, APO, PPO, and BOP argument contracts.
#[test]
fn ultosc_apo_ppo_bop_argument_contract() {
    let data = [1.0, 2.0, 3.0, 4.0];
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

    let short = [1.0, 2.0, 3.0];
    for out in [
        ultosc(&short, &short, &short, 7, 14, 28).expect("ultosc short"),
        apo(&short, 12, 26, 0).expect("apo short"),
        ppo(&short, 12, 26, 0).expect("ppo short"),
    ] {
        assert_eq!(out.len(), short.len());
        assert!(out.iter().all(|v| v.is_nan()));
    }

    assert!(ultosc(&[], &[], &[], 7, 14, 28).expect("empty").is_empty());
    assert!(apo(&[], 12, 26, 0).expect("empty").is_empty());
    assert!(ppo(&[], 12, 26, 0).expect("empty").is_empty());
    assert!(bop(&[], &[], &[], &[]).expect("empty").is_empty());

    let bop_out = bop(&data, &data, &data, &data).expect("bop");
    assert_eq!(bop_out.len(), data.len());
    assert!(bop_out.iter().all(|v| v.is_finite()));

    let apo_mama = apo(&data, 12, 26, 7).expect("apo mama short");
    assert!(apo_mama.iter().all(|v| v.is_nan()));
    assert!(matches!(
        ppo(&data, 12, 26, 99),
        Err(TaError::UnsupportedMaType { matype: 99, .. })
    ));
}

/// Check stochastic period, length, short-input, and `matype` contracts.
#[test]
fn stochastics_argument_contract() {
    let data = [1.0, 2.0, 3.0, 4.0, 5.0];
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

/// Check MAMA, SAR, SAREXT, and MAVP parameter and shape contracts.
#[test]
#[allow(clippy::too_many_lines)] // one flat table of the four kernels' contracts.
fn parked_four_argument_contract() {
    let data = [1.0, 2.0, 3.0, 4.0, 5.0];

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
    let (mama_out, fama_out) = mama(&data, 0.5, 0.05).expect("mama short");
    assert_eq!(mama_out.len(), data.len());
    assert!(mama_out.iter().chain(&fama_out).all(|v| v.is_nan()));
    let (mama_out, fama_out) = mama(&[], 0.5, 0.05).expect("mama empty");
    assert!(mama_out.is_empty() && fama_out.is_empty());

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
    let short = mavp(&data, &periods, 5, 20, 0).expect("mavp short");
    assert_eq!(short.len(), data.len());
    assert!(short.iter().all(|v| v.is_nan()));
    assert!(mavp(&[], &[], 5, 20, 0).expect("mavp empty").is_empty());
}

/// Check volume-family period and shape contracts.
#[test]
fn volume_family_argument_contract() {
    let data = [1.0, 2.0, 3.0, 4.0];
    let ad_out = ad(&data, &data, &data, &data).expect("ad");
    assert_eq!(ad_out.len(), data.len());
    assert!(ad_out.iter().all(|v| v.is_finite()));
    let obv_out = obv(&data, &data).expect("obv");
    assert_eq!(obv_out.len(), data.len());
    assert!(obv_out.iter().all(|v| v.is_finite()));
    assert_eq!(obv_out[0].to_bits(), data[0].to_bits());

    assert!(ad(&[], &[], &[], &[]).expect("ad empty").is_empty());
    assert!(obv(&[], &[]).expect("obv empty").is_empty());
    assert!(
        adosc(&[], &[], &[], &[], 3, 10)
            .expect("adosc empty")
            .is_empty()
    );

    assert_eq!(
        adosc(&data, &data, &data, &data, 1, 10),
        Err(TaError::InvalidPeriod {
            name: "optInFastPeriod",
            value: 1,
            min: 2,
        })
    );
    assert!(matches!(
        adosc(&data, &data, &data, &data, 3, usize::MAX),
        Err(TaError::InvalidPeriod {
            name: "optInSlowPeriod",
            ..
        })
    ));
    let short = adosc(&data, &data, &data, &data, 3, 10).expect("adosc short");
    assert_eq!(short.len(), data.len());
    assert!(short.iter().all(|v| v.is_nan()));

    let a = [1.0, 2.0, 3.0];
    let b = [1.0, 2.0];
    assert_eq!(
        ad(&a, &b, &a, &a),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        adosc(&a, &a, &a, &b, 3, 10),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        obv(&a, &b),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
    assert_eq!(
        mfi(&a, &a, &b, &a, 2),
        Err(TaError::LengthMismatch { left: 3, right: 2 })
    );
}
