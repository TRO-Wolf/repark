pub(super) fn leading_invalid_run(series: &[&[f64]]) -> usize {
    series
        .iter()
        .map(|values| {
            values
                .iter()
                .position(|value| !value.is_nan())
                .unwrap_or(values.len())
        })
        .max()
        .unwrap_or(0)
}

fn skipped_start(series: &[&[f64]]) -> usize {
    let shortest = series.iter().map(|values| values.len()).min().unwrap_or(0);
    leading_invalid_run(series).min(shortest)
}

fn trimmed<'a>(series: &[&'a [f64]], start: usize) -> Vec<&'a [f64]> {
    series.iter().map(|values| &values[start..]).collect()
}

fn reprefixed(start: usize, tail: Vec<f64>) -> Vec<f64> {
    let mut out = Vec::with_capacity(start + tail.len());
    out.resize(start, f64::NAN);
    out.extend(tail);
    out
}

pub(super) fn run_with_prefix_skipped(
    series: &[&[f64]],
    run: impl FnOnce(&[&[f64]]) -> crate::Result<Vec<f64>>,
) -> crate::Result<Vec<f64>> {
    let start = skipped_start(series);
    if start == 0 {
        return run(series);
    }
    run(&trimmed(series, start)).map(|tail| reprefixed(start, tail))
}

pub(super) fn run_all_with_prefix_skipped(
    series: &[&[f64]],
    run: impl FnOnce(&[&[f64]]) -> crate::Result<Vec<Vec<f64>>>,
) -> crate::Result<Vec<Vec<f64>>> {
    let start = skipped_start(series);
    if start == 0 {
        return run(series);
    }
    run(&trimmed(series, start)).map(|bands| {
        bands
            .into_iter()
            .map(|tail| reprefixed(start, tail))
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, ArrayRef, Float64Array};
    use datafusion::logical_expr::PartitionEvaluator;

    use super::super::{TaEvaluator, TaFn};
    use super::*;
    use crate::{atr, ema, trange};

    fn evaluate(func: TaFn, params: Vec<f64>, arrays: &[ArrayRef]) -> Vec<f64> {
        let mut evaluator = TaEvaluator {
            func,
            params,
            densify_scratch: Vec::new(),
        };
        let out = evaluator
            .evaluate_all(arrays, arrays[0].len())
            .expect("evaluate_all");
        let floats = out
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        floats.values().to_vec()
    }

    #[test]
    fn leading_run_is_skipped_and_reprefixed_for_ema() {
        let tail: Vec<f64> = (1..=12).map(f64::from).collect();
        let input: Vec<f64> = [f64::NAN, f64::NAN].into_iter().chain(tail).collect();
        let out = run_with_prefix_skipped(&[input.as_slice()], |s| ema(s[0], 3)).expect("ema");
        assert_eq!(out.len(), 14);
        assert!(out[..4].iter().all(|value| value.is_nan()));
        let expected: Vec<f64> = (2..=11).map(f64::from).collect();
        assert_eq!(out[4..].to_vec(), expected);
        assert_eq!(leading_invalid_run(&[input.as_slice()]), 2);
    }

    #[test]
    fn interior_nan_after_the_start_still_propagates() {
        let high = [f64::NAN, 3.0, 4.0, 5.0, 6.0, 7.0];
        let low = [1.0, 2.0, f64::NAN, 4.0, 5.0, 6.0];
        let close = [2.0, 2.5, 3.5, 4.5, 5.5, 6.5];
        let hlc: [&[f64]; 3] = [&high, &low, &close];
        assert_eq!(leading_invalid_run(&hlc), 1);
        let tr = run_with_prefix_skipped(&hlc, |s| trange(s[0], s[1], s[2])).expect("trange");
        assert_eq!(tr.len(), 6);
        assert!(tr[..3].iter().all(|value| value.is_nan()));
        assert_eq!(tr[3..].to_vec(), vec![1.5, 1.5, 1.5]);
        let atr_out = run_with_prefix_skipped(&hlc, |s| atr(s[0], s[1], s[2], 2)).expect("atr");
        assert_eq!(atr_out.len(), 6);
        assert!(atr_out.iter().all(|value| value.is_nan()));
    }

    #[test]
    fn all_invalid_input_yields_one_nan_band_per_output() {
        let input = vec![f64::NAN; 8];
        let bands = run_all_with_prefix_skipped(&[input.as_slice()], |s| {
            TaFn::BbandsUpper.compute_all(s, &[5.0, 2.0, 2.0])
        })
        .expect("bbands");
        assert_eq!(bands.len(), 3);
        for band in &bands {
            assert_eq!(band.len(), 8);
            assert!(band.iter().all(|value| value.is_nan()));
        }
    }

    #[test]
    fn evaluate_all_chained_trange_into_ema_is_finite_after_lookback() {
        let close: Vec<f64> = (0..30).map(|i| 50.0 + f64::from(i % 7)).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 1.5).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 1.0).collect();
        let hlc: Vec<ArrayRef> = [high, low, close]
            .into_iter()
            .map(|values| Arc::new(Float64Array::from(values)) as ArrayRef)
            .collect();
        let tr = evaluate(TaFn::Trange, Vec::new(), &hlc);
        assert!(tr[0].is_nan());
        let tr_array: ArrayRef = Arc::new(Float64Array::from(tr));
        let out = evaluate(TaFn::Ema, vec![5.0], &[tr_array]);
        assert_eq!(out.len(), 30);
        assert!(out[..5].iter().all(|value| value.is_nan()));
        assert!(out[5..].iter().all(|value| value.is_finite()));
    }
}
