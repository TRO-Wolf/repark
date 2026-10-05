use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array};

pub(super) fn float64_array_from_vec(values: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}

pub(super) fn try_borrow_null_free_f64(array: &ArrayRef) -> Option<&[f64]> {
    let floats = array.as_any().downcast_ref::<Float64Array>()?;
    if floats.null_count() == 0 {
        Some(floats.values().as_ref())
    } else {
        None
    }
}

pub(super) fn try_borrow_all_null_free(arrays: &[ArrayRef]) -> Option<Vec<&[f64]>> {
    arrays.iter().map(try_borrow_null_free_f64).collect()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Float64Builder;
    use datafusion::logical_expr::PartitionEvaluator;

    use super::super::prefix::{run_all_with_prefix_skipped, run_with_prefix_skipped};
    use super::super::{TaEvaluator, TaFn, densify_series_into, multi_out_clear};
    use super::*;

    fn wave(len: usize, phase: f64, scale: f64) -> Vec<f64> {
        (0..len)
            .map(|index| {
                let x = f64::from(u32::try_from(index).unwrap_or(u32::MAX));
                100.0 + scale * (x * 0.37 + phase).sin() + x * 0.013
            })
            .collect()
    }

    fn ohlc(len: usize) -> Vec<ArrayRef> {
        let close = wave(len, 0.0, 3.0);
        let high: Vec<f64> = close
            .iter()
            .zip(wave(len, 1.1, 0.9))
            .map(|(c, w)| c + 1.0 + (w - 100.0).abs())
            .collect();
        let low: Vec<f64> = close
            .iter()
            .zip(wave(len, 2.3, 0.7))
            .map(|(c, w)| c - 1.0 - (w - 100.0).abs())
            .collect();
        vec![
            Arc::new(Float64Array::from(high)),
            Arc::new(Float64Array::from(low)),
            Arc::new(Float64Array::from(close)),
        ]
    }

    fn legacy_dense(values: &[f64]) -> ArrayRef {
        let mut builder = Float64Builder::with_capacity(values.len());
        builder.append_slice(values);
        Arc::new(builder.finish())
    }

    fn legacy_single(func: TaFn, params: &[f64], series: &[ArrayRef]) -> ArrayRef {
        let mut scratches = vec![Vec::new(); series.len()];
        densify_series_into(series, &mut scratches).expect("densify");
        let slices: Vec<&[f64]> = scratches.iter().map(Vec::as_slice).collect();
        let out = run_with_prefix_skipped(&slices, |s| func.compute(s, params)).expect("kernel");
        legacy_dense(&out)
    }

    fn legacy_band(func: TaFn, params: &[f64], series: &[ArrayRef], band: usize) -> ArrayRef {
        let mut scratches = vec![Vec::new(); series.len()];
        densify_series_into(series, &mut scratches).expect("densify");
        let slices: Vec<&[f64]> = scratches.iter().map(Vec::as_slice).collect();
        let bands =
            run_all_with_prefix_skipped(&slices, |s| func.compute_all(s, params)).expect("kernel");
        legacy_dense(&bands[band])
    }

    fn assert_bit_identical(name: &str, got: &ArrayRef, expected: &ArrayRef) {
        let got = got
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("got f64");
        let expected = expected
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("expected f64");
        assert_eq!(got.len(), expected.len(), "{name} len");
        assert_eq!(got.null_count(), expected.null_count(), "{name} nulls");
        assert_eq!(got.nulls(), expected.nulls(), "{name} validity");
        for (index, (a, b)) in got
            .values()
            .iter()
            .zip(expected.values().iter())
            .enumerate()
        {
            assert_eq!(
                a.to_bits(),
                b.to_bits(),
                "{name} row {index}: {a:?} vs {b:?}"
            );
        }
    }

    fn evaluate(func: TaFn, params: &[f64], series: &[ArrayRef]) -> ArrayRef {
        let mut evaluator = TaEvaluator {
            func,
            params: params.to_vec(),
            densify_scratch: Vec::new(),
        };
        evaluator
            .evaluate_all(series, series[0].len())
            .expect("evaluate_all")
    }

    #[test]
    fn ta_glue_zero_copy_and_borrow_bit_identical() {
        let series = ohlc(512);
        let borrowed = try_borrow_all_null_free(&series).expect("all null-free borrow");
        for (slice, array) in borrowed.iter().zip(&series) {
            let floats = array.as_any().downcast_ref::<Float64Array>().expect("f64");
            assert_eq!(slice.as_ptr(), floats.values().as_ptr());
            assert_eq!(slice.len(), floats.len());
        }
        let owned = vec![1.5_f64, f64::NAN, -0.0, 2.25];
        let owned_ptr = owned.as_ptr();
        let zero_copy = float64_array_from_vec(owned.clone());
        assert_bit_identical("zero_copy", &zero_copy, &legacy_dense(&owned));
        let moved = float64_array_from_vec(owned);
        let moved_floats = moved.as_any().downcast_ref::<Float64Array>().expect("f64");
        assert_eq!(moved_floats.values().as_ptr(), owned_ptr);

        let high_low_close = &series[..3];
        let close = &series[2..3];
        let cases: [(&str, TaFn, Vec<f64>, &[ArrayRef]); 6] = [
            ("adx", TaFn::Adx, vec![14.0], high_low_close),
            ("trange", TaFn::Trange, vec![], high_low_close),
            ("atr", TaFn::Atr, vec![5.0], high_low_close),
            ("willr", TaFn::Willr, vec![9.0], high_low_close),
            ("ema", TaFn::Ema, vec![21.0], close),
            ("rsi", TaFn::Rsi, vec![13.0], close),
        ];
        for (name, func, params, inputs) in &cases {
            multi_out_clear();
            let got = evaluate(*func, params, inputs);
            assert_bit_identical(name, &got, &legacy_single(*func, params, inputs));
        }
        let params = [5.0, 3.0, 0.0, 3.0, 0.0];
        for (band, func) in [TaFn::StochSlowk, TaFn::StochSlowd].into_iter().enumerate() {
            multi_out_clear();
            let got = evaluate(func, &params, high_low_close);
            let expected = legacy_band(func, &params, high_low_close, band);
            assert_bit_identical("stoch", &got, &expected);
        }
        multi_out_clear();
    }
}
