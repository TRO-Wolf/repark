#![cfg(feature = "datafusion")]

use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, ArrayRef, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::function::PartitionEvaluatorArgs;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use repark_ta::udf::window_udfs;

const PREFIX_RUNS: [(&str, usize); 6] = [
    ("open", 0),
    ("high", 3),
    ("low", 7),
    ("close", 5),
    ("periods", 0),
    ("volume", 2),
];

const PREFIX_SERIES: usize = 13;

fn goldens_dir() -> PathBuf {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").into());
    PathBuf::from(manifest_dir).join("tests").join("goldens")
}

static PREFIX_MANIFEST: LazyLock<serde_json::Value> = LazyLock::new(|| {
    let path = goldens_dir().join("prefix").join("manifest.json");
    let text =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("missing {}: {e}", path.display()));
    serde_json::from_str(&text).expect("prefix manifest.json parses")
});

fn read_bits(path: &PathBuf) -> Vec<f64> {
    let bytes = fs::read(path).unwrap_or_else(|e| panic!("missing {}: {e}", path.display()));
    bytes
        .chunks_exact(8)
        .map(|chunk| {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(chunk);
            f64::from_le_bytes(buf)
        })
        .collect()
}

fn prefix_golden(name: &str) -> Vec<f64> {
    let series = PREFIX_MANIFEST["series"]
        .as_object()
        .expect("prefix manifest series map");
    assert_eq!(series.len(), PREFIX_SERIES, "prefix manifest series count");
    let rows = series[name]
        .as_u64()
        .unwrap_or_else(|| panic!("{name} missing from prefix manifest.json"));
    let values = read_bits(&goldens_dir().join("prefix").join(format!("{name}.bin")));
    assert_eq!(
        u64::try_from(values.len()).expect("row count fits u64"),
        rows,
        "{name}: size disagrees with prefix manifest.json"
    );
    values
}

fn input(name: &str) -> Vec<f64> {
    if name.starts_with("prefix_") {
        return prefix_golden(name);
    }
    let run = PREFIX_RUNS
        .iter()
        .find(|(column, _)| *column == name)
        .map_or_else(
            || panic!("{name} is not a prefix fixture column"),
            |(_, run)| *run,
        );
    let mut values = read_bits(&goldens_dir().join(format!("fixture_{name}.bin")));
    values[..run].fill(f64::NAN);
    values
}

fn as_array(values: &[f64], as_null: bool) -> ArrayRef {
    let run = values.iter().take_while(|value| value.is_nan()).count();
    if as_null {
        let nullable: Vec<Option<f64>> = values
            .iter()
            .enumerate()
            .map(|(index, value)| (index >= run).then_some(*value))
            .collect();
        let array = Float64Array::from(nullable);
        assert_eq!(array.null_count(), run);
        Arc::new(array)
    } else {
        Arc::new(Float64Array::from(values.to_vec()))
    }
}

fn evaluate(udf_name: &str, series: &[Vec<f64>], params: &[f64], as_null: bool) -> Vec<f64> {
    let udf = window_udfs()
        .into_iter()
        .find(|udf| udf.name() == udf_name)
        .unwrap_or_else(|| panic!("{udf_name} is not registered"));
    let mut exprs: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
    let mut fields: Vec<FieldRef> = Vec::new();
    for index in 0..series.len() {
        let name = format!("series_{index}");
        exprs.push(Arc::new(Column::new(&name, index)));
        fields.push(Arc::new(Field::new(name, DataType::Float64, true)));
    }
    for (index, param) in params.iter().enumerate() {
        exprs.push(Arc::new(Literal::new(ScalarValue::Float64(Some(*param)))));
        fields.push(Arc::new(Field::new(
            format!("param_{index}"),
            DataType::Float64,
            false,
        )));
    }
    let mut evaluator = udf
        .partition_evaluator_factory(PartitionEvaluatorArgs::new(&exprs, &fields, false, false))
        .expect("partition evaluator");
    let arrays: Vec<ArrayRef> = series
        .iter()
        .map(|values| as_array(values, as_null))
        .collect();
    let rows = series[0].len();
    let out = evaluator.evaluate_all(&arrays, rows).expect("evaluate_all");
    let floats = out
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array output");
    (0..floats.len())
        .map(|index| {
            if floats.is_null(index) {
                f64::NAN
            } else {
                floats.value(index)
            }
        })
        .collect()
}

fn check(golden: &str, udf_name: &str, inputs: &[&str], params: &[f64], as_null: bool) {
    let series: Vec<Vec<f64>> = inputs.iter().map(|name| input(name)).collect();
    let ours = evaluate(udf_name, &series, params, as_null);
    let expected = prefix_golden(golden);
    assert_eq!(ours.len(), expected.len(), "{golden}: length mismatch");
    for (row, (a, b)) in ours.iter().zip(&expected).enumerate() {
        if a.is_nan() && b.is_nan() {
            continue;
        }
        assert!(
            a.to_bits() == b.to_bits(),
            "{golden}: bit mismatch at row {row}: ours {a:?} ({:#018x}) vs polars_talib {b:?} ({:#018x})",
            a.to_bits(),
            b.to_bits()
        );
    }
}

macro_rules! prefix_golden_twins {
    ($nan:ident, $null:ident, $golden:literal, $udf:literal, [$($input:literal),*], [$($param:expr),*]) => {
        #[test]
        fn $nan() {
            check($golden, $udf, &[$($input),*], &[$($param),*], false);
        }

        #[test]
        fn $null() {
            check($golden, $udf, &[$($input),*], &[$($param),*], true);
        }
    };
}

prefix_golden_twins!(
    ema_21_nan_run,
    ema_21_null_run,
    "prefix_ema_21",
    "ta_ema",
    ["close"],
    [21.0]
);
prefix_golden_twins!(
    sma_10_nan_run,
    sma_10_null_run,
    "prefix_sma_10",
    "ta_sma",
    ["close"],
    [10.0]
);
prefix_golden_twins!(
    rsi_14_nan_run,
    rsi_14_null_run,
    "prefix_rsi_14",
    "ta_rsi",
    ["close"],
    [14.0]
);
prefix_golden_twins!(
    adx_14_nan_run,
    adx_14_null_run,
    "prefix_adx_14",
    "ta_adx",
    ["high", "low", "close"],
    [14.0]
);
prefix_golden_twins!(
    trange_nan_run,
    trange_null_run,
    "prefix_trange",
    "ta_trange",
    ["high", "low", "close"],
    []
);
prefix_golden_twins!(
    atr_14_nan_run,
    atr_14_null_run,
    "prefix_atr_14",
    "ta_atr",
    ["high", "low", "close"],
    [14.0]
);
prefix_golden_twins!(
    bbands_upper_20_nan_run,
    bbands_upper_20_null_run,
    "prefix_bbands_upper_20",
    "ta_bbands_upper",
    ["close"],
    [20.0, 2.0, 2.0]
);
prefix_golden_twins!(
    macd_12_26_9_nan_run,
    macd_12_26_9_null_run,
    "prefix_macd_12_26_9",
    "ta_macd",
    ["close"],
    [12.0, 26.0, 9.0]
);
prefix_golden_twins!(
    stoch_slowk_nan_run,
    stoch_slowk_null_run,
    "prefix_stoch_slowk",
    "ta_stoch_slowk",
    ["high", "low", "close"],
    [5.0, 3.0, 0.0, 3.0, 0.0]
);
prefix_golden_twins!(
    obv_nan_run,
    obv_null_run,
    "prefix_obv",
    "ta_obv",
    ["close", "volume"],
    []
);
prefix_golden_twins!(
    linearreg_5_nan_run,
    linearreg_5_null_run,
    "prefix_linearreg_5",
    "ta_linearreg",
    ["close"],
    [5.0]
);
prefix_golden_twins!(
    mavp_sma_nan_run,
    mavp_sma_null_run,
    "prefix_mavp_sma",
    "ta_mavp",
    ["close", "periods"],
    [5.0, 20.0, 0.0]
);
prefix_golden_twins!(
    chain_ema21_of_trange_nan_run,
    chain_ema21_of_trange_null_run,
    "prefix_chain_ema21_of_trange",
    "ta_ema",
    ["prefix_trange"],
    [21.0]
);
