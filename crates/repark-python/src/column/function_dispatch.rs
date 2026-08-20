//! Name → DataFusion-expr match tables for [`super::PyColumn`].
//!
//! Extracted from `mod.rs` so `call_scalar` / `aggregate` / `aggregate_binary`
//! `#[pymethods]` stay thin and the file-size EXCEPTIONS row can ratchet DOWN.
//! New function names land as arms here, not in `mod.rs`. Not `#[pymethods]` —
//! PyO3 `multiple-pymethods` stays off.

use std::sync::Arc;

use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion::functions_aggregate::bit_and_or_xor::{bit_and_udaf, bit_or_udaf, bit_xor_udaf};
use datafusion::functions_aggregate::correlation::corr_udaf;
use datafusion::functions_aggregate::covariance::{covar_pop_udaf, covar_samp_udaf};
use datafusion::functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
use datafusion::functions_aggregate::grouping::grouping_udaf;
use datafusion::functions_aggregate::median::median_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::regr::{
    regr_avgx_udaf, regr_avgy_udaf, regr_count_udaf, regr_intercept_udaf, regr_r2_udaf,
    regr_slope_udaf, regr_sxx_udaf, regr_sxy_udaf, regr_syy_udaf,
};
use datafusion::functions_aggregate::stddev::{stddev_pop_udaf, stddev_udaf};
use datafusion::functions_aggregate::string_agg::string_agg_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::functions_aggregate::variance::{var_pop_udaf, var_samp_udaf};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{AggregateUDF, Expr, lit};
use datafusion::scalar::ScalarValue;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::expr_build::reciprocal_trig_or_inf;

/// ===========================================================================================
/// Lower a facade `call_scalar` name + already-built argument [`Expr`]s.
/// ===========================================================================================
///
/// Unknown names and arity mismatches are `ValueError` (same strings as the pre-extract
/// `PyColumn::call_scalar` match). Cardinality refusals stay `AnalysisException` via
/// [`crate::datafusion_to_py_err`].
#[allow(clippy::too_many_lines)] // large match table of expr_fn bindings
#[allow(clippy::needless_pass_by_value)] // owned Vec is the pre-extract table shape
pub(super) fn call_scalar_expr(name: &str, exprs: Vec<Expr>) -> PyResult<Expr> {
    use datafusion::functions::expr_fn;
    use datafusion::functions_nested::expr_fn as nested_fn;
    let need = |n: usize| -> PyResult<()> {
        if exprs.len() != n {
            return Err(PyValueError::new_err(format!(
                "call_scalar({name}) expects {n} args, got {}",
                exprs.len()
            )));
        }
        Ok(())
    };
    let need_at_least = |n: usize| -> PyResult<()> {
        if exprs.len() < n {
            return Err(PyValueError::new_err(format!(
                "call_scalar({name}) expects at least {n} args, got {}",
                exprs.len()
            )));
        }
        Ok(())
    };
    let expr = match name {
        "lower" => {
            need(1)?;
            expr_fn::lower(exprs[0].clone())
        }
        "upper" => {
            need(1)?;
            expr_fn::upper(exprs[0].clone())
        }
        "length" | "character_length" => {
            need(1)?;
            expr_fn::length(exprs[0].clone())
        }
        "trim" | "btrim" => {
            need_at_least(1)?;
            expr_fn::trim(exprs.clone())
        }
        "ltrim" => {
            need_at_least(1)?;
            expr_fn::ltrim(exprs.clone())
        }
        "rtrim" => {
            need_at_least(1)?;
            expr_fn::rtrim(exprs.clone())
        }
        "initcap" => {
            need(1)?;
            expr_fn::initcap(exprs[0].clone())
        }
        "lpad" => {
            need_at_least(2)?;
            expr_fn::lpad(exprs.clone())
        }
        "rpad" => {
            need_at_least(2)?;
            expr_fn::rpad(exprs.clone())
        }
        "instr" | "strpos" => {
            need(2)?;
            expr_fn::instr(exprs[0].clone(), exprs[1].clone())
        }
        "concat_ws" => {
            need_at_least(1)?;
            let delimiter = exprs[0].clone();
            let rest = exprs[1..].to_vec();
            expr_fn::concat_ws(delimiter, rest)
        }
        "regexp_replace" => {
            need_at_least(3)?;
            // Spark `regexp_replace` is always global (all matches). DataFusion defaults
            // to first-match only unless flags include `g` (F2 / Apache test_regexp_replace).
            // Optional 4th arg is an explicit flags Column (advanced); default is `"g"`.
            let flags = if exprs.len() >= 4 {
                Some(exprs[3].clone())
            } else {
                Some(lit("g"))
            };
            expr_fn::regexp_replace(exprs[0].clone(), exprs[1].clone(), exprs[2].clone(), flags)
        }
        "sqrt" => {
            need(1)?;
            expr_fn::sqrt(exprs[0].clone())
        }
        "floor" => {
            need(1)?;
            expr_fn::floor(exprs[0].clone())
        }
        "ceil" | "ceiling" => {
            need(1)?;
            expr_fn::ceil(exprs[0].clone())
        }
        "signum" | "sign" => {
            need(1)?;
            expr_fn::signum(exprs[0].clone())
        }
        "exp" => {
            need(1)?;
            expr_fn::exp(exprs[0].clone())
        }
        "pow" | "power" => {
            need(2)?;
            expr_fn::power(exprs[0].clone(), exprs[1].clone())
        }
        // ---- X1 census: trig / hyperbolic / inverse (DataFusion math expr_fn) ----------
        "cos" => {
            need(1)?;
            expr_fn::cos(exprs[0].clone())
        }
        "sin" => {
            need(1)?;
            expr_fn::sin(exprs[0].clone())
        }
        "tan" => {
            need(1)?;
            expr_fn::tan(exprs[0].clone())
        }
        "cosh" => {
            need(1)?;
            expr_fn::cosh(exprs[0].clone())
        }
        "sinh" => {
            need(1)?;
            expr_fn::sinh(exprs[0].clone())
        }
        "tanh" => {
            need(1)?;
            expr_fn::tanh(exprs[0].clone())
        }
        "acos" => {
            need(1)?;
            expr_fn::acos(exprs[0].clone())
        }
        "asin" => {
            need(1)?;
            expr_fn::asin(exprs[0].clone())
        }
        "atan" => {
            need(1)?;
            expr_fn::atan(exprs[0].clone())
        }
        "atan2" => {
            need(2)?;
            expr_fn::atan2(exprs[0].clone(), exprs[1].clone())
        }
        "acosh" => {
            need(1)?;
            expr_fn::acosh(exprs[0].clone())
        }
        "asinh" => {
            need(1)?;
            expr_fn::asinh(exprs[0].clone())
        }
        "atanh" => {
            need(1)?;
            expr_fn::atanh(exprs[0].clone())
        }
        "cot" => {
            need(1)?;
            // DF cot already yields ±Inf at tan=0; bare 1/tan would hit SparkExprSemantics
            // nullif-zero → NULL (F2 / Apache test_reciprocal_trig_functions).
            expr_fn::cot(exprs[0].clone())
        }
        // Spark sec/csc = 1/cos, 1/sin. Bare `/` is rewritten by SparkExprSemantics to
        // `nullif(divisor, 0)` (non-ANSI div-by-zero → NULL). Live Spark 4.1.2 still
        // returns ±Inf from F.sec/F.csc at exact zeros (csc(0)=Inf) — special-case via
        // CASE so the reciprocal trig surface matches without loosening the global
        // div-by-zero rule (F2 FAIL-VALUE harvest).
        "sec" => {
            need(1)?;
            reciprocal_trig_or_inf(expr_fn::cos(exprs[0].clone()))
        }
        "csc" => {
            need(1)?;
            reciprocal_trig_or_inf(expr_fn::sin(exprs[0].clone()))
        }
        "hypot" => {
            need(2)?;
            let xx = expr_fn::power(exprs[0].clone(), lit(2i64));
            let yy = expr_fn::power(exprs[1].clone(), lit(2i64));
            expr_fn::sqrt(xx + yy)
        }
        // Bitwise (Spark Column.bitwiseAND / | / ^) via DF Operator.
        "bitwise_and" | "bit_and_scalar" => {
            need(2)?;
            datafusion::logical_expr::binary_expr(
                exprs[0].clone(),
                datafusion::logical_expr::Operator::BitwiseAnd,
                exprs[1].clone(),
            )
        }
        "bitwise_or" | "bit_or_scalar" => {
            need(2)?;
            datafusion::logical_expr::binary_expr(
                exprs[0].clone(),
                datafusion::logical_expr::Operator::BitwiseOr,
                exprs[1].clone(),
            )
        }
        "bitwise_xor" | "bit_xor_scalar" => {
            need(2)?;
            datafusion::logical_expr::binary_expr(
                exprs[0].clone(),
                datafusion::logical_expr::Operator::BitwiseXor,
                exprs[1].clone(),
            )
        }
        "is_not_distinct_from" | "eqnullsafe" | "eq_null_safe" => {
            need(2)?;
            datafusion::logical_expr::binary_expr(
                exprs[0].clone(),
                datafusion::logical_expr::Operator::IsNotDistinctFrom,
                exprs[1].clone(),
            )
        }
        "like" => {
            need(2)?;
            exprs[0].clone().like(exprs[1].clone())
        }
        "ilike" => {
            need(2)?;
            exprs[0].clone().ilike(exprs[1].clone())
        }
        "rlike" | "regexp_like" => {
            need(2)?;
            expr_fn::regexp_like(exprs[0].clone(), exprs[1].clone(), None)
        }
        "round" => {
            need_at_least(1)?;
            if exprs.len() == 1 {
                expr_fn::round(vec![exprs[0].clone()])
            } else {
                expr_fn::round(vec![exprs[0].clone(), exprs[1].clone()])
            }
        }
        // Spark `log` is natural log (ln). Keep `log10` as base-10 for callers who ask.
        "log" | "ln" => {
            need(1)?;
            expr_fn::ln(exprs[0].clone())
        }
        "log10" => {
            need(1)?;
            expr_fn::log10(exprs[0].clone())
        }
        "md5" => {
            need(1)?;
            expr_fn::md5(exprs[0].clone())
        }
        "isnan" => {
            need(1)?;
            expr_fn::isnan(exprs[0].clone())
        }
        "nanvl" => {
            need(2)?;
            expr_fn::nanvl(exprs[0].clone(), exprs[1].clone())
        }
        "greatest" => {
            need_at_least(1)?;
            expr_fn::greatest(exprs.clone())
        }
        "least" => {
            need_at_least(1)?;
            expr_fn::least(exprs.clone())
        }
        "current_date" => {
            need(0)?;
            expr_fn::current_date()
        }
        "to_date" => {
            need(1)?;
            repark_functions::expr_fn::to_date(exprs[0].clone())
        }
        "to_timestamp" => {
            need_at_least(1)?;
            repark_functions::expr_fn::to_timestamp(exprs.clone())
        }
        "from_unixtime" => {
            // Spark returns a STRING formatted timestamp, not a timestamp type.
            need(1)?;
            let ts = expr_fn::to_timestamp_seconds(vec![exprs[0].clone()]);
            expr_fn::to_char(ts, lit("%Y-%m-%d %H:%M:%S"))
        }
        // ---- R-FN-BATCH2: strings / collections (engine expr_fn lowerings) ---------------
        "reverse" => {
            need(1)?;
            expr_fn::reverse(exprs[0].clone())
        }
        // === r24 SB1: cardinality ceilings ===
        "repeat" => {
            need(2)?;
            repark_functions::cardinality::refuse_facade_literal_expansion("repeat", &exprs)
                .map_err(crate::datafusion_to_py_err)?;
            expr_fn::repeat(exprs[0].clone(), exprs[1].clone())
        }
        "translate" => {
            need(3)?;
            expr_fn::translate(exprs[0].clone(), exprs[1].clone(), exprs[2].clone())
        }
        "substring_index" | "substr_index" => {
            need(3)?;
            expr_fn::substr_index(exprs[0].clone(), exprs[1].clone(), exprs[2].clone())
        }
        "levenshtein" => {
            need(2)?;
            expr_fn::levenshtein(exprs[0].clone(), exprs[1].clone())
        }
        "ascii" => {
            need(1)?;
            expr_fn::ascii(exprs[0].clone())
        }
        "chr" => {
            need(1)?;
            expr_fn::chr(exprs[0].clone())
        }
        "overlay" => {
            // DF overlay(str, replace, pos [, len]) — 3 or 4 args.
            // Spark default len=-1 means "use length of replace" (same as the
            // 3-arg form). DataFusion treats len=-1 as "replace to end of string",
            // so drop a literal -1 4th arg (F2 octo C1-Q-002 / SQL overlay).
            need_at_least(3)?;
            let mut overlay_args = exprs.clone();
            if overlay_args.len() >= 4 {
                let is_spark_default_len = matches!(
                    &overlay_args[3],
                    Expr::Literal(
                        ScalarValue::Int64(Some(-1))
                            | ScalarValue::Int32(Some(-1))
                            | ScalarValue::Int8(Some(-1))
                            | ScalarValue::Int16(Some(-1)),
                        _
                    )
                );
                if is_spark_default_len {
                    overlay_args.truncate(3);
                }
            }
            expr_fn::overlay(overlay_args)
        }
        "find_in_set" => {
            need(2)?;
            expr_fn::find_in_set(exprs[0].clone(), exprs[1].clone())
        }
        "locate" | "position" => {
            // Spark locate(substr, str) / position(substr IN str) → DF strpos(str, substr).
            need(2)?;
            expr_fn::strpos(exprs[1].clone(), exprs[0].clone())
        }
        "encode" => {
            need(2)?;
            expr_fn::encode(exprs[0].clone(), exprs[1].clone())
        }
        "decode" => {
            need(2)?;
            expr_fn::decode(exprs[0].clone(), exprs[1].clone())
        }
        "base64" => {
            need(1)?;
            expr_fn::encode(exprs[0].clone(), lit("base64"))
        }
        "unbase64" => {
            need(1)?;
            expr_fn::decode(exprs[0].clone(), lit("base64"))
        }
        "size" | "cardinality" => {
            need(1)?;
            nested_fn::cardinality(exprs[0].clone())
        }
        "array_distinct" => {
            need(1)?;
            nested_fn::array_distinct(exprs[0].clone())
        }
        "array_except" => {
            need(2)?;
            nested_fn::array_except(exprs[0].clone(), exprs[1].clone())
        }
        "array_intersect" => {
            need(2)?;
            nested_fn::array_intersect(exprs[0].clone(), exprs[1].clone())
        }
        "array_union" => {
            need(2)?;
            nested_fn::array_union(exprs[0].clone(), exprs[1].clone())
        }
        "array_join" | "array_to_string" => {
            // Spark array_join(arr, delim [, null_rep]); DF array_to_string is 2-arg.
            need_at_least(2)?;
            nested_fn::array_to_string(exprs[0].clone(), exprs[1].clone())
        }
        "array_max" => {
            need(1)?;
            nested_fn::array_max(exprs[0].clone())
        }
        "array_min" => {
            need(1)?;
            nested_fn::array_min(exprs[0].clone())
        }
        "array_position" => {
            // DF array_position(array, element, index) — index defaults to 1 (Spark).
            need_at_least(2)?;
            let index = if exprs.len() >= 3 {
                exprs[2].clone()
            } else {
                lit(1i64)
            };
            nested_fn::array_position(exprs[0].clone(), exprs[1].clone(), index)
        }
        // r21 T7 census-r6: Spark array_contains → DF array_has
        "array_contains" | "array_has" => {
            need(2)?;
            nested_fn::array_has(exprs[0].clone(), exprs[1].clone())
        }
        "array_remove" => {
            need(2)?;
            nested_fn::array_remove(exprs[0].clone(), exprs[1].clone())
        }
        // === r24 SB1: cardinality ceilings ===
        "array_repeat" => {
            need(2)?;
            repark_functions::cardinality::refuse_facade_literal_expansion("array_repeat", &exprs)
                .map_err(crate::datafusion_to_py_err)?;
            nested_fn::array_repeat(exprs[0].clone(), exprs[1].clone())
        }
        "array_sort" | "sort_array" => {
            // DF array_sort(array [, order: 'ASC'|'DESC' [, nulls: 'NULLS FIRST'|…]]).
            // Spark sort_array(arr [, asc: bool]). Default ascending uses 1-arg form.
            need_at_least(1)?;
            if exprs.len() == 1 {
                nested_fn::array_sort(exprs[0].clone(), lit("ASC"), lit("NULLS FIRST"))
            } else if exprs.len() == 2 {
                // Second arg from Python may already be ASC/DESC lit.
                nested_fn::array_sort(exprs[0].clone(), exprs[1].clone(), lit("NULLS FIRST"))
            } else {
                nested_fn::array_sort(exprs[0].clone(), exprs[1].clone(), exprs[2].clone())
            }
        }
        "array_slice" => {
            // DF array_slice(arr, begin, end) — end inclusive 1-indexed.
            need_at_least(3)?;
            nested_fn::array_slice(exprs[0].clone(), exprs[1].clone(), exprs[2].clone(), None)
        }
        "slice" => {
            // Spark slice(arr, start, length) → DF array_slice(arr, start, start+length-1).
            need(3)?;
            let end = exprs[1].clone() + exprs[2].clone() - lit(1i64);
            nested_fn::array_slice(exprs[0].clone(), exprs[1].clone(), end, None)
        }
        "flatten" => {
            need(1)?;
            nested_fn::flatten(exprs[0].clone())
        }
        "map_keys" => {
            need(1)?;
            nested_fn::map_keys(exprs[0].clone())
        }
        "map_values" => {
            need(1)?;
            nested_fn::map_values(exprs[0].clone())
        }
        "map_entries" => {
            need(1)?;
            nested_fn::map_entries(exprs[0].clone())
        }
        // === r24 SB1: cardinality ceilings ===
        "sequence" | "generate_series" | "gen_series" => {
            // Spark sequence(start, stop [, step]); DF gen_series(start, stop, step).
            need_at_least(2)?;
            let step = if exprs.len() >= 3 {
                exprs[2].clone()
            } else {
                lit(1i64)
            };
            let check_args = vec![exprs[0].clone(), exprs[1].clone(), step.clone()];
            repark_functions::cardinality::refuse_facade_literal_expansion("sequence", &check_args)
                .map_err(crate::datafusion_to_py_err)?;
            nested_fn::gen_series(exprs[0].clone(), exprs[1].clone(), step)
        }
        "elt" => {
            // Spark elt(n, e1, e2, ...) — 1-indexed pick; DF array_element is 0-indexed.
            need_at_least(2)?;
            let index = exprs[0].clone() - lit(1i64);
            let elements = nested_fn::make_array(exprs[1..].to_vec());
            nested_fn::array_element(elements, index)
        }
        // Spark ``Column[i]`` (0-based) via owned ``__repark_array_get__`` (octo C1-L-001).
        // Not DF's 1-based ``array_element`` — avoids array_slice 1-element-list residual.
        "array_element" => {
            need(2)?;
            Expr::ScalarFunction(ScalarFunction::new_udf(
                repark_functions::collection::spark_array_get_udf(),
                vec![exprs[0].clone(), exprs[1].clone()],
            ))
        }
        // Polymorphic Spark GetItem: array 0-based **or** map-by-key (octo C2-L-001).
        // Used for Column / non-int/non-str keys so getitem never fails open to parent.
        "getitem" => {
            need(2)?;
            Expr::ScalarFunction(ScalarFunction::new_udf(
                repark_functions::collection::spark_get_item_udf(),
                vec![exprs[0].clone(), exprs[1].clone()],
            ))
        }
        // Struct field extract for ``Column['field']`` (octo C1-L-002); free-SQL still
        // quotes the ident on the Python side (octo C1-SEC-001).
        "get_field" => {
            need(2)?;
            datafusion::functions::core::get_field().call(exprs.clone())
        }
        // Spark ``array(e1, e2, …)`` / ``lit([…])`` — zero-arg is empty array.
        "array" | "make_array" => nested_fn::make_array(exprs.clone()),
        // ---- R-FN-BATCH3: datetime / extract lowerings --------------------------------
        "next_day" => {
            need(2)?;
            repark_functions::expr_fn::next_day(exprs[0].clone(), exprs[1].clone())
        }
        "hour" => {
            need(1)?;
            repark_functions::expr_fn::hour(exprs[0].clone())
        }
        "minute" => {
            need(1)?;
            repark_functions::expr_fn::minute(exprs[0].clone())
        }
        "second" => {
            need(1)?;
            repark_functions::expr_fn::second(exprs[0].clone())
        }
        // The facade's `F.unix_date` builds the ENGINE's function, not a hand-rolled
        // `CAST(x AS DATE) AS INT` chain — that pair is refused at analysis (G6-3), and
        // the UDF's own `simplify` re-creates it in the optimizer where it is legal.
        "unix_date" => {
            need(1)?;
            repark_functions::expr_fn::unix_date(exprs[0].clone())
        }
        "date_part" | "datepart" => {
            need(2)?;
            // Spark date_part(field, source); DF same order.
            expr_fn::date_part(exprs[0].clone(), exprs[1].clone())
        }
        "timestamp_seconds" | "to_timestamp_seconds" => {
            need(1)?;
            expr_fn::to_timestamp_seconds(vec![exprs[0].clone()])
        }
        "timestamp_millis" | "to_timestamp_millis" => {
            need(1)?;
            expr_fn::to_timestamp_millis(vec![exprs[0].clone()])
        }
        "timestamp_micros" | "to_timestamp_micros" => {
            need(1)?;
            expr_fn::to_timestamp_micros(vec![exprs[0].clone()])
        }
        "sha256" => {
            need(1)?;
            expr_fn::sha256(exprs[0].clone())
        }
        // r20 G2: Spark XORShift `rand`/`randn`/`random` (seeded; overwrites DF random).
        "random" | "rand" => {
            if exprs.len() > 1 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects 0 or 1 seed arg, got {}",
                    exprs.len()
                )));
            }
            repark_functions::random::spark_rand_udf().call(exprs.clone())
        }
        "randn" => {
            if exprs.len() > 1 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar(randn) expects 0 or 1 seed arg, got {}",
                    exprs.len()
                )));
            }
            repark_functions::random::spark_randn_udf().call(exprs.clone())
        }
        // R-POLARS-NS: string predicates / substr (plan path, no SQL string concat)
        "starts_with" => {
            need(2)?;
            expr_fn::starts_with(exprs[0].clone(), exprs[1].clone())
        }
        "ends_with" => {
            need(2)?;
            expr_fn::ends_with(exprs[0].clone(), exprs[1].clone())
        }
        "contains" => {
            need(2)?;
            expr_fn::contains(exprs[0].clone(), exprs[1].clone())
        }
        "substr" | "substring" => {
            // Embed owned Spark `substring` UDF (pos 0 ≡ 1, negative from end) —
            // never DF built-in `expr_fn::substr`/`substring`. Analyzer rewrites
            // only planner-embedded name=="substr"; call_scalar 3-arg used to
            // embed DF `substring` (name ≠ "substr") so Column.__getitem__ slices
            // and F.substr bypassed the shim (octo C7-L-001: 'hello'[0:3] → 'he').
            need_at_least(2)?;
            if exprs.len() > 3 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects 2 or 3 args, got {}",
                    exprs.len()
                )));
            }
            let args = if exprs.len() == 2 {
                vec![exprs[0].clone(), exprs[1].clone()]
            } else {
                vec![exprs[0].clone(), exprs[1].clone(), exprs[2].clone()]
            };
            Expr::ScalarFunction(ScalarFunction::new_udf(
                repark_functions::string::substring_udf(),
                args,
            ))
        }
        // ---- FN-GT1: leftover THIN-WIRE math / string / bitwise / utf8 ----------------------
        "bin" => {
            need(1)?;
            repark_functions::expr_fn::bin(exprs[0].clone())
        }
        "hex" => {
            need(1)?;
            repark_functions::expr_fn::hex(exprs[0].clone())
        }
        "unhex" => {
            need(1)?;
            repark_functions::expr_fn::unhex(exprs[0].clone())
        }
        "factorial" => {
            need(1)?;
            repark_functions::expr_fn::factorial(exprs[0].clone())
        }
        "rint" => {
            need(1)?;
            repark_functions::expr_fn::rint(exprs[0].clone())
        }
        "width_bucket" => {
            need(4)?;
            repark_functions::expr_fn::width_bucket(
                exprs[0].clone(),
                exprs[1].clone(),
                exprs[2].clone(),
                exprs[3].clone(),
            )
        }
        "bit_count" => {
            need(1)?;
            repark_functions::expr_fn::bit_count(exprs[0].clone())
        }
        "bit_get" | "getbit" => {
            need(2)?;
            repark_functions::expr_fn::bit_get(exprs[0].clone(), exprs[1].clone())
        }
        "shiftleft" => {
            need(2)?;
            repark_functions::expr_fn::shiftleft(exprs[0].clone(), exprs[1].clone())
        }
        "shiftright" => {
            need(2)?;
            repark_functions::expr_fn::shiftright(exprs[0].clone(), exprs[1].clone())
        }
        "shiftrightunsigned" => {
            need(2)?;
            repark_functions::expr_fn::shiftrightunsigned(exprs[0].clone(), exprs[1].clone())
        }
        "split_part" => {
            need(3)?;
            repark_functions::expr_fn::split_part(
                exprs[0].clone(),
                exprs[1].clone(),
                exprs[2].clone(),
            )
        }
        "regexp_count" => {
            // One semantics source: spark_regexp.rs (NULL-in NULL-out, int32).
            need(2)?;
            repark_functions::expr_fn::regexp_count(exprs[0].clone(), exprs[1].clone())
        }
        "regexp_instr" => {
            // One semantics source: spark_regexp.rs. 3rd arg is Spark idx
            // (NULL-propagate, value ignored) — never DataFusion start-position.
            need_at_least(2)?;
            if exprs.len() > 3 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects 2 or 3 args, got {}",
                    exprs.len()
                )));
            }
            repark_functions::expr_fn::regexp_instr(exprs.clone())
        }
        "bit_length" => {
            need(1)?;
            repark_functions::expr_fn::bit_length(exprs[0].clone())
        }
        "octet_length" => {
            need(1)?;
            repark_functions::expr_fn::octet_length(exprs[0].clone())
        }
        "is_valid_utf8" => {
            need(1)?;
            repark_functions::expr_fn::is_valid_utf8(exprs[0].clone())
        }
        "make_valid_utf8" => {
            need(1)?;
            repark_functions::expr_fn::make_valid_utf8(exprs[0].clone())
        }
        // ---- FN-GT2: leftover THIN-WIRE datetime / collections / url / bitmap ---------------
        "make_date" => {
            need(3)?;
            repark_functions::expr_fn::make_date(
                exprs[0].clone(),
                exprs[1].clone(),
                exprs[2].clone(),
            )
        }
        "make_interval" => {
            if exprs.len() > 7 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects at most 7 args, got {}",
                    exprs.len()
                )));
            }
            repark_functions::expr_fn::make_interval(exprs.clone())
        }
        "make_dt_interval" => {
            if exprs.len() > 4 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects at most 4 args, got {}",
                    exprs.len()
                )));
            }
            repark_functions::expr_fn::make_dt_interval(exprs.clone())
        }
        "unix_micros" => {
            need(1)?;
            repark_functions::expr_fn::unix_micros(exprs[0].clone())
        }
        // ---- FNP-3: names the SQL door already resolved; the facade had no arm ---------
        "crc32" => {
            need(1)?;
            repark_functions::expr_fn::crc32(exprs[0].clone())
        }
        "sha1" | "sha" => {
            need(1)?;
            repark_functions::expr_fn::sha1(exprs[0].clone())
        }
        "xxhash64" => {
            need_at_least(1)?;
            repark_functions::expr_fn::xxhash64(exprs.clone())
        }
        "regexp_extract_all" => {
            need_at_least(2)?;
            repark_functions::expr_fn::regexp_extract_all(exprs.clone())
        }
        "regexp_substr" => {
            need(2)?;
            repark_functions::expr_fn::regexp_substr(exprs[0].clone(), exprs[1].clone())
        }
        "soundex" => {
            need(1)?;
            repark_functions::expr_fn::soundex(exprs[0].clone())
        }
        "format_string" => {
            need_at_least(1)?;
            repark_functions::expr_fn::format_string(exprs.clone())
        }
        "from_utc_timestamp" => {
            need(2)?;
            repark_functions::expr_fn::from_utc_timestamp(exprs[0].clone(), exprs[1].clone())
        }
        "to_utc_timestamp" => {
            need(2)?;
            repark_functions::expr_fn::to_utc_timestamp(exprs[0].clone(), exprs[1].clone())
        }
        "map_from_arrays" => {
            need(2)?;
            repark_functions::expr_fn::map_from_arrays(exprs[0].clone(), exprs[1].clone())
        }
        // Spark's older spelling of `date_diff`; PySpark 4.1.2 defines both with the same
        // (end, start) order over the same Catalyst expression, so they share one arm.
        "date_diff" | "datediff" => {
            need(2)?;
            repark_functions::expr_fn::date_diff(exprs[0].clone(), exprs[1].clone())
        }
        "element_at" => {
            need(2)?;
            repark_functions::expr_fn::element_at(exprs[0].clone(), exprs[1].clone())
        }
        "array_compact" => {
            need(1)?;
            nested_fn::array_compact(exprs[0].clone())
        }
        // X2: Spark 4.0 `shuffle(array, seed)`. The facade used to drop the seed; the SQL door
        // already had it, so the two doors disagreed on a *deterministic* result.
        "shuffle" => {
            need_at_least(1)?;
            if exprs.len() > 2 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects 1 or 2 args, got {}",
                    exprs.len()
                )));
            }
            repark_functions::expr_fn::shuffle(exprs.clone())
        }
        "map_from_entries" => {
            need(1)?;
            repark_functions::expr_fn::map_from_entries(exprs[0].clone())
        }
        "str_to_map" => {
            need_at_least(1)?;
            if exprs.len() > 3 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects 1 to 3 args, got {}",
                    exprs.len()
                )));
            }
            let pair_delim = exprs.get(1).cloned().unwrap_or_else(|| lit(","));
            let key_value_delim = exprs.get(2).cloned().unwrap_or_else(|| lit(":"));
            repark_functions::expr_fn::str_to_map(exprs[0].clone(), pair_delim, key_value_delim)
        }
        "parse_url" => {
            need_at_least(2)?;
            if exprs.len() > 3 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects 2 or 3 args, got {}",
                    exprs.len()
                )));
            }
            repark_functions::expr_fn::parse_url(exprs.clone())
        }
        "try_parse_url" => {
            need_at_least(2)?;
            if exprs.len() > 3 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects 2 or 3 args, got {}",
                    exprs.len()
                )));
            }
            repark_functions::expr_fn::try_parse_url(exprs.clone())
        }
        "url_decode" => {
            need(1)?;
            repark_functions::expr_fn::url_decode(exprs[0].clone())
        }
        "url_encode" => {
            need(1)?;
            repark_functions::expr_fn::url_encode(exprs[0].clone())
        }
        "try_url_decode" => {
            need(1)?;
            repark_functions::expr_fn::try_url_decode(exprs[0].clone())
        }
        "bitmap_bit_position" => {
            need(1)?;
            repark_functions::expr_fn::bitmap_bit_position(exprs[0].clone())
        }
        "bitmap_bucket_number" => {
            need(1)?;
            repark_functions::expr_fn::bitmap_bucket_number(exprs[0].clone())
        }
        "bitmap_count" => {
            need(1)?;
            repark_functions::expr_fn::bitmap_count(exprs[0].clone())
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "call_scalar: unsupported function {other:?}"
            )));
        }
    };
    Ok(expr)
}

/// ===========================================================================================
/// Unary aggregate UDAF for [`super::PyColumn::aggregate`] (`collect_list`/`set` stay in
/// the pymethod — they are not this match table).
/// ===========================================================================================
pub(super) fn unary_aggregate_udaf(kind: &str) -> PyResult<Arc<AggregateUDF>> {
    let udaf = match kind {
        "sum" => sum_udaf(),
        "avg" => repark_functions::aggregate::avg_udaf(),
        "min" => min_udaf(),
        "max" => max_udaf(),
        "first" => first_value_udaf(),
        "last" => last_value_udaf(),
        // R-FN-BATCH4 unary stats / bits
        "stddev" | "stddev_samp" => stddev_udaf(),
        "stddev_pop" => stddev_pop_udaf(),
        "variance" | "var_samp" => var_samp_udaf(),
        "var_pop" => var_pop_udaf(),
        "median" => median_udaf(),
        "bit_and" => bit_and_udaf(),
        "bit_or" => bit_or_udaf(),
        "bit_xor" => bit_xor_udaf(),
        // FNP-5: already in `all_default_aggregate_functions()`, so the SQL door resolved these
        // and only the facade had no arm. `approx_count_distinct` is Spark's spelling of
        // DataFusion's `approx_distinct` — see the facade wrapper for the HLL/HLL++ divergence.
        "approx_count_distinct" | "approx_distinct" => approx_distinct_udaf(),
        "grouping" => grouping_udaf(),
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown aggregate function {other:?}"
            )));
        }
    };
    Ok(udaf)
}

/// ===========================================================================================
/// Binary aggregate UDAF for [`super::PyColumn::aggregate_binary`].
/// ===========================================================================================
pub(super) fn binary_aggregate_udaf(kind: &str) -> PyResult<Arc<AggregateUDF>> {
    let udaf = match kind {
        "corr" => corr_udaf(),
        "covar_pop" => covar_pop_udaf(),
        "covar_samp" | "covar" => covar_samp_udaf(),
        // FNP-5: the nine linear-regression aggregates. All are in
        // `all_default_aggregate_functions()`, so `register_all` already put them on every
        // session and the SQL door resolved them — only the facade had no arm.
        "regr_avgx" => regr_avgx_udaf(),
        "regr_avgy" => regr_avgy_udaf(),
        "regr_count" => regr_count_udaf(),
        "regr_intercept" => regr_intercept_udaf(),
        "regr_r2" => regr_r2_udaf(),
        "regr_slope" => regr_slope_udaf(),
        "regr_sxx" => regr_sxx_udaf(),
        "regr_sxy" => regr_sxy_udaf(),
        "regr_syy" => regr_syy_udaf(),
        // Spark `listagg` is `string_agg` under its Spark spelling (both take a delimiter).
        "string_agg" | "listagg" => string_agg_udaf(),
        other_kind => {
            return Err(PyValueError::new_err(format!(
                "unknown binary aggregate {other_kind:?}"
            )));
        }
    };
    Ok(udaf)
}
