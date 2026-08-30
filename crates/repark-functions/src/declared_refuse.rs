//! Parse-altitude refusals for Spark functions this engine will not build.

use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{Expr, ObjectName, Statement, Visit, Visitor};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;

const FNP15: &[(&str, &str)] = &[
    (
        "java_method",
        "java_method is unreachable: it loads a Java class by name and invokes a static \
         method by reflection, which needs a live JVM. repark has no JVM. See \
         docs/spark-sql-iceberg-parity.md (FNP-15 java_method).",
    ),
    (
        "reflect",
        "reflect is unreachable: it is Spark's CallMethodViaReflection spelling of \
         java_method, which needs a live JVM. repark has no JVM. See \
         docs/spark-sql-iceberg-parity.md (FNP-15 reflect).",
    ),
    (
        "try_reflect",
        "try_reflect is unreachable: it is reflect with exception-to-NULL, and still \
         needs a live JVM. repark has no JVM. See docs/spark-sql-iceberg-parity.md \
         (FNP-15 try_reflect).",
    ),
    (
        "unwrap_udt",
        "unwrap_udt is unreachable: Spark UserDefinedType unwrap walks the JVM UDT \
         registry; with no JVM there is no UDT system to unwrap from. See \
         docs/spark-sql-iceberg-parity.md (FNP-15 unwrap_udt).",
    ),
    (
        "input_file_block_start",
        "input_file_block_start is unreachable: it reads Spark's InputFileBlockHolder \
         thread-local, populated by HadoopRDD/FileScanRDD as a split is handed to a \
         task. DataFusion has no equivalent surface, and repark's input_file_name is \
         itself still a stub. See docs/spark-sql-iceberg-parity.md \
         (FNP-15 input_file_block_start).",
    ),
    (
        "input_file_block_length",
        "input_file_block_length is unreachable: it reads Spark's InputFileBlockHolder \
         thread-local, populated by HadoopRDD/FileScanRDD as a split is handed to a \
         task. DataFusion has no equivalent surface, and repark's input_file_name is \
         itself still a stub. See docs/spark-sql-iceberg-parity.md \
         (FNP-15 input_file_block_length).",
    ),
];

const SKETCHES: &[&str] = &[
    "hll_sketch_agg",
    "hll_sketch_estimate",
    "hll_union",
    "hll_union_agg",
    "kll_merge_agg_bigint",
    "kll_merge_agg_double",
    "kll_merge_agg_float",
    "kll_sketch_agg_bigint",
    "kll_sketch_agg_double",
    "kll_sketch_agg_float",
    "kll_sketch_get_n_bigint",
    "kll_sketch_get_n_double",
    "kll_sketch_get_n_float",
    "kll_sketch_get_quantile_bigint",
    "kll_sketch_get_quantile_double",
    "kll_sketch_get_quantile_float",
    "kll_sketch_get_rank_bigint",
    "kll_sketch_get_rank_double",
    "kll_sketch_get_rank_float",
    "kll_sketch_merge_bigint",
    "kll_sketch_merge_double",
    "kll_sketch_merge_float",
    "kll_sketch_to_string_bigint",
    "kll_sketch_to_string_double",
    "kll_sketch_to_string_float",
    "theta_difference",
    "theta_intersection",
    "theta_intersection_agg",
    "theta_sketch_agg",
    "theta_sketch_estimate",
    "theta_union",
    "theta_union_agg",
];

const SKETCH_REASON: &str = "is reachable without a JVM and is deferred by cost: Spark sketch \
     columns are Apache DataSketches binary blobs, and DataFusion's hyperloglog.rs is a \
     different format that cannot serve the blob. See docs/spark-sql-iceberg-parity.md \
     (FNP-16 sketches).";

const CSV_XML_XPATH: &[&str] = &[
    "to_csv",
    "to_xml",
    "xpath",
    "xpath_boolean",
    "xpath_double",
    "xpath_float",
    "xpath_int",
    "xpath_long",
    "xpath_number",
    "xpath_short",
    "xpath_string",
];

const CSV_XML_XPATH_REASON: &str = "is reachable without a JVM and is deferred by cost: the \
     xpath family needs an XPath 1.0 engine matching javax.xml.xpath, and datafusion-spark's \
     csv and xml modules are empty. See docs/spark-sql-iceberg-parity.md (FNP-16 CSV/XML/XPath).";

const VARIANT: &[&str] = &[
    "is_variant_null",
    "parse_json",
    "schema_of_variant",
    "schema_of_variant_agg",
    "to_variant_object",
    "try_parse_json",
    "try_variant_get",
    "variant_get",
];

const VARIANT_REASON: &str = "is reachable without a JVM and is deferred by cost: Spark VARIANT \
     is a specific value/metadata binary encoding; repark's VariantType is a shell with \
     nothing behind it. See docs/spark-sql-iceberg-parity.md (FNP-16 VARIANT).";

/// Message for a declared-absent Spark function, if this unit currently owns the name.
#[must_use]
pub fn refusal_message(name: &str) -> Option<String> {
    let key = name.to_ascii_lowercase();
    if let Some((_, message)) = FNP15.iter().find(|(owned, _)| *owned == key) {
        return Some((*message).to_string());
    }
    if SKETCHES.binary_search(&key.as_str()).is_ok() {
        return Some(format!("{key} {SKETCH_REASON}"));
    }
    if CSV_XML_XPATH.binary_search(&key.as_str()).is_ok() {
        return Some(format!("{key} {CSV_XML_XPATH_REASON}"));
    }
    if VARIANT.binary_search(&key.as_str()).is_ok() {
        return Some(format!("{key} {VARIANT_REASON}"));
    }
    None
}

/// Names this unit currently refuses at parse altitude.
#[must_use]
pub fn armed_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = FNP15.iter().map(|(name, _)| *name).collect();
    names.extend(SKETCHES.iter().copied());
    names.extend(CSV_XML_XPATH.iter().copied());
    names.extend(VARIANT.iter().copied());
    names
}

/// Refuse a declared-absent function on a parsed statement.
/// # Errors
/// [`DataFusionError::NotImplemented`] naming the registry reason.
pub fn refuse_in_statement(statement: &Statement) -> Result<()> {
    let mut probe = FunctionProbe { message: None };
    if statement.visit(&mut probe).is_break()
        && let Some(message) = probe.message
    {
        return Err(DataFusionError::NotImplemented(message));
    }
    Ok(())
}

/// Refuse a declared-absent function in a SQL string or expression fragment.
/// # Errors
/// [`DataFusionError::NotImplemented`] when an armed name is present.
pub fn refuse_in_sql(sql: &str) -> Result<()> {
    if let Ok(statements) = Parser::parse_sql(&DatabricksDialect {}, sql) {
        return refuse_parsed(&statements);
    }
    let wrapped = format!("SELECT ({sql})");
    if let Ok(statements) = Parser::parse_sql(&DatabricksDialect {}, &wrapped) {
        return refuse_parsed(&statements);
    }
    Ok(())
}

fn refuse_parsed(statements: &[Statement]) -> Result<()> {
    for statement in statements {
        refuse_in_statement(statement)?;
    }
    Ok(())
}

struct FunctionProbe {
    message: Option<String>,
}

impl Visitor for FunctionProbe {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        let name = object_name_last(&function.name);
        let Some(message) = refusal_message(&name) else {
            return ControlFlow::Continue(());
        };
        self.message = Some(message);
        ControlFlow::Break(())
    }
}

fn object_name_last(name: &ObjectName) -> String {
    name.0.last().and_then(|part| part.as_ident()).map_or_else(
        || name.to_string().to_ascii_lowercase(),
        |ident| ident.value.to_ascii_lowercase(),
    )
}

#[cfg(test)]
mod tests {
    // pins: fnp-15-16/C-001, C-002, C-008, C-009, C-010, C-017
    use super::*;

    #[test]
    fn fnp15_six_are_unreachable_and_armed() {
        assert_eq!(FNP15.len(), 6);
        for (name, _) in FNP15 {
            let message = refusal_message(name).expect("FNP-15 name has a message");
            assert!(message.contains("unreachable"), "{name}: {message}");
            assert!(!message.contains("deferred by cost"), "{name}: {message}");
        }
    }

    #[test]
    fn sketches_are_deferred_by_cost_and_sorted() {
        assert!(SKETCHES.is_sorted());
        assert_eq!(SKETCHES.len(), 32);
        for name in SKETCHES {
            let message = refusal_message(name).expect("sketch name has a message");
            assert!(message.contains("deferred by cost"), "{name}: {message}");
            assert!(
                message.contains("reachable without a JVM"),
                "{name}: {message}"
            );
            assert!(!message.contains("unreachable"), "{name}: {message}");
            let error = refuse_in_sql(&format!("SELECT {name}(1)")).expect_err(*name);
            assert!(matches!(error, DataFusionError::NotImplemented(_)));
            assert!(error.to_string().contains("deferred by cost"));
        }
    }

    #[test]
    fn csv_xml_xpath_are_deferred_by_cost_and_sorted() {
        assert!(CSV_XML_XPATH.is_sorted());
        assert_eq!(CSV_XML_XPATH.len(), 11);
        for name in CSV_XML_XPATH {
            let message = refusal_message(name).expect("csv/xml/xpath name has a message");
            assert!(message.contains("deferred by cost"), "{name}: {message}");
            assert!(
                message.contains("reachable without a JVM"),
                "{name}: {message}"
            );
            assert!(!message.contains("unreachable"), "{name}: {message}");
            let error = refuse_in_sql(&format!("SELECT {name}(1)")).expect_err(*name);
            assert!(matches!(error, DataFusionError::NotImplemented(_)));
        }
    }

    #[test]
    fn variant_is_deferred_by_cost_and_sorted() {
        assert!(VARIANT.is_sorted());
        assert_eq!(VARIANT.len(), 8);
        for name in VARIANT {
            let message = refusal_message(name).expect("variant name has a message");
            assert!(message.contains("deferred by cost"), "{name}: {message}");
            assert!(
                message.contains("reachable without a JVM"),
                "{name}: {message}"
            );
            assert!(!message.contains("unreachable"), "{name}: {message}");
            let error = refuse_in_sql(&format!("SELECT {name}(1)")).expect_err(*name);
            assert!(matches!(error, DataFusionError::NotImplemented(_)));
        }
    }

    #[test]
    fn java_method_sql_refuses() {
        let error = refuse_in_sql("SELECT java_method(1)").expect_err("must refuse");
        let text = error.to_string();
        assert!(matches!(error, DataFusionError::NotImplemented(_)));
        assert!(text.contains("unreachable"));
        assert!(text.contains("java_method"));
        assert!(text.contains("reflection"));
    }

    #[test]
    fn live_function_sql_is_untouched() {
        refuse_in_sql("SELECT abs(1)").expect("abs is not a declared refusal");
        refuse_in_sql("SELECT 1").expect("SELECT 1 is not a declared refusal");
    }
}
