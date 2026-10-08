use std::collections::HashMap;

use super::*;
use pyo3::exceptions::PyRuntimeError;
use repark_core::Error;

#[path = "executable_twin_tests.rs"]
mod executable_twin;
#[path = "join_exact_tests.rs"]
mod join_exact;

/// Pin exception classification, inheritance, message preservation, and parse-analysis relations.
#[test]
fn to_py_err_routes_to_typed_exceptions_subclassing_runtime_error() {
    Python::attach(|py| {
        let parse = to_py_err(Error::Parse("Expected an expression".into()));
        assert!(parse.is_instance_of::<ParseException>(py));
        assert!(parse.is_instance_of::<PySparkException>(py));
        assert!(parse.is_instance_of::<PyRuntimeError>(py));
        assert!(parse.is_instance_of::<AnalysisException>(py));
        assert!(parse.to_string().contains("Expected an expression"));

        let analysis = to_py_err(Error::Analysis("No field named zzz".into()));
        assert!(analysis.is_instance_of::<AnalysisException>(py));
        assert!(analysis.is_instance_of::<PySparkException>(py));
        assert!(analysis.is_instance_of::<PyRuntimeError>(py));
        assert!(!analysis.is_instance_of::<ParseException>(py));
        assert!(analysis.to_string().contains("No field named zzz"));

        let arithmetic = to_py_err(Error::Arithmetic(
            "[ARITHMETIC_OVERFLOW] conv overflow. SQLSTATE: 22003".into(),
        ));
        assert!(arithmetic.is_instance_of::<ArithmeticException>(py));
        assert!(arithmetic.is_instance_of::<PySparkException>(py));
        assert!(arithmetic.is_instance_of::<PyRuntimeError>(py));
        assert!(!arithmetic.is_instance_of::<AnalysisException>(py));
        assert!(!arithmetic.is_instance_of::<ParseException>(py));
        assert!(arithmetic.to_string().contains("ARITHMETIC_OVERFLOW"));

        let base = to_py_err(Error::DataFusion("Cast error: boom".into()));
        assert!(base.is_instance_of::<PySparkException>(py));
        assert!(base.is_instance_of::<PyRuntimeError>(py));
        assert!(!base.is_instance_of::<AnalysisException>(py));
        assert!(!base.is_instance_of::<ParseException>(py));
        assert!(base.to_string().contains("Cast error: boom"));

        let unsupported = to_py_err(Error::NotImplemented(
            "This feature is not implemented: partitioned MERGE".into(),
        ));
        assert!(unsupported.is_instance_of::<UnsupportedOperationException>(py));
        assert!(unsupported.is_instance_of::<PySparkException>(py));
        assert!(unsupported.is_instance_of::<PyRuntimeError>(py));
        assert!(!unsupported.is_instance_of::<AnalysisException>(py));
        assert!(!unsupported.is_instance_of::<ParseException>(py));
        assert!(
            unsupported
                .to_string()
                .contains("This feature is not implemented: partitioned MERGE")
        );

        let iceberg = to_py_err(Error::Iceberg(
            "CatalogCommitConflicts => metadata changed concurrently".into(),
        ));
        assert!(iceberg.is_instance_of::<PySparkException>(py));
        assert!(iceberg.is_instance_of::<PyRuntimeError>(py));
        assert!(!iceberg.is_instance_of::<UnsupportedOperationException>(py));
        assert!(!iceberg.is_instance_of::<AnalysisException>(py));
        assert!(iceberg.to_string().contains("CatalogCommitConflicts"));

        // MUTATION: route `Error::Config` back to `ErrorClass::Base` → RED.
        let config = to_py_err(Error::Config(
            "spark.sql.catalog.foo.type has an unrecognized value 'nosuchtype'".into(),
        ));
        assert!(config.is_instance_of::<IllegalArgumentException>(py));
        assert!(config.is_instance_of::<PySparkException>(py));
        assert!(config.is_instance_of::<PyRuntimeError>(py));
        assert!(!config.is_instance_of::<AnalysisException>(py));
        assert!(!config.is_instance_of::<ParseException>(py));
        assert!(!config.is_instance_of::<UnsupportedOperationException>(py));
        assert!(config.to_string().contains("nosuchtype"));

        assert!(!base.is_instance_of::<IllegalArgumentException>(py));
        assert!(!analysis.is_instance_of::<IllegalArgumentException>(py));
        assert!(!unsupported.is_instance_of::<IllegalArgumentException>(py));
    });
}

#[test]
fn to_py_err_number_format_is_an_illegal_argument_leaf() {
    Python::attach(|py| {
        let raised = to_py_err(Error::NumberFormat("For input string: \"x\"".into()));
        assert!(raised.is_instance_of::<NumberFormatException>(py));
        assert!(raised.is_instance_of::<IllegalArgumentException>(py));
        assert!(raised.is_instance_of::<PySparkException>(py));
        assert!(!raised.is_instance_of::<AnalysisException>(py));
        assert_eq!(raised.value(py).to_string(), "For input string: \"x\"");
        let plain = to_py_err(Error::IllegalArgument("bad".into()));
        assert!(!plain.is_instance_of::<NumberFormatException>(py));
    });
}

#[test]
fn to_py_err_commit_state_unknown_is_typed_and_carries_operation_id() {
    Python::attach(|py| {
        let stamped = to_py_err(Error::CommitStateUnknown {
            message: "CommitStateUnknown => lost UpdateTable response".into(),
            operation_id: Some("op-42".into()),
        });
        assert!(stamped.is_instance_of::<CommitStateUnknownException>(py));
        assert!(stamped.is_instance_of::<PySparkException>(py));
        assert!(stamped.is_instance_of::<PyRuntimeError>(py));
        assert!(!stamped.is_instance_of::<AnalysisException>(py));
        assert!(!stamped.is_instance_of::<ParseException>(py));
        assert!(!stamped.is_instance_of::<UnsupportedOperationException>(py));
        assert!(!stamped.is_instance_of::<IllegalArgumentException>(py));
        assert!(stamped.to_string().contains("CommitStateUnknown"));
        let operation_id = stamped
            .value(py)
            .getattr("operation_id")
            .expect("operation_id attribute")
            .extract::<Option<String>>()
            .expect("operation_id is str or None");
        assert_eq!(operation_id.as_deref(), Some("op-42"));

        let unstamped = to_py_err(Error::CommitStateUnknown {
            message: "CommitStateUnknown => probe".into(),
            operation_id: None,
        });
        assert!(unstamped.is_instance_of::<CommitStateUnknownException>(py));
        assert!(unstamped.is_instance_of::<PySparkException>(py));
        assert!(
            unstamped
                .value(py)
                .getattr("operation_id")
                .expect("operation_id attribute")
                .is_none()
        );
    });
}

/// MUTATION: give `repark-core` its own `Error` enum (a plausible future re-split) and this crate stops compiling here — loudly, at the seam — instead of silently binding `to_py_err`'s exhaustive fold to a type the doors no longer raise.
/// The native and facade error paths use the same core error type.
const _: fn(repark_common::Error) -> repark_core::Error = |error| error;
const _: fn(repark_core::Error) -> repark_common::Error = |error| error;
const _: fn(repark_common::ErrorClass) -> repark_core::ErrorClass = |class| class;
const _: fn(repark_core::ErrorClass) -> repark_common::ErrorClass = |class| class;

/// Runtime companion pin for the compile-time type-identity coercions.
#[test]
fn repark_core_error_is_the_repark_common_error_type() {
    Python::attach(|py| {
        let via_common: repark_common::Error =
            repark_common::Error::NotImplemented("re-home identity probe".into());
        let via_core: repark_core::Error = via_common;
        assert_eq!(
            via_core.exception_class(),
            repark_common::ErrorClass::Unsupported,
            "the classifier hop is the same fold through either path"
        );
        let raised = to_py_err(via_core);
        assert!(raised.is_instance_of::<UnsupportedOperationException>(py));
        assert!(raised.to_string().contains("re-home identity probe"));
    });
}

#[cfg(not(feature = "allocator-mimalloc"))]
#[test]
fn allocator_mimalloc_is_off_unless_the_feature_is_enabled() {
    const { assert!(!cfg!(feature = "allocator-mimalloc")) };
}

#[cfg(feature = "allocator-mimalloc")]
#[test]
fn allocator_mimalloc_feature_compiles_the_global_allocator_module() {
    const { assert!(cfg!(feature = "allocator-mimalloc")) };
}

fn table_rows(session: &PyReparkSession, table: &str) -> Vec<String> {
    let query = format!("SELECT id, data, cat FROM {table} ORDER BY id");
    let batches = session.runtime.block_on(async {
        let frame = session.session.sql(&query).await.expect("read query");
        frame.collect().await.expect("read the table back")
    });
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|column| {
                    arrow::util::display::array_value_to_string(column, row).expect("render cell")
                })
                .collect();
            rows.push(cells.join(","));
        }
    }
    rows
}

fn overwrite_through_the_binding(
    session_mode: &str,
    force_static_overwrite: bool,
    force_dynamic_overwrite: bool,
) -> Vec<String> {
    overwrite_spec_through_the_binding(
        session_mode,
        "PARTITIONED BY (cat)",
        "INSERT OVERWRITE sc.ns.t PARTITION (cat) SELECT 9, 'z', 'x'",
        force_static_overwrite,
        force_dynamic_overwrite,
    )
}

fn overwrite_spec_through_the_binding(
    session_mode: &str,
    spec: &str,
    statement: &str,
    force_static_overwrite: bool,
    force_dynamic_overwrite: bool,
) -> Vec<String> {
    let warehouse = std::env::temp_dir().join(format!(
        "repark-py-intent-{}-{session_mode}-{force_static_overwrite}-{force_dynamic_overwrite}-{}",
        std::process::id(),
        spec.len() + statement.len()
    ));
    let config = HashMap::from([(
        "spark.sql.sources.partitionOverwriteMode".to_string(),
        session_mode.to_string(),
    )]);
    let rows = Python::attach(|py| {
        let session = Py::new(
            py,
            PyReparkSession::new(py, None, None, None, Some(config), None).expect("session"),
        )
        .expect("session object");
        let session_ref = session.borrow(py);
        session_ref
            .register_memory_catalog(py, "sc", &warehouse.to_string_lossy())
            .expect("memory catalog");
        for statement in [
            "CREATE NAMESPACE sc.ns",
            &format!(
                "CREATE TABLE sc.ns.t (id BIGINT, data STRING, cat STRING) USING iceberg {spec}"
            ),
            "INSERT INTO sc.ns.t VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')",
        ] {
            session_ref.sql(py, statement).expect("seed statement");
        }
        drop(session_ref);
        crate::session_write_options::session_sql_with_write_options(
            py,
            session.borrow(py),
            statement,
            HashMap::new(),
            force_static_overwrite,
            force_dynamic_overwrite,
            false,
        )
        .expect("overwrite through the binding");
        table_rows(&session.borrow(py), "sc.ns.t")
    });
    let _ = std::fs::remove_dir_all(&warehouse);
    rows
}

#[test]
fn binding_dynamic_intent_keeps_the_untouched_partition_in_a_static_session() {
    assert_eq!(
        overwrite_through_the_binding("static", false, true),
        ["2,b,y", "9,z,x"]
    );
}

#[test]
fn binding_static_intent_replaces_the_table_in_either_session_mode() {
    assert_eq!(
        overwrite_through_the_binding("static", true, false),
        ["9,z,x"]
    );
    assert_eq!(
        overwrite_through_the_binding("dynamic", true, false),
        ["9,z,x"]
    );
}

#[test]
fn binding_session_intent_follows_the_session_mode() {
    assert_eq!(
        overwrite_through_the_binding("static", false, false),
        ["9,z,x"]
    );
    assert_eq!(
        overwrite_through_the_binding("dynamic", false, false),
        ["2,b,y", "9,z,x"]
    );
}

#[test]
fn binding_dynamic_intent_without_a_clause_replaces_the_staged_partitions_of_a_transform_spec() {
    let statement = "INSERT OVERWRITE sc.ns.t (id, data, cat) SELECT 9, 'z', 'x'";
    for session_mode in ["static", "dynamic"] {
        assert_eq!(
            overwrite_spec_through_the_binding(
                session_mode,
                "PARTITIONED BY (cat, bucket(1, id))",
                statement,
                false,
                true
            ),
            ["2,b,y", "9,z,x"]
        );
        assert_eq!(
            overwrite_spec_through_the_binding(
                session_mode,
                "PARTITIONED BY (bucket(1, id))",
                statement,
                false,
                true
            ),
            ["9,z,x"]
        );
    }
}

#[test]
fn binding_refuses_both_intent_flags() {
    Python::attach(|py| {
        let session = Py::new(
            py,
            PyReparkSession::new(py, None, None, None, None, None).expect("session"),
        )
        .expect("session object");
        let refusal = crate::session_write_options::session_sql_with_write_options(
            py,
            session.borrow(py),
            "SELECT 1",
            HashMap::new(),
            true,
            true,
            false,
        )
        .err()
        .expect("both flags refuse");
        assert!(refusal.is_instance_of::<pyo3::exceptions::PyValueError>(py));
    });
}

#[test]
fn binding_stamps_resolves_and_re_mints_a_self_join() {
    Python::attach(|py| {
        let session = PyReparkSession::new(py, None, None, None, None, None).expect("session");
        let frame = session
            .sql(py, "SELECT 1 AS id, 'a' AS data")
            .expect("source frame");
        let stamped =
            crate::dataframe_names::stamp_attribute_ids(Py::new(py, frame).expect("handle"))
                .expect("stamp");
        let bound = stamped.bind(py);
        let stamped = bound.borrow();
        let held = crate::dataframe_names::attribute_ids(&stamped);
        assert!(held.iter().all(Option::is_some));
        assert_ne!(held[0], held[1]);
        let pair = vec!["id".to_string(), "data".to_string()];
        let resolve = |frame: &PyDataFrame, written: &str, displays: &[String], exact: bool| {
            crate::dataframe_names::resolve_display_name(
                frame,
                written,
                None,
                displays.to_vec(),
                exact,
                None,
            )
            .expect("resolve")
        };
        assert_eq!(
            resolve(&stamped, "ID", &pair, false),
            ("bound".to_string(), vec![0], vec![None::<Vec<String>>])
        );
        assert_eq!(
            resolve(&stamped, "ID", &pair, true),
            ("missing".to_string(), Vec::new(), Vec::new())
        );
        for view in ["attr_l", "attr_r"] {
            session
                .create_or_replace_temp_view(view, &stamped)
                .expect("temp view");
        }
        let joined = session
            .sql(
                py,
                "SELECT attr_l.id AS l_id, attr_l.data AS l_data, attr_r.id AS r_id, \
                 attr_r.data AS r_data FROM attr_l CROSS JOIN attr_r",
            )
            .expect("self-join");
        assert_eq!(
            crate::dataframe_names::attribute_ids(&joined)[2..],
            held[..]
        );
        let left_node = crate::frame_lineage::frame_root(&stamped).expect("left node");
        let right_node = crate::frame_lineage::frame_root(&stamped).expect("right node");
        let shared =
            repark_core::frame_names::shared_ids(&left_node.node, right_node.node.outputs());
        let remint = shared
            .iter()
            .map(|id| {
                (
                    id.as_str().to_string(),
                    repark_core::frame_names::AttrId::mint()
                        .as_str()
                        .to_string(),
                )
            })
            .collect::<HashMap<_, _>>();
        let (requalified, node) = crate::dataframe_names::requalify_join_sides(
            &joined,
            &stamped,
            Some(&stamped),
            &left_node,
            &right_node,
            remint,
            true,
        )
        .expect("requalify");
        assert!(node.node.renews());
        let after = crate::dataframe_names::attribute_ids(&requalified);
        assert_eq!(after[..2], held[..]);
        assert!(
            after[2..]
                .iter()
                .all(|id| id.is_some() && !held.contains(id))
        );
        let quad = ["id", "data", "id", "data"].map(str::to_string);
        assert_eq!(
            resolve(&requalified, "id", &quad, false),
            (
                "ambiguous".to_string(),
                vec![0, 2],
                vec![None::<Vec<String>>, None::<Vec<String>>]
            )
        );
    });
}

#[test]
fn binding_re_mints_using_join_collisions_of_a_self_join() {
    Python::attach(|py| {
        let session = PyReparkSession::new(py, None, None, None, None, None).expect("session");
        let frame = session
            .sql(py, "SELECT 1 AS id, 'a' AS data")
            .expect("source frame");
        let stamped =
            crate::dataframe_names::stamp_attribute_ids(Py::new(py, frame).expect("handle"))
                .expect("stamp");
        let bound = stamped.bind(py);
        let stamped = bound.borrow();
        let held = crate::dataframe_names::attribute_ids(&stamped);
        for view in ["using_l", "using_r"] {
            session
                .create_or_replace_temp_view(view, &stamped)
                .expect("temp view");
        }
        let left = session.sql(py, "SELECT * FROM using_l").expect("left");
        let right = session.sql(py, "SELECT * FROM using_r").expect("right");
        let left_node = crate::frame_lineage::frame_root(&left).expect("left node");
        let right_node = crate::frame_lineage::frame_root(&right).expect("right node");
        let (joined, node) = crate::dataframe_names::join_on_keys(
            &left,
            &right,
            &["id".to_string()],
            datafusion::logical_expr::JoinType::Inner,
            &left_node,
            &right_node,
        )
        .expect("using self-join");
        let joined = PyDataFrame::new(joined, stamped.runtime_handle());
        let after = crate::dataframe_names::attribute_ids(&joined);
        assert_eq!(after.len(), 3);
        assert_eq!(after[0], held[0]);
        assert_eq!(after[1], held[1]);
        assert!(after[2].is_some() && !held.contains(&after[2]));
        assert!(node.node.renews());
    });
}

#[test]
fn binding_re_mints_lateral_join_collisions_of_a_self_join() {
    Python::attach(|py| {
        let session = PyReparkSession::new(py, None, None, None, None, None).expect("session");
        let frame = session
            .sql(py, "SELECT 1 AS id, 'a' AS data")
            .expect("source frame");
        let stamped =
            crate::dataframe_names::stamp_attribute_ids(Py::new(py, frame).expect("handle"))
                .expect("stamp");
        let bound = stamped.bind(py);
        let stamped = bound.borrow();
        let held = crate::dataframe_names::attribute_ids(&stamped);
        let left = crate::subquery::subquery_alias(&stamped, "l").expect("left alias");
        let right = crate::subquery::subquery_alias(&stamped, "r").expect("right alias");
        for join_type in ["inner", "left"] {
            let joined = crate::subquery::lateral_join(&left, &right, join_type, None, None)
                .expect("lateral self-join");
            let after = crate::dataframe_names::attribute_ids(&joined);
            assert_eq!(after.len(), 4);
            assert_eq!(after[..2], held[..]);
            assert!(
                after[2..]
                    .iter()
                    .all(|id| id.is_some() && !held.contains(id))
            );
            assert_ne!(after[2], after[3]);
        }
    });
}

const SPARK_1182_ONE: &str = "Column v#1L are ambiguous. It's probably because you joined \
several Datasets together, and some of these Datasets are the same. This column points to one of \
the Datasets but Spark is unable to figure out which one. Please alias the Datasets with \
different names via `Dataset.as` before joining them, and specify the column using qualified \
name, e.g. `df.as(\"a\").join(df.as(\"b\"), $\"a.id\" > $\"b.id\")`. You can also set \
spark.sql.analyzer.failAmbiguousSelfJoin to false to disable this check.";

fn without_expr_ids(text: &str) -> String {
    let mut kept = String::new();
    let mut rest = text;
    while let Some(at) = rest.find('#') {
        kept.push_str(&rest[..at]);
        let after = &rest[at + 1..];
        let digits = after.bytes().take_while(u8::is_ascii_digit).count();
        if digits > 0 && after[digits..].starts_with('L') {
            rest = &after[digits + 1..];
        } else {
            kept.push('#');
            rest = after;
        }
    }
    kept.push_str(rest);
    kept
}

fn spark_error_parts(py: Python<'_>, raised: &PyErr) -> (String, HashMap<String, String>, String) {
    let value = raised.value(py);
    let condition = value
        .getattr("_spark_error_class")
        .and_then(|held| held.extract::<String>())
        .expect("condition");
    let params = value
        .getattr("_spark_message_parameters")
        .and_then(|held| held.extract::<HashMap<String, String>>())
        .expect("params");
    (condition, params, value.to_string())
}

fn self_join_frame(
    py: Python<'_>,
    session: &PyReparkSession,
) -> (Py<PyDataFrame>, crate::frame_lineage::PyFrameNode) {
    let frame = session
        .sql(py, "SELECT 1 AS id, 'a' AS data")
        .expect("source frame");
    let stamped = crate::dataframe_names::stamp_attribute_ids(Py::new(py, frame).expect("handle"))
        .expect("stamp");
    let node = crate::frame_lineage::frame_root(&stamped.bind(py).borrow()).expect("root node");
    (stamped, node)
}

fn token(node: &crate::frame_lineage::PyFrameNode, position: usize) -> String {
    format!(
        "__REPARK_ATTR_{}__F{}____",
        node.node.outputs()[position].as_str(),
        node.node.id().get()
    )
}

#[test]
fn self_join_refusal_is_spark_1182_on_class_config_template_and_names() {
    Python::attach(|py| {
        let raised = crate::frame_lineage::self_join_error(py, &["v".to_string()]);
        assert!(raised.is_instance_of::<AnalysisException>(py));
        let (condition, params, message) = spark_error_parts(py, &raised);
        assert_eq!(condition, "_LEGACY_ERROR_TEMP_1182");
        assert_eq!(
            params,
            HashMap::from([
                ("ambiguousAttrs".to_string(), "v".to_string()),
                (
                    "config".to_string(),
                    "spark.sql.analyzer.failAmbiguousSelfJoin".to_string()
                ),
            ])
        );
        assert_eq!(message, without_expr_ids(SPARK_1182_ONE));
        let pair = crate::frame_lineage::self_join_error(py, &["id".to_string(), "id".to_string()]);
        let (_, params, message) = spark_error_parts(py, &pair);
        assert_eq!(params["ambiguousAttrs"], "id, id");
        assert_eq!(
            message,
            without_expr_ids(&SPARK_1182_ONE.replace("v#1L", "id#0L, id#0L"))
        );
    });
}

#[test]
fn missing_attribute_refusals_follow_spark_subclasses() {
    Python::attach(|py| {
        let appear = crate::frame_lineage::missing_attributes_error(
            py,
            &["k".to_string()],
            &["k".to_string(), "k".to_string()],
            &["k".to_string()],
        );
        let (condition, params, message) = spark_error_parts(py, &appear);
        assert_eq!(
            condition,
            "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION"
        );
        assert_eq!(params["missingAttributes"], "\"k\"");
        assert_eq!(params["input"], "\"k\", \"k\"");
        assert_eq!(params["operation"], "\"k\"");
        assert!(message.starts_with(
            "[MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION] Resolved attribute(s) \
             \"k\" missing from \"k\", \"k\" in operator !Join"
        ));
        let missing = crate::frame_lineage::missing_attributes_error(
            py,
            &["k".to_string()],
            &["id".to_string(), "v".to_string()],
            &[],
        );
        let (condition, params, _) = spark_error_parts(py, &missing);
        assert_eq!(
            condition,
            "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT"
        );
        assert!(!params.contains_key("operation"));
    });
}

#[test]
fn self_join_confs_route_through_the_runtime_conf_and_read_live() {
    Python::attach(|py| {
        let session = Py::new(
            py,
            PyReparkSession::new(py, None, None, None, None, None).expect("session"),
        )
        .expect("session object");
        let live = |session: &Py<PyReparkSession>| {
            let config = crate::session_runtime::session_self_join(&session.borrow(py).session);
            (config.fail_ambiguous, config.auto_resolve)
        };
        assert_eq!(live(&session), (true, true));
        let fail = "spark.sql.analyzer.failAmbiguousSelfJoin";
        let auto = "spark.sql.selfJoinAutoResolveAmbiguity";
        crate::session_runtime::set_runtime_config(session.borrow(py), fail, "false")
            .expect("set fail");
        assert_eq!(live(&session), (false, true));
        crate::session_runtime::set_runtime_config(session.borrow(py), auto, "FALSE")
            .expect("set auto");
        assert_eq!(live(&session), (false, false));
        crate::session_runtime::unset_runtime_config(session.borrow(py), fail).expect("unset");
        assert_eq!(live(&session), (true, false));
        let refused = crate::session_runtime::set_runtime_config(session.borrow(py), auto, "maybe")
            .expect_err("a non-boolean refuses");
        assert!(refused.is_instance_of::<IllegalArgumentException>(py));
        assert!(refused.value(py).to_string().starts_with(
            "[INVALID_CONF_VALUE.TYPE_MISMATCH] The value 'maybe' in the config \
             \"spark.sql.selfJoinAutoResolveAmbiguity\" is invalid."
        ));
        assert_eq!(live(&session), (true, false));
    });
}

#[test]
fn binding_builds_inert_empty_root_for_never_stamped_plans() {
    Python::attach(|py| {
        let session = PyReparkSession::new(py, None, None, None, None, None).expect("session");
        let plan = session
            .sql(py, "EXPLAIN SELECT 1 AS id")
            .expect("explain frame");
        let held = Py::new(py, plan).expect("handle");
        let bound = held.bind(py);
        let borrowed = bound.borrow();
        let root = crate::frame_lineage::frame_root(&borrowed).expect("inert root");
        assert!(!root.node.renews());
        assert!(root.node.outputs().is_empty());
        let (_frame, parent) = self_join_frame(py, &session);
        let cut = crate::frame_lineage::frame_derived(&borrowed, &parent, Vec::new()).expect("cut");
        assert!(!cut.node.renews());
        assert!(cut.node.outputs().is_empty());
    });
}

#[test]
fn binding_prepares_self_join_conditions_by_the_session_rules() {
    Python::attach(|py| {
        let session = Py::new(
            py,
            PyReparkSession::new(py, None, None, None, None, None).expect("session"),
        )
        .expect("session object");
        let (frame, d) = self_join_frame(py, &session.borrow(py));
        let frame = frame.bind(py).borrow();
        let f = crate::frame_lineage::frame_derived(&frame, &d, Vec::new()).expect("derived");
        assert!(!d.node.renews() && !f.node.renews());
        let shown = vec!["id".to_string(), "data".to_string()];
        let prepare = |condition: &str| {
            crate::frame_lineage::prepare_join_condition(
                py,
                &session.borrow(py),
                condition,
                &d,
                &f,
                &frame,
                &frame,
                "_l",
                "_r",
                shown.clone(),
                shown.clone(),
                HashMap::new(),
                None,
                None,
            )
        };
        let (sql, remint) = prepare(&format!("({} = {})", token(&d, 0), token(&f, 0)))
            .expect("the equi self-join is rewritten");
        assert_eq!(sql, "(_l.`id` = _r.`id`)");
        assert_eq!(remint.len(), 2);
        let refused = prepare(&format!("({} > {})", token(&d, 0), token(&f, 0)))
            .expect_err("the shared lineage refuses");
        let (condition, params, _) = spark_error_parts(py, &refused);
        assert_eq!(condition, "_LEGACY_ERROR_TEMP_1182");
        assert_eq!(params["ambiguousAttrs"], "id, id");
        crate::session_runtime::set_runtime_config(
            session.borrow(py),
            "spark.sql.analyzer.failAmbiguousSelfJoin",
            "false",
        )
        .expect("conf off");
        let (sql, _) = prepare(&format!("({} < {})", token(&f, 0), token(&d, 0)))
            .expect("the conf off binds left");
        assert_eq!(sql, "(_l.`id` < _l.`id`)");
    });
}

#[test]
fn binding_refuses_post_join_references_through_the_join_lineage() {
    Python::attach(|py| {
        let session = PyReparkSession::new(py, None, None, None, None, None).expect("session");
        let (_frame, d) = self_join_frame(py, &session);
        let held = d.node.outputs().to_vec();
        let renewed = held
            .iter()
            .map(|_| repark_core::frame_names::AttrId::mint())
            .collect::<Vec<_>>();
        let fields = held
            .iter()
            .chain(&renewed)
            .enumerate()
            .map(|(position, id)| {
                datafusion::arrow::datatypes::Field::new(
                    format!("c{position}"),
                    datafusion::arrow::datatypes::DataType::Int64,
                    true,
                )
                .with_metadata(HashMap::from([(
                    "repark.attr".to_string(),
                    id.as_str().to_string(),
                )]))
            })
            .collect::<Vec<_>>();
        let schema = datafusion::common::DFSchema::try_from(
            datafusion::arrow::datatypes::Schema::new(fields),
        )
        .expect("joined schema");
        let joined = crate::frame_lineage::PyFrameNode {
            node: repark_core::frame_names::FrameNode::join(
                &schema,
                std::sync::Arc::clone(&d.node),
                std::sync::Arc::clone(&d.node),
                held.iter().cloned().zip(renewed).collect(),
                true,
            )
            .expect("join node"),
        };
        let shown = ["id", "data", "id", "data"].map(str::to_string).to_vec();
        let refused = crate::frame_lineage::refuse_self_join_refs(
            py,
            &session,
            &joined,
            vec![token(&d, 0)],
            shown.clone(),
        )
        .expect_err("the parent's id is on both sides");
        let (condition, params, _) = spark_error_parts(py, &refused);
        assert_eq!(condition, "_LEGACY_ERROR_TEMP_1182");
        assert_eq!(params["ambiguousAttrs"], "id");
        crate::frame_lineage::refuse_self_join_refs(py, &session, &d, vec![token(&d, 0)], shown)
            .expect("a frame with no self-join pays one branch");
    });
}

#[test]
fn grown_stack_guard_rejects_bypass_sites() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let patterns = [
        ".df.clone()",
        ".expr.clone()",
        "inner().clone()",
        "frame.clone()",
        "logical_plan().clone()",
        "plan.clone()",
        "to_owned()",
    ];
    let markers = [
        "grown_sync",
        "grown_clone",
        "run_grown_if",
        "block_on_grown",
        "run_on_grown_stack",
        "grown_read",
    ];
    let allowed: &[(&str, &str)] = &[
        ("cdf_infer/infer.rs", "name.to_owned()"),
        ("type_bridge.rs", "collation.into_owned()"),
    ];
    let prod_markers = ["pub fn", "pub(crate) fn", "#[pyfunction]", "#[pymethods]"];
    let mut offenders = Vec::new();
    let mut stack = vec![root];
    while let Some(path) = stack.pop() {
        if path.is_dir() {
            let entries = std::fs::read_dir(&path).expect("src entries read");
            for entry in entries {
                stack.push(entry.expect("a dir entry reads").path());
            }
            continue;
        }
        if path.extension().is_none_or(|ext| ext != "rs") {
            continue;
        }
        let name = path.file_name().expect("a file name").to_string_lossy();
        if name.contains("test") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("a source file reads");
        let mut in_tests = false;
        for (index, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("").trim();
            if code.contains("mod tests {") {
                in_tests = true;
                continue;
            }
            if in_tests {
                if prod_markers.iter().any(|marker| code.contains(marker)) {
                    offenders.push(format!(
                        "{}:{}: production code after `mod tests` hides from the guard",
                        path.display(),
                        index + 1
                    ));
                }
                continue;
            }
            if !patterns.iter().any(|pattern| code.contains(pattern)) {
                continue;
            }
            let listed = allowed.iter().any(|(file, needle)| {
                path.to_string_lossy().ends_with(file) && code.contains(needle)
            });
            if listed {
                continue;
            }
            let home = path.file_name().is_some_and(|name| name == "deep_stack.rs");
            let nested = markers.iter().any(|marker| code.contains(marker));
            if home && nested {
                continue;
            }
            if nested {
                offenders.push(format!(
                    "{}:{}: a raw clone inside a grown region must use grown_clone_* ({code})",
                    path.display(),
                    index + 1
                ));
            } else {
                offenders.push(format!("{}:{}: {code}", path.display(), index + 1));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "owning-type clones outside the grown helpers:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn boundary_mask_leaves_a_credential_free_message_byte_identical() {
    let plain =
        "config `repark.sql.maxArrayElements` must be a positive integer (got \"notabool\")";
    let zone = "spark.sql.session.timeZone must be a zone id";
    Python::attach(|py| {
        let raised = to_py_err(Error::Analysis(plain.to_string()));
        assert_eq!(raised.value(py).to_string(), plain);
        let expected_plan =
            repark_core::engine_err(datafusion::error::DataFusionError::Plan(plain.to_string()))
                .to_string();
        let via_df =
            datafusion_to_py_err(datafusion::error::DataFusionError::Plan(plain.to_string()));
        assert_eq!(via_df.value(py).to_string(), expected_plan);
        let expected_config = repark_core::engine_err(
            datafusion::error::DataFusionError::Configuration(zone.to_string()),
        )
        .to_string();
        let configured = datafusion_to_py_err(datafusion::error::DataFusionError::Configuration(
            zone.to_string(),
        ));
        assert_eq!(configured.value(py).to_string(), expected_config);
    });
}

#[test]
fn boundary_mask_hides_a_url_password_in_plan_and_configuration_messages() {
    let url = "postgresql://u:pw@h/db";
    let masked = "postgresql://u:***@h/db";
    let plan_inner = format!("config `k` must be a positive integer (got {url:?})");
    let zone_inner = format!("`spark.sql.session.timeZone` = {url:?} is not a zone");
    Python::attach(|py| {
        let plan =
            datafusion_to_py_err(datafusion::error::DataFusionError::Plan(plan_inner.clone()));
        let plan_text = plan.value(py).to_string();
        assert!(plan_text.contains(masked));
        assert!(!plan_text.contains("u:pw@"));
        assert!(plan.is_instance_of::<AnalysisException>(py));
        let configured = datafusion_to_py_err(datafusion::error::DataFusionError::Configuration(
            zone_inner.clone(),
        ));
        let config_text = configured.value(py).to_string();
        assert!(config_text.contains(masked));
        assert!(!config_text.contains("u:pw@"));
        assert!(configured.is_instance_of::<IllegalArgumentException>(py));
        let direct = to_py_err(Error::Config(zone_inner));
        let direct_text = direct.value(py).to_string();
        assert!(direct_text.contains(masked));
        assert!(!direct_text.contains("u:pw@"));
        assert!(direct.is_instance_of::<IllegalArgumentException>(py));
    });
}

#[test]
fn masking_a_message_twice_is_a_no_op() {
    let text = "saw postgresql://u:pw@h/db and notabool";
    let once = repark_core::redaction::mask_value_credentials(text);
    let twice = repark_core::redaction::mask_value_credentials(&once);
    assert_eq!(once, twice);
    assert!(once.contains("postgresql://u:***@h/db"));
    assert!(!once.contains("u:pw@"));
    let plain = "notabool stays";
    assert_eq!(repark_core::redaction::mask_value_credentials(plain), plain);
    let via = super::exceptions::mask_user_visible(text);
    assert_eq!(super::exceptions::mask_user_visible(&via), via);
}
