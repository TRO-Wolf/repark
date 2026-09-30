use std::collections::HashMap;

use super::*;
use pyo3::exceptions::PyRuntimeError;
use repark_core::Error;

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
            )
            .expect("resolve")
        };
        assert_eq!(
            resolve(&stamped, "ID", &pair, false),
            ("bound".to_string(), vec![0])
        );
        assert_eq!(
            resolve(&stamped, "ID", &pair, true),
            ("missing".to_string(), Vec::new())
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
        let requalified =
            crate::dataframe_names::requalify_join_sides(&joined, &stamped, Some(&stamped))
                .expect("requalify");
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
            ("ambiguous".to_string(), vec![0, 2])
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
        let joined = crate::dataframe_names::join_on_keys(
            left.inner(),
            right.inner(),
            &["id".to_string()],
            datafusion::logical_expr::JoinType::Inner,
        )
        .expect("using self-join");
        let joined = PyDataFrame::new(joined, stamped.runtime_handle());
        let after = crate::dataframe_names::attribute_ids(&joined);
        assert_eq!(after.len(), 3);
        assert_eq!(after[0], held[0]);
        assert_eq!(after[1], held[1]);
        assert!(after[2].is_some() && !held.contains(&after[2]));
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
