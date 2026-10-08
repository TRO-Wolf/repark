use datafusion::logical_expr::{Expr, col, lit};
use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::session::PyReparkSession;

const KEYS: [(&str, &str); 20] = [
    ("BOOLEAN", "BOOLEAN"),
    ("TINYINT", "TINYINT"),
    ("SMALLINT", "SMALLINT"),
    ("INT", "INT"),
    ("BIGINT", "BIGINT"),
    ("FLOAT", "FLOAT"),
    ("DOUBLE", "DOUBLE"),
    ("DECIMAL(10,2)", "DECIMAL(10,2)"),
    ("DECIMAL(38,0)", "DECIMAL(38,0)"),
    ("STRING", "STRING"),
    ("DATE", "DATE"),
    ("TIMESTAMP", "TIMESTAMP"),
    ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
    ("BINARY", "BINARY"),
    ("ARRAY<INT>", "ARRAY<INT>"),
    ("STRUCT<a: INT>", "STRUCT<a: INT>"),
    ("INT", "BIGINT"),
    ("INT", "STRING"),
    ("DATE", "TIMESTAMP"),
    ("DECIMAL(10,2)", "DOUBLE"),
];
const HOWS: [(&str, &str, bool); 6] = [
    ("inner", "INNER", true),
    ("left", "LEFT OUTER", true),
    ("right", "RIGHT OUTER", true),
    ("full", "FULL OUTER", true),
    ("leftsemi", "LEFT SEMI", false),
    ("leftanti", "LEFT ANTI", false),
];

type Outputs<'a> = &'a [(&'a str, &'a str)];

struct Sides {
    left: Py<PyDataFrame>,
    right: Py<PyDataFrame>,
    left_alias: String,
    right_alias: String,
}

fn stamped(py: Python<'_>, session: &PyReparkSession, query: &str) -> Py<PyDataFrame> {
    let frame = session.sql(py, query).expect("a side plans");
    crate::dataframe_names::stamp_attribute_ids(Py::new(py, frame).expect("handle"))
        .expect("a side stamps")
}

fn sides(py: Python<'_>, session: &PyReparkSession, left: &str, right: &str) -> Sides {
    let home = session.temp_view_home().expect("the session has a home");
    let alias = |name: &str| format!("`{}`.`{}`.`{name}`", home[0], home[1]);
    let sides = Sides {
        left: stamped(py, session, left),
        right: stamped(py, session, right),
        left_alias: alias("_repark_jl_pin"),
        right_alias: alias("_repark_jr_pin"),
    };
    session
        .create_or_replace_temp_view(&sides.left_alias, &sides.left.bind(py).borrow())
        .expect("the left view registers");
    session
        .create_or_replace_temp_view(&sides.right_alias, &sides.right.bind(py).borrow())
        .expect("the right view registers");
    sides
}

fn keyed_sides(py: Python<'_>, session: &PyReparkSession, keys: (&str, &str)) -> Sides {
    sides(
        py,
        session,
        &format!("SELECT CAST(NULL AS {}) AS k, 10 AS v", keys.0),
        &format!("SELECT CAST(NULL AS {}) AS k, 'x' AS W", keys.1),
    )
}

fn derived_with(py: Python<'_>, frame: &Py<PyDataFrame>, outputs: Vec<Expr>) -> Py<PyDataFrame> {
    let frame = frame.bind(py).borrow();
    let grown = frame
        .inner()
        .clone()
        .select(outputs)
        .expect("the derived side plans");
    let derived = PyDataFrame::new(grown, frame.runtime_handle());
    crate::dataframe_names::stamp_attribute_ids(Py::new(py, derived).expect("handle"))
        .expect("the derived side stamps")
}

fn derived(py: Python<'_>, frame: &Py<PyDataFrame>) -> Py<PyDataFrame> {
    derived_with(py, frame, vec![col("k"), (col("v") + lit(1.5)).alias("v")])
}

fn join_query(sides: &Sides, how_sql: &str, right: &[(&str, &str)], left_first: bool) -> String {
    let mut parts = projection(&sides.left_alias, &LEFT);
    parts.extend(projection(&sides.right_alias, right));
    let (first, second) = if left_first {
        (&sides.left_alias, &sides.right_alias)
    } else {
        (&sides.right_alias, &sides.left_alias)
    };
    format!(
        "SELECT {} FROM {} {how_sql} JOIN {} ON ({first}.`k` = {second}.`k`)",
        parts.join(", "),
        sides.left_alias,
        sides.right_alias,
    )
}

fn pairs(outputs: &[(&str, &str)]) -> Vec<(String, String)> {
    outputs
        .iter()
        .map(|(source, output)| ((*source).to_string(), (*output).to_string()))
        .collect()
}

fn projection(alias: &str, outputs: &[(&str, &str)]) -> Vec<String> {
    outputs
        .iter()
        .map(|(source, output)| format!("{alias}.`{source}` AS `{output}`"))
        .collect()
}

fn native(
    py: Python<'_>,
    session: &PyReparkSession,
    sides: &Sides,
    how: &str,
    keys: Option<(&str, &str, bool)>,
    outputs: (Outputs<'_>, Outputs<'_>),
) -> PyResult<Option<PyDataFrame>> {
    crate::frame_lineage::join_exact_sides(
        session,
        &sides.left.bind(py).borrow(),
        &sides.right.bind(py).borrow(),
        &sides.left_alias,
        &sides.right_alias,
        how,
        keys.map(|(left, right, left_first)| (left.to_string(), right.to_string(), left_first)),
        pairs(outputs.0),
        pairs(outputs.1),
    )
}

fn assert_same_plan(native: &PyDataFrame, sql: &PyDataFrame, shape: &str) {
    assert_eq!(
        native.inner().logical_plan(),
        sql.inner().logical_plan(),
        "{shape}\n{}\n{}",
        native.inner().logical_plan().display_indent_schema(),
        sql.inner().logical_plan().display_indent_schema(),
    );
    let (native, sql) = (native.depths(), sql.depths());
    assert_eq!(
        (native.plan, native.limited, native.expression),
        (sql.plan, sql.limited, sql.expression),
        "{shape}"
    );
}

const LEFT: [(&str, &str); 2] = [("k", "__repark_l_0_k"), ("v", "v")];
const RIGHT: [(&str, &str); 2] = [("k", "__repark_r_2_k"), ("W", "W")];

#[test]
fn exact_key_join_builds_the_sql_route_plan_for_every_key_type_and_how() {
    Python::attach(|py| {
        let session = PyReparkSession::new(py, None, None, None, None, None).expect("session");
        for key in KEYS {
            let sides = keyed_sides(py, &session, key);
            for (how, how_sql, emits_right) in HOWS {
                let right: &[(&str, &str)] = if emits_right { &RIGHT } else { &[] };
                for left_first in [true, false] {
                    let query = join_query(&sides, how_sql, right, left_first);
                    let sql = session.sql_built(py, &query).expect("the SQL route plans");
                    let keys = Some(("k", "k", left_first));
                    let built = native(py, &session, &sides, how, keys, (&LEFT, right))
                        .expect("the native door answers")
                        .expect("an exact key plans natively");
                    assert_same_plan(&built, &sql, &format!("{key:?} {how} {left_first}"));
                }
            }
        }
    });
}

#[test]
fn unanalyzed_sides_are_analyzed_as_the_sql_route_analyzes_them() {
    Python::attach(|py| {
        let session = PyReparkSession::new(py, None, None, None, None, None).expect("session");
        let base = keyed_sides(py, &session, ("TIMESTAMP", "TIMESTAMP"));
        let grown = Sides {
            left: derived(py, &base.left),
            right: base.right.clone_ref(py),
            left_alias: base.left_alias.clone(),
            right_alias: base.right_alias.clone(),
        };
        session
            .create_or_replace_temp_view(&grown.left_alias, &grown.left.bind(py).borrow())
            .expect("the derived view registers");
        let unanalyzed = grown.left.bind(py).borrow().inner().logical_plan().clone();
        let state = session.session.context().state();
        let analyzed = repark_spark::analyze_built_plan(&state, unanalyzed.clone())
            .expect("the derived side analyzes");
        assert_ne!(analyzed, unanalyzed);
        let query = join_query(&grown, "INNER", &RIGHT, true);
        let sql = session.sql_built(py, &query).expect("the SQL route plans");
        let keys = Some(("k", "k", true));
        let built = native(py, &session, &grown, "inner", keys, (&LEFT, &RIGHT))
            .expect("the native door answers")
            .expect("an exact key plans natively");
        assert_same_plan(&built, &sql, "derived inner");
        let mut parts = projection(&grown.left_alias, &LEFT);
        parts.extend(projection(&grown.right_alias, &RIGHT));
        let query = format!(
            "SELECT {} FROM {} CROSS JOIN {}",
            parts.join(", "),
            grown.left_alias,
            grown.right_alias,
        );
        let sql = session.sql_built(py, &query).expect("the SQL route plans");
        let built = native(py, &session, &grown, "cross", None, (&LEFT, &RIGHT))
            .expect("the native door answers")
            .expect("a cross join plans natively");
        assert_same_plan(&built, &sql, "derived cross");
    });
}

#[test]
fn cross_join_builds_the_sql_route_plan() {
    Python::attach(|py| {
        let session = PyReparkSession::new(py, None, None, None, None, None).expect("session");
        let sides = keyed_sides(py, &session, ("MAP<INT, INT>", "MAP<INT, INT>"));
        let mut parts = projection(&sides.left_alias, &LEFT);
        parts.extend(projection(&sides.right_alias, &RIGHT));
        let query = format!(
            "SELECT {} FROM {} CROSS JOIN {}",
            parts.join(", "),
            sides.left_alias,
            sides.right_alias,
        );
        let sql = session.sql_built(py, &query).expect("the SQL route plans");
        for keys in [None, Some(("k", "k", true))] {
            let built = native(py, &session, &sides, "cross", keys, (&LEFT, &RIGHT))
                .expect("the native door answers")
                .expect("a cross join plans natively");
            assert_same_plan(&built, &sql, "cross");
        }
    });
}

#[test]
fn a_map_key_refuses_on_both_routes() {
    Python::attach(|py| {
        let session = PyReparkSession::new(py, None, None, None, None, None).expect("session");
        let sides = keyed_sides(py, &session, ("MAP<INT, INT>", "MAP<INT, INT>"));
        let query = join_query(&sides, "INNER", &RIGHT, true);
        let sql = session
            .sql_built(py, &query)
            .err()
            .expect("the SQL route refuses a map key");
        let keys = Some(("k", "k", true));
        let built = native(py, &session, &sides, "inner", keys, (&LEFT, &RIGHT))
            .err()
            .expect("the native door refuses a map key");
        let needle = "[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE]";
        assert!(sql.value(py).to_string().contains(needle), "{sql}");
        assert!(built.value(py).to_string().contains(needle), "{built}");
    });
}

#[test]
fn inexact_joins_are_left_to_the_sql_route() {
    Python::attach(|py| {
        let session = PyReparkSession::new(py, None, None, None, None, None).expect("session");
        let exact = keyed_sides(py, &session, ("INT", "INT"));
        let keyed = Some(("k", "k", true));
        let missed = |sides: &Sides, how: &str, keys, outputs| {
            native(py, &session, sides, how, keys, outputs)
                .expect("the native door answers")
                .is_none()
        };
        assert!(!missed(&exact, "inner", keyed, (&LEFT, &RIGHT)));
        assert!(missed(&exact, "inner", None, (&LEFT, &RIGHT)));
        let absent = Some(("K", "k", true));
        assert!(missed(&exact, "inner", absent, (&LEFT, &RIGHT)));
        let spaced: [(&str, &str); 2] = [("k", "a b"), ("v", "v")];
        assert!(missed(&exact, "inner", keyed, (&spaced, &RIGHT)));
        assert!(missed(&exact, "cross", None, (&spaced, &RIGHT)));
        let unheld: [(&str, &str); 2] = [("k", "k"), ("w", "w")];
        assert!(missed(&exact, "inner", keyed, (&LEFT, &unheld)));
        let bare = Sides {
            left: exact.left.clone_ref(py),
            right: exact.right.clone_ref(py),
            left_alias: "`_repark_jl_pin`".to_string(),
            right_alias: exact.right_alias.clone(),
        };
        assert!(missed(&bare, "inner", keyed, (&LEFT, &RIGHT)));
        assert!(missed(&bare, "cross", None, (&LEFT, &RIGHT)));
        let twins = Sides {
            left: derived_with(
                py,
                &exact.left,
                vec![col("k"), col("v"), lit(11).alias("V")],
            ),
            right: exact.right.clone_ref(py),
            left_alias: exact.left_alias.clone(),
            right_alias: exact.right_alias.clone(),
        };
        assert!(missed(&twins, "inner", keyed, (&LEFT, &RIGHT)));
        assert!(missed(&twins, "cross", None, (&LEFT, &RIGHT)));
    });
}
