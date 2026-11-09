mod setop_reorder_common;

use datafusion::arrow::array::RecordBatch;
use setop_reorder_common::{
    Door, Doors, Form, State, attempt, check_refusal_text, create_shaped_table, observe,
    open_doors, render_rows, session_of, shape_by_id,
};

const D2_L: &str = "named_struct('a', 1, 'b', 2)";
const D2_R: &str = "named_struct('b', 3, 'a', 4)";
const D2_RS: &str = "named_struct('a', 3, 'b', 4)";

#[derive(Clone, Copy, Debug)]
enum DeltaExpect {
    Refuse {
        word: &'static str,
        path: &'static str,
        right: &'static str,
        left: &'static str,
    },
    Answer(&'static str),
    Divergence {
        want: &'static str,
        spark: &'static str,
    },
    PreparedWrite,
    PrepareAnswer(&'static str),
}

const D2_REFUSE_UNION: DeltaExpect = DeltaExpect::Refuse {
    word: "UNION",
    path: "s",
    right: "b, a",
    left: "a, b",
};
const D2_REFUSE_UNION_ALL: DeltaExpect = DeltaExpect::Refuse {
    word: "UNION ALL",
    path: "s",
    right: "b, a",
    left: "a, b",
};
const D2_REFUSE_INTERSECT: DeltaExpect = DeltaExpect::Refuse {
    word: "INTERSECT",
    path: "s",
    right: "b, a",
    left: "a, b",
};
const D2_REFUSE_EXCEPT: DeltaExpect = DeltaExpect::Refuse {
    word: "EXCEPT",
    path: "s",
    right: "b, a",
    left: "a, b",
};

struct DeltaRow {
    id: &'static str,
    shape: &'static str,
    expect: DeltaExpect,
}

const DELTA_UNION_ROWS: [DeltaRow; 15] = [
    DeltaRow {
        id: "d2-row1-union-all",
        shape: "reordered",
        expect: D2_REFUSE_UNION,
    },
    DeltaRow {
        id: "d2-row1-union",
        shape: "reordered",
        expect: D2_REFUSE_UNION,
    },
    DeltaRow {
        id: "d2-row2-union-all",
        shape: "nested-reordered",
        expect: DeltaExpect::Refuse {
            word: "UNION",
            path: "s.x",
            right: "b, a",
            left: "a, b",
        },
    },
    DeltaRow {
        id: "d2-row3-union-all",
        shape: "reordered",
        expect: D2_REFUSE_UNION,
    },
    DeltaRow {
        id: "d2-row4-intersect",
        shape: "reordered",
        expect: D2_REFUSE_INTERSECT,
    },
    DeltaRow {
        id: "d2-row4-except",
        shape: "reordered",
        expect: D2_REFUSE_EXCEPT,
    },
    DeltaRow {
        id: "d2-row5-union-all",
        shape: "reordered",
        expect: D2_REFUSE_UNION,
    },
    DeltaRow {
        id: "d2-row6-ctl-null-middle",
        shape: "reordered",
        expect: D2_REFUSE_UNION,
    },
    DeltaRow {
        id: "d2-row6-ctl-null-nested",
        shape: "reordered",
        expect: D2_REFUSE_UNION,
    },
    DeltaRow {
        id: "d2-row6-ctl-same-names",
        shape: "reordered",
        expect: DeltaExpect::Answer("[1,2]|[3,4]|null"),
    },
    DeltaRow {
        id: "d2-row12-cast-STRUCT-SETOP-USER-CAST-OVERREFUSAL-1",
        shape: "reordered",
        expect: D2_REFUSE_UNION,
    },
    DeltaRow {
        id: "d2-row9-rec-reordered",
        shape: "reordered",
        expect: D2_REFUSE_UNION_ALL,
    },
    DeltaRow {
        id: "d2-row9-rec-same",
        shape: "reordered",
        expect: DeltaExpect::Answer("[1,10]|[12,11]"),
    },
    DeltaRow {
        id: "d2-joincast-intersect-control",
        shape: "reordered",
        expect: DeltaExpect::Answer(""),
    },
    DeltaRow {
        id: "d2-joincast-except-STRUCT-USER-CAST-POSITIONAL-1_divergence",
        shape: "reordered",
        expect: DeltaExpect::Divergence {
            want: "[4,3]",
            spark: "[{a: 3, b: 4}]",
        },
    },
];

fn delta_union_select(id: &str) -> String {
    let left = format!("SELECT {D2_L} AS s");
    let right = format!("SELECT {D2_R} AS s");
    let null = "SELECT NULL AS s".to_string();
    match id {
        "d2-row1-union-all" => format!("{null} UNION ALL {left} UNION ALL {right}"),
        "d2-row1-union" => format!("{null} UNION {left} UNION {right}"),
        "d2-row2-union-all" => "SELECT named_struct('x', NULL) AS s UNION ALL SELECT named_struct('x', named_struct('a', 1, 'b', 2)) AS s UNION ALL SELECT named_struct('x', named_struct('b', 3, 'a', 4)) AS s".to_string(),
        "d2-row3-union-all" => {
            format!("SELECT CASE WHEN true THEN NULL END AS s UNION ALL {left} UNION ALL {right}")
        }
        "d2-row4-intersect" => format!("({null} UNION ALL {left}) INTERSECT {right}"),
        "d2-row4-except" => format!("({null} UNION ALL {left}) EXCEPT {right}"),
        "d2-row5-union-all" => {
            format!("SELECT CAST(NULL AS STRUCT<a: INT, b: INT>) AS s UNION ALL {left} UNION ALL {right}")
        }
        "d2-row6-ctl-null-middle" => format!("{left} UNION ALL {null} UNION ALL {right}"),
        "d2-row6-ctl-null-nested" => format!("{null} UNION ALL ({left} UNION ALL {right})"),
        "d2-row6-ctl-same-names" => {
            format!("{null} UNION ALL {left} UNION ALL SELECT {D2_RS} AS s")
        }
        "d2-row12-cast-STRUCT-SETOP-USER-CAST-OVERREFUSAL-1" => {
            format!("SELECT CAST({D2_R} AS STRUCT<a: INT, b: INT>) AS s UNION ALL {left}")
        }
        "d2-joincast-intersect-control" => {
            format!("SELECT CAST({D2_R} AS STRUCT<a: INT, b: INT>) AS s INTERSECT {left}")
        }
        "d2-joincast-except-STRUCT-USER-CAST-POSITIONAL-1_divergence" => {
            format!("SELECT CAST({D2_R} AS STRUCT<a: INT, b: INT>) AS s EXCEPT {left}")
        }
        "d2-row9-rec-reordered" => "WITH RECURSIVE r(s) AS (SELECT named_struct('a', 1, 'b', 10) AS s UNION ALL SELECT named_struct('b', r.s.b + 1, 'a', r.s.a + 11) AS s FROM r WHERE r.s.b < 11) SELECT s FROM r".to_string(),
        _ => "WITH RECURSIVE r(s) AS (SELECT named_struct('a', 1, 'b', 10) AS s UNION ALL SELECT named_struct('a', r.s.a + 11, 'b', r.s.b + 1) AS s FROM r WHERE r.s.b < 11) SELECT s FROM r".to_string(),
    }
}

struct DeltaViewRow {
    id: &'static str,
    verb: &'static str,
    expect: DeltaExpect,
}

const DELTA_VIEW_ROWS: [DeltaViewRow; 7] = [
    DeltaViewRow {
        id: "d2-row7-view-create",
        verb: "CREATE VIEW",
        expect: D2_REFUSE_UNION,
    },
    DeltaViewRow {
        id: "d2-row7-view-replace",
        verb: "CREATE OR REPLACE VIEW",
        expect: D2_REFUSE_UNION,
    },
    DeltaViewRow {
        id: "d2-row7-view-intersect",
        verb: "CREATE VIEW",
        expect: D2_REFUSE_INTERSECT,
    },
    DeltaViewRow {
        id: "d2-row7-view-except",
        verb: "CREATE VIEW",
        expect: D2_REFUSE_EXCEPT,
    },
    DeltaViewRow {
        id: "d2-row7-view-scalar",
        verb: "CREATE VIEW",
        expect: D2_REFUSE_UNION,
    },
    DeltaViewRow {
        id: "d2-row7-view-cte",
        verb: "CREATE VIEW",
        expect: D2_REFUSE_UNION,
    },
    DeltaViewRow {
        id: "d2-row13-view-same-name",
        verb: "CREATE VIEW",
        expect: DeltaExpect::Answer("[1,2]|[3,4]"),
    },
];

fn delta_view_body(id: &str) -> String {
    let left = format!("SELECT {D2_L} AS s");
    let right = format!("SELECT {D2_R} AS s");
    let union = format!("{left} UNION ALL {right}");
    match id {
        "d2-row7-view-intersect" => format!("{left} INTERSECT {right}"),
        "d2-row7-view-except" => format!("{left} EXCEPT {right}"),
        "d2-row7-view-scalar" => format!("SELECT (SELECT s FROM ({union}) q LIMIT 1) AS s"),
        "d2-row7-view-cte" => format!("WITH u AS ({union}) SELECT s FROM u"),
        "d2-row13-view-same-name" => {
            format!("SELECT {D2_L} AS s UNION ALL SELECT {D2_RS} AS s")
        }
        _ => union,
    }
}

struct DeltaPrepareRow {
    id: &'static str,
    expect: DeltaExpect,
}

const DELTA_PREPARE_ROWS: [DeltaPrepareRow; 6] = [
    DeltaPrepareRow {
        id: "d2-row8-prepare-union",
        expect: D2_REFUSE_UNION,
    },
    DeltaPrepareRow {
        id: "d2-row8-prepare-union-insert",
        expect: DeltaExpect::PreparedWrite,
    },
    DeltaPrepareRow {
        id: "d2-row8-prepare-param",
        expect: D2_REFUSE_UNION,
    },
    DeltaPrepareRow {
        id: "d2-row8-prepare-param-insert",
        expect: DeltaExpect::PreparedWrite,
    },
    DeltaPrepareRow {
        id: "d2-row13-prepare-same",
        expect: DeltaExpect::PrepareAnswer("[1,2]|[3,4]"),
    },
    DeltaPrepareRow {
        id: "d2-row13-prepare-same-insert",
        expect: DeltaExpect::PreparedWrite,
    },
];

fn delta_prepare_text(id: &str, name: &str, table: Option<&str>) -> String {
    let left = format!("SELECT {D2_L} AS s");
    let right = format!("SELECT {D2_R} AS s");
    let union = format!("{left} UNION ALL {right}");
    match id {
        "d2-row8-prepare-param" => {
            format!(
                "PREPARE {name}(INT) AS {left} UNION ALL SELECT named_struct('b', $1, 'a', 4) AS s"
            )
        }
        "d2-row8-prepare-param-insert" => format!(
            "PREPARE {name}(INT) AS INSERT INTO ice.ns.{} {left} UNION ALL SELECT named_struct('b', $1, 'a', 4) AS s",
            table.expect("prepare table")
        ),
        "d2-row13-prepare-same" => {
            format!("PREPARE {name} AS SELECT {D2_L} AS s UNION ALL SELECT {D2_RS} AS s")
        }
        _ if id.ends_with("insert") => format!(
            "PREPARE {name} AS INSERT INTO ice.ns.{} {union}",
            table.expect("prepare table")
        ),
        _ => format!("PREPARE {name} AS {union}"),
    }
}

const DELTA_DOORS: [Door; 2] = [Door::NativeDataFusion, Door::Ansi];

fn delta_door_token(door: Door) -> &'static str {
    match door {
        Door::NativeDataFusion => "native",
        Door::Ansi => "ansi",
        Door::Spark => "spark",
    }
}

fn delta_name(door: Door, id: &str, form: Form) -> String {
    let form_token = match form {
        Form::Read => "read",
        Form::Insert => "insert",
    };
    format!("{}-{id}-{form_token}", delta_door_token(door))
}

fn delta_table_key(door: Door, id: &str) -> String {
    format!(
        "d2_{}_{}_insert",
        delta_door_token(door),
        id.replace('-', "_").to_lowercase()
    )
}

fn check_delta_read(
    label: &str,
    failures: &mut Vec<String>,
    outcome: Result<Vec<RecordBatch>, String>,
    expect: &DeltaExpect,
) {
    match outcome {
        Ok(batches) => match expect {
            DeltaExpect::Answer(want) => {
                let rows = render_rows(&batches);
                if rows != *want {
                    failures.push(format!("{label}: rows {rows} != {want}"));
                }
            }
            DeltaExpect::Divergence { want, spark } => {
                let rows = render_rows(&batches);
                if rows != *want {
                    failures.push(format!(
                        "{label}: rows {rows} != {want}; Spark answers {spark}; the cause is the explicit user cast, as the bare SELECT CAST shows"
                    ));
                }
            }
            DeltaExpect::Refuse { .. }
            | DeltaExpect::PreparedWrite
            | DeltaExpect::PrepareAnswer(_) => {
                failures.push(format!("{label}: answered {}", render_rows(&batches)));
            }
        },
        Err(error) => match expect {
            DeltaExpect::Refuse {
                word,
                path,
                right,
                left,
            } => {
                check_refusal_text(label, failures, &error, word, path, right, left);
            }
            DeltaExpect::Answer(want) | DeltaExpect::PrepareAnswer(want) => {
                failures.push(format!("{label}: refused {error}; want {want}"));
            }
            DeltaExpect::Divergence { want, spark } => {
                failures.push(format!(
                    "{label}: refused {error}; want divergent rows {want} (Spark answers {spark})"
                ));
            }
            DeltaExpect::PreparedWrite => {
                failures.push(format!("{label}: refused with wrong arm {error}"));
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn check_delta_insert(
    doors: &Doors,
    door: Door,
    table: &str,
    label: &str,
    failures: &mut Vec<String>,
    outcome: Result<Vec<RecordBatch>, String>,
    expect: &DeltaExpect,
    before: &State,
) {
    match outcome {
        Ok(_) => match expect {
            DeltaExpect::Answer(want) => {
                let after = observe(doors, door, table).await;
                if after.rows != *want {
                    failures.push(format!("{label}: rows {} != {want}", after.rows));
                }
                if !want.is_empty() && after.snapshot == before.snapshot {
                    failures.push(format!("{label}: snapshot unchanged"));
                }
            }
            DeltaExpect::Divergence { want, spark } => {
                let after = observe(doors, door, table).await;
                if after.rows != *want {
                    failures.push(format!(
                        "{label}: rows {} != {want}; Spark answers {spark}; the cause is the explicit user cast, as the bare SELECT CAST shows",
                        after.rows
                    ));
                }
                if after.snapshot == before.snapshot {
                    failures.push(format!("{label}: snapshot unchanged"));
                }
            }
            DeltaExpect::Refuse { .. }
            | DeltaExpect::PreparedWrite
            | DeltaExpect::PrepareAnswer(_) => {
                let after = observe(doors, door, table).await;
                failures.push(format!("{label}: answered {}", after.rows));
            }
        },
        Err(error) => {
            let after = observe(doors, door, table).await;
            if after.rows != before.rows {
                failures.push(format!("{label}: rows changed {}", after.rows));
            }
            if after.snapshot != before.snapshot {
                failures.push(format!("{label}: snapshot changed"));
            }
            match expect {
                DeltaExpect::Refuse {
                    word,
                    path,
                    right,
                    left,
                } => {
                    check_refusal_text(label, failures, &error, word, path, right, left);
                }
                DeltaExpect::Answer(want) | DeltaExpect::PrepareAnswer(want) => {
                    failures.push(format!("{label}: refused {error}; want {want}"));
                }
                DeltaExpect::Divergence { want, spark } => {
                    failures.push(format!(
                        "{label}: refused {error}; want divergent rows {want} (Spark answers {spark})"
                    ));
                }
                DeltaExpect::PreparedWrite => {
                    failures.push(format!("{label}: refused with wrong arm {error}"));
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delta2_union_rows_refuse_or_answer_on_native_and_ansi() {
    let doors = open_doors().await;
    let mut failures = Vec::new();
    for door in DELTA_DOORS {
        for row in DELTA_UNION_ROWS {
            let select = delta_union_select(row.id);
            let label = delta_name(door, row.id, Form::Read);
            let outcome = attempt(session_of(&doors, door), door, &select).await;
            check_delta_read(&label, &mut failures, outcome, &row.expect);
            let table = delta_table_key(door, row.id);
            let shape = shape_by_id(row.shape);
            create_shaped_table(&doors, door, &table, shape).await;
            let before = observe(&doors, door, &table).await;
            if !before.rows.is_empty() {
                failures.push(format!("{label}: table not empty {}", before.rows));
                continue;
            }
            let insert = format!("INSERT INTO ice.ns.{table} {select}");
            let outcome = attempt(session_of(&doors, door), door, &insert).await;
            let label = delta_name(door, row.id, Form::Insert);
            check_delta_insert(
                &doors,
                door,
                &table,
                &label,
                &mut failures,
                outcome,
                &row.expect,
                &before,
            )
            .await;
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn delta2_view_reads_refuse_or_answer_on_native_and_ansi() {
    let doors = open_doors().await;
    let mut failures = Vec::new();
    for door in DELTA_DOORS {
        for row in DELTA_VIEW_ROWS {
            let view = format!(
                "d2v_{}_{}",
                row.id.replace('-', "_"),
                delta_door_token(door)
            );
            let create = format!("{} {view} AS {}", row.verb, delta_view_body(row.id));
            let label = delta_name(door, row.id, Form::Read);
            if let Err(error) = attempt(session_of(&doors, door), door, &create).await {
                failures.push(format!("{label}: create refused {error}"));
                continue;
            }
            let read = format!("SELECT * FROM {view}");
            let outcome = attempt(session_of(&doors, door), door, &read).await;
            check_delta_read(&label, &mut failures, outcome, &row.expect);
            let table = delta_table_key(door, row.id);
            let shape = shape_by_id("reordered");
            create_shaped_table(&doors, door, &table, shape).await;
            let before = observe(&doors, door, &table).await;
            if !before.rows.is_empty() {
                failures.push(format!("{label}: table not empty {}", before.rows));
                continue;
            }
            let insert = format!("INSERT INTO ice.ns.{table} SELECT * FROM {view}");
            let outcome = attempt(session_of(&doors, door), door, &insert).await;
            let label = delta_name(door, row.id, Form::Insert);
            check_delta_insert(
                &doors,
                door,
                &table,
                &label,
                &mut failures,
                outcome,
                &row.expect,
                &before,
            )
            .await;
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn delta2_prepare_rows_refuse_or_answer_on_native_and_ansi() {
    let doors = open_doors().await;
    let mut failures = Vec::new();
    for door in DELTA_DOORS {
        for row in DELTA_PREPARE_ROWS {
            let name = format!(
                "d2p_{}_{}",
                delta_door_token(door),
                row.id.replace('-', "_")
            );
            let label = format!("{}-{}-prepare", delta_door_token(door), row.id);
            match &row.expect {
                DeltaExpect::Refuse {
                    word,
                    path,
                    right,
                    left,
                } => {
                    let text = delta_prepare_text(row.id, &name, None);
                    match attempt(session_of(&doors, door), door, &text).await {
                        Ok(_) => failures.push(format!("{label}: prepared")),
                        Err(error) => {
                            check_refusal_text(
                                &label,
                                &mut failures,
                                &error,
                                word,
                                path,
                                right,
                                left,
                            );
                        }
                    }
                }
                DeltaExpect::PreparedWrite => {
                    let table = delta_table_key(door, row.id);
                    let shape = shape_by_id("reordered");
                    create_shaped_table(&doors, door, &table, shape).await;
                    let before = observe(&doors, door, &table).await;
                    let text = delta_prepare_text(row.id, &name, Some(&table));
                    match attempt(session_of(&doors, door), door, &text).await {
                        Ok(_) => failures.push(format!("{label}: prepared")),
                        Err(error) => {
                            if !error.contains("is refused: its statement writes to a table") {
                                failures.push(format!("{label}: wrong refusal {error}"));
                            }
                            let after = observe(&doors, door, &table).await;
                            if after.rows != before.rows {
                                failures.push(format!("{label}: rows changed {}", after.rows));
                            }
                            if after.snapshot != before.snapshot {
                                failures.push(format!("{label}: snapshot changed"));
                            }
                        }
                    }
                }
                DeltaExpect::PrepareAnswer(want) => {
                    let text = delta_prepare_text(row.id, &name, None);
                    if let Err(error) = attempt(session_of(&doors, door), door, &text).await {
                        failures.push(format!("{label}: prepare refused {error}"));
                        continue;
                    }
                    let execute = format!("EXECUTE {name}");
                    match attempt(session_of(&doors, door), door, &execute).await {
                        Ok(batches) => {
                            let rows = render_rows(&batches);
                            if rows != *want {
                                failures.push(format!("{label}: rows {rows} != {want}"));
                            }
                        }
                        Err(error) => {
                            failures.push(format!("{label}: execute refused {error}"));
                        }
                    }
                }
                DeltaExpect::Answer(_) | DeltaExpect::Divergence { .. } => {
                    failures.push(format!("{label}: miswired row"));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
