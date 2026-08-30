use super::super::*;

use datafusion::datasource::MemTable;
use iceberg::NamespaceIdent;

/// An empty [`PartitionStream`].
#[derive(Debug)]
struct EmptyTargetStream(SchemaRef);

impl PartitionStream for EmptyTargetStream {
    fn schema(&self) -> &SchemaRef {
        &self.0
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.0),
            futures::stream::empty(),
        ))
    }
}

fn spec(matched: Vec<MatchedClause>, not_matched: Vec<InsertClause>) -> MergeSpec {
    MergeSpec {
        target: TableIdent::new(NamespaceIdent::new("sales".to_string()), "t".to_string()),
        target_alias: "t".to_string(),
        source_from_sql: "src".to_string(),
        source_alias: "s".to_string(),
        on_sql: "t.id = s.id".to_string(),
        matched,
        not_matched,
    }
}

fn update(predicate: Option<&str>, sets: &[(&str, &str)]) -> MatchedClause {
    MatchedClause {
        predicate_sql: predicate.map(ToString::to_string),
        action: MatchedAction::Update {
            assignments: sets
                .iter()
                .map(|(c, e)| ((*c).to_string(), (*e).to_string()))
                .collect(),
        },
    }
}

fn delete(predicate: Option<&str>) -> MatchedClause {
    MatchedClause {
        predicate_sql: predicate.map(ToString::to_string),
        action: MatchedAction::Delete,
    }
}

fn arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ])
}

/// Flatten every recorded field so asserts can match message text and `scratch=…`.
struct FieldVisitor(String);

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        // Debug-format of a &str message includes quotes — strip for matching.
        let rendered = format!("{value:?}");
        let rendered = rendered.trim_matches('"');
        if field.name() == "message" {
            self.0.push_str(rendered);
        } else {
            let _ = write!(self.0, "{}={rendered}", field.name());
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        use std::fmt::Write as _;
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        if field.name() == "message" {
            self.0.push_str(value);
        } else {
            let _ = write!(self.0, "{}={value}", field.name());
        }
    }
}

/// Captures `tracing` WARN event messages (target + rendered message body).
struct WarnCapture {
    lines: std::sync::Mutex<Vec<String>>,
}

impl WarnCapture {
    fn new() -> Self {
        Self {
            lines: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl tracing::Subscriber for WarnCapture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::WARN
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() > tracing::Level::WARN {
            return;
        }
        let mut visitor = FieldVisitor(String::new());
        event.record(&mut visitor);
        let line = format!("{}|{}", event.metadata().target(), visitor.0);
        self.lines
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(line);
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// SAF-010 / WU-5: missing scratch → DataFusion `Ok(None)` → **WARN** + `Err` naming the scratch.
#[test]
fn deregister_merge_scratch_warns_when_table_missing() {
    let capture = std::sync::Arc::new(WarnCapture::new());
    let missing = "__repark_merge_target_does_not_exist";
    tracing::subscriber::with_default(capture.clone(), || {
        let ctx = SessionContext::new();
        let err = deregister_merge_scratch(&ctx, missing).expect_err("missing scratch must fail");
        assert!(
            err.to_string().contains(missing)
                && err.to_string().contains("not found at deregister time"),
            "expected plan error naming the scratch, got: {err}"
        );
    });
    let lines = capture.lines();
    assert!(
        lines.iter().any(|line| {
            line.contains("repark_write::merge")
                && line.contains("not registered at deregister time")
        }),
        "expected tracing WARN for Ok(None) cleanup failure, got: {lines:?}"
    );
}

/// Happy path removes a registered scratch; a second call hits the missing-table WARN path.
#[test]
fn deregister_merge_scratch_succeeds_when_registered_then_warns_on_second() {
    let capture = std::sync::Arc::new(WarnCapture::new());
    tracing::subscriber::with_default(capture.clone(), || {
        let ctx = SessionContext::new();
        let schema: SchemaRef = Arc::new(arrow_schema());
        let name = register_streaming_target(
            &ctx,
            Arc::clone(&schema),
            Arc::new(EmptyTargetStream(Arc::clone(&schema))),
        )
        .expect("register empty scratch");
        deregister_merge_scratch(&ctx, &name).expect("registered scratch must deregister");
        let err = deregister_merge_scratch(&ctx, &name).expect_err("second deregister must fail");
        assert!(
            err.to_string().contains(&name),
            "second deregister should name the scratch, got: {err}"
        );
    });
    let lines = capture.lines();
    assert!(
        lines.iter().any(|line| {
            line.contains("repark_write::merge")
                && line.contains("not registered at deregister time")
        }),
        "expected WARN on second deregister (Ok(None)), got: {lines:?}"
    );
}

/// SAF-010: a hard `deregister_table` `Err` emits WARN with the scratch name.
#[test]
fn deregister_merge_scratch_warns_when_deregister_returns_err() {
    let capture = std::sync::Arc::new(WarnCapture::new());
    // Multi-part name with a non-existent catalog → `schema_for_ref` fails with Err.
    let missing_catalog_table = "no_such_catalog.ns.__repark_merge_scratch";
    tracing::subscriber::with_default(capture.clone(), || {
        let ctx = SessionContext::new();
        let err = deregister_merge_scratch(&ctx, missing_catalog_table)
            .expect_err("unknown catalog path must hard-fail deregister");
        assert!(
            !err.to_string().is_empty(),
            "expected a DataFusion error from schema resolution"
        );
    });
    let lines = capture.lines();
    assert!(
        lines.iter().any(|line| {
            line.contains("repark_write::merge")
                && line.contains("failed to deregister MERGE scratch table")
        }),
        "expected tracing WARN for hard deregister Err, got: {lines:?}"
    );
}

/// Clause predicates are 3VL-hardened: a NULL predicate means it does not apply.
#[test]
fn applies_wraps_predicates_in_coalesce() {
    assert_eq!(MergeSql::applies(None), "TRUE");
    assert_eq!(
        MergeSql::applies(Some("s.flag = 1")),
        "COALESCE((s.flag = 1), FALSE)"
    );
    let predicates = [Some("p1"), Some("p2"), None];
    // Scout #18: prior_clauses_do_not_apply is O(C) clause_id CASE, not O(C²) AND-chain.
    let prior = MergeSql::prior_clauses_do_not_apply(&predicates, 2);
    assert!(
        prior.contains("COALESCE((p1), FALSE)") && prior.contains("COALESCE((p2), FALSE)"),
        "prior must still 3VL-harden each predicate, got: {prior}"
    );
    assert!(
        !prior.contains("NOT COALESCE"),
        "scout #18 drops O(C²) NOT-applies AND-chains, got: {prior}"
    );
    assert!(
        prior.contains(">= 2") || prior.contains("IS NULL"),
        "prior must be clause_id IS NULL OR clause_id >= index, got: {prior}"
    );
    let clause_id = MergeSql::clause_id_case(&predicates);
    assert_eq!(
        clause_id,
        "CASE WHEN COALESCE((p1), FALSE) THEN 0 WHEN COALESCE((p2), FALSE) THEN 1 WHEN TRUE THEN 2 END"
    );
}

/// First-match-wins: rewrite CASE is driven by a single `matched_clause_id` (scout #18).
#[test]
fn rewrite_case_encodes_clause_order() {
    let spec = spec(
        vec![
            update(Some("s.op = 'a'"), &[("name", "s.name")]),
            update(None, &[("id", "s.id + 1")]),
        ],
        vec![],
    );
    let sql = MergeSql {
        spec: &spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
    };
    let name_case = sql.rewrite_column("name");
    // Clause 0 sets name; clause 1 does not — exactly one value branch, ELSE keeps original.
    assert!(
        name_case.contains("WHEN 0 THEN (s.name)"),
        "name branch must key on clause_id 0, got: {name_case}"
    );
    assert!(name_case.contains("ELSE t.\"name\""));
    assert!(
        name_case.contains("COALESCE((s.op = 'a'), FALSE)"),
        "clause_id CASE must still 3VL-harden clause 0 predicate, got: {name_case}"
    );
    let id_case = sql.rewrite_column("id");
    // Clause 1 sets id; first-match via clause_id (clause 0's predicate appears in the id CASE).
    assert!(
        id_case.contains("WHEN 1 THEN (s.id + 1)"),
        "id branch must key on clause_id 1, got: {id_case}"
    );
    assert!(
        id_case.contains("COALESCE((s.op = 'a'), FALSE)"),
        "clause_id must encode first-match against earlier clause, got: {id_case}"
    );
    assert!(
        !id_case.contains("NOT COALESCE"),
        "scout #18: no O(C²) prefix-negation AND-chain, got: {id_case}"
    );
}

/// DELETE clauses claim a `clause_id` slot so later UPDATE branches fire only for their own id.
#[test]
fn delete_clause_shapes_filter_not_projection() {
    let spec = spec(
        vec![
            delete(Some("s.op = 'd'")),
            update(None, &[("name", "s.name")]),
        ],
        vec![],
    );
    let sql = MergeSql {
        spec: &spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
    };
    let deleted = sql.delete_applies();
    assert!(
        deleted.contains("COALESCE((s.op = 'd'), FALSE)"),
        "delete_applies must 3VL-harden the DELETE predicate via clause_id, got: {deleted}"
    );
    assert!(
        deleted.contains("= 0") || deleted.contains("=0"),
        "delete is matched clause index 0, got: {deleted}"
    );
    let name_case = sql.rewrite_column("name");
    // Update is clause index 1 — only that id rewrites name (DELETE claimed 0).
    assert!(
        name_case.contains("WHEN 1 THEN (s.name)"),
        "update branch must key on clause_id 1 (after DELETE at 0), got: {name_case}"
    );
    assert!(
        name_case.contains("COALESCE((s.op = 'd'), FALSE)"),
        "clause_id CASE must still encode the earlier DELETE predicate, got: {name_case}"
    );
}

/// M11: skip only a lone unconditional MATCHED DELETE — every other matched shape stays checked.
#[test]
fn skip_cardinality_only_lone_unconditional_delete() {
    assert!(skip_cardinality(&spec(vec![delete(None)], vec![])));
    assert!(!skip_cardinality(&spec(
        vec![delete(Some("t.id > 0"))],
        vec![]
    )));
    assert!(!skip_cardinality(&spec(
        vec![update(None, &[("name", "s.name")])],
        vec![],
    )));
    assert!(!skip_cardinality(&spec(vec![], vec![])));
    assert!(!skip_cardinality(&spec(
        vec![delete(None), update(None, &[("name", "s.name")])],
        vec![],
    )));
}

/// R5: null Stage-B flag columns fail loud (mutation pin).
#[test]
fn consume_matched_work_batch_rejects_null_flag_columns() {
    let write_schema = Arc::new(arrow_schema());
    // Columns: _file, _pos, match_count, is_mutated, is_update, id, name
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("_file", DataType::Utf8, true),
            Field::new("_pos", DataType::Int64, true),
            Field::new("match_count", DataType::Int64, true),
            Field::new("is_mutated", DataType::Int64, true),
            Field::new("is_update", DataType::Int64, true),
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec![Some("f.parquet")])),
            Arc::new(Int64Array::from(vec![Some(0)])),
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![None::<i64>])), // null is_mutated
            Arc::new(Int64Array::from(vec![Some(0)])),
            Arc::new(datafusion::arrow::array::Int32Array::from(vec![Some(1)])),
            Arc::new(StringArray::from(vec![Some("n")])),
        ],
    )
    .expect("synthetic matched_work batch");
    let mut path_intern = HashMap::new();
    let mut unique_paths = Vec::new();
    let mut seen_pair = HashSet::new();
    let mut pair_indices = Vec::new();
    let mut update_batches = Vec::new();
    let err = consume_matched_work_batch(
        &batch,
        &write_schema,
        write_schema.fields().len(),
        &mut path_intern,
        &mut unique_paths,
        &mut seen_pair,
        &mut pair_indices,
        &mut update_batches,
        false,
    )
    .expect_err("null is_mutated must fail");
    assert!(
        err.to_string().contains("NULL is_mutated"),
        "must name is_mutated, got: {err}"
    );
}

/// P2a: Stage B intern shares one `Arc<str>` across pairs for the same path.
#[test]
fn stage_b_path_intern_shares_arc_across_pairs() {
    let write_schema = Arc::new(arrow_schema());
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("_file", DataType::Utf8, true),
            Field::new("_pos", DataType::Int64, true),
            Field::new("match_count", DataType::Int64, true),
            Field::new("is_mutated", DataType::Int64, true),
            Field::new("is_update", DataType::Int64, true),
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec![
                Some("s3://b/f0.parquet"),
                Some("s3://b/f0.parquet"),
                Some("s3://b/f1.parquet"),
            ])),
            Arc::new(Int64Array::from(vec![Some(0), Some(1), Some(0)])),
            Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(1)])),
            Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(1)])),
            Arc::new(Int64Array::from(vec![Some(0), Some(0), Some(0)])),
            Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
            ])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
        ],
    )
    .expect("synthetic matched_work batch");
    let mut path_intern = HashMap::new();
    let mut unique_paths: Vec<Arc<str>> = Vec::new();
    let mut seen_pair = HashSet::new();
    let mut pair_indices = Vec::new();
    let mut update_batches = Vec::new();
    consume_matched_work_batch(
        &batch,
        &write_schema,
        write_schema.fields().len(),
        &mut path_intern,
        &mut unique_paths,
        &mut seen_pair,
        &mut pair_indices,
        &mut update_batches,
        false,
    )
    .expect("consume");
    assert_eq!(unique_paths.len(), 2, "two distinct paths interned");
    assert_eq!(pair_indices.len(), 3);
    let pairs: Vec<crate::write::position_delete::PositionDeletePair> = pair_indices
        .into_iter()
        .map(|(path_index, pos)| (Arc::clone(&unique_paths[path_index]), pos))
        .collect();
    assert!(
        Arc::ptr_eq(&pairs[0].0, &pairs[1].0),
        "same path must share Arc for pos 0 and pos 1"
    );
    assert!(!Arc::ptr_eq(&pairs[0].0, &pairs[2].0));
    // unique_paths[0] + two pair clones still point at the same allocation.
    assert_eq!(Arc::strong_count(&unique_paths[0]), 3);
    assert_eq!(Arc::strong_count(&unique_paths[1]), 2);
}

/// Shared null-flag helper fails loud (Stage A + Stage B).
#[test]
fn require_non_null_i64_rejects_null() {
    let array = Int64Array::from(vec![Some(1), None]);
    assert_eq!(require_non_null_i64(&array, 0, "match_count").unwrap(), 1);
    let err = require_non_null_i64(&array, 1, "match_count").unwrap_err();
    assert!(err.to_string().contains("NULL match_count"), "got: {err}");
}

/// `insert_sql` encodes NOT MATCHED first-match via `clause_id`, not O(C²) `NOT applies` chains.
#[test]
fn insert_sql_uses_clause_id_not_oc2_prior_chain() {
    let merge_spec = spec(
        vec![],
        vec![
            InsertClause {
                predicate_sql: Some("s.flag = 1".to_string()),
                action: InsertAction::Explicit {
                    columns: vec!["id".to_string(), "name".to_string()],
                    values_sql: vec!["s.id".to_string(), "'a'".to_string()],
                },
            },
            InsertClause {
                predicate_sql: None,
                action: InsertAction::Explicit {
                    columns: vec!["id".to_string(), "name".to_string()],
                    values_sql: vec!["s.id".to_string(), "'b'".to_string()],
                },
            },
        ],
    );
    let sql = MergeSql {
        spec: &merge_spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
    };
    let schema = arrow_schema();
    let insert0 = sql.insert_sql(0, &schema).expect("insert 0");
    let insert1 = sql.insert_sql(1, &schema).expect("insert 1");
    for (index, text) in [(0, &insert0), (1, &insert1)] {
        assert!(
            text.contains("COALESCE((s.flag = 1), FALSE)"),
            "insert {index} must 3VL-harden clause0 predicate, got: {text}"
        );
        assert!(
            text.contains(&format!(") = {index}")) || text.contains(&format!(")={index}")),
            "insert {index} must key on clause_id = {index}, got: {text}"
        );
        assert!(
            !text.contains("NOT COALESCE"),
            "insert {index} must not emit O(C²) NOT-applies chains, got: {text}"
        );
    }
}

/// Scout #18: allowlisted rewrite omits `_file IN (...)`; path semi-join uses INNER JOIN.
#[test]
fn rewrite_sql_drops_in_list_when_allowlisted_else_path_semijoin() {
    let merge_spec = spec(vec![update(None, &[("name", "s.name")])], vec![]);
    let sql = MergeSql {
        spec: &merge_spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
    };
    let schema = arrow_schema();
    let allowlisted = sql.rewrite_sql_allowlisted("scoped_target", &schema);
    // Note: do not search bare `IN (` — `JOIN (` contains that substring.
    assert!(
        !allowlisted.contains("\"_file\" IN (") && !allowlisted.contains("_file IN ("),
        "allowlisted rewrite must not embed path IN list, got: {allowlisted}"
    );
    assert!(
        allowlisted.contains("FROM \"scoped_target\" AS t"),
        "allowlisted rewrite must FROM the scoped target, got: {allowlisted}"
    );
    assert!(
        allowlisted.contains("WHERE NOT ("),
        "allowlisted rewrite must still filter deletes, got: {allowlisted}"
    );

    let semi = sql.rewrite_sql_path_semijoin("scratch", "aff_paths", &schema);
    assert!(
        semi.contains("INNER JOIN \"aff_paths\" AS __repark_aff"),
        "else-path must semi-join path MemTable, got: {semi}"
    );
    assert!(
        semi.contains("t.\"_file\" = __repark_aff.\"path\""),
        "semi-join key must be _file = path, got: {semi}"
    );
    assert!(
        !semi.contains("\"_file\" IN (") && !semi.contains("_file IN ("),
        "path semi-join must not also embed path IN list, got: {semi}"
    );
}

/// Scout #18 generation-time microbench: 20 matched UPDATE clauses × 100 columns.
#[test]
fn rewrite_projection_20_clauses_100_cols_generation_time() {
    const CLAUSE_COUNT: usize = 20;
    const COL_COUNT: usize = 100;
    let mut matched = Vec::with_capacity(CLAUSE_COUNT);
    for clause_index in 0..CLAUSE_COUNT {
        let mut assignments = Vec::with_capacity(COL_COUNT);
        for col_index in 0..COL_COUNT {
            assignments.push((
                format!("c{col_index:03}"),
                format!("s.c{col_index:03} + {clause_index}"),
            ));
        }
        matched.push(MatchedClause {
            predicate_sql: Some(format!("s.bucket = {clause_index}")),
            action: MatchedAction::Update { assignments },
        });
    }
    let merge_spec = spec(matched, vec![]);
    let sql = MergeSql {
        spec: &merge_spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
    };
    let fields: Vec<Field> = (0..COL_COUNT)
        .map(|col_index| Field::new(format!("c{col_index:03}"), DataType::Int32, true))
        .collect();
    let schema = ArrowSchema::new(fields);

    // --- residual O(C²) generator (pre-#18 shape) for before numbers -----------------------.
    let before_started = std::time::Instant::now();
    let before_projection = legacy_oc2_rewrite_projection(&sql, &schema);
    let before_elapsed = before_started.elapsed();
    assert!(
        before_projection.contains("NOT COALESCE"),
        "legacy generator must emit O(C²) NOT-applies chains"
    );

    // --- scout #18 O(C) path --------------------------------------------------------------.
    let started = std::time::Instant::now();
    let projection = sql.rewrite_projection(&schema);
    let elapsed = started.elapsed();
    // Shape pins (correctness of the O(C) encoding — not a flaky wall budget).
    assert!(
        !projection.contains("NOT COALESCE"),
        "20×100 projection must not emit O(C²) NOT-applies chains"
    );
    assert!(
        projection.contains("WHEN 0 THEN") && projection.contains("WHEN 19 THEN"),
        "projection must cover first and last clause ids"
    );
    // clause_id CASE appears (first-match arms use applies-wrapped predicates).
    assert!(
        projection.contains("COALESCE((s.bucket = 0), FALSE)"),
        "clause_id must 3VL-harden clause 0, got prefix: {}",
        &projection[..projection.len().min(200)]
    );
    // Generation must be well under a second on any reasonable host (O(C×cols) text).
    assert!(
        elapsed.as_secs() < 2,
        "20×100 rewrite_projection took {elapsed:?} — expected sub-second generation"
    );
    // O(C) SQL must be strictly smaller than O(C²) text for C=20.
    assert!(
        projection.len() < before_projection.len(),
        "clause_id SQL ({} B) must be smaller than O(C²) legacy ({} B)",
        projection.len(),
        before_projection.len()
    );
    // Record wall for ledger before/after (eprintln so `cargo test -- --nocapture` shows it).
    eprintln!(
        "p1b_scout18_gen_bench: clauses={CLAUSE_COUNT} cols={COL_COUNT} \
         before_wall_us={} before_sql_bytes={} \
         after_wall_us={} after_sql_bytes={}",
        before_elapsed.as_micros(),
        before_projection.len(),
        elapsed.as_micros(),
        projection.len()
    );
}

/// Pre-#18 O(C²) rewrite projection — generation-time twin for the microbench only.
fn legacy_oc2_rewrite_projection(sql: &MergeSql<'_>, write_schema: &ArrowSchema) -> String {
    write_schema
        .fields()
        .iter()
        .map(|field| {
            let column = field.name();
            let ta = &sql.spec.target_alias;
            let quoted = quote_ident(column);
            let original = format!("{ta}.{quoted}");
            let predicates = sql.matched_predicates();
            let branches: Vec<String> = sql
                .spec
                .matched
                .iter()
                .enumerate()
                .filter_map(|(index, clause)| {
                    let MatchedAction::Update { assignments } = &clause.action else {
                        return None;
                    };
                    let (_, expr) = assignments
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(column))?;
                    let prior = MergeSql::prior_clauses_do_not_apply_legacy(&predicates, index);
                    let condition = format!(
                        "{} AND {} AND {}",
                        sql.matched(),
                        prior,
                        MergeSql::applies(predicates[index])
                    );
                    Some(format!("WHEN {condition} THEN ({expr})"))
                })
                .collect();
            if branches.is_empty() {
                format!("{original} AS {quoted}")
            } else {
                format!(
                    "CASE {} ELSE {original} END AS {quoted}",
                    branches.join(" ")
                )
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// PIN.
#[test]
fn merge_sql_keys_identity_on_file_and_pos() {
    let spec = spec(
        vec![update(None, &[("name", "s.name")])],
        vec![insert(&["id", "name"], &["s.id", "s.name"])],
    );
    let sql = MergeSql {
        spec: &spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
    };

    // Stage A live SQL (QUAL-08 deleted residual cardinality_sql / affected_files_sql).
    let discovery = sql.match_discovery_sql();
    assert!(
        discovery.contains("GROUP BY t.\"_file\", t.\"_pos\""),
        "match discovery must group on the (_file, _pos) identity, got: {discovery}"
    );
    // INNER JOIN puts SOURCE first so it is the hash build side and the target is the probe.
    assert!(
        discovery.contains(") AS s JOIN \"scratch\" AS t"),
        "must build the join on the source (source JOIN target), got: {discovery}"
    );
    let insert = sql.insert_sql(0, &arrow_schema()).unwrap();
    // Audit M4: the anti-join `_pos` rides through the source-only scope as a sentinel alias.
    assert!(
        insert.contains("t.\"_pos\" AS \"__repark_not_matched_pos\"")
            && insert.contains("WHERE \"__repark_not_matched_pos\" IS NULL"),
        "insert anti-join must key on the sentinel `_pos` copy inside the source-only scope, \
         got: {insert}"
    );
    for query in [&discovery, &insert] {
        assert!(
            !query.contains("__repark_row_id"),
            "no generated query may reference the retired __repark_row_id, got: {query}"
        );
    }
}

fn insert(columns: &[&str], values: &[&str]) -> InsertClause {
    InsertClause {
        predicate_sql: None,
        action: InsertAction::Explicit {
            columns: columns.iter().map(ToString::to_string).collect(),
            values_sql: values.iter().map(ToString::to_string).collect(),
        },
    }
}

/// Insert projection: named columns take VALUES; unnamed nullable become NULL, required reject.
#[test]
fn insert_projection_validates_columns() {
    let schema = arrow_schema();
    assert_eq!(
        insert_projection(&insert(&["id", "name"], &["s.id", "s.name"]), &schema).unwrap(),
        "(s.id) AS \"id\", (s.name) AS \"name\""
    );

    assert_eq!(
        insert_projection(&insert(&["id"], &["s.id"]), &schema).unwrap(),
        "(s.id) AS \"id\", NULL AS \"name\""
    );

    let err = insert_projection(&insert(&["name"], &["s.name"]), &schema).unwrap_err();
    assert!(err.to_string().contains("required column `id`"));

    let err = insert_projection(&insert(&["nope"], &["1"]), &schema).unwrap_err();
    assert!(err.to_string().contains("`nope` does not exist"));

    let err = insert_projection(&insert(&["id", "name"], &["s.id"]), &schema).unwrap_err();
    assert!(err.to_string().contains("2 columns but 1 VALUES"));
}

/// A column named twice in the INSERT list is an error, never a silent last-value-wins.
#[test]
fn insert_projection_rejects_duplicate_columns() {
    let schema = arrow_schema();
    let err = insert_projection(&insert(&["id", "id"], &["1", "2"]), &schema).unwrap_err();
    assert!(err.to_string().contains("more than once"));
}

/// Positional insert (no column list) maps VALUES onto the schema order.
#[test]
fn insert_projection_positional() {
    let schema = arrow_schema();
    assert_eq!(
        insert_projection(&insert(&[], &["s.id", "s.name"]), &schema).unwrap(),
        "(s.id) AS \"id\", (s.name) AS \"name\""
    );
}

/// An unexpanded `INSERT *` marker reaching SQL generation is an executor bug.
#[test]
fn insert_projection_rejects_unexpanded_star() {
    let schema = arrow_schema();
    let star = InsertClause {
        predicate_sql: None,
        action: InsertAction::All,
    };
    let err = insert_projection(&star, &schema).unwrap_err();
    assert!(err.to_string().contains("unexpanded"));
}

/// UPDATE validation runs on the expanded spec, so a leftover `UpdateAll` is an internal error.
#[test]
fn validate_update_columns_rejects_unexpanded_star() {
    let schema = arrow_schema();
    let star = spec(
        vec![MatchedClause {
            predicate_sql: None,
            action: MatchedAction::UpdateAll,
        }],
        vec![],
    );
    let err = validate_update_columns(&star, &schema).unwrap_err();
    assert!(err.to_string().contains("unexpanded"));
}

/// Audit BUG-006: `UPDATE SET` / `INSERT` column names resolve case-insensitively.
#[test]
fn validate_update_columns_case_insensitive() {
    let schema = arrow_schema();
    let ok = spec(
        vec![MatchedClause {
            predicate_sql: None,
            action: MatchedAction::Update {
                assignments: vec![("Name".to_string(), "s.name".to_string())],
            },
        }],
        vec![],
    );
    validate_update_columns(&ok, &schema).expect("Name must resolve to name");

    let bad = spec(
        vec![MatchedClause {
            predicate_sql: None,
            action: MatchedAction::Update {
                assignments: vec![("nope".to_string(), "1".to_string())],
            },
        }],
        vec![],
    );
    let err = validate_update_columns(&bad, &schema).unwrap_err();
    assert!(err.to_string().contains("does not exist"));
}

/// Critic-1 Q-003 / Critic-2 SAF-001: case-differing duplicate SET keys must not first-win.
#[test]
fn validate_update_columns_rejects_case_insensitive_duplicates() {
    let schema = arrow_schema();
    let dup = spec(
        vec![MatchedClause {
            predicate_sql: None,
            action: MatchedAction::Update {
                assignments: vec![
                    ("name".to_string(), "'a'".to_string()),
                    ("NAME".to_string(), "'b'".to_string()),
                ],
            },
        }],
        vec![],
    );
    let err = validate_update_columns(&dup, &schema).unwrap_err();
    assert!(
        err.to_string().contains("more than once"),
        "duplicate case-fold SET must fail, got {err}"
    );
}

/// Audit BUG-006: `rewrite_column` matches assignment keys case-insensitively.
#[test]
fn rewrite_column_case_insensitive_assignment() {
    let merge_spec = spec(
        vec![MatchedClause {
            predicate_sql: None,
            action: MatchedAction::Update {
                assignments: vec![("NAME".to_string(), "'x'".to_string())],
            },
        }],
        vec![],
    );
    let sql = MergeSql {
        spec: &merge_spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
    };
    let rewritten = sql.rewrite_column("name");
    assert!(
        rewritten.contains("'x'"),
        "case-insensitive SET must apply, got {rewritten}"
    );
}

/// Audit BUG-006: INSERT column list resolves case-insensitively to schema field names.
#[test]
fn insert_projection_case_insensitive_columns() {
    let schema = arrow_schema();
    assert_eq!(
        insert_projection(&insert(&["ID", "Name"], &["s.id", "s.name"]), &schema).unwrap(),
        "(s.id) AS \"id\", (s.name) AS \"name\""
    );
    let err = insert_projection(&insert(&["id", "ID"], &["1", "2"]), &schema).unwrap_err();
    assert!(
        err.to_string().contains("more than once"),
        "case-differing duplicate must fail, got {err}"
    );
}

/// Star expansion.
#[tokio::test]
async fn expand_star_clauses_resolves_by_name() {
    let ctx = SessionContext::new();
    let source_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("extra", DataType::Int32, true),
    ]));
    let source = MemTable::try_new(source_schema, vec![vec![]]).unwrap();
    ctx.register_table("src", Arc::new(source)).unwrap();

    let star = spec(
        vec![MatchedClause {
            predicate_sql: None,
            action: MatchedAction::UpdateAll,
        }],
        vec![InsertClause {
            predicate_sql: None,
            action: InsertAction::All,
        }],
    );
    let target_schema = arrow_schema();
    let expanded = expand_star_clauses(&ctx, &star, &target_schema)
        .await
        .unwrap();
    let MatchedAction::Update { assignments } = &expanded.matched[0].action else {
        panic!("expected an expanded UPDATE clause");
    };
    assert_eq!(
        assignments,
        &[
            ("id".to_string(), "s.\"id\"".to_string()),
            ("name".to_string(), "s.\"name\"".to_string()),
        ]
    );
    let InsertAction::Explicit {
        columns,
        values_sql,
    } = &expanded.not_matched[0].action
    else {
        panic!("expected an expanded INSERT clause");
    };
    assert_eq!(columns, &["id", "name"]);
    assert_eq!(values_sql, &["s.\"id\"", "s.\"name\""]);

    let plain = spec(vec![delete(None)], vec![]);
    let untouched = expand_star_clauses(&ctx, &plain, &target_schema)
        .await
        .unwrap();
    assert!(matches!(untouched, Cow::Borrowed(_)));
}

/// A target column the source cannot provide is an up-front error naming the column.
#[tokio::test]
async fn expand_star_clauses_errors_on_missing_source_column() {
    let ctx = SessionContext::new();
    let source_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let source = MemTable::try_new(source_schema, vec![vec![]]).unwrap();
    ctx.register_table("src", Arc::new(source)).unwrap();

    let star = spec(
        vec![MatchedClause {
            predicate_sql: None,
            action: MatchedAction::UpdateAll,
        }],
        vec![],
    );
    let err = expand_star_clauses(&ctx, &star, &arrow_schema())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("missing from the source: `name`"));
}

/// PIN PL-5.
#[tokio::test]
async fn expand_star_clauses_resolves_source_case_insensitively() {
    let ctx = SessionContext::new();
    let source_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new("NAME", DataType::Utf8, true),
        Field::new("EXTRA", DataType::Int32, true),
    ]));
    let source = MemTable::try_new(source_schema, vec![vec![]]).unwrap();
    ctx.register_table("src", Arc::new(source)).unwrap();

    let star = spec(
        vec![MatchedClause {
            predicate_sql: None,
            action: MatchedAction::UpdateAll,
        }],
        vec![InsertClause {
            predicate_sql: None,
            action: InsertAction::All,
        }],
    );
    let expanded = expand_star_clauses(&ctx, &star, &arrow_schema())
        .await
        .unwrap();
    let MatchedAction::Update { assignments } = &expanded.matched[0].action else {
        panic!("expected an expanded UPDATE clause");
    };
    assert_eq!(
        assignments,
        &[
            ("id".to_string(), "s.\"ID\"".to_string()),
            ("name".to_string(), "s.\"NAME\"".to_string()),
        ],
        "target keeps its own name; the value binds to the actual (uppercase) source column"
    );
    let InsertAction::Explicit {
        columns,
        values_sql,
    } = &expanded.not_matched[0].action
    else {
        panic!("expected an expanded INSERT clause");
    };
    assert_eq!(columns, &["id", "name"]);
    assert_eq!(values_sql, &["s.\"ID\"", "s.\"NAME\""]);
}

/// PIN PL-6 — two source columns colliding on one target is a loud AMBIGUOUS error naming both.
#[tokio::test]
async fn expand_star_clauses_rejects_case_ambiguous_source() {
    let ctx = SessionContext::new();
    let source_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("ID", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let source = MemTable::try_new(source_schema, vec![vec![]]).unwrap();
    ctx.register_table("src", Arc::new(source)).unwrap();

    let star = spec(
        vec![MatchedClause {
            predicate_sql: None,
            action: MatchedAction::UpdateAll,
        }],
        vec![],
    );
    let err = expand_star_clauses(&ctx, &star, &arrow_schema())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("`id` is ambiguous") && err.to_string().contains("`id`, `ID`"),
        "the error must name the ambiguous target and both colliding source columns, got: {err}"
    );
}

/// String literals in the affected-file `IN` list survive embedded quotes.
#[test]
fn sql_literal_escapes_quotes() {
    assert_eq!(sql_literal("a'b"), "'a''b'");
    assert_eq!(sql_literal("plain"), "'plain'");
}

/// Schema-derived names keep embedded double quotes from breaking generated-SQL identifiers.
#[test]
fn generated_sql_quotes_identifiers() {
    assert_eq!(quote_ident("plain"), "\"plain\"");
    assert_eq!(quote_ident("na\"me"), "\"na\"\"me\"");

    let weird = ArrowSchema::new(vec![Field::new("na\"me", DataType::Utf8, true)]);
    let projected = insert_projection(&insert(&["na\"me"], &["s.x"]), &weird).unwrap();
    assert_eq!(projected, "(s.x) AS \"na\"\"me\"");

    let plain_spec = spec(vec![], vec![]);
    let sql = MergeSql {
        spec: &plain_spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
    };
    assert_eq!(
        sql.rewrite_column("na\"me"),
        "t.\"na\"\"me\" AS \"na\"\"me\""
    );
}

// === idents ===
/// MERGE `quote_ident` joins the shared Spark/DF injection-probe battery (`idents::probes`).
#[test]
fn qi1_merge_quote_ident_joins_spark_injection_battery() {
    for probe in crate::write::idents::probes::SPARK_INJECTION_PROBES {
        let via_merge = quote_ident(probe);
        let via_ssot = crate::write::idents::quote_ident_spark(probe);
        assert_eq!(via_merge, via_ssot, "merge must delegate to idents SSOT");
        // Independent oracle — not undouble-only.
        let expected = format!("\"{}\"", probe.replace('"', "\"\""));
        assert_eq!(via_merge, expected, "under-quote residual for {probe:?}");
    }
}
