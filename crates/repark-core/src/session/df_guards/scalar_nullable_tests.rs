use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{DFSchema, Result, ScalarValue, Spans, exec_err};
use datafusion::datasource::MemTable;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::logical_expr::expr::{AggregateFunction, Alias, ScalarFunction, Sort, Unnest};
use datafusion::logical_expr::logical_plan::{
    Aggregate, Distinct, EmptyRelation, Filter, Limit, Projection, Sort as SortPlan, Subquery,
    SubqueryAlias, Union, Values,
};
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, GroupingSet, LogicalPlan, LogicalPlanBuilder, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, col, lit,
};
use datafusion::optimizer::{AnalyzerRule, OptimizerContext, OptimizerRule};

use super::{
    ANALYZER_RULE_NAME, ReparkScalarSubqueryNullability, SCALAR_NULLABLE_NAME, SPARK_NONNULL_NAME,
    StripScalarNullable, guaranteed_at_least_one_row, insert_before_coercion,
    scalar_subquery_needs_wrap, udf, wrap_plan, wrap_subquery_output,
};
use crate::ReparkSession;

#[derive(Debug, PartialEq, Eq, Hash)]
struct StubUdf {
    name: &'static str,
    nullable: bool,
    signature: Signature,
}

impl StubUdf {
    fn new(name: &'static str, nullable: bool) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            name,
            nullable,
            signature: Signature::any(1, Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for StubUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Int32))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_type = args
            .arg_fields
            .first()
            .map_or(DataType::Int32, |field| field.data_type().clone());
        Ok(Arc::new(Field::new(self.name, data_type, self.nullable)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("stub udf invoked")
    }
}

fn one_row_plan() -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: true,
        schema: Arc::new(DFSchema::empty()),
    })
}

fn zero_row_plan() -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    })
}

fn count_star_expr() -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        count_udaf(),
        vec![lit(1_i64)],
        false,
        None,
        vec![],
        None,
    ))
}

fn nonnull_scan_plan() -> LogicalPlan {
    let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
    let table = MemTable::try_new(schema, vec![vec![]]).unwrap();
    LogicalPlanBuilder::scan(
        "empty_nn",
        datafusion::datasource::provider_as_source(Arc::new(table)),
        None,
    )
    .unwrap()
    .build()
    .unwrap()
}

fn field_id_scan_plan(name: &str, data_type: DataType, nullable: bool) -> LogicalPlan {
    let mut metadata = HashMap::new();
    metadata.insert("PARQUET:field_id".to_string(), "2".to_string());
    let field = Field::new(name, data_type, nullable).with_metadata(metadata);
    let schema = Arc::new(Schema::new(vec![field]));
    let table = MemTable::try_new(schema, vec![vec![]]).unwrap();
    LogicalPlanBuilder::scan(
        "ice_tbl",
        datafusion::datasource::provider_as_source(Arc::new(table)),
        None,
    )
    .unwrap()
    .build()
    .unwrap()
}

#[test]
fn row_guarantee_empty_relations_and_values() {
    assert!(guaranteed_at_least_one_row(&one_row_plan()));
    assert!(!guaranteed_at_least_one_row(&zero_row_plan()));
    let non_empty_values = LogicalPlan::Values(Values {
        schema: Arc::new(
            DFSchema::from_unqualified_fields(
                vec![Field::new("c", DataType::Int32, false)].into(),
                HashMap::new(),
            )
            .unwrap(),
        ),
        values: vec![vec![lit(1_i32)]],
    });
    assert!(guaranteed_at_least_one_row(&non_empty_values));
    let empty_values = LogicalPlan::Values(Values {
        schema: Arc::new(
            DFSchema::from_unqualified_fields(
                vec![Field::new("c", DataType::Int32, false)].into(),
                HashMap::new(),
            )
            .unwrap(),
        ),
        values: vec![],
    });
    assert!(!guaranteed_at_least_one_row(&empty_values));
    assert!(!guaranteed_at_least_one_row(&nonnull_scan_plan()));
}

#[test]
fn row_guarantee_aggregates_and_grouping_sets() {
    let scan = Arc::new(nonnull_scan_plan());
    let ungrouped = LogicalPlan::Aggregate(
        Aggregate::try_new(Arc::clone(&scan), vec![], vec![count_star_expr()]).unwrap(),
    );
    assert!(guaranteed_at_least_one_row(&ungrouped));
    let grouped = LogicalPlan::Aggregate(
        Aggregate::try_new(Arc::clone(&scan), vec![col("b")], vec![count_star_expr()]).unwrap(),
    );
    assert!(!guaranteed_at_least_one_row(&grouped));
    let rollup = LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::clone(&scan),
            vec![Expr::GroupingSet(GroupingSet::Rollup(vec![col("b")]))],
            vec![count_star_expr()],
        )
        .unwrap(),
    );
    assert!(!guaranteed_at_least_one_row(&rollup));
    let cube = LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::clone(&scan),
            vec![Expr::GroupingSet(GroupingSet::Cube(vec![col("b")]))],
            vec![count_star_expr()],
        )
        .unwrap(),
    );
    assert!(!guaranteed_at_least_one_row(&cube));
    let grouping_sets = LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::clone(&scan),
            vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![vec![]]))],
            vec![count_star_expr()],
        )
        .unwrap(),
    );
    assert!(!guaranteed_at_least_one_row(&grouping_sets));
    let with_having = LogicalPlan::Filter(
        Filter::try_new(col("count(Int64(1))").gt(lit(0_i64)), Arc::new(ungrouped)).unwrap(),
    );
    assert!(!guaranteed_at_least_one_row(&with_having));
}

#[test]
fn row_guarantee_projection_sort_limit_filter_distinct_union() {
    let lit_proj = LogicalPlan::Projection(
        Projection::try_new(vec![lit(1_i32)], Arc::new(one_row_plan())).unwrap(),
    );
    assert!(guaranteed_at_least_one_row(&lit_proj));
    let empty_list = ScalarValue::List(Arc::new(ListArray::new_null(
        Arc::new(Field::new("item", DataType::Int32, true)),
        1,
    )));
    let unnest_proj = LogicalPlan::Projection(
        Projection::try_new(
            vec![Expr::Unnest(Unnest::new(lit(empty_list)))],
            Arc::new(one_row_plan()),
        )
        .unwrap(),
    );
    assert!(!guaranteed_at_least_one_row(&unnest_proj));
    let sort_no_fetch = LogicalPlan::Sort(SortPlan {
        expr: vec![Sort::new(lit(1_i32), true, false)],
        input: Arc::new(lit_proj.clone()),
        fetch: None,
    });
    assert!(guaranteed_at_least_one_row(&sort_no_fetch));
    let sort_fetch_one = LogicalPlan::Sort(SortPlan {
        expr: vec![Sort::new(lit(1_i32), true, false)],
        input: Arc::new(lit_proj.clone()),
        fetch: Some(1),
    });
    assert!(guaranteed_at_least_one_row(&sort_fetch_one));
    let sort_fetch_zero = LogicalPlan::Sort(SortPlan {
        expr: vec![Sort::new(lit(1_i32), true, false)],
        input: Arc::new(lit_proj.clone()),
        fetch: Some(0),
    });
    assert!(!guaranteed_at_least_one_row(&sort_fetch_zero));
    let limit_one = LogicalPlan::Limit(Limit {
        skip: Some(Box::new(lit(0_i32))),
        fetch: Some(Box::new(lit(1_u64))),
        input: Arc::new(lit_proj.clone()),
    });
    assert!(guaranteed_at_least_one_row(&limit_one));
    let limit_zero = LogicalPlan::Limit(Limit {
        skip: None,
        fetch: Some(Box::new(lit(0_i64))),
        input: Arc::new(lit_proj.clone()),
    });
    assert!(!guaranteed_at_least_one_row(&limit_zero));
    let limit_offset = LogicalPlan::Limit(Limit {
        skip: Some(Box::new(lit(1_i64))),
        fetch: Some(Box::new(lit(1_i64))),
        input: Arc::new(lit_proj.clone()),
    });
    assert!(!guaranteed_at_least_one_row(&limit_offset));
    let filter_true =
        LogicalPlan::Filter(Filter::try_new(lit(true), Arc::new(lit_proj.clone())).unwrap());
    assert!(guaranteed_at_least_one_row(&filter_true));
    let filter_false =
        LogicalPlan::Filter(Filter::try_new(lit(false), Arc::new(lit_proj.clone())).unwrap());
    assert!(!guaranteed_at_least_one_row(&filter_false));
    let distinct_all = LogicalPlan::Distinct(Distinct::All(Arc::new(lit_proj.clone())));
    assert!(guaranteed_at_least_one_row(&distinct_all));
    let alias = LogicalPlan::SubqueryAlias(
        SubqueryAlias::try_new(Arc::new(lit_proj.clone()), "a").unwrap(),
    );
    assert!(guaranteed_at_least_one_row(&alias));
    let zero_proj = LogicalPlan::Projection(
        Projection::try_new(vec![lit(1_i32)], Arc::new(zero_row_plan())).unwrap(),
    );
    let union_guaranteed = LogicalPlan::Union(
        Union::try_new(vec![Arc::new(zero_proj.clone()), Arc::new(lit_proj)]).unwrap(),
    );
    assert!(guaranteed_at_least_one_row(&union_guaranteed));
    let union_empty = LogicalPlan::Union(
        Union::try_new(vec![Arc::new(zero_proj.clone()), Arc::new(zero_proj)]).unwrap(),
    );
    assert!(!guaranteed_at_least_one_row(&union_empty));
}

#[test]
fn seat_helper_places_rule_immediately_before_type_coercion() {
    let rules = insert_before_coercion(datafusion::optimizer::Analyzer::new().rules).unwrap();
    let names: Vec<&str> = rules.iter().map(|rule| rule.name()).collect();
    let seat = names
        .iter()
        .position(|name| *name == ANALYZER_RULE_NAME)
        .unwrap();
    let coercion = names
        .iter()
        .position(|name| *name == "type_coercion")
        .unwrap();
    assert_eq!(seat + 1, coercion);
}

#[test]
fn synthetic_alias_has_no_metadata_while_existing_alias_keeps_its_metadata() {
    let scan = Arc::new(field_id_scan_plan("d", DataType::Date32, false));
    let bare_col_proj =
        LogicalPlan::Projection(Projection::try_new(vec![col("d")], Arc::clone(&scan)).unwrap());
    let wrapped_col = wrap_subquery_output(bare_col_proj).unwrap();
    let LogicalPlan::Projection(proj) = &wrapped_col else {
        panic!("expected projection");
    };
    let Expr::Alias(Alias { metadata, .. }) = &proj.expr[0] else {
        panic!("expected synthetic alias");
    };
    assert!(metadata.is_none());
    assert_eq!(
        wrapped_col
            .schema()
            .field(0)
            .metadata()
            .get("PARQUET:field_id"),
        Some(&"2".to_string())
    );

    let cast_proj = LogicalPlan::Projection(
        Projection::try_new(
            vec![Expr::Cast(Cast::new(
                Box::new(col("d")),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ))],
            Arc::clone(&scan),
        )
        .unwrap(),
    );
    let wrapped_cast = wrap_subquery_output(cast_proj).unwrap();
    let LogicalPlan::Projection(cast_out) = &wrapped_cast else {
        panic!("expected projection");
    };
    let Expr::Alias(Alias {
        metadata: cast_alias_meta,
        ..
    }) = &cast_out.expr[0]
    else {
        panic!("expected synthetic alias on cast");
    };
    assert!(cast_alias_meta.is_none());

    let limit_plan = LogicalPlan::Limit(Limit {
        skip: None,
        fetch: Some(Box::new(lit(1_i64))),
        input: Arc::clone(&scan),
    });
    let wrapped_limit = wrap_subquery_output(limit_plan).unwrap();
    let LogicalPlan::Projection(limit_out) = &wrapped_limit else {
        panic!("expected projection over limit");
    };
    let Expr::Alias(Alias {
        metadata: limit_meta,
        ..
    }) = &limit_out.expr[0]
    else {
        panic!("expected synthetic alias over limit");
    };
    assert!(limit_meta.is_none());

    let mut custom = HashMap::new();
    custom.insert("custom_key".to_string(), "custom_val".to_string());
    let expected_meta = FieldMetadata::from(custom);
    let aliased_proj = LogicalPlan::Projection(
        Projection::try_new(
            vec![col("d").alias_with_metadata("dt", Some(expected_meta.clone()))],
            Arc::clone(&scan),
        )
        .unwrap(),
    );
    let wrapped_aliased = wrap_subquery_output(aliased_proj).unwrap();
    let LogicalPlan::Projection(aliased_out) = &wrapped_aliased else {
        panic!("expected projection");
    };
    let Expr::Alias(Alias {
        name,
        metadata: kept_meta,
        ..
    }) = &aliased_out.expr[0]
    else {
        panic!("expected preserved alias");
    };
    assert_eq!(name, "dt");
    assert_eq!(kept_meta.as_ref(), Some(&expected_meta));
}

#[test]
fn unexpanded_generator_placeholders_defer_wrap_until_expanded() {
    for placeholder in [
        "explode",
        "explode_outer",
        "posexplode",
        "posexplode_outer",
        "inline",
        "inline_outer",
        "json_tuple",
        "stack",
        "__repark_gen_alias",
        "__repark_gen_bare",
    ] {
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(
            StubUdf::new(placeholder, false),
            vec![lit(1_i32)],
        ));
        let proj = LogicalPlan::Projection(
            Projection::try_new(vec![call], Arc::new(one_row_plan())).unwrap(),
        );
        assert!(!guaranteed_at_least_one_row(&proj), "{placeholder}");
        let subq = Subquery {
            subquery: Arc::new(proj.clone()),
            outer_ref_columns: vec![],
            spans: Spans::default(),
        };
        assert!(!scalar_subquery_needs_wrap(&subq), "{placeholder}");

        let limited = LogicalPlan::Limit(Limit {
            skip: None,
            fetch: Some(Box::new(lit(1_i64))),
            input: Arc::new(proj),
        });
        let subq_limited = Subquery {
            subquery: Arc::new(limited),
            outer_ref_columns: vec![],
            spans: Spans::default(),
        };
        assert!(!scalar_subquery_needs_wrap(&subq_limited), "{placeholder}");
    }

    let unnest_call = Expr::ScalarFunction(ScalarFunction::new_udf(
        StubUdf::new("unnest", false),
        vec![lit(1_i32)],
    ));
    let unnest_proj = LogicalPlan::Projection(
        Projection::try_new(vec![unnest_call], Arc::new(one_row_plan())).unwrap(),
    );
    assert!(!guaranteed_at_least_one_row(&unnest_proj));
    let unnest_subq = Subquery {
        subquery: Arc::new(unnest_proj),
        outer_ref_columns: vec![],
        spans: Spans::default(),
    };
    assert!(scalar_subquery_needs_wrap(&unnest_subq));
}

#[test]
fn second_pass_wraps_post_coercion_nonnull_subquery() {
    let nullable_scan = Arc::new(field_id_scan_plan("b", DataType::Int32, true));
    let nullable_subq = Subquery {
        subquery: Arc::new(LogicalPlan::Projection(
            Projection::try_new(vec![col("b")], Arc::clone(&nullable_scan)).unwrap(),
        )),
        outer_ref_columns: vec![],
        spans: Spans::default(),
    };
    let outer_before = LogicalPlan::Projection(
        Projection::try_new(
            vec![Expr::ScalarSubquery(nullable_subq).alias("r")],
            Arc::new(one_row_plan()),
        )
        .unwrap(),
    );
    let pass1 = wrap_plan(outer_before).unwrap();
    assert!(!pass1.transformed);

    let spark_nonnull = StubUdf::new(SPARK_NONNULL_NAME, false);
    let forced_nonnull_subq = Subquery {
        subquery: Arc::new(LogicalPlan::Projection(
            Projection::try_new(
                vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                    spark_nonnull,
                    vec![col("b")],
                ))],
                Arc::clone(&nullable_scan),
            )
            .unwrap(),
        )),
        outer_ref_columns: vec![],
        spans: Spans::default(),
    };
    let outer_after_spark_nullability = LogicalPlan::Projection(
        Projection::try_new(
            vec![Expr::ScalarSubquery(forced_nonnull_subq).alias("r")],
            Arc::new(one_row_plan()),
        )
        .unwrap(),
    );
    assert!(
        !outer_after_spark_nullability
            .schema()
            .field(0)
            .is_nullable()
    );
    let pass2 = wrap_plan(outer_after_spark_nullability).unwrap();
    assert!(pass2.transformed);
    assert!(pass2.data.schema().field(0).is_nullable());
    let pass3 = wrap_plan(pass2.data).unwrap();
    assert!(!pass3.transformed);
}

#[test]
fn second_pass_peels_stale_cast_nonnull_and_keeps_guaranteed_date() {
    let spark_nonnull = StubUdf::new(SPARK_NONNULL_NAME, false);
    let empty_date_scan = Arc::new(field_id_scan_plan("d", DataType::Date32, false));
    let date_subq = Subquery {
        subquery: Arc::new(LogicalPlan::Projection(
            Projection::try_new(vec![col("d")], empty_date_scan).unwrap(),
        )),
        outer_ref_columns: vec![],
        spans: Spans::default(),
    };
    let stale_cast = Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(&spark_nonnull),
        vec![Expr::Cast(Cast::new(
            Box::new(Expr::ScalarSubquery(date_subq)),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ))],
    ));
    let outer_stale = LogicalPlan::Projection(
        Projection::try_new(vec![stale_cast.alias("ts")], Arc::new(one_row_plan())).unwrap(),
    );
    assert!(!outer_stale.schema().field(0).is_nullable());
    let peeled = wrap_plan(outer_stale).unwrap();
    assert!(peeled.transformed);
    assert!(peeled.data.schema().field(0).is_nullable());
    assert!(
        !peeled
            .data
            .display_indent()
            .to_string()
            .contains(SPARK_NONNULL_NAME)
    );

    let guaranteed_date_subq = Subquery {
        subquery: Arc::new(LogicalPlan::Projection(
            Projection::try_new(
                vec![lit(ScalarValue::Date32(Some(19723)))],
                Arc::new(one_row_plan()),
            )
            .unwrap(),
        )),
        outer_ref_columns: vec![],
        spans: Spans::default(),
    };
    let kept_cast = Expr::ScalarFunction(ScalarFunction::new_udf(
        spark_nonnull,
        vec![Expr::Cast(Cast::new(
            Box::new(Expr::ScalarSubquery(guaranteed_date_subq)),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ))],
    ));
    let outer_kept = LogicalPlan::Projection(
        Projection::try_new(vec![kept_cast.alias("ts")], Arc::new(one_row_plan())).unwrap(),
    );
    let unchanged = wrap_plan(outer_kept).unwrap();
    assert!(!unchanged.transformed);
    assert!(!unchanged.data.schema().field(0).is_nullable());
    assert!(
        unchanged
            .data
            .display_indent()
            .to_string()
            .contains(SPARK_NONNULL_NAME)
    );
}

#[tokio::test]
async fn session_seats_rule_before_coercion_and_at_end() {
    let session = ReparkSession::new().unwrap();
    let analyzer_names: Vec<String> = session
        .context()
        .state()
        .analyzer()
        .rules
        .iter()
        .map(|rule| rule.name().to_string())
        .collect();
    let first_seat = analyzer_names
        .iter()
        .position(|name| name == ANALYZER_RULE_NAME)
        .unwrap();
    let coercion = analyzer_names
        .iter()
        .position(|name| name == "type_coercion")
        .unwrap();
    assert_eq!(first_seat + 1, coercion);
    assert_eq!(
        analyzer_names.last().map(String::as_str),
        Some(ANALYZER_RULE_NAME)
    );
}

#[tokio::test]
async fn analyzer_wraps_empty_subquery_and_keeps_count_star_nonnull() {
    let session = ReparkSession::new().unwrap();

    let empty_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
    let empty_tbl = MemTable::try_new(empty_schema, vec![vec![]]).unwrap();
    session
        .context()
        .register_table("empty_nn", Arc::new(empty_tbl))
        .unwrap();
    let left_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .unwrap();
    let left_tbl = MemTable::try_new(left_schema, vec![vec![left_batch]]).unwrap();
    session
        .context()
        .register_table("l_nn", Arc::new(left_tbl))
        .unwrap();

    let empty_df = session
        .sql("SELECT (SELECT b FROM empty_nn) IS NULL AS r")
        .await
        .unwrap();
    let empty_opt = empty_df.clone().into_optimized_plan().unwrap();
    let empty_plan_text = empty_opt.display_indent().to_string();
    assert!(!empty_plan_text.contains(SCALAR_NULLABLE_NAME));
    let empty_batches = empty_df.collect().await.unwrap();
    let flag = empty_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        .unwrap()
        .value(0);
    assert!(flag);

    let count_df = session
        .sql("SELECT (SELECT COUNT(*) FROM empty_nn) IS NULL AS r")
        .await
        .unwrap();
    let count_opt = count_df.clone().into_optimized_plan().unwrap();
    assert!(
        count_opt
            .display_indent()
            .to_string()
            .contains("Boolean(false)")
    );
    let count_batches = count_df.collect().await.unwrap();
    let count_flag = count_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        .unwrap()
        .value(0);
    assert!(!count_flag);

    let cte_df = session
        .sql("WITH c AS (SELECT id, (SELECT b FROM empty_nn) AS r FROM l_nn) SELECT id, r IS NULL AS n, COALESCE(r, 42) AS c FROM c ORDER BY id")
        .await
        .unwrap();
    let cte_batches = cte_df.collect().await.unwrap();
    let n_col = cte_batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        .unwrap();
    let c_col = cte_batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(n_col.value(0) && n_col.value(1));
    assert_eq!(c_col.value(0), 42);
    assert_eq!(c_col.value(1), 42);

    let raw_plan = session
        .context()
        .state()
        .create_logical_plan("SELECT (SELECT b FROM empty_nn) AS r")
        .await
        .unwrap();
    let analyzed_once = ReparkScalarSubqueryNullability
        .analyze(raw_plan, session.context().state().config_options())
        .unwrap();
    assert!(analyzed_once.schema().field(0).is_nullable());
    let optimizer_ctx = OptimizerContext::new();
    let second = ReparkScalarSubqueryNullability
        .rewrite(analyzed_once.clone(), &optimizer_ctx)
        .unwrap();
    assert!(!second.transformed);
    let stripped_surviving = StripScalarNullable
        .rewrite(analyzed_once, &optimizer_ctx)
        .unwrap();
    assert!(!stripped_surviving.transformed);
    assert!(udf().name() == SCALAR_NULLABLE_NAME);
}
