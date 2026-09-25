use super::super::*;
use super::common::*;
use super::replace_where::{PARTITIONED, SEED};

type Row = (Option<i64>, &'static str, Option<&'static str>);

async fn typed_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) -> Vec<(Option<i64>, String, Option<String>)> {
    let batches = execute(ctx, catalogs, "SELECT id, data, cat FROM ice.sales.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let data = datafusion::arrow::compute::cast(batch.column(1), &DataType::Utf8).unwrap();
        let data = data.as_any().downcast_ref::<StringArray>().unwrap();
        let cats = datafusion::arrow::compute::cast(batch.column(2), &DataType::Utf8).unwrap();
        let cats = cats.as_any().downcast_ref::<StringArray>().unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                ids.is_valid(index).then(|| ids.value(index)),
                data.value(index).to_string(),
                cats.is_valid(index).then(|| cats.value(index).to_string()),
            ));
        }
    }
    rows.sort();
    rows
}

fn owned(rows: &[Row]) -> Vec<(Option<i64>, String, Option<String>)> {
    let mut rows: Vec<_> = rows
        .iter()
        .map(|(id, data, cat)| (*id, (*data).to_string(), cat.map(str::to_string)))
        .collect();
    rows.sort();
    rows
}

async fn replaced(
    create: &str,
    extra_seed: &str,
    predicate: &str,
) -> Vec<(Option<i64>, String, Option<String>)> {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, create).await;
    run(&ctx, &catalogs, SEED).await;
    run(&ctx, &catalogs, extra_seed).await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.t REPLACE WHERE {predicate} SELECT 9, 'z', 'x'"),
    )
    .await;
    typed_rows(&ctx, &catalogs).await
}

const NULL_CAT_CASES: [(&str, &[Row]); 49] = [
    (
        "cat = 'x'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat <> 'x'",
        &[
            (Some(1), "a", Some("x")),
            (Some(3), "c", Some("x")),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat != 'y'",
        &[(Some(2), "b", Some("y")), (Some(9), "z", Some("x"))],
    ),
    (
        "NOT cat = 'y'",
        &[(Some(2), "b", Some("y")), (Some(9), "z", Some("x"))],
    ),
    (
        "cat < 'y'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat <= 'x'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat > 'a'",
        &[(Some(4), "n", None), (Some(9), "z", Some("x"))],
    ),
    (
        "cat >= 'x'",
        &[(Some(4), "n", None), (Some(9), "z", Some("x"))],
    ),
    (
        "NOT cat < 'y'",
        &[
            (Some(1), "a", Some("x")),
            (Some(3), "c", Some("x")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "NOT cat <= 'x'",
        &[
            (Some(1), "a", Some("x")),
            (Some(3), "c", Some("x")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "NOT cat > 'x'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "NOT cat >= 'y'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "'y' > cat",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "NOT 'y' > cat",
        &[
            (Some(1), "a", Some("x")),
            (Some(3), "c", Some("x")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat BETWEEN 'a' AND 'x'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat NOT BETWEEN 'y' AND 'z'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "NOT (cat BETWEEN 'y' AND 'z')",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat IN ('x')",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat IN ('x', 'y')",
        &[(Some(4), "n", None), (Some(9), "z", Some("x"))],
    ),
    (
        "cat NOT IN ('y')",
        &[(Some(2), "b", Some("y")), (Some(9), "z", Some("x"))],
    ),
    (
        "cat NOT IN ('y', 'q')",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat NOT IN ('y', 'y')",
        &[(Some(2), "b", Some("y")), (Some(9), "z", Some("x"))],
    ),
    (
        "NOT cat IN ('y')",
        &[(Some(2), "b", Some("y")), (Some(9), "z", Some("x"))],
    ),
    (
        "NOT (cat IN ('y', 'q'))",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat IN ('x', NULL)",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat NOT IN ('y', NULL)",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat LIKE 'x%'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat LIKE 'x'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat NOT LIKE 'y'",
        &[(Some(2), "b", Some("y")), (Some(9), "z", Some("x"))],
    ),
    (
        "cat IS NULL",
        &[
            (Some(1), "a", Some("x")),
            (Some(2), "b", Some("y")),
            (Some(3), "c", Some("x")),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat IS NOT NULL",
        &[(Some(4), "n", None), (Some(9), "z", Some("x"))],
    ),
    (
        "NOT cat IS NULL",
        &[(Some(4), "n", None), (Some(9), "z", Some("x"))],
    ),
    (
        "cat = 'x' OR cat IS NULL",
        &[(Some(2), "b", Some("y")), (Some(9), "z", Some("x"))],
    ),
    (
        "cat < 'y' OR cat IS NULL",
        &[(Some(2), "b", Some("y")), (Some(9), "z", Some("x"))],
    ),
    (
        "NOT (cat = 'x' AND cat = 'y')",
        &[(Some(9), "z", Some("x"))],
    ),
    (
        "NOT (cat = 'y' OR cat < 'x')",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "NOT (cat <> 'x')",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat <=> 'x'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "NOT cat <=> 'x'",
        &[
            (Some(1), "a", Some("x")),
            (Some(3), "c", Some("x")),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat <=> NULL",
        &[
            (Some(1), "a", Some("x")),
            (Some(2), "b", Some("y")),
            (Some(3), "c", Some("x")),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "NOT (cat <=> NULL)",
        &[(Some(4), "n", None), (Some(9), "z", Some("x"))],
    ),
    (
        "NOT NOT cat = 'x'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "NOT (cat = 'x' AND cat IN ('y'))",
        &[(Some(9), "z", Some("x"))],
    ),
    (
        "NOT (cat IN ('x', 'y') OR cat = 'q')",
        &[
            (Some(1), "a", Some("x")),
            (Some(2), "b", Some("y")),
            (Some(3), "c", Some("x")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat < 'y' AND cat > 'a'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    (
        "cat < 'b' OR cat = 'x'",
        &[
            (Some(2), "b", Some("y")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    ("true", &[(Some(9), "z", Some("x"))]),
    (
        "NOT true",
        &[
            (Some(1), "a", Some("x")),
            (Some(2), "b", Some("y")),
            (Some(3), "c", Some("x")),
            (Some(4), "n", None),
            (Some(9), "z", Some("x")),
        ],
    ),
    ("NOT false", &[(Some(9), "z", Some("x"))]),
];

#[tokio::test]
async fn a_null_partition_key_is_kept_or_replaced_as_spark_does() {
    let cases = NULL_CAT_CASES;
    for (predicate, expected) in cases {
        let actual = replaced(
            PARTITIONED,
            "INSERT INTO ice.sales.t VALUES (4, 'n', NULL)",
            predicate,
        )
        .await;
        assert_eq!(actual, owned(expected), "{predicate}");
    }
}

const NULL_ID_CASES: [(&str, &[Row]); 7] = [
    (
        "id < 3",
        &[
            (Some(3), "c", Some("x")),
            (Some(9), "z", Some("x")),
            (None, "n", Some("w")),
        ],
    ),
    (
        "id <= 2",
        &[
            (Some(3), "c", Some("x")),
            (Some(9), "z", Some("x")),
            (None, "n", Some("w")),
        ],
    ),
    (
        "NOT id >= 3",
        &[
            (Some(3), "c", Some("x")),
            (Some(9), "z", Some("x")),
            (None, "n", Some("w")),
        ],
    ),
    (
        "id NOT IN (1)",
        &[(Some(1), "a", Some("x")), (Some(9), "z", Some("x"))],
    ),
    (
        "id NOT IN (1, 2)",
        &[
            (Some(1), "a", Some("x")),
            (Some(2), "b", Some("y")),
            (Some(9), "z", Some("x")),
            (None, "n", Some("w")),
        ],
    ),
    (
        "id <> 1",
        &[(Some(1), "a", Some("x")), (Some(9), "z", Some("x"))],
    ),
    (
        "id NOT BETWEEN 2 AND 3",
        &[
            (Some(2), "b", Some("y")),
            (Some(3), "c", Some("x")),
            (Some(9), "z", Some("x")),
            (None, "n", Some("w")),
        ],
    ),
];

#[tokio::test]
async fn a_null_identity_key_on_a_long_partition_follows_spark() {
    let cases = NULL_ID_CASES;
    let by_id = "CREATE TABLE ice.sales.t (id BIGINT, data STRING, cat STRING) USING iceberg \
                 PARTITIONED BY (id)";
    for (predicate, expected) in cases {
        let actual = replaced(
            by_id,
            "INSERT INTO ice.sales.t VALUES (NULL, 'n', 'w')",
            predicate,
        )
        .await;
        assert_eq!(actual, owned(expected), "{predicate}");
    }
}

#[tokio::test]
async fn a_replace_where_source_with_repeated_names_writes_as_spark_does() {
    let cases: [(&str, &[Row]); 3] = [
        (
            "SELECT id, CAST(id AS STRING), 'x' FROM ice.sales.t WHERE cat = 'x'",
            &[
                (Some(1), "1", Some("x")),
                (Some(2), "b", Some("y")),
                (Some(3), "3", Some("x")),
            ],
        ),
        (
            "SELECT 9 AS a, 'z' AS a, 'x'",
            &[(Some(2), "b", Some("y")), (Some(9), "z", Some("x"))],
        ),
        (
            "SELECT 9 AS a, 'z' AS a, 'x' UNION ALL SELECT 10, 'w', 'x'",
            &[
                (Some(10), "w", Some("x")),
                (Some(2), "b", Some("y")),
                (Some(9), "z", Some("x")),
            ],
        ),
    ];
    for (source, expected) in cases {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        run(&ctx, &catalogs, PARTITIONED).await;
        run(&ctx, &catalogs, SEED).await;
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' {source}"),
        )
        .await;
        assert_eq!(
            typed_rows(&ctx, &catalogs).await,
            owned(expected),
            "{source}"
        );
    }
}

#[tokio::test]
async fn a_replace_where_into_a_missing_namespace_answers_table_or_view_not_found() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = match execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.nope.t REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
    )
    .await
    {
        Ok(frame) => frame
            .collect()
            .await
            .expect_err("a missing namespace refuses"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains(
            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `ice`.`nope`.`t` cannot be found."
        ),
        "{error}"
    );
}
