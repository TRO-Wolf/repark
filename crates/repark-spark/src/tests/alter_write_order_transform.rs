use super::super::*;
use super::accept_any_refusals::refusal;
use super::common::*;
use iceberg::spec::{NullOrder, SortDirection};

const TRANSFORM_TARGET: &str = "CREATE TABLE ice.sales.wo (id BIGINT, ts TIMESTAMP, s STRING, \
     d DATE, dec DECIMAL(10,2)) USING iceberg";

async fn transform_door(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    run(&ctx, &catalogs, TRANSFORM_TARGET).await;
    (ctx, catalogs)
}

fn rendered_order(table: &iceberg::table::Table) -> Vec<String> {
    let schema = table.metadata().current_schema();
    table
        .metadata()
        .default_sort_order()
        .fields
        .iter()
        .map(|field| {
            let direction = match field.direction {
                SortDirection::Ascending => "asc",
                SortDirection::Descending => "desc",
            };
            let nulls = match field.null_order {
                NullOrder::First => "nulls-first",
                NullOrder::Last => "nulls-last",
            };
            let source = schema.name_by_field_id(field.source_id).unwrap();
            format!("{} {source} {direction} {nulls}", field.transform)
        })
        .collect()
}

fn distribution_mode(table: &iceberg::table::Table) -> Option<String> {
    table
        .metadata()
        .properties()
        .get("write.distribution-mode")
        .cloned()
}

#[tokio::test]
async fn write_ordered_by_transforms_lands_the_order_spark_measured() {
    let cases: [(&str, &[&str]); 20] = [
        (
            "bucket(4, id), days(ts) DESC NULLS FIRST",
            &["bucket[4] id asc nulls-first", "day ts desc nulls-first"],
        ),
        (
            "bucket(4, id) DESC, days(ts)",
            &["bucket[4] id desc nulls-last", "day ts asc nulls-first"],
        ),
        (
            "truncate(2, s) ASC NULLS LAST",
            &["truncate[2] s asc nulls-last"],
        ),
        (
            "years(ts), months(ts), hours(ts)",
            &[
                "year ts asc nulls-first",
                "month ts asc nulls-first",
                "hour ts asc nulls-first",
            ],
        ),
        (
            "id, bucket(16, s) DESC NULLS LAST",
            &[
                "identity id asc nulls-first",
                "bucket[16] s desc nulls-last",
            ],
        ),
        ("identity(id)", &["identity id asc nulls-first"]),
        (
            "year(ts), month(ts), hour(ts), date(ts), date_hour(ts)",
            &[
                "year ts asc nulls-first",
                "month ts asc nulls-first",
                "hour ts asc nulls-first",
                "day ts asc nulls-first",
                "hour ts asc nulls-first",
            ],
        ),
        ("truncate(s, 2)", &["truncate[2] s asc nulls-first"]),
        ("bucket(id, 4)", &["bucket[4] id asc nulls-first"]),
        ("bucket(4, id) NULLS LAST", &["bucket[4] id asc nulls-last"]),
        ("truncate(4, dec)", &["truncate[4] dec asc nulls-first"]),
        ("BUCKET(4, ID) DESC", &["bucket[4] id desc nulls-last"]),
        (
            "bucket(4, id), bucket(4, id)",
            &[
                "bucket[4] id asc nulls-first",
                "bucket[4] id asc nulls-first",
            ],
        ),
        (
            "days(ts), ts",
            &["day ts asc nulls-first", "identity ts asc nulls-first"],
        ),
        ("months(d)", &["month d asc nulls-first"]),
        ("days(ts) NULLS LAST", &["day ts asc nulls-last"]),
        (
            "truncate(2, id), days(TS)",
            &["truncate[2] id asc nulls-first", "day ts asc nulls-first"],
        ),
        ("bucket(4L, id)", &["bucket[4] id asc nulls-first"]),
        ("bucket(4l, id)", &["bucket[4] id asc nulls-first"]),
        ("truncate(s, 2L)", &["truncate[2] s asc nulls-first"]),
    ];
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = transform_door(&wh).await;
    for (spec, expected) in cases {
        run(
            &ctx,
            &catalogs,
            &format!("ALTER TABLE ice.sales.wo WRITE ORDERED BY {spec}"),
        )
        .await;
        let table = load_sales_table(&catalogs, "wo").await;
        assert_eq!(rendered_order(&table), expected, "{spec}");
        assert_eq!(
            distribution_mode(&table).as_deref(),
            Some("range"),
            "{spec}"
        );
    }
}

#[tokio::test]
async fn locally_and_distributed_transform_orders_keep_their_distribution() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = transform_door(&wh).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.wo WRITE LOCALLY ORDERED BY bucket(4, id)",
    )
    .await;
    let table = load_sales_table(&catalogs, "wo").await;
    assert_eq!(rendered_order(&table), ["bucket[4] id asc nulls-first"]);
    assert_eq!(distribution_mode(&table), None);
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.wo WRITE DISTRIBUTED BY PARTITION ORDERED BY (days(ts) DESC)",
    )
    .await;
    let table = load_sales_table(&catalogs, "wo").await;
    assert_eq!(rendered_order(&table), ["day ts desc nulls-last"]);
    assert_eq!(distribution_mode(&table).as_deref(), Some("hash"));
}

#[tokio::test]
async fn write_ordered_by_transform_refusals_match_spark_and_commit_nothing() {
    let illegal: [(&str, &str); 11] = [
        ("zorder(id, s)", "Term must be unbound"),
        ("zorder(id)", "Term must be unbound"),
        (
            "bucket(0, id)",
            "Unsupported width for transform: bucket(0, id)",
        ),
        (
            "truncate(0, s)",
            "Unsupported width for transform: truncate(0, s)",
        ),
        (
            "bucket(-1, id)",
            "Unsupported width for transform: bucket(-1, id)",
        ),
        (
            "bucket(2147483648, id)",
            "Unsupported width for transform: bucket(2147483648, id)",
        ),
        (
            "bucket(4)",
            "Cannot convert transform with more than one column reference: bucket(4)",
        ),
        (
            "bucket(4, id, s)",
            "Cannot convert transform with more than one column reference: bucket(4, id, s)",
        ),
        (
            "bucket(4, 2)",
            "Cannot convert transform with more than one column reference: bucket(4, 2)",
        ),
        (
            "truncate(s)",
            "Cannot find width for transform: truncate(s)",
        ),
        (
            "bucket('4', id)",
            "Cannot find width for transform: bucket('4', id)",
        ),
    ];
    let unsupported: [(&str, &str); 3] = [
        ("void(id)", "Transform is not supported: void(id)"),
        ("foo(id)", "Transform is not supported: foo(id)"),
        (
            "bucket(4, id) ASC NULLS FIRST, void(id)",
            "Transform is not supported: void(id)",
        ),
    ];
    let unbindable: [(&str, &str); 5] = [
        (
            "days(id)",
            "Cannot bind: day cannot transform long values from 'id'",
        ),
        (
            "hours(d)",
            "Cannot bind: hour cannot transform date values from 'd'",
        ),
        (
            "years(s)",
            "Cannot bind: year cannot transform string values from 's'",
        ),
        (
            "truncate(4, ts)",
            "Cannot bind: truncate[4] cannot transform timestamptz values from 'ts'",
        ),
        ("bucket(4, nope)", "Cannot find field nope in table schema"),
    ];
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = transform_door(&wh).await;
    let sql = |spec: &str| format!("ALTER TABLE ice.sales.wo WRITE ORDERED BY {spec}");
    for (spec, expected) in illegal {
        let mapped = refusal(&ctx, &catalogs, &sql(spec)).await;
        assert!(
            matches!(&mapped, repark_common::Error::IllegalArgument(message) if message == expected),
            "{spec}: got {mapped:?}"
        );
    }
    for (spec, expected) in unsupported {
        let mapped = refusal(&ctx, &catalogs, &sql(spec)).await;
        assert!(
            matches!(&mapped, repark_common::Error::NotImplemented(message) if message == expected),
            "{spec}: got {mapped:?}"
        );
    }
    for (spec, expected) in unbindable {
        let mapped = refusal(&ctx, &catalogs, &sql(spec)).await;
        assert!(
            matches!(&mapped, repark_common::Error::Iceberg(message)
                if message == &format!("DataInvalid => {expected}")),
            "{spec}: got {mapped:?}"
        );
    }
    let table = load_sales_table(&catalogs, "wo").await;
    assert_eq!(table.metadata().sort_orders_iter().len(), 1);
    assert_eq!(table.metadata().default_sort_order_id(), 0);
    assert_eq!(distribution_mode(&table), None);
}

#[tokio::test]
async fn insert_after_a_bucket_order_sorts_by_the_bucket() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.wo (id BIGINT, s STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.wo WRITE ORDERED BY bucket(4, id)",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.wo VALUES (5,'a'),(1,'b'),(3,'c'),(2,'d'),(4,'e')",
    )
    .await;
    let table = load_sales_table(&catalogs, "wo").await;
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifests = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    for manifest in manifests.entries() {
        for entry in manifest
            .load_manifest(table.file_io())
            .await
            .unwrap()
            .entries()
        {
            assert_eq!(entry.data_file().sort_order_id(), Some(1));
        }
    }
    let batches = execute(&ctx, &catalogs, "SELECT id FROM ice.sales.wo")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let buckets: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap();
            ids.values().to_vec()
        })
        .map(|id| [0, 0, 0, 3, 2, 3][usize::try_from(id).unwrap()])
        .collect();
    assert_eq!(buckets, [0, 0, 2, 3, 3]);
}

#[tokio::test]
async fn delete_after_a_repark_bucket_order_is_the_identity_only_residue() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.wo (id BIGINT, s STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.wo VALUES (1,'a'),(2,'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.wo WRITE ORDERED BY bucket(4, id)",
    )
    .await;
    let snapshots = load_sales_table(&catalogs, "wo")
        .await
        .metadata()
        .snapshots()
        .count();
    let mapped = refusal(&ctx, &catalogs, "DELETE FROM ice.sales.wo WHERE id = 1").await;
    assert!(
        matches!(&mapped, repark_common::Error::NotImplemented(message)
            if message == "This feature is not implemented: sorting by the table's default sort \
                order uses transform `bucket[4]` on source id 1, only identity sort fields are \
                supported"),
        "got {mapped:?}"
    );
    let table = load_sales_table(&catalogs, "wo").await;
    assert_eq!(table.metadata().snapshots().count(), snapshots);
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.wo").await, 2);
}

#[tokio::test]
async fn typed_width_literals_and_parse_shapes_answer_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = transform_door(&wh).await;
    let sql = |spec: &str| format!("ALTER TABLE ice.sales.wo WRITE ORDERED BY {spec}");
    for (spec, expected) in [
        (
            "bucket(4S, id)",
            "Cannot find width for transform: bucket(4, id)",
        ),
        (
            "bucket(4Y, id)",
            "Cannot find width for transform: bucket(4, id)",
        ),
        (
            "bucket(4BD, id)",
            "Cannot find width for transform: bucket(4, id)",
        ),
        (
            "bucket(4.0, id)",
            "Cannot find width for transform: bucket(4.0, id)",
        ),
        (
            "bucket(4D, id)",
            "Cannot find width for transform: bucket(4.0, id)",
        ),
        (
            "bucket(4F, id)",
            "Cannot find width for transform: bucket(4.0, id)",
        ),
        (
            "bucket(0L, id)",
            "Unsupported width for transform: bucket(0, id)",
        ),
        (
            "bucket(-4L, id)",
            "Unsupported width for transform: bucket(-4, id)",
        ),
        (
            "bucket(3000000000L, id)",
            "Unsupported width for transform: bucket(3000000000, id)",
        ),
    ] {
        let mapped = refusal(&ctx, &catalogs, &sql(spec)).await;
        assert!(
            matches!(&mapped, repark_common::Error::IllegalArgument(message) if message == expected),
            "{spec}: got {mapped:?}"
        );
    }
    for (spec, detail) in [
        ("bucket()", "no viable alternative at input ')'"),
        ("bucket(4,)", "no viable alternative at input ')'"),
        ("hours()", "no viable alternative at input ')'"),
        ("bucket(+4, id)", "no viable alternative at input '+'"),
        ("bucket((4), id)", "no viable alternative at input '('"),
        (
            "bucket(4, id)(x)",
            "mismatched input '(' expecting {<EOF>, ',', 'ASC', 'DESC', 'DISTRIBUTED', \
             'LOCALLY', 'NULLS', 'ORDERED', 'UNORDERED'}",
        ),
    ] {
        let mapped = refusal(&ctx, &catalogs, &sql(spec)).await;
        let expected = format!(
            "Error during planning: \n{detail}\n== SQL ==\n{}",
            sql(spec)
        );
        assert!(
            matches!(&mapped, repark_common::Error::Analysis(message) if message == &expected),
            "{spec}: got {mapped:?}"
        );
    }
    let table = load_sales_table(&catalogs, "wo").await;
    assert_eq!(table.metadata().sort_orders_iter().len(), 1);
    assert_eq!(table.metadata().default_sort_order_id(), 0);
}
