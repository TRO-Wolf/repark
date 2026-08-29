/// ===========================================================================================
/// GROUP O pins static Spark `INSERT OVERWRITE` on transform-partitioned tables. The commit is a
/// whole-table replace; manifest partition slots, fork transform values, and Arrow values/types
/// are checked. Dynamic per-partition overwrite is out of scope.
/// ===========================================================================================
use std::collections::{HashMap as StdHashMap, HashSet};

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int32Type;
use iceberg::spec::{DataFile, Literal, ManifestContentType, PrimitiveLiteral, Transform};
use iceberg::table::Table;
use iceberg::transform::create_transform_function;

use super::super::*;
use super::common::*;

async fn loaded_table(catalogs: &CatalogRegistry, table: &str) -> Table {
    catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            table.to_string(),
        ))
        .await
        .expect("load table")
}

async fn live_data_files(table: &Table) -> Vec<DataFile> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Vec::new();
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("load manifest list");
    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("load manifest");
        for entry in manifest.entries() {
            if entry.is_alive() {
                files.push(entry.data_file().clone());
            }
        }
    }
    files
}

/// The `i32` partition slot at `index` (bucket ordinal / day ordinal / truncated int).
fn slot_int(file: &DataFile, index: usize) -> i32 {
    match file.partition().fields().get(index).cloned().flatten() {
        Some(Literal::Primitive(PrimitiveLiteral::Int(value))) => value,
        other => panic!("expected an int partition slot at {index}, got {other:?}"),
    }
}

/// The string partition slot at `index` (identity / truncate on a string column).
fn slot_str(file: &DataFile, index: usize) -> String {
    match file.partition().fields().get(index).cloned().flatten() {
        Some(Literal::Primitive(PrimitiveLiteral::String(value))) => value,
        other => panic!("expected a string partition slot at {index}, got {other:?}"),
    }
}

/// slot value → total manifest record count over the live files (the committed proof that
/// each file carries the partition value its rows actually belong to).
async fn int_slot_counts(table: &Table) -> StdHashMap<i32, u64> {
    let mut counts = StdHashMap::new();
    for file in &live_data_files(table).await {
        *counts.entry(slot_int(file, 0)).or_insert(0) += file.record_count();
    }
    counts
}

/// The sorted partition slots as `Option<i32>` — NULL slot preserved (the O7 oracle).
async fn nullable_int_slots(table: &Table) -> Vec<Option<i32>> {
    let mut slots: Vec<Option<i32>> = live_data_files(table)
        .await
        .iter()
        .map(
            |file| match file.partition().fields().first().cloned().flatten() {
                Some(Literal::Primitive(PrimitiveLiteral::Int(value))) => Some(value),
                None => None,
                other => panic!("unexpected slot literal {other:?}"),
            },
        )
        .collect();
    slots.sort_unstable();
    slots
}

/// The FORK's own `Bucket(n)` over `ids` — the self-oracle for expected routing.
fn fork_buckets(ids: &[i32], num_buckets: u32) -> Vec<i32> {
    let transform =
        create_transform_function(&Transform::Bucket(num_buckets)).expect("bucket transform fn");
    let out = transform
        .transform(Arc::new(Int32Array::from(ids.to_vec())))
        .expect("apply bucket transform");
    let out = out.as_primitive::<Int32Type>();
    (0..out.len()).map(|row| out.value(row)).collect()
}

/// The FORK's own `Day` over micro-timestamps — the temporal self-oracle.
fn fork_days(timestamps: &[i64]) -> Vec<i32> {
    use datafusion::arrow::array::TimestampMicrosecondArray;

    let transform = create_transform_function(&Transform::Day).expect("day transform fn");
    let out = transform
        .transform(Arc::new(TimestampMicrosecondArray::from(
            timestamps.to_vec(),
        )))
        .expect("apply day transform");
    let out = datafusion::arrow::compute::cast(&out, &DataType::Int32).expect("day ordinal to i32");
    let out = out.as_primitive::<Int32Type>();
    (0..out.len()).map(|row| out.value(row)).collect()
}

/// A `(id int NOT NULL, name string NULL)` source — the NULL-partition-source fixtures.
fn register_nullable_source(ctx: &SessionContext, name: &str, rows: &[(i32, Option<&str>)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ctx.register_batch(name, batch).unwrap();
}

/// Expected slot → count map from a self-oracle slot vector.
fn counts_of(slots: &[i32]) -> StdHashMap<i32, u64> {
    let mut counts = StdHashMap::new();
    for slot in slots {
        *counts.entry(*slot).or_insert(0) += 1;
    }
    counts
}

fn register_ts_source(ctx: &SessionContext, name: &str, rows: &[(i32, &str, i64)]) {
    use datafusion::arrow::array::TimestampMicrosecondArray;
    use datafusion::arrow::datatypes::TimeUnit;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ctx.register_batch(name, batch).unwrap();
}

/// PIN O1 (Group O) — a NON-EMPTY `INSERT OVERWRITE` into a `bucket(4, id)` table is a
/// STATIC whole-table replace whose new files carry the FORK's own `Bucket(4)` ordinals.
///
/// Three things are pinned at once, all discriminating:
/// 1. **Static, not dynamic** — the fixture's old ids cover a bucket the NEW ids never
///    land in; after the overwrite that bucket has ZERO live files. Dynamic-mode
///    (`overwriteDynamicPartitions`) would have left it behind, and the surviving rows
///    would be a silent Spark divergence.
/// 2. **Transform routing survives the overwrite** — the manifest slot → record-count map
///    equals the fork's `Bucket(4)` over the new ids exactly (not identity, not one
///    unpartitioned blob).
/// 3. **Spec is untouched** and the rows round-trip on the Arrow path (value AND type).
///
/// RED on: routing `bucket` → identity in `build_partition_spec` (slot map wrong), or
/// degrading the non-empty overwrite to an append in `execute_insert_overwrite` (old rows
/// survive / the old-only bucket comes back).
#[tokio::test]
async fn overwrite_bucket_table_static_replace_and_fork_hash_routing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let old_ids: Vec<i32> = vec![1, 2, 3, 5, 8, 13, 21, 34];
    let old_rows: Vec<(i32, &str)> = old_ids.iter().map(|&id| (id, "old")).collect();
    register_source(&ctx, "o1_old", &old_rows);
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bkt USING iceberg PARTITIONED BY (bucket(4, id)) AS \
             SELECT * FROM o1_old",
    )
    .await
    .expect("bucket CTAS must succeed");

    let new_ids: Vec<i32> = vec![100, 200, 300];
    let new_rows: Vec<(i32, &str)> = new_ids.iter().map(|&id| (id, "new")).collect();
    register_source(&ctx, "o1_new", &new_rows);

    let old_buckets: HashSet<i32> = fork_buckets(&old_ids, 4).into_iter().collect();
    let new_bucket_slots = fork_buckets(&new_ids, 4);
    let new_buckets: HashSet<i32> = new_bucket_slots.iter().copied().collect();
    let old_only: HashSet<i32> = old_buckets.difference(&new_buckets).copied().collect();
    assert!(
        !old_only.is_empty(),
        "fixture must leave an old-only bucket so static vs dynamic overwrite is \
             distinguishable: old={old_buckets:?} new={new_buckets:?}"
    );

    let count = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.bkt SELECT * FROM o1_new",
    )
    .await
    .expect("non-empty INSERT OVERWRITE into a bucket-partitioned table must succeed")
    .collect()
    .await
    .expect("collect the overwrite count batch");
    assert_eq!(
        count.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "the overwrite returns one count row"
    );

    let table = loaded_table(&catalogs, "bkt").await;
    let spec = table.metadata().default_partition_spec();
    assert_eq!(spec.fields().len(), 1, "overwrite must not alter the spec");
    assert_eq!(spec.fields()[0].name, "id_bucket");
    assert_eq!(spec.fields()[0].transform, Transform::Bucket(4));

    let actual = int_slot_counts(&table).await;
    assert_eq!(
        actual,
        counts_of(&new_bucket_slots),
        "post-overwrite manifest routing must equal the fork's Bucket(4) over the NEW ids"
    );
    for bucket in &old_only {
        assert!(
            !actual.contains_key(bucket),
            "STATIC overwrite must leave NO live file in old-only bucket {bucket} \
                 (dynamic-mode semantics would — that is the silent-wrong-wipe divergence)"
        );
    }

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.bkt").await,
        vec![
            (100, "new".to_string()),
            (200, "new".to_string()),
            (300, "new".to_string()),
        ],
        "the whole table is replaced by exactly the overwrite source rows"
    );
}

/// PIN O2a (Group O) — the same static-replace + transform-routing contract on a TEMPORAL
/// `days(ts)` table: the overwrite's rows land in the fork's own `Day` ordinals, and the
/// old days (none of which the new data touches) are gone. Reverting to identity or
/// degrading the overwrite to an append turns the slot-map / row asserts RED.
#[tokio::test]
async fn overwrite_days_table_static_replace_and_day_routing() {
    const DAY: i64 = 86_400_000_000;
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let old_ts: Vec<i64> = vec![0, DAY, 2 * DAY];
    let old_rows: Vec<(i32, &str, i64)> = old_ts
        .iter()
        .enumerate()
        .map(|(index, &ts)| (i32::try_from(index).expect("small index") + 1, "old", ts))
        .collect();
    register_ts_source(&ctx, "o2_old", &old_rows);
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.dy USING iceberg PARTITIONED BY (days(ts)) AS \
             SELECT * FROM o2_old",
    )
    .await
    .expect("days CTAS must succeed");

    // Two new rows, both on ONE day far from every old day: static overwrite must leave
    // exactly that one partition, dynamic would have kept the three old ones.
    let new_ts: Vec<i64> = vec![100 * DAY, 100 * DAY + 3_600_000_000];
    let new_rows: Vec<(i32, &str, i64)> = vec![(50, "new", new_ts[0]), (51, "new", new_ts[1])];
    register_ts_source(&ctx, "o2_new", &new_rows);

    let old_days: HashSet<i32> = fork_days(&old_ts).into_iter().collect();
    let new_day_slots = fork_days(&new_ts);
    let new_days: HashSet<i32> = new_day_slots.iter().copied().collect();
    assert!(
        old_days.is_disjoint(&new_days),
        "fixture must not overlap days: old={old_days:?} new={new_days:?}"
    );

    execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.dy SELECT * FROM o2_new",
    )
    .await
    .expect("non-empty INSERT OVERWRITE into a days(ts) table must succeed");

    let table = loaded_table(&catalogs, "dy").await;
    let spec = table.metadata().default_partition_spec();
    assert_eq!(spec.fields()[0].name, "ts_day");
    assert_eq!(spec.fields()[0].transform, Transform::Day);

    let actual = int_slot_counts(&table).await;
    assert_eq!(
        actual,
        counts_of(&new_day_slots),
        "post-overwrite manifest routing must equal the fork's Day(ts) over the NEW rows"
    );
    for day in &old_days {
        assert!(
            !actual.contains_key(day),
            "STATIC overwrite must drop old day partition {day}"
        );
    }
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.dy").await,
        vec![(50, "new".to_string()), (51, "new".to_string())]
    );
}

/// PIN O2b (Group O) — MIXED `PARTITIONED BY (name, bucket(4, id))`: a non-empty overwrite
/// keeps BOTH slots on every committed file, the identity slot is the row's `name` and the
/// transform slot is the fork's `Bucket(4)` ordinal, and every old (name, bucket) pair is
/// gone. Reverting the transform to identity, or appending instead of overwriting, is RED.
#[tokio::test]
async fn overwrite_mixed_identity_and_transform_table_static_replace() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    register_source(
        &ctx,
        "o2b_old",
        &[(1, "alpha"), (2, "alpha"), (3, "beta"), (4, "beta")],
    );
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mx USING iceberg \
             PARTITIONED BY (name, bucket(4, id)) AS SELECT * FROM o2b_old",
    )
    .await
    .expect("mixed CTAS must succeed");

    let new_ids: Vec<i32> = vec![100, 200, 300];
    register_source(
        &ctx,
        "o2b_new",
        &[(100, "gamma"), (200, "gamma"), (300, "delta")],
    );
    execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.mx SELECT * FROM o2b_new",
    )
    .await
    .expect("non-empty INSERT OVERWRITE into a mixed-spec table must succeed");

    let table = loaded_table(&catalogs, "mx").await;
    let spec = table.metadata().default_partition_spec();
    let names: Vec<_> = spec.fields().iter().map(|f| f.name.clone()).collect();
    assert_eq!(names, vec!["name".to_string(), "id_bucket".to_string()]);
    assert_eq!(spec.fields()[1].transform, Transform::Bucket(4));

    let buckets = fork_buckets(&new_ids, 4);
    let expected: HashSet<(String, i32)> = vec![
        ("gamma".to_string(), buckets[0]),
        ("gamma".to_string(), buckets[1]),
        ("delta".to_string(), buckets[2]),
    ]
    .into_iter()
    .collect();

    let mut actual: HashSet<(String, i32)> = HashSet::new();
    let mut rows = 0;
    for file in &live_data_files(&table).await {
        assert_eq!(
            file.partition().fields().len(),
            2,
            "both partition slots on every committed file after the overwrite"
        );
        actual.insert((slot_str(file, 0), slot_int(file, 1)));
        rows += file.record_count();
    }
    assert_eq!(
        actual, expected,
        "mixed slots must be (identity name, fork Bucket(4) ordinal) for the NEW rows"
    );
    assert!(
        !actual
            .iter()
            .any(|(name, _)| name == "alpha" || name == "beta"),
        "STATIC overwrite must drop every old (name, bucket) partition: {actual:?}"
    );
    assert_eq!(rows, 3, "manifest record counts cover exactly the new rows");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.mx").await,
        vec![
            (100, "gamma".to_string()),
            (200, "gamma".to_string()),
            (300, "delta".to_string()),
        ]
    );
}

/// PIN O2c (Group O) — `truncate(3, name)` (the string-truncate transform, the remaining
/// in-scope transform family): a non-empty overwrite's files carry the 3-char prefix as the
/// partition slot, the old prefixes are gone (static replace), and the rows round-trip.
/// Reverting the transform to identity routes by the whole string and is RED.
#[tokio::test]
async fn overwrite_truncate_str_table_static_replace_and_prefix_routing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(
        &ctx,
        "o2c_old",
        &[(1, "apple"), (2, "apricot"), (3, "cherry")],
    );
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tr USING iceberg PARTITIONED BY (truncate(3, name)) AS \
             SELECT * FROM o2c_old",
    )
    .await
    .expect("truncate CTAS must succeed");

    register_source(&ctx, "o2c_new", &[(7, "xqaaa"), (8, "xqabb"), (9, "xqbcc")]);
    execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.tr SELECT * FROM o2c_new",
    )
    .await
    .expect("non-empty INSERT OVERWRITE into a truncate table must succeed");

    let table = loaded_table(&catalogs, "tr").await;
    let spec = table.metadata().default_partition_spec();
    assert_eq!(spec.fields()[0].name, "name_trunc");
    assert_eq!(spec.fields()[0].transform, Transform::Truncate(3));

    let mut actual: StdHashMap<String, u64> = StdHashMap::new();
    for file in &live_data_files(&table).await {
        *actual.entry(slot_str(file, 0)).or_insert(0) += file.record_count();
    }
    assert_eq!(
        actual,
        StdHashMap::from([("xqa".to_string(), 2), ("xqb".to_string(), 1)]),
        "slots must be the 3-char Iceberg truncate prefixes of the NEW names"
    );
    for old in ["app", "apr", "che"] {
        assert!(
            !actual.contains_key(old),
            "STATIC overwrite must drop old prefix partition {old}"
        );
    }
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tr").await,
        vec![
            (7, "xqaaa".to_string()),
            (8, "xqabb".to_string()),
            (9, "xqbcc".to_string()),
        ]
    );
}

/// A NULL computed partition value must land in the NULL partition slot, where `IS NULL` finds it.
///
/// Mutation: re-pinning the workspace to pre-Unit-1 makes this RED (no `None` slot /
/// `IS NULL` = 0). The CTAS control remains as a same-engine oracle that the splitter
/// path was always correct.
#[tokio::test]
async fn overwrite_null_partition_source_lands_in_null_slot() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_nullable_source(&ctx, "o7_old", &[(1, Some("a"))]);
    register_nullable_source(
        &ctx,
        "o7_new",
        &[(7, Some("g")), (8, Some("zz")), (9, Some("i"))],
    );
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nz USING iceberg PARTITIONED BY (bucket(4, name)) AS \
             SELECT * FROM o7_old",
    )
    .await
    .expect("bucket-on-name CTAS must succeed");

    execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.nz \
             SELECT id, CASE WHEN id = 8 THEN NULL ELSE name END AS name FROM o7_new",
    )
    .await
    .expect("computed NULL partition-source overwrite must succeed");

    let table = loaded_table(&catalogs, "nz").await;
    assert_eq!(
        nullable_int_slots(&table).await,
        vec![None, Some(2), Some(3)],
        "NULL partition-source value must occupy the NULL slot"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT id FROM ice.sales.nz").await,
        3,
        "all three rows present"
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.nz WHERE name IS NOT NULL"
        )
        .await,
        2
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.nz WHERE name IS NULL"
        )
        .await,
        1,
        "`IS NULL` must find the expression-derived NULL row"
    );

    // The CTAS control must match the provider path.
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nz_ctas USING iceberg PARTITIONED BY (bucket(4, name)) AS \
             SELECT id, CASE WHEN id = 8 THEN NULL ELSE name END AS name FROM o7_new",
    )
    .await
    .expect("CTAS with the same expression must succeed");
    assert_eq!(
        nullable_int_slots(&loaded_table(&catalogs, "nz_ctas").await).await,
        vec![None, Some(2), Some(3)],
        "CTAS control: same correct slot vector"
    );
}

/// FROM-less literal sources must succeed for partitioned tables and preserve payload and
/// partition correctness.
#[test]
fn overwrite_fromless_literal_source_succeeds_on_partitioned_table() {
    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Ok,
        Err,
        Panic,
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build a test runtime");

    let mut observed = Vec::new();
    for (shape, ddl) in [
        (
            "unpartitioned",
            "CREATE TABLE ice.sales.pk AS SELECT * FROM pk_old",
        ),
        (
            "partitioned",
            "CREATE TABLE ice.sales.pk USING iceberg PARTITIONED BY (id) AS \
                 SELECT * FROM pk_old",
        ),
    ] {
        for (form, statement) in [
            (
                "fromless-literal",
                "INSERT OVERWRITE ice.sales.pk \
                     SELECT 11 AS id, CAST('z' AS STRING) AS name",
            ),
            ("values", "INSERT OVERWRITE ice.sales.pk VALUES (11, 'z')"),
        ] {
            let wh = TempDir::new().unwrap();
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                runtime.block_on(async {
                    let (ctx, catalogs) = setup(&wh).await;
                    register_source(&ctx, "pk_old", &[(1, "a"), (2, "b")]);
                    execute(&ctx, &catalogs, ddl).await.expect("ctas");
                    execute(&ctx, &catalogs, statement).await.map(|_| ())?;
                    // Payload must be the FROM-less row (static whole-table replace).
                    let got = table_rows(&ctx, &catalogs, "ice.sales.pk").await;
                    assert_eq!(
                        got,
                        vec![(11, "z".to_string())],
                        "{shape}/{form}: written rows must be [(11, z)], got {got:?}"
                    );
                    if shape == "partitioned" {
                        // Identity on `id` — recorded partition tuple is the written id.
                        let slots = live_data_files(&loaded_table(&catalogs, "pk").await)
                            .await
                            .iter()
                            .map(|file| slot_int(file, 0))
                            .collect::<Vec<_>>();
                        assert_eq!(
                            slots,
                            vec![11],
                            "{shape}/{form}: partition slot must be id=11, got {slots:?}"
                        );
                    }
                    Ok::<(), datafusion::error::DataFusionError>(())
                })
            }));
            observed.push((
                format!("{shape}/{form}"),
                match outcome {
                    Ok(Ok(())) => Outcome::Ok,
                    Ok(Err(_)) => Outcome::Err,
                    Err(_) => Outcome::Panic,
                },
            ));
        }
    }

    let observed: Vec<(&str, &Outcome)> = observed
        .iter()
        .map(|(label, outcome)| (label.as_str(), outcome))
        .collect();
    assert_eq!(
        observed,
        vec![
            ("unpartitioned/fromless-literal", &Outcome::Ok),
            ("unpartitioned/values", &Outcome::Ok),
            ("partitioned/fromless-literal", &Outcome::Ok),
            ("partitioned/values", &Outcome::Ok),
        ],
        "post-Unit-1/G0: FROM-less literal INSERT OVERWRITE must succeed on partitioned \
             and unpartitioned targets; VALUES remains fine everywhere"
    );
}

/// O8 companion — FROM-less literal into a PARTITIONED table whose partition column is
/// **optional** (nullable). Unit-1 alone still rejected optional-column tables; Unit-2
/// G0 nullability widening makes this `Ok` at pin `a08a0957`. Schema-aware expectation
/// per the fork work order. Asserts payload **and** recorded identity partition tuple.
#[tokio::test]
async fn fromless_literal_into_optional_partition_column_succeeds() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_nullable_source(&ctx, "opt_old", &[(1, Some("a")), (2, Some("b"))]);
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt USING iceberg PARTITIONED BY (name) AS \
             SELECT * FROM opt_old",
    )
    .await
    .expect("identity-on-nullable-name CTAS");
    execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.opt \
             SELECT 11 AS id, CAST('z' AS STRING) AS name",
    )
    .await
    .expect("FROM-less literal into optional partition column must succeed at a08a0957");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.opt").await,
        vec![(11, "z".to_string())],
        "payload must be the FROM-less row"
    );
    let mut slots: Vec<String> = live_data_files(&loaded_table(&catalogs, "opt").await)
        .await
        .iter()
        .map(|file| slot_str(file, 0))
        .collect();
    slots.sort();
    assert_eq!(
        slots,
        vec!["z".to_string()],
        "recorded identity partition tuple must be the written name"
    );
}

/// PIN O3 (Group O) — an EMPTY `INSERT OVERWRITE` into a transform-partitioned table still
/// wipes (the C1-Q-001 empty-overwrite wipe intercept, which exists because the fork's provider
/// would silent-no-op an empty write), and the table SURVIVES with its transform spec
/// intact — a wipe must not degrade `bucket(4, id)` to unpartitioned or drop the table.
/// Removing the empty-source intercept in `execute_insert_overwrite` is RED.
#[tokio::test]
async fn empty_overwrite_on_transform_table_wipes_and_keeps_spec() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "o3_src", &[(1, "a"), (2, "b"), (3, "c"), (5, "e")]);
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bkt USING iceberg PARTITIONED BY (bucket(4, id)) AS \
             SELECT * FROM o3_src",
    )
    .await
    .expect("bucket CTAS must succeed");
    assert!(
        !int_slot_counts(&loaded_table(&catalogs, "bkt").await)
            .await
            .is_empty(),
        "precondition: the bucket table has live partitioned files"
    );

    execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.bkt SELECT * FROM o3_src WHERE false",
    )
    .await
    .expect("empty INSERT OVERWRITE on a transform table must wipe, not error");

    let table = loaded_table(&catalogs, "bkt").await;
    let spec = table.metadata().default_partition_spec();
    assert_eq!(
        spec.fields()[0].transform,
        Transform::Bucket(4),
        "the wipe must leave the transform spec intact"
    );
    assert_eq!(spec.fields()[0].name, "id_bucket");
    assert_eq!(
        int_slot_counts(&table).await,
        StdHashMap::new(),
        "no live data file may survive the empty-overwrite wipe"
    );
    assert!(
        table_rows(&ctx, &catalogs, "ice.sales.bkt")
            .await
            .is_empty(),
        "empty INSERT OVERWRITE must wipe every row on a transform table too"
    );
}

/// PIN O4 (Group O) — the C5-Q-001 / O4-C2-Q-001 assignment check still fires on a
/// TRANSFORM-partitioned target: an empty overwrite whose source types (or arity) do not
/// match must fail LOUD and leave the prior rows AND their partition routing untouched.
/// The fail-open here would be the worst outcome in the group — a full-table wipe of a
/// partitioned table for a statement that a non-empty run would have rejected at cast.
/// Skipping `assert_empty_overwrite_types_assignment_compatible` is RED.
#[tokio::test]
async fn empty_overwrite_type_mismatch_on_transform_table_does_not_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let ids: Vec<i32> = vec![1, 2, 3, 5];
    let rows: Vec<(i32, &str)> = ids.iter().map(|&id| (id, "keep")).collect();
    register_source(&ctx, "o4_src", &rows);
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bkt USING iceberg PARTITIONED BY (bucket(4, id)) AS \
             SELECT * FROM o4_src",
    )
    .await
    .expect("bucket CTAS must succeed");
    let before = int_slot_counts(&loaded_table(&catalogs, "bkt").await).await;
    assert_eq!(before, counts_of(&fork_buckets(&ids, 4)));

    // Same arity, incompatible types (Utf8 → Int32): only fails at cast when rows exist.
    let type_error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.bkt SELECT 'x' AS id, 'y' AS name WHERE false",
    )
    .await
    .expect_err("type-mismatch empty overwrite must be refused on a transform table");
    assert!(
        type_error.to_string().contains("not assignment-compatible"),
        "must be the assignment-compat refusal, got: {type_error}"
    );

    // Wrong arity — refused one step earlier, by the C5-Q-001 plan-validate
    // (`ctx.sql(INSERT …)`), before the assignment check is even reached.
    let arity_error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.bkt SELECT 'only' AS id WHERE false",
    )
    .await
    .expect_err("arity-mismatch empty overwrite must be refused on a transform table");
    assert!(
        arity_error
            .to_string()
            .contains("Column count doesn't match"),
        "must be the plan-validate arity refusal, got: {arity_error}"
    );

    let table = loaded_table(&catalogs, "bkt").await;
    assert_eq!(
        int_slot_counts(&table).await,
        before,
        "a refused empty overwrite must not touch the committed partition files"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.bkt").await,
        vec![
            (1, "keep".to_string()),
            (2, "keep".to_string()),
            (3, "keep".to_string()),
            (5, "keep".to_string()),
        ],
        "the prior rows survive a refused empty overwrite"
    );
}

/// PIN O5 (Group O) — `INSERT OVERWRITE … PARTITION (…)` on a TRANSFORM-partitioned table
/// is still a loud `NotImplemented`, empty source AND non-empty source, and neither form
/// touches a row. A transform table is exactly where a silently-degraded partition
/// overwrite would be most destructive (the `PARTITION (id = 1)` predicate does not even
/// name a partition field of a `bucket(4, id)` spec). Dropping the `insert.partitioned`
/// reject is RED.
#[tokio::test]
async fn overwrite_partition_clause_on_transform_table_still_rejected() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let ids: Vec<i32> = vec![1, 2, 3, 5];
    let rows: Vec<(i32, &str)> = ids.iter().map(|&id| (id, "keep")).collect();
    register_source(&ctx, "o5_src", &rows);
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bkt USING iceberg PARTITIONED BY (bucket(4, id)) AS \
             SELECT * FROM o5_src",
    )
    .await
    .expect("bucket CTAS must succeed");
    let before = int_slot_counts(&loaded_table(&catalogs, "bkt").await).await;

    for sql in [
        "INSERT OVERWRITE ice.sales.bkt PARTITION (id = 1) SELECT * FROM o5_src \
             WHERE false",
        "INSERT OVERWRITE ice.sales.bkt PARTITION (id = 1) SELECT * FROM o5_src \
             WHERE id = 1",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("PARTITION (…) overwrite must be refused on a transform table");
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "must stay a typed NotImplemented, got: {error}"
        );
        assert!(
            error.to_string().contains("PARTITION"),
            "the message must name the unsupported PARTITION form, got: {error}"
        );
    }

    assert_eq!(
        int_slot_counts(&loaded_table(&catalogs, "bkt").await).await,
        before,
        "a refused PARTITION overwrite must not touch the committed files"
    );
    assert_eq!(table_rows(&ctx, &catalogs, "ice.sales.bkt").await.len(), 4);
}

/// PIN O6 (Group O) — regression: the IDENTITY-partitioned overwrite keeps the same static
/// whole-table-replace semantics (old identity partitions gone, new files carry their own
/// identity slot) and the UNPARTITIONED overwrite still replaces all rows. The transform
/// work must not have shifted either. (The unpartitioned round-trip is also pinned by
/// `insert_overwrite_replaces_all`; this asserts it beside the partitioned case so a
/// single mutation of the overwrite path shows up in one place.)
#[tokio::test]
async fn overwrite_identity_and_unpartitioned_regression() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "o6_old", &[(1, "a"), (2, "b"), (3, "c")]);
    register_source(&ctx, "o6_new", &[(7, "g"), (8, "h")]);

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.idp USING iceberg PARTITIONED BY (id) AS \
             SELECT * FROM o6_old",
    )
    .await
    .expect("identity CTAS must succeed");
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.up AS SELECT * FROM o6_old",
    )
    .await
    .expect("unpartitioned CTAS must succeed");

    execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.idp SELECT * FROM o6_new",
    )
    .await
    .expect("identity-partitioned overwrite must succeed");
    execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.up SELECT * FROM o6_new",
    )
    .await
    .expect("unpartitioned overwrite must succeed");

    let identity = loaded_table(&catalogs, "idp").await;
    assert_eq!(
        identity.metadata().default_partition_spec().fields()[0].transform,
        Transform::Identity
    );
    assert_eq!(
        int_slot_counts(&identity).await,
        StdHashMap::from([(7, 1), (8, 1)]),
        "identity overwrite: only the NEW keys have live files (static replace)"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.idp").await,
        vec![(7, "g".to_string()), (8, "h".to_string())]
    );

    let unpartitioned = loaded_table(&catalogs, "up").await;
    assert!(
        unpartitioned
            .metadata()
            .default_partition_spec()
            .is_unpartitioned()
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.up").await,
        vec![(7, "g".to_string()), (8, "h".to_string())]
    );
}

/// ===================================================================================
/// Provider partition correctness on the public router path.
/// ===================================================================================
mod provider_partition_correctness {
    use super::*;

    fn register_ab_source(ctx: &SessionContext, name: &str, rows: &[(&str, &str)]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        ctx.register_batch(name, batch).unwrap();
    }

    fn fork_string_buckets(values: &[&str], num_buckets: u32) -> Vec<i32> {
        let transform = create_transform_function(&Transform::Bucket(num_buckets))
            .expect("bucket transform fn");
        let out = transform
            .transform(Arc::new(StringArray::from(values.to_vec())))
            .expect("apply bucket transform");
        let out = out.as_primitive::<Int32Type>();
        (0..out.len()).map(|row| out.value(row)).collect()
    }

    fn distinct_slots(ordinals: &[i32]) -> Vec<Option<i32>> {
        let mut slots: Vec<Option<i32>> = ordinals.iter().copied().map(Some).collect();
        slots.sort_unstable();
        slots.dedup();
        slots
    }

    async fn distinct_committed_slots(table: &Table) -> Vec<Option<i32>> {
        let mut slots = nullable_int_slots(table).await;
        slots.dedup();
        slots
    }

    async fn string_slots(table: &Table) -> Vec<String> {
        let mut slots: Vec<String> = live_data_files(table)
            .await
            .iter()
            .map(|file| slot_str(file, 0))
            .collect();
        slots.sort();
        slots
    }

    async fn ab_rows(
        ctx: &SessionContext,
        catalogs: &CatalogRegistry,
        table: &str,
    ) -> Vec<(String, String)> {
        let batches = execute(
            ctx,
            catalogs,
            &format!("SELECT a, b FROM {table} ORDER BY a"),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let mut rows = Vec::new();
        for batch in &batches {
            let first = batch.column(0).as_string::<i32>();
            let second = batch.column(1).as_string::<i32>();
            for index in 0..batch.num_rows() {
                rows.push((
                    first.value(index).to_string(),
                    second.value(index).to_string(),
                ));
            }
        }
        rows
    }

    /// Computed partition-source column commits the post-expression tuple.
    #[tokio::test]
    async fn computed_partition_source_commits_post_expression_tuple() {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        register_nullable_source(&ctx, "aa6_old", &[(1, Some("a")), (2, Some("b"))]);
        register_nullable_source(&ctx, "aa6_new", &[(7, Some("g")), (8, Some("h"))]);
        execute(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.aa6 USING iceberg PARTITIONED BY (bucket(4, name)) \
                 AS SELECT * FROM aa6_old",
        )
        .await
        .expect("bucket-on-name CTAS must succeed");
        execute(
            &ctx,
            &catalogs,
            "INSERT OVERWRITE ice.sales.aa6 SELECT id, concat(name, 'X') AS name \
                 FROM aa6_new",
        )
        .await
        .expect("computed partition-source overwrite must succeed");

        let computed = distinct_slots(&fork_string_buckets(&["gX", "hX"], 4));
        let pre_expression = distinct_slots(&fork_string_buckets(&["g", "h"], 4));
        assert_ne!(
            computed, pre_expression,
            "fixture precondition: candidate slot vectors must differ"
        );

        let table = loaded_table(&catalogs, "aa6").await;
        assert_eq!(
            distinct_committed_slots(&table).await,
            computed,
            "committed bucket ordinals must be Bucket(4) over the POST-concat names"
        );
        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.aa6").await,
            vec![(7, "gX".to_string()), (8, "hX".to_string())],
            "data columns carry the computed values"
        );
    }

    /// Identity-partitioned reads use the recorded tuple and return the computed expression.
    #[tokio::test]
    async fn identity_partitioned_computed_source_read_back_matches_expression() {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        register_nullable_source(&ctx, "aa6i_old", &[(1, Some("a")), (2, Some("b"))]);
        register_nullable_source(&ctx, "aa6i_new", &[(7, Some("g")), (8, Some("h"))]);
        execute(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.aa6i USING iceberg PARTITIONED BY (name) AS \
                 SELECT * FROM aa6i_old",
        )
        .await
        .expect("identity-on-name CTAS must succeed");
        execute(
            &ctx,
            &catalogs,
            "INSERT OVERWRITE ice.sales.aa6i SELECT id, concat(name, 'X') AS name \
                 FROM aa6i_new",
        )
        .await
        .expect("computed identity partition-source overwrite must succeed");

        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.aa6i").await,
            vec![(7, "gX".to_string()), (8, "hX".to_string())],
            "served rows must match the computed expression (correct recorded tuple)"
        );
        assert_eq!(
            string_slots(&loaded_table(&catalogs, "aa6i").await).await,
            vec!["gX".to_string(), "hX".to_string()],
            "recorded partition tuple must be the post-expression values"
        );
    }

    /// Column reorder through the provider writes the permuted values.
    #[tokio::test]
    async fn reordered_same_typed_columns_write_the_permuted_values() {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        register_ab_source(&ctx, "aa6r_old", &[("p", "q")]);
        register_ab_source(&ctx, "aa6r_new", &[("w", "x"), ("y", "z")]);
        execute(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.aa6r USING iceberg PARTITIONED BY (a) AS \
                 SELECT * FROM aa6r_old",
        )
        .await
        .expect("identity-on-a CTAS must succeed");
        execute(
            &ctx,
            &catalogs,
            "INSERT OVERWRITE ice.sales.aa6r SELECT b, a FROM aa6r_new",
        )
        .await
        .expect("reordered SELECT must succeed");

        assert_eq!(
            ab_rows(&ctx, &catalogs, "ice.sales.aa6r").await,
            vec![
                ("x".to_string(), "w".to_string()),
                ("z".to_string(), "y".to_string()),
            ],
            "permutation must be honoured: SELECT b, a into (a, b)"
        );
        assert_eq!(
            string_slots(&loaded_table(&catalogs, "aa6r").await).await,
            vec!["x".to_string(), "z".to_string()],
            "partition tuple follows column a after the permutation"
        );
    }

    /// FROM-less literal INSERT into a partitioned target succeeds (was O8 panic).
    /// Payload + identity partition slot — not merely row count (CCC Q-002 / L-001).
    #[tokio::test]
    async fn fromless_literal_into_partitioned_target_succeeds() {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        register_source(&ctx, "aa5b_old", &[(1, "a")]);
        execute(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.aa5b USING iceberg PARTITIONED BY (id) AS \
                 SELECT * FROM aa5b_old",
        )
        .await
        .expect("ctas");
        execute(
            &ctx,
            &catalogs,
            "INSERT OVERWRITE ice.sales.aa5b \
                 SELECT 11 AS id, CAST('z' AS STRING) AS name",
        )
        .await
        .expect("FROM-less literal into partitioned target must succeed");
        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.aa5b").await,
            vec![(11, "z".to_string())],
            "payload must be the FROM-less row"
        );
        let slots = live_data_files(&loaded_table(&catalogs, "aa5b").await)
            .await
            .iter()
            .map(|file| slot_int(file, 0))
            .collect::<Vec<_>>();
        assert_eq!(slots, vec![11], "identity partition slot must be id=11");
    }

    /// INSERT INTO with a computed partition-source column preserves post-expression slots.
    #[tokio::test]
    async fn computed_partition_source_insert_into_commits_post_expression_tuple() {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        register_nullable_source(&ctx, "into_old", &[(1, Some("a"))]);
        register_nullable_source(&ctx, "into_new", &[(7, Some("g")), (8, Some("h"))]);
        execute(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.into_bkt USING iceberg PARTITIONED BY (bucket(4, name)) \
                 AS SELECT * FROM into_old",
        )
        .await
        .expect("bucket CTAS");
        execute(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.into_bkt SELECT id, concat(name, 'X') AS name \
                 FROM into_new",
        )
        .await
        .expect("computed partition-source INSERT INTO must succeed");

        let computed = distinct_slots(&fork_string_buckets(&["gX", "hX"], 4));
        // Append leaves the seed row's slot too — only assert the written rows' values.
        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.into_bkt").await,
            vec![
                (1, "a".to_string()),
                (7, "gX".to_string()),
                (8, "hX".to_string()),
            ],
        );
        let slots = distinct_committed_slots(&loaded_table(&catalogs, "into_bkt").await).await;
        for slot in &computed {
            assert!(
                slots.contains(slot),
                "post-expression slot {slot:?} must be present among {slots:?}"
            );
        }
    }
}
