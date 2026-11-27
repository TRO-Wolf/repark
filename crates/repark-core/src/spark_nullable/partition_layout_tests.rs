use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_expr::{Operator, binary_expr};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{DataFrame, Expr, cast, col, in_list, lit};

use crate::ReparkSession;

fn write_rows(path: &Path, rows: &[(i64, &str)]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, text)| *text).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn write_zero(path: &Path) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, b"").unwrap();
}

async fn read(
    session: &ReparkSession,
    root: &Path,
    name: &str,
) -> repark_common::Result<DataFrame> {
    session
        .read_parquet(root.join(name).to_str().unwrap())
        .await
}

async fn row_count(frame: DataFrame) -> usize {
    frame
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

async fn refusal(session: &ReparkSession, root: &Path, name: &str) -> String {
    read(session, root, name).await.unwrap_err().to_string()
}

async fn filtered(
    session: &ReparkSession,
    root: &Path,
    name: &str,
    predicate: Expr,
) -> datafusion::error::Result<usize> {
    let frame = read(session, root, name).await.unwrap().filter(predicate)?;
    let batches = frame.collect().await?;
    Ok(batches.iter().map(RecordBatch::num_rows).sum())
}

fn cast_int(name: &str) -> Expr {
    cast(col(name), DataType::Int32)
}

#[tokio::test]
async fn base_path_rule_refuses_conflicting_directory_structures() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let session = ReparkSession::new().unwrap();
    write_rows(&root.join("bases/sub1/a=1/part-0.parquet"), &[(1, "a")]);
    write_rows(&root.join("bases/sub2/a=2/part-0.parquet"), &[(2, "b")]);
    write_rows(&root.join("rootsub/a=1/part-0.parquet"), &[(1, "a")]);
    write_rows(&root.join("rootsub/sub/a=2/part-0.parquet"), &[(2, "b")]);
    write_rows(&root.join("subfile/p=1/good.parquet"), &[(1, "a")]);
    write_rows(&root.join("subfile/sub/x.parquet"), &[(99, "s")]);
    write_rows(&root.join("subzero/p=1/good.parquet"), &[(1, "a")]);
    write_zero(&root.join("subzero/sub/empty.parquet"));
    write_rows(
        &root.join("diffmid/a=1/sub/b=2/part-0.parquet"),
        &[(1, "a")],
    );
    write_rows(
        &root.join("diffmid/a=2/sub/b=3/part-0.parquet"),
        &[(2, "b")],
    );
    write_rows(&root.join("dirsfirst/a=1/part-0.parquet"), &[(1, "a")]);
    write_rows(&root.join("dirsfirst/sub/b=2/part-0.parquet"), &[(2, "b")]);
    for name in [
        "bases",
        "rootsub",
        "subfile",
        "subzero",
        "diffmid",
        "dirsfirst",
    ] {
        let message = refusal(&session, root, name).await;
        assert!(
            message.contains("[CONFLICTING_DIRECTORY_STRUCTURES]") && message.contains("KD009"),
            "{name}: {message}"
        );
    }
    let bases_message = refusal(&session, root, "bases").await;
    assert!(bases_message.contains("/bases/sub1\n\t"), "{bases_message}");

    write_rows(&root.join("rootfile/p=1/good.parquet"), &[(1, "a")]);
    write_rows(&root.join("rootfile/root.parquet"), &[(99, "r")]);
    write_rows(&root.join("hiddensub/p=1/good.parquet"), &[(1, "a")]);
    write_rows(&root.join("hiddensub/sub/_hidden.parquet"), &[(99, "h")]);
    write_rows(&root.join("basefile/s1/a=1/part-0.parquet"), &[(1, "a")]);
    write_rows(&root.join("basefile/s1/x.parquet"), &[(99, "x")]);
    for name in ["rootfile", "hiddensub", "basefile"] {
        let frame = read(&session, root, name).await.unwrap();
        assert_eq!(frame.schema().fields().len(), 3, "{name}");
        assert_eq!(row_count(frame).await, 1, "{name}");
    }

    write_rows(
        &root.join("samemid/a=1/sub/b=2/part-0.parquet"),
        &[(1, "a")],
    );
    write_rows(
        &root.join("samemid/a=1/sub/b=3/part-0.parquet"),
        &[(2, "b")],
    );
    write_rows(&root.join("casebase/Sub/a=1/part-0.parquet"), &[(1, "a")]);
    write_rows(&root.join("casebase/sub/a=2/part-0.parquet"), &[(2, "b")]);
    for (name, column) in [("samemid", "b"), ("casebase", "a")] {
        let frame = read(&session, root, name).await.unwrap();
        assert_eq!(frame.schema().field(2).name(), column, "{name}");
        assert_eq!(row_count(frame).await, 2, "{name}");
    }
}

#[tokio::test]
async fn zero_byte_footer_follows_spark_path_order() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let session = ReparkSession::new().unwrap();
    write_zero(&root.join("rootfirst/empty.parquet"));
    write_rows(&root.join("rootfirst/p=1/good.parquet"), &[(1, "a")]);
    write_zero(&root.join("caseorder/p=1/Z.parquet"));
    write_rows(&root.join("caseorder/p=1/a.parquet"), &[(1, "a")]);
    write_zero(&root.join("unpartfirst/empty.parquet"));
    write_rows(&root.join("unpartfirst/good.parquet"), &[(1, "a")]);
    for name in ["rootfirst", "caseorder", "unpartfirst"] {
        let message = refusal(&session, root, name).await;
        assert!(
            message.contains("[FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER]"),
            "{name}: {message}"
        );
    }

    write_zero(&root.join("rootlast/zzz.parquet"));
    write_rows(&root.join("rootlast/p=1/good.parquet"), &[(1, "a")]);
    write_rows(&root.join("partlater/p=1/good.parquet"), &[(1, "a")]);
    write_zero(&root.join("partlater/p=2/empty.parquet"));
    write_rows(&root.join("unpartlast/good.parquet"), &[(1, "a")]);
    write_zero(&root.join("unpartlast/zzz.parquet"));
    write_rows(&root.join("hiddenlast/_h.parquet"), &[(99, "h")]);
    write_rows(&root.join("hiddenlast/good.parquet"), &[(1, "a")]);
    write_zero(&root.join("hiddenlast/zzz.parquet"));
    for name in ["rootlast", "partlater", "unpartlast", "hiddenlast"] {
        let frame = read(&session, root, name).await.unwrap();
        assert_eq!(row_count(frame).await, 1, "{name}");
    }

    write_rows(&root.join("zeroleaf/p=1/good.parquet"), &[(1, "a")]);
    write_zero(&root.join("zeroleaf/p=x/empty.parquet"));
    let zeroleaf = read(&session, root, "zeroleaf").await.unwrap();
    assert_eq!(zeroleaf.schema().field(2).data_type(), &DataType::Utf8);
    assert_eq!(row_count(zeroleaf).await, 1);

    write_rows(&root.join("rootschema/a.parquet"), &[(99, "r")]);
    write_zero(&root.join("rootschema/p=1/empty.parquet"));
    let rootschema = read(&session, root, "rootschema").await.unwrap();
    assert_eq!(rootschema.schema().fields().len(), 3);
    assert_eq!(rootschema.schema().field(2).data_type(), &DataType::Int32);
    assert_eq!(row_count(rootschema).await, 0);

    write_zero(&root.join("namesfirst/p=1/empty.parquet"));
    write_rows(&root.join("namesfirst/q=2/good.parquet"), &[(2, "b")]);
    let names = refusal(&session, root, "namesfirst").await;
    assert!(
        names.contains("[CONFLICTING_PARTITION_COLUMN_NAMES]"),
        "{names}"
    );
}

#[tokio::test]
async fn constant_propagation_prunes_before_raising() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let session = ReparkSession::new().unwrap();
    write_rows(&root.join("s1x/p=1/part-0.parquet"), &[(1, "one")]);
    write_rows(&root.join("s1x/p=x/part-0.parquet"), &[]);
    write_rows(&root.join("ab/a=1/b=1/part-0.parquet"), &[(1, "one")]);
    write_rows(&root.join("ab/a=x/b=1/part-0.parquet"), &[]);
    write_rows(&root.join("ab/a=1/b=2/part-0.parquet"), &[(3, "three")]);

    let answered = [
        (cast_int("p").eq(lit(1i32)).and(col("p").eq(lit("1"))), 1),
        (cast_int("p").eq(lit(1i32)).and(col("p").eq(lit("99"))), 0),
        (cast_int("p").eq(lit(1i32)).and(lit("1").eq(col("p"))), 1),
        (
            cast_int("p").eq(lit(1i32)).and(binary_expr(
                col("p"),
                Operator::IsNotDistinctFrom,
                lit("1"),
            )),
            1,
        ),
        (
            cast_int("p")
                .eq(lit(1i32))
                .and(col("p").eq(lit("1")))
                .or(col("p").eq(lit("2"))),
            1,
        ),
        (
            cast_int("p")
                .eq(lit(1i32))
                .and(col("p").eq(lit("1")))
                .and(col("p").eq(lit("2"))),
            0,
        ),
        (
            cast_int("p")
                .eq(lit(1i32))
                .and(col("id").gt_eq(lit(0i64)))
                .and(col("p").eq(lit("1"))),
            1,
        ),
    ];
    for (predicate, expected) in answered {
        let shown = predicate.to_string();
        assert_eq!(
            filtered(&session, root, "s1x", predicate).await.unwrap(),
            expected,
            "{shown}"
        );
    }

    let raised = [
        cast_int("p").eq(lit(1i32)).and(col("p").eq(lit("y"))),
        col("p").eq(lit("y")).and(cast_int("p").eq(lit(1i32))),
        in_list(cast_int("p"), vec![lit(1i32), lit(2i32)], false).and(col("p").eq(lit("1"))),
        col("p")
            .not_eq(lit("x"))
            .and(cast_int("p").eq(lit(1i32)))
            .and(col("p").eq(lit("x"))),
        cast_int("p").eq(lit(1i32)).and(col("p").not_eq(lit("x"))),
        cast_int("p")
            .eq(lit(1i32))
            .and(col("p").eq(lit("1")).or(col("p").eq(lit("2")))),
    ];
    for predicate in raised {
        let shown = predicate.to_string();
        let error = filtered(&session, root, "s1x", predicate)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Cast error"), "{shown}: {error}");
    }

    let two_bound = cast_int("a")
        .eq(lit(1i32))
        .and(col("a").eq(lit("1")))
        .and(col("b").eq(lit(1i32)));
    assert_eq!(filtered(&session, root, "ab", two_bound).await.unwrap(), 1);
    let partial = cast_int("a").eq(col("b")).and(col("a").eq(lit("1")));
    assert_eq!(filtered(&session, root, "ab", partial).await.unwrap(), 1);
    let guard = cast_int("a").eq(lit(1i32)).and(col("b").eq(lit(1i32)));
    assert!(filtered(&session, root, "ab", guard).await.is_err());
    let data_bound = cast(cast_int("p"), DataType::Int64)
        .eq(col("id"))
        .and(col("id").eq(lit(1i64)));
    assert!(filtered(&session, root, "s1x", data_bound).await.is_err());
}

fn write_text(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, text).unwrap();
}

fn text_options(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn field_names(frame: &DataFrame) -> Vec<String> {
    frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

fn write_text_table(root: &Path, extension: &str, header: &str, rows: [&str; 2]) {
    for (index, row) in rows.iter().enumerate() {
        let body = format!("{header}{row}\n");
        write_text(
            &root.join(format!("p={}/part-{index}{extension}", index + 1)),
            &body,
        );
    }
}

#[tokio::test]
async fn csv_and_json_partitioned_reads_share_discovery() {
    let dir = tempfile::tempdir().unwrap();
    let session = ReparkSession::new().unwrap();
    let csv = dir.path().join("csv");
    write_text_table(&csv, ".csv", "id,s\n", ["1,a", "2,b"]);
    write_text(&csv.join("p=1/README"), "id,s\n9,z\n");
    write_text(&csv.join("p=1/_hidden.csv"), "id,s\n8,y\n");
    write_text(&csv.join("_temporary/0/part.csv"), "id,s\n7,x\n");
    write_zero(&csv.join("p=3/empty.csv"));
    let header = text_options(&[("header", "true"), ("text_scan", "header")]);
    let frame = session
        .read_csv(csv.to_str().unwrap(), &header)
        .await
        .unwrap();
    assert_eq!(field_names(&frame), ["id", "s", "p"]);
    assert_eq!(
        frame.schema().field(2).data_type(),
        &DataType::Int32,
        "p infers int"
    );
    assert_eq!(super::partition::partition_column_names(&frame), ["p"]);
    let plan = frame.clone().create_physical_plan().await.unwrap();
    let listed = crate::plan_introspect::input_files(&plan);
    assert!(listed.iter().any(|name| name.ends_with("p=3/empty.csv")));
    assert_eq!(listed.len(), 3);
    assert_eq!(row_count(frame.clone()).await, 2);
    let pruned = frame
        .filter(binary_expr(col("p"), Operator::Eq, lit(2)))
        .unwrap();
    assert_eq!(row_count(pruned).await, 1);
    let no_header = text_options(&[("header", "false"), ("text_scan", "header")]);
    let bare = dir.path().join("bare");
    write_text_table(&bare, ".csv", "", ["1,a", "2,b"]);
    let frame = session
        .read_csv(bare.to_str().unwrap(), &no_header)
        .await
        .unwrap();
    assert_eq!(field_names(&frame), ["_c0", "_c1", "p"]);
    let json = dir.path().join("json");
    write_text_table(
        &json,
        ".json",
        "",
        [r#"{"id":1,"s":"a"}"#, r#"{"id":2,"s":"b"}"#],
    );
    let frame = session
        .read_json(json.to_str().unwrap(), &text_options(&[]))
        .await
        .unwrap();
    assert_eq!(field_names(&frame), ["id", "s", "p"]);
    assert_eq!(row_count(frame).await, 2);
}

#[tokio::test]
async fn csv_and_json_unpartitioned_reads_keep_the_listing_read() {
    let dir = tempfile::tempdir().unwrap();
    let session = ReparkSession::new().unwrap();
    let flat = dir.path().join("flat");
    write_text(&flat.join("part-0.csv"), "id,s\n1,a\n");
    write_text(&flat.join("part-1.csv"), "id,s\n2,b\n");
    let header = text_options(&[("header", "true"), ("text_scan", "header")]);
    let frame = session
        .read_csv(flat.to_str().unwrap(), &header)
        .await
        .unwrap();
    assert_eq!(field_names(&frame), ["id", "s"]);
    assert!(super::partition::partition_column_names(&frame).is_empty());
    assert_eq!(row_count(frame).await, 2);
    let json = dir.path().join("json");
    write_text(&json.join("part-0.json"), "{\"id\":1,\"s\":\"a\"}\n");
    write_text(&json.join("part-1.json"), "{\"id\":2,\"s\":\"b\"}\n");
    let frame = session
        .read_json(json.to_str().unwrap(), &text_options(&[]))
        .await
        .unwrap();
    assert_eq!(row_count(frame).await, 2);
    let overlap = dir.path().join("overlap");
    write_text(&overlap.join("p=10/part-0.csv"), "id,p,s\n1,99,x\n");
    let frame = session
        .read_csv(overlap.to_str().unwrap(), &header)
        .await
        .unwrap();
    assert_eq!(field_names(&frame), ["id", "p", "s"]);
    assert!(super::partition::partition_column_names(&frame).is_empty());
    let table = dir.path().join("table");
    write_text_table(&table, ".csv", "id,s\n", ["1,a", "2,b"]);
    let positions = text_options(&[("header", "true"), ("text_scan", "positions")]);
    let frame = session
        .read_csv(table.to_str().unwrap(), &positions)
        .await
        .unwrap();
    assert_eq!(field_names(&frame), ["_c0", "_c1"]);
    let null_value = text_options(&[
        ("header", "true"),
        ("text_scan", "header"),
        ("nullvalue", "b"),
    ]);
    let frame = session
        .read_csv(table.to_str().unwrap(), &null_value)
        .await
        .unwrap();
    assert_eq!(field_names(&frame), ["id", "s"]);
    let reread = text_options(&[("header", "true"), ("utf8_columns", "id")]);
    let frame = session
        .read_csv(table.to_str().unwrap(), &reread)
        .await
        .unwrap();
    assert_eq!(field_names(&frame), ["id", "s", "p"]);
    assert_eq!(frame.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(super::partition::partition_column_names(&frame), ["p"]);
    assert_eq!(row_count(frame).await, 2);
}

#[tokio::test]
async fn json_overlap_takes_the_directory_value_and_case_rule() {
    let dir = tempfile::tempdir().unwrap();
    let session = ReparkSession::new().unwrap();
    let overlap = dir.path().join("overlap");
    write_text(
        &overlap.join("p=10/part-0.json"),
        "{\"id\":1,\"p\":99,\"s\":\"x\"}\n",
    );
    let frame = session
        .read_json(overlap.to_str().unwrap(), &text_options(&[]))
        .await
        .unwrap();
    assert_eq!(field_names(&frame), ["id", "p", "s"]);
    assert_eq!(frame.schema().field(1).data_type(), &DataType::Int32);
    let mixed = dir.path().join("mixed");
    write_text(&mixed.join("p=1/part-0.json"), "{\"id\":1}\n");
    write_text(&mixed.join("P=2/part-1.json"), "{\"id\":2}\n");
    let path = mixed.to_str().unwrap();
    let options = text_options(&[]);
    let folded = session
        .read_json_with_case_sensitivity(path, &options, false)
        .await
        .unwrap();
    assert_eq!(field_names(&folded), ["id", "p"]);
    let refused = session
        .read_json_with_case_sensitivity(path, &options, true)
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("[CONFLICTING_PARTITION_COLUMN_NAMES]"));
}

#[tokio::test]
async fn root_files_beside_partitions_are_listed_but_not_read() {
    let dir = tempfile::tempdir().unwrap();
    let session = ReparkSession::new().unwrap();
    let csv = dir.path().join("csv");
    write_text(&csv.join("part-root.csv"), "id,s\n9,z\n");
    write_text(&csv.join("p=1/part-0.csv"), "id,s\n1,a\n");
    let header = text_options(&[("header", "true"), ("text_scan", "header")]);
    let frame = session
        .read_csv(csv.to_str().unwrap(), &header)
        .await
        .unwrap();
    let plan = frame.clone().create_physical_plan().await.unwrap();
    let listed = crate::plan_introspect::input_files(&plan);
    assert!(
        listed
            .iter()
            .any(|name| name.ends_with("csv/part-root.csv"))
    );
    assert_eq!(listed.len(), 2);
    assert_eq!(row_count(frame).await, 1);
    let parquet = dir.path().join("parquet");
    write_rows(&parquet.join("part-root.parquet"), &[(9, "z")]);
    write_rows(&parquet.join("p=1/part-0.parquet"), &[(1, "a")]);
    let frame = read(&session, dir.path(), "parquet").await.unwrap();
    let plan = frame.clone().create_physical_plan().await.unwrap();
    assert_eq!(crate::plan_introspect::input_files(&plan).len(), 1);
    assert_eq!(row_count(frame).await, 1);
}

#[cfg(unix)]
fn write_unreadable(path: &Path, text: &str) -> bool {
    use std::os::unix::fs::PermissionsExt;
    write_text(path, text);
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o000)).unwrap();
    File::open(path).is_err()
}

#[cfg(unix)]
type RootEntry = (
    object_store::ObjectMeta,
    Vec<(String, Option<String>)>,
    usize,
    String,
);

#[cfg(unix)]
async fn root_entry(dir: &Path, name: &str, text: &str, unreadable: bool) -> (RootEntry, bool) {
    use object_store::ObjectStoreExt;
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    write_text(&path, text);
    let location = object_store::path::Path::from_filesystem_path(&path).unwrap();
    let meta = object_store::local::LocalFileSystem::new()
        .head(&location)
        .await
        .unwrap();
    if unreadable {
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
    }
    (
        (meta, Vec::new(), 1, String::new()),
        File::open(&path).is_err(),
    )
}

#[cfg(unix)]
#[tokio::test]
async fn root_file_opens_follow_what_spark_reads_at_inference() {
    use super::partition::{RootFileOpens, refuse_unreadable_root_files};
    let dir = tempfile::tempdir().unwrap();
    let store = object_store::local::LocalFileSystem::new();
    let (part, _) = root_entry(dir.path(), "part.csv", "id,s\n1,a\n", false).await;
    let (big_bad, denied) = root_entry(dir.path(), "big-bad.csv", "id,s\n9,z\n8,y\n", true).await;
    if !denied {
        return;
    }
    let (big_bad2, _) = root_entry(dir.path(), "big-bad2.csv", "id,s\n7,z\n6,y\n", true).await;
    let (big_ok, _) = root_entry(dir.path(), "big-ok.csv", "id,s\n5,z\n4,y\n", false).await;
    let (small_bad, _) = root_entry(dir.path(), "small-bad.csv", "id,s\n9\n", true).await;
    let (empty_bad, _) = root_entry(dir.path(), "empty-bad.csv", "", true).await;
    let mut huge_bad = big_bad.clone();
    huge_bad.0.size = 5 * 1024 * 1024;
    let parts = [part];
    let refuses = |roots: Vec<RootEntry>, opens| {
        let store = &store;
        let parts = &parts;
        async move {
            refuse_unreadable_root_files(store, &roots, parts, opens)
                .await
                .map_err(|error| error.to_string())
        }
    };
    let largest = refuses(vec![big_bad.clone()], RootFileOpens::Largest).await;
    assert!(largest.unwrap_err().contains("Unable to open file"));
    assert!(
        refuses(vec![small_bad.clone()], RootFileOpens::Largest)
            .await
            .is_ok()
    );
    assert!(
        refuses(vec![big_ok, big_bad.clone()], RootFileOpens::Largest)
            .await
            .is_ok()
    );
    assert!(
        refuses(vec![big_bad.clone(), big_bad2], RootFileOpens::Largest)
            .await
            .is_err()
    );
    assert!(
        refuses(vec![huge_bad], RootFileOpens::Largest)
            .await
            .is_ok()
    );
    assert!(refuses(vec![small_bad], RootFileOpens::All).await.is_err());
    assert!(refuses(vec![empty_bad], RootFileOpens::All).await.is_ok());
    assert!(refuses(vec![big_bad], RootFileOpens::Never).await.is_ok());
}

#[cfg(unix)]
#[tokio::test]
async fn unreadable_root_files_refuse_json_and_inferred_csv_reads() {
    let dir = tempfile::tempdir().unwrap();
    let session = ReparkSession::new().unwrap();
    write_text(&dir.path().join("csv/p=1/part-0.csv"), "id,s\n1,a\n");
    write_text(&dir.path().join("json/p=1/part-0.json"), "{\"id\":1}\n");
    let denied = write_unreadable(&dir.path().join("csv/root.csv"), "id,s\n9\n");
    if !denied || !write_unreadable(&dir.path().join("json/root.json"), "{}\n") {
        return;
    }
    let csv = dir.path().join("csv").to_str().unwrap().to_string();
    let json = dir.path().join("json").to_str().unwrap().to_string();
    let header = text_options(&[("header", "true")]);
    assert!(session.read_csv(&csv, &header).await.is_err());
    assert!(session.read_json(&json, &text_options(&[])).await.is_err());
    let user = text_options(&[("user_schema", "true")]);
    assert_eq!(
        row_count(session.read_json(&json, &user).await.unwrap()).await,
        1
    );
}
