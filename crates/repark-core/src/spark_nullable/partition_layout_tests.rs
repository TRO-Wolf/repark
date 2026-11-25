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
