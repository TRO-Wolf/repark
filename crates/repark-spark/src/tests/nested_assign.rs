use super::super::*;
use super::accept_any_refusals::refusal;
use super::common::*;

async fn struct_table(rows: &str) -> (TempDir, SessionContext, CatalogRegistry) {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id BIGINT, st STRUCT<a: INT, b: STRING>) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.t VALUES {rows}"),
    )
    .await;
    (wh, ctx, catalogs)
}

async fn text(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<String> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
        .lines()
        .filter(|line| line.starts_with("| "))
        .skip(1)
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect()
}

async fn last_summary(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<String> {
    text(
        ctx,
        catalogs,
        "SELECT operation, summary['added-records'], summary['deleted-records'], \
         summary['total-records'], summary['added-data-files'], summary['deleted-data-files'] \
         FROM ice.sales.t.snapshots ORDER BY committed_at DESC LIMIT 1",
    )
    .await
}

async fn columns(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<String> {
    let frame = execute(ctx, catalogs, "SELECT * FROM ice.sales.t")
        .await
        .unwrap();
    frame
        .schema()
        .fields()
        .iter()
        .map(|field| format!("{}: {}", field.name(), field.data_type()))
        .collect()
}

#[tokio::test]
async fn update_sets_one_struct_field_and_keeps_its_siblings() {
    let (_wh, ctx, catalogs) = struct_table("(1, named_struct('a', 1, 'b', 'p'))").await;
    let before = columns(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.t SET st.a = 99 WHERE id = 1",
    )
    .await;
    assert_eq!(
        text(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        ["| 1 | {a: 99, b: p} |"]
    );
    assert_eq!(columns(&ctx, &catalogs).await, before);
    assert_eq!(
        last_summary(&ctx, &catalogs).await,
        ["| overwrite | 1 | 1 | 1 | 1 | 1 |"]
    );
}

#[tokio::test]
async fn merge_sets_one_struct_field_from_the_source() {
    let (_wh, ctx, catalogs) =
        struct_table("(1, named_struct('a', 1, 'b', 'p')), (2, named_struct('a', 2, 'b', 'q'))")
            .await;
    let before = columns(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t t USING (SELECT 2 AS id, 20 AS na) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.st.a = s.na",
    )
    .await;
    assert_eq!(
        text(&ctx, &catalogs, "SELECT * FROM ice.sales.t ORDER BY id").await,
        ["| 1 | {a: 1, b: p} |", "| 2 | {a: 20, b: q} |"]
    );
    assert_eq!(columns(&ctx, &catalogs).await, before);
    assert_eq!(
        last_summary(&ctx, &catalogs).await,
        ["| overwrite | 2 | 2 | 2 | 1 | 1 |"]
    );
}

#[tokio::test]
async fn whole_struct_merge_assignments_pass_the_store_assignment_gate() {
    let (_wh, ctx, catalogs) =
        struct_table("(1, named_struct('a', 1, 'b', 'p')), (2, named_struct('a', 2, 'b', 'q'))")
            .await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t t USING (SELECT 2 AS id, named_struct('a', 22, 'b', 'w') AS st) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET * \
         WHEN NOT MATCHED THEN INSERT *",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t t USING (SELECT 3 AS id, 30 AS na) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, named_struct('a', s.na, 'b', 'n'))",
    )
    .await;
    assert_eq!(
        text(&ctx, &catalogs, "SELECT * FROM ice.sales.t ORDER BY id").await,
        [
            "| 1 | {a: 1, b: p} |",
            "| 2 | {a: 22, b: w} |",
            "| 3 | {a: 30, b: n} |"
        ]
    );
}

const SEED: &str = "(1, named_struct('a', 1, 'b', 'p')), (2, named_struct('a', 2, 'b', 'q'))";

async fn whole_struct_answer(statement: &str) -> Result<Vec<String>, String> {
    let (_wh, ctx, catalogs) = struct_table(SEED).await;
    let sql = statement.replace("{T}", "ice.sales.t");
    let refused = match execute(&ctx, &catalogs, &sql).await {
        Ok(frame) => frame.collect().await.err(),
        Err(error) => Some(error),
    };
    if refused.is_some() {
        let mapped = refusal(&ctx, &catalogs, &sql).await;
        return Err(mapped
            .to_string()
            .trim_start_matches("Error during planning: ")
            .to_string());
    }
    Ok(text(&ctx, &catalogs, "SELECT * FROM ice.sales.t ORDER BY id").await)
}

#[tokio::test]
async fn whole_struct_values_resolve_by_name_on_update_and_merge() {
    let missing = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data \
                   for the table ``: Cannot find data for the output column `st`.`a`. \
                   SQLSTATE: KD000";
    let extra = "[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS] Cannot write incompatible \
                 data for the table ``: Cannot write extra fields `x` to the struct `st`. \
                 SQLSTATE: KD000";
    let update = "UPDATE {T} SET st = ";
    let matched = "MERGE INTO {T} t USING (SELECT 2 AS id, 20 AS na) s ON t.id = s.id \
                   WHEN MATCHED THEN UPDATE SET t.st = ";
    let unqualified = "MERGE INTO {T} t USING (SELECT 2 AS id, 20 AS na) s ON t.id = s.id \
                       WHEN MATCHED THEN UPDATE SET st = ";
    let by_source = "MERGE INTO {T} t USING (SELECT 2 AS id, 20 AS na) s ON t.id = s.id \
                     WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.st = ";
    for prefix in [update, matched, unqualified, by_source] {
        let suffix = if prefix == update {
            " WHERE id = 1"
        } else {
            ""
        };
        for (value, expected) in [
            ("named_struct('q', 1, 'b', 'z')", missing),
            ("named_struct('a', 1, 'b', 'z', 'x', 2)", extra),
        ] {
            assert_eq!(
                whole_struct_answer(&format!("{prefix}{value}{suffix}")).await,
                Err(expected.to_string()),
                "{prefix}{value}"
            );
        }
    }
    let written = |first: &str, second: &str| Ok(vec![first.to_string(), second.to_string()]);
    assert_eq!(
        whole_struct_answer("UPDATE {T} SET st = named_struct('b', 'z', 'a', 5) WHERE id = 1")
            .await,
        written("| 1 | {a: 5, b: z} |", "| 2 | {a: 2, b: q} |")
    );
    assert_eq!(
        whole_struct_answer(&format!("{matched}named_struct('b', 'z', 'a', s.na)")).await,
        written("| 1 | {a: 1, b: p} |", "| 2 | {a: 20, b: z} |")
    );
    assert_eq!(
        whole_struct_answer(&format!("{by_source}named_struct('b', 'z', 'a', 9)")).await,
        written("| 1 | {a: 9, b: z} |", "| 2 | {a: 2, b: q} |")
    );
    assert_eq!(
        whole_struct_answer("UPDATE {T} SET st = named_struct('A', 5, 'B', 'z') WHERE id = 1")
            .await,
        written("| 1 | {a: 5, b: z} |", "| 2 | {a: 2, b: q} |")
    );
}
