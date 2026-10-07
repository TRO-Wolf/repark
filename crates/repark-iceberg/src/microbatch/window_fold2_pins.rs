use super::window_fold_pins::expect_non_append;
use super::*;

async fn appends_then(
    name: &str,
    non_append: Operation,
) -> (TempDir, Arc<dyn Catalog>, TableIdent, [i64; 3]) {
    let (warehouse, catalog, ident) = fixture_table(name).await;
    let first_file = synthetic_file("a.parquet", 1);
    let first = commit_append(&catalog, &ident, vec![first_file.clone()]).await;
    let second = commit_append(&catalog, &ident, vec![synthetic_file("b.parquet", 1)]).await;
    let third = match non_append {
        Operation::Delete => commit_delete(&catalog, &ident, vec![first_file]).await,
        _ => {
            commit_overwrite(
                &catalog,
                &ident,
                vec![synthetic_file("x.parquet", 2)],
                vec![first_file],
            )
            .await
        }
    };
    (warehouse, catalog, ident, [first, second, third])
}

#[tokio::test]
async fn unbounded_walk_refuses_the_first_non_append_before_any_file() {
    for operation in [Operation::Overwrite, Operation::Delete] {
        let (_warehouse, catalog, ident, [first, _, third]) =
            appends_then("r17-unbounded", operation.clone()).await;
        let table = load(&catalog, &ident).await;
        assert_eq!(operation_of(&table, third), operation);
        for caps in [uncapped(), capped(Some(1), None)] {
            let planner = WindowPlanner::new(table.clone(), caps);
            let error = planner
                .next_window(&input_offset(&table, first, 0), WindowLimit::Unbounded)
                .await
                .expect_err("an AvailableNow target walk refuses before batch 0");
            expect_non_append(&error, third, &operation, Some(first), third);
        }
    }
}

#[tokio::test]
async fn capped_walk_delivers_both_appends_then_refuses_the_non_append() {
    for operation in [Operation::Overwrite, Operation::Delete] {
        let (_warehouse, catalog, ident, [first, second, third]) =
            appends_then("r17-capped", operation.clone()).await;
        let table = load(&catalog, &ident).await;
        let planner = WindowPlanner::new(table.clone(), capped(Some(1), None));
        let mut cursor = input_offset(&table, first, 0);
        let mut ends = Vec::new();
        let error = loop {
            match planner.next_window(&cursor, WindowLimit::Capped).await {
                Ok(Some(plan)) => {
                    cursor = plan.end.clone();
                    ends.push(plan.end);
                }
                Ok(None) => panic!("the walk must reach the {operation:?}"),
                Err(error) => break error,
            }
        };
        assert_eq!(
            ends,
            vec![
                input_offset(&table, first, 1),
                input_offset(&table, second, 1)
            ]
        );
        expect_non_append(&error, third, &operation, Some(second), third);
    }
}
