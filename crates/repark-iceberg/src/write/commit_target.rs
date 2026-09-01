use iceberg::table::Table;

#[must_use]
pub fn snapshot_id_for_commit(table: &Table, branch: Option<&str>) -> Option<i64> {
    match branch {
        Some(name) => table
            .metadata()
            .snapshot_for_ref(name)
            .map(|snapshot| snapshot.snapshot_id()),
        None => table.metadata().current_snapshot_id(),
    }
}

#[must_use]
pub fn maybe_to_branch<A>(
    action: A,
    branch: Option<&str>,
    to_branch: impl FnOnce(A, &str) -> A,
) -> A {
    match branch {
        Some(name) => to_branch(action, name),
        None => action,
    }
}
