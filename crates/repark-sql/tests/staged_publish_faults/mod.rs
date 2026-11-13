use std::path::PathBuf;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;
use std::task::Poll;

use repark_core::ReparkSession;

use super::{FaultCatalog, KILL, KILLED, local, metadata_files, run, version};

pub(super) fn inject(location: &str) {
    let table_dir = PathBuf::from(local(location));
    let seeded = table_dir.parent().expect("namespace dir").join("t");
    let newest = metadata_files(&seeded)
        .into_iter()
        .filter(|path| path.contains("/v"))
        .max_by_key(|path| version(path))
        .expect("seeded metadata");
    let next = metadata_files(&table_dir)
        .iter()
        .filter(|path| path.contains("/v"))
        .map(|path| version(path))
        .max()
        .map_or(1, |at| at + 1);
    std::fs::copy(
        newest,
        table_dir.join(format!("metadata/v{next}.metadata.json")),
    )
    .expect("inject");
}

pub(super) async fn run_until_killed(
    writer: &ReparkSession,
    stmt: &str,
    fault: &FaultCatalog,
) -> Option<String> {
    let mut statement = std::pin::pin!(run(writer, stmt));
    std::future::poll_fn(|context| {
        if let Poll::Ready(done) = statement.as_mut().poll(context) {
            return Poll::Ready(done.err());
        }
        *fault.killer.lock().unwrap_or_else(PoisonError::into_inner) =
            Some(context.waker().clone());
        if fault.point.load(Ordering::SeqCst) == KILL && fault.tripped.load(Ordering::SeqCst) {
            return Poll::Ready(Some(KILLED.to_string()));
        }
        Poll::Pending
    })
    .await
}

pub(super) fn recorded_refusal(repark: &str) -> &str {
    repark
        .strip_prefix("failure: ")
        .and_then(|rest| rest.split("; ").next())
        .expect("recorded refusal")
}
