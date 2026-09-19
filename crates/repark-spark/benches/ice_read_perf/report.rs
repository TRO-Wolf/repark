use std::fmt::Write as _;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use repark_core::ReparkSession;
use repark_iceberg::catalog::{IcebergFileClass, IcebergIoOp, IcebergIoStats};
use serde_json::{Value, json};

use crate::run::{QuerySpec, Timing};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoDelta {
    pub io: IcebergIoStats,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_body_fetches: u64,
    pub peak_rss_kib: Option<u64>,
    pub peak_rss_reset: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Before {
    cache: (u64, u64, u64),
    peak_rss_reset: bool,
}

pub fn probe_before(session: &ReparkSession) -> Before {
    session.reset_iceberg_io_stats();
    Before {
        cache: session.iceberg_metadata_cache_stats().unwrap_or((0, 0, 0)),
        peak_rss_reset: std::fs::write("/proc/self/clear_refs", "5").is_ok(),
    }
}

pub fn probe_after(session: &ReparkSession, before: &Before) -> IoDelta {
    let (hits, misses, fetches) = session.iceberg_metadata_cache_stats().unwrap_or((0, 0, 0));
    IoDelta {
        io: session.iceberg_io_stats(),
        cache_hits: hits.saturating_sub(before.cache.0),
        cache_misses: misses.saturating_sub(before.cache.1),
        cache_body_fetches: fetches.saturating_sub(before.cache.2),
        peak_rss_kib: peak_rss_kib(),
        peak_rss_reset: before.peak_rss_reset,
    }
}

#[must_use]
pub fn peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRecord {
    pub name: String,
    pub label: String,
    pub sql: String,
    pub timing: Timing,
    pub delta: Option<IoDelta>,
    pub register_io: Option<IcebergIoStats>,
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

impl QueryRecord {
    #[must_use]
    pub fn new(spec: &QuerySpec, timing: Timing, delta: Option<&IoDelta>) -> Self {
        Self {
            name: spec.name.to_string(),
            label: spec.label.to_string(),
            sql: spec.sql.clone(),
            timing,
            delta: delta.copied(),
            register_io: None,
        }
    }

    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "label": self.label,
            "sql": self.sql,
            "planning_ms": millis(self.timing.planning),
            "first_batch_ms": self.timing.first_batch.map(millis),
            "total_ms": millis(self.timing.total),
            "rows": self.timing.rows,
            "io": self.delta.as_ref().map(|delta| io_json(&delta.io)),
            "metadata_cache": self.delta.as_ref().map(|delta| json!({
                "hits": delta.cache_hits,
                "misses": delta.cache_misses,
                "body_fetches": delta.cache_body_fetches,
            })),
            "peak_rss_kib": self.delta.as_ref().and_then(|delta| delta.peak_rss_kib),
            "peak_rss_reset_before_query": self.delta.as_ref().map(|delta| delta.peak_rss_reset),
            "register_io": self.register_io.as_ref().map(io_json),
        })
    }
}

#[must_use]
pub fn io_json(stats: &IcebergIoStats) -> Value {
    let mut by_op = serde_json::Map::new();
    for op in IcebergIoOp::ALL {
        let count = stats.by_op(op);
        let mut classes = serde_json::Map::new();
        for class in IcebergFileClass::ALL {
            let cell = stats.get(op, class);
            if cell.requests > 0 || cell.bytes > 0 {
                classes.insert(
                    class.name().to_string(),
                    json!({"requests": cell.requests, "bytes": cell.bytes}),
                );
            }
        }
        by_op.insert(
            op.name().to_string(),
            json!({"requests": count.requests, "bytes": count.bytes, "by_class": classes}),
        );
    }
    let mut by_class = serde_json::Map::new();
    for class in IcebergFileClass::ALL {
        let count = stats.by_class(class);
        by_class.insert(
            class.name().to_string(),
            json!({"requests": count.requests, "bytes": count.bytes}),
        );
    }
    let total = stats.total();
    json!({
        "total": {"requests": total.requests, "bytes": total.bytes},
        "by_op": by_op,
        "by_class": by_class,
    })
}

fn command_text(program: &str, args: &[&str], directory: &Path) -> Option<String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(directory)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn fork_pin(workspace: &Path) -> Option<String> {
    let manifest = std::fs::read_to_string(workspace.join("Cargo.toml")).ok()?;
    manifest
        .lines()
        .filter(|line| line.starts_with("iceberg = "))
        .find_map(|line| line.split("rev = \"").nth(1))
        .and_then(|rest| rest.split('"').next())
        .map(str::to_string)
}

fn cpu_model() -> Option<String> {
    let info = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    info.lines()
        .find_map(|line| line.strip_prefix("model name"))
        .and_then(|rest| rest.split_once(':'))
        .map(|(_, model)| model.trim().to_string())
}

fn utc_now() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
        .and_then(|seconds| chrono::DateTime::from_timestamp(seconds, 0))
        .map_or_else(
            || "unknown".to_string(),
            |moment| moment.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        )
}

#[must_use]
pub fn environment() -> Value {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let dirty = command_text("git", &["status", "--porcelain"], &workspace)
        .map(|status| !status.is_empty());
    json!({
        "git_head": command_text("git", &["rev-parse", "HEAD"], &workspace),
        "git_dirty": dirty,
        "fork_pin": fork_pin(&workspace),
        "profile": if cfg!(debug_assertions) { "debug" } else { "release (bench inherits release)" },
        "cpu_count": std::thread::available_parallelism().map(std::num::NonZero::get).ok(),
        "cpu_model": cpu_model(),
        "kernel": command_text("uname", &["-r"], &workspace),
        "loadavg": std::fs::read_to_string("/proc/loadavg").ok().map(|text| text.trim().to_string()),
        "date_utc": utc_now(),
    })
}

fn cell(stats: &IcebergIoStats, op: IcebergIoOp, class: IcebergFileClass) -> String {
    let count = stats.get(op, class);
    format!("{}/{}", count.requests, count.bytes)
}

#[must_use]
pub fn markdown(mode: &str, records: &[QueryRecord], group: Option<&QueryRecord>) -> String {
    let mut text = String::new();
    let _ = writeln!(
        text,
        "| mode | query | plan ms | first batch ms | total ms | rows | data ranged req/bytes | \
         meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | \
         peak RSS MiB |"
    );
    let _ = writeln!(
        text,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    );
    for record in records.iter().chain(group) {
        let first = record
            .timing
            .first_batch
            .map_or_else(|| "-".to_string(), |value| format!("{:.1}", millis(value)));
        let (data, meta, list, manifest, total, cache, rss) = match &record.delta {
            Some(delta) => {
                let total = delta.io.total();
                (
                    cell(
                        &delta.io,
                        IcebergIoOp::RangedRead,
                        IcebergFileClass::DataFile,
                    ),
                    delta
                        .io
                        .by_class(IcebergFileClass::TableMetadata)
                        .requests
                        .to_string(),
                    delta
                        .io
                        .by_class(IcebergFileClass::ManifestList)
                        .requests
                        .to_string(),
                    delta
                        .io
                        .by_class(IcebergFileClass::Manifest)
                        .requests
                        .to_string(),
                    format!("{}/{}", total.requests, total.bytes),
                    format!("{}/{}", delta.cache_hits, delta.cache_misses),
                    delta
                        .peak_rss_kib
                        .map_or_else(|| "-".to_string(), |kib| (kib / 1024).to_string()),
                )
            }
            None => (
                "(group)".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
            ),
        };
        let _ = writeln!(
            text,
            "| {mode} | {} | {:.1} | {first} | {:.1} | {} | {data} | {meta} | {list} | {manifest} | \
             {total} | {cache} | {rss} |",
            record.name,
            millis(record.timing.planning),
            millis(record.timing.total),
            record.timing.rows,
        );
    }
    text
}
