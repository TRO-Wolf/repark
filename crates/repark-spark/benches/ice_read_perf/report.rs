use std::fmt::Write as _;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use repark_core::ReparkSession;
use repark_iceberg::catalog::{
    IcebergFileClass, IcebergIoCount, IcebergIoOp, IcebergIoStats, TableMetadataCacheStats,
};
use serde_json::{Value, json};

use crate::run::{Mode, QuerySpec, Timing};

#[derive(Debug, Default)]
pub struct RunTally {
    requests: AtomicU64,
    bytes: AtomicU64,
}

impl RunTally {
    pub fn drain(&self, session: &ReparkSession) {
        let total = session.iceberg_io_stats().total();
        self.requests.fetch_add(total.requests, Ordering::Relaxed);
        self.bytes.fetch_add(total.bytes, Ordering::Relaxed);
        session.reset_iceberg_io_stats();
    }

    #[must_use]
    pub fn total(&self) -> IcebergIoCount {
        IcebergIoCount {
            requests: self.requests.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoDelta {
    pub io: IcebergIoStats,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_body_fetches: u64,
    pub cache_evictions: u64,
    pub peak_rss_kib: Option<u64>,
    pub rss_at_reset_kib: Option<u64>,
    pub peak_rss_reset: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Before {
    cache: TableMetadataCacheStats,
    peak_rss_reset: bool,
    rss_at_reset_kib: Option<u64>,
}

pub async fn probe_before(session: &ReparkSession, tally: &RunTally) -> Before {
    session.settle_iceberg_metadata_cache().await;
    tally.drain(session);
    let peak_rss_reset = std::fs::write("/proc/self/clear_refs", "5").is_ok();
    Before {
        cache: session.iceberg_metadata_cache_report().unwrap_or_default(),
        peak_rss_reset,
        rss_at_reset_kib: status_kib("VmRSS:"),
    }
}

pub async fn probe_after(session: &ReparkSession, before: &Before) -> IoDelta {
    session.settle_iceberg_metadata_cache().await;
    let after = session.iceberg_metadata_cache_report().unwrap_or_default();
    IoDelta {
        io: session.iceberg_io_stats(),
        cache_hits: after.hits.saturating_sub(before.cache.hits),
        cache_misses: after.misses.saturating_sub(before.cache.misses),
        cache_body_fetches: after.body_fetches.saturating_sub(before.cache.body_fetches),
        cache_evictions: after.evictions.saturating_sub(before.cache.evictions),
        peak_rss_kib: status_kib("VmHWM:"),
        rss_at_reset_kib: before.rss_at_reset_kib,
        peak_rss_reset: before.peak_rss_reset,
    }
}

#[must_use]
pub fn status_kib(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix(field))
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
            "planning_ms": millis(self.timing.planning),
            "first_batch_ms": self.timing.first_batch.map(millis),
            "execute_to_first_batch_ms": self.timing.execute_to_first_batch().map(millis),
            "total_ms": millis(self.timing.total),
            "rows": self.timing.rows,
            "io": self.delta.as_ref().map(|delta| io_json(&delta.io)),
            "metadata_cache": self.delta.as_ref().map(|delta| json!({
                "hits": delta.cache_hits,
                "misses": delta.cache_misses,
                "body_fetches": delta.cache_body_fetches,
                "evictions": delta.cache_evictions,
            })),
            "peak_rss_kib": self.delta.as_ref().and_then(|delta| delta.peak_rss_kib),
            "rss_at_reset_kib": self.delta.as_ref().and_then(|delta| delta.rss_at_reset_kib),
            "peak_rss_reset_before_query": self.delta.as_ref().map(|delta| delta.peak_rss_reset),
            "register_io": self.register_io.as_ref().map(io_json),
        })
    }
}

#[must_use]
pub fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 1 {
        Some(values[middle])
    } else {
        Some(f64::midpoint(values[middle - 1], values[middle]))
    }
}

fn median_of(samples: &[&QueryRecord], value: impl Fn(&QueryRecord) -> Option<f64>) -> Option<f64> {
    let mut values: Vec<f64> = samples.iter().filter_map(|record| value(record)).collect();
    median(&mut values)
}

fn kib_as_f64(kib: Option<u64>) -> Option<f64> {
    kib.and_then(|value| u32::try_from(value).ok())
        .map(f64::from)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Medians {
    pub planning_ms: Option<f64>,
    pub first_batch_ms: Option<f64>,
    pub execute_to_first_batch_ms: Option<f64>,
    pub total_ms: Option<f64>,
    pub peak_rss_kib: Option<f64>,
    pub rss_at_reset_kib: Option<f64>,
}

#[must_use]
pub fn medians(samples: &[&QueryRecord]) -> Medians {
    Medians {
        planning_ms: median_of(samples, |record| Some(millis(record.timing.planning))),
        first_batch_ms: median_of(samples, |record| record.timing.first_batch.map(millis)),
        execute_to_first_batch_ms: median_of(samples, |record| {
            record.timing.execute_to_first_batch().map(millis)
        }),
        total_ms: median_of(samples, |record| Some(millis(record.timing.total))),
        peak_rss_kib: median_of(samples, |record| {
            kib_as_f64(record.delta.as_ref().and_then(|delta| delta.peak_rss_kib))
        }),
        rss_at_reset_kib: median_of(samples, |record| {
            kib_as_f64(
                record
                    .delta
                    .as_ref()
                    .and_then(|delta| delta.rss_at_reset_kib),
            )
        }),
    }
}

#[must_use]
pub fn sample_mismatches(mode: &str, samples: &[&QueryRecord]) -> Vec<String> {
    let Some(first) = samples.first() else {
        return Vec::new();
    };
    let first_io = first.delta.as_ref().map(|delta| delta.io);
    let mut lines = Vec::new();
    for (index, sample) in samples.iter().enumerate().skip(1) {
        let io = sample.delta.as_ref().map(|delta| delta.io);
        if io == first_io && sample.timing.rows == first.timing.rows {
            continue;
        }
        let describe = |io: Option<IcebergIoStats>| {
            io.map_or_else(
                || "-".to_string(),
                |io| format!("{}/{}", io.total().requests, io.total().bytes),
            )
        };
        let line = format!(
            "IO-MISMATCH mode={mode} query={} sample={index} rows={}/{} requests/bytes={} vs {}",
            first.name,
            sample.timing.rows,
            first.timing.rows,
            describe(io),
            describe(first_io)
        );
        println!("{line}");
        eprintln!("{line}");
        lines.push(line);
    }
    lines
}

fn entry_json(predicate: Option<&str>, samples: &[&QueryRecord], mismatches: &[String]) -> Value {
    let first = samples.first();
    let stats = medians(samples);
    json!({
        "name": first.map(|record| record.name.clone()),
        "label": first.map(|record| record.label.clone()),
        "sql": first.map(|record| record.sql.clone()),
        "iceberg_scan_predicate": predicate,
        "rows": first.map(|record| record.timing.rows),
        "io": first.and_then(|record| record.delta.as_ref()).map(|delta| io_json(&delta.io)),
        "io_identical_across_samples": mismatches.is_empty(),
        "samples_taken": samples.len(),
        "median": {
            "planning_ms": stats.planning_ms,
            "first_batch_ms": stats.first_batch_ms,
            "execute_to_first_batch_ms": stats.execute_to_first_batch_ms,
            "total_ms": stats.total_ms,
            "peak_rss_kib": stats.peak_rss_kib,
            "rss_at_reset_kib": stats.rss_at_reset_kib,
        },
        "samples": samples.iter().map(|record| record.to_json()).collect::<Vec<Value>>(),
    })
}

fn samples_named<'a>(records: &'a [QueryRecord], name: &str) -> Vec<&'a QueryRecord> {
    records
        .iter()
        .filter(|record| record.name == name)
        .collect()
}

#[must_use]
pub fn queries_json(
    mode: &str,
    specs: &[QuerySpec],
    predicates: &[(&str, Option<String>)],
    records: &[QueryRecord],
) -> (Vec<Value>, Vec<String>) {
    let mut entries = Vec::new();
    let mut all = Vec::new();
    for spec in specs {
        let samples = samples_named(records, spec.name);
        if samples.is_empty() {
            continue;
        }
        let predicate = predicates
            .iter()
            .find(|(name, _)| *name == spec.name)
            .and_then(|(_, predicate)| predicate.as_deref());
        let mismatches = sample_mismatches(mode, &samples);
        entries.push(entry_json(predicate, &samples, &mismatches));
        all.extend(mismatches);
    }
    (entries, all)
}

#[must_use]
pub fn group_json(mode: &str, groups: &[QueryRecord]) -> (Value, Vec<String>) {
    let samples: Vec<&QueryRecord> = groups.iter().collect();
    let mismatches = sample_mismatches(mode, &samples);
    (entry_json(None, &samples, &mismatches), mismatches)
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
    let ranged = |class: IcebergFileClass| {
        let footer = stats.get(IcebergIoOp::FooterRead, class);
        let page = stats.get(IcebergIoOp::RangedRead, class);
        json!({
            "footer": {"requests": footer.requests, "bytes": footer.bytes},
            "page": {"requests": page.requests, "bytes": page.bytes},
        })
    };
    let total = stats.total();
    json!({
        "total": {"requests": total.requests, "bytes": total.bytes},
        "data_file_ranged": ranged(IcebergFileClass::DataFile),
        "delete_file_ranged": ranged(IcebergFileClass::DeleteFile),
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
pub fn environment(mode: Mode) -> Value {
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
        "rustc_version": command_text("rustc", &["--version"], &workspace),
        "cold_kind": mode.cold_kind(),
        "concurrent_kind": mode.concurrent_kind(),
        "os_page_cache": "not_dropped",
        "peak_rss_kind": "vmhwm_after_clear_refs_5_floor_is_rss_at_reset",
        "first_batch_kind": "first_batch_ms_counts_from_sql_start_execute_to_first_batch_ms_excludes_planning",
    })
}

fn cell(stats: &IcebergIoStats, op: IcebergIoOp, class: IcebergFileClass) -> String {
    let count = stats.get(op, class);
    format!("{}/{}", count.requests, count.bytes)
}

fn ms(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.1}"))
}

fn mib(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |kib| format!("{:.0}", kib / 1024.0))
}

fn io_cells(delta: Option<&IoDelta>) -> [String; 8] {
    let Some(delta) = delta else {
        return [
            "(group)".to_string(),
            "(group)".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ];
    };
    let io = &delta.io;
    let total = io.total();
    let deletes = io.by_class(IcebergFileClass::DeleteFile);
    [
        cell(io, IcebergIoOp::FooterRead, IcebergFileClass::DataFile),
        cell(io, IcebergIoOp::RangedRead, IcebergFileClass::DataFile),
        format!("{}/{}", deletes.requests, deletes.bytes),
        io.by_class(IcebergFileClass::TableMetadata)
            .requests
            .to_string(),
        io.by_class(IcebergFileClass::ManifestList)
            .requests
            .to_string(),
        io.by_class(IcebergFileClass::Manifest).requests.to_string(),
        format!("{}/{}", total.requests, total.bytes),
        format!(
            "{}/{}/{}",
            delta.cache_hits, delta.cache_misses, delta.cache_evictions
        ),
    ]
}

fn row(text: &mut String, mode: &str, samples: &[&QueryRecord], same: bool) {
    let Some(first) = samples.first() else {
        return;
    };
    let stats = medians(samples);
    let [footer, page, deletes, meta, list, manifest, total, cache] =
        io_cells(first.delta.as_ref());
    let _ = writeln!(
        text,
        "| {mode} | {} | {} | {} | {} | {} | {} | {} | {footer} | {page} | {deletes} | {meta} | \
         {list} | {manifest} | {total} | {cache} | {} | {} | {} |",
        first.name,
        samples.len(),
        ms(stats.planning_ms),
        ms(stats.execute_to_first_batch_ms),
        ms(stats.first_batch_ms),
        ms(stats.total_ms),
        first.timing.rows,
        mib(stats.rss_at_reset_kib),
        mib(stats.peak_rss_kib),
        if same { "yes" } else { "**NO**" },
    );
}

#[must_use]
pub fn markdown(
    mode: &str,
    specs: &[QuerySpec],
    records: &[QueryRecord],
    groups: &[QueryRecord],
) -> String {
    let mut text = String::new();
    let _ = writeln!(
        text,
        "| mode | query | n | plan ms | exec→first ms | first batch ms | total ms | rows | \
         data footer req/bytes | data page req/bytes | delete req/bytes | meta json req | \
         manifest-list req | manifest req | total req/bytes | cache hit/miss/evict | RSS at reset MiB | \
         peak RSS MiB | io same |"
    );
    let _ = writeln!(
        text,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    );
    for spec in specs {
        let samples = samples_named(records, spec.name);
        let same = sample_mismatches_quiet(&samples);
        row(&mut text, mode, &samples, same);
    }
    let group: Vec<&QueryRecord> = groups.iter().collect();
    let same = sample_mismatches_quiet(&group);
    row(&mut text, mode, &group, same);
    let _ = writeln!(
        text,
        "\ntimings and RSS are medians over n samples; requests and bytes are sample 1's \
         (\"io same\" says whether every sample matched it)"
    );
    text
}

fn sample_mismatches_quiet(samples: &[&QueryRecord]) -> bool {
    let Some(first) = samples.first() else {
        return true;
    };
    samples.iter().all(|sample| {
        sample.delta.as_ref().map(|delta| delta.io) == first.delta.as_ref().map(|delta| delta.io)
            && sample.timing.rows == first.timing.rows
    })
}
