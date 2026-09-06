//! Install `FairSpillPool` and intercept runtime memory and temporary-directory settings.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::{FairSpillPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{DataFrame, SessionContext};
use repark_common::{Error, Result};

use crate::engine_err;
use crate::pool_refusals::{PoolRefusalLog, RefusalRecordingPool};

/// Bytes in one GiB (the `memory_limit_gb` conversion unit).
pub(crate) const BYTES_PER_GB: usize = 1024 * 1024 * 1024;

/// Smallest non-zero `memory_limit_bytes` accepted by [`crate::ReparkSessionBuilder::build`].
pub(crate) const MIN_MEMORY_LIMIT_BYTES: usize = 1024 * 1024;

/// Cap of the RAM-relative default `FairSpillPool` (and the historical 8 GiB constant).
pub(crate) const DEFAULT_MEMORY_LIMIT_BYTES: usize = 8 * BYTES_PER_GB;

/// RAM-relative default pool: `clamp(0.6 × detected, MIN, 8 GiB)`.
pub(crate) fn default_memory_limit_bytes() -> usize {
    clamp_default_memory_limit_bytes(detect_host_memory_bytes())
}

/// Pure clamp used by [`default_memory_limit_bytes`] (and tests with fixture detections).
pub(crate) fn clamp_default_memory_limit_bytes(detected: Option<usize>) -> usize {
    let Some(detected) = detected else {
        return DEFAULT_MEMORY_LIMIT_BYTES;
    };
    let sixty_percent = detected.saturating_mul(3) / 5;
    sixty_percent.clamp(MIN_MEMORY_LIMIT_BYTES, DEFAULT_MEMORY_LIMIT_BYTES)
}

/// Parse cgroup v2 `memory.max` (`max` / empty → `None`).
pub(crate) fn parse_cgroup_memory_max(text: &str) -> Option<usize> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("max") {
        return None;
    }
    trimmed
        .parse::<u128>()
        .ok()
        .and_then(|bytes| usize::try_from(bytes).ok())
}

/// Parse `/proc/meminfo` `MemTotal` (kB) into bytes.
pub(crate) fn parse_meminfo_mem_total(text: &str) -> Option<usize> {
    for line in text.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("MemTotal:") else {
            continue;
        };
        let number = rest.split_whitespace().next()?;
        let kib: u128 = number.parse().ok()?;
        return usize::try_from(kib.saturating_mul(1024)).ok();
    }
    None
}

fn detect_host_memory_bytes() -> Option<usize> {
    if let Ok(text) = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        && let Some(bytes) = parse_cgroup_memory_max(&text)
    {
        return Some(bytes);
    }
    if let Ok(text) = std::fs::read_to_string("/proc/meminfo") {
        return parse_meminfo_mem_total(&text);
    }
    None
}

/// Canonical runtime / builder pseudo-key for the live `FairSpillPool` size.
pub(crate) const MEMORY_LIMIT_KEY: &str = "datafusion.runtime.memory_limit";

/// Build-time spill directory (`RuntimeEnvBuilder::with_temp_file_path`).
pub(crate) const TEMP_DIRECTORY_KEY: &str = "datafusion.runtime.temp_directory";

/// Loud runtime refusal: `DiskManager` is fixed after `RuntimeEnv` build.
pub(crate) const TEMP_DIRECTORY_RUNTIME_REFUSAL: &str = "datafusion.runtime.temp_directory cannot be changed at runtime \
     (the DiskManager is fixed when the RuntimeEnv is built). \
     Set TMPDIR in the process environment before start, or \
     SparkSession.builder.config('datafusion.runtime.temp_directory', path).getOrCreate() \
     at build time.";

/// Builder-only `FairSpillPool` size keys (Python also refuses a runtime `conf.set` of these).
const REPARK_MEMORY_LIMIT_KEYS: &[&str] =
    &["repark.memory.limit.gb", "spark.repark.memory.limit.gb"];

/// Repark-owned `datafusion.` pseudo-keys excluded from DataFusion option parsing.
pub const REPARK_OWNED_DATAFUSION_PSEUDO_KEYS: &[&str] = &[MEMORY_LIMIT_KEY, TEMP_DIRECTORY_KEY];

/// Install `pool_bytes` as a [`FairSpillPool`], or leave DataFusion's unbounded default.
pub(crate) fn with_memory_pool(
    runtime: RuntimeEnvBuilder,
    pool_bytes: Option<usize>,
) -> RuntimeEnvBuilder {
    match pool_bytes {
        Some(bytes) => runtime.with_memory_pool(Arc::new(RefusalRecordingPool::new(
            Arc::new(FairSpillPool::new(bytes)),
            Arc::new(PoolRefusalLog::default()),
        ))),
        None => runtime,
    }
}

/// Apply builder `datafusion.runtime.temp_directory` via `with_temp_file_path` (build time only).
/// # Errors
/// [`Error::Config`] when the key is present and empty.
pub(crate) fn with_temp_directory(
    runtime: RuntimeEnvBuilder,
    config: &HashMap<String, String>,
) -> Result<RuntimeEnvBuilder> {
    match config.get(TEMP_DIRECTORY_KEY) {
        Some(path) => {
            let trimmed = path.trim();
            if trimmed.is_empty() {
                return Err(Error::Config(format!(
                    "{TEMP_DIRECTORY_KEY} requires a non-empty path"
                )));
            }
            Ok(runtime.with_temp_file_path(trimmed))
        }
        None => Ok(runtime),
    }
}

/// Resolve the build-time pool: typed setter, else the `memory_limit` pseudo-key, else default.
/// # Errors
/// [`Error::Config`] on dual-set, a tiny non-zero budget, or an unparsable capacity string.
pub(crate) fn resolve_build_time_pool_bytes(
    typed: Option<usize>,
    config: &HashMap<String, String>,
) -> Result<Option<usize>> {
    refuse_dual_memory_knobs(typed.is_some(), config)?;
    let resolved = match typed {
        Some(0) => None,
        Some(bytes) => Some(bytes),
        None => match config.get(MEMORY_LIMIT_KEY) {
            Some(value) => {
                let bytes = parse_memory_limit_value(value)?;
                if bytes == 0 { None } else { Some(bytes) }
            }
            None => Some(default_memory_limit_bytes()),
        },
    };
    if let Some(bytes) = resolved
        && bytes < MIN_MEMORY_LIMIT_BYTES
    {
        return Err(Error::Config(format!(
            "memory_limit_bytes must be 0 (unbounded) or >= {MIN_MEMORY_LIMIT_BYTES} \
             (1 MiB); got {bytes}"
        )));
    }
    Ok(resolved)
}

/// Refuse a builder that names both `FairSpillPool` knobs (same pool, ambiguous initial size).
/// # Errors
/// [`Error::Config`] naming both knobs.
pub(crate) fn refuse_dual_memory_knobs(
    has_typed_setter: bool,
    config: &HashMap<String, String>,
) -> Result<()> {
    let has_repark_key = config.keys().any(|key| {
        REPARK_MEMORY_LIMIT_KEYS
            .iter()
            .any(|owned| key.eq_ignore_ascii_case(owned))
    });
    let has_datafusion = config.keys().any(|key| key == MEMORY_LIMIT_KEY);
    if (has_typed_setter || has_repark_key) && has_datafusion {
        return Err(Error::Config(format!(
            "both 'repark.memory.limit.gb' and '{MEMORY_LIMIT_KEY}' are set. \
             They configure the same FairSpillPool — use exactly one: \
             repark.memory.limit.gb / memory_limit_gb at build (RAM-relative default, \
             cap 8 GiB; 0 = unbounded), or {MEMORY_LIMIT_KEY} via builder / SQL SET \
             (same pool, one truth, not two knobs)"
        )));
    }
    Ok(())
}

/// Intercept runtime `SET datafusion.runtime.memory_limit` before the dialect reaches DataFusion.
/// # Errors
/// Capacity-parse failures and `RuntimeEnv` rebuild failures fold through [`engine_err`].
pub(crate) fn maybe_apply_runtime_set(
    context: &SessionContext,
    query: &str,
) -> Result<Option<DataFrame>> {
    let Some((key, value)) = parse_set_assignment(query) else {
        return Ok(None);
    };
    if key.eq_ignore_ascii_case(TEMP_DIRECTORY_KEY) {
        return Err(Error::Config(TEMP_DIRECTORY_RUNTIME_REFUSAL.to_string()));
    }
    if !key.eq_ignore_ascii_case(MEMORY_LIMIT_KEY) {
        return Ok(None);
    }
    let bytes = parse_memory_limit_value(&value)?;
    swap_fair_spill_pool(context, if bytes == 0 { None } else { Some(bytes) })?;
    context.read_empty().map_err(engine_err).map(Some)
}

/// Swap the live `RuntimeEnv`'s memory pool.
/// # Errors
/// [`Error::DataFusion`] if the `RuntimeEnv` rebuild fails.
fn swap_fair_spill_pool(context: &SessionContext, pool_bytes: Option<usize>) -> Result<()> {
    let state_lock = context.state_ref();
    let mut state = state_lock.write();
    let mut builder = RuntimeEnvBuilder::from_runtime_env(state.runtime_env());
    builder = match pool_bytes {
        Some(bytes) => builder.with_memory_pool(Arc::new(FairSpillPool::new(bytes))),
        None => builder.with_memory_pool(Arc::new(UnboundedMemoryPool::default())),
    };
    let runtime = builder.build().map_err(engine_err)?;
    *state = SessionStateBuilder::from(state.clone())
        .with_runtime_env(Arc::new(runtime))
        .build();
    Ok(())
}

/// Parse a DataFusion K/M/G capacity (`'256M'`, `'1.5G'`, `'0'`).
/// # Errors
/// [`Error::Config`] naming the key when the string is not a DF capacity.
fn parse_memory_limit_value(value: &str) -> Result<usize> {
    SessionContext::parse_capacity_limit(MEMORY_LIMIT_KEY, value)
        .map_err(|error| Error::Config(format!("invalid {MEMORY_LIMIT_KEY} = '{value}': {error}")))
}

/// Parse `SET [VARIABLE] <key> = <value>` (quoted or bare).
fn parse_set_assignment(sql: &str) -> Option<(String, String)> {
    let stripped = strip_leading_sql_comments(sql);
    let after_set = strip_keyword(stripped, "set")?;
    let after_set = match strip_keyword(after_set, "variable") {
        Some(rest) => rest,
        None => after_set,
    };
    let (key, after_key) = split_ident_path(after_set)?;
    let after_eq = after_key.trim_start();
    let after_eq = after_eq.strip_prefix('=')?;
    let value = parse_sql_scalar(after_eq)?;
    Some((key, value))
}

fn strip_leading_sql_comments(sql: &str) -> &str {
    let mut rest = sql.trim_start();
    loop {
        if rest.starts_with("--") {
            rest = match rest.find('\n') {
                Some(index) => rest[index + 1..].trim_start(),
                None => return "",
            };
            continue;
        }
        if let Some(after) = rest.strip_prefix("/*") {
            rest = match after.find("*/") {
                Some(index) => after[index + 2..].trim_start(),
                None => return "",
            };
            continue;
        }
        break;
    }
    rest
}

fn strip_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    if trimmed.len() < keyword.len() {
        return None;
    }
    let (head, tail) = trimmed.split_at(keyword.len());
    if !head.eq_ignore_ascii_case(keyword) {
        return None;
    }
    if tail.starts_with(|ch: char| ch.is_ascii_alphanumeric() || ch == '_') {
        return None;
    }
    Some(tail)
}

fn split_ident_path(input: &str) -> Option<(String, &str)> {
    let trimmed = input.trim_start();
    let end = trimmed.find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'))?;
    if end == 0 {
        return None;
    }
    let (key, rest) = trimmed.split_at(end);
    if !key
        .bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
    {
        return None;
    }
    Some((key.to_string(), rest))
}

fn parse_sql_scalar(input: &str) -> Option<String> {
    let trimmed = input.trim_start();
    if let Some(rest) = trimmed.strip_prefix('\'') {
        return parse_quoted(rest, '\'');
    }
    if let Some(rest) = trimmed.strip_prefix('"') {
        return parse_quoted(rest, '"');
    }
    let token = trimmed.trim_end().trim_end_matches(';').trim();
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

fn parse_quoted(input: &str, quote: char) -> Option<String> {
    let mut output = String::new();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == quote {
            if chars.peek() == Some(&quote) {
                chars.next();
                output.push(quote);
            } else {
                let rest: String = chars.collect();
                let rest = rest.trim().trim_end_matches(';').trim();
                if rest.is_empty() {
                    return Some(output);
                }
                return None;
            }
        } else {
            output.push(ch);
        }
    }
    None
}

#[cfg(test)]
mod spill_unit_tests {
    use super::*;

    #[test]
    fn parse_set_accepts_facade_and_bare_forms() {
        let cases = [
            (
                "SET datafusion.runtime.memory_limit = '256M'",
                "datafusion.runtime.memory_limit",
                "256M",
            ),
            (
                "set DATAFUSION.RUNTIME.MEMORY_LIMIT='1G'",
                "DATAFUSION.RUNTIME.MEMORY_LIMIT",
                "1G",
            ),
            (
                "SET VARIABLE datafusion.runtime.memory_limit = 64M;",
                "datafusion.runtime.memory_limit",
                "64M",
            ),
            (
                "/* c */ SET datafusion.runtime.memory_limit = \"8M\"",
                "datafusion.runtime.memory_limit",
                "8M",
            ),
            (
                "-- comment\nSET datafusion.runtime.memory_limit = '0'",
                "datafusion.runtime.memory_limit",
                "0",
            ),
        ];
        for (sql, key, value) in cases {
            let parsed = parse_set_assignment(sql).expect(sql);
            assert_eq!(parsed.0, key, "{sql}");
            assert_eq!(parsed.1, value, "{sql}");
        }
    }

    #[test]
    fn parse_set_ignores_non_set_and_other_keys() {
        assert!(parse_set_assignment("SELECT 1").is_none());
        assert!(parse_set_assignment("UPDATE t SET x = 1").is_none());
        let other = parse_set_assignment("SET datafusion.execution.batch_size = '4096'")
            .expect("still a SET");
        assert_eq!(other.0, "datafusion.execution.batch_size");
    }

    #[test]
    fn parse_cgroup_and_meminfo_fixtures() {
        assert_eq!(parse_cgroup_memory_max("max"), None);
        assert_eq!(parse_cgroup_memory_max("MAX\n"), None);
        assert_eq!(parse_cgroup_memory_max(""), None);
        assert_eq!(
            parse_cgroup_memory_max("1073741824\n"),
            Some(1024 * 1024 * 1024)
        );
        assert_eq!(
            parse_meminfo_mem_total("MemTotal:       1048576 kB\nMemFree: 1 kB\n"),
            Some(1024 * 1024 * 1024)
        );
        assert_eq!(parse_meminfo_mem_total("MemFree: 1 kB\n"), None);
    }

    #[test]
    fn clamp_default_is_sixty_percent_between_floor_and_eight_gib() {
        assert_eq!(
            clamp_default_memory_limit_bytes(None),
            DEFAULT_MEMORY_LIMIT_BYTES
        );
        let four_gib = 4 * BYTES_PER_GB;
        assert_eq!(
            clamp_default_memory_limit_bytes(Some(four_gib)),
            four_gib * 3 / 5
        );
        assert_eq!(
            clamp_default_memory_limit_bytes(Some(100 * BYTES_PER_GB)),
            DEFAULT_MEMORY_LIMIT_BYTES
        );
        assert_eq!(
            clamp_default_memory_limit_bytes(Some(MIN_MEMORY_LIMIT_BYTES)),
            MIN_MEMORY_LIMIT_BYTES
        );
    }

    #[test]
    fn parse_capacity_zero_and_units() {
        assert_eq!(parse_memory_limit_value("0").expect("0"), 0);
        assert_eq!(parse_memory_limit_value("1M").expect("1M"), 1024 * 1024);
        assert_eq!(
            parse_memory_limit_value("256M").expect("256M"),
            256 * 1024 * 1024
        );
        assert!(parse_memory_limit_value("nope").is_err());
    }

    #[test]
    fn dual_knobs_refuse_typed_and_config() {
        let mut config = HashMap::new();
        config.insert(MEMORY_LIMIT_KEY.to_string(), "4G".to_string());
        let error = refuse_dual_memory_knobs(true, &config).expect_err("typed + DF");
        let message = error.to_string();
        assert!(message.contains("repark.memory.limit.gb"), "{message}");
        assert!(message.contains(MEMORY_LIMIT_KEY), "{message}");
        assert!(message.contains("FairSpillPool"), "{message}");
    }

    #[test]
    fn dual_knobs_refuse_repark_config_key() {
        let mut config = HashMap::new();
        config.insert("repark.memory.limit.gb".to_string(), "2".to_string());
        config.insert(MEMORY_LIMIT_KEY.to_string(), "4G".to_string());
        assert!(refuse_dual_memory_knobs(false, &config).is_err());
    }

    #[test]
    fn dual_knobs_allow_either_alone() {
        let mut only_df = HashMap::new();
        only_df.insert(MEMORY_LIMIT_KEY.to_string(), "256M".to_string());
        refuse_dual_memory_knobs(false, &only_df).expect("DF alone");
        refuse_dual_memory_knobs(true, &HashMap::new()).expect("typed alone");
    }
}

#[cfg(test)]
mod spill_session_tests {
    use super::*;
    use crate::ReparkSession;
    use datafusion::execution::memory_pool::MemoryLimit;

    fn pool_limit(session: &ReparkSession) -> MemoryLimit {
        session.context().runtime_env().memory_pool.memory_limit()
    }

    #[tokio::test]
    async fn builder_datafusion_memory_limit_installs_fair_spill_pool() {
        let session = ReparkSession::builder()
            .config(MEMORY_LIMIT_KEY, "256M")
            .build()
            .expect("pseudo-key must apply at build");
        match pool_limit(&session) {
            MemoryLimit::Finite(bytes) => {
                assert_eq!(
                    bytes,
                    256 * 1024 * 1024,
                    "builder key must size FairSpillPool"
                );
            }
            MemoryLimit::Infinite => panic!("expected Finite(256 MiB), got Infinite"),
            MemoryLimit::Unknown => panic!("expected Finite(256 MiB), got Unknown"),
        }
    }

    #[tokio::test]
    async fn runtime_set_memory_limit_swaps_fair_spill_pool() {
        let session = ReparkSession::builder()
            .memory_limit_bytes(64 * 1024 * 1024)
            .build()
            .expect("build");
        session
            .sql("SET datafusion.runtime.memory_limit = '16M'")
            .await
            .expect("SET must be intercepted");
        match pool_limit(&session) {
            MemoryLimit::Finite(bytes) => assert_eq!(bytes, 16 * 1024 * 1024),
            MemoryLimit::Infinite => panic!("expected Finite(16 MiB) after SET, got Infinite"),
            MemoryLimit::Unknown => panic!("expected Finite(16 MiB) after SET, got Unknown"),
        }
    }

    #[tokio::test]
    async fn runtime_set_memory_limit_zero_is_unbounded() {
        let session = ReparkSession::builder()
            .memory_limit_bytes(64 * 1024 * 1024)
            .build()
            .expect("build");
        session
            .sql("SET datafusion.runtime.memory_limit = '0'")
            .await
            .expect("SET 0");
        match pool_limit(&session) {
            MemoryLimit::Infinite => {}
            MemoryLimit::Finite(bytes) => {
                panic!("SET 0 must install unbounded, got Finite({bytes})")
            }
            MemoryLimit::Unknown => panic!("SET 0 must install unbounded, got Unknown"),
        }
    }

    #[tokio::test]
    async fn runtime_set_memory_limit_oom_is_fair_not_greedy() {
        // Eight partitions times 2 MiB reservation exceed the 4 MiB pool, forcing a spill.
        let session = ReparkSession::builder()
            .memory_limit_bytes(64 * 1024 * 1024)
            .target_partitions(8)
            .config(
                "datafusion.execution.sort_spill_reservation_bytes",
                "2097152",
            )
            .build()
            .expect("build");
        session
            .sql("SET datafusion.runtime.memory_limit = '4M'")
            .await
            .expect("SET");
        let plan = session
            .sql(
                "SELECT value, md5(cast(value AS varchar)) AS h \
                 FROM generate_series(1, 200000) \
                 ORDER BY h DESC",
            )
            .await
            .expect("plan");
        let error = plan
            .collect()
            .await
            .expect_err("tiny FairSpillPool must OOM");
        let message = error.to_string();
        assert!(
            message.contains("fair("),
            "runtime SET must stay on FairSpillPool: {message}"
        );
        assert!(
            !message.contains("greedy("),
            "runtime SET must not install DataFusion's greedy pool: {message}"
        );
    }

    #[test]
    fn builder_dual_memory_knobs_refuse() {
        let error = ReparkSession::builder()
            .memory_limit_gb(2)
            .config(MEMORY_LIMIT_KEY, "4G")
            .build()
            .expect_err("dual knobs");
        let message = error.to_string();
        assert!(message.contains("repark.memory.limit.gb"), "{message}");
        assert!(message.contains(MEMORY_LIMIT_KEY), "{message}");
        assert!(message.contains("FairSpillPool"), "{message}");
    }

    #[test]
    fn builder_pseudo_key_zero_opts_out() {
        let session = ReparkSession::builder()
            .config(MEMORY_LIMIT_KEY, "0")
            .build()
            .expect("0 opts out");
        match pool_limit(&session) {
            MemoryLimit::Infinite => {}
            MemoryLimit::Finite(bytes) => {
                panic!("builder '0' must be unbounded, got Finite({bytes})")
            }
            MemoryLimit::Unknown => panic!("builder '0' must be unbounded, got Unknown"),
        }
    }

    #[tokio::test]
    async fn runtime_set_temp_directory_refuses_loud_naming_tmpdir() {
        let session = ReparkSession::builder().build().expect("build");
        let error = session
            .sql("SET datafusion.runtime.temp_directory = '/tmp/repark-spill'")
            .await
            .expect_err("runtime SET must refuse");
        let message = error.to_string();
        assert!(
            message.contains("TMPDIR"),
            "refusal must name TMPDIR: {message}"
        );
        assert!(
            message.contains(TEMP_DIRECTORY_KEY),
            "refusal must name the key: {message}"
        );
    }

    #[test]
    fn builder_temp_directory_wires_disk_manager() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_str().expect("utf8 path");
        let session = ReparkSession::builder()
            .config(TEMP_DIRECTORY_KEY, path)
            .build()
            .expect("build-time temp_directory must apply");
        let paths = session
            .context()
            .runtime_env()
            .disk_manager
            .temp_dir_paths();
        assert!(
            paths
                .iter()
                .any(|installed| installed.starts_with(dir.path())),
            "DiskManager paths must live under the configured dir, got {paths:?}"
        );
    }

    #[test]
    fn builder_empty_temp_directory_refuses() {
        let error = ReparkSession::builder()
            .config(TEMP_DIRECTORY_KEY, "   ")
            .build()
            .expect_err("empty path");
        assert!(error.to_string().contains(TEMP_DIRECTORY_KEY));
    }

    #[test]
    fn builder_temp_directory_typo_still_fails_loud() {
        let error = ReparkSession::builder()
            .config("datafusion.runtime.temp_directory2", "/tmp/x")
            .build()
            .expect_err("typo is an unknown ConfigOptions key");
        assert!(
            error
                .to_string()
                .contains("datafusion.runtime.temp_directory2")
        );
    }
}
