//! `FairSpillPool` install, runtime SET intercept, and spill-disk (`temp_directory`) policy.
//!
//! DataFusion 54.1's `SET datafusion.runtime.memory_limit` handler rebuilds the live
//! `RuntimeEnv` with a `GreedyMemoryPool` inside `TrackConsumersPool` — the pool type its
//! own rustdoc says "works well for queries that do not need to spill". Repark intercepts
//! that key at [`crate::ReparkSession::sql_with`] (every door, including raw SQL `SET`) and
//! swaps a **new** [`FairSpillPool`] of the requested size. `FairSpillPool` stores
//! `pool_size` outside its mutex — there is no in-place resize — so in-flight reservations
//! stay on the old pool.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::{FairSpillPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{DataFrame, SessionContext};
use repark_common::{Error, Result};

use crate::engine_err;

/// Bytes in one GiB (the `memory_limit_gb` conversion unit).
pub(crate) const BYTES_PER_GB: usize = 1024 * 1024 * 1024;

/// Smallest non-zero `memory_limit_bytes` accepted by [`crate::ReparkSessionBuilder::build`].
/// Explicit `0` still opts out (unbounded). Pathological 1-byte budgets thrash the spill pool
/// without a clean config error (audit SAF-007 / 2026-07-25).
pub(crate) const MIN_MEMORY_LIMIT_BYTES: usize = 1024 * 1024;

/// Default `FairSpillPool` budget when the builder leaves memory unset (C1-Q-002 / C1-L-005).
///
/// Unbounded DataFusion defaults OOM on large sorts/aggregations/MERGE; 8 GiB is a conservative
/// single-node ceiling that still spills rather than hard-failing. Override with
/// [`crate::ReparkSessionBuilder::memory_limit_gb`] /
/// [`crate::ReparkSessionBuilder::memory_limit_bytes`]; pass `memory_limit_bytes(0)` to opt out
/// and keep DataFusion's unbounded pool.
///
/// `sort_spill_reservation_bytes × target_partitions` is a **non-spillable** floor claimed
/// per partition by `ExternalSorterMerge` before a row is sorted. At shipped defaults that
/// floor scales with host cores (10 MiB × cores).
pub(crate) const DEFAULT_MEMORY_LIMIT_BYTES: usize = 8 * BYTES_PER_GB;

/// Canonical runtime / builder pseudo-key for the live `FairSpillPool` size.
pub(crate) const MEMORY_LIMIT_KEY: &str = "datafusion.runtime.memory_limit";

/// Build-time spill directory (`RuntimeEnvBuilder::with_temp_file_path`). Runtime SET refuses.
pub(crate) const TEMP_DIRECTORY_KEY: &str = "datafusion.runtime.temp_directory";

/// Loud runtime refusal: `DiskManager` is fixed after `RuntimeEnv` build. Names `TMPDIR`.
pub(crate) const TEMP_DIRECTORY_RUNTIME_REFUSAL: &str = "datafusion.runtime.temp_directory cannot be changed at runtime \
     (the DiskManager is fixed when the RuntimeEnv is built). \
     Set TMPDIR in the process environment before start, or \
     SparkSession.builder.config('datafusion.runtime.temp_directory', path).getOrCreate() \
     at build time.";

/// Builder-only `FairSpillPool` size keys (Python also refuses a runtime `conf.set` of these).
const REPARK_MEMORY_LIMIT_KEYS: &[&str] =
    &["repark.memory.limit.gb", "spark.repark.memory.limit.gb"];

/// Repark-owned pseudo-keys that share the `datafusion.` prefix but are NOT DataFusion
/// `ConfigOptions` keys, excluded from `apply_datafusion_config_keys`'s build-time sweep.
///
/// `datafusion.runtime.memory_limit` is applied to a [`FairSpillPool`] at `build()` and on
/// runtime `SET`. `datafusion.runtime.temp_directory` is applied via
/// `RuntimeEnvBuilder::with_temp_file_path` at `build()` only; runtime SET refuses loud
/// (names `TMPDIR`). Pushing either through `options_mut().set` fails loud. The exclusion is
/// EXACT-KEY, never a prefix: a typo of a pseudo-key must still fail loud.
pub const REPARK_OWNED_DATAFUSION_PSEUDO_KEYS: &[&str] = &[MEMORY_LIMIT_KEY, TEMP_DIRECTORY_KEY];

/// ===========================================================================================
/// Install `pool_bytes` as a [`FairSpillPool`], or leave DataFusion's unbounded default.
/// ===========================================================================================
pub(crate) fn with_memory_pool(
    runtime: RuntimeEnvBuilder,
    pool_bytes: Option<usize>,
) -> RuntimeEnvBuilder {
    match pool_bytes {
        Some(bytes) => runtime.with_memory_pool(Arc::new(FairSpillPool::new(bytes))),
        None => runtime,
    }
}

/// ===========================================================================================
/// Apply builder `datafusion.runtime.temp_directory` via `with_temp_file_path` (build time only).
/// ===========================================================================================
///
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

/// ===========================================================================================
/// Resolve the build-time pool: typed setter, else the `memory_limit` pseudo-key, else default.
///
/// `Some(0)` / `'0'` opts out (unbounded). Dual typed/`repark.memory.limit.gb` +
/// `datafusion.runtime.memory_limit` refuses. Non-zero budgets below
/// [`MIN_MEMORY_LIMIT_BYTES`] refuse (SAF-007).
/// ===========================================================================================
///
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
            None => Some(DEFAULT_MEMORY_LIMIT_BYTES),
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

/// ===========================================================================================
/// Refuse a builder that names both `FairSpillPool` knobs (same pool, ambiguous initial size).
/// ===========================================================================================
///
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
             repark.memory.limit.gb / memory_limit_gb at build (default 8 GiB; \
             0 = unbounded), or {MEMORY_LIMIT_KEY} via builder / SQL SET \
             (same pool, one truth, not two knobs)"
        )));
    }
    Ok(())
}

/// ===========================================================================================
/// Intercept runtime `SET datafusion.runtime.memory_limit` before the dialect reaches DataFusion.
///
/// Other statements (including other `SET` keys) return `Ok(None)` so the dialect runs unchanged.
/// A handled `SET` returns an empty [`DataFrame`], matching DataFusion's own SET result.
///
/// DataFusion 54.1 has no [`FairSpillPool`] resize: this **swaps** a new pool. In-flight
/// reservations stay on the old pool.
/// ===========================================================================================
///
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

/// ===========================================================================================
/// Swap the live `RuntimeEnv`'s memory pool. `None` installs DataFusion's unbounded default.
/// ===========================================================================================
///
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

/// ===========================================================================================
/// Parse a DataFusion K/M/G capacity (`'256M'`, `'1.5G'`, `'0'`).
/// ===========================================================================================
///
/// # Errors
/// [`Error::Config`] naming the key when the string is not a DF capacity.
fn parse_memory_limit_value(value: &str) -> Result<usize> {
    SessionContext::parse_capacity_limit(MEMORY_LIMIT_KEY, value)
        .map_err(|error| Error::Config(format!("invalid {MEMORY_LIMIT_KEY} = '{value}': {error}")))
}

/// ===========================================================================================
/// Parse `SET [VARIABLE] <key> = <value>` (quoted or bare). `None` if this is not a SET.
/// ===========================================================================================
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
        // 8 partitions × 2 MiB non-spillable reservation > 4 MiB pool, plus enough rows
        // that ExternalSorter actually claims those reservations.
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
