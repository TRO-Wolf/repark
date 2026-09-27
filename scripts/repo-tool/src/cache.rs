use crate::repository::{Repository, checked_path};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::Write;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    key: String,
    value: Value,
    checksum: String,
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Cache error contract lives in README.md"
)]
pub fn execute(
    repository: &Repository,
    directory: &str,
    arguments: &[String],
    operation: impl FnOnce() -> Result<Value>,
) -> Result<Value> {
    let cache = checked_path(&repository.root, directory)?;
    if !matches!(
        arguments.first().map(String::as_str),
        Some("maps" | "trace" | "state" | "gates" | "context")
    ) || arguments
        .iter()
        .any(|argument| matches!(argument.as_str(), "--write" | "--init" | "--apply"))
    {
        bail!("only read-only structural commands can use the cache");
    }
    let mut digest = Sha256::new();
    digest.update(repository.snapshot.as_bytes());
    digest.update(serde_json::to_vec(&repository.source)?);
    digest.update(serde_json::to_vec(arguments)?);
    digest.update(std::fs::read(std::env::current_exe()?)?);
    let key = format!("{:x}", digest.finalize());
    let entry_path = checked_path(&repository.root, &format!("{directory}/{key}.json"))?;
    if let Ok(bytes) = std::fs::read(&entry_path)
        && let Ok(entry) = serde_json::from_slice::<Entry>(&bytes)
    {
        let checksum = format!("{:x}", Sha256::digest(serde_json::to_vec(&entry.value)?));
        if entry.key == key
            && entry.checksum == checksum
            && entry.value.get("ok").and_then(Value::as_bool).is_some()
        {
            return Ok(entry.value);
        }
    }
    let value = operation()?;
    if value.get("ok").and_then(Value::as_bool).is_none() {
        bail!("cache result lacks a boolean ok field");
    }
    let checksum = format!("{:x}", Sha256::digest(serde_json::to_vec(&value)?));
    let bytes = serde_json::to_vec(&Entry {
        key,
        value: value.clone(),
        checksum,
    })?;
    std::fs::create_dir_all(&cache).context("create cache directory")?;
    let temporary = cache.join(format!(".{}-write", std::process::id()));
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let result = (|| -> Result<()> {
        output.write_all(&bytes)?;
        output.sync_all()?;
        std::fs::rename(&temporary, &entry_path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result?;
    Ok(value)
}
