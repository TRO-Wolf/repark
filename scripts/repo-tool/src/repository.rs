use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InputScope {
    Full,
    Maps,
    Files(BTreeSet<String>),
}

impl InputScope {
    fn reads(&self, path: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Maps => path == "map.md" || path.ends_with("/map.md"),
            Self::Files(paths) => paths.contains(path),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Repository {
    pub root: PathBuf,
    pub source: String,
    pub snapshot: String,
    pub input_scope: InputScope,
    pub paths: BTreeSet<String>,
    pub files: BTreeMap<String, String>,
}

impl Repository {
    #[expect(
        clippy::missing_errors_doc,
        reason = "Snapshot error contracts live in README.md"
    )]
    pub fn load(root: &Path, source: &str) -> Result<Self> {
        Self::load_scoped(root, source, InputScope::Full)
    }

    #[expect(
        clippy::missing_errors_doc,
        reason = "Scoped input contracts live in README.md"
    )]
    pub fn load_scoped(root: &Path, source: &str, input_scope: InputScope) -> Result<Self> {
        let root = root
            .canonicalize()
            .context("repository root does not exist")?;
        let top = git(&root, &["rev-parse", "--show-toplevel"])?;
        let top = PathBuf::from(String::from_utf8(top)?.trim()).canonicalize()?;
        if top != root {
            bail!("--repo must name the repository root");
        }
        let mut records = snapshot_records(&root, source)?;
        if let InputScope::Files(selected) = &input_scope {
            for path in selected {
                validate_relative(path)?;
            }
            records.retain(|path, _| selected.contains(path));
        }
        let objects: Vec<_> = records
            .iter()
            .filter(|(path, (mode, _))| mode != "160000" && input_scope.reads(path))
            .map(|(_, (_, object))| object.clone())
            .collect();
        let mut blobs = if source == "worktree" {
            Vec::new()
        } else {
            read_blobs(&root, &objects)?
        }
        .into_iter();
        let mut paths = BTreeSet::new();
        let mut files = BTreeMap::new();
        let mut digest = Sha256::new();
        if input_scope != InputScope::Full {
            digest.update(b"scoped-input-v1\0");
            digest.update(serde_json::to_vec(&input_scope)?);
        }
        if input_scope == InputScope::Full {
            for (path, (mode, object)) in &records {
                if mode == "160000" {
                    hash_gitlink(&mut digest, path, mode, object);
                }
            }
        }
        for (path, (mode, _)) in records {
            if mode == "160000" && input_scope == InputScope::Full {
                paths.insert(path);
                continue;
            }
            digest.update(path.as_bytes());
            digest.update([0]);
            digest.update(mode.as_bytes());
            if input_scope != InputScope::Full {
                digest.update([0]);
            }
            let selected = input_scope.reads(&path);
            let bytes = if mode == "160000" {
                None
            } else if source == "worktree" {
                if !worktree_present(&root, &path, &mode)? {
                    digest.update(b"missing");
                    continue;
                }
                if selected {
                    Some(read_worktree(&root, &path, &mode)?)
                } else {
                    None
                }
            } else if selected {
                Some(blobs.next().context("missing Git blob")?)
            } else {
                None
            };
            paths.insert(path.clone());
            if let Some(bytes) = bytes {
                digest.update((bytes.len() as u64).to_le_bytes());
                digest.update(&bytes);
                if mode != "120000"
                    && let Ok(text) = String::from_utf8(bytes)
                {
                    files.insert(path, text);
                }
            }
        }
        Ok(Self {
            root,
            source: source.to_owned(),
            snapshot: format!("{:x}", digest.finalize()),
            input_scope,
            paths,
            files,
        })
    }

    #[expect(
        clippy::missing_errors_doc,
        reason = "Snapshot error contracts live in README.md"
    )]
    pub fn read(&self, path: &str) -> Result<&str> {
        validate_relative(path)?;
        self.files
            .get(path)
            .map(String::as_str)
            .with_context(|| format!("text file absent from snapshot: {path}"))
    }

    #[must_use]
    pub fn contains(&self, path: &str) -> bool {
        let prefix = format!("{path}/");
        self.paths.contains(path)
            || self
                .paths
                .range(prefix.clone()..)
                .next()
                .is_some_and(|entry| entry.starts_with(&prefix))
    }

    #[must_use]
    pub fn directories(&self) -> BTreeSet<String> {
        let mut directories = BTreeSet::new();
        for path in &self.paths {
            let mut parent = Path::new(path).parent();
            while let Some(directory) = parent {
                directories.insert(directory.to_string_lossy().into_owned());
                parent = directory.parent();
            }
        }
        directories
    }

    #[expect(
        clippy::missing_errors_doc,
        reason = "Snapshot error contracts live in README.md"
    )]
    pub fn write_text(&self, path: &str, text: &str) -> Result<()> {
        if self.source != "worktree" {
            bail!("writes require a worktree snapshot");
        }
        let destination = checked_path(&self.root, path)?;
        let current = match std::fs::read_to_string(&destination) {
            Ok(text) => Some(text),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).with_context(|| format!("read before write: {path}")),
        };
        if current.as_deref() != self.files.get(path).map(String::as_str) {
            bail!("file changed since snapshot: {path}");
        }
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(destination, text).with_context(|| format!("write {path}"))
    }
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Snapshot error contracts live in README.md"
)]
pub fn validate_relative(path: &str) -> Result<()> {
    if path.is_empty()
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || path.contains('\\')
        || Path::new(path)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("expected a repository-relative path without traversal: {path}");
    }
    Ok(())
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Snapshot error contracts live in README.md"
)]
pub fn checked_path(root: &Path, path: &str) -> Result<PathBuf> {
    validate_relative(path)?;
    let mut current = root.to_path_buf();
    for component in Path::new(path).components() {
        current.push(component);
        if let Ok(metadata) = std::fs::symlink_metadata(&current)
            && metadata.file_type().is_symlink()
        {
            bail!("symlink path refused: {path}");
        }
    }
    Ok(current)
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Snapshot error contracts live in README.md"
)]
pub fn git(root: &Path, arguments: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()?;
    if !output.status.success() {
        bail!(
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

fn read_blobs(root: &Path, objects: &[String]) -> Result<Vec<Vec<u8>>> {
    if objects.is_empty() {
        return Ok(Vec::new());
    }
    let mut child = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut input = child.stdin.take().context("Git stdin unavailable")?;
    let requests = format!("{}\n", objects.join("\n"));
    let writer = std::thread::spawn(move || input.write_all(requests.as_bytes()));
    let output = child.wait_with_output()?;
    writer
        .join()
        .map_err(|_| anyhow::anyhow!("Git input writer failed"))??;
    if !output.status.success() {
        bail!("Git blob batch failed");
    }
    let mut bytes = output.stdout.as_slice();
    let mut blobs = Vec::new();
    for _ in objects {
        let newline = bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .context("missing blob header")?;
        let header = std::str::from_utf8(&bytes[..newline])?;
        let fields: Vec<_> = header.split_whitespace().collect();
        if fields.len() != 3 || fields[1] != "blob" {
            bail!("invalid blob header: {header}");
        }
        let length: usize = fields[2].parse()?;
        bytes = &bytes[newline + 1..];
        if bytes.len() <= length || bytes[length] != b'\n' {
            bail!("truncated Git blob");
        }
        blobs.push(bytes[..length].to_vec());
        bytes = &bytes[length + 1..];
    }
    if !bytes.is_empty() {
        bail!("unexpected Git blob output");
    }
    Ok(blobs)
}

fn hash_gitlink(digest: &mut Sha256, path: &str, mode: &str, object: &str) {
    digest.update(path.as_bytes());
    digest.update([0]);
    digest.update(mode.as_bytes());
    digest.update(object.as_bytes());
    digest.update([0]);
}

fn snapshot_records(root: &Path, source: &str) -> Result<BTreeMap<String, (String, String)>> {
    let staged = source == "worktree" || source == "index";
    let entries = if staged {
        git(root, &["ls-files", "--stage", "-z"])?
    } else {
        let revision = String::from_utf8(git(
            root,
            &["rev-parse", "--verify", &format!("{source}^{{tree}}")],
        )?)?;
        git(root, &["ls-tree", "-rz", "--full-tree", revision.trim()])?
    };
    let mut records = BTreeMap::new();
    for record in entries
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        let record = std::str::from_utf8(record).context("Git path is not UTF-8")?;
        let (metadata, path) = record.split_once('\t').context("malformed Git record")?;
        validate_relative(path)?;
        let fields: Vec<_> = metadata.split_whitespace().collect();
        if fields.len() != 3 {
            bail!("malformed Git record: {path}");
        }
        if staged && fields[2] != "0" {
            bail!("unmerged index entry: {path}");
        }
        records.insert(
            path.to_owned(),
            (
                fields[0].to_owned(),
                fields[if staged { 1 } else { 2 }].to_owned(),
            ),
        );
    }
    Ok(records)
}

fn worktree_present(root: &Path, path: &str, mode: &str) -> Result<bool> {
    if let Some((parent, _)) = path.rsplit_once('/') {
        checked_path(root, parent)?;
    }
    match std::fs::symlink_metadata(root.join(path)) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() != (mode == "120000") {
                bail!("tracked path changed symlink type: {path}");
            }
            if !metadata.is_file() && !metadata.file_type().is_symlink() {
                bail!("tracked path is not a file: {path}");
            }
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect {path}")),
    }
}

fn read_worktree(root: &Path, path: &str, mode: &str) -> Result<Vec<u8>> {
    if mode == "120000" {
        Ok(std::fs::read_link(root.join(path))?
            .as_os_str()
            .as_encoded_bytes()
            .to_vec())
    } else {
        std::fs::read(checked_path(root, path)?).with_context(|| format!("read {path}"))
    }
}
