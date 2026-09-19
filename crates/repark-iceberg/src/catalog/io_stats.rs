use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IcebergIoOp {
    Exists,
    Metadata,
    Read,
    RangedRead,
    Write,
    Delete,
    List,
}

impl IcebergIoOp {
    pub const ALL: [Self; 7] = [
        Self::Exists,
        Self::Metadata,
        Self::Read,
        Self::RangedRead,
        Self::Write,
        Self::Delete,
        Self::List,
    ];

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Exists => "exists",
            Self::Metadata => "metadata",
            Self::Read => "read",
            Self::RangedRead => "ranged_read",
            Self::Write => "write",
            Self::Delete => "delete",
            Self::List => "list",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Exists => 0,
            Self::Metadata => 1,
            Self::Read => 2,
            Self::RangedRead => 3,
            Self::Write => 4,
            Self::Delete => 5,
            Self::List => 6,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IcebergFileClass {
    TableMetadata,
    ManifestList,
    Manifest,
    DataFile,
    DeleteFile,
    Other,
}

impl IcebergFileClass {
    pub const ALL: [Self; 6] = [
        Self::TableMetadata,
        Self::ManifestList,
        Self::Manifest,
        Self::DataFile,
        Self::DeleteFile,
        Self::Other,
    ];

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::TableMetadata => "table_metadata",
            Self::ManifestList => "manifest_list",
            Self::Manifest => "manifest",
            Self::DataFile => "data_file",
            Self::DeleteFile => "delete_file",
            Self::Other => "other",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::TableMetadata => 0,
            Self::ManifestList => 1,
            Self::Manifest => 2,
            Self::DataFile => 3,
            Self::DeleteFile => 4,
            Self::Other => 5,
        }
    }
}

#[must_use]
pub fn classify_iceberg_path(path: &str) -> IcebergFileClass {
    let trimmed = path.trim_end_matches('/');
    let (parent, name) = trimmed.rsplit_once('/').unwrap_or(("", trimmed));
    let name = name.to_ascii_lowercase();
    let in_metadata_dir = parent.rsplit('/').next() == Some("metadata");
    if name.ends_with(".metadata.json") || name.ends_with(".metadata.json.gz") {
        return IcebergFileClass::TableMetadata;
    }
    let names_deletes = name.starts_with("pos-del")
        || name.starts_with("eq-del")
        || name.starts_with("dv-")
        || name.contains("-deletes");
    if name.ends_with(".avro") && in_metadata_dir {
        return if name.starts_with("snap-") {
            IcebergFileClass::ManifestList
        } else {
            IcebergFileClass::Manifest
        };
    }
    if name.ends_with(".puffin") {
        return if names_deletes || !in_metadata_dir {
            IcebergFileClass::DeleteFile
        } else {
            IcebergFileClass::Other
        };
    }
    if [".parquet", ".orc", ".avro"]
        .iter()
        .any(|extension| name.ends_with(extension))
    {
        return if names_deletes {
            IcebergFileClass::DeleteFile
        } else {
            IcebergFileClass::DataFile
        };
    }
    IcebergFileClass::Other
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IcebergIoCount {
    pub requests: u64,
    pub bytes: u64,
}

impl IcebergIoCount {
    fn add(self, other: Self) -> Self {
        Self {
            requests: self.requests + other.requests,
            bytes: self.bytes + other.bytes,
        }
    }
}

const OPS: usize = IcebergIoOp::ALL.len();
const CLASSES: usize = IcebergFileClass::ALL.len();

#[derive(Debug, Default)]
struct IoCell {
    requests: AtomicU64,
    bytes: AtomicU64,
}

#[derive(Debug, Default)]
pub struct IcebergIoCounters {
    cells: [[IoCell; CLASSES]; OPS],
}

impl IcebergIoCounters {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, op: IcebergIoOp, class: IcebergFileClass, requests: u64, bytes: u64) {
        let cell = &self.cells[op.index()][class.index()];
        if requests > 0 {
            cell.requests.fetch_add(requests, Ordering::Relaxed);
        }
        if bytes > 0 {
            cell.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub fn record_path(&self, op: IcebergIoOp, path: &str, requests: u64, bytes: u64) {
        self.record(op, classify_iceberg_path(path), requests, bytes);
    }

    #[must_use]
    pub fn snapshot(&self) -> IcebergIoStats {
        let mut cells = [[IcebergIoCount::default(); CLASSES]; OPS];
        for (row, counters) in cells.iter_mut().zip(self.cells.iter()) {
            for (count, cell) in row.iter_mut().zip(counters.iter()) {
                *count = IcebergIoCount {
                    requests: cell.requests.load(Ordering::Relaxed),
                    bytes: cell.bytes.load(Ordering::Relaxed),
                };
            }
        }
        IcebergIoStats { cells }
    }

    pub fn reset(&self) {
        for cell in self.cells.iter().flatten() {
            cell.requests.store(0, Ordering::Relaxed);
            cell.bytes.store(0, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IcebergIoStats {
    cells: [[IcebergIoCount; CLASSES]; OPS],
}

impl IcebergIoStats {
    #[must_use]
    pub fn get(&self, op: IcebergIoOp, class: IcebergFileClass) -> IcebergIoCount {
        self.cells[op.index()][class.index()]
    }

    #[must_use]
    pub fn by_op(&self, op: IcebergIoOp) -> IcebergIoCount {
        self.cells[op.index()]
            .iter()
            .fold(IcebergIoCount::default(), |sum, count| sum.add(*count))
    }

    #[must_use]
    pub fn by_class(&self, class: IcebergFileClass) -> IcebergIoCount {
        self.cells
            .iter()
            .fold(IcebergIoCount::default(), |sum, row| {
                sum.add(row[class.index()])
            })
    }

    #[must_use]
    pub fn total(&self) -> IcebergIoCount {
        self.cells
            .iter()
            .flatten()
            .fold(IcebergIoCount::default(), |sum, count| sum.add(*count))
    }
}
