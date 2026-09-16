mod augment;
mod ensure;
mod error;
mod status;
mod udf;

#[cfg(test)]
mod tests;

mod scan;

pub(crate) use ensure::mark_file_scan;
pub use ensure::{ensure_file_metadata, expr_mentions_file_metadata};
pub use error::FileMetadataError;
pub use scan::FileKind;
pub use status::{FileMetadataStatus, file_metadata_status};
pub const METADATA_COLUMN_NAME: &str = "_metadata";

pub const SHADOW_METADATA_COLUMN_NAME: &str = "__metadata";

pub const FILE_SOURCE_METADATA_KEY: &str = "__file_source_metadata_col";

pub const METADATA_COL_KEY: &str = "__metadata_col";
