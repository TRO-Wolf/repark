mod augment;
mod ensure;
mod error;
mod status;
mod udf;

#[cfg(test)]
mod tests;

mod scan;

pub use ensure::{ensure_file_metadata, expr_mentions_file_metadata};
pub(crate) use ensure::{hidden_field_or_reject, mark_file_scan, rewrite_metadata_refs};
pub use error::FileMetadataError;
pub use scan::FileKind;
pub use status::{FileMetadataStatus, file_metadata_status};
pub(crate) use status::{WalkOutcome, marker_kind};
pub(crate) use udf::{
    METADATA_FIELD_NAMES, METADATA_UDF_NAME, METADATA_UDF_NAME_NO_ROW_INDEX, file_metadata_call,
    metadata_fields, metadata_outer_field,
};
pub const METADATA_COLUMN_NAME: &str = "_metadata";

pub const SHADOW_METADATA_COLUMN_NAME: &str = "__metadata";

pub const FILE_SOURCE_METADATA_KEY: &str = "__file_source_metadata_col";

pub const METADATA_COL_KEY: &str = "__metadata_col";
