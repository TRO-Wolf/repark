use datafusion::error::{DataFusionError, Result};
use iceberg::spec::{DataFileFormat, FormatVersion};
use iceberg::table::Table;

use crate::write::MorDmlKind;

pub const DELETE_FORMAT_PROPERTY: &str = "write.delete.format.default";
const DELETE_FORMAT_OPTION: &str = "delete-format";
const DATA_FORMAT_PROPERTY: &str = "write.format.default";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkDeleteFormat {
    Readable,
    Inherited(DataFileFormat),
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve_data_format(
    staging_format: Option<&str>,
    table_default: Option<&str>,
) -> Result<DataFileFormat> {
    if let Some(raw) = staging_format {
        return parse_format(raw);
    }
    if let Some(raw) = table_default {
        return parse_format(raw);
    }
    Ok(DataFileFormat::Parquet)
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve_delete_format(
    staging_format: Option<&str>,
    table_property: Option<&str>,
    format_version: FormatVersion,
) -> Result<DataFileFormat> {
    if format_version >= FormatVersion::V3 {
        return Ok(DataFileFormat::Puffin);
    }
    if let Some(raw) = staging_format {
        return readable_delete_format(raw, DELETE_FORMAT_OPTION);
    }
    if let Some(raw) = table_property {
        return readable_delete_format(raw, DELETE_FORMAT_PROPERTY);
    }
    Ok(DataFileFormat::Parquet)
}

#[allow(clippy::missing_errors_doc)]
pub fn fork_mor_delete_format(table: &Table, kind: MorDmlKind) -> Result<ForkDeleteFormat> {
    let metadata = table.metadata();
    if metadata.format_version() != FormatVersion::V2 {
        return Ok(ForkDeleteFormat::Readable);
    }
    let properties = metadata.properties();
    let merge_on_read = properties
        .get(kind.mode_property())
        .is_some_and(|mode| mode.trim().eq_ignore_ascii_case("merge-on-read"));
    if !merge_on_read {
        return Ok(ForkDeleteFormat::Readable);
    }
    if let Some(raw) = properties.get(DELETE_FORMAT_PROPERTY) {
        readable_delete_format(raw, DELETE_FORMAT_PROPERTY)?;
        return Ok(ForkDeleteFormat::Readable);
    }
    match resolve_data_format(
        None,
        properties.get(DATA_FORMAT_PROPERTY).map(String::as_str),
    )? {
        DataFileFormat::Avro => Ok(ForkDeleteFormat::Inherited(DataFileFormat::Avro)),
        DataFileFormat::Orc => Ok(ForkDeleteFormat::Inherited(DataFileFormat::Orc)),
        _ => Ok(ForkDeleteFormat::Readable),
    }
}

#[must_use]
pub fn inherited_delete_format_refusal(table_sql: &str, format: DataFileFormat) -> DataFusionError {
    DataFusionError::Plan(format!(
        "table `{table_sql}` inherits the position-delete format '{format}' from \
         `{DATA_FORMAT_PROPERTY}`, and this statement shape runs on a writer that writes \
         {format} position-delete files, which RePark cannot read yet; the merge-on-read write \
         is refused before any file is written. Hint: set `{DELETE_FORMAT_PROPERTY}` to \
         'parquet' (Spark reads Parquet position-delete files on {format} tables)"
    ))
}

fn readable_delete_format(raw: &str, source: &str) -> Result<DataFileFormat> {
    match parse_format(raw)? {
        DataFileFormat::Parquet => Ok(DataFileFormat::Parquet),
        format => Err(DataFusionError::Plan(format!(
            "`{source}` = '{raw}' asks for {format} position-delete files, which RePark cannot \
             read yet; the merge-on-read write is refused before any file is written. Hint: set \
             `{source}` to 'parquet'"
        ))),
    }
}

fn parse_format(raw: &str) -> Result<DataFileFormat> {
    match raw.to_ascii_lowercase().as_str() {
        "parquet" => Ok(DataFileFormat::Parquet),
        "orc" => Ok(DataFileFormat::Orc),
        "avro" => Ok(DataFileFormat::Avro),
        _ => Err(crate::write::illegal_argument_error(format!(
            "Invalid file format: {raw}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::IllegalArgumentMarker;

    #[test]
    fn staging_format_beats_table_property() {
        let resolved = resolve_data_format(Some("orc"), Some("parquet")).expect("orc option");
        assert_eq!(resolved, DataFileFormat::Orc);
    }

    #[test]
    fn table_property_used_when_no_staging_format() {
        let resolved = resolve_data_format(None, Some("avro")).expect("avro property");
        assert_eq!(resolved, DataFileFormat::Avro);
    }

    #[test]
    fn absent_option_and_property_default_to_parquet() {
        let resolved = resolve_data_format(None, None).expect("default");
        assert_eq!(resolved, DataFileFormat::Parquet);
    }

    #[test]
    fn format_names_match_case_insensitively() {
        let upper = resolve_data_format(Some("ORC"), None).expect("upper orc");
        assert_eq!(upper, DataFileFormat::Orc);
        let mixed = resolve_data_format(None, Some("AvRo")).expect("mixed avro");
        assert_eq!(mixed, DataFileFormat::Avro);
    }

    #[test]
    fn unknown_staging_format_refuses_with_java_shape() {
        let error = resolve_data_format(Some("csv"), Some("parquet")).expect_err("csv option");
        let DataFusionError::External(inner) = &error else {
            panic!("expected an External marker, got {error:?}");
        };
        let marker = inner
            .downcast_ref::<IllegalArgumentMarker>()
            .expect("expected an IllegalArgumentMarker");
        assert_eq!(marker.0, "Invalid file format: csv");
    }

    #[test]
    fn unknown_table_property_refuses_with_java_shape() {
        let error = resolve_data_format(None, Some("delta")).expect_err("delta property");
        let DataFusionError::External(inner) = &error else {
            panic!("expected an External marker, got {error:?}");
        };
        let marker = inner
            .downcast_ref::<IllegalArgumentMarker>()
            .expect("expected an IllegalArgumentMarker");
        assert_eq!(marker.0, "Invalid file format: delta");
    }

    #[test]
    fn v3_delete_side_is_puffin() {
        let resolved = resolve_delete_format(None, None, FormatVersion::V3).expect("v3 delete");
        assert_eq!(resolved, DataFileFormat::Puffin);
    }

    #[test]
    fn v3_delete_side_ignores_explicit_formats() {
        let resolved =
            resolve_delete_format(Some("orc"), Some("avro"), FormatVersion::V3).expect("v3 delete");
        assert_eq!(resolved, DataFileFormat::Puffin);
    }

    #[test]
    fn v2_delete_defaults_to_parquet_whatever_the_data_format() {
        let resolved = resolve_delete_format(None, None, FormatVersion::V2).expect("v2 delete");
        assert_eq!(resolved, DataFileFormat::Parquet);
    }

    #[test]
    fn explicit_parquet_delete_property_resolves_parquet() {
        let resolved = resolve_delete_format(None, Some("PARQUET"), FormatVersion::V2)
            .expect("delete property");
        assert_eq!(resolved, DataFileFormat::Parquet);
    }

    #[test]
    fn staging_delete_format_beats_table_property() {
        let resolved = resolve_delete_format(Some("parquet"), Some("avro"), FormatVersion::V2)
            .expect("staging delete");
        assert_eq!(resolved, DataFileFormat::Parquet);
    }

    #[test]
    fn explicit_avro_or_orc_delete_format_refuses_naming_its_source() {
        for (staging, property, source, format) in [
            (None, Some("avro"), DELETE_FORMAT_PROPERTY, "avro"),
            (None, Some("ORC"), DELETE_FORMAT_PROPERTY, "orc"),
            (Some("avro"), Some("parquet"), DELETE_FORMAT_OPTION, "avro"),
            (Some("orc"), None, DELETE_FORMAT_OPTION, "orc"),
        ] {
            let error = resolve_delete_format(staging, property, FormatVersion::V2)
                .expect_err("an unreadable delete format refuses");
            let DataFusionError::Plan(message) = &error else {
                panic!("expected a plan error, got {error:?}");
            };
            assert!(message.starts_with(&format!("`{source}` = ")), "{message}");
            assert!(
                message.contains(&format!("{format} position-delete files")),
                "{message}"
            );
            assert!(
                message.contains(&format!("set `{source}` to 'parquet'")),
                "{message}"
            );
        }
    }

    #[test]
    fn unknown_delete_format_refuses_with_java_shape() {
        let error = resolve_delete_format(None, Some("csv"), FormatVersion::V2)
            .expect_err("csv delete property");
        let DataFusionError::External(inner) = &error else {
            panic!("expected an External marker, got {error:?}");
        };
        let marker = inner
            .downcast_ref::<IllegalArgumentMarker>()
            .expect("expected an IllegalArgumentMarker");
        assert_eq!(marker.0, "Invalid file format: csv");
    }
}
