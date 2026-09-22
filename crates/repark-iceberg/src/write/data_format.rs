use datafusion::error::{DataFusionError, Result};
use iceberg::spec::{DataFileFormat, FormatVersion};

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
    data_format: DataFileFormat,
    format_version: FormatVersion,
) -> Result<DataFileFormat> {
    if format_version >= FormatVersion::V3 {
        return Ok(DataFileFormat::Puffin);
    }
    if let Some(raw) = staging_format {
        return parse_format(raw);
    }
    if let Some(raw) = table_property {
        return parse_format(raw);
    }
    Ok(data_format)
}

fn parse_format(raw: &str) -> Result<DataFileFormat> {
    match raw.to_ascii_lowercase().as_str() {
        "parquet" => Ok(DataFileFormat::Parquet),
        "orc" => Ok(DataFileFormat::Orc),
        "avro" => Ok(DataFileFormat::Avro),
        _ => Err(DataFusionError::Plan(format!("Invalid file format: {raw}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(
            error.to_string().contains("Invalid file format: csv"),
            "{error}"
        );
    }

    #[test]
    fn unknown_table_property_refuses_with_java_shape() {
        let error = resolve_data_format(None, Some("delta")).expect_err("delta property");
        assert!(
            error.to_string().contains("Invalid file format: delta"),
            "{error}"
        );
    }

    #[test]
    fn v3_delete_side_is_puffin_for_every_data_format() {
        for data_format in [
            DataFileFormat::Parquet,
            DataFileFormat::Orc,
            DataFileFormat::Avro,
        ] {
            let resolved = resolve_delete_format(None, None, data_format, FormatVersion::V3)
                .expect("v3 delete");
            assert_eq!(resolved, DataFileFormat::Puffin);
        }
    }

    #[test]
    fn v3_delete_side_ignores_explicit_formats() {
        let resolved = resolve_delete_format(
            Some("orc"),
            Some("avro"),
            DataFileFormat::Orc,
            FormatVersion::V3,
        )
        .expect("v3 delete");
        assert_eq!(resolved, DataFileFormat::Puffin);
    }

    #[test]
    fn delete_falls_back_to_data_format() {
        let orc = resolve_delete_format(None, None, DataFileFormat::Orc, FormatVersion::V2)
            .expect("orc delete");
        assert_eq!(orc, DataFileFormat::Orc);
        let avro = resolve_delete_format(None, None, DataFileFormat::Avro, FormatVersion::V2)
            .expect("avro delete");
        assert_eq!(avro, DataFileFormat::Avro);
    }

    #[test]
    fn delete_property_beats_data_format() {
        let resolved = resolve_delete_format(
            None,
            Some("parquet"),
            DataFileFormat::Orc,
            FormatVersion::V2,
        )
        .expect("delete property");
        assert_eq!(resolved, DataFileFormat::Parquet);
    }

    #[test]
    fn staging_delete_format_beats_table_property() {
        let resolved = resolve_delete_format(
            Some("avro"),
            Some("parquet"),
            DataFileFormat::Orc,
            FormatVersion::V2,
        )
        .expect("staging delete");
        assert_eq!(resolved, DataFileFormat::Avro);
    }

    #[test]
    fn unknown_delete_format_refuses_with_java_shape() {
        let error =
            resolve_delete_format(None, Some("csv"), DataFileFormat::Orc, FormatVersion::V2)
                .expect_err("csv delete property");
        assert!(
            error.to_string().contains("Invalid file format: csv"),
            "{error}"
        );
    }
}
