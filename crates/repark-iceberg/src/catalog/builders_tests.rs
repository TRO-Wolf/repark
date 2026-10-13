use iceberg::{Error, ErrorKind};

use super::iceberg_to_datafusion;

fn mapped(kind: ErrorKind, message: &str) -> String {
    iceberg_to_datafusion(Error::new(kind, message)).to_string()
}

fn unchanged(kind: ErrorKind, message: &str) {
    let expected = format!("External error: {}", Error::new(kind, message));
    assert_eq!(mapped(kind, message), expected);
}

#[test]
fn a_v2_nanosecond_schema_error_becomes_the_format_v3_refusal() {
    assert_eq!(
        mapped(
            ErrorKind::DataInvalid,
            "Invalid schema for v2:\n- Invalid type for c: timestamp_ns is not supported until v3"
        ),
        "Error during planning: [TIMESTAMP_NANOS_REQUIRES_FORMAT_V3] Invalid type for c: \
         timestamp_ns is not supported until v3. Create the table with TBLPROPERTIES \
         ('format-version'='3') (writeTo: tableProperty) and \
         `repark.sql.allowCreateFormatVersion3` = true."
    );
    assert_eq!(
        mapped(
            ErrorKind::DataInvalid,
            "Invalid schema for v2:\n\
             - Invalid type for c: timestamp_ns is not supported until v3\n\
             - Invalid type for s.at: timestamptz_ns is not supported until v3"
        ),
        "Error during planning: [TIMESTAMP_NANOS_REQUIRES_FORMAT_V3] Invalid type for c: \
         timestamp_ns is not supported until v3; Invalid type for s.at: timestamptz_ns is not \
         supported until v3. Create the table with TBLPROPERTIES ('format-version'='3') \
         (writeTo: tableProperty) and `repark.sql.allowCreateFormatVersion3` = true."
    );
}

#[test]
fn every_other_error_passes_unchanged() {
    unchanged(
        ErrorKind::DataInvalid,
        "Invalid schema for v2:\n- Invalid type for v: variant is not supported until v3",
    );
    unchanged(
        ErrorKind::DataInvalid,
        "Invalid schema for v2:\n\
         - Invalid type for c: timestamp_ns is not supported until v3\n\
         - Invalid type for v: variant is not supported until v3",
    );
    unchanged(
        ErrorKind::DataInvalid,
        "Invalid schema for v2:\n- Invalid type for timestamp_ns: variant is not supported until v3",
    );
    unchanged(ErrorKind::DataInvalid, "Invalid schema for v2:");
    unchanged(
        ErrorKind::DataInvalid,
        "Cannot change column type: c: timestamp_ns is not supported until v3",
    );
    unchanged(
        ErrorKind::Unexpected,
        "Invalid schema for v2:\n- Invalid type for c: timestamp_ns is not supported until v3",
    );
    unchanged(ErrorKind::TableNotFound, "no such table");
}
