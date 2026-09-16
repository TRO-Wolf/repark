use super::{
    ArrowNameSurface, SPATIAL_MIXED_SRID, SparkDataType, arrow_name_at_depth,
    arrow_type_from_spark, logical_type_key, parse_ddl, simple_string,
};
use datafusion::arrow::datatypes::DataType as ArrowDataType;

#[test]
fn logical_key_reports_spark_narrow_widths() {
    for (data_type, expected) in [
        (ArrowDataType::Int8, "byte"),
        (ArrowDataType::Int16, "short"),
        (ArrowDataType::Int32, "int"),
        (ArrowDataType::Int64, "long"),
        (ArrowDataType::Float32, "float"),
        (ArrowDataType::Float64, "double"),
        (ArrowDataType::Binary, "binary"),
        (ArrowDataType::LargeBinary, "binary"),
        (ArrowDataType::BinaryView, "binary"),
    ] {
        assert_eq!(logical_type_key(&data_type), expected);
    }
}

#[test]
fn describe_keeps_spark_ddl_spellings_for_narrow_widths() {
    for (data_type, expected) in [
        (ArrowDataType::Int8, "tinyint"),
        (ArrowDataType::Int16, "smallint"),
        (ArrowDataType::Int64, "bigint"),
        (ArrowDataType::Float32, "float"),
        (ArrowDataType::Binary, "binary"),
    ] {
        assert_eq!(
            arrow_name_at_depth(&data_type, ArrowNameSurface::Describe, 0),
            expected
        );
    }
}

const PARSE_CELLS: &[(&str, &str)] = &[
    ("g geometry(4326)", "struct<g:geometry(4326)>"),
    ("geometry(4326)", "geometry(4326)"),
    ("geography(4326)", "geography(4326)"),
    ("geometry(0)", "geometry(0)"),
    ("geometry(3857)", "geometry(3857)"),
    ("geometry(any)", "geometry(any)"),
    ("geography(any)", "geography(any)"),
    ("geometry(ANY)", "geometry(any)"),
    ("GEOMETRY(4326)", "geometry(4326)"),
    ("geometry( 4326 )", "geometry(4326)"),
    (
        "a int, g geography(4326)",
        "struct<a:int,g:geography(4326)>",
    ),
    ("struct<g: geometry(4326)>", "struct<g:geometry(4326)>"),
    ("array<geography(4326)>", "array<geography(4326)>"),
    ("map<string, geometry(0)>", "map<string,geometry(0)>"),
];

const REFUSE_CELLS: &[&str] = &[
    "geography(3857)",
    "geography(0)",
    "geometry",
    "geography",
    "geometry(9999)",
    "geography(-1)",
    "geometry(OGC:CRS84)",
];

#[test]
fn spatial_ddl_accept_cells_parse() {
    for (ddl, simple) in PARSE_CELLS {
        let parsed = parse_ddl(ddl).unwrap_or_else(|error| panic!("{ddl} parses: {error}"));
        assert_eq!(simple_string(&parsed), (*simple).to_string(), "{ddl}");
    }
}

#[test]
fn spatial_ddl_refuse_cells_keep_parse_error() {
    for ddl in REFUSE_CELLS {
        let error = parse_ddl(ddl).expect_err(&format!("{ddl} refuses"));
        assert!(error.to_string().contains("cannot parse datatype"), "{ddl}");
    }
}

#[test]
fn spatial_srid_follows_spark_integer_value_grammar() {
    let parsed =
        parse_ddl("geometry(04326)").unwrap_or_else(|error| panic!("leading zeros parse: {error}"));
    assert_eq!(simple_string(&parsed), "geometry(4326)");
    for ddl in ["geometry(4_326)", "geometry(+4326)", "geometry(４３２６)"] {
        let error = parse_ddl(ddl).expect_err(&format!("{ddl} refuses"));
        assert!(error.to_string().contains("cannot parse datatype"), "{ddl}");
    }
}

#[test]
fn spatial_mixed_form_uses_spark_any_marker() {
    for ddl in ["geometry(any)", "geography(any)", "geometry(ANY)"] {
        let parsed = parse_ddl(ddl).unwrap_or_else(|error| panic!("{ddl} parses: {error}"));
        let srid = match parsed {
            SparkDataType::Geometry { srid } | SparkDataType::Geography { srid } => srid,
            other => panic!("{ddl} parses spatial: {}", simple_string(&other)),
        };
        assert_eq!(srid, SPATIAL_MIXED_SRID, "{ddl}");
    }
}

#[test]
fn spatial_arrow_mapping_refuses_naming_the_type() {
    for data_type in [
        SparkDataType::Geometry { srid: 4326 },
        SparkDataType::Geography { srid: 4326 },
        SparkDataType::Geometry {
            srid: SPATIAL_MIXED_SRID,
        },
    ] {
        let simple = simple_string(&data_type);
        let error = arrow_type_from_spark(&data_type).expect_err(&format!("{simple} refuses"));
        assert!(error.to_string().contains(&simple), "{simple}");
    }
}
