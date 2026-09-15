use super::{SPATIAL_MIXED_SRID, SparkDataType, arrow_type_from_spark, parse_ddl, simple_string};

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
