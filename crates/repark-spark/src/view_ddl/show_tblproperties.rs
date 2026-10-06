use iceberg::TableIdent;
use iceberg::table::Table;

const HIDDEN_STORED_PROPERTIES: [&str; 2] = ["comment", "owner"];

const COMPUTED_PROPERTIES: [&str; 3] = ["current-snapshot-id", "format", "format-version"];

const COMPRESSION_CODEC_PROPERTY: &str = "write.parquet.compression-codec";

const SPARK_DEFAULT_COMPRESSION_CODEC: &str = "zstd";

const WRITE_FORMAT_DEFAULT_PROPERTY: &str = "write.format.default";

const DEFAULT_FILE_FORMAT: &str = "parquet";

pub(crate) fn table_rows(
    table: &Table,
    catalog: &str,
    ident: &TableIdent,
    key: Option<&str>,
) -> Vec<(String, String)> {
    let metadata = table.metadata();
    let stored = metadata.properties();
    let file_format = stored
        .get(WRITE_FORMAT_DEFAULT_PROPERTY)
        .map_or(DEFAULT_FILE_FORMAT, String::as_str);
    let snapshot = metadata
        .current_snapshot_id()
        .map_or_else(|| "none".to_string(), |id| id.to_string());
    let mut rows = vec![
        ("current-snapshot-id".to_string(), snapshot),
        ("format".to_string(), format!("iceberg/{file_format}")),
        (
            "format-version".to_string(),
            crate::table_props_view::format_version_number(metadata.format_version()).to_string(),
        ),
    ];
    let mut stored_rows = stored
        .iter()
        .filter(|(name, _)| !is_hidden(name))
        .map(|(name, value)| {
            (
                name.clone(),
                crate::table_props_view::displayed_property_value(name, value),
            )
        })
        .collect::<Vec<_>>();
    stored_rows.sort();
    rows.extend(stored_rows);
    if !rows
        .iter()
        .any(|(name, _)| name == COMPRESSION_CODEC_PROPERTY)
    {
        rows.push((
            COMPRESSION_CODEC_PROPERTY.to_string(),
            SPARK_DEFAULT_COMPRESSION_CODEC.to_string(),
        ));
    }
    rows.sort();
    match key {
        Some(key) => vec![(key.to_string(), lookup_value(&rows, catalog, ident, key))],
        None => rows,
    }
}

fn lookup_value(rows: &[(String, String)], catalog: &str, ident: &TableIdent, key: &str) -> String {
    rows.iter().find(|(name, _)| name == key).map_or_else(
        || {
            format!(
                "Table {catalog}.{namespace}.{name} does not have property: {key}",
                namespace = ident.namespace(),
                name = ident.name(),
            )
        },
        |(_, value)| value.clone(),
    )
}

fn is_hidden(name: &str) -> bool {
    HIDDEN_STORED_PROPERTIES.contains(&name) || COMPUTED_PROPERTIES.contains(&name)
}
