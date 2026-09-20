use super::common::*;

fn fs_path(file_path: &str) -> String {
    for prefix in ["file://", "file:"] {
        if let Some(rest) = file_path.strip_prefix(prefix) {
            return if rest.starts_with('/') {
                rest.to_string()
            } else {
                format!("/{rest}")
            };
        }
    }
    file_path.to_string()
}

fn dictionary_pages(paths: &std::collections::HashSet<String>) -> (usize, usize) {
    use datafusion::parquet::file::metadata::ParquetMetaDataReader;
    let mut with_dictionary = 0;
    let mut columns = 0;
    for path in paths {
        let file = std::fs::File::open(fs_path(path)).expect("data file opens");
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&file)
            .expect("footer parses");
        for group in metadata.row_groups() {
            for column in group.columns() {
                columns += 1;
                if column.dictionary_page_offset().is_some() {
                    with_dictionary += 1;
                }
            }
        }
    }
    (with_dictionary, columns)
}

async fn seeded_dictionary_pages(table_name: &str, properties: &str) -> (usize, usize) {
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table_name} USING iceberg{properties} AS SELECT * FROM src"
        ),
    )
    .await;
    dictionary_pages(&live_data_file_paths(&catalogs, table_name).await)
}

#[tokio::test]
async fn a_table_without_the_dictionary_property_writes_dictionary_pages() {
    let _: &str = "pins: ice-session-write-conf-1/C-064";
    let (with_dictionary, columns) = seeded_dictionary_pages("dictdefault", "").await;
    assert!(
        columns > 0,
        "the append must write at least one column chunk"
    );
    assert_eq!(
        with_dictionary, columns,
        "parquet-mr's `parquet.enable.dictionary` default is true and Iceberg passes it \
         through, so a table that never names the property is dictionary-encoded"
    );
}

#[tokio::test]
async fn parquet_enable_dictionary_false_turns_the_dictionary_pages_off() {
    let _: &str = "pins: ice-session-write-conf-1/C-064";
    let (with_dictionary, columns) = seeded_dictionary_pages(
        "dictoff",
        " TBLPROPERTIES ('parquet.enable.dictionary' = 'false')",
    )
    .await;
    assert!(
        columns > 0,
        "the append must write at least one column chunk"
    );
    assert_eq!(
        with_dictionary, 0,
        "`parquet.enable.dictionary` = false turns every dictionary page off"
    );
}

#[tokio::test]
async fn parquet_enable_dictionary_true_keeps_the_dictionary_pages_on() {
    let _: &str = "pins: ice-session-write-conf-1/C-064";
    let (with_dictionary, columns) = seeded_dictionary_pages(
        "dicton",
        " TBLPROPERTIES ('parquet.enable.dictionary' = 'TRUE')",
    )
    .await;
    assert_eq!(
        with_dictionary, columns,
        "an explicit true is read case-insensitively, as Java reads it"
    );
}

async fn inserted_dictionary_pages(table_name: &str, conf: Option<(&str, &str)>) -> (usize, usize) {
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!("CREATE TABLE ice.sales.{table_name} (id BIGINT, data STRING) USING iceberg"),
    )
    .await;
    if let Some((key, value)) = conf {
        super::session_write_conf::set_session_conf(&ctx, key, value);
    }
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.{table_name} VALUES (1,'a'),(2,'b'),(3,'a')"),
    )
    .await;
    if let Some((key, _)) = conf {
        super::session_write_conf::unset_session_conf(&ctx, key);
    }
    dictionary_pages(&live_data_file_paths(&catalogs, table_name).await)
}

#[tokio::test]
async fn the_owned_plain_insert_takes_the_fork_insert_exec_dictionary_rule() {
    let _: &str = "pins: ice-session-write-conf-1/C-064";
    let unowned = inserted_dictionary_pages("dictfork", None).await;
    let owned = inserted_dictionary_pages(
        "dictowned",
        Some(("spark.sql.iceberg.snapshot-property.team", "a")),
    )
    .await;
    assert_eq!(
        unowned.0, 0,
        "the fork's insert exec reads `parquet.enable.dictionary` as absent = off"
    );
    assert_eq!(
        owned, unowned,
        "a session write conf picks the owned route, and the owned route writes the \
         fork's layout: no session conf decides the bytes"
    );
}
