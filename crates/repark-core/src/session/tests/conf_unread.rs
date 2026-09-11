use super::super::*;

#[tokio::test]
async fn conf_unread_coalesce_batches_build_refuses_naming_key() {
    let error = ReparkSession::builder()
        .config("datafusion.execution.coalesce_batches", "false")
        .build()
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("datafusion.execution.coalesce_batches"),
        "the refusal must name the key, got: {message}"
    );
    assert!(
        message.contains("cannot take effect"),
        "the refusal must state why the value is refused, got: {message}"
    );
}

#[tokio::test]
async fn conf_unread_coalesce_batches_runtime_set_refuses() {
    let session = ReparkSession::builder().build().unwrap();
    let error = session
        .sql("SET datafusion.execution.coalesce_batches = false")
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("datafusion.execution.coalesce_batches"),
        "the runtime refusal must name the key, got: {message}"
    );
}

#[tokio::test]
async fn conf_unread_enable_page_index_reaches_scan_source_options() {
    let default = ReparkSession::builder().build().unwrap();
    assert!(
        default
            .context()
            .copied_table_options()
            .parquet
            .global
            .enable_page_index,
        "the control session carries the DataFusion default"
    );
    let session = ReparkSession::builder()
        .config("datafusion.execution.parquet.enable_page_index", "false")
        .build()
        .unwrap();
    assert!(
        !session
            .context()
            .copied_config()
            .options()
            .execution
            .parquet
            .enable_page_index,
        "the builder value must reach SessionConfig"
    );
    assert!(
        !session
            .context()
            .copied_table_options()
            .parquet
            .global
            .enable_page_index,
        "the builder value must reach the table options the scan source reads"
    );
}

#[tokio::test]
async fn conf_unread_bloom_filter_on_read_reaches_scan_source_options() {
    let default = ReparkSession::builder().build().unwrap();
    assert!(
        default
            .context()
            .copied_table_options()
            .parquet
            .global
            .bloom_filter_on_read,
        "the control session carries the DataFusion default"
    );
    let session = ReparkSession::builder()
        .config("datafusion.execution.parquet.bloom_filter_on_read", "false")
        .build()
        .unwrap();
    assert!(
        !session
            .context()
            .copied_config()
            .options()
            .execution
            .parquet
            .bloom_filter_on_read,
        "the builder value must reach SessionConfig"
    );
    assert!(
        !session
            .context()
            .copied_table_options()
            .parquet
            .global
            .bloom_filter_on_read,
        "the builder value must reach the table options the scan source reads"
    );
}

#[tokio::test]
async fn conf_unread_write_batch_size_reaches_writer_options() {
    let default = ReparkSession::builder().build().unwrap();
    assert_eq!(
        default
            .context()
            .copied_table_options()
            .parquet
            .global
            .write_batch_size,
        1024,
        "the control session carries the DataFusion default"
    );
    let session = ReparkSession::builder()
        .config("datafusion.execution.parquet.write_batch_size", "1000")
        .build()
        .unwrap();
    assert_eq!(
        session
            .context()
            .copied_config()
            .options()
            .execution
            .parquet
            .write_batch_size,
        1000,
        "the builder value must reach SessionConfig"
    );
    assert_eq!(
        session
            .context()
            .copied_table_options()
            .parquet
            .global
            .write_batch_size,
        1000,
        "the builder value must reach the table options the writer reads"
    );
}
