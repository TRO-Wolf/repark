//! Parquet [`WriterProperties`] from Iceberg table properties.

use datafusion::error::{DataFusionError, Result};
use iceberg::table::Table;
use iceberg::writer::base_writer::position_delete_writer::position_delete_writer_properties;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// Iceberg table property for the Parquet compression codec (Java / Spark key).
pub const COMPRESSION_CODEC_PROP: &str = "write.parquet.compression-codec";

/// Optional Iceberg table property for codec level (`gzip` / `zstd` only).
pub const COMPRESSION_LEVEL_PROP: &str = "write.parquet.compression-level";

/// Accepted codec spellings (case-insensitive), shown in loud-error messages.
pub const ACCEPTED_CODECS: &str = "zstd, snappy, gzip, lz4, uncompressed";

/// Build [`WriterProperties`] for `table` from `write.parquet.compression-codec` (+ level).
/// # Errors
/// Unknown codec, unparsable level, or level out of range for gzip/zstd.
pub fn writer_properties_for(table: &Table) -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(compression_for(table)?)
        .build())
}

pub(crate) fn position_delete_writer_properties_for(table: &Table) -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(compression_for(table)?)
        .set_statistics_truncate_length(
            position_delete_writer_properties().statistics_truncate_length(),
        )
        .build())
}

fn compression_for(table: &Table) -> Result<Compression> {
    let properties = table.metadata().properties();
    let codec_raw = properties.get(COMPRESSION_CODEC_PROP).map(String::as_str);
    let level_raw = properties.get(COMPRESSION_LEVEL_PROP).map(String::as_str);
    parse_compression(codec_raw, level_raw)
}

/// Parse codec (+ optional level) into parquet-rs [`Compression`].
/// # Errors
/// Unknown codec name, non-integer level, or level outside the codec's accepted range.
pub fn parse_compression(codec_raw: Option<&str>, level_raw: Option<&str>) -> Result<Compression> {
    let codec_name = codec_raw.unwrap_or("zstd").trim();
    let normalized = codec_name.to_ascii_lowercase();
    match normalized.as_str() {
        "zstd" => Ok(Compression::ZSTD(zstd_level(level_raw)?)),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(gzip_level(level_raw)?)),
        // Modern Parquet "lz4" is LZ4_RAW (PARQUET-2032 deprecates the non-standard LZ4 block).
        "lz4" | "lz4_raw" => Ok(Compression::LZ4_RAW),
        "uncompressed" => Ok(Compression::UNCOMPRESSED),
        _ => Err(DataFusionError::Plan(format!(
            "table property `{COMPRESSION_CODEC_PROP}` has unknown value {codec_name:?}; \
             accepted codecs (case-insensitive): {ACCEPTED_CODECS}"
        ))),
    }
}

fn zstd_level(level_raw: Option<&str>) -> Result<ZstdLevel> {
    match level_raw {
        None => Ok(ZstdLevel::default()),
        Some(raw) => {
            let level: i32 = raw.trim().parse().map_err(|_| {
                DataFusionError::Plan(format!(
                    "table property `{COMPRESSION_LEVEL_PROP}` must be an integer for zstd \
                     (got {raw:?})"
                ))
            })?;
            ZstdLevel::try_new(level).map_err(|error| {
                DataFusionError::Plan(format!(
                    "table property `{COMPRESSION_LEVEL_PROP}` is not a valid zstd level \
                     ({level}): {error}"
                ))
            })
        }
    }
}

fn gzip_level(level_raw: Option<&str>) -> Result<GzipLevel> {
    match level_raw {
        None => Ok(GzipLevel::default()),
        Some(raw) => {
            let level: u32 = raw.trim().parse().map_err(|_| {
                DataFusionError::Plan(format!(
                    "table property `{COMPRESSION_LEVEL_PROP}` must be a non-negative integer \
                     for gzip (got {raw:?})"
                ))
            })?;
            GzipLevel::try_new(level).map_err(|error| {
                DataFusionError::Plan(format!(
                    "table property `{COMPRESSION_LEVEL_PROP}` is not a valid gzip level \
                     ({level}): {error}"
                ))
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Compression;
    use tempfile::TempDir;

    use crate::write::append::append;
    use crate::write::concurrency::WriteConcurrency;
    use crate::write::merge::write_data_files;

    // === Pure parse pins ===

    #[test]
    fn absent_codec_defaults_to_zstd() {
        let compression = parse_compression(None, None).expect("default");
        assert!(
            matches!(compression, Compression::ZSTD(_)),
            "Java Iceberg default is zstd; got {compression:?}"
        );
    }

    #[test]
    fn uncompressed_escape_hatch() {
        let compression = parse_compression(Some("uncompressed"), None).expect("parse");
        assert_eq!(compression, Compression::UNCOMPRESSED);
        let upper = parse_compression(Some("UNCOMPRESSED"), None).expect("case");
        assert_eq!(upper, Compression::UNCOMPRESSED);
    }

    #[test]
    fn accepted_codecs_case_insensitive() {
        assert!(matches!(
            parse_compression(Some("ZSTD"), None).unwrap(),
            Compression::ZSTD(_)
        ));
        assert_eq!(
            parse_compression(Some("Snappy"), None).unwrap(),
            Compression::SNAPPY
        );
        assert!(matches!(
            parse_compression(Some("GZIP"), None).unwrap(),
            Compression::GZIP(_)
        ));
        assert_eq!(
            parse_compression(Some("lz4"), None).unwrap(),
            Compression::LZ4_RAW
        );
    }

    #[test]
    fn unknown_codec_is_loud_with_property_and_accepted_set() {
        let error = parse_compression(Some("brotli"), None).expect_err("unknown");
        let message = error.to_string();
        assert!(
            message.contains(COMPRESSION_CODEC_PROP),
            "must name the property: {message}"
        );
        assert!(
            message.contains("brotli") && message.contains("zstd"),
            "must name the bad value and accepted set: {message}"
        );
    }

    #[test]
    fn zstd_level_is_honored_when_present() {
        let compression = parse_compression(Some("zstd"), Some("3")).expect("level 3");
        match compression {
            Compression::ZSTD(level) => assert_eq!(level.compression_level(), 3),
            other => panic!("expected ZSTD, got {other:?}"),
        }
    }

    #[test]
    fn bad_level_is_loud() {
        let error = parse_compression(Some("zstd"), Some("not-a-number")).expect_err("level");
        assert!(
            error.to_string().contains(COMPRESSION_LEVEL_PROP),
            "{}",
            error
        );
    }

    async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
        let path = warehouse
            .path()
            .to_str()
            .expect("utf-8 warehouse path")
            .to_string();
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
                )
                .await
                .expect("build memory catalog"),
        );
        catalog
            .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
            .await
            .expect("create namespace");
        catalog
    }

    fn numeric_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "v", Type::Primitive(PrimitiveType::Double)).into(),
            ])
            .build()
            .expect("schema")
    }

    async fn create_table(
        catalog: &Arc<dyn Catalog>,
        name: &str,
        properties: HashMap<String, String>,
    ) -> TableIdent {
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(numeric_schema())
            .properties(properties)
            .build();
        catalog
            .create_table(&NamespaceIdent::new("sales".to_string()), creation)
            .await
            .expect("create table");
        TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string())
    }

    /// Synthetic numeric frame: highly compressible (repeated floats) so zstd wins handily.
    fn numeric_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let row_count = i64::try_from(rows).expect("test row count fits i64");
        let ids: Vec<i64> = (0..row_count).collect();
        let values: Vec<f64> = (0..row_count)
            .map(|index| f64::from(u32::try_from(index % 100).expect("mod 100 fits u32")))
            .collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .expect("batch")
    }

    /// Read the first column-chunk compression from a Parquet file via the table `FileIO`.
    async fn footer_compression(
        catalog: &Arc<dyn Catalog>,
        ident: &TableIdent,
        path: &str,
    ) -> Compression {
        let table = catalog.load_table(ident).await.expect("load");
        let bytes = table
            .file_io()
            .new_input(path)
            .expect("open")
            .read()
            .await
            .expect("read");
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).expect("parquet");
        builder.metadata().row_group(0).column(0).compression()
    }

    async fn live_data_files(
        catalog: &Arc<dyn Catalog>,
        ident: &TableIdent,
    ) -> Vec<iceberg::spec::DataFile> {
        use iceberg::spec::{DataContentType, ManifestContentType};
        let table = catalog.load_table(ident).await.expect("load");
        let metadata = table.metadata();
        let snapshot = metadata.current_snapshot().expect("snapshot");
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await
            .expect("manifest list");
        let mut files = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .expect("manifest");
            for entry in manifest.entries() {
                if entry.is_alive() && entry.data_file().content_type() == DataContentType::Data {
                    files.push(entry.data_file().clone());
                }
            }
        }
        files
    }

    #[tokio::test]
    async fn default_table_writes_zstd_footer_on_append() {
        let warehouse = TempDir::new().expect("tmp");
        let catalog = memory_catalog(&warehouse).await;
        // No compression property → engine default zstd.
        let ident = create_table(&catalog, "t_default", HashMap::new()).await;
        append(&catalog, &ident, vec![numeric_batch(200)])
            .await
            .expect("append");
        let files = live_data_files(&catalog, &ident).await;
        assert!(!files.is_empty());
        let compression = footer_compression(&catalog, &ident, files[0].file_path()).await;
        assert!(
            matches!(compression, Compression::ZSTD(_)),
            "default (absent property) must write zstd; got {compression:?}"
        );
    }

    #[tokio::test]
    async fn uncompressed_property_writes_uncompressed_footer() {
        let warehouse = TempDir::new().expect("tmp");
        let catalog = memory_catalog(&warehouse).await;
        let props = HashMap::from([(
            COMPRESSION_CODEC_PROP.to_string(),
            "uncompressed".to_string(),
        )]);
        let ident = create_table(&catalog, "t_raw", props).await;
        append(&catalog, &ident, vec![numeric_batch(200)])
            .await
            .expect("append");
        let files = live_data_files(&catalog, &ident).await;
        let compression = footer_compression(&catalog, &ident, files[0].file_path()).await;
        assert_eq!(compression, Compression::UNCOMPRESSED);
    }

    #[tokio::test]
    async fn zstd_file_smaller_than_half_uncompressed_on_synthetic_numeric() {
        let warehouse = TempDir::new().expect("tmp");
        let catalog = memory_catalog(&warehouse).await;
        let rows = 50_000;

        let zstd_ident = create_table(
            &catalog,
            "t_zstd",
            HashMap::from([(COMPRESSION_CODEC_PROP.to_string(), "zstd".to_string())]),
        )
        .await;
        append(&catalog, &zstd_ident, vec![numeric_batch(rows)])
            .await
            .expect("zstd append");
        let zstd_files = live_data_files(&catalog, &zstd_ident).await;
        let zstd_bytes: u64 = zstd_files
            .iter()
            .map(iceberg::spec::DataFile::file_size_in_bytes)
            .sum();

        let raw_ident = create_table(
            &catalog,
            "t_unc",
            HashMap::from([(
                COMPRESSION_CODEC_PROP.to_string(),
                "uncompressed".to_string(),
            )]),
        )
        .await;
        append(&catalog, &raw_ident, vec![numeric_batch(rows)])
            .await
            .expect("raw append");
        let raw_files = live_data_files(&catalog, &raw_ident).await;
        let raw_bytes: u64 = raw_files
            .iter()
            .map(iceberg::spec::DataFile::file_size_in_bytes)
            .sum();

        assert!(
            zstd_bytes > 0 && raw_bytes > 0,
            "both codecs must produce files (zstd={zstd_bytes}, raw={raw_bytes})"
        );
        // Integer form of "zstd < 0.5 × raw" avoids f64 precision-loss lints on large sizes.
        assert!(
            zstd_bytes.saturating_mul(2) < raw_bytes,
            "zstd must be < 0.5× uncompressed on this compressible frame \
             (zstd={zstd_bytes}, raw={raw_bytes})"
        );
    }

    #[tokio::test]
    async fn zstd_and_uncompressed_round_trip_equal_values_and_schema() {
        use datafusion::arrow::array::Array;
        use futures::TryStreamExt;

        let warehouse = TempDir::new().expect("tmp");
        let catalog = memory_catalog(&warehouse).await;
        let batch = numeric_batch(1_000);

        for (name, codec) in [("rt_zstd", "zstd"), ("rt_raw", "uncompressed")] {
            let ident = create_table(
                &catalog,
                name,
                HashMap::from([(COMPRESSION_CODEC_PROP.to_string(), codec.to_string())]),
            )
            .await;
            append(&catalog, &ident, vec![batch.clone()])
                .await
                .expect("append");
            let table = catalog.load_table(&ident).await.expect("load");
            let scan = table.scan().select(["id", "v"]).build().expect("scan");
            let read: Vec<RecordBatch> = scan
                .to_arrow()
                .await
                .expect("to_arrow")
                .try_collect()
                .await
                .expect("collect");
            assert_eq!(read.len(), 1, "one batch for {codec}");
            assert_eq!(read[0].num_rows(), batch.num_rows());
            assert_eq!(
                read[0].column(0).data_type(),
                &DataType::Int64,
                "id type under {codec}"
            );
            assert_eq!(
                read[0].column(1).data_type(),
                &DataType::Float64,
                "v type under {codec}"
            );
            for column in 0..batch.num_columns() {
                assert_eq!(
                    read[0].column(column).as_ref(),
                    batch.column(column).as_ref(),
                    "column {column} values must match under {codec}"
                );
            }
        }
    }

    #[tokio::test]
    async fn merge_data_file_path_carries_codec_in_footer() {
        let warehouse = TempDir::new().expect("tmp");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(
            &catalog,
            "t_merge_data",
            HashMap::from([(COMPRESSION_CODEC_PROP.to_string(), "snappy".to_string())]),
        )
        .await;
        let table = catalog.load_table(&ident).await.expect("load");
        // `write_data_files` is the MERGE/CTAS unpartitioned writer construction path.
        let files = write_data_files(&table, vec![numeric_batch(100)])
            .await
            .expect("write_data_files");
        assert!(!files.is_empty());
        let compression = footer_compression(&catalog, &ident, files[0].file_path()).await;
        assert_eq!(
            compression,
            Compression::SNAPPY,
            "MERGE data-file path must honor table codec"
        );
    }

    #[tokio::test]
    async fn position_delete_file_carries_codec_in_footer() {
        let warehouse = TempDir::new().expect("tmp");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(
            &catalog,
            "t_posdel",
            HashMap::from([(COMPRESSION_CODEC_PROP.to_string(), "gzip".to_string())]),
        )
        .await;
        // Need a live data file so the pos-delete writer can resolve partitions.
        append(&catalog, &ident, vec![numeric_batch(10)])
            .await
            .expect("seed");
        let data_path: std::sync::Arc<str> = {
            let files = live_data_files(&catalog, &ident).await;
            std::sync::Arc::from(files[0].file_path())
        };
        let pairs = vec![
            (std::sync::Arc::clone(&data_path), 0_i64),
            (data_path, 1_i64),
        ];
        let table = catalog.load_table(&ident).await.expect("reload");
        let written = crate::write::position_delete::write_position_deletes(
            &table,
            &pairs,
            WriteConcurrency::new(1).expect("K=1"),
        )
        .await
        .expect("pos deletes");
        assert!(!written.is_empty());
        let compression = footer_compression(&catalog, &ident, written[0].file_path()).await;
        assert!(
            matches!(compression, Compression::GZIP(_)),
            "position-delete files must use the same table codec; got {compression:?}"
        );
    }

    #[tokio::test]
    async fn unknown_codec_on_table_fails_loud_at_write() {
        let warehouse = TempDir::new().expect("tmp");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(
            &catalog,
            "t_bad",
            HashMap::from([(COMPRESSION_CODEC_PROP.to_string(), "brotli".to_string())]),
        )
        .await;
        let error = append(&catalog, &ident, vec![numeric_batch(10)])
            .await
            .expect_err("unknown codec must fail at write");
        let message = error.to_string();
        assert!(
            message.contains(COMPRESSION_CODEC_PROP) && message.contains("brotli"),
            "loud error must name property and value: {message}"
        );
    }
}
