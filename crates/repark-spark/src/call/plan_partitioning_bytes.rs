use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::FOOTER_SIZE;
use datafusion::parquet::file::metadata::ParquetMetaDataReader;
use iceberg::io::FileIO;

use super::plan_partitioning_score::FALLBACK_BYTE_RATIO;

pub(super) struct ByteRatio {
    pub(super) value: f64,
    pub(super) source: &'static str,
}

fn fallback() -> ByteRatio {
    ByteRatio {
        value: FALLBACK_BYTE_RATIO,
        source: "fallback",
    }
}

async fn column_chunk_sums(file_io: &FileIO, path: &str) -> Option<(u64, u64)> {
    let input = file_io.new_input(path).ok()?;
    let size = input.metadata().await.ok()?.size;
    let reader = input.reader().await.ok()?;
    let mut tail = reader
        .read(size.saturating_sub(FOOTER_SIZE as u64)..size)
        .await
        .ok()?;
    let mut parser = ParquetMetaDataReader::new();
    loop {
        match parser.try_parse_sized(&tail, size) {
            Ok(()) => break,
            Err(ParquetError::NeedMoreData(needed)) => {
                let needed = u64::try_from(needed).ok()?;
                if needed > size || needed <= tail.len() as u64 {
                    return None;
                }
                tail = reader.read(size - needed..size).await.ok()?;
            }
            Err(_) => return None,
        }
    }
    let metadata = parser.finish().ok()?;
    let mut compressed = 0_u64;
    let mut uncompressed = 0_u64;
    for group in metadata.row_groups() {
        for column in group.columns() {
            compressed = compressed.checked_add(u64::try_from(column.compressed_size()).ok()?)?;
            uncompressed =
                uncompressed.checked_add(u64::try_from(column.uncompressed_size()).ok()?)?;
        }
    }
    Some((compressed, uncompressed))
}

#[allow(clippy::cast_precision_loss)]
pub(super) async fn byte_ratio(file_io: &FileIO, paths: &[String]) -> ByteRatio {
    let mut compressed = 0_u64;
    let mut uncompressed = 0_u64;
    for path in paths {
        let Some(pair) = column_chunk_sums(file_io, path).await else {
            return fallback();
        };
        compressed += pair.0;
        uncompressed += pair.1;
    }
    if uncompressed == 0 {
        return fallback();
    }
    ByteRatio {
        value: compressed as f64 / uncompressed as f64,
        source: "footers",
    }
}
