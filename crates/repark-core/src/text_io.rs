use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use arrow::array::{
    Array, GenericStringArray, LargeStringArray, OffsetSizeTrait, RecordBatch, StringArray,
    StringViewArray,
};
use arrow::datatypes::DataType;
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use datafusion::prelude::DataFrame;
use futures::StreamExt;

use crate::{Error, Result, engine_err};

fn spark_text_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "VOID".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 | DataType::UInt8 => "TINYINT".to_string(),
        DataType::Int16 | DataType::UInt16 => "SMALLINT".to_string(),
        DataType::Int32 | DataType::UInt32 => "INT".to_string(),
        DataType::Int64 | DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Duration(_) | DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BINARY".to_string(),
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Dictionary(_, _)
        | DataType::RunEndEncoded(_, _) => "STRING".to_string(),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _) => "ARRAY".to_string(),
        DataType::Struct(_) => "STRUCT".to_string(),
        DataType::Map(_, _) => "MAP".to_string(),
        DataType::Union(_, _) => "UNION".to_string(),
    }
}

fn text_unsupported_column(column: &str, data_type: &DataType) -> Error {
    Error::Analysis(format!(
        "[UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE] The Text datasource doesn't support the column `{column}` of the type \"{}\". SQLSTATE: 0A000",
        spark_text_type_name(data_type)
    ))
}

fn check_text_write_schema(schema: &DFSchema) -> Result<()> {
    let mut offender: Option<(&str, &DataType)> = None;
    for field in schema.fields() {
        let string = matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );
        if !string && offender.is_none() {
            offender = Some((field.name().as_str(), field.data_type()));
        }
    }
    if let Some((name, data_type)) = offender {
        return Err(text_unsupported_column(name, data_type));
    }
    if schema.fields().len() != 1 {
        return Err(Error::Analysis(format!(
            "Text data source supports only a single column, and you have {} columns.",
            schema.fields().len()
        )));
    }
    Ok(())
}

fn write_offset_strings<O: OffsetSizeTrait>(
    writer: &mut BufWriter<File>,
    values: &GenericStringArray<O>,
    part: &Path,
    separator: &[u8],
) -> Result<()> {
    for row in 0..values.len() {
        if !values.is_null(row) {
            writer
                .write_all(values.value(row).as_bytes())
                .map_err(|error| {
                    Error::Analysis(format!("text write to {} failed: {error}", part.display()))
                })?;
        }
        writer.write_all(separator).map_err(|error| {
            Error::Analysis(format!("text write to {} failed: {error}", part.display()))
        })?;
    }
    Ok(())
}

fn write_text_batch(batch: &RecordBatch, part: &Path, separator: &[u8]) -> Result<()> {
    let file = File::create(part).map_err(|error| {
        Error::Analysis(format!(
            "text write cannot create {}: {error}",
            part.display()
        ))
    })?;
    let mut writer = BufWriter::new(file);
    let column = batch.column(0);
    match column.data_type() {
        DataType::Utf8 => {
            let values = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("text write column is not a Utf8 array".to_string())
                })
                .map_err(engine_err)?;
            write_offset_strings(&mut writer, values, part, separator)?;
        }
        DataType::LargeUtf8 => {
            let values = column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "text write column is not a LargeUtf8 array".to_string(),
                    )
                })
                .map_err(engine_err)?;
            write_offset_strings(&mut writer, values, part, separator)?;
        }
        DataType::Utf8View => {
            let values = column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "text write column is not a Utf8View array".to_string(),
                    )
                })
                .map_err(engine_err)?;
            for row in 0..values.len() {
                if !values.is_null(row) {
                    writer
                        .write_all(values.value(row).as_bytes())
                        .map_err(|error| {
                            Error::Analysis(format!(
                                "text write to {} failed: {error}",
                                part.display()
                            ))
                        })?;
                }
                writer.write_all(separator).map_err(|error| {
                    Error::Analysis(format!("text write to {} failed: {error}", part.display()))
                })?;
            }
        }
        other => {
            return Err(engine_err(DataFusionError::Execution(format!(
                "text write reached an unsupported column type: {other}"
            ))));
        }
    }
    writer.flush().map_err(|error| {
        Error::Analysis(format!("text write to {} failed: {error}", part.display()))
    })?;
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
pub async fn write_text_frame(frame: &DataFrame, dir: &Path, line_sep: &str) -> Result<()> {
    check_text_write_schema(frame.schema())?;
    if line_sep.is_empty() {
        return Err(Error::Analysis(
            "requirement failed: 'lineSep' cannot be an empty string.".to_string(),
        ));
    }
    std::fs::create_dir_all(dir).map_err(|error| {
        Error::Analysis(format!(
            "text write cannot create directory {}: {error}",
            dir.display()
        ))
    })?;
    let separator = line_sep.as_bytes();
    let mut stream = frame.clone().execute_stream().await.map_err(engine_err)?;
    let mut part_index = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(engine_err)?;
        if batch.num_rows() == 0 {
            continue;
        }
        let part = dir.join(format!("part-{part_index:05}.txt"));
        part_index += 1;
        write_text_batch(&batch, &part, separator)?;
    }
    if part_index == 0 {
        let part = dir.join("part-00000.txt");
        File::create(&part).map_err(|error| {
            Error::Analysis(format!(
                "text write cannot create {}: {error}",
                part.display()
            ))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn test_session() -> crate::ReparkSession {
        crate::ReparkSession::builder().build().unwrap()
    }

    async fn text_rows(frame: &DataFrame) -> Vec<Option<String>> {
        let batches = frame.clone().collect().await.unwrap();
        let mut rows: Vec<Option<String>> = Vec::new();
        for batch in &batches {
            let column = batch.column(0);
            let values = column.as_any().downcast_ref::<StringArray>().unwrap();
            for row in 0..batch.num_rows() {
                rows.push(if values.is_null(row) {
                    None
                } else {
                    Some(values.value(row).to_string())
                });
            }
        }
        rows
    }

    #[tokio::test]
    async fn text_write_names_first_non_string_column() {
        let session = test_session();
        let frame = session
            .sql("SELECT 'x' AS key, CAST(1 AS INT) AS a, 2 AS b")
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        let Err(error) = write_text_frame(&frame, &target, "\n").await else {
            panic!("a multi-column frame must refuse the text write");
        };
        assert_eq!(
            error.to_string(),
            "[UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE] The Text datasource doesn't support the column `a` of the type \"INT\". SQLSTATE: 0A000"
        );
        assert!(!target.exists());
    }

    #[tokio::test]
    async fn text_write_two_string_columns_match_spark() {
        let session = test_session();
        let frame = session
            .sql("SELECT 'x' AS value, 'y' AS extra")
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        let Err(error) = write_text_frame(&frame, &target, "\n").await else {
            panic!("a two-string frame must refuse the text write");
        };
        assert_eq!(
            error.to_string(),
            "Text data source supports only a single column, and you have 2 columns."
        );
        assert!(!target.exists());
    }

    #[tokio::test]
    async fn text_write_empty_line_sep_refuses() {
        let session = test_session();
        let frame = session.sql("SELECT 'a' AS value").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        let Err(error) = write_text_frame(&frame, &target, "").await else {
            panic!("an empty lineSep must refuse the text write");
        };
        assert_eq!(
            error.to_string(),
            "requirement failed: 'lineSep' cannot be an empty string."
        );
        assert!(!target.exists());
    }

    #[tokio::test]
    async fn text_write_round_trip_keeps_null_empty() {
        let session = test_session();
        let frame = session
            .sql("SELECT * FROM (VALUES ('a'), (NULL)) AS t(value)")
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        write_text_frame(&frame, &target, "\n").await.unwrap();
        let part = target.join("part-00000.txt");
        assert_eq!(std::fs::read_to_string(&part).unwrap(), "a\n\n");
        let back = session
            .read_text(target.to_str().unwrap(), false, None)
            .await
            .unwrap();
        let rows = text_rows(&back).await;
        assert_eq!(rows, vec![Some("a".to_string()), Some(String::new())]);
    }

    #[tokio::test]
    async fn text_write_streams_batches_to_sequential_parts() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        std::fs::create_dir_all(&target).unwrap();
        write_text_batch(&batch, &target.join("part-00000.txt"), b"\n").unwrap();
        write_text_batch(&batch, &target.join("part-00001.txt"), b";").unwrap();
        assert_eq!(
            std::fs::read_to_string(target.join("part-00000.txt")).unwrap(),
            "a\nb\n"
        );
        assert_eq!(
            std::fs::read_to_string(target.join("part-00001.txt")).unwrap(),
            "a;b;"
        );
    }
}
