use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use chrono::NaiveDate;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{Column, DataFusionError, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::DataFrame;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use repark_common::{Error, Result};

use crate::text_partition::{HIVE_DEFAULT_PARTITION, escape_partition_value};

const HIVE_PARTITION_VALUE: &str = "repark_hive_partition_value";
const EPOCH_DAYS_FROM_CE: i32 = 719_163;
const MILLIS_PER_DAY: i64 = 86_400_000;

#[derive(Debug)]
struct HivePartitionValue {
    signature: Signature,
}

impl PartialEq for HivePartitionValue {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for HivePartitionValue {}

impl Hash for HivePartitionValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        HIVE_PARTITION_VALUE.hash(state);
    }
}

impl ScalarUDFImpl for HivePartitionValue {
    fn name(&self) -> &str {
        HIVE_PARTITION_VALUE
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!(
                "'{HIVE_PARTITION_VALUE}' requires 1 argument, got {}",
                args.args.len()
            );
        };
        let array = value.to_array(args.number_rows)?;
        Ok(ColumnarValue::Array(Arc::new(hive_partition_texts(
            &array,
        )?)))
    }
}

fn renders_as_hive_text(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Boolean
        | DataType::Date32
        | DataType::Date64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64 => true,
        DataType::Dictionary(_, value) => value.as_ref() == &DataType::Utf8,
        _ => false,
    }
}

fn or_default<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(
        || HIVE_DEFAULT_PARTITION.to_string(),
        |held| held.to_string(),
    )
}

fn text_or_default(value: Option<&str>) -> String {
    match value {
        Some(text) if !text.is_empty() => text.to_string(),
        _ => HIVE_DEFAULT_PARTITION.to_string(),
    }
}

fn date_text(days: i32) -> datafusion::common::Result<String> {
    EPOCH_DAYS_FROM_CE
        .checked_add(days)
        .and_then(NaiveDate::from_num_days_from_ce_opt)
        .map(|date| date.format("%Y-%m-%d").to_string())
        .ok_or_else(|| {
            DataFusionError::Execution(format!("partition date out of range: {days} days"))
        })
}

fn date64_text(millis: i64) -> datafusion::common::Result<String> {
    let days = i32::try_from(millis / MILLIS_PER_DAY).map_err(|_| {
        DataFusionError::Execution(format!("partition date out of range: {millis} millis"))
    })?;
    date_text(days)
}

fn wrong_array(data_type: &DataType) -> DataFusionError {
    DataFusionError::Internal(format!(
        "'{HIVE_PARTITION_VALUE}' got an array that is not {data_type}"
    ))
}

macro_rules! primitive_texts {
    ($array:expr, $kind:ty) => {
        $array
            .as_primitive_opt::<$kind>()
            .map(|values| values.iter().map(or_default).collect::<Vec<String>>())
            .ok_or_else(|| wrong_array($array.data_type()))?
    };
}

fn string_texts(array: &ArrayRef) -> datafusion::common::Result<Vec<String>> {
    let texts = match array.data_type() {
        DataType::Utf8 => array
            .as_string_opt::<i32>()
            .map(|values| values.iter().map(text_or_default).collect()),
        DataType::LargeUtf8 => array
            .as_string_opt::<i64>()
            .map(|values| values.iter().map(text_or_default).collect()),
        DataType::Utf8View => array
            .as_string_view_opt()
            .map(|values| values.iter().map(text_or_default).collect()),
        DataType::Dictionary(_, _) => {
            let plain = cast(array.as_ref(), &DataType::Utf8)?;
            return string_texts(&plain);
        }
        _ => None,
    };
    texts.ok_or_else(|| wrong_array(array.data_type()))
}

fn hive_partition_texts(array: &ArrayRef) -> datafusion::common::Result<StringArray> {
    let texts: Vec<String> = match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Dictionary(_, _) => {
            string_texts(array)?
        }
        DataType::Boolean => array
            .as_boolean_opt()
            .map(|values| values.iter().map(or_default).collect())
            .ok_or_else(|| wrong_array(array.data_type()))?,
        DataType::Date32 => array
            .as_primitive_opt::<Date32Type>()
            .ok_or_else(|| wrong_array(array.data_type()))?
            .iter()
            .map(|value| value.map_or_else(|| Ok(HIVE_DEFAULT_PARTITION.to_string()), date_text))
            .collect::<datafusion::common::Result<Vec<String>>>()?,
        DataType::Date64 => array
            .as_primitive_opt::<Date64Type>()
            .ok_or_else(|| wrong_array(array.data_type()))?
            .iter()
            .map(|value| value.map_or_else(|| Ok(HIVE_DEFAULT_PARTITION.to_string()), date64_text))
            .collect::<datafusion::common::Result<Vec<String>>>()?,
        DataType::Int8 => primitive_texts!(array, Int8Type),
        DataType::Int16 => primitive_texts!(array, Int16Type),
        DataType::Int32 => primitive_texts!(array, Int32Type),
        DataType::Int64 => primitive_texts!(array, Int64Type),
        DataType::UInt8 => primitive_texts!(array, UInt8Type),
        DataType::UInt16 => primitive_texts!(array, UInt16Type),
        DataType::UInt32 => primitive_texts!(array, UInt32Type),
        DataType::UInt64 => primitive_texts!(array, UInt64Type),
        DataType::Float16 => primitive_texts!(array, Float16Type),
        DataType::Float32 => primitive_texts!(array, Float32Type),
        DataType::Float64 => primitive_texts!(array, Float64Type),
        other => {
            return exec_err!("'{HIVE_PARTITION_VALUE}' cannot name a {other} partition");
        }
    };
    Ok(StringArray::from_iter_values(texts))
}

#[allow(clippy::missing_errors_doc)]
pub fn hive_partition_frame(
    frame: DataFrame,
    partitions: &[String],
) -> datafusion::common::Result<DataFrame> {
    let wanted: HashSet<&str> = partitions.iter().map(String::as_str).collect();
    let schema = frame.schema().clone();
    if !schema.fields().iter().any(|field| {
        wanted.contains(field.name().as_str()) && renders_as_hive_text(field.data_type())
    }) {
        return Ok(frame);
    }
    let udf = Arc::new(ScalarUDF::from(HivePartitionValue {
        signature: Signature::any(1, Volatility::Immutable),
    }));
    let projection = schema
        .iter()
        .map(|(qualifier, field)| {
            let column = Expr::Column(Column::new(qualifier.cloned(), field.name()));
            if wanted.contains(field.name().as_str()) && renders_as_hive_text(field.data_type()) {
                let metadata = (!field.metadata().is_empty())
                    .then(|| FieldMetadata::from(field.metadata().clone()));
                udf.call(vec![column])
                    .alias_with_metadata(field.name(), metadata)
            } else {
                column
            }
        })
        .collect::<Vec<_>>();
    frame.select(projection)
}

fn percent_decoded(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while let Some(&byte) = bytes.get(at) {
        if byte == b'%' {
            let pair = bytes.get(at + 1..at + 3)?;
            if !pair.iter().all(u8::is_ascii_hexdigit) {
                return None;
            }
            out.push(u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?);
            at += 3;
        } else {
            out.push(byte);
            at += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[must_use]
pub fn spark_partition_segment(raw: &str) -> Option<String> {
    let decoded = percent_decoded(raw)?;
    let (name, value) = decoded.split_once('=')?;
    let spelled = format!(
        "{}={}",
        escape_partition_value(name).ok()?,
        escape_partition_value(value).ok()?
    );
    (spelled != raw).then_some(spelled)
}

fn local_io_error(path: &Path, error: &std::io::Error) -> Error {
    Error::DataFusion(format!(
        "cannot name partition directory {}: {error}",
        path.display()
    ))
}

#[allow(clippy::missing_errors_doc)]
pub fn respell_local_partition_dirs(root: &Path, depth: usize) -> Result<()> {
    if depth == 0 || !root.is_dir() {
        return Ok(());
    }
    let mut children: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(root).map_err(|error| local_io_error(root, &error))? {
        let entry = entry.map_err(|error| local_io_error(root, &error))?;
        let kind = entry
            .file_type()
            .map_err(|error| local_io_error(&entry.path(), &error))?;
        if kind.is_dir() {
            children.push(entry.path());
        }
    }
    for child in children {
        let spelled = child
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(spark_partition_segment);
        let next = match spelled {
            Some(name) => {
                let target = child.with_file_name(name);
                if target.exists() {
                    return Err(Error::DataFusion(format!(
                        "cannot name partition directory {}: {} already exists",
                        child.display(),
                        target.display()
                    )));
                }
                std::fs::rename(&child, &target).map_err(|error| local_io_error(&child, &error))?;
                target
            }
            None => child,
        };
        respell_local_partition_dirs(&next, depth - 1)?;
    }
    Ok(())
}

fn spelled_key(key: &ObjectPath, base: usize, depth: usize) -> Option<ObjectPath> {
    let parts: Vec<String> = key.parts().map(|part| part.as_ref().to_string()).collect();
    let leaf = parts.len().checked_sub(1)?;
    let end = leaf.min(base.saturating_add(depth));
    let mut changed = false;
    let mut spelled = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        match (index >= base && index < end)
            .then(|| spark_partition_segment(part))
            .flatten()
        {
            Some(name) => {
                changed = true;
                spelled.push(name);
            }
            None => spelled.push(part.clone()),
        }
    }
    if !changed {
        return None;
    }
    ObjectPath::parse(spelled.join("/")).ok()
}

pub(crate) async fn respell_store_partition_keys(
    store: &dyn ObjectStore,
    keys: &[ObjectPath],
    base: usize,
    depth: usize,
    moved: &mut Vec<ObjectPath>,
    origin: &str,
) -> Result<()> {
    for key in keys {
        let Some(target) = spelled_key(key, base, depth) else {
            continue;
        };
        moved.push(target.clone());
        store.rename(key, &target).await.map_err(|error| {
            Error::DataFusion(format!(
                "cannot name partition object {key} as {target} under {origin}: {error}"
            ))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BooleanArray, Date32Array, DictionaryArray, Float64Array, Int32Array,
        StringArray, StringViewArray,
    };
    use arrow::datatypes::Int8Type;
    use object_store::path::Path as ObjectPath;

    use super::{
        HIVE_DEFAULT_PARTITION, hive_partition_texts, respell_local_partition_dirs,
        spark_partition_segment, spelled_key,
    };

    fn texts(array: ArrayRef) -> Vec<String> {
        let rendered = hive_partition_texts(&array).unwrap();
        rendered
            .iter()
            .map(|value| value.unwrap().to_string())
            .collect()
    }

    #[test]
    fn a_null_partition_value_names_the_hive_default_directory() {
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(0), Some(-7)]));
        assert_eq!(texts(ints), [HIVE_DEFAULT_PARTITION, "0", "-7"]);
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![None, Some(0), Some(19_783)]));
        assert_eq!(
            texts(dates),
            [HIVE_DEFAULT_PARTITION, "1970-01-01", "2024-03-01"]
        );
        let flags: ArrayRef = Arc::new(BooleanArray::from(vec![None, Some(true), Some(false)]));
        assert_eq!(texts(flags), [HIVE_DEFAULT_PARTITION, "true", "false"]);
    }

    #[test]
    fn an_empty_or_null_string_names_the_hive_default_directory() {
        let plain: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            Some(""),
            Some("a"),
            Some(" "),
        ]));
        assert_eq!(
            texts(plain),
            [HIVE_DEFAULT_PARTITION, HIVE_DEFAULT_PARTITION, "a", " "]
        );
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![None, Some(""), Some("b")]));
        assert_eq!(
            texts(view),
            [HIVE_DEFAULT_PARTITION, HIVE_DEFAULT_PARTITION, "b"]
        );
        let dictionary: DictionaryArray<Int8Type> =
            vec![Some("x"), None, Some("")].into_iter().collect();
        let dictionary: ArrayRef = Arc::new(dictionary);
        assert_eq!(
            texts(dictionary),
            ["x", HIVE_DEFAULT_PARTITION, HIVE_DEFAULT_PARTITION]
        );
    }

    #[test]
    fn a_non_null_value_keeps_the_demux_text() {
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(1.5),
            Some(f64::NAN),
            Some(-0.0),
            Some(f64::INFINITY),
            Some(1e20),
        ]));
        assert_eq!(
            texts(floats),
            ["1", "1.5", "NaN", "-0", "inf", "100000000000000000000"]
        );
    }

    #[test]
    fn a_segment_is_respelled_with_spark_escaping() {
        let cases = [
            ("p=a:b", "p=a%3Ab"),
            ("p=a=b", "p=a%3Db"),
            ("p=a'b", "p=a%27b"),
            ("p=%C3%A9", "p=é"),
            ("p=%E6%97%A5%E6%9C%AC", "p=日本"),
            ("p=a%7Eb", "p=a~b"),
            ("p=a%7Bb%7D", "p=a%7Bb}"),
            ("p=a%3Cb%3E", "p=a<b>"),
            ("p=a%60b", "p=a`b"),
            ("p=a%7Cb", "p=a|b"),
            ("%C3%A9t=1", "ét=1"),
        ];
        for (raw, spelled) in cases {
            assert_eq!(
                spark_partition_segment(raw).as_deref(),
                Some(spelled),
                "{raw}"
            );
        }
        for kept in [
            "p=a%2Fb",
            "p=a%25b",
            "p=%2541",
            "p=a%09b",
            "p=a%23b",
            "p=__HIVE_DEFAULT_PARTITION__",
            "p=5",
            "p=a b",
            "p=..",
            "nosign",
            "p=%zz",
        ] {
            assert_eq!(spark_partition_segment(kept), None, "{kept}");
        }
    }

    #[test]
    fn local_directories_are_respelled_to_the_partition_depth() {
        let root = tempfile::tempdir().unwrap();
        let leaf = root.path().join("p=a:b").join("q=%C3%A9");
        std::fs::create_dir_all(&leaf).unwrap();
        std::fs::write(leaf.join("part.parquet"), b"x").unwrap();
        let deeper = root.path().join("p=5").join("q=x").join("r=a:b");
        std::fs::create_dir_all(&deeper).unwrap();
        respell_local_partition_dirs(root.path(), 2).unwrap();
        assert!(
            root.path()
                .join("p=a%3Ab")
                .join("q=é")
                .join("part.parquet")
                .is_file()
        );
        assert!(root.path().join("p=5").join("q=x").join("r=a:b").is_dir());
        assert!(!root.path().join("p=a:b").exists());
    }

    #[test]
    fn store_keys_are_respelled_below_the_prefix_only() {
        let key = ObjectPath::parse("cell/a:b/p=a:b/q=%C3%A9/part.parquet").unwrap();
        let spelled = spelled_key(&key, 2, 2).unwrap();
        assert_eq!(spelled.as_ref(), "cell/a:b/p=a%3Ab/q=é/part.parquet");
        let plain = ObjectPath::parse("cell/p=5/part.parquet").unwrap();
        assert_eq!(spelled_key(&plain, 1, 1), None);
        let leaf = ObjectPath::parse("cell/p=a:b").unwrap();
        assert_eq!(spelled_key(&leaf, 1, 1), None);
    }
}
