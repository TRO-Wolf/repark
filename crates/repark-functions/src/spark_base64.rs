use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

#[must_use]
pub fn base64_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkBase64::new()))
}

#[must_use]
pub fn unbase64_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkUnBase64::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![base64_udf(), unbase64_udf()]
}

#[derive(Debug)]
struct SparkBase64 {
    signature: Signature,
}

impl SparkBase64 {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkBase64 {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkBase64 {}

impl Hash for SparkBase64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkBase64 {
    crate::shim_udf_boilerplate!("base64");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new("base64", DataType::Utf8, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type] if accepted_bytes(data_type) => Ok(vec![data_type.clone()]),
            [DataType::Null] => Ok(vec![DataType::Utf8]),
            [data_type] => Err(unexpected_input_type("base64", data_type)),
            _ => exec_err!("'base64' expects one argument, got {}", arg_types.len()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(arg) = args.args.first() else {
            return exec_err!("'base64' expects one argument");
        };
        let array = arg.to_array(args.number_rows)?;
        let rows = bytes_column(&array)?;
        let values: Vec<Option<String>> = rows.iter().map(|row| row.map(encode_mime)).collect();
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
    }
}

#[derive(Debug)]
struct SparkUnBase64 {
    signature: Signature,
}

impl SparkUnBase64 {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkUnBase64 {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkUnBase64 {}

impl Hash for SparkUnBase64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkUnBase64 {
    crate::shim_udf_boilerplate!("unbase64");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new("unbase64", DataType::Binary, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type] if accepted_bytes(data_type) => Ok(vec![data_type.clone()]),
            [DataType::Null] => Ok(vec![DataType::Utf8]),
            [data_type] => Err(unexpected_input_type("unbase64", data_type)),
            _ => exec_err!("'unbase64' expects one argument, got {}", arg_types.len()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(arg) = args.args.first() else {
            return exec_err!("'unbase64' expects one argument");
        };
        let array = arg.to_array(args.number_rows)?;
        let rows = bytes_column(&array)?;
        let decoded: Vec<Option<Vec<u8>>> = rows
            .iter()
            .map(|row| match row {
                None => Ok(None),
                Some(bytes) => decode_mime(bytes).map(Some),
            })
            .collect::<Result<_>>()?;
        let values: Vec<Option<&[u8]>> = decoded.iter().map(|row| row.as_deref()).collect();
        Ok(ColumnarValue::Array(Arc::new(BinaryArray::from(values))))
    }
}

fn accepted_bytes(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
    )
}

fn unexpected_input_type(name: &str, got: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}(<expr>)\" due to \
         data type mismatch: The first parameter requires the \"BINARY\" type, however the \
         argument has the type \"{got}\"."
    ))
}

fn bytes_column(array: &ArrayRef) -> Result<Vec<Option<&[u8]>>> {
    let len = array.len();
    match array.data_type() {
        DataType::Utf8 => {
            let column = array.as_string::<i32>();
            Ok((0..len)
                .map(|index| (!column.is_null(index)).then(|| column.value(index).as_bytes()))
                .collect())
        }
        DataType::LargeUtf8 => {
            let column = array.as_string::<i64>();
            Ok((0..len)
                .map(|index| (!column.is_null(index)).then(|| column.value(index).as_bytes()))
                .collect())
        }
        DataType::Utf8View => {
            let column = array.as_string_view();
            Ok((0..len)
                .map(|index| (!column.is_null(index)).then(|| column.value(index).as_bytes()))
                .collect())
        }
        DataType::Binary => {
            let column = array.as_binary::<i32>();
            Ok((0..len)
                .map(|index| (!column.is_null(index)).then(|| column.value(index)))
                .collect())
        }
        DataType::LargeBinary => {
            let column = array.as_binary::<i64>();
            Ok((0..len)
                .map(|index| (!column.is_null(index)).then(|| column.value(index)))
                .collect())
        }
        DataType::BinaryView => {
            let column = array.as_binary_view();
            Ok((0..len)
                .map(|index| (!column.is_null(index)).then(|| column.value(index)))
                .collect())
        }
        DataType::Null => Ok(vec![None; len]),
        other => exec_err!("'base64'/'unbase64' on unsupported type {other}"),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn encode_mime(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4 + bytes.len() / 57 * 2);
    let mut column = 0usize;
    for chunk in bytes.chunks(3) {
        let word = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        let sextets = [
            ALPHABET[((word >> 18) & 63) as usize],
            ALPHABET[((word >> 12) & 63) as usize],
            if chunk.len() > 1 {
                ALPHABET[((word >> 6) & 63) as usize]
            } else {
                b'='
            },
            if chunk.len() > 2 {
                ALPHABET[(word & 63) as usize]
            } else {
                b'='
            },
        ];
        for ch in sextets {
            if column == 76 {
                out.push('\r');
                out.push('\n');
                column = 0;
            }
            out.push(char::from(ch));
            column += 1;
        }
    }
    out
}

#[allow(clippy::cast_possible_truncation)]
fn decode_mime(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut table = [u8::MAX; 256];
    for (index, symbol) in ALPHABET.iter().enumerate() {
        table[usize::from(*symbol)] = u8::try_from(index).unwrap_or(u8::MAX);
    }
    let mut sextets = Vec::with_capacity(bytes.len());
    for byte in bytes {
        if *byte == b'=' {
            break;
        }
        let value = table[usize::from(*byte)];
        if value != u8::MAX {
            sextets.push(value);
        }
    }
    let mut out = Vec::with_capacity(sextets.len() * 3 / 4);
    for group in sextets.chunks(4) {
        let word = group
            .iter()
            .fold(0u32, |acc, symbol| (acc << 6) | u32::from(*symbol));
        match group.len() {
            4 => {
                out.push((word >> 16) as u8);
                out.push((word >> 8) as u8);
                out.push(word as u8);
            }
            3 => {
                let padded = word << 6;
                out.push((padded >> 16) as u8);
                out.push((padded >> 8) as u8);
            }
            2 => {
                let padded = word << 12;
                out.push((padded >> 16) as u8);
            }
            _ => {
                return exec_err!(
                    "unbase64: input ends with a single base64 digit, which cannot decode"
                );
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use datafusion::common::ScalarValue;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> ScalarValue {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("exec {sql}: {error}"));
        ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0)
            .unwrap_or_else(|error| panic!("scalar {sql}: {error}"))
    }

    #[tokio::test]
    async fn base64_pads_and_chunks() {
        let ctx = ctx();
        for (sql, want) in [
            ("SELECT base64('Spark')", "U3Bhcms="),
            ("SELECT base64('A')", "QQ=="),
            ("SELECT base64('Apache')", "QXBhY2hl"),
            ("SELECT base64('AB')", "QUI="),
            ("SELECT base64('')", ""),
            ("SELECT base64(X'00FF')", "AP8="),
        ] {
            assert_eq!(
                one(&ctx, sql).await,
                ScalarValue::Utf8(Some(want.to_string())),
                "{sql}"
            );
        }
        assert_eq!(
            one(&ctx, "SELECT base64(CAST(NULL AS STRING))").await,
            ScalarValue::Utf8(None)
        );
        let long = one(&ctx, "SELECT base64(repeat('x', 100))").await;
        let ScalarValue::Utf8(Some(encoded)) = long else {
            panic!("expected utf8");
        };
        assert_eq!(encoded.len(), 138);
        assert_eq!(&encoded[76..78], "\r\n");
    }

    #[tokio::test]
    async fn unbase64_decodes_leniently() {
        let ctx = ctx();
        for (sql, want) in [
            ("SELECT unbase64('U3Bhcms=')", b"Spark".as_slice()),
            ("SELECT unbase64('U3Bhcms')", b"Spark".as_slice()),
            ("SELECT unbase64('!!')", b"".as_slice()),
        ] {
            assert_eq!(
                one(&ctx, sql).await,
                ScalarValue::Binary(Some(want.to_vec())),
                "{sql}"
            );
        }
        assert_eq!(
            one(&ctx, "SELECT cast(unbase64('eHh4\r\neHh4') AS STRING)").await,
            ScalarValue::Utf8View(Some("xxxxxx".to_string()))
        );
        assert_eq!(
            one(&ctx, "SELECT unbase64(CAST(NULL AS STRING))").await,
            ScalarValue::Binary(None)
        );
    }
}
