use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryArray, StringArray};
use datafusion::arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
const fn decode_table() -> [i16; 256] {
    let mut table = [-1i16; 256];
    let mut index = 0usize;
    while index < 64 {
        table[ALPHABET[index] as usize] = index as i16;
        index += 1;
    }
    table[b'=' as usize] = -2;
    table
}

static DECODE_TABLE: [i16; 256] = decode_table();

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
        let mut offsets = Vec::with_capacity(array.len() + 1);
        let mut values = Vec::new();
        offsets.push(0i32);
        each_bytes(&array, |row| {
            if let Some(bytes) = row {
                encode_mime_into(bytes, &mut values);
            }
            offsets.push(i32::try_from(values.len()).map_err(|_| {
                DataFusionError::Execution("'base64' output exceeds i32 offsets".to_string())
            })?);
            Ok(())
        })?;
        let encoded = StringArray::try_new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from(values),
            array.logical_nulls(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(encoded)))
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
        let mut offsets = Vec::with_capacity(array.len() + 1);
        let mut values = Vec::new();
        offsets.push(0i32);
        each_bytes(&array, |row| {
            if let Some(bytes) = row {
                decode_mime_into(bytes, &mut values)?;
            }
            offsets.push(i32::try_from(values.len()).map_err(|_| {
                DataFusionError::Execution("'unbase64' output exceeds i32 offsets".to_string())
            })?);
            Ok(())
        })?;
        let decoded = BinaryArray::try_new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from(values),
            array.logical_nulls(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(decoded)))
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

fn each_bytes(array: &ArrayRef, mut visit: impl FnMut(Option<&[u8]>) -> Result<()>) -> Result<()> {
    match array.data_type() {
        DataType::Utf8 => {
            for row in array.as_string::<i32>() {
                visit(row.map(str::as_bytes))?;
            }
        }
        DataType::LargeUtf8 => {
            for row in array.as_string::<i64>() {
                visit(row.map(str::as_bytes))?;
            }
        }
        DataType::Utf8View => {
            for row in array.as_string_view() {
                visit(row.map(str::as_bytes))?;
            }
        }
        DataType::Binary => {
            for row in array.as_binary::<i32>() {
                visit(row)?;
            }
        }
        DataType::LargeBinary => {
            for row in array.as_binary::<i64>() {
                visit(row)?;
            }
        }
        DataType::BinaryView => {
            for row in array.as_binary_view() {
                visit(row)?;
            }
        }
        DataType::Null => {
            for _ in 0..array.len() {
                visit(None)?;
            }
        }
        other => return exec_err!("'base64'/'unbase64' on unsupported type {other}"),
    }
    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
fn encode_mime_into(bytes: &[u8], out: &mut Vec<u8>) {
    let mut first_line = true;
    for line in bytes.chunks(57) {
        if first_line {
            first_line = false;
        } else {
            out.extend_from_slice(b"\r\n");
        }
        for quad in line.chunks(3) {
            let word = (u32::from(quad[0]) << 16)
                | (u32::from(*quad.get(1).unwrap_or(&0)) << 8)
                | u32::from(*quad.get(2).unwrap_or(&0));
            out.push(ALPHABET[((word >> 18) & 63) as usize]);
            out.push(ALPHABET[((word >> 12) & 63) as usize]);
            out.push(if quad.len() > 1 {
                ALPHABET[((word >> 6) & 63) as usize]
            } else {
                b'='
            });
            out.push(if quad.len() > 2 {
                ALPHABET[(word & 63) as usize]
            } else {
                b'='
            });
        }
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn decode_mime_into(bytes: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let mut bits: u32 = 0;
    let mut shiftto: i32 = 18;
    let mut cursor = 0usize;
    let limit = bytes.len();
    while cursor < limit {
        let value = DECODE_TABLE[usize::from(bytes[cursor])];
        cursor += 1;
        if value < 0 {
            if value == -2 {
                let missing_pad = shiftto == 6
                    && (cursor == limit || {
                        let next = bytes[cursor];
                        cursor += 1;
                        next != b'='
                    });
                if missing_pad || shiftto == 18 {
                    return exec_err!("Input byte array has wrong 4-byte ending unit");
                }
                break;
            }
            continue;
        }
        bits |= (value as u32) << shiftto;
        shiftto -= 6;
        if shiftto < 0 {
            out.push((bits >> 16) as u8);
            out.push((bits >> 8) as u8);
            out.push(bits as u8);
            shiftto = 18;
            bits = 0;
        }
    }
    if shiftto == 6 {
        out.push((bits >> 16) as u8);
    } else if shiftto == 0 {
        out.push((bits >> 16) as u8);
        out.push((bits >> 8) as u8);
    } else if shiftto == 12 {
        return exec_err!("Last unit does not have enough valid bits");
    }
    while cursor < limit {
        let value = DECODE_TABLE[usize::from(bytes[cursor])];
        cursor += 1;
        if value < 0 {
            continue;
        }
        return exec_err!("Input byte array has incorrect ending byte at {cursor}");
    }
    Ok(())
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

    async fn error_text(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => frame
                .collect()
                .await
                .err()
                .unwrap_or_else(|| panic!("{sql} should refuse"))
                .to_string(),
        }
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
            ("SELECT unbase64('QR')", b"A".as_slice()),
            ("SELECT unbase64('QQQ')", b"A\x04".as_slice()),
            ("SELECT unbase64('U3Bh cms=')", b"Spark".as_slice()),
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

    #[tokio::test]
    async fn unbase64_refuses_bad_endings_with_java_text() {
        let ctx = ctx();
        for (sql, fragment) in [
            (
                "SELECT unbase64('QQ==QQ')",
                "Input byte array has incorrect ending byte at 5",
            ),
            (
                "SELECT unbase64('U3Bhcms=QQ')",
                "Input byte array has incorrect ending byte at 9",
            ),
            (
                "SELECT unbase64('QQ=')",
                "Input byte array has wrong 4-byte ending unit",
            ),
            (
                "SELECT unbase64('=QQ')",
                "Input byte array has wrong 4-byte ending unit",
            ),
            (
                "SELECT unbase64('Q')",
                "Last unit does not have enough valid bits",
            ),
        ] {
            let error = error_text(&ctx, sql).await;
            assert!(error.contains(fragment), "{sql} -> {error}");
        }
    }
}
