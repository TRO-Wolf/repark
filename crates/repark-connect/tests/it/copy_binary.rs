use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::Int32Type;
use repark_connect::postgres::{PgTypeKind, PlannedColumn, TypeMod};
use repark_connect::{
    BatchLimits, COPY_SIGNATURE, ConnectError, CopyBinaryDecoder, DEFAULT_BATCH_BYTES,
    DEFAULT_BATCH_ROWS, MAX_BATCH_BYTES, MAX_FIELD_BYTES, ProtocolViolation, ValueRefusal,
};

pub(crate) type Field = Option<Vec<u8>>;

fn planned(name: &str, typname: &str, kind: PgTypeKind, typmod: TypeMod) -> PlannedColumn {
    PlannedColumn::resolve(Arc::from(name), typname, kind, typmod).expect("a mapped column")
}

pub(crate) fn base(name: &str, typname: &str) -> PlannedColumn {
    planned(name, typname, PgTypeKind::Base, TypeMod::NONE)
}

pub(crate) fn header_with(flags: u32, extension: &[u8]) -> Vec<u8> {
    let mut stream = COPY_SIGNATURE.to_vec();
    stream.extend_from_slice(&flags.to_be_bytes());
    let length = i32::try_from(extension.len()).expect("extension length");
    stream.extend_from_slice(&length.to_be_bytes());
    stream.extend_from_slice(extension);
    stream
}

pub(crate) fn tuple(fields: &[Field]) -> Vec<u8> {
    let count = i16::try_from(fields.len()).expect("field count");
    let mut bytes = count.to_be_bytes().to_vec();
    for field in fields {
        match field {
            Some(value) => {
                let length = i32::try_from(value.len()).expect("field length");
                bytes.extend_from_slice(&length.to_be_bytes());
                bytes.extend_from_slice(value);
            }
            None => bytes.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }
    bytes
}

fn stream(tuples: &[Vec<Field>]) -> Vec<u8> {
    let mut bytes = header_with(0, &[]);
    for fields in tuples {
        bytes.extend(tuple(fields));
    }
    bytes.extend_from_slice(&(-1_i16).to_be_bytes());
    bytes
}

fn int4s(values: &[Option<i32>]) -> Vec<Vec<Field>> {
    values
        .iter()
        .map(|value| vec![value.map(|value| value.to_be_bytes().to_vec())])
        .collect()
}

fn limits(rows: usize, bytes: usize) -> BatchLimits {
    BatchLimits::new(
        NonZeroUsize::new(rows).expect("rows"),
        NonZeroUsize::new(bytes).expect("bytes"),
    )
}

fn decoder(columns: Vec<PlannedColumn>) -> CopyBinaryDecoder {
    CopyBinaryDecoder::new(columns, BatchLimits::default()).expect("a decoder")
}

fn feed(
    decoder: &mut CopyBinaryDecoder,
    chunks: &[&[u8]],
) -> Result<Vec<RecordBatch>, ConnectError> {
    let mut batches = Vec::new();
    for chunk in chunks {
        let mut rest: &[u8] = chunk;
        while !rest.is_empty() {
            let before = rest.len();
            match decoder.decode(&mut rest)? {
                Some(batch) => batches.push(batch),
                None => assert!(
                    rest.is_empty(),
                    "decode returned without consuming its chunk"
                ),
            }
            assert!(rest.len() < before, "decode made no progress");
        }
    }
    decoder.finish()?;
    Ok(batches)
}

fn decode_all(columns: Vec<PlannedColumn>, bytes: &[u8]) -> Result<Vec<RecordBatch>, ConnectError> {
    feed(&mut decoder(columns), &[bytes])
}

fn protocol(violation: ProtocolViolation) -> ConnectError {
    ConnectError::Protocol { violation }
}

#[test]
fn copy_header_is_the_signature_flags_and_extension() {
    assert_eq!(&COPY_SIGNATURE, b"PGCOPY\n\xff\r\n\0");
    let mut bytes = header_with(0x0000_ffff, b"ext!!");
    bytes.extend(tuple(&[Some(7_i32.to_be_bytes().to_vec())]));
    bytes.extend_from_slice(&(-1_i16).to_be_bytes());
    let batches = decode_all(vec![base("n", "int4")], &bytes).expect("low flag bits are ignored");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].column(0).as_primitive::<Int32Type>().value(0), 7);
    for position in 0..COPY_SIGNATURE.len() {
        let mut bytes = stream(&int4s(&[Some(1)]));
        bytes[position] ^= 0x01;
        let error = decode_all(vec![base("n", "int4")], &bytes).expect_err("a bad signature");
        assert_eq!(
            error,
            protocol(ProtocolViolation::Signature),
            "byte {position}"
        );
    }
}

#[test]
fn copy_oid_flag_refuses() {
    let error =
        decode_all(vec![base("n", "int4")], &header_with(1 << 16, &[])).expect_err("OIDs refuse");
    assert_eq!(error, protocol(ProtocolViolation::OidColumns));
    for flags in [1_u32 << 17, 1 << 31, 0xFFFE_0000] {
        let error = decode_all(vec![base("n", "int4")], &header_with(flags, &[]))
            .expect_err("critical flags refuse");
        assert_eq!(error, protocol(ProtocolViolation::CriticalFlags { flags }));
    }
    let mut negative = COPY_SIGNATURE.to_vec();
    negative.extend_from_slice(&0_u32.to_be_bytes());
    negative.extend_from_slice(&(-1_i32).to_be_bytes());
    let error = decode_all(vec![base("n", "int4")], &negative).expect_err("negative extension");
    assert_eq!(
        error,
        protocol(ProtocolViolation::HeaderExtension { length: -1 })
    );
}

#[test]
fn copy_critical_flag_bits_each_refuse() {
    for bit in [18_u32, 24, 30] {
        let flags = 1_u32 << bit;
        let error = decode_all(vec![base("n", "int4")], &header_with(flags, &[]))
            .expect_err("a critical flag refuses");
        assert_eq!(
            error,
            protocol(ProtocolViolation::CriticalFlags { flags }),
            "bit {bit}"
        );
    }
}

#[test]
fn copy_trailer_ends_and_trailing_bytes_refuse() {
    let bytes = stream(&int4s(&[Some(1), Some(2)]));
    let mut decoder = decoder(vec![base("n", "int4")]);
    let batches = feed(&mut decoder, &[&bytes]).expect("a complete stream");
    assert!(decoder.is_done());
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let mut trailing = bytes.clone();
    trailing.push(0);
    let error = decode_all(vec![base("n", "int4")], &trailing).expect_err("one more byte");
    assert_eq!(error, protocol(ProtocolViolation::TrailingBytes));
    let error = feed(&mut self::decoder(vec![base("n", "int4")]), &[&bytes, &[0]])
        .expect_err("a trailing chunk");
    assert_eq!(error, protocol(ProtocolViolation::TrailingBytes));
    let empty = stream(&[]);
    let batches = decode_all(vec![base("n", "int4")], &empty).expect("no rows");
    assert!(batches.is_empty());
}

#[test]
fn copy_truncated_stream_is_disconnected() {
    let bytes = stream(&int4s(&[Some(1), None, Some(3)]));
    let header_end = header_with(0, &[]).len();
    for cut in [
        0,
        5,
        header_end,
        header_end + 1,
        header_end + 4,
        bytes.len() - 3,
        bytes.len() - 2,
        bytes.len() - 1,
    ] {
        let error = decode_all(vec![base("n", "int4")], &bytes[..cut]).expect_err("truncated");
        assert_eq!(error, ConnectError::Disconnected, "cut at {cut}");
    }
    let mut decoder = decoder(vec![base("n", "int4")]);
    let mut rest = &bytes[..bytes.len() - 2];
    while !rest.is_empty() {
        assert!(
            decoder
                .decode(&mut rest)
                .expect("well-formed so far")
                .is_none()
        );
    }
    assert_eq!(decoder.buffered_rows(), 3);
    assert_eq!(decoder.finish(), Err(ConnectError::Disconnected));
}

#[test]
fn decoder_is_poisoned_after_an_error() {
    let mut decoder = decoder(vec![base("n", "int4")]);
    let bad = header_with(1 << 16, &[]);
    let mut rest: &[u8] = &bad;
    let error = decoder.decode(&mut rest).expect_err("OIDs refuse");
    assert_eq!(error, protocol(ProtocolViolation::OidColumns));
    let valid = stream(&int4s(&[Some(1)]));
    let mut chunk: &[u8] = &valid;
    let error = decoder.decode(&mut chunk).expect_err("the poison holds");
    assert_eq!(error, protocol(ProtocolViolation::OidColumns));
    assert_eq!(chunk, valid.as_slice(), "a poisoned decode reads no input");
    assert_eq!(
        decoder.finish(),
        Err(protocol(ProtocolViolation::OidColumns))
    );
}

fn every_mapping() -> (Vec<PlannedColumn>, Vec<Vec<Field>>) {
    let columns = vec![
        base("flag", "bool"),
        base("small", "int2"),
        base("id", "int4"),
        base("big", "int8"),
        base("ratio", "float4"),
        base("score", "float8"),
        base("note", "text"),
        base("blob", "bytea"),
        planned(
            "amount",
            "numeric",
            PgTypeKind::Base,
            TypeMod::numeric(8, 3),
        ),
        base("day", "date"),
        base("wall", "timestamp"),
        base("instant", "timestamptz"),
        base("span", "interval"),
        base("key", "uuid"),
        base("doc", "json"),
        base("tree", "jsonb"),
        planned("mood", "mood", PgTypeKind::Enum, TypeMod::NONE),
    ];
    let value = |bytes: &[u8]| Some(bytes.to_vec());
    let first = vec![
        value(&[1]),
        value(&(-2_i16).to_be_bytes()),
        value(&7_i32.to_be_bytes()),
        value(&i64::MIN.to_be_bytes()),
        value(&1.5_f32.to_be_bytes()),
        value(&(-0.25_f64).to_be_bytes()),
        value("naïve ☃".as_bytes()),
        value(&[0, 255, 16]),
        value(&[
            0x00, 0x03, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x09, 0x29, 0x1a, 0x7c,
        ]),
        value(&[0x00, 0x00, 0x22, 0x83]),
        value(&[0x00, 0x02, 0xb6, 0x4b, 0xee, 0xe1, 0xd0, 0x00]),
        value(&[0xff, 0xfc, 0xa2, 0xfe, 0xc4, 0xc8, 0x20, 0x00]),
        value(b"1 day 02:03:04"),
        value(&[
            0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38,
            0x0a, 0x11,
        ]),
        value(b" [1, 2] "),
        value(b"\x01{\"a\": 1}"),
        value(b"happy"),
    ];
    let nulls = vec![None; columns.len()];
    let third = vec![
        value(&[0]),
        value(&i16::MAX.to_be_bytes()),
        None,
        value(&0_i64.to_be_bytes()),
        None,
        value(&f64::NAN.to_be_bytes()),
        value(b""),
        value(b""),
        value(&[0x00, 0x01, 0xff, 0xff, 0x40, 0x00, 0x00, 0x01, 0x13, 0x88]),
        value(&[0xff, 0xff, 0xd5, 0x33]),
        None,
        value(&[0x00, 0x02, 0xb6, 0x4b, 0xee, 0xe1, 0xd0, 0x00]),
        value(b""),
        value(&[0; 16]),
        value(b"{}"),
        value(b"\x01null"),
        None,
    ];
    (columns, vec![first, nulls, third])
}

#[test]
fn copy_decode_is_independent_of_chunking() {
    let (columns, tuples) = every_mapping();
    let bytes = stream(&tuples);
    let whole = decode_all(columns.clone(), &bytes).expect("one chunk");
    assert_eq!(whole.len(), 1);
    let expected = &whole[0];
    assert_eq!(expected.num_rows(), 3);
    assert_eq!(expected.num_columns(), columns.len());
    assert_eq!(
        expected.column(13).as_string::<i32>().value(0),
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    );
    assert_eq!(
        expected.column(15).as_string::<i32>().value(0),
        r#"{"a": 1}"#
    );
    for column in expected.columns() {
        assert!(column.is_null(1), "the all-NULL tuple");
    }
    for split in 0..=bytes.len() {
        let (head, tail) = bytes.split_at(split);
        let batches = feed(&mut decoder(columns.clone()), &[head, tail]).expect("two chunks");
        let joined = concat_batches(&expected.schema(), &batches).expect("concat");
        assert_eq!(&joined, expected, "split at byte {split}");
    }
    let single_bytes: Vec<&[u8]> = bytes.chunks(1).collect();
    let batches = feed(&mut decoder(columns.clone()), &single_bytes).expect("one-byte chunks");
    let joined = concat_batches(&expected.schema(), &batches).expect("concat");
    assert_eq!(&joined, expected);
    let triples: Vec<&[u8]> = bytes.chunks(3).collect();
    let batches = feed(&mut decoder(columns), &triples).expect("three-byte chunks");
    let joined = concat_batches(&expected.schema(), &batches).expect("concat");
    assert_eq!(&joined, expected);
}

#[test]
fn copy_field_count_must_match_projection() {
    let columns = || vec![base("a", "int4"), base("b", "int4"), base("c", "int4")];
    let short = stream(&[vec![Some(1_i32.to_be_bytes().to_vec()), None]]);
    let error = decode_all(columns(), &short).expect_err("two fields for three columns");
    assert_eq!(
        error,
        protocol(ProtocolViolation::FieldCount {
            expected: 3,
            actual: 2,
        })
    );
    let mut negative = header_with(0, &[]);
    negative.extend_from_slice(&(-2_i16).to_be_bytes());
    let error = decode_all(columns(), &negative).expect_err("a negative count");
    assert_eq!(
        error,
        protocol(ProtocolViolation::FieldCount {
            expected: 3,
            actual: -2,
        })
    );
    let counted = stream(&[vec![], vec![], vec![]]);
    let batches = decode_all(vec![], &counted).expect("an empty projection counts rows");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
    assert_eq!(batches[0].num_columns(), 0);
}

#[test]
fn copy_fields_refuse_bad_lengths_nulls_and_values_naming_the_column() {
    let mut bytes = header_with(0, &[]);
    bytes.extend_from_slice(&1_i16.to_be_bytes());
    bytes.extend_from_slice(&(-7_i32).to_be_bytes());
    let error = decode_all(vec![base("n", "int4")], &bytes).expect_err("negative length");
    assert_eq!(
        error,
        protocol(ProtocolViolation::FieldLength { length: -7 })
    );
    let not_null = vec![base("n", "int4").with_nullable(false)];
    let error = decode_all(not_null, &stream(&int4s(&[Some(1), None])))
        .expect_err("NULL in a NOT NULL column");
    assert_eq!(
        error,
        protocol(ProtocolViolation::NullInNotNullColumn { column: 0 })
    );
    let nan = vec![0x00, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00];
    let tuples = vec![
        vec![
            Some(1_i32.to_be_bytes().to_vec()),
            Some(vec![0, 0, 0, 0, 0, 0, 0, 0]),
        ],
        vec![Some(2_i32.to_be_bytes().to_vec()), Some(nan)],
    ];
    let columns = vec![base("id", "int4"), base("amount", "numeric")];
    let error = decode_all(columns, &stream(&tuples)).expect_err("NaN");
    assert_eq!(
        error,
        ConnectError::UnrepresentableValue {
            column: Arc::from("amount"),
            postgres_type: "numeric",
            index: 1,
            reason: ValueRefusal::NumericNaN,
        }
    );
    let error = decode_all(vec![base("n", "int4")], &stream(&[vec![Some(vec![1, 2])]]))
        .expect_err("short int4");
    assert_eq!(
        error,
        ConnectError::WireLength {
            postgres_name: "int4",
            index: 0,
            expected: 4,
            actual: 2,
        }
    );
}

#[test]
fn batches_flush_at_rows_and_at_bytes() {
    assert_eq!(DEFAULT_BATCH_ROWS.get(), 8192);
    assert_eq!(DEFAULT_BATCH_BYTES.get(), 64 << 20);
    assert_eq!(BatchLimits::default().bytes(), DEFAULT_BATCH_BYTES);
    let seven: Vec<Option<i32>> = (1..=7).map(Some).collect();
    let bytes = stream(&int4s(&seven));
    let mut decoder =
        CopyBinaryDecoder::new(vec![base("n", "int4")], limits(3, 1 << 30)).expect("a decoder");
    let batches = feed(&mut decoder, &[&bytes]).expect("seven rows");
    let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(sizes, vec![3, 3, 1]);
    let values: Vec<i32> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, (1..=7).collect::<Vec<_>>());
    assert_eq!(decoder.tuples(), 7);
    assert_eq!(decoder.buffered_rows(), 0);

    let large = vec![0xab_u8; DEFAULT_BATCH_BYTES.get()];
    let tuples = vec![
        vec![Some(large), Some(1_i32.to_be_bytes().to_vec())],
        vec![Some(vec![1]), Some(2_i32.to_be_bytes().to_vec())],
        vec![Some(vec![2]), Some(3_i32.to_be_bytes().to_vec())],
    ];
    let bytes = stream(&tuples);
    let columns = vec![base("payload", "bytea"), base("n", "int4")];
    let mut decoder = CopyBinaryDecoder::new(columns, limits(1000, DEFAULT_BATCH_BYTES.get()))
        .expect("a decoder");
    let mut rest: &[u8] = &bytes;
    let first = decoder
        .decode(&mut rest)
        .expect("the large row")
        .expect("the 64 MiB cap flushes after one row");
    assert_eq!(first.num_rows(), 1);
    assert_eq!(first.column(0).as_binary::<i32>().value(0).len(), 64 << 20);
    assert!(
        !rest.is_empty(),
        "the rest of the chunk stays with the caller"
    );
    assert!(decoder.decode(&mut rest).expect("small rows").is_some());
    assert!(rest.is_empty());
    decoder.finish().expect("complete");
    let small = vec![vec![Some(vec![9; 100])], vec![Some(vec![9; 100])]];
    let mut decoder = CopyBinaryDecoder::new(vec![base("payload", "bytea")], limits(1000, 1000))
        .expect("a decoder");
    let mut rest: &[u8] = &stream(&small);
    assert!(decoder.decode(&mut rest).expect("below the cap").is_some());
    assert_eq!(decoder.buffered_bytes(), 0);
}

#[test]
fn batch_flushes_at_the_exact_byte_cap() {
    let bytes = stream(&int4s(&[Some(1), Some(2)]));
    let mut decoder =
        CopyBinaryDecoder::new(vec![base("n", "int4")], limits(1000, 5)).expect("a decoder");
    let mut rest: &[u8] = &bytes;
    let first = decoder
        .decode(&mut rest)
        .expect("the exact cap flushes")
        .expect("a batch on the first row");
    assert_eq!(first.num_rows(), 1);
    assert!(!rest.is_empty(), "the second row stays with the caller");
    let second = decoder
        .decode(&mut rest)
        .expect("the second row")
        .expect("a batch on the second row");
    assert_eq!(second.num_rows(), 1);
    assert!(decoder.decode(&mut rest).expect("the trailer").is_none());
    assert!(rest.is_empty());
    decoder.finish().expect("complete");
}

#[test]
fn copy_field_longer_than_postgres_max_refuses() {
    let over = i32::try_from(MAX_FIELD_BYTES + 1).expect("one past the maximum fits i32");
    for length in [i32::MAX, over] {
        let mut bytes = header_with(0, &[]);
        bytes.extend(tuple(&[Some(vec![b'a'])]));
        bytes.extend_from_slice(&1_i16.to_be_bytes());
        bytes.extend_from_slice(&length.to_be_bytes());
        let mut decoder = decoder(vec![base("payload", "text")]);
        let mut rest: &[u8] = &bytes;
        let error = decoder
            .decode(&mut rest)
            .expect_err("a field past the maximum refuses");
        assert_eq!(
            error,
            protocol(ProtocolViolation::FieldTooLong {
                length: usize::try_from(length).expect("a refused length is positive"),
                max: MAX_FIELD_BYTES,
            })
        );
    }
}

#[test]
fn copy_field_at_postgres_max_is_accepted() {
    let mut bytes = header_with(0, &[]);
    bytes.extend_from_slice(&1_i16.to_be_bytes());
    let length = i32::try_from(MAX_FIELD_BYTES).expect("the maximum fits i32");
    bytes.extend_from_slice(&length.to_be_bytes());
    let mut decoder = decoder(vec![base("payload", "text")]);
    let mut rest: &[u8] = &bytes;
    assert!(
        decoder
            .decode(&mut rest)
            .expect("a field at the maximum passes the length check")
            .is_none()
    );
    assert!(rest.is_empty());
}

#[test]
fn batch_byte_cap_saturates_at_max_batch_bytes() {
    assert_eq!(BatchLimits::default().bytes(), DEFAULT_BATCH_BYTES);
    assert_eq!(limits(8192, usize::MAX).bytes().get(), MAX_BATCH_BYTES);
    assert_eq!(limits(8192, MAX_BATCH_BYTES).bytes().get(), MAX_BATCH_BYTES);
    assert_eq!(limits(8192, 64).bytes().get(), 64);
}

#[test]
fn one_column_never_outgrows_arrow_i32_offsets() {
    let worst = MAX_BATCH_BYTES
        .checked_add(MAX_FIELD_BYTES)
        .expect("the two bounds add without overflow");
    let arrow_offset_limit = usize::try_from(i32::MAX).expect("i32::MAX fits usize");
    assert!(
        worst <= arrow_offset_limit,
        "a batch at the cap plus one maximal field is {worst} bytes, past {arrow_offset_limit}"
    );
}

#[test]
fn carry_releases_capacity_past_the_byte_cap() {
    let field = vec![b'a'; 64 << 20];
    let mut bytes = header_with(0, &[]);
    bytes.extend(tuple(&[Some(field)]));
    bytes.extend(tuple(&[Some(vec![b'b'])]));
    bytes.extend_from_slice(&(-1_i16).to_be_bytes());
    let cap = 1 << 20;
    let mut decoder =
        CopyBinaryDecoder::new(vec![base("payload", "text")], limits(usize::MAX, cap))
            .expect("a decoder");
    let chunks: Vec<&[u8]> = bytes.chunks(64 << 10).collect();
    let batches = feed(&mut decoder, &chunks).expect("two rows");
    assert_eq!(batches.len(), 2);
    assert!(
        decoder.buffered_bytes() < cap,
        "buffered {} stays under the 1 MiB cap",
        decoder.buffered_bytes()
    );
}

#[test]
fn null_rows_charge_their_builder_bytes() {
    let rows = 1_000_000;
    let one = tuple(&[None, None, None]);
    let mut bytes = header_with(0, &[]);
    for _ in 0..rows {
        bytes.extend_from_slice(&one);
    }
    bytes.extend_from_slice(&(-1_i16).to_be_bytes());
    let columns = vec![
        base("n", "numeric"),
        base("t", "timestamp"),
        base("s", "text"),
    ];
    let mut decoder =
        CopyBinaryDecoder::new(columns, limits(usize::MAX, 1 << 20)).expect("a decoder");
    let mut rest: &[u8] = &bytes;
    let mut batches = 0;
    while !rest.is_empty() {
        if decoder.decode(&mut rest).expect("decode").is_some() {
            batches += 1;
        }
    }
    decoder.finish().expect("complete");
    assert!(
        batches >= 2,
        "one million all-NULL rows flush more than once, got {batches}"
    );
}
