use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use repark_common::{Error, ErrorClass};
use repark_connect::postgres::{PgTypeKind, PlannedColumn, TypeMod, UTC_ZONE_LABEL, WriteCarriage};
use repark_connect::{
    BatchLimits, COPY_SIGNATURE, ConnectError, CopyBinaryDecoder, CopyBinaryEncoder, PgIdent,
    Privilege, QualifiedRelation, ResolvedSource, RowFallback, ScanColumn, ScanSource,
    TARGET_FACTS, WritePath, WriteRefusal, WriteRequest, WriteValueRefusal,
};

pub(crate) const ROWS: usize = 8;
pub(crate) const MIN_DAYS: i32 = -2_440_588;
pub(crate) const MAX_DAYS: i32 = 2_145_042_905;
pub(crate) const MICROS_PER_DAY: i64 = 86_400_000_000;
const MAX_38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

pub(crate) struct Sample {
    pub(crate) name: &'static str,
    pub(crate) ddl: &'static str,
    pub(crate) typname: &'static str,
    pub(crate) kind: PgTypeKind,
    pub(crate) typmod: TypeMod,
    pub(crate) send: &'static str,
    pub(crate) values: ArrayRef,
}

fn sample(name: &'static str, ddl: &'static str, send: &'static str, values: ArrayRef) -> Sample {
    assert_eq!(values.len(), ROWS, "{name}");
    assert!(values.null_count() > 0, "{name} carries a NULL");
    Sample {
        name,
        ddl,
        typname: ddl,
        kind: PgTypeKind::Base,
        typmod: TypeMod::NONE,
        send,
        values,
    }
}

fn spread<T>(values: [T; 6]) -> Vec<Option<T>> {
    let [first, second, third, fourth, fifth, sixth] = values;
    vec![
        Some(first),
        Some(second),
        Some(third),
        Some(fourth),
        None,
        Some(fifth),
        Some(sixth),
        None,
    ]
}

fn decimals(values: [i128; 6], precision: u8, scale: i8) -> ArrayRef {
    let array = Decimal128Array::from(spread(values))
        .with_precision_and_scale(precision, scale)
        .expect("a decimal type");
    Arc::new(array)
}

fn micros(zone: Option<&str>) -> ArrayRef {
    let array = TimestampMicrosecondArray::from(spread([
        i64::from(MIN_DAYS) * MICROS_PER_DAY,
        0,
        946_684_800_000_000,
        i64::MAX,
        -1,
        253_402_300_799_999_999,
    ]));
    match zone {
        Some(zone) => Arc::new(array.with_timezone(zone)),
        None => Arc::new(array),
    }
}

fn texts(values: [&str; 6]) -> ArrayRef {
    Arc::new(StringArray::from(spread(values)))
}

fn numbers() -> Vec<Sample> {
    let payload_32 = f32::from_bits(0xffc0_1234);
    let payload_64 = f64::from_bits(0xfff8_0000_dead_beef);
    let flags = BooleanArray::from(spread([true, false, true, false, true, false]));
    let smalls = Int16Array::from(spread([i16::MIN, i16::MAX, 0, -1, 1, 7]));
    let mediums = Int32Array::from(spread([i32::MIN, i32::MAX, 0, -1, 1, 7]));
    let bigs = Int64Array::from(spread([i64::MIN, i64::MAX, 0, -1, 1, 7]));
    let reals = [0.0, -0.0, f32::from_bits(1), f32::MAX, f32::NAN, payload_32];
    let doubles = [
        0.0,
        -0.0,
        f64::from_bits(1),
        f64::INFINITY,
        f64::NAN,
        payload_64,
    ];
    let amounts = [
        0,
        1_000_000_000_000_000_000,
        MAX_38,
        -MAX_38,
        1,
        -123_456_789_012_345_678_901_234_567,
    ];
    let prices = [0, 9_999_999_999, -9_999_999_999, 1, -1, 12_345];
    let wholes = [MAX_38, -MAX_38, 0, 10_000, 100_000_000, -99_990_000];
    let parts = [
        MAX_38,
        1,
        -1,
        10_i128.pow(37),
        5 * 10_i128.pow(33),
        10_i128.pow(34),
    ];
    let constrained = |name, ddl, precision: u8, scale: i8, values| Sample {
        typname: "numeric",
        typmod: TypeMod::numeric(i32::from(precision), i32::from(scale)),
        ..sample(
            name,
            ddl,
            "numeric_send",
            decimals(values, precision, scale),
        )
    };
    vec![
        sample("flag", "bool", "boolsend", Arc::new(flags)),
        sample("small", "int2", "int2send", Arc::new(smalls)),
        sample("medium", "int4", "int4send", Arc::new(mediums)),
        sample("big", "int8", "int8send", Arc::new(bigs)),
        sample(
            "real",
            "float4",
            "float4send",
            Arc::new(Float32Array::from(spread(reals))),
        ),
        sample(
            "double",
            "float8",
            "float8send",
            Arc::new(Float64Array::from(spread(doubles))),
        ),
        sample(
            "amount",
            "numeric",
            "numeric_send",
            decimals(amounts, 38, 18),
        ),
        constrained("price", "numeric(10,2)", 10, 2, prices),
        constrained("whole", "numeric(38,0)", 38, 0, wholes),
        constrained("part", "numeric(38,38)", 38, 38, parts),
    ]
}

fn strings() -> Vec<Sample> {
    let long = "x".repeat(70_000);
    let every_byte: Vec<u8> = (0..=u8::MAX).collect();
    let bodies = [
        "",
        "é𝄞",
        "tab\tnew\nline\\ \"q\" 'q'",
        &long,
        "\u{1}\u{7f}",
        " lead trail ",
    ];
    let raws: [&[u8]; 6] = [
        &[],
        &[0],
        &every_byte,
        &[0xff; 3],
        b"\\x00",
        &COPY_SIGNATURE,
    ];
    let tokens = [
        "00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "123e4567-e89b-12d3-a456-426614174000",
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        "01234567-89ab-cdef-0123-456789abcdef",
        "fedcba98-7654-3210-fedc-ba9876543210",
    ];
    let docs = ["{\"a\": 1}", "null", "[1,  2]", "\"x\"", "1e3", " true "];
    let trees = ["{\"a\": 1}", "null", "[1, 2]", "\"x\"", "{}", "1000"];
    let moods = ["happy", "sad", "so so", "happy", "sad", "happy"];
    vec![
        sample("body", "text", "textsend", texts(bodies)),
        sample(
            "label",
            "varchar",
            "varcharsend",
            texts(["", "plain", "é𝄞", "a,b\\N", "\\.", "trail  "]),
        ),
        sample(
            "code",
            "bpchar",
            "bpcharsend",
            texts(["", "a", "ab", "é", "x y", "z"]),
        ),
        sample(
            "raw",
            "bytea",
            "byteasend",
            Arc::new(BinaryArray::from(spread(raws))),
        ),
        sample("token", "uuid", "uuid_send", texts(tokens)),
        sample("doc", "json", "json_send", texts(docs)),
        sample("tree", "jsonb", "jsonb_send", texts(trees)),
        Sample {
            typname: "mood",
            kind: PgTypeKind::Enum,
            ..sample("mood", "{schema}.mood", "enum_send", texts(moods))
        },
    ]
}

pub(crate) fn samples() -> Vec<Sample> {
    let days = [MIN_DAYS, 0, 10_957, MAX_DAYS, -719_162, 2_932_896];
    let mut samples = numbers();
    samples.extend(strings());
    samples.extend([
        sample(
            "day",
            "date",
            "date_send",
            Arc::new(Date32Array::from(spread(days))),
        ),
        sample("moment", "timestamp", "timestamp_send", micros(None)),
        sample(
            "instant",
            "timestamptz",
            "timestamptz_send",
            micros(Some(UTC_ZONE_LABEL)),
        ),
        Sample {
            typname: "int4",
            ..sample(
                "rating",
                "{schema}.rating",
                "int4send",
                Arc::new(Int32Array::from(spread([0, 5, 3, 1, 2, 4]))),
            )
        },
    ]);
    samples
}

pub(crate) fn batch_of(columns: &[(&str, ArrayRef)]) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    let arrays = columns.iter().map(|(_, array)| Arc::clone(array)).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("a batch")
}

pub(crate) fn sample_batch(samples: &[Sample]) -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = samples
        .iter()
        .map(|sample| (sample.name, Arc::clone(&sample.values)))
        .collect();
    batch_of(&columns)
}

fn planned(sample: &Sample) -> PlannedColumn {
    PlannedColumn::resolve(
        Arc::from(sample.name),
        sample.typname,
        sample.kind,
        sample.typmod,
    )
    .expect("a mapped column")
}

fn base(name: &str, typname: &str) -> PlannedColumn {
    PlannedColumn::resolve(Arc::from(name), typname, PgTypeKind::Base, TypeMod::NONE)
        .expect("a mapped column")
}

fn relation() -> QualifiedRelation {
    QualifiedRelation::new(
        PgIdent::new("s").expect("schema"),
        PgIdent::new("t \"x\"").expect("table"),
    )
}

fn target(columns: &[(&str, &str)]) -> ResolvedSource {
    let columns = columns
        .iter()
        .map(|(name, typname)| {
            let name = PgIdent::new(*name).expect("column");
            ScanColumn::resolve(name, typname, PgTypeKind::Base, TypeMod::NONE, true)
                .expect("a mapped column")
        })
        .collect();
    ResolvedSource {
        source: ScanSource::Relation(relation()),
        columns,
        server_version_num: 160_000,
        server_encoding: Arc::from("UTF8"),
    }
}

fn encoded(columns: Vec<PlannedColumn>, batches: &[RecordBatch], chunk_bytes: usize) -> Vec<u8> {
    let mut encoder = CopyBinaryEncoder::new(columns, chunk_bytes);
    let mut stream = Vec::new();
    for batch in batches {
        let mut chunks = encoder.batch(batch).expect("a matching batch");
        while let Some(chunk) = chunks.next_chunk().expect("a chunk") {
            stream.extend(chunk);
        }
    }
    stream.extend(encoder.finish());
    stream
}

fn decoded(columns: Vec<PlannedColumn>, stream: &[u8]) -> Vec<RecordBatch> {
    let limits = BatchLimits::new(NonZeroUsize::MAX, NonZeroUsize::MAX);
    let mut decoder = CopyBinaryDecoder::new(columns, limits).expect("a decoder");
    let mut rest = stream;
    let mut batches = Vec::new();
    while !rest.is_empty() {
        batches.extend(decoder.decode(&mut rest).expect("a well-formed stream"));
    }
    decoder.finish().expect("the trailer");
    batches
}

fn numeric_wire(weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> Vec<u8> {
    let count = u16::try_from(digits.len()).expect("digit count");
    let mut wire = count.to_be_bytes().to_vec();
    wire.extend_from_slice(&weight.to_be_bytes());
    wire.extend_from_slice(&sign.to_be_bytes());
    wire.extend_from_slice(&dscale.to_be_bytes());
    for digit in digits {
        wire.extend_from_slice(&digit.to_be_bytes());
    }
    wire
}

fn numeric_field(unscaled: i128, precision: u8, scale: i8) -> Vec<u8> {
    let typmod = TypeMod::numeric(i32::from(precision), i32::from(scale));
    let column = PlannedColumn::resolve(Arc::from("n"), "numeric", PgTypeKind::Base, typmod)
        .expect("a numeric column");
    let array = Decimal128Array::from(vec![unscaled])
        .with_precision_and_scale(precision, scale)
        .expect("a decimal type");
    column
        .encode(&array)
        .expect("a numeric encodes")
        .remove(0)
        .expect("a value")
}

fn unwritable(error: &ConnectError) -> (usize, WriteValueRefusal) {
    match error {
        ConnectError::UnwritableValue { index, reason, .. } => (*index, *reason),
        other => panic!("expected a refused value, got {other:?}"),
    }
}

#[test]
fn every_mapped_type_encodes_to_the_wire_form_its_decoder_reads() {
    for sample in samples() {
        let column = planned(&sample);
        let wire = column.encode(sample.values.as_ref()).expect(sample.name);
        let borrowed: Vec<Option<&[u8]>> = wire.iter().map(Option::as_deref).collect();
        let back = column.decode(&borrowed).expect(sample.name);
        assert_eq!(&back, &sample.values, "{}", sample.name);
    }
    for row in repark_connect::postgres::POSTGRES_TYPES {
        let sampled = samples()
            .iter()
            .any(|sample| sample.typname == row.postgres_name);
        let mapped = row.mapping.data_type().is_some();
        assert_eq!(
            sampled || row.postgres_name == "interval",
            mapped,
            "{} is mapped and has no sample column",
            row.postgres_name
        );
    }
    let gap = base("gap", "interval");
    let texts = StringArray::from(vec![Some("1 day"), None, Some("P1Y")]);
    let wire = gap.encode(&texts).expect("interval text");
    assert_eq!(
        wire,
        [Some(b"1 day".to_vec()), None, Some(b"P1Y".to_vec())],
        "an interval travels as the text the server parses"
    );
}

#[test]
fn numeric_encodes_as_numeric_send_writes_it() {
    let cases: [(i128, u8, i8, Vec<u8>); 12] = [
        (12_345_678, 8, 3, numeric_wire(1, 0, 3, &[1, 2345, 6780])),
        (0, 8, 3, numeric_wire(0, 0, 3, &[])),
        (-1, 8, 3, numeric_wire(-1, 0x4000, 3, &[10])),
        (10_000, 8, 0, numeric_wire(1, 0, 0, &[1])),
        (1, 8, 8, numeric_wire(-2, 0, 8, &[1])),
        (100_010_000, 12, 4, numeric_wire(1, 0, 4, &[1, 1])),
        (-5, 1, 0, numeric_wire(0, 0x4000, 0, &[5])),
        (
            MAX_38,
            38,
            0,
            numeric_wire(
                9,
                0,
                0,
                &[99, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999],
            ),
        ),
        (
            -MAX_38,
            38,
            38,
            numeric_wire(
                -1,
                0x4000,
                38,
                &[9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9900],
            ),
        ),
        (1, 38, 38, numeric_wire(-10, 0, 38, &[100])),
        (
            MAX_38,
            38,
            18,
            numeric_wire(
                4,
                0,
                18,
                &[9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9900],
            ),
        ),
        (15_000, 10, 4, numeric_wire(0, 0, 4, &[1, 5000])),
    ];
    for (unscaled, precision, scale, expected) in cases {
        assert_eq!(
            numeric_field(unscaled, precision, scale),
            expected,
            "{unscaled} at ({precision},{scale})"
        );
    }
    for scale in 0..=38_i8 {
        let column = PlannedColumn::resolve(
            Arc::from("n"),
            "numeric",
            PgTypeKind::Base,
            TypeMod::numeric(38, i32::from(scale)),
        )
        .expect("a numeric column");
        let values: Vec<i128> = (0..=38)
            .flat_map(|power| {
                let unit = 10_i128.pow(power);
                [unit, -unit, unit - 1, 1 - unit, 7 * (unit / 10)]
            })
            .filter(|value| value.abs() <= MAX_38)
            .collect();
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(38, scale)
                .expect("a decimal type"),
        );
        let wire = column.encode(array.as_ref()).expect("a grid encodes");
        let borrowed: Vec<Option<&[u8]>> = wire.iter().map(Option::as_deref).collect();
        assert_eq!(
            &column.decode(&borrowed).expect("decode"),
            &array,
            "{scale}"
        );
    }
}

#[test]
fn uuid_text_parses_as_uuid_in_parses_it() {
    let token = base("token", "uuid");
    let raw = [
        0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40,
        0x00,
    ];
    let accepted = [
        "123e4567-e89b-12d3-a456-426614174000",
        "123E4567-E89B-12D3-A456-426614174000",
        "123e4567e89b12d3a456426614174000",
        "{123e4567-e89b-12d3-a456-426614174000}",
        "{123e4567e89b12d3a456426614174000}",
        "123e-4567-e89b-12d3-a456-4266-1417-4000",
        "123e4567-e89b12d3-a456426614174000",
    ];
    let wire = token
        .encode(&StringArray::from(accepted.to_vec()))
        .expect("every form uuid_in takes");
    assert_eq!(wire, vec![Some(raw.to_vec()); accepted.len()]);
    let refused = [
        "",
        "123e4567-e89b-12d3-a456-42661417400",
        "123e4567-e89b-12d3-a456-4266141740000",
        "123e4567-e89b-12d3-a456-42661417400g",
        " 123e4567-e89b-12d3-a456-426614174000",
        "123e4567-e89b-12d3-a456-426614174000 ",
        "{123e4567-e89b-12d3-a456-426614174000",
        "123e4567-e89b-12d3-a456-426614174000}",
        "123e4567--e89b-12d3-a456-426614174000",
        "1-23e4567-e89b-12d3-a456-426614174000",
        "12-3e4567-e89b-12d3-a456-426614174000",
        "123e45-67-e89b-12d3-a456-426614174000",
        "123e4567-e89b-12d3-a456-426614174000-",
        "+23e4567-e89b-12d3-a456-426614174000",
    ];
    for text in refused {
        let array = StringArray::from(vec![
            Some("00000000-0000-0000-0000-000000000000"),
            Some(text),
        ]);
        let error = token.encode(&array).expect_err(text);
        assert_eq!(
            unwritable(&error),
            (1, WriteValueRefusal::UuidSyntax),
            "{text}"
        );
        let message = error.to_string();
        assert!(message.contains("`token`"), "{message}");
        assert!(message.contains("row 1"), "{message}");
        assert!(text.len() < 8 || !message.contains(text), "{message}");
    }
}

#[test]
fn a_date_or_timestamp_the_wire_reads_as_infinite_or_cannot_count_refuses() {
    let day = base("day", "date");
    let kept = Date32Array::from(vec![i32::MIN + 10_958, i32::MAX, 10_957]);
    let wire = day.encode(&kept).expect("the widest days the wire counts");
    assert_eq!(
        wire,
        [
            Some((i32::MIN + 1).to_be_bytes().to_vec()),
            Some((i32::MAX - 10_957).to_be_bytes().to_vec()),
            Some(0_i32.to_be_bytes().to_vec()),
        ]
    );
    for days in [i32::MIN + 10_957, i32::MIN + 10_956, i32::MIN] {
        let error = day
            .encode(&Date32Array::from(vec![0, 0, days]))
            .expect_err("a day the wire cannot carry");
        assert_eq!(unwritable(&error), (2, WriteValueRefusal::DateOutOfRange));
    }
    let epoch = 946_684_800_000_000_i64;
    for typname in ["timestamp", "timestamptz"] {
        let moment = base("moment", typname);
        let zone = (typname == "timestamptz").then_some("Europe/Paris");
        let array = |values: Vec<i64>| {
            let array = TimestampMicrosecondArray::from(values);
            match zone {
                Some(zone) => array.with_timezone(zone),
                None => array,
            }
        };
        let wire = moment
            .encode(&array(vec![i64::MIN + epoch + 1, i64::MAX, epoch]))
            .expect("the widest instants the wire counts");
        assert_eq!(
            wire,
            [
                Some((i64::MIN + 1).to_be_bytes().to_vec()),
                Some((i64::MAX - epoch).to_be_bytes().to_vec()),
                Some(0_i64.to_be_bytes().to_vec()),
            ]
        );
        for micros in [i64::MIN + epoch, i64::MIN + epoch - 1, i64::MIN] {
            let error = moment
                .encode(&array(vec![micros]))
                .expect_err("an instant the wire cannot carry");
            assert_eq!(
                unwritable(&error),
                (0, WriteValueRefusal::TimestampOutOfRange)
            );
        }
    }
    let naive = TimestampMicrosecondArray::from(vec![0]);
    let error = base("instant", "timestamptz")
        .encode(&naive)
        .expect_err("a wall clock is not an instant");
    assert!(matches!(error, ConnectError::ArrowType { .. }), "{error:?}");
    let zoned = naive.with_timezone("UTC");
    let error = base("moment", "timestamp")
        .encode(&zoned)
        .expect_err("an instant is not a wall clock");
    assert!(matches!(error, ConnectError::ArrowType { .. }), "{error:?}");
}

#[test]
fn the_copy_stream_is_the_one_the_decoder_reads_whatever_the_chunking() {
    let samples = samples();
    let columns: Vec<PlannedColumn> = samples.iter().map(planned).collect();
    let batch = sample_batch(&samples);
    let whole = encoded(columns.clone(), std::slice::from_ref(&batch), usize::MAX);
    assert!(whole.starts_with(&COPY_SIGNATURE));
    assert_eq!(
        &whole[COPY_SIGNATURE.len()..COPY_SIGNATURE.len() + 8],
        [0; 8]
    );
    assert!(whole.ends_with(&[0xff, 0xff]));
    let back = decoded(columns.clone(), &whole);
    let back = concat_batches(&back[0].schema(), &back).expect("one batch");
    assert_eq!(back.columns(), batch.columns());
    for chunk_bytes in [0, 1, 7, 64, 4096] {
        let mut encoder = CopyBinaryEncoder::new(columns.clone(), chunk_bytes);
        let mut chunks = encoder.batch(&batch).expect("a matching batch");
        let mut pieces = Vec::new();
        while let Some(chunk) = chunks.next_chunk().expect("a chunk") {
            pieces.push(chunk);
        }
        if chunk_bytes <= 64 {
            assert_eq!(pieces.len(), ROWS, "a chunk never splits a row");
        }
        pieces.push(encoder.finish());
        assert_eq!(pieces.concat(), whole, "chunk_bytes = {chunk_bytes}");
    }
    let halves = [
        batch.slice(0, 3),
        batch.slice(3, 0),
        batch.slice(3, ROWS - 3),
    ];
    assert_eq!(encoded(columns.clone(), &halves, 64), whole);
    let empty = encoded(columns.clone(), &[], 64);
    let mut header_and_trailer = COPY_SIGNATURE.to_vec();
    header_and_trailer.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff]);
    assert_eq!(empty, header_and_trailer);
    assert_eq!(encoded(columns.clone(), &[batch.slice(0, 0)], 64), empty);
    assert!(decoded(columns, &empty).is_empty());
}

#[test]
fn a_copy_tuple_is_a_field_count_then_length_prefixed_fields() {
    let columns = vec![
        base("id", "int4"),
        base("body", "text"),
        base("tree", "jsonb"),
    ];
    let batch = batch_of(&[
        ("id", Arc::new(Int32Array::from(vec![Some(7), None]))),
        ("body", Arc::new(StringArray::from(vec![None, Some("é")]))),
        ("tree", Arc::new(StringArray::from(vec![Some("[]"), None]))),
    ]);
    let stream = encoded(columns, &[batch], 1024);
    let mut expected = COPY_SIGNATURE.to_vec();
    expected.extend_from_slice(&[0; 8]);
    expected.extend_from_slice(&[0, 3, 0, 0, 0, 4, 0, 0, 0, 7]);
    expected.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]);
    expected.extend_from_slice(&[0, 0, 0, 3, 1, b'[', b']']);
    expected.extend_from_slice(&[0, 3, 0xff, 0xff, 0xff, 0xff]);
    expected.extend_from_slice(&[0, 0, 0, 2, 0xc3, 0xa9]);
    expected.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
    assert_eq!(stream, expected);
}

#[test]
fn a_refused_value_names_its_row_in_the_whole_stream() {
    let columns = vec![base("token", "uuid")];
    let good = batch_of(&[(
        "token",
        Arc::new(StringArray::from(vec![
            "00000000-0000-0000-0000-000000000000";
            5
        ])),
    )]);
    let bad = batch_of(&[(
        "token",
        Arc::new(StringArray::from(vec![
            Some("00000000-0000-0000-0000-000000000000"),
            None,
            Some("nope"),
        ])),
    )]);
    let mut encoder = CopyBinaryEncoder::new(columns, 1);
    let mut chunks = encoder.batch(&good).expect("a matching batch");
    while chunks.next_chunk().expect("a chunk").is_some() {}
    let mut chunks = encoder.batch(&bad).expect("a matching batch");
    let error = loop {
        match chunks.next_chunk() {
            Ok(Some(_)) => {}
            Ok(None) => panic!("the third row refuses"),
            Err(error) => break error,
        }
    };
    assert_eq!(unwritable(&error), (7, WriteValueRefusal::UuidSyntax));
}

#[test]
fn the_selector_falls_back_to_rows_for_a_column_copy_binary_cannot_carry() {
    let carried = target(&[("id", "int4"), ("body", "text"), ("tree", "jsonb")]);
    let request = WriteRequest::new(&carried).expect("a relation takes rows");
    assert_eq!(request.carriages(), [WriteCarriage::CopyBinary; 3]);
    assert_eq!(request.path(WritePath::Bulk), WritePath::Bulk);
    assert_eq!(request.path(WritePath::Row), WritePath::Row);
    let with_gap = target(&[("id", "int4"), ("gap", "interval")]);
    let request = WriteRequest::new(&with_gap).expect("a relation takes rows");
    assert_eq!(
        request.carriages(),
        [WriteCarriage::CopyBinary, WriteCarriage::RowText]
    );
    assert_eq!(request.path(WritePath::Bulk), WritePath::Row);
    assert_eq!(request.path(WritePath::Row), WritePath::Row);
    let without_gap = request.columns(&[0]).expect("the first column");
    assert_eq!(without_gap.path(WritePath::Bulk), WritePath::Bulk);
    let row_only: Vec<&str> = repark_connect::postgres::POSTGRES_TYPES
        .iter()
        .filter(|row| row.mapping.data_type().is_some())
        .filter(|row| base("c", row.postgres_name).carriage() == WriteCarriage::RowText)
        .map(|row| row.postgres_name)
        .collect();
    assert_eq!(row_only, ["interval"]);
}

#[test]
fn the_statements_quote_every_name_and_cast_only_the_text_carried_column() {
    let resolved = target(&[("id", "int4"), ("a \"b\"", "text"), ("gap", "interval")]);
    let request = WriteRequest::new(&resolved).expect("a relation takes rows");
    assert_eq!(request.relation(), &relation());
    assert_eq!(
        request.copy_statement(),
        "COPY \"s\".\"t \"\"x\"\"\" (\"id\", \"a \"\"b\"\"\", \"gap\") FROM STDIN (FORMAT BINARY)"
    );
    assert_eq!(
        request.insert_statement(1),
        "INSERT INTO \"s\".\"t \"\"x\"\"\" (\"id\", \"a \"\"b\"\"\", \"gap\") VALUES \
         ($1, $2, $3::pg_catalog.text::pg_catalog.interval)"
    );
    assert_eq!(
        request.insert_statement(2),
        "INSERT INTO \"s\".\"t \"\"x\"\"\" (\"id\", \"a \"\"b\"\"\", \"gap\") VALUES \
         ($1, $2, $3::pg_catalog.text::pg_catalog.interval), \
         ($4, $5, $6::pg_catalog.text::pg_catalog.interval)"
    );
    let reordered = request.clone().columns(&[2, 0]).expect("two columns");
    assert_eq!(
        reordered.copy_statement(),
        "COPY \"s\".\"t \"\"x\"\"\" (\"gap\", \"id\") FROM STDIN (FORMAT BINARY)"
    );
    assert_eq!(
        reordered.insert_statement(1),
        "INSERT INTO \"s\".\"t \"\"x\"\"\" (\"gap\", \"id\") VALUES \
         ($1::pg_catalog.text::pg_catalog.interval, $2)"
    );
    assert!(request.columns(&[0, 3]).is_none(), "no fourth column");
}

#[test]
fn a_write_that_cannot_be_planned_refuses_before_any_connection() {
    let query = ResolvedSource {
        source: ScanSource::query("SELECT 1 AS id"),
        ..target(&[("id", "int4")])
    };
    let error = WriteRequest::new(&query).expect_err("a query takes no rows");
    assert_eq!(
        error,
        ConnectError::WriteRefused {
            refusal: WriteRefusal::QueryTarget
        }
    );
    assert_eq!(
        Error::from(error).exception_class(),
        ErrorClass::IllegalArgument
    );
    let columns = vec![base("id", "int4"), base("body", "text")];
    let narrow = batch_of(&[("id", Arc::new(Int32Array::from(vec![1])))]);
    let mut encoder = CopyBinaryEncoder::new(columns.clone(), 64);
    let error = encoder.batch(&narrow).err().expect("one column of two");
    assert_eq!(
        error,
        ConnectError::WriteRefused {
            refusal: WriteRefusal::ColumnCount {
                expected: 2,
                actual: 1
            }
        }
    );
    assert_eq!(
        error.to_string(),
        "a write to a Postgres source refuses: the write names 2 columns and a batch carries 1"
    );
    assert_eq!(Error::from(error).exception_class(), ErrorClass::Analysis);
    let swapped = batch_of(&[
        ("id", Arc::new(StringArray::from(vec!["1"]))),
        ("body", Arc::new(Int32Array::from(vec![1]))),
    ]);
    let error = encoder
        .batch(&swapped)
        .err()
        .expect("the types are swapped");
    assert_eq!(
        error,
        ConnectError::ArrowType {
            postgres_name: "int4",
            expected: DataType::Int32,
            actual: DataType::Utf8,
        }
    );
    let wide = batch_of(&[(
        "id",
        Arc::new(TimestampMicrosecondArray::from(vec![0])) as ArrayRef,
    )]);
    let mut seconds = CopyBinaryEncoder::new(vec![base("at", "timestamp")], 64);
    assert!(seconds.batch(&wide).is_ok());
    let millis = batch_of(&[(
        "at",
        Arc::new(arrow::array::TimestampMillisecondArray::from(vec![0])) as ArrayRef,
    )]);
    let error = seconds.batch(&millis).err().expect("only microseconds");
    assert_eq!(
        error,
        ConnectError::ArrowType {
            postgres_name: "timestamp",
            expected: DataType::Timestamp(TimeUnit::Microsecond, None),
            actual: DataType::Timestamp(TimeUnit::Millisecond, None),
        }
    );
}

#[test]
fn write_errors_fold_by_class_and_carry_no_value() {
    let refused = base("token", "uuid")
        .encode(&StringArray::from(vec!["secret-looking-text"]))
        .expect_err("not a uuid");
    assert_eq!(
        refused.to_string(),
        "column `token` (Postgres `uuid`) at row 0 holds a text that is not a uuid, which the \
         Postgres type cannot take"
    );
    assert_eq!(Error::from(refused).exception_class(), ErrorClass::Base);
    let denied = ConnectError::PermissionDenied {
        relation: relation(),
        privilege: Privilege::Insert,
    };
    assert_eq!(
        denied.to_string(),
        "the Postgres role lacks the INSERT privilege that writing to \"s\".\"t \"\"x\"\"\" needs"
    );
    let unknown = ConnectError::CommitUnknown {
        relation: relation(),
    };
    assert_eq!(
        unknown.to_string(),
        "the wait for COMMIT of the write to \"s\".\"t \"\"x\"\"\" ended without an answer: its \
         rows are stored or absent as a whole; read the table before any retry"
    );
    assert_eq!(
        Error::from(unknown).exception_class(),
        ErrorClass::CommitStateUnknown
    );
    let no_columns = ConnectError::WriteRefused {
        refusal: WriteRefusal::NoColumns,
    };
    assert_eq!(
        Error::from(no_columns).exception_class(),
        ErrorClass::IllegalArgument
    );
}

#[test]
fn a_named_identity_or_generated_column_refuses_in_one_class_and_names_the_fix() {
    let column = String::from("n");
    let identity = ConnectError::WriteRefused {
        refusal: WriteRefusal::IdentityAlways {
            column: column.clone(),
        },
    };
    assert_eq!(
        identity.to_string(),
        "a write to a Postgres source refuses: column `n` is a GENERATED ALWAYS identity, which \
         takes no written value; leave it out of the write and the server assigns it"
    );
    let generated = ConnectError::WriteRefused {
        refusal: WriteRefusal::GeneratedColumn { column },
    };
    assert_eq!(
        generated.to_string(),
        "a write to a Postgres source refuses: column `n` is a generated column, which takes no \
         written value; leave it out of the write and the server computes it"
    );
    for refused in [identity, generated] {
        assert_eq!(Error::from(refused).exception_class(), ErrorClass::Analysis);
    }
}

#[test]
fn every_fallback_reason_says_why_copy_was_not_used() {
    let reasons = [
        (
            RowFallback::View,
            "the target is a view, which COPY cannot write",
        ),
        (
            RowFallback::ForeignTable,
            "the target is, or routes rows to, a foreign table",
        ),
        (
            RowFallback::InsertRule,
            "the target has an INSERT rule, which COPY does not fire",
        ),
        (
            RowFallback::RowSecurity,
            "row-level security applies to the role, and COPY FROM refuses under it",
        ),
        (
            RowFallback::StatementTrigger,
            "the target has a statement-level INSERT trigger, which COPY fires once",
        ),
        (
            RowFallback::ColumnType,
            "a written column's type has no COPY BINARY form here",
        ),
    ];
    for (reason, text) in reasons {
        assert_eq!(reason.to_string(), text);
    }
    assert_eq!(TARGET_FACTS.matches('$').count(), 5, "three bound values");
    for qualified in [
        "pg_catalog.pg_class",
        "pg_catalog.pg_rewrite",
        "pg_catalog.pg_trigger",
    ] {
        assert!(TARGET_FACTS.contains(qualified), "{qualified}");
    }
}
