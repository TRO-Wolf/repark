use repark_connect::postgres::PlannedColumn;
use repark_connect::{BatchLimits, CopyBinaryDecoder};

use crate::copy_binary::{Field, base, header_with, tuple};

const OFFSET_BYTES: usize = size_of::<i32>();
const INT4_BYTES: usize = size_of::<i32>();

fn validity_bytes(slots: usize) -> usize {
    slots.div_ceil(8)
}

fn opened_field(rows: &[Vec<Field>], columns: i16, length: usize, sent: usize) -> Vec<u8> {
    let mut bytes = header_with(0, &[]);
    for fields in rows {
        bytes.extend(tuple(fields));
    }
    bytes.extend_from_slice(&columns.to_be_bytes());
    let declared = i32::try_from(length).expect("the declared length fits i32");
    bytes.extend_from_slice(&declared.to_be_bytes());
    bytes.extend(std::iter::repeat_n(b'a', sent));
    bytes
}

fn buffered(columns: Vec<PlannedColumn>, chunk: &[u8]) -> CopyBinaryDecoder {
    let mut decoder = CopyBinaryDecoder::new(columns, BatchLimits::default()).expect("a decoder");
    feed_unflushed(&mut decoder, chunk);
    decoder
}

fn feed_unflushed(decoder: &mut CopyBinaryDecoder, chunk: &[u8]) {
    let mut rest = chunk;
    assert!(decoder.decode(&mut rest).expect("a valid chunk").is_none());
    assert!(rest.is_empty());
}

#[test]
fn carry_reserves_the_declared_length_on_the_first_partial_chunk() {
    let length = 4096;
    let first = opened_field(&[], 1, length, 100);
    let mut decoder = buffered(vec![base("payload", "text")], &first);
    assert_eq!(decoder.buffered_rows(), 0);
    assert_eq!(decoder.buffered_bytes(), length);
    feed_unflushed(&mut decoder, &[b'a'; 1000]);
    assert_eq!(decoder.buffered_bytes(), length);
}

#[test]
fn buffered_bytes_counts_a_mid_field_carry() {
    let length = 64;
    let rows = [vec![Some(b"abc".to_vec())]];
    let chunk = opened_field(&rows, 1, length, 10);
    let decoder = buffered(vec![base("payload", "text")], &chunk);
    assert_eq!(decoder.buffered_rows(), 1);
    let builders = 3 + OFFSET_BYTES + validity_bytes(1);
    assert_eq!(decoder.buffered_bytes(), builders + length);
}

#[test]
fn variable_width_nulls_charge_their_offset_slot() {
    let rows = 3;
    let columns = vec![base("s", "text"), base("b", "bytea"), base("n", "int4")];
    let mut bytes = header_with(0, &[]);
    for _ in 0..rows {
        bytes.extend(tuple(&[None, None, None]));
    }
    let decoder = buffered(columns, &bytes);
    assert_eq!(decoder.buffered_rows(), rows);
    let per_row = OFFSET_BYTES + OFFSET_BYTES + INT4_BYTES;
    assert_eq!(
        decoder.buffered_bytes(),
        rows * per_row + validity_bytes(rows * 3)
    );
}

#[test]
fn variable_width_values_charge_bytes_and_offset() {
    let columns = vec![base("s", "text"), base("b", "bytea"), base("n", "int4")];
    let mut bytes = header_with(0, &[]);
    bytes.extend(tuple(&[
        Some(b"abc".to_vec()),
        Some(vec![1, 2, 3, 4, 5]),
        Some(7_i32.to_be_bytes().to_vec()),
    ]));
    bytes.extend(tuple(&[
        Some(Vec::new()),
        Some(Vec::new()),
        Some(8_i32.to_be_bytes().to_vec()),
    ]));
    let decoder = buffered(columns, &bytes);
    assert_eq!(decoder.buffered_rows(), 2);
    let first = (3 + OFFSET_BYTES) + (5 + OFFSET_BYTES) + INT4_BYTES;
    let second = OFFSET_BYTES + OFFSET_BYTES + INT4_BYTES;
    assert_eq!(
        decoder.buffered_bytes(),
        first + second + validity_bytes(2 * 3)
    );
}
