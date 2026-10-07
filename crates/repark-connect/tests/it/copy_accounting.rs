use std::process::Command;

use repark_common::{Error, ErrorClass};
use repark_connect::postgres::PlannedColumn;
use repark_connect::{BatchLimits, ConnectError, CopyBinaryDecoder, MAX_FIELD_BYTES};

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

const LIMITED: &str = "REPARK_CONNECT_CARRY_LIMITED";
const LIMIT_KIB: usize = 512 << 10;
const REFUSAL_TEST: &str = "copy_accounting::carry_allocation_failure_is_an_error_not_an_abort";

#[test]
fn a_length_word_ending_the_chunk_reserves_nothing() {
    let first = opened_field(&[], 1, MAX_FIELD_BYTES, 0);
    let decoder = buffered(vec![base("payload", "bytea")], &first);
    assert_eq!(decoder.buffered_bytes(), 0);
}

#[test]
fn a_hostile_length_charges_only_the_bytes_received() {
    let first = opened_field(&[], 1, MAX_FIELD_BYTES, 16);
    let mut decoder = buffered(vec![base("payload", "bytea")], &first);
    assert_eq!(decoder.buffered_bytes(), 16);
    feed_unflushed(&mut decoder, &[b'a'; 100]);
    assert_eq!(decoder.buffered_bytes(), 116);
    feed_unflushed(&mut decoder, &[b'a'; 10]);
    assert_eq!(decoder.buffered_bytes(), 232);
}

#[test]
fn carry_growth_stops_at_the_declared_length() {
    let length = 4096;
    let first = opened_field(&[], 1, length, 100);
    let mut decoder = buffered(vec![base("payload", "text")], &first);
    assert_eq!(decoder.buffered_rows(), 0);
    assert_eq!(decoder.buffered_bytes(), 100);
    for expected in [1100, 2200, length] {
        feed_unflushed(&mut decoder, &[b'a'; 1000]);
        assert_eq!(decoder.buffered_bytes(), expected);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn carry_allocation_failure_is_an_error_not_an_abort() {
    if std::env::var_os(LIMITED).is_some() {
        let error = refused_field();
        assert_eq!(error, ConnectError::FieldBuffer);
        assert_eq!(Error::from(error).exception_class(), ErrorClass::Base);
        return;
    }
    let binary = std::env::current_exe().expect("the test binary");
    let script = format!("ulimit -v {LIMIT_KIB} && exec \"$0\" --exact {REFUSAL_TEST}");
    let output = Command::new("sh")
        .args(["-c", &script])
        .arg(binary)
        .env(LIMITED, "1")
        .output()
        .expect("the limited child runs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{:?}: {stdout}{stderr}",
        output.status
    );
    assert!(stdout.contains("1 passed"), "{stdout}");
}

fn refused_field() -> ConnectError {
    let first = opened_field(&[], 1, MAX_FIELD_BYTES, 0);
    let mut decoder = buffered(vec![base("payload", "bytea")], &first);
    let chunk = vec![b'a'; 1 << 20];
    for _ in 0..MAX_FIELD_BYTES / chunk.len() {
        let mut rest: &[u8] = &chunk;
        if let Err(error) = decoder.decode(&mut rest) {
            return error;
        }
    }
    panic!("a {MAX_FIELD_BYTES}-byte carry fit under a {LIMIT_KIB} KiB address-space limit");
}

#[test]
fn buffered_bytes_counts_a_mid_field_carry() {
    let length = 64;
    let rows = [vec![Some(b"abc".to_vec())]];
    let chunk = opened_field(&rows, 1, length, 10);
    let decoder = buffered(vec![base("payload", "text")], &chunk);
    assert_eq!(decoder.buffered_rows(), 1);
    let builders = 3 + OFFSET_BYTES + validity_bytes(1);
    assert_eq!(decoder.buffered_bytes(), builders + 10);
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
