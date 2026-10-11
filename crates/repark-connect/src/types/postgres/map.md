# map — repark-connect/src/types/postgres

## Purpose

The per-family codecs behind the Postgres type map in [../postgres.rs](../postgres.rs) (C-2a,
2026-10-06; sketch [c-2-design.md](../../../../../task/wo/c-2-design.md) §2.7). Each codec is a
pure function from one COPY BINARY field (Postgres's `*_send` form) to an Arrow-ready value or a
typed `CodecError`; `ColumnAppender` in `../postgres.rs` dispatches to them and writes into the
Arrow builders. No codec allocates per value. Since C-4 step 1 each family also has the
inverse, from an Arrow value to the field, and `encode.rs` dispatches to those. See [../map.md](../map.md).

## Contents

- `encode.rs` — **C-4 fold 1 (2026-10-09):** `ColumnEncoder` owns its typed arrays and takes
  other encodings of the planned type's values. `plain_type` names what came with its
  encoding removed (`Dictionary` unpacked to its values, `LargeUtf8` and `Utf8View` as `Utf8`,
  `LargeBinary` and `BinaryView` as `Binary`); the type check is made on that, and Arrow's
  `cast` runs only when the given array is not already plain. A mismatch still names the
  type that came, encoding included. The engine hands out `Utf8View` and dictionaries, and a
  refusal by encoding would be a refusal of values the column can hold. pins: c-4/C-021
- `encode.rs` — **C-4 step 1 (2026-10-08).** `ColumnEncoder<'a>` (crate-private) is the write
  direction of `ColumnAppender`: one variant per wire form, each borrowing its typed Arrow
  array. `new(column, array)` refuses an array whose type is not the planned column's with
  `ConnectError::ArrowType`; a `timestamptz` column takes a microsecond timestamp with any
  zone label (the value is the instant) and refuses one with none. `append(row, &mut Vec<u8>)`
  writes the value's binary wire form with no length word and no allocation of its own; the
  COPY framer and the row lane both call it, which is why the two paths store the same bytes.
  `is_null(row)` is asked first. `WriteCarriage` (public) and `PlannedColumn::carriage`,
  `encode` and `unwritable` live here. A text-carried column (`interval`) appends its text:
  the row lane binds it as `text`.
  pins: c-4/C-001, C-003
- `numeric.rs` — **C-4 step 1:** `encode(i128, DecimalTarget, &mut Vec<u8>)`, the inverse of
  `decode`. The magnitude splits at the decimal point into twenty base-10000 groups, ten each
  side (`i128` has at most 39 digits and the scale is at most 38). The fraction is cut from
  its top digits down and its last partial group is padded on the right, so no intermediate
  passes `u128`. Leading and trailing zero groups are dropped as `numeric_send` drops them,
  the weight is nine less the first kept group's index, zero is no digit with weight 0 and a
  positive sign, and `dscale` is the Arrow scale. pins: c-4/C-002
- `temporal.rs` — **C-4 step 1:** `date_wire` and `timestamp_wire` shift to the 2000 epoch
  with checked subtraction and refuse (`WriteValueRefusal`) an overflow and the one result
  equal to `i32::MIN` / `i64::MIN`, the word Postgres reads as `-infinity`. The server would
  store that as infinity with no error. `+infinity`'s word cannot be reached by the shift.
  Every other range is the server's check (`22008`). pins: c-4/C-003
- `text_like.rs` — **C-4 step 1:** `uuid_wire(&str)` is `uuid_in`'s grammar (`string_to_uuid`):
  an optional `{`…`}` pair, sixteen pairs of hex digits of either case, an optional `-` after
  any even-numbered byte but the last, nothing before or after. `JSONB_VERSION` is shared
  with the encoder, which writes it in front of the text. pins: c-4/C-003
- `numeric.rs` — `DecimalTarget` (the planned `Decimal128(p,s)`) and its resolution from the
  `atttypmod` (`((p << 16) | (s & 0x7ff)) + 4`, the 11-bit scale sign-extended as PostgreSQL 15
  packs it): no modifier gives `Decimal128(38,18)` (Spark's `SYSTEM_DEFAULT`); a constrained
  modifier gives Spark 4.1.2's `DecimalType.boundedPreferIntegralDigits` over pgjdbc's raw
  scale (`s & 0xffff`, so every negative scale lands at `(38,38)`): effective precision
  `max(p,s)` at or under 38 gives `(max(p,s),s)`, past 38 gives `(38, max(0, s-(max(p,s)-38)))`;
  only a precision outside `1..=1000` refuses the column (D-M2 DM2-T05…T10). `decode` reads
  `ndigits`, `weight`, `sign`,
  `dscale` and the base-10000 digits (`numeric_send`) straight into an `i128` at the planned
  scale: each digit contributes `digit × 10^(4·(weight − i) + s)` with checked arithmetic, the
  first dropped decimal decides HALF_UP (away from zero, which is both Spark's
  `Decimal.set` and Postgres's `round_var`), and a magnitude at or above `10^p` refuses
  (`NumericOutOfRange`). Sign words `0xC000` (`NaN`) and `0xD000` / `0xF000` (`±Infinity`)
  refuse per value; any other unknown sign word, and a digit outside `0..10000`, is a malformed
  stream. `builder` makes the `Decimal128Builder` for a target (arrow-rs accepts every target the
  resolution produces, so H-ARROW did not fire). pins: c-2/C-009
- `temporal.rs` — `date` (`int32` days since 2000-01-01, plus `POSTGRES_EPOCH_DAYS` = 10957,
  checked) and `timestamp` / `timestamptz` (`int64` µs since 2000-01-01, plus
  `POSTGRES_EPOCH_MICROS` = 946 684 800 000 000, checked). The infinities (`i32::MAX` /
  `i32::MIN`, `i64::MAX` / `i64::MIN`, `date.h` `DATEVAL_NOEND` / `NOBEGIN`, `timestamp.h`
  `DT_NOEND` / `NOBEGIN`) refuse per value; a timestamp whose 1970-based µs overflow `i64`
  (after 294247-01-10) refuses as out of range. `UTC_ZONE_LABEL` = `"UTC"`, the label an
  Iceberg read exports for `timestamptz` (measured on main `4a643e56`), so a federated join
  compares like types (C-2d pins the join). pins: c-2/C-007, C-008
- `text_like.rs` — UTF-8-validated text (never lossy), `jsonb` (version byte `01` stripped;
  any other version, or an empty value, is a malformed stream) and `uuid` (16 bytes rendered
  into a 36-byte stack buffer as lowercase `8-4-4-4-12`, `uuid_out`'s form). pins: c-2/C-010

## Pointers

- Up: [../map.md](../map.md)
- Ledger: [c-2-ledger.md](../../../../../task/ledgers/staging/c-2-ledger.md)

## Debug

First checks: `cargo test -p repark-connect --test it postgres_types`. Escalate to:
[../map.md#debug](../map.md).
