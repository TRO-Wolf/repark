# `python/repark/tests/fixtures/parquet_int96_as_ntz_1` map

Plain Parquet directories behind `../../io/parquet_int96_as_ntz_1_oracle.json`, written by
`../../io/_record_parquet_int96_as_ntz_1.py --fixtures` (PySpark 4.1.2 in a UTC session, and pyarrow
25.0.1) and copied without `.crc` sidecars. The synthetic rows are seven ids: two 2025 instants (winter
and summer), a NULL, a pre-1970 instant with a fraction, a year-2999 instant past the nanosecond range,
and two instants either side of a New York and a Kolkata date boundary.

## Contents

- [`spark_int96/`](spark_int96) — Spark's default writer: `ts` and the nested `s.t`, `a[*]` and
  `m['k']` are `INT96`; `ntz` is `INT64` with `isAdjustedToUTC=false`.
- [`spark_micros/`](spark_micros) — the same rows with `outputTimestampType=TIMESTAMP_MICROS`, so every
  `TIMESTAMP` leaf is `INT64` adjusted to UTC.
- [`spark_mixdir/`](spark_mixdir) — one directory of two files: ids 1..3 as `INT96`, ids 4..7 as
  `TIMESTAMP_MICROS`.
- [`pyarrow_adjusted/`](pyarrow_adjusted), [`pyarrow_naive/`](pyarrow_naive) — pyarrow `INT64`
  microseconds, adjusted to UTC and not.
- [`pyarrow_int96/`](pyarrow_int96) — pyarrow with `use_deprecated_int96_timestamps`, an `INT96` leaf
  from a writer other than parquet-mr; its embedded Arrow schema names UTC, so it read as an instant
  before the fix too.
