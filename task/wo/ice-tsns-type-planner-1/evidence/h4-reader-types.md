# H4 measurements: Arrow type of a timestamp column per reader

Release module of main on the unit branch (`ebe53487`), Spark door unless
noted. Each row ran against a fresh local session and warehouse; the probe
scripts are scratch (uncommitted). Types below are the read-back Arrow field
types from `to_arrow().schema`.

| # | Reader | Statement / file | Arrow type on main |
|---|---|---|---|
| 1 | CSV, no `inferSchema` | `read_csv` over `ts` + two `2024-…` walls | `string` |
| 2 | CSV, `inferSchema=true`, no header | same file | `string` |
| 3 | CSV, `inferSchema=true`, `header=true` | same file | `timestamp[s]` |
| 4 | JSON | `read_json` over `{"ts": "2024-…"}` lines | `string` |
| 5 | Parquet, `pa.timestamp("us")` column | `read_parquet` | `timestamp[us]` |
| 6 | Parquet, `pa.timestamp("ns")` column | `read_parquet` | `timestamp[ns]` |
| 7 | Parquet, `pa.timestamp("ns", tz="UTC")` column | `read_parquet` | `timestamp[ns, tz=UTC]` |
| 8 | Iceberg v2, `TIMESTAMP` column | `CREATE TABLE … (ts TIMESTAMP) USING iceberg`, insert one literal, `SELECT ts` | `timestamp[us, tz=UTC]` |
| 9 | Iceberg v3, `timestamp_ns` column | `USING iceberg TBLPROPERTIES ('format-version'='3')`, insert one `CAST(… AS timestamp_ns)`, `SELECT ts` | `timestamp[ns]` |
| 10 | Iceberg v3, `timestamptz_ns` column | same shape with `timestamptz_ns` | `timestamp[ns, tz=UTC]` |
| 11 | Postgres `timestamp` / `timestamptz` | not measured | not measured: `make pg-up` fails in this lane because no Docker daemon socket is present, so no disposable Postgres can start |

Notes:

- Creating a v3 table refuses without the session conf
  `repark.sql.allowCreateFormatVersion3=true` (default create stays v2);
  a v2 `CREATE TABLE` naming `timestamp_ns` refuses with `Invalid schema
  for v2`.
- No text reader infers nanoseconds: CSV without inference and JSON read
  `string`; CSV with inference reads `timestamp[s]` (seconds, not
  microseconds or nanoseconds). Parquet and Iceberg v3 keep real
  nanoseconds tick for tick.

## Reading for H4

H4 is NOT MET: no text reader infers `timestamp[ns]`. Row 3 (`timestamp[s]`
from CSV inference) is a producer the sketch does not list; it gets a new
ledger row with an empty disposition.
