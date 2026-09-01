# Charter ledger — V3-6 · remaining v3 types (binary variant, ns timestamps, unknown, column defaults)

**Date:** 2026-08-31 · **Branch:** `feat/v3-6-v3-types` · **Base:** `be2d754` (`main`) ·
**Design:** [docs/design/format-v3-track.md](../../../docs/design/format-v3-track.md) §5
(V3-6) and §6 item 4 (F-15 gates each type independently) · **Path:** STANDARD.
**risk_tier:** standard.

**Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** Format-v3 types still refuse at CREATE (`V3-GEO-1` holds geometry/geography
DECLARED and today's `VARIANT` refusal so this landing can red it). RP-3 C-009 measured
fork #233 `write_default` fill inside `DataFileWriter::write` with no engine setter.
Fork pin `33be9a0`. Shredded-Parquet variant stays DECLARED (`V3-VARIANT-SHRED-1`).
The v2-to-v3 in-place upgrade surface is out of this unit.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-V3-6-CHARTER
  agent: Actor
  action: ACTOR_BUILD charter + per-type audit (no product SQL mapping yet)
  charter_trace: C-001
  preconditions:
    - base be2d754 on feat/v3-6-v3-types: SATISFIED
    - fork pin 33be9a0: SATISFIED
    - live oracle PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0: SATISFIED
  success_condition: C-001 matrix in this ledger; pin tests cite it
  uncertainty: NONE on the measured cells below
  verdict: PROCEED
```

## Scope

| In | Out |
|---|---|
| Per-type audit at fork pin `33be9a0` (read AND write) | Geometry / geography (`V3-GEO-1`) |
| Engine consumption of types the fork actually I/O-supports | Shredded-Parquet variant (`V3-VARIANT-SHRED-1`) |
| Honest pins + fork-row filings for partial/absent I/O | v2-to-v3 in-place upgrade surface |
| Column-default consumption of fork `write_default` / `initial_default` | Fork pin bump / `[patch.crates-io]` |
| Three doors; values and Arrow types on collect/to_arrow | Inventing Spark-SQL DEFAULT syntax Spark parse-fails |

## C-001 measurement matrix (2026-08-31)

Oracle: live PySpark 4.1.2 + Iceberg 1.11.0 Hadoop catalog, Java 17. Incidental
controls included. Spark cannot produce `timestamp_ns` / `unknown` via SQL; those
cells are fixture/fork-API controlled.

### Binary variant (unshredded)

| Cell | Fork `33be9a0` | Engine today | Spark 4.1.2 + Iceberg 1.11.0 |
|---|---|---|---|
| CREATE v3 `VARIANT` | schema `Type::Variant` accepted | refuses naming VARIANT (`V3-GEO-1` pin) | CREATE succeeds; DESCRIBE `variant` |
| CREATE v2 `VARIANT` | V3-only gate | n/a (CREATE refuses the type first) | `Invalid type for v: variant is not supported until v3` |
| Parquet write | `ParquetWriterBuilder::build` `FeatureUnsupported` naming variant; no file | never reached | INSERT `parse_json` / `CAST AS VARIANT` writes Parquet |
| Arrow shape | `arrow.parquet.variant` `Struct<metadata: Binary, value: Binary>` | n/a | `struct<value: binary, metadata: binary>` + field metadata `variant: 'true'` |
| Scan / collect | reader `reject_variant_projection` | n/a | `typeof(v)=variant`; values are the two binaries |
| Shredded Parquet | DECLARED out of v1.0 (`V3-VARIANT-SHRED-1`) | n/a | not this cell — Spark wrote unshredded binaries |

Disposition: **queued fork work** against GAP_MATRIX R88 (file I/O still refused). Do not
invent a parquet-variant writer here. Pin today's CREATE/read/write refusal so a fork I/O
landing reds it. Land the `V3-VARIANT-SHRED-1` registry row now that binary vs shredded
are distinguishable.

### Nanosecond timestamps

| Cell | Fork `33be9a0` | Engine today | Spark 4.1.2 + Iceberg 1.11.0 |
|---|---|---|---|
| Schema `timestamp_ns` / `timestamptz_ns` | `PrimitiveType::TimestampNs` / `TimestamptzNs`; V3-only | SQL CREATE refuses (unmapped type) | parser `[UNSUPPORTED_DATATYPE] TIMESTAMP_NS` / `TIMESTAMPTZ_NS` SQLSTATE `0A000` |
| Parquet write | engine `append` + scan round-trip; Arrow `timestamp[ns]` / `timestamp[ns, +00:00]` | append works once the Iceberg schema exists | cannot produce the type via SQL |
| `TIMESTAMP_NTZ` on v3 | Iceberg `Timestamp` (µs) | already mapped to `PrimitiveType::Timestamp` | CREATE+INSERT+SELECT Arrow `timestamp[us]`; `typeof=timestamp_ntz` |

Disposition: **consume**. Fork I/O is green. Spark cannot spell the Iceberg type names;
ns cells are fixture-controlled. Land SQL `timestamp_ns` / `timestamptz_ns` behind the
existing v3 create opt-in (Iceberg type names, not a Spark dialect invention). Do not
change `TIMESTAMP_NTZ` (µs). Upgrade surface untouched — ns columns only on opt-in
CREATE v3, not ALTER format-version.

### `unknown`

| Cell | Fork `33be9a0` | Engine today | Spark 4.1.2 + Iceberg 1.11.0 |
|---|---|---|---|
| Schema | `PrimitiveType::Unknown`; Arrow `Null`; V3-only | SQL CREATE refuses | parser `[UNSUPPORTED_DATATYPE] UNKNOWN` SQLSTATE `0A000` |
| Parquet write | engine `append` of a Null column **commits** | n/a | cannot produce via SQL |
| Scan | `DataInvalid => cannot visit arrow data type: null` | n/a | n/a |

Disposition: **do not CREATE an unreadable table**. Pin SQL CREATE refusal. File the
scan gap against GAP_MATRIX R91 (row claimed data I/O deferred-loud; write is further
along than the row text, scan is still red). Queued fork work, not DECLARED.

### Column defaults

| Cell | Fork `33be9a0` | Engine today | Spark 4.1.2 + Iceberg 1.11.0 |
|---|---|---|---|
| `UpdateSchema.add_column_with_default` | sets **both** `initial_default` and `write_default` | `SchemaChange::AddColumn` has no default fields | n/a (catalog Java API) |
| `DataFileWriter::write` fill | fills missing top-level primitive `write_default` | engine `append` / `conform_batches` refuses `missing column` **before** the writer | n/a |
| CREATE `col STRING DEFAULT 'x'` | n/a | unmapped | `UNSUPPORTED_FEATURE.TABLE_OPERATION` … `does not support column default value` |
| `ADD COLUMNS (tag STRING DEFAULT 'x')` | n/a | ADD COLUMN option not NULL/COMMENT | `Cannot add column tag since setting default values in Spark is currently unsupported` |
| `ADD COLUMN tag STRING WITH DEFAULT 'x'` | n/a | option not supported | Spark `PARSE_SYNTAX_ERROR` near `WITH` |
| `ALTER COLUMN c SET DEFAULT` | n/a | ALTER COLUMN DEFAULT refused | `Cannot apply unknown table change: TableChange$UpdateColumnDefaultValue` |
| ADD COLUMN no default then INSERT omit | n/a | ADD COLUMN works | new column NULL on old and new rows |

Disposition: **Spark-equal SQL refuse** for DEFAULT DDL (do not invent a WITH DEFAULT
spelling Spark parse-fails). **Consume** fork fill on the write path when the schema
already carries `write_default` (catalog/API-created, or a future fork-read of
Spark-written metadata). **Consume** `initial_default` on read (fork Parquet/Avro
readers already apply it). Incidental: Iceberg Spark catalog does not implement
Spark's `UpdateColumnDefaultValue` table change.

## PROPOSITION LEDGER — V3-6 — 2026-08-31

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | **Measure first.** Per-type fork read/write at `33be9a0`, engine CREATE/ALTER today, and live Spark 4.1.2 + Iceberg 1.11.0 (incidental controls) are recorded in this ledger before any product SQL mapping. | Matrix above; pin tests. | **PROVEN** | Fork I/O pins in `crates/repark-iceberg/src/tests/v3_types.rs`; ledger-token pin in `crates/repark-spark/src/tests/v3_types.rs`. |
| C-002 | Binary variant: CREATE/read/write refuse loud naming `VARIANT` on all three doors; shredded stays DECLARED (`V3-VARIANT-SHRED-1` lands as a registry row citing a pin that distinguishes binary vs shredded). Gap filed against fork R88. | Three-door pins; registry row. | **OPEN** | Spark writes unshredded parquet variant; fork parquet I/O refuses. Product mapping waits on this pin + registry. |
| C-003 | `timestamp_ns` / `timestamptz_ns`: opt-in v3 CREATE stores the Iceberg primitive; append+scan round-trip Arrow ns types and values on all three doors. `TIMESTAMP_NTZ` stays µs. Spark parser cannot spell the Iceberg names (oracle note). | Three-door pins; red-first vs C-001 refuse. | **PROVEN** | Spark CREATE+append+SELECT ns (value+type); ANSI CREATE both names + ns value round-trip; facade CREATE+`to_arrow` ns schema — the facade has no ns write surface (SQL TIMESTAMP insert is µs and does not coerce), so its reachable assertion is the schema pin. v2 CREATE refuses. |
| C-004 | `unknown`: SQL CREATE refuses naming the type on all three doors; no table left behind. Scan gap filed against fork R91. Do not CREATE a table the scan cannot read. | Three-door refuse pins. | **OPEN** | Write commits, scan `cannot visit arrow data type: null`. |
| C-005 | Column defaults: Spark-equal refuse of ADD COLUMN / CREATE / ALTER SET DEFAULT DDL; engine write consumes fork `write_default` when the schema already has one (omitted column fills, supplied column kept); `initial_default` applied on read of files missing the column. | Refuse pins + fill/read pins. | **PROVEN** | Refuse pins on all three doors (ANSI CREATE DEFAULT red-first; ADD COLUMN/SET DEFAULT green pins; facade + Spark-door battery). Fill pin red-first vs the old "missing column" refuse; supplied-column-kept pin; `initial_default` read pin (fork reader `ColumnSource::Add`). No engine surface sets a default — the SQL refusals keep it that way. |
| C-006 | v2-to-v3 in-place upgrade surface is byte-untouched. Ns/unknown/variant types land only behind CREATE opt-in. | Identity check of ALTER format-version refuse pins. | **OPEN** | HALT if a finding requires moving the upgrade surface. |
| C-007 | Documents match the pins: registry rows, STATUS Next, maps lockstep. Geometry/geography stay `V3-GEO-1`. | Registry, STATUS, `check-map-sync`. | **OPEN** | After per-type landings. |

VERDICT: 3 PROVEN, 4 OPEN, 0 REJECTED. C-003 ns mapping and C-005 column-default consumption landed. Next: C-002/C-004 pins, then C-006/C-007 truth-up + gates.

## Sequence

1. This charter + C-001 pins (this commit).
2. C-003 ns timestamps, smallest SQL mapping that flips the red CREATE pin.
3. C-005 write_default consumption on append + Spark-equal DEFAULT refuse.
4. C-002 / C-004 honest refusal pins + registry rows.
5. C-006 / C-007 truth-up + gates.
