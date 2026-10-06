# Unit ledger — C-2 · the Postgres read path (COPY BINARY decode, the type map, pushdown, mounted providers)

**Date:** 2026-10-06 · **Branch:** `feat/c-2-postgres-read` · **Base:** `09704cce` (the C-2 design
sketch merged under the slices) · **Model:** Claude Opus 5.5 (`claude-opus-5-5`, high) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Order:** [c-2-postgres-read.md](../../wo/c-2-postgres-read.md), grade B, R-1…R-7. **Sketch:**
[c-2-design.md](../../wo/c-2-design.md) (PR #964; the orchestrator's default go for C-2a at about
09:30 on 2026-10-06, the owner not objecting; the six §9 questions acted on under their leans).
**Standing defaults:** [the North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md)
NS-1…NS-19. **Card:** 1.6 of [the design plan](../../roadmap/epic-term/roadmap-design-plan-2026-08-29.md).

**This ledger opens with C-2a**, the pure slice of sketch §7: no driver, no network, no new
dependency. C-2b (the connection), C-2c (provider, pushdown, EXPLAIN) and C-2d (the mount, both
stubs, the Python door) append their clauses, mutations and gates here.

## PROPOSITION LEDGER — C-2a — 2026-10-06

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | One error enum (NS-15, sketch §2.2): `repark_connect::ConnectError` (`thiserror`) and `Result<T>`, re-exported at the crate root. C-1's `SettingsError` and `TypeMapError` fold into it with their six variants' names, fields and message text unchanged; reasons are enums (`ValueRefusal`, `ProtocolViolation`), never strings a caller matches. The fold into `repark_common::Error` is by class: `InvalidAuthMethod` → `Config`; `DeclaredAuthMethod`, `Declared`, `UnmappedType`, `UnrepresentableValue`, `EncodeNotBuilt` → `NotImplemented`; the operational variants → `DataFusion`. | C-1's settings and type pins green with only the type name changed; `decode_errors_fold_by_class`; `unmapped_types_refuse_at_resolution` (class). | PROVEN | §5 gates: `cargo test -p repark-connect` green. The C-1 tests differ from `origin/main` only in the imported type name (`git diff origin/main -- crates/repark-connect/tests/it/settings.rs` is the rename). |
| C-002 | The COPY BINARY header (sketch §2.6, docs COPY "Binary Format"): the 11-byte signature `50 47 43 4f 50 59 0a ff 0d 0a 00` is checked byte for byte; flag bit 16 (OIDs) refuses with `ProtocolViolation::OidColumns`; any of bits 17–31 refuses with `CriticalFlags`; bits 0–15 are ignored; the extension is skipped, a negative extension length refuses. | `copy_header_is_the_signature_flags_and_extension`, `copy_oid_flag_refuses`; mutations m1, m2. | PROVEN | §1. |
| C-003 | The trailer `ff ff` ends the stream; any byte after it, in the same chunk or a later one, refuses with `TrailingBytes`; a stream that ends anywhere before the trailer (in the header, mid-tuple, between tuples) is `ConnectError::Disconnected` at `finish()`, and nothing decoded so far is returned. | `copy_trailer_ends_and_trailing_bytes_refuse`, `copy_truncated_stream_is_disconnected`; mutations m3, m4. | PROVEN | §1. |
| C-004 | The decode is independent of chunking and allocates no value per row: a field or fixed header word wholly inside a chunk decodes from a borrowed slice; only one that straddles a chunk boundary copies into the decoder's one reusable carry buffer. One three-tuple stream carrying every mapping and an all-NULL tuple decodes to the same batch when split at every byte offset, and in one-byte and three-byte chunks. | `copy_decode_is_independent_of_chunking`; mutation m5. | PROVEN | §1. |
| C-005 | Each tuple's field count must equal the planned column count, else `Protocol(FieldCount)` (negative counts other than the trailer included); an empty projection counts rows from field count `0`; a negative field length other than `-1` refuses; a NULL in a planned NOT NULL column refuses; a refused value names the column, the Postgres type and the tuple index in the stream. | `copy_field_count_must_match_projection`, `copy_fields_refuse_bad_lengths_nulls_and_values_naming_the_column`; mutation m6. | PROVEN | §1. |
| C-006 | `decode(&mut chunk)` returns at most one batch and leaves the unconsumed rest of the chunk with the caller; a batch flushes at `BatchLimits::rows` (default 8192) or at `bytes` of builder growth (default 64 MiB), whichever comes first; a tuple is never split, so a value larger than the cap forms a one-row batch; `buffered_bytes()` is the memory-charge seam for the C-2c scan's `MemoryReservation`. | `batches_flush_at_rows_and_at_bytes`; mutation m16. | PROVEN | §1. |
| C-007 | `date` → `Date32` = wire + 10957, checked; the four §2.7 anchors decode exactly; `infinity` / `-infinity` (`0x7FFFFFFF` / `0x80000000`) refuse per value with `ValueRefusal::InfiniteDate` naming `CONNECT-DECL-pg-infinite-datetime`; a value past `Date32` (no finite Postgres date reaches it) refuses as `DateOutOfRange`. | `date_anchors_round_trip`; mutation m7. | PROVEN | §1. |
| C-008 | `timestamp` → `Timestamp(Microsecond, None)` (the wall clock) and `timestamptz` → `Timestamp(Microsecond, "+00:00")` (the instant; iceberg-rust's `UTC_TIME_ZONE` label) = wire + 946 684 800 000 000 µs, checked; both §2.7 anchors decode exactly, and so does the last representable value; one µs further (after 294247-01-10) refuses `TimestampOutOfRange` (`CONNECT-DECL-pg-out-of-range`); `±infinity` refuses `InfiniteTimestamp`. | `timestamp_ntz_anchors_round_trip`, `timestamptz_anchors_round_trip`; mutation m8. | PROVEN | §1. The localisation of `timestamp` into the session zone (Spark's default `TimestampType`) is C-2c's `WallClockLocaliser`; C-2a decodes the wall clock. |
| C-009 | `numeric` (sketch §2.7 rows 1–4): the modifier resolves `(p,s)` with `1 ≤ p ≤ 38` to `Decimal128(p,s)`, `p > 38` to `Decimal128(38, min(s,38))`, none to `Decimal128(38,18)`, and refuses the column for `s < 0` or `s > p`; the codec reads `numeric_send`'s base-10000 digits into an `i128` at the planned scale with checked arithmetic; fractional digits beyond the scale round HALF_UP (away from zero); a magnitude at or above `10^p` refuses `NumericOutOfRange`; `NaN` and `±Infinity` refuse `NumericNaN` / `NumericInfinity` (`CONNECT-DECL-pg-numeric-special`); unknown sign words and out-of-range digits are malformed. The anchors `12345.678` (`numeric(8,3)`) and `-0.5` (`numeric(2,1)`) decode exactly, as do the 38-digit maximum and scale padding. | `numeric_anchors_round_trip`, `numeric_typmods_resolve_to_spark_decimal_types`, `numeric_special_values_refuse`, `unconstrained_numeric_rounds_half_up_at_scale_18`, `bounded_numeric_overflow_refuses`; mutations m9, m10, m11, m12. | PROVEN | §1. H-ARROW did not fire: arrow-rs builds every `Decimal128(p,s)` with `1 ≤ p ≤ 38`, `0 ≤ s ≤ p`, including `(38,38)`. |
| C-010 | The text-rendered types: `uuid` → `Utf8`, the 16 bytes as lowercase `8-4-4-4-12`; `json` → the stored text byte for byte, UTF-8 validated; `jsonb` → version byte `01` stripped, any other version (or none) a malformed stream; `interval` → `ServerText`, the server's own text verbatim (the scan casts it to `pg_catalog.text`, C-2b); an enum reads as its label. No text is trimmed or replaced. | `uuid_renders_lowercase_canonical`, `jsonb_strips_version_one_and_refuses_others`, `json_and_interval_are_text_verbatim`, `unmapped_types_refuse_at_resolution` (enum); mutations m13, m14, m15. | PROVEN | §1. |
| C-011 | Resolution and the declared remainder: `PlannedColumn::resolve` refuses with `ConnectError::UnmappedType`, naming the column, the type, the row and the fix, a base type outside the table (arrays included), any class other than base and enum, and a declared row (`time`, under `CONNECT-DECL-pg-time`); `time` stays declared because D-M2 is deferred (§3); encoding a C-2a mapping answers `EncodeNotBuilt`, naming the write path (C-4). | `unmapped_types_refuse_at_resolution`, `declared_types_refuse_naming_their_row`, `new_mappings_wait_for_the_write_path_to_encode`; mutation m19. | PROVEN | §1. |
| C-012 | C-1's ten round trips run through the appender: `PostgresTypeRow::decode` is a thin wrapper over `PlannedColumn::decode` → `ColumnAppender`, the same dispatch the stream uses, and every C-1 pin stays green unchanged. | The ten `<type>_round_trips`, `wire_values_of_the_wrong_length_refuse`, `text_refuses_invalid_utf8`; C-1's m1 and m2 replayed as m17, m18. | PROVEN | §1. |
| C-013 | The registry (sketch §4): eight `CONNECT-DECL-pg-*` rows retire per the registry's §6 (`-numeric`, `-date`, `-timestamp`, `-timestamptz`, `-interval`, `-uuid`, `-json`, `-jsonb`), each naming the pin that replaces it; four rows are added and dated 2026-10-06 (`CONNECT-DECL-pg-numeric-special`, `-pg-infinite-datetime`, `-pg-out-of-range`, `-pg-unmapped`), each with the four-field shape and a live pin; the kept rows (`-pg-time`, the two auth rows) name `ConnectError`. The retired rows' pin went RED on purpose. The R-7 citations (ConnectorX, ADBC) are §4. | The rows; `python3 scripts/check_docs_links.py`; every cited pin is a `fn` in `crates/repark-connect/tests/it/`; the C-1 declared list run against the new table. | PROVEN | §1 m20: C-1's nine-row declared list against the C-2a table is red at `postgres_types.rs:229` (the table now declares `time` alone). |
| C-014 | The C-2a file list and ceilings of sketch §7 hold; `Cargo.toml` and `Cargo.lock` are unchanged (no dependency added); `#![forbid(unsafe_code)]` rides the workspace lints; no code comments; every gate of sketch §7 is green. | §5. | PROVEN | §5. |

## 1. Mutations

Each mutation was applied to the committed tree alone, the crate's integration binary run, and
the tree restored (C-1's practice). "Red" names the pin and the first failing line.

| id | clause | mutation (file) | red? | red in |
|---|---|---|---|---|
| m1 | C-002 | accept a 10-byte signature (`src/copy_binary.rs`) | RED | `copy_header_is_the_signature_flags_and_extension` at `copy_binary.rs:118` |
| m2 | C-002 | ignore bit 16 (`src/copy_binary.rs`) | RED | `copy_oid_flag_refuses` at `copy_binary.rs:131` |
| m3 | C-003 | stop at the trailer without checking the rest (`src/copy_binary.rs`) | RED | `copy_trailer_ends_and_trailing_bytes_refuse` at `copy_binary.rs:156` |
| m4 | C-003 | return what was decoded so far (`src/copy_binary.rs`) | RED | `copy_truncated_stream_is_disconnected` at `copy_binary.rs:180` |
| m5 | C-004 | copy a straddling field without its carry (`src/copy_binary.rs`) | RED | `copy_decode_is_independent_of_chunking` at `copy_binary.rs:292` |
| m6 | C-005 | skip the field-count check (`src/copy_binary.rs`) | RED | `copy_field_count_must_match_projection` at `copy_binary.rs:311` |
| m7 | C-007 | epoch shift 10957 -> 10958 (`src/types/postgres/temporal.rs`) | RED | `date_anchors_round_trip` at `postgres_types.rs:388` |
| m8 | C-008 | unchecked timestamp add (`src/types/postgres/temporal.rs`) | RED | `timestamp_ntz_anchors_round_trip` at `postgres_types.rs:433`; `timestamptz_anchors_round_trip` at `postgres_types.rs:433` |
| m9 | C-009 | digit base 10000 -> 1000 (`src/types/postgres/numeric.rs`) | RED | `bounded_numeric_overflow_refuses` at `postgres_types.rs:590`; `numeric_anchors_round_trip` at `postgres_types.rs:469`; `numeric_typmods_resolve_to_spark_decimal_types` at `postgres_types.rs:538`; `unconstrained_numeric_rounds_half_up_at_scale_18` at `postgres_types.rs:576` |
| m10 | C-009 | treat 0xC000 as negative (`src/types/postgres/numeric.rs`) | RED | `copy_fields_refuse_bad_lengths_nulls_and_values_naming_the_column` at `copy_binary.rs:361`; `decode_errors_fold_by_class` at `postgres_types.rs:777`; `numeric_special_values_refuse` at `postgres_types.rs:545` |
| m11 | C-009 | round half-even (`src/types/postgres/numeric.rs`) | RED | `unconstrained_numeric_rounds_half_up_at_scale_18` at `postgres_types.rs:576` |
| m12 | C-009 | wrap on overflow (`src/types/postgres/numeric.rs`) | RED | `bounded_numeric_overflow_refuses` at `postgres_types.rs:590` |
| m13 | C-010 | uppercase hex (`src/types/postgres/text_like.rs`) | RED | `copy_decode_is_independent_of_chunking` at `copy_binary.rs:279`; `uuid_renders_lowercase_canonical` at `postgres_types.rs:623` |
| m14 | C-010 | skip the jsonb version byte without checking it (`src/types/postgres/text_like.rs`) | RED | `jsonb_strips_version_one_and_refuses_others` at `postgres_types.rs:657` |
| m15 | C-010 | trim whitespace (`src/types/postgres.rs`) | RED | `bpchar_round_trips` at `postgres_types.rs:38`; `json_and_interval_are_text_verbatim` at `postgres_types.rs:689`; `text_round_trips` at `postgres_types.rs:38`; `varchar_round_trips` at `postgres_types.rs:38` |
| m16 | C-006 | flush on rows only (`src/copy_binary.rs`) | RED | `batches_flush_at_rows_and_at_bytes` at `copy_binary.rs:425` |
| m17 | C-012 | C-1 m1 replayed: bool decode as == 1 (`src/types/postgres.rs`) | RED | `bool_round_trips` at `postgres_types.rs:50` |
| m18 | C-012 | C-1 m2 replayed: int4 little-endian (`src/types/postgres.rs`) | RED | `batches_flush_at_rows_and_at_bytes` at `copy_binary.rs:406`; `copy_header_is_the_signature_flags_and_extension` at `copy_binary.rs:114`; `int4_round_trips` at `postgres_types.rs:38` |
| m19 | C-011 | map types outside the table as text (`src/types/postgres.rs`) | RED | `unmapped_types_refuse_at_resolution` at `postgres_types.rs:717` |
| m20 | C-013 | C-1 declared list against the C-2a table (`tests/it/postgres_types.rs`) | RED | `declared_types_refuse_naming_their_row` at `postgres_types.rs:229` |

All twenty are red; none survived. **m3's first encoding** (`if false` around the trailing-byte
check) made `decode` return without consuming its chunk, so the test's feed loop spun until the
run was killed: a harness hang, not a survivor. Two changes followed: the test feed now asserts
progress and that a `None` return means the chunk is exhausted (the decoder's contract, so a
non-consuming decoder is red instead of hung), and m3 was re-encoded as the realistic fault,
swallowing the bytes after the trailer. The table is the final run against the committed tree.

## 2. Four-line records (2026-10-06)

The C-2 sketch's FL-1…FL-14 (§3 there), copied as dated rows of this unit, each acted on under its
default. The question, Flink's answer (the JDBC and CDC connectors), Spark's answer (the JDBC data
source), and the North Star default. The engine behaviours are cited as documented; D-M2 measures
the Spark halves (§3).

| id | date | question | Flink | Spark | default acted on |
|---|---|---|---|---|---|
| FL-1 | 2026-10-06 | When does a malformed source entry refuse, and do unknown keys refuse? | Connector options are validated when the statement using the table is planned; unknown option keys fail validation. | JDBC options parse when the relation or catalog is first used; unknown keys are forwarded to the driver as connection properties. | Refuse on the source's first resolution, before any I/O; unknown keys refuse (NS §5). Session build stays I/O- and validation-free (CFG-2 D-4). Disagreement on unknown keys → `CONNECT-DIV-pg-unknown-option` (C-2b), Q6. |
| FL-2 | 2026-10-06 | Unconstrained `numeric` and `numeric(p > 38)` | `DECIMAL(38,18)` for unbounded `numeric`; HALF_UP to the scale; **null** on precision overflow. | `DecimalType.SYSTEM_DEFAULT` (38,18); `DecimalType.bounded` caps `p` at 38; HALF_UP to the scale; precision overflow **raises**. | `Decimal128(38,18)` / bounded, HALF_UP; overflow refuses per value (Spark's answer and NS-6). Taking Spark's answer changes no Spark row, so North Star §8.2 does not trigger. C-2a builds it (C-009). |
| FL-3 | 2026-10-06 | `numeric` `NaN` / `±Infinity` | pgjdbc cannot build a `BigDecimal` from them; the read fails. | Same driver, same failure. | Refuse per value, `CONNECT-DECL-pg-numeric-special`. They agree. C-2a builds it (C-009). |
| FL-4 | 2026-10-06 | `date` / `timestamp` `±infinity` | pgjdbc's infinity sentinels reach the converter; no documented mapping. | The PostgreSQL dialect special-cases infinite timestamps in recent releases; 4.1.2's mapping is D-M2's. | Refuse per value, `CONNECT-DECL-pg-infinite-datetime` (card 1.6: declare, never approximate). Q1 asks whether to switch once measured. C-2a builds it (C-007, C-008). |
| FL-5 | 2026-10-06 | `timestamp` without time zone | `TIMESTAMP` (no zone). | `TimestampType`, the wall clock placed in the JVM default zone; `TimestampNTZType` under `preferTimestampNTZ=true`. | Spark's surface: `TimestampType` via the session zone, NTZ under the option; the session-versus-JVM choice is `CONNECT-DIV-pg-timestamp-zone` (C-2c). C-2a decodes the wall clock the localiser will place. |
| FL-6 | 2026-10-06 | A column whose type is outside the map | The table refuses: "Doesn't support Postgres type". | Driver `OTHER` types read as `StringType`, the server's text. | Spark's surface for the types an oracle cell confirms (`uuid`, `json`, `jsonb`, `interval`, enums); every other type refuses at resolution, naming the column and the fix (`CONNECT-DECL-pg-unmapped`, rank 4 until measured). C-2a builds it (C-010, C-011). |
| FL-7 | 2026-10-06 | Cache the remote schema? | Resolved through the catalog when each statement is planned. | `resolveTable` runs each time a relation is created. | Resolve per statement, no cache. They agree. (C-2b.) |
| FL-8 | 2026-10-06 | List a source's schemas and tables | The JDBC catalog lists. | The JDBC table catalog lists. | Listing returns empty under `CONNECT-DECL-pg-listing`, naming the fix: DataFusion's listing hooks are synchronous and session build is I/O-free. (C-2c.) |
| FL-9 | 2026-10-06 | Push `LIMIT` | The JDBC source implements limit push-down. | `pushDownLimit` (its 4.1.2 default is D-M2's). | Push `LIMIT n` when no residual remains for the scan. They agree on the mechanism. (C-2c.) |
| FL-10 | 2026-10-06 | String comparisons and collation | Pushed against the column; the Postgres collation decides. | Compiled into the `WHERE` text; the Postgres collation decides. | The card's ruled rule outranks both: pushdown never changes semantics. Every pushed text comparison carries `COLLATE "C"`; `bpchar` stays residual; `CONNECT-DIV-pg-text-collation`, Q4. (C-2c.) |
| FL-11 | 2026-10-06 | A transient failure mid-scan | The restart strategy re-reads the split. | `local[N]` does not retry a task; cluster mode retries. | NS-8: the query fails with a retryable classification (`Disconnected` here) and nothing retries inside the scan. |
| FL-12 | 2026-10-06 | The default TLS posture | The URL goes to pgjdbc, whose default `sslmode` falls back to plaintext. | Same. | NS §5: `verify-full` by default; plaintext only by `sslmode=disable`; `CONNECT-DIV-pg-sslmode`, Q5. (C-2b.) |
| FL-13 | 2026-10-06 | Literal values in pushed predicates | Bound through a prepared statement. | Compiled into the SQL text. | NS §5: values are bound — inside COPY, a transaction-local `set_config` with bound parameters read back by `current_setting(…)::type`. Q2. (C-2b, C-2c.) |
| FL-14 | 2026-10-06 | Identifier case | Quoted as given. | The JDBC table catalog quotes the identifier as given. | Match exactly what the door hands over; D-M2 adds a mixed-case table cell. (C-2b.) |

**New in C-2a.** No question arose that a ruled row, the sketch or FL-1…FL-14 did not settle; the
choices below are placements inside the sketch's allowance, recorded as executor readings, not
four-line records.

## 3. Executor readings and deferred measurements (no halt)

- **D-M2 is deferred; `time` stays declared.** The oracle environment (`/tmp/sparkenv`) carries
  no PostgreSQL JDBC driver: no `postgresql-*.jar` under `pyspark/jars`, the Ivy cache or anywhere
  on the box, and loading one would mean a network fetch the slice does not make. So the Spark
  4.1.2 cells of sketch §8 (the types of `time`, unconstrained `numeric`, `numeric(50,10)`,
  `timestamp` in both modes, `timestamptz`, `interval`, `uuid`, `json`, `jsonb`, an enum and a
  domain; the infinities, `NaN`, the DST wall clocks, a mixed-case table, `pushDownLimit`, unknown
  options) are **deferred** to the unit that installs pgjdbc into the oracle environment. Every
  Spark half in the registry rows stays "documented; no value claim". `CONNECT-DECL-pg-time` is
  kept as C-1 had it (sketch §2.7: the row flips only on the measurement). H-ORACLE did not fire:
  nothing was measured.
- **The memory charge is a seam, not a dependency.** Sketch §7 lets C-2a add DataFusion's
  memory types "if the memory charge needs them; otherwise nothing". The decoder reports
  `buffered_bytes()` (the builder bytes of the batch in progress) and flushes at the byte cap;
  the C-2c scan, which owns the `MemoryReservation`, resizes it to that number after each chunk.
  So C-2a adds no dependency and `Cargo.lock` is unchanged.
- **No source name in C-2a's errors.** Sketch §2.2 lists `source` on the operational variants.
  The decoder is pure and is built from planned columns, not from a source, so C-2a's
  `UnrepresentableValue`, `Protocol` and `Disconnected` carry the column, the type and the row
  index but no source. C-2b, which builds the scan from a mounted source, attaches the source
  when it adds the connection variants (it owns that error surface).
- **Encode for the new mappings.** The write path is C-4. C-2a's mappings answer
  `ConnectError::EncodeNotBuilt` ("the write path (C-4) adds it") rather than a half-built
  encoder; no user surface reaches `encode`, so no registry row is filed. `interval`'s `ServerText`
  mapping could not encode in any case: its wire is the server's text, not `interval_send`.
- **`PostgresMapping` carries the decimal target.** `Numeric(DecimalTarget)` keeps one enum for
  the table and the planned column: the row holds the unconstrained target and resolution
  replaces it from the modifier. `ColumnAppender` is one variant per mapping, as the sketch
  names it. The row wrapper (`PostgresTypeRow::decode`) names the column after its type.
- **The byte estimate.** `ColumnAppender::append` returns the wire length (at least one byte)
  plus four offset bytes, or 40 for a rendered `uuid`: an estimate of builder growth that bounds
  the batch, not an exact accounting. The 64 MiB pin fills the cap with one value.
- **Gate shape.** `cargo clippy -p repark-connect --all-targets -- -D warnings` as written reds
  on `clippy::disallowed_methods` (`expect` in tests), including C-1's untouched settings tests
  on `origin/main`: `clippy.toml`'s list applies to every target, and `make rust-clippy` passes
  `-A clippy::disallowed_methods` for that reason while `make rust-panic-ban` holds the list on
  `--lib --bins`. The gate is run as the Makefile runs it (§5), plus the lib-only form with the
  list live.

## 4. R-7 citations (ConnectorX and ADBC)

- **ConnectorX shaped the read path.** Its PostgreSQL source reads `COPY (query) TO STDOUT WITH
  BINARY` through the rust-postgres family and writes each value into a typed Arrow destination
  column. C-2 keeps the protocol and the typed destination. It replaces ConnectorX's per-row
  parse through the driver's `BinaryCopyOutRow` with per-column appenders over the raw stream
  (`ColumnAppender` over `CopyBinaryDecoder`, C-004), so no value allocates. ConnectorX's
  partitioned reads (a partition column split by a min/max query) are C-3's, and its `read_sql`
  is D-M4's and C-3's benchmark bar. **Where C-2 departs:** ConnectorX renders values into the
  query text, and C-2 binds them (sketch §2.4, C-2b).
- **Arrow ADBC shaped the type contract and the error discipline.** C-1's Arrow types came from
  the ADBC PostgreSQL driver's documented type-mapping table; C-2 keeps them for the ten C-1
  rows. Per its documentation, ADBC reads results through `COPY … (FORMAT binary)` and executes
  a statement with bound parameters as a prepared statement. C-2 keeps COPY for both (R-2) and
  binds through the sketch's §2.4 carriage instead. **Where C-2 departs:** for `numeric`,
  `interval` and `uuid`, ADBC's documented Arrow types (string, month-day-nano, fixed-size
  binary) are replaced by the types Spark's dialect surfaces, because Spark governs the surface
  (North Star §2) and RePark answers Spark; each departure is a sketch §2.7 row with its
  citation, built here (C-009, C-010). ADBC's cursor contract is met by the one-partition
  stream: one statement, one snapshot, batches until the trailer, a typed error otherwise
  (C-003, C-006). Both bars are their documents; nothing ran either library.

## 5. Gates

Run on the finished tree (the committed tree for the last two), each cargo command under the
build-slot lock, exit codes verbatim.

| command | exit | output |
|---|---|---|
| `cargo test -p repark-connect` | 0 | 44 passed, 0 failed (C-1's 22 plus 22 new) |
| `cargo clippy -p repark-connect --all-targets -- -D warnings` | 101 | 56 errors, every one `clippy::disallowed_methods` on an `expect` in `tests/it/` (4 of them in C-1's untouched `settings.rs` lines, so the form is red on `origin/main` too); no other lint (§3 "Gate shape") |
| `cargo clippy -p repark-connect --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 | no diagnostics |
| `make rust-clippy` | 0 | workspace, all targets, no diagnostics |
| `cargo fmt --check` | 0 | no output |
| `make rust-panic-ban` | 0 | clean (the `disallowed_methods` list live on `--lib --bins`) |
| `python3 scripts/check_rust_file_size.py` | 0 | 1049 files clean; the sketch's C-2a ceilings also hold: `error.rs` 234/260, `settings.rs` 96/160, `types/postgres.rs` 519/520, `numeric.rs` 163/300, `temporal.rs` 25/260, `text_like.rs` 43/180, `copy_binary.rs` 350/480, `lib.rs` 11/40, `tests/it/copy_binary.rs` 440/700, `tests/it/postgres_types.rs` 788/900 |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 363 maps clean |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | no output |
| `python3 scripts/check_docs_links.py` | 0 | 1336 files, 7164 links clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 303 live ledgers clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xc2a origin/main HEAD` | 0 | `comment-ban hits=0` |

The pre-commit hook (map lockstep, map sync, crate DAG, lib roots, file sizes, manifest, docs
compaction, `cargo fmt --check`, taplo, typos) passed on the commit.

```
COVERAGE_ATTESTATION:
  pr_unit: c-2a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Sketch §7's C-2a file list, ceilings and deliverables, §2.2's error shape, §2.6's state machine and §2.7's rows and anchors are each mapped to a clause; the departures (no source field yet, the memory-charge seam, EncodeNotBuilt) are recorded in §3.
      artifacts: [crates/repark-connect/src/error.rs, crates/repark-connect/src/copy_binary.rs, crates/repark-connect/src/types/postgres.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Every byte offset as a chunk boundary; one- and three-byte chunks; empty projection; all-NULL tuple; signature bytes each flipped; OID and critical flag bits; negative extension, field count and field length; trailer followed by a byte in the same and the next chunk; truncation in the header, mid-tuple and before the trailer; numeric zero, 38-digit maximum and its negative, scale padding, half-up at 5 and not at 4999, rounding carry past 38 digits, absurd weight; timestamp last representable and one past; both infinities of each temporal type; jsonb with no version byte; invalid UTF-8 in every text type; a 64 MiB value.
      artifacts: [crates/repark-connect/tests/it/copy_binary.rs, crates/repark-connect/tests/it/postgres_types.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every failure is a typed ConnectError with an enum reason; values refuse per value with their registry row; a truncated stream is Disconnected rather than a partial result; checked arithmetic on every epoch shift and decimal accumulation; no unwrap, expect or panic in product code (panic-ban gate).
      artifacts: [crates/repark-connect/src/error.rs, crates/repark-connect/src/types/postgres/numeric.rs, crates/repark-connect/src/types/postgres/temporal.rs]
    - id: AT-4
      status: N/A
      justification: The decoder is a single-owner value with no shared state, no locks and no async; the stream that drives it arrives in C-2b.
    - id: AT-5
      status: ATTACKED
      evidence: Errors carry column names, type names and row indices, never a value or a prop; ConnectionSettings keeps C-1's keys-only Debug (its pin unchanged); no I/O and no SQL in the slice.
      artifacts: [crates/repark-connect/src/error.rs, crates/repark-connect/tests/it/settings.rs]
    - id: AT-6
      status: ATTACKED
      evidence: No value is approximated: NaN, infinities and out-of-range values refuse with a registry row; the only rounding is HALF_UP beyond the planned scale, Spark's and Postgres's own; text is never trimmed or replaced; jsonb versions other than 1 refuse.
      artifacts: [crates/repark-connect/tests/it/postgres_types.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Fields inside a chunk decode from borrowed slices into Arrow builders; one carry buffer is reused across straddling fields; uuid renders into a stack buffer; batches are bounded by rows and bytes, and the decoder returns at most one batch per call.
      artifacts: [crates/repark-connect/src/copy_binary.rs, crates/repark-connect/src/types/postgres/text_like.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency added; Cargo.toml and Cargo.lock unchanged; the crate keeps its one connect to common edge; unsafe stays forbidden by the workspace lints.
      artifacts: [crates/repark-connect/Cargo.toml]
    - id: AT-9
      status: ATTACKED
      evidence: Refusals name the column, the type, the registry row and the registry path; UnmappedType names the fix; EncodeNotBuilt names C-4; ProtocolViolation renders which wire check failed.
      artifacts: [crates/repark-connect/src/error.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Twenty mutations, each red in its named pin and the tree restored; the declared-list pin red on purpose against the new table; the pin-liveness test reads the test file so a row cannot cite a missing pin.
      artifacts: [crates/repark-connect/tests/it/postgres_types.rs, crates/repark-connect/tests/it/copy_binary.rs]
  complete: true
```
