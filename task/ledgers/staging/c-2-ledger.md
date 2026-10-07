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
| C-008 | `timestamp` → `Timestamp(Microsecond, None)` (the wall clock) and `timestamptz` → `Timestamp(Microsecond, "UTC")` (the instant; the label an Iceberg read exports, so a Postgres × Iceberg union or join sees one type) = wire + 946 684 800 000 000 µs, checked; both §2.7 anchors decode exactly, and so does the last representable value; one µs further (after 294247-01-10) refuses `TimestampOutOfRange` (`CONNECT-DECL-pg-out-of-range`); `±infinity` refuses `InfiniteTimestamp`. | `timestamp_ntz_anchors_round_trip`, `timestamptz_anchors_round_trip`; mutation m8. | PROVEN | §1. The localisation of `timestamp` into the session zone (Spark's default `TimestampType`) is C-2c's `WallClockLocaliser`; C-2a decodes the wall clock. Round B (2026-10-06): the zone label corrected from `+00:00` to `UTC`, measured on main `4a643e56` (an Iceberg read and a plain `TIMESTAMP` literal both export `tz=UTC`). |
| C-009 | `numeric` (sketch §2.7 rows 1–4): the modifier resolves `(p,s)` by Spark 4.1.2's `DecimalType.boundedPreferIntegralDigits` over pgjdbc's raw scale (`s & 0xffff`, so every negative scale lands at `(38,38)`): effective precision `max(p,s)` at or under 38 gives `(max(p,s),s)`, past 38 gives `(38, max(0, s-(max(p,s)-38)))`; none gives `(38,18)`; only a precision outside `1..=1000` refuses the column; the codec reads `numeric_send`'s base-10000 digits into an `i128` at the planned scale with checked arithmetic; fractional digits beyond the scale round HALF_UP (away from zero); a magnitude at or above `10^p` refuses `NumericOutOfRange`; `NaN` and `±Infinity` refuse `NumericNaN` / `NumericInfinity` (`CONNECT-DECL-pg-numeric-special`); unknown sign words and out-of-range digits are malformed. The anchors `12345.678` (`numeric(8,3)`) and `-0.5` (`numeric(2,1)`) decode exactly, as do the 38-digit maximum and scale padding. | `numeric_anchors_round_trip`, `numeric_typmods_resolve_to_spark_decimal_types`, `numeric_special_values_refuse`, `unconstrained_numeric_rounds_half_up_at_scale_18`, `bounded_numeric_overflow_refuses`; mutations m9, m10, m11, m12. | PROVEN | §1. H-ARROW did not fire: arrow-rs builds every `Decimal128(p,s)` with `1 ≤ p ≤ 38`, `0 ≤ s ≤ p`, including `(38,38)`. Round B (2026-10-06): the rule is now `boundedPreferIntegralDigits`, evidence D-M2 DM2-T05…T10; `(1001,0)` pins the kept precision guard. |
| C-010 | The text-rendered types: `uuid` → `Utf8`, the 16 bytes as lowercase `8-4-4-4-12`; `json` → the stored text byte for byte, UTF-8 validated; `jsonb` → version byte `01` stripped, any other version (or none) a malformed stream; `interval` → `ServerText`, the server's own text verbatim (the scan casts it to `pg_catalog.text`, C-2b); an enum reads as its label. No text is trimmed or replaced. | `uuid_renders_lowercase_canonical`, `jsonb_strips_version_one_and_refuses_others`, `json_and_interval_are_text_verbatim`, `unmapped_types_refuse_at_resolution` (enum); mutations m13, m14, m15. | PROVEN | §1. |
| C-011 | Resolution and the declared remainder: `PlannedColumn::resolve` refuses with `ConnectError::UnmappedType`, naming the column, the type, the row and the fix, a base type outside the table (arrays included), any class other than base and enum, and a declared row (`time`, under `CONNECT-DECL-pg-time`); `time` stays declared because D-M2 is deferred (§3); encoding a C-2a mapping answers `EncodeNotBuilt`, naming the write path (C-4). | `unmapped_types_refuse_at_resolution`, `declared_types_refuse_naming_their_row`, `new_mappings_wait_for_the_write_path_to_encode`; mutation m19. | PROVEN | §1. |
| C-012 | C-1's ten round trips run through the appender: `PostgresTypeRow::decode` is a thin wrapper over `PlannedColumn::decode` → `ColumnAppender`, the same dispatch the stream uses, and every C-1 pin stays green unchanged. | The ten `<type>_round_trips`, `wire_values_of_the_wrong_length_refuse`, `text_refuses_invalid_utf8`; C-1's m1 and m2 replayed as m17, m18. | PROVEN | §1. |
| C-013 | The registry (sketch §4): seven `CONNECT-DECL-pg-*` rows retire per the registry's §6 (`-numeric`, `-date`, `-timestamptz`, `-interval`, `-uuid`, `-json`, `-jsonb`), each naming the pin that replaces it; four rows are added and dated 2026-10-06 (`CONNECT-DECL-pg-numeric-special`, `-pg-infinite-datetime`, `-pg-out-of-range`, `-pg-unmapped`), each with the four-field shape and a live pin; the kept rows (`-pg-time`, the two auth rows) name `ConnectError`. The retired rows' pin went RED on purpose. The R-7 citations (ConnectorX, ADBC) are §4. | The rows; `python3 scripts/check_docs_links.py`; every cited pin is a `fn` in `crates/repark-connect/tests/it/`; the C-1 declared list run against the new table. | PROVEN | §1 m20: C-1's nine-row declared list against the C-2a table is red at `postgres_types.rs:229` (the table now declares `time` alone); round D (2026-10-06): `-timestamp` re-declared on D-M2 DM2-T11/T12, so seven rows stay retired. |
| C-014 | The C-2a file list and ceilings of sketch §7 hold; `Cargo.toml` and `Cargo.lock` are unchanged (no dependency added); `#![forbid(unsafe_code)]` rides the workspace lints; no code comments; every gate of sketch §7 is green. | §5. | PROVEN | §5. |
| C-015 | F-1, the offset-overflow S1 (C-2a fold, round A, 2026-10-06): no COPY BINARY stream can push a variable-width Arrow builder past `i32::MAX` values bytes. A field length past `MAX_FIELD_BYTES` (1 GiB, the largest field Postgres sends) refuses with the new `ProtocolViolation::FieldTooLong { length, max }` at the length word, before any allocation or carry; `BatchLimits::new` saturates the byte cap at `MAX_BATCH_BYTES` ((1 << 30) - 1). A batch flushes at the first tuple reaching the cap and the appender reports at least the stored bytes, so one column holds under (cap - 1) + `MAX_FIELD_BYTES` = 2^31 - 2, strictly below `i32::MAX`; the -1 sits in the batch cap. `ConnectError::WireLength` did not fit: its `expected` is an exact codec width, not a maximum. | `copy_field_longer_than_postgres_max_refuses`, `copy_field_at_postgres_max_is_accepted`, `batch_byte_cap_saturates_at_max_batch_bytes`; mutations m21, m22; the verifier's `offset_overflow.rs` returns `Err`, never panic, on both cases. | PROVEN | `cargo test -p repark-connect` green; m21, m22 red (§1); `offset_overflow.rs` at head: both cases `Err` (tail in the round-A hand-back). |
| C-016 | F-4, the carry S2 (C-2a fold, round C, 2026-10-06): the decoder's one reusable carry buffer no longer keeps a past cap-sized capacity after a straddling field completes; a capacity past `BatchLimits::bytes` is dropped for a fresh `Vec`, a smaller one stays cleared for reuse. `buffered_bytes()` reports builder bytes plus the carry capacity, so the C-2c reservation seam sees the retained allocation. | `carry_releases_capacity_past_the_byte_cap`; mutation m23; the verifier's `memory.rs` `one_huge_field_peak_and_carry_retention` reports a released carry. | PROVEN | `cargo test -p repark-connect` green; m23 red (§1); `memory.rs` tail in the round-C hand-back. |
| C-017 | F-5, the builder-charge S2 (C-2a fold, round C, 2026-10-06): the byte cap charges what the builders hold. `ColumnAppender::arrow_width` is the one width table: fixed widths (bool 1, int2 2, int4/float4/date 4, int8/float8/timestamps 8, numeric 16) charge for values and NULLs; variable values charge stored bytes plus 4 offset bytes (`jsonb` the stripped body, `uuid` the 36 rendered bytes) and NULLs the 4 offset bytes; validity charges ceil(buffered rows x columns / 8), counted in the flush test and `buffered_bytes()`. | `null_rows_charge_their_builder_bytes`; mutation m24; the verifier's `memory.rs` `null_rows_are_charged_zero_bytes` now reports flushes. | PROVEN | `cargo test -p repark-connect` green; m24 red (§1); `memory.rs` tail in the round-C hand-back. |

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
| m21 | C-015 | remove the field-length check (`src/copy_binary.rs`) | RED | `copy_field_longer_than_postgres_max_refuses` |
| m22 | C-015 | remove the byte-cap saturation (`src/copy_binary.rs`) | RED | `batch_byte_cap_saturates_at_max_batch_bytes` |
| m23 | C-016 | keep `clear()` only, never drop the carry (`src/copy_binary.rs`) | RED | `carry_releases_capacity_past_the_byte_cap` |
| m24 | C-017 | `append_null` charges 0 (`src/types/postgres.rs`) | RED | `null_rows_charge_their_builder_bytes` |

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
| FL-2 | 2026-10-06 | Unconstrained `numeric` and `numeric(p > 38)` | `DECIMAL(38,18)` for unbounded `numeric`; HALF_UP to the scale; **null** on precision overflow. | `DecimalType.SYSTEM_DEFAULT` (38,18); `DecimalType.boundedPreferIntegralDigits` keeps the integral digits (`(38, max(0, s-(p-38)))` past 38); HALF_UP to the scale; precision overflow **raises** (D-M2 DM2-T05…T10). | `Decimal128(38,18)` / integral-first bound, HALF_UP; overflow refuses per value (Spark's answer and NS-6). Taking Spark's answer changes no Spark row, so North Star §8.2 does not trigger. C-2a builds it (C-009). |
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
- **C-2b open item (2026-10-06, round D): the statement builder casts `interval` (and any
  `ServerText` mapping) to `::text`, and a pin asserts the cast.** `ServerText` decode keeps
  bytes verbatim, which is only correct for server-rendered text; a raw binary interval is
  usually valid UTF-8 and would decode silently without the cast.
- **CLOSED (2026-10-07, §13, C-089): the straddling-field peak is still about 3x the field.**
  The carry grew by `Vec` doubling from the first partial chunk, so its capacity neared twice
  the field before the builder copy (re-verify `memory.rs`, 512–1023 MiB fields). The carry now
  reserves the declared field length on the first partial chunk, and the peak is 2.00x
  (§13.2). Pin: `copy_accounting.rs::carry_reserves_the_declared_length_on_the_first_partial_chunk`.
  C-3 still owns the gated memory benchmark.
- **CLOSED (2026-10-07, §13, C-090): three accounting mutations survive.** They were
  `buffered_bytes()` without the carry (r11), a variable-width NULL charged 0 (r13), and
  variable-width values charged with no offset bytes (r15)
  (the re-verify's `mutate_rv.py`). Exact-count pins now kill each one (§13.1 m142–m144):
  `copy_accounting.rs::buffered_bytes_counts_a_mid_field_carry`,
  `::variable_width_nulls_charge_their_offset_slot` and
  `::variable_width_values_charge_bytes_and_offset`.
- **D-M1, the dependency measurement (2026-10-07, C-2b round 1a, branch
  `feat/c-2b-postgres-connection` from main `575f57ca`).** Round 1a is dependencies and the
  feature only: the root `[workspace.dependencies]` gains `tokio-postgres 0.7` (locked 0.7.18),
  `tokio-postgres-rustls 0.13` (locked 0.13.0) and `rustls-native-certs 0.8` (locked 0.8.4,
  already transitive); `repark-connect` gains `default = ["postgres"]`, the six optional driver
  deps and unconditional `datafusion` (the provider traits). `Cargo.lock` is additions-only
  (279 insertions, 4 edge rewirings, no version moved): the first attempt,
  `cargo generate-lockfile --offline`, re-resolved cached AWS crates upward and was discarded
  for a minimal `cargo metadata --offline` resolve. `cargo deny check` ends `advisories ok,
  bans ok, licenses ok, sources ok`, so H-DENY did not fire (tail verbatim below). The rustls
  feature grep at head lists `aws-lc-rs`, `aws_lc_rs`, `default`, `http1`, `http2`,
  `native-tokio`, `prefer-post-quantum`, `ring`, `rustls-native-certs`, `std`, `tls12`,
  `webpki-roots`, `webpki-tokio`: new against the base list are `default`, `webpki-roots` and
  `webpki-tokio`, all `hyper-rustls` / `tokio-rustls` feature lines matched by the `rustls
  feature` substring, none a crypto backend (one `rustls 0.23.45`, no `fips`), so H-CRYPTO did
  not fire. `cargo tree -d` is recorded below, not judged. `repark-core` carries no
  `repark-connect` edge yet, so the step-3 feature re-export is skipped; C-2d adds the edge
  with the mount.

D-M1 `cargo deny check | tail -25` (exit 0, verbatim):

```text
      │   └── tokio-postgres v0.7.18 (*)
      ├── stacker v0.1.25
      │   ├── recursive v0.1.1
      │   │   ├── datafusion-common v54.1.0 (*)
      │   │   ├── datafusion-expr v54.1.0 (*)
      │   │   ├── datafusion-optimizer v54.1.0 (*)
      │   │   ├── datafusion-physical-expr v54.1.0 (*)
      │   │   ├── datafusion-physical-optimizer v54.1.0 (*)
      │   │   ├── datafusion-sql v54.1.0 (*)
      │   │   └── sqlparser v0.62.0
      │   │       ├── datafusion v54.1.0 (*)
      │   │       ├── datafusion-common v54.1.0 (*)
      │   │       ├── datafusion-expr v54.1.0 (*)
      │   │       └── datafusion-sql v54.1.0 (*)
      │   └── repark-core v1.5.2 (*)
      ├── tempfile v3.27.0 (*)
      ├── tokio v1.53.1 (*)
      └── winapi-util v0.1.11
          ├── same-file v1.0.6
          │   └── walkdir v2.5.0
          │       ├── criterion v0.8.2 (*)
          │       └── object_store v0.13.2 (*)
          └── walkdir v2.5.0 (*)

advisories ok, bans ok, licenses ok, sources ok
```

D-M1 `cargo tree -d --locked | head -40` (verbatim):

```text
block-buffer v0.10.4
└── digest v0.10.7
    ├── apache-avro v0.21.0
    │   └── iceberg v0.9.1 (https://github.com/TRO-Wolf/iceberg-rust?rev=076d5f982d1d2ef352c3bb5d76d02cff27bc164a#076d5f98)
    │       ├── iceberg-catalog-glue v0.9.1 (https://github.com/TRO-Wolf/iceberg-rust?rev=076d5f982d1d2ef352c3bb5d76d02cff27bc164a#076d5f98)
    │       │   └── repark-iceberg v1.5.2 (/tmp/xc2b/crates/repark-iceberg)
    │       │       ├── repark-core v1.5.2 (/tmp/xc2b/crates/repark-core)
    │       │       │   ├── repark-distributed v1.5.2 (/tmp/xc2b/crates/repark-distributed)
    │       │       │   ├── repark-python v1.5.2 (/tmp/xc2b/crates/repark-python)
    │       │       │   ├── repark-spark v1.5.2 (/tmp/xc2b/crates/repark-spark)
    │       │       │   │   └── repark-python v1.5.2 (/tmp/xc2b/crates/repark-python)
    │       │       │   │   [dev-dependencies]
    │       │       │   │   └── repark-sql v1.5.2 (/tmp/xc2b/crates/repark-sql)
    │       │       │   ├── repark-sql v1.5.2 (/tmp/xc2b/crates/repark-sql)
    │       │       │   └── repark-ta v1.5.2 (/tmp/xc2b/crates/repark-ta)
    │       │       │       ├── repark-python v1.5.2 (/tmp/xc2b/crates/repark-python)
    │       │       │       └── repark-spark v1.5.2 (/tmp/xc2b/crates/repark-spark) (*)
    │       │       │       [dev-dependencies]
    │       │       │       └── repark-sql v1.5.2 (/tmp/xc2b/crates/repark-sql)
    │       │       ├── repark-spark v1.5.2 (/tmp/xc2b/crates/repark-spark) (*)
    │       │       └── repark-sql v1.5.2 (/tmp/xc2b/crates/repark-sql)
    │       ├── iceberg-catalog-s3tables v0.9.1 (https://github.com/TRO-Wolf/iceberg-rust?rev=076d5f982d1d2ef352c3bb5d76d02cff27bc164a#076d5f98)
    │       │   └── repark-iceberg v1.5.2 (/tmp/xc2b/crates/repark-iceberg) (*)
    │       ├── iceberg-datafusion v0.9.1 (https://github.com/TRO-Wolf/iceberg-rust?rev=076d5f982d1d2ef352c3bb5d76d02cff27bc164a#076d5f98)
    │       │   ├── repark-core v1.5.2 (/tmp/xc2b/crates/repark-core) (*)
    │       │   ├── repark-iceberg v1.5.2 (/tmp/xc2b/crates/repark-iceberg) (*)
    │       │   └── repark-spark v1.5.2 (/tmp/xc2b/crates/repark-spark) (*)
    │       ├── iceberg-storage-opendal v0.9.1 (https://github.com/TRO-Wolf/iceberg-rust?rev=076d5f982d1d2ef352c3bb5d76d02cff27bc164a#076d5f98)
    │       │   ├── iceberg-catalog-glue v0.9.1 (https://github.com/TRO-Wolf/iceberg-rust?rev=076d5f982d1d2ef352c3bb5d76d02cff27bc164a#076d5f98) (*)
    │       │   ├── iceberg-catalog-s3tables v0.9.1 (https://github.com/TRO-Wolf/iceberg-rust?rev=076d5f982d1d2ef352c3bb5d76d02cff27bc164a#076d5f98) (*)
    │       │   └── repark-iceberg v1.5.2 (/tmp/xc2b/crates/repark-iceberg) (*)
    │       ├── repark-core v1.5.2 (/tmp/xc2b/crates/repark-core) (*)
    │       ├── repark-functions v1.5.2 (/tmp/xc2b/crates/repark-functions)
    │       │   ├── repark-python v1.5.2 (/tmp/xc2b/crates/repark-python)
    │       │   ├── repark-spark v1.5.2 (/tmp/xc2b/crates/repark-spark) (*)
    │       │   └── repark-sql v1.5.2 (/tmp/xc2b/crates/repark-sql)
    │       ├── repark-iceberg v1.5.2 (/tmp/xc2b/crates/repark-iceberg) (*)
    │       ├── repark-spark v1.5.2 (/tmp/xc2b/crates/repark-spark) (*)
    │       └── repark-sql v1.5.2 (/tmp/xc2b/crates/repark-sql)
    ├── blake2 v0.10.6
```

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

## 6. C-2b round 1b — the settings keys and identifiers (2026-10-07)

**Branch:** `feat/c-2b-postgres-connection` from `6f693f8d` (round 1a). **Model:** Claude Opus 5.5
(`claude-opus-5-5`, high). **Scope:** sketch §2.3 (keys, aliases, URL parsing, redaction), the §2.2
identifier newtypes and §5.5's pins, all pure: no driver call, no network. TLS, the pool, the read
and `CONNECT-DECL-pg-server-version` belong to rounds 2 and 3. The attestation of §5 is the
Critic's to extend for C-2b; this round does not edit it.

### PROPOSITION LEDGER — C-2b round 1b — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-018 | `PostgresSettings::from_props(props, door)` reads the twenty §2.3 keys (`POSTGRES_KEYS`) with the sketch's defaults (port 5432, database = user, `verify-full`, connect 10 s, read 60 s, query unlimited, lock 10 s, batch = the session's, NTZ off, pushdown on, pool 4, checkout 30 s, idle 300 s, `repark`). `auth_method` still goes through C-1's `ConnectionSettings::from_props`. `url` takes `postgresql://`, `postgres://` and `jdbc:postgresql://`, percent-decoded, a bracketed IPv6 host included. `host` and `user` are required. Integers are ASCII digits in a per-key range. Any key outside the list, the declared keys and `driver` refuses with `InvalidSpecification` / `SpecRefusal::UnknownKey`, which names the key, lists the accepted keys and echoes no value; it folds to IllegalArgument. | `every_endpoint_key_parses_and_unknown_keys_refuse`; mutation m25 (accept unknown keys). | PROVEN | §6.1 m25 red; §6.3 gates. |
| C-019 | The Spark and pgjdbc aliases (`POSTGRES_ALIASES`) are matched ASCII case-insensitively on the `read_postgres` door and inside a `jdbc:` URL query; `repark.toml` and a libpq URI query take canonical spellings only. One setting given twice (two spellings, a URL part and a key, or one URL parameter twice) refuses with `SpecRefusal::Conflict`, naming both spellings. `connectTimeout`, `socketTimeout` and `queryTimeout` are seconds, converted to milliseconds, with the range reported in seconds. | `aliases_are_case_insensitive_and_conflicts_refuse`, `alias_units_convert`; mutations m26 (last spelling wins), m27 (no unit conversion). | PROVEN | §6.1 m26, m27 red. |
| C-020 | `sslmode` defaults to `verify-full`, and `disable` is accepted (`CONNECT-DIV-pg-sslmode`). `prefer`, `allow`, `require` and `verify-ca`, as a key or in the `url` query, refuse with `DeclaredSetting::UnverifiedSslmode` naming `CONNECT-DECL-sslmode-unverified` and the `verify-full` + `sslrootcert` fix; they fold to Unsupported. Any other spelling is an invalid specification listing the six. | `sslmode_default_is_verify_full`, `unverified_sslmodes_refuse`; mutations m28 (default to `require`), m36 (accept `require`). | PROVEN | §6.1 m28, m36 red. |
| C-021 | `sslcert` and `sslkey` refuse under `CONNECT-DECL-pg-client-cert`. `sessionInitStatement`, `customSchema` and `options` (`options` in the URL query too) refuse under `CONNECT-DECL-pg-session-sql`. A host list, from `host` or the URL authority, refuses under `CONNECT-DECL-pg-multi-host`. On the `read_postgres` door only, the five partitioned-read options refuse under `CONNECT-DECL-pg-partitioned-read`; in `repark.toml` they are unknown keys. Each fold is Unsupported. | `declared_keys_refuse_naming_their_row`; mutation m29 (accept `options`). | PROVEN | §6.1 m29 red. The `-pg-partitioned-read` registry row lands with C-2d (sketch §4); the code names it now. |
| C-022 | `read_timeout_ms = 0`, and `socketTimeout = 0` on the `read_postgres` door or in a `jdbc:` URL, refuse with `SpecRefusal::ZeroReadTimeout`, citing NS-7. | `read_timeout_zero_refuses`; mutation m30 (accept `0`). | PROVEN | §6.1 m30 red. |
| C-023 | `redact_source_prop(key, value)` is the connect-owned seam for C-2d's `sources()` rows. It delegates to `repark_common::redaction::redact_value`: `postgresql://u:pw@h/db` → `postgresql://u:***@h/db`; a `jdbc:` URL's `password=` parameter → `***`; a `password` key → `***`; a userinfo with no colon is masked whole (CONNECT-DIV-url-userinfo); URLs without a userinfo password are unchanged, including an `@` after the authority. | `redact_source_prop_masks_url_credentials`; mutation m31 (mask the userinfo only when an `@` follows a `:`). | PROVEN | §6.1 m31 red. The sketch's "a URL without a password is unchanged" holds for URLs with no userinfo; `postgresql://u@h` masks `u`, the common redactor's fail-closed reading. |
| C-024 | No new type renders a value. `PostgresSettings`'s `Debug` prints `auth_method` and `sslmode` only. Each new error's `Display` and `Debug` (unknown key, conflict, integer, boolean, sslmode, driver, URL scheme, escape, query pair, IPv6, multi-host, declared key) carries a spelling and an enum reason, never the URL, a password or a value. This extends C-1's C-006. | `settings_debug_never_renders_a_value`; mutation m32 (print the URL in the URL refusal). | PROVEN | §6.1 m32 red. |
| C-025 | `PgIdent::new` refuses an empty name, a NUL byte and more than 63 **bytes** with `InvalidIdentifier` (`IdentRefusal`, IllegalArgument). `Display` is the only rendering: double-quoted, each embedded `"` doubled. `QualifiedRelation` renders `"schema"."table"`. | `identifiers_render_double_quoted_with_quotes_doubled`, `identifiers_refuse_empty_nul_and_more_than_63_bytes`; mutations m33 (no quote doubling), m34 (count chars, not bytes). | PROVEN | §6.1 m33, m34 red. |
| C-026 | The registry gains six dated rows (2026-10-07), each with the four-field shape and a live pin in `tests/it/settings.rs`: `CONNECT-DECL-sslmode-unverified`, `-pg-client-cert`, `-pg-session-sql`, `-pg-multi-host`, `CONNECT-DIV-pg-sslmode` and `-pg-unknown-option`. Each landed in the same commit as its pin. C-1's four pins pass unchanged, and C-1's m3 replays red through the shared parser. | The rows; `python3 scripts/check_docs_links.py`; each cited pin is a `fn` in `tests/it/settings.rs`; m35. | PROVEN | §6.1 m35 red; §6.3. |
| C-027 | The round's files hold their sketch ceilings: `settings/postgres.rs` 543/560, `ident.rs` 86/200, `tests/it/settings.rs` 591/600. `error.rs` is 260 against C-2a's 260. Beyond the brief's list come `tests/it/ident.rs` (40 lines, the identifier pins) and `src/settings/map.md`, which the new directory needs. No dependency changes, no code comments, and every gate in §6.3 passes. | §6.3. | PROVEN | §6.3. |

### 6.1 Mutations (round 1b)

Each mutation was applied alone to the committed tree (`493f30ca`), the crate's integration binary
run, and the file restored with `git checkout`. "Red in" names the failing pins. A line inside a
shared helper (`refusal`'s `expect_err` at `settings.rs:139`) is the panic site for pins that call
it.

| id | clause | mutation (file) | red? | red in |
|---|---|---|---|---|
| m25 | C-018 | an unknown key returns `Ok` (`src/settings/postgres.rs`) | RED | `every_endpoint_key_parses_and_unknown_keys_refuse`, `aliases_are_case_insensitive_and_conflicts_refuse`, `declared_keys_refuse_naming_their_row`, `settings_debug_never_renders_a_value` (all at `settings.rs:139`) |
| m26 | C-019 | `give` never sees the first spelling, so the last wins (`src/settings/postgres.rs`) | RED | `aliases_are_case_insensitive_and_conflicts_refuse`, `settings_debug_never_renders_a_value` (`settings.rs:139`) |
| m27 | C-019 | the seconds scale is 1 (`src/settings/postgres.rs`) | RED | `alias_units_convert` at `settings.rs:414`; `aliases_are_case_insensitive_and_conflicts_refuse` at `settings.rs:339` |
| m28 | C-020 | an absent `sslmode` resolves to `Require` (`src/settings/postgres.rs`) | RED | `sslmode_default_is_verify_full` at `settings.rs:436`; `every_endpoint_key_parses_and_unknown_keys_refuse` at `settings.rs:294`; `settings_debug_never_renders_a_value` at `settings.rs:563` |
| m29 | C-021 | `options` returns `Ok` before classification (`src/settings/postgres.rs`) | RED | `declared_keys_refuse_naming_their_row` (`settings.rs:139`) |
| m30 | C-022 | the zero read-timeout arm never matches (`src/settings/postgres.rs`) | RED | `read_timeout_zero_refuses` (`settings.rs:139`) |
| m31 | C-023 | `redact_source_prop` masks from the first `:` after `://` to the last `@`, else nothing (`src/settings/postgres.rs`) | RED | `redact_source_prop_masks_url_credentials` at `settings.rs:542` |
| m32 | C-024 | the URL refusal's spelling is the URL itself (`src/settings/postgres.rs`) | RED | `settings_debug_never_renders_a_value` at `settings.rs:588` |
| m33 | C-025 | an embedded `"` renders single (`src/ident.rs`) | RED | `identifiers_render_double_quoted_with_quotes_doubled` at `ident.rs:12` |
| m34 | C-025 | the length test counts chars (`src/ident.rs`) | RED | `identifiers_refuse_empty_nul_and_more_than_63_bytes` at `ident.rs:33` |
| m35 | C-026 | C-1's m3 replayed: skip the declared auth refusal (`src/settings.rs`) | RED | `iam_token_is_a_declared_refusal` at `settings.rs:48`; `kerberos_is_a_declared_refusal` at `settings.rs:66` |
| m36 | C-020 | `require` joins the accepted modes (`src/settings/postgres.rs`) | RED | `unverified_sslmodes_refuse` (`settings.rs:139`) |

All twelve are red; none survived. The sketch names one mutation for both sslmode pins. m28 reds the default pin only, so m36 was added for `unverified_sslmodes_refuse`.

### 6.2 Readings acted on (no halt)

- **Alias positions.** The sketch puts aliases on the `read_postgres` door and in a `jdbc:` URL's
  query. A libpq URI's query takes canonical keys only. On both doors the declared keys follow
  the same case rule as their position, so `repark.toml` matches them exactly.
- **`driver`** is accepted only at the top level of the `read_postgres` door. It is a Spark
  option, so `repark.toml` and the URL refuse it as an unknown key.
- **Timeout floors.** The network waits (connect, read, pool checkout, pool idle) start at 1 ms.
  `query_timeout_ms` and `lock_timeout_ms` take `0`: `0` means unlimited for the first and is
  passed through for the second. Every millisecond value is capped at `i32::MAX`, Postgres's own
  bound for `statement_timeout` and `lock_timeout`.
- **`fetchsize = 0` refuses.** Spark's default is `0` (the driver's choice), so a script that
  sets it explicitly to `0` refuses with the range `1..`. Filed in the hand-back for a ruling.
- **No source name yet.** Sketch §2.2 puts `source` on every variant. The settings errors carry
  the spelling and reason only, as C-1's `DeclaredAuthMethod` does, because `from_props` sees
  props, not the source. C-2d's resolution owns the source name.
- **Spark relation options.** `dbtable` and `query` are not settings keys. C-2d's door must take
  them out of the props before `from_props`, or they refuse as unknown.
- **`+` in a `jdbc:` query** is not decoded as a space (pgjdbc uses `URLDecoder`). Only `%XX`
  escapes decode, as in libpq.

### 6.3 Gates (round 1b)

Run on the finished tree. Each cargo command ran under the build-slot lock; exit codes are
verbatim.

| command | exit | output |
|---|---|---|
| `cargo test -p repark-connect` | 0 | 65 passed, 0 failed (54 at round 1a, plus nine §5.5 settings pins and two identifier pins; C-1's seven settings tests unchanged) |
| `cargo build -p repark-connect --no-default-features` | 0 | the pure core builds without the driver |
| `make rust-clippy` | 0 | workspace, all targets, no diagnostics |
| `cargo fmt --check` | 0 | no output |
| `make rust-panic-ban` | 0 | clean |
| `python3 scripts/check_rust_file_size.py` | 0 | 1052 files clean |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 365 maps clean |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | no output |
| `python3 scripts/check_docs_links.py` | 0 | clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 303 live ledgers clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xc2b origin/main HEAD` | 0 | `comment-ban hits=0` |

## 7. C-2b round 2 — TLS and the query pool (2026-10-07)

**Branch:** `feat/c-2b-postgres-connection` from `58003175` (round 1b). **Model:** Claude Opus 5.5
(`claude-opus-5-5`, high). **Scope:** sketch §2.4 (the TLS config, the session pins, the TLS
refusal) and §2.5 (the query pool, the checkout guard, the connect timeout and the timeout
helper the read path uses), plus the round-1b ruling on `fetchsize = 0`. No Postgres server ran:
the pins drive the pool with fake connections and the connector against loopback listeners that
speak one byte of the protocol (or a TLS handshake) and nothing else. The read path, discovery,
`CONNECT-DECL-pg-server-version` and §5.6's live matrix are round 3.

### PROPOSITION LEDGER — C-2b round 2 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-028 | `verify_full_config(sslrootcert)` builds the `verify-full` rustls `ClientConfig`: the system roots (`rustls-native-certs`) plus every certificate in `sslrootcert` when set, rustls's WebPKI verifier (chain and host name), no client certificate, and the crypto provider named explicitly (`rustls::crypto::ring`) through `builder_with_provider`. A leaf signed by the `sslrootcert` CA verifies for the name it carries; another name refuses `NotValidForName`; a CA outside the store refuses `UnknownIssuer`. | `verify_full_trusts_sslrootcert_and_checks_the_host_name` (in-memory handshakes over the static fixtures); mutation m38. | PROVEN | §7.1 m38 red. |
| C-029 | A bad `sslrootcert` refuses before any connect with `ConnectError::TlsHandshake { kind }`: an unreadable file is `RootCertUnreadable { kind: io::ErrorKind }`; a file with no PEM certificate, a garbled one, or one rustls cannot take as a trust anchor is `RootCertInvalid`; an empty store is `NoTrustedRoots`. The message names `sslrootcert` and never the path; the fold is the operational class (`DataFusion`, PySpark `Base`). | `sslrootcert_must_be_a_readable_pem_ca_bundle`; mutation m40. | PROVEN | §7.1 m40 red. `NoTrustedRoots` has no pin: an empty system store needs `SSL_CERT_FILE`, and setting the environment in a test is `unsafe` under edition 2024, which the workspace forbids. |
| C-030 | H-CRYPTO holds: one rustls crypto provider. The workspace `cargo tree -e features -i rustls --locked` feature set is the same thirteen features before and after (`ring` and `aws-lc-rs` were both already on). `repark-connect` declares `rustls` with `ring`, `std` and `tls12` and no default features; the `ring` crate was already in its tree under `tokio-postgres-rustls`, and no `aws-lc` crate enters it. `ClientConfig::builder()` is never called, because it cannot pick a process default with both providers compiled in. `cargo deny check` is clean. `Cargo.lock` gains one dependency edge and no package. | §7.3. | PROVEN | §7.3. |
| C-031 | `QueryPool` (sketch §2.5): a semaphore of `pool_max_size` permits; checkout waits at most `pool_checkout_timeout_ms` for a permit, else `PoolExhausted { waited }`; a checkout reuses the most recent idle connection; on checkout every idle connection that `is_closed()` or has idled past `pool_idle_timeout_ms` is reaped (dropped outside the lock); a connection closed at release is never pooled. A permit is held for the whole lease and released after the connection is idle, so open connections never exceed `pool_max_size`. | `a_clean_release_is_reused_by_the_next_checkout`, `checkout_beyond_pool_max_size_is_pool_exhausted`, `closed_and_idle_expired_connections_are_never_reused`; mutations m42, m43, m44, m45. | PROVEN | §7.1. |
| C-032 | `PooledClient` returns its connection only through `release_clean()`. Dropped any other way (cancel, error, an early `LIMIT`), its lease aborts the connection's task, so the socket closes and the server ends the backend, and the connection never re-enters the pool. No cleanup task is spawned. The only task is the driver's connection task, spawned once per connection under `#[expect(clippy::disallowed_methods)]` with its lifecycle stated: `PgConnection` holds its handle. | `a_lease_dropped_before_release_aborts_its_connection` (F-2's unit half); mutation m41. | PROVEN | §7.1 m41 red. The live F-2 (`pg_stat_activity`) is round 3. Since fold 1 (C-054) an abandoned lease also spawns one bounded task, the cancel request. |
| C-033 | `query_config(settings)` builds the only `Config` the pool connects with: host, port, user, password, database, `application_name`, `connect_timeout`, keepalives on, `ssl_mode` `Disable` for `disable` and `Require` otherwise (verification is rustls's), and the §2.4 session pins in one `-c` options list: `client_encoding=UTF8`, `DateStyle=ISO`, `IntervalStyle=postgres`, `TimeZone=UTC`, `search_path=` (empty), `default_transaction_read_only=on`, `lock_timeout`, `statement_timeout` (= `query_timeout_ms`, `0` unlimited) and `idle_in_transaction_session_timeout` (= `read_timeout_ms`). Nothing in `pool.rs` sets a replication mode. | `query_config_pins_the_session_in_the_startup_packet`; mutations m46, m48. | PROVEN | §7.1. Whether the server honours each pin is round 3's live reading. |
| C-034 | NS-7 at connect: `PostgresConnector::connect` bounds the whole connect (TCP, the TLS negotiation and handshake, authentication) by `connect_timeout_ms` and refuses with `Timeout { which: TimeoutSetting::Connect }`, whose message names the key. The driver's own TCP timeout classifies the same way. `within(which, limit, work)` is the helper the read path wraps every request and COPY chunk in. | `connect_timeout_bounds_a_server_that_never_answers` (a listener that accepts and never answers, under `disable` and `verify-full`); mutation m47. | PROVEN | §7.1 m47 red (the 3 s guard fires). |
| C-035 | Connect failures classify without matching driver text: a server that refuses TLS under `verify-full` is `TlsRequired`, which names `sslmode=disable` as the only switch, because the handshake-tracking connector never started; a rustls certificate refusal is `TlsHandshake` with `UntrustedCertificate`, `HostNameMismatch`, `CertificateExpired` or `Handshake`; a host name rustls cannot verify is `ServerName`; any other I/O failure is `Unreachable { kind }`; a server error is `Server { sqlstate, message }`; a connection closed during startup is `Unreachable { UnexpectedEof }`; the rest, which are driver-side authentication failures (no password, SASL), is `AuthenticationFailed`. All fold to the operational class. | `plaintext_server_refuses_under_verify_full`, `verify_full_refuses_an_untrusted_or_misnamed_server_certificate`, `a_refused_port_is_unreachable`; mutations m48, m39, m49, m50. | PROVEN | §7.1. `Server` carried every server error at connect, including SQLSTATE class 28. Corrected by fold 1 (2026-10-07): round 3 split off only `PermissionDenied` (`42501`), and a wrong password stayed `Server { 28P01 }`; fold 1 maps class 28 to `AuthenticationFailed` with a live pin (C-055). |
| C-036 | The round-1b Q1 ruling (orchestrator, 2026-10-07, ACCEPT): on the `read_postgres` door, the `fetchsize` alias with the value `0` (any run of zeros) leaves `batch_rows` unset, which is the session batch size. `batch_rows = 0` refuses on both doors, as does `fetchsize = 0` inside a `jdbc:` URL query. No registry row: Spark's `fetchsize` is a round-trip hint and pgjdbc treats `0` as its default, so no row of a result changes. | `fetchsize_zero_is_the_session_batch_default`; mutation m37. | PROVEN | §7.1 m37 red. |
| C-037 | The error shape stays NS-15's: seven `ConnectError` variants (`TlsRequired`, `TlsHandshake`, `Unreachable`, `Timeout`, `PoolExhausted`, `AuthenticationFailed`, `Server`) exist under the `postgres` feature, each with an enum reason (`TlsFailure`, `TimeoutSetting`, `io::ErrorKind`) or the server's own text, never a setting's value. `ProtocolViolation` moves beside its producer in `copy_binary.rs` and is re-exported from `error.rs`, so its public path is unchanged. The round's files hold their ceilings: `tls.rs` 163/200, `pool.rs` 357/450, `tests/it/tls.rs` 129/200, `error.rs` 235/260, `copy_binary.rs` 469/480, `settings/postgres.rs` 553/560, `tests/it/settings.rs` 599/600. No code comments. Every gate in §7.3 passes. | §7.3. | PROVEN | §7.3. |

### 7.1 Mutations (round 2)

Each mutation was applied alone to the committed code (`c9416531`), the crate's integration
binary run with the module's filter, and the file restored with `git checkout`. "Red at" is the
assertion's line; for m38 and m40 the line in `root_refusal` or `handshake`'s caller is the site.

| id | clause | mutation (file) | red? | red in |
|---|---|---|---|---|
| m37 | C-036 | the `fetchsize = 0` arm never fires (`src/settings/postgres.rs`) | RED | `fetchsize_zero_is_the_session_batch_default` (`settings.rs:135`, `parse`'s expect) |
| m38 | C-028 | `sslrootcert` is ignored (`src/tls.rs`) | RED | `verify_full_trusts_sslrootcert_and_checks_the_host_name` at `tls.rs:72`; `sslrootcert_must_be_a_readable_pem_ca_bundle` at `tls.rs:91` |
| m39 | C-035 | the tracked connector verifies `localhost` whatever `host` says (`src/tls.rs`) | RED | `verify_full_refuses_an_untrusted_or_misnamed_server_certificate` at `pool.rs:346` |
| m40 | C-029 | a bundle with no certificate is accepted (`src/tls.rs`) | RED | `sslrootcert_must_be_a_readable_pem_ca_bundle` at `tls.rs:91` |
| m41 | C-032 | `release_clean` on `Drop`: the lease no longer aborts (`src/pool.rs`) | RED | `a_lease_dropped_before_release_aborts_its_connection` at `pool.rs:114` |
| m42 | C-031 | `release_clean` drops the connection instead of pooling it (`src/pool.rs`) | RED | `a_clean_release_is_reused_by_the_next_checkout` at `pool.rs:99`; `closed_and_idle_expired_connections_are_never_reused` at `pool.rs:150` |
| m43 | C-031 | an unbounded semaphore (`src/pool.rs`) | RED | `checkout_beyond_pool_max_size_is_pool_exhausted` at `pool.rs:89` (the second checkout succeeds) |
| m44 | C-031 | no health check on checkout (`src/pool.rs`) | RED | `closed_and_idle_expired_connections_are_never_reused` at `pool.rs:149` |
| m45 | C-031 | no idle reap on checkout (`src/pool.rs`) | RED | `closed_and_idle_expired_connections_are_never_reused` at `pool.rs:153` |
| m46 | C-033 | drop the `default_transaction_read_only` pin (`src/pool.rs`) | RED | `query_config_pins_the_session_in_the_startup_packet` at `pool.rs:186` |
| m47 | C-034 | no timeout around the connect (`src/pool.rs`) | RED | `connect_timeout_bounds_a_server_that_never_answers` at `pool.rs:91` (hung past the guard) |
| m48 | C-033, C-035 | `verify-full` maps to the driver's `Prefer` (`src/pool.rs`) | RED | `plaintext_server_refuses_under_verify_full` at `pool.rs:292`; `query_config_pins_the_session_in_the_startup_packet` at `pool.rs:202` |
| m49 | C-035 | every rustls error classifies as `Handshake` (`src/tls.rs`) | RED | `verify_full_refuses_an_untrusted_or_misnamed_server_certificate` at `pool.rs:346` |
| m50 | C-035 | every I/O failure is `Unreachable { Other }` (`src/pool.rs`) | RED | `a_refused_port_is_unreachable` at `pool.rs:311` |

All fourteen are red; none survived. The sketch's live mutations for F-2 ("`release_clean` on
`Drop`") and F-5 ("an unbounded semaphore") were run here against the unit halves as m41 and
m43; round 3 replays them against the server.

### 7.2 Readings acted on (no halt)

- **The startup pins live in `pool.rs`.** Sketch §7 lists "the startup pins" under
  `read/postgres.rs` (round 3), but §2.5 makes the pool the only builder of a query-mode
  `Config`. The pins are part of that `Config`, so `query_config` carries them now and round 3
  reads them through the pool.
- **`rustls` is a direct dependency.** The rustls config needs the `rustls` API, and the
  provider must be named. `rustls` 0.23 (already in the lock) joins `[workspace.dependencies]`
  with no default features, and `repark-connect` enables `ring`, `std` and `tls12`. The crate
  also names its `tokio` features (`net`, `rt`, `sync`, `time`), which it had only received
  through `tokio-postgres`. Both edits are on sketch §7's C-2b file list (`Cargo.toml`,
  `Cargo.lock`), outside this round's narrower list.
- **Why `ring`.** The workspace already compiles rustls with both providers, so either is "the
  one the tree builds". `ring` is already in `repark-connect`'s own tree, so `cargo build -p
  repark-connect` compiles no new crypto crate; `aws-lc-rs` would add `aws-lc-sys` there.
- **`disable` connects with `NoTls`.** `tokio-postgres` calls `make_tls_connect` even when TLS
  is off, and `tokio-postgres-rustls` refuses the empty name a Unix-socket host gives it. With no
  TLS connector, `sslmode=disable` reaches a socket directory. Under `verify-full` such a host
  is `TlsHandshake { ServerName }`.
- **`error.rs` at its ceiling.** Round 1b left `error.rs` at 260/260, and §2.2 puts the
  connection variants there. The reason enums live with their producers (`TlsFailure` in
  `tls.rs`, `TimeoutSetting` in `pool.rs`), as round 1b did for the settings reasons, and
  `ProtocolViolation` moves beside the decoder. The new variants are `#[cfg(feature =
  "postgres")]`: they arise only from the driver (CC-5).
- **No source name.** As in round 1b, the connection errors carry no `source`; C-2d adds it at
  resolution (round-1b Q2 ruling).
- **Test fixtures.** `rcgen` is not in the lock, so the certificates are static PEM files under
  `tests/it/fixtures/`, generated once with the local `openssl` (EC P-256, valid to 2126): a CA,
  a `localhost` leaf it signs (with its private key), and a second CA that signs nothing the
  tests trust. The key is a test identity and protects nothing.

### 7.3 Gates (round 2)

Run on the finished tree. Each cargo command ran under the build-slot lock; exit codes are
verbatim.

| command | exit | output |
|---|---|---|
| `cargo tree -e features -i rustls --locked \| grep -oE 'rustls feature "[a-z0-9_-]+"' \| sort -u` (before, after) | 0, 0 | the same 13 features: `aws-lc-rs`, `aws_lc_rs`, `default`, `http1`, `http2`, `native-tokio`, `prefer-post-quantum`, `ring`, `rustls-native-certs`, `std`, `tls12`, `webpki-roots`, `webpki-tokio` |
| `cargo tree -p repark-connect -e features -i rustls --locked` (after) | 0 | `default`, `ring`, `std`, `tls12`; no `aws-lc` crate in the crate's tree |
| `cargo deny check 2>&1 \| tail -5` | 0 | `advisories ok, bans ok, licenses ok, sources ok` |
| `cargo test -p repark-connect` | 0 | 77 passed, 0 failed (65 at round 1b, plus the `fetchsize` pin, two TLS pins and nine pool pins); the eleven pool and TLS pins ran fifteen times more, all green |
| `cargo build -p repark-connect --no-default-features` | 0 | the pure core builds without the driver |
| `cargo clippy -p repark-connect --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 | no diagnostics, with and without default features |
| `make rust-clippy` | 0 | workspace, all targets, no diagnostics |
| `cargo fmt --check` | 0 | no output |
| `make rust-panic-ban` | 0 | clean; the one `tokio::spawn` carries its `#[expect]` |
| `python3 scripts/check_rust_file_size.py` | 0 | 1056 files clean |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 365 maps clean |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | no output |
| `python3 scripts/check_docs_links.py` | 0 | 1339 files, 7198 links clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 303 live ledgers clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xc2b origin/main HEAD` | 0 | `comment-ban hits=0` |

## 8. C-2b round 3 — the read path, discovery and the live cells (2026-10-07)

**Branch:** `feat/c-2b-postgres-connection` from `1e603495` (round 2). **Model:** Claude Opus 5.5
(`claude-opus-5-5`, high). **Scope:** sketch §2.4 (the `set_config` carriage, the server floor),
§2.6 (the COPY statement and stream), §2.8 (discovery), §5.6 (the live crash matrix) and §6 (the
Rust live cells), plus C-2a F-8 (the `::text` cast) and the last C-2b registry row. The live
cells ran against `make pg-up` (PostgreSQL 16, rootless Docker under `repark.slice`), torn down
with `make pg-down` at the end.

### PROPOSITION LEDGER — C-2b round 3 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-038 | The statement builder (sketch §2.6, C-2a F-8): `ScanRequest::statement()` renders `COPY (SELECT <col>::<cast>, … FROM <source> [WHERE …] [LIMIT n]) TO STDOUT (FORMAT BINARY)` with a cast on every projected column. `interval`, every `ServerText` mapping and enums cast to `pg_catalog.text`; a `numeric` with a modifier casts to `pg_catalog.numeric(p,s)` (a negative scale included); every other mapped type casts to `pg_catalog.<row name>`. An empty projection is `SELECT FROM`; a `query` source is `(…) AS repark_q`; a `dbtable` starting with `(` is `SELECT * FROM <dbtable>` in query mode. Identifiers render through `PgIdent`. | `statement_casts_every_column_and_server_text_to_text`, `query_mode_wraps_the_statement_and_an_empty_projection_selects_nothing`, live `mapped_types_round_trip_through_the_scan` and `schema_drift_between_plan_and_scan_fails_loud_or_stays_typed`; mutations m56, m60. | PROVEN | §8.1. |
| C-039 | The `set_config` carriage (sketch §2.4, FL-13): a pushed value takes the next `ParamSlot` (`repark.p0` … `repark.p1023`); the predicate reads it back as `pg_catalog.current_setting('repark.pN')::<the column's cast without its modifier>`, comparing a text-cast column as text; the values reach the server only as bound parameters of one `SELECT pg_catalog.set_config($1, $2, true), …` inside `BEGIN READ ONLY`, never in SQL text. A scan with no value sends no transaction. A 1025th value or an out-of-range column answers `None`. The same pushed scan run twice returns identical batches. | `pushed_values_ride_set_config_never_the_statement_text`, `param_slots_stop_at_1024_and_bad_indexes_refuse`, live `scan_is_read_only_and_idempotent`; mutation m61. | PROVEN | §8.1. `compare` is the seed C-2c's classifier extends; the caller renders the value text (sketch §2.9). |
| C-040 | The stream (sketch §2.5, §2.6): `scan` returns a `try_unfold` stream whose first poll checks out the client, opens the transaction and sends the `set_config` call and the COPY, so building the stream does no I/O. Each step feeds chunks to `CopyBinaryDecoder` and yields at most one batch. After the decoder's `finish()` the scan commits (when a transaction is open) and calls `release_clean()`. Any error, and any drop before the end, aborts the lease: the client is never pooled and the backend ends. | Live `backend_killed_mid_copy_is_disconnected` (F-1), `stream_dropped_mid_copy_closes_the_backend` (F-2), `schema_drift_between_plan_and_scan_fails_loud_or_stays_typed` (an error mid-stream leaves the idle count at 0), `scan_is_read_only_and_idempotent` (a clean scan returns its client); mutations m51, m52. | PROVEN | §8.1. m51 is green in F-1 alone: the pool's `put_idle` drops a closed client (C-031, m44). It is red in F-7, where the connection survives a server error. |
| C-041 | NS-7 and the request classification: every request (`BEGIN`, `set_config`, `copy_out`, `COMMIT`, the discovery queries and `prepare`) and every COPY chunk runs under `within(TimeoutSetting::Read, read_timeout_ms, …)`. Driver errors classify by SQLSTATE, never by text: `55P03` → `Timeout { Lock }`, `57014` → `Timeout { Query }`, `25P03` → `Timeout { Read }`; with a known relation, `42501` → `PermissionDenied { relation, Select }` and `42P01` → `RelationNotFound`; class `57P` and a closed or failed socket → `Disconnected`; any other server error → `Server`; any other driver failure → `Protocol(UnexpectedResponse)`. | Live `idle_read_timeout_fires` (F-3), `query_timeout_is_the_server_statement_timeout`, `lock_timeout_fires` (F-4), `backend_killed_mid_copy_is_disconnected`, `missing_select_grant_names_the_privilege`, `schema_drift_between_plan_and_scan_fails_loud_or_stays_typed`; mutations m53, m54, m58, m67, m68. | PROVEN | §8.1. In query mode the relation is unknown, so `42501` stays `Server` with the server's text (§8.2). |
| C-042 | Relation discovery (sketch §2.8): one bound catalog query per resolution, in `BEGIN READ ONLY` with a 30 s local `statement_timeout`, keyed on `nspname = $1`, `relname = $2`, `relkind IN ('r','v','m','f','p')`. A miss is `RelationNotFound`; a missing schema `USAGE` or any-column `SELECT` privilege is `PermissionDenied`, naming the relation and the privilege; columns are `attnum > 0`, not dropped, in `attnum` order; nullability follows `attnotnull`; a domain resolves through every level to its base type, the innermost `typtypmod` standing in when the column has none; a base type counts only in `pg_catalog`; enums read as labels; each column carries its collation and `collisdeterministic`. No cache (FL-7). | Live `relation_discovery_resolves_domains_nullability_and_collation` (a domain over a domain over `numeric(10,2)`, a dropped column, `NOT NULL`, `COLLATE "C"`), `missing_select_grant_names_the_privilege`; mutations m63, m64, m65, m66. | PROVEN | §8.1. m65 (keep dropped columns) is equivalent: a dropped column's `atttypid` is `0`, so the type join already excludes it. The filter stays as the sketch's stated intent. |
| C-043 | Query discovery (sketch §2.8, D-M7): the wrapped statement `SELECT * FROM (<query>) AS repark_q` is `prepare`d (Parse and Describe, no execution). Each column takes the RowDescription type, `Column::type_modifier()` and nullable `true`; the server reports a domain column as its base type. H-TYPEMOD did not fire: `tokio-postgres` 0.7.18 exposes the modifier, so `numeric(8,3)` in a query reads as `Decimal128(8,3)`. | Live `mapped_types_round_trip_through_the_scan` (query mode, `numeric(8,3)` and `numeric(2,1)` keep their scale), `idle_read_timeout_fires`. | PROVEN | §8.1. |
| C-044 | The server floor (sketch §2.4): discovery reads `server_version_num` before any catalog query; below `140000` it refuses with `ConnectError::DeclaredServerVersion`, naming the number and `CONNECT-DECL-pg-server-version`, in the Unsupported class. The registry row lands in the same commit as its pin. | `servers_older_than_14_are_declared`; mutation m62. | PROVEN | §8.1. No live pin: the container is PostgreSQL 16. Every scan resolves first (FL-7) through the same pool, so the check needs no round trip at connect. |
| C-045 | `dbtable` parsing (sketch §2.8, FL-14, FL-15): `QualifiedRelation::parse` takes one or two `.`-separated parts, each bare and taken exactly, or double-quoted with `""` for a quote. One part takes schema `public`. Three parts, an unterminated quote or text after a quoted part refuse with `IdentRefusal::Qualification`; an empty part refuses `Empty`. | `dbtable_parses_exact_qualified_and_quoted_parts`. | PROVEN | §8.1. FL-15 is new (§8.3). |
| C-046 | Sketch §5.6, live under `make pg-up`. F-1: `pg_terminate_backend` after the first batch gives `Disconnected`, the idle count stays 0, and the next scan runs on a different backend. F-2: dropping the stream after one batch leaves no backend with the cell's `application_name` within the read timeout (5 s), and nothing pooled. F-3: a stall before the first row and one mid-stream each give `Timeout { Read }` within 3 s at `read_timeout_ms = 500`. F-4: `ACCESS EXCLUSIVE` held by the fixture gives `Timeout { Lock }`. F-5: `pool_max_size = 1` with one scan held gives `PoolExhausted` after 300 ms. F-7: an `int4` widened to `int8` reads back typed; one retyped to `text` fails `22P02` mid-stream. F-8: a SQL function that deletes refuses `25006` and the row count and `n_tup_del` are unchanged. Every pooled backend is a `client backend` under the configured `application_name`. A role without `SELECT` gets `PermissionDenied { Select }` at discovery and at the scan. A plaintext server under the default is `TlsRequired`. The cells panic without `REPARK_PG_URL`. | The fourteen `live_pg.rs` cells, six full runs green; mutations m51–m59, m63, m67, m68. | PROVEN | §8.1, §8.4. |
| C-047 | The anchors are the server's bytes: the eleven sketch §2.7 anchors, read back by `COPY (SELECT <literal>) TO STDOUT (FORMAT BINARY)` from PostgreSQL 16, equal the hand-computed bytes. Every mapped C-2a type reads through the scan to its §2.7 Arrow value: `date`, `timestamp`, `timestamptz`, both `numeric` anchors, `interval` as the server's `1 day 02:00:00`, `uuid`, `jsonb`, `json` byte for byte, an enum label and `int8`. `infinity` dates and `NaN` refuse per value with their `ValueRefusal`. | Live `server_bytes_are_the_wire_anchors`, `mapped_types_round_trip_through_the_scan`; mutation m60. | PROVEN | §8.1. |
| C-048 | Shape and files: three `ConnectError` variants under `postgres`, `PermissionDenied { relation: QualifiedRelation, privilege: Privilege }`, `RelationNotFound { relation }` and `DeclaredServerVersion { server_version_num }`, plus `ProtocolViolation::UnexpectedResponse` and `IdentRefusal::Qualification`, each an enum or a typed field, never a value. Ceilings: `read.rs` 1/20, `read/postgres.rs` 412/600, `discover.rs` 340/380, `tests/it/live_pg.rs` 735/900, `error.rs` 260/260, `copy_binary.rs` 473/480, `ident.rs` 137/200. No dependency change. No code comments. Every gate in §8.4 passes. | `read_errors_name_the_relation_and_fold_as_operational`, `servers_older_than_14_are_declared`; §8.4. | PROVEN | §8.4. |

### 8.1 Mutations (round 3)

Each mutation was applied alone to the committed code (`49aea45a`), the named pins run (the
live ones with `--include-ignored` against the container), and the file restored with
`git checkout`. "Red at" is the failing assertion's line.

| id | clause | mutation (file) | red? | red in |
|---|---|---|---|---|
| m51 | C-040, F-1 | return the client to the pool on error (`src/read/postgres.rs`) | RED | `schema_drift_between_plan_and_scan_fails_loud_or_stays_typed` at `live_pg.rs:377`; green in `backend_killed_mid_copy_is_disconnected`, where the pool's health check drops the dead client |
| m52 | C-040, F-2 | `release_clean` on `Drop`: the lease never aborts (`src/pool.rs`) | RED | `stream_dropped_mid_copy_closes_the_backend` at `live_pg.rs:222` |
| m53 | C-041, F-3 | no timeout around the chunk await (`src/read/postgres.rs`) | RED | `idle_read_timeout_fires` at `live_pg.rs:249` (the mid-stream stall completes) |
| m54 | C-041, F-4 | drop the `lock_timeout` startup pin (`src/pool.rs`) | RED | `lock_timeout_fires` at `live_pg.rs:299` (`Timeout { Read }`) |
| m55 | C-046, F-5 | an unbounded semaphore (`src/pool.rs`) | RED | `pool_exhaustion_times_out` at `live_pg.rs:323` |
| m56 | C-038, F-7 | drop the per-column cast (`src/read/postgres.rs`) | RED | `schema_drift_between_plan_and_scan_fails_loud_or_stays_typed` at `live_pg.rs:356`; `statement_casts_every_column_and_server_text_to_text` at `read.rs:54` |
| m57 | C-046, F-8 | drop `default_transaction_read_only` (`src/pool.rs`) | RED | `scan_is_read_only_and_idempotent` at `live_pg.rs:408` |
| m58 | C-041 | map `42501` to `Server` (`src/read/postgres.rs`) | RED | `missing_select_grant_names_the_privilege` at `live_pg.rs:526` |
| m59 | C-046 | default `sslmode` to `disable` (`src/settings/postgres.rs`) | RED | `plaintext_server_refuses_under_the_default` at `live_pg.rs:551` |
| m60 | C-038, C-047 | `interval` not cast to text (`src/discover.rs`) | RED | `statement_casts_every_column_and_server_text_to_text` at `read.rs:54`; `mapped_types_round_trip_through_the_scan` at `live_pg.rs:639` |
| m61 | C-039 | the setting name is off by one (`src/read/postgres.rs`) | RED | `pushed_values_ride_set_config_never_the_statement_text` at `read.rs:96`; `scan_is_read_only_and_idempotent` at `live_pg.rs:431` |
| m62 | C-044 | the floor at 13 (`src/discover.rs`) | RED | `servers_older_than_14_are_declared` at `read.rs:185` |
| m63 | C-042 | discovery ignores the `SELECT` privilege (`src/discover.rs`) | RED | `missing_select_grant_names_the_privilege` at `live_pg.rs:508` |
| m64 | C-042 | discovery ignores `attnotnull` (`src/discover.rs`) | RED | `relation_discovery_resolves_domains_nullability_and_collation` at `live_pg.rs:713` |
| m65 | C-042 | discovery keeps dropped columns (`src/discover.rs`) | GREEN (equivalent) | a dropped column's `atttypid` is `0`, so the type join already excludes it |
| m66 | C-042 | the domain walk stops at the first level (`src/discover.rs`) | RED | `relation_discovery_resolves_domains_nullability_and_collation` at `live_pg.rs:713` |
| m67 | C-041 | `57014` classifies as `Server` (`src/read/postgres.rs`) | RED | `query_timeout_is_the_server_statement_timeout` at `live_pg.rs:270` |
| m68 | C-041 | `42P01` classifies as `Server` (`src/read/postgres.rs`) | RED | `schema_drift_between_plan_and_scan_fails_loud_or_stays_typed` at `live_pg.rs:385` |

Seventeen are red and one, m65, is an equivalent mutant. Every sketch §5.6 mutation is red in at
least one pin. The first runs turned up two surviving mutants. m53 survived because the server
buffers `CopyOutResponse` until its first flush, so a stall before any row trips the `copy_out`
timeout instead of the chunk timeout; F-3 now stalls mid-stream as well. m51 survived F-1 for
the reason given above; F-7 now fails mid-stream as well. Both pins were strengthened before
this table was taken.

### 8.2 Readings acted on (no halt)

- **Files beyond the round's list.** `error.rs` (the three variants, as round 2 did for its
  own), `copy_binary.rs` (`ProtocolViolation::UnexpectedResponse`), `ident.rs`
  (`QualifiedRelation::parse`, a C-2b file) and `tests/it/read.rs` (the pure pins, so
  `live_pg.rs` holds only live cells) sit outside the brief's narrow list. Sketch §2.2 assigns
  the variants to `error.rs`. `error.rs` sits at its 260-line ceiling, so the server-version
  message is one line and `PermissionDenied` carries a relation, not an `Option`.
- **`42501` in query mode stays `Server`.** The scan cannot name a relation of the user's SQL,
  and the server's text names it. Relation mode checks both privileges at discovery, as Spark's
  schema probe fails at resolution, and maps a scan-time `42501` (a grant revoked after
  planning) to `PermissionDenied { Select }`.
- **The server floor lives in discovery.** Sketch §2.4 says the version is read "on connect".
  The startup packet does not carry `server_version_num`, so reading it there costs a query per
  connection. Every scan resolves first (FL-7) through the same pool, so the discovery
  transaction reads it with the encoding in the same round trip.
- **F-3's statement.** The sketch's `SELECT pg_sleep(3), 1` refuses at resolution, because
  `pg_sleep` returns `void`, which is unmapped. The cell uses `SELECT 1 AS one FROM
  pg_catalog.pg_sleep(3)` and a stall after 100 000 streamed rows.
- **F-8's write.** A `query` of `DELETE … RETURNING` never executes. Wrapped as
  `(…) AS repark_q` it is a syntax error at discovery, so it cannot reach the read-only check.
  The pin keeps that refusal and adds a SQL function that deletes, which executes inside the
  scan and refuses `25006` under `default_transaction_read_only`.
- **Streaming fixtures.** `generate_series` in `FROM` materialises the whole set before the
  first row, which kept a dropped scan's backend alive for about 20 s after its socket closed. The cells
  call it in the target list, which streams.
- **Discovery's lock wait** stays the session's `lock_timeout_ms`. Only `statement_timeout`
  is raised to 30 s for the transaction. A `query` whose parse waits on a lock times out like a
  scan.
- **No source name and no memory reservation.** As in rounds 1b and 2, errors carry no
  source (C-2d). The `MemoryReservation` around `buffered_bytes()` is C-2c's
  `PostgresScanExec`. The stream yields plain batches.

### 8.3 Four-line record (2026-10-07)

| id | date | question | Flink | Spark | default acted on |
|---|---|---|---|---|---|
| FL-15 | 2026-10-07 | Which schema does an unqualified `dbtable` name? | The Postgres JDBC catalog's table path defaults the schema to `public`. | The name goes into the SQL text and the server's `search_path` resolves it (by default `"$user"`, then `public`). | `public`. The read path pins an empty `search_path` (§2.4), so the server cannot resolve it, and a role-dependent lookup is hidden state. The name is matched exactly (FL-14). |

### 8.4 Gates (round 3)

Run on the finished tree. Each cargo command ran under the build-slot lock; exit codes are
verbatim.

| command | exit | output |
|---|---|---|
| `cargo deny check 2>&1 \| tail -5` | 0 | `advisories ok, bans ok, licenses ok, sources ok` |
| `cargo test -p repark-connect` | 0 | 84 passed, 14 ignored (the live cells), 0 failed (77 at round 2, plus the seven `read.rs` pins) |
| `cargo test -p repark-connect --test it live_pg -- --ignored` under `make pg-up` | 0 | 14 passed, on six runs of the final cells (three before the docs commit, three on it) |
| `cargo test -p repark-connect --test it live_pg::idle -- --ignored` without `REPARK_PG_URL` | 101 | the cell panics: "live cells need REPARK_PG_URL: run make pg-up" |
| `cargo build -p repark-connect --no-default-features` | 0 | the pure core builds without the driver |
| `cargo clippy -p repark-connect --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 | no diagnostics, with and without default features |
| `make rust-clippy` | 0 | workspace, all targets, no diagnostics |
| `cargo fmt --check` | 0 | no output |
| `make rust-panic-ban` | 0 | clean; no new spawn |
| `python3 scripts/check_rust_file_size.py` | 0 | 1061 files clean |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 366 maps clean |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | no output (on the docs commit) |
| `python3 scripts/check_docs_links.py` | 0 | 1340 files, 7202 links clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 303 live ledgers clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xc2b origin/main HEAD` | 0 | `comment-ban hits=0` |

A cell that panics leaves its `c2_<tag>` schema (and the grant cell's role) behind. The mutation
runs left seventy schemas and eight roles in the container, which `make pg-down` removes with
its volume. The cells never touch another schema.

## 9. C-2b fold 1 — the verifier's S1s and S2s (2026-10-07)

**Branch:** `feat/c-2b-postgres-connection` from `a8a90081` (round 3, PR #982). **Model:** Claude
Opus 5.5 (`claude-opus-5-5`, high). **Scope:** the verifier's FAIL verdict on #982 under the
orchestrator's pre-made rulings X1–X7 and the S3 items. The live cells ran against `make pg-up`
(PostgreSQL 16) under a private `XDG_RUNTIME_DIR`, torn down with `make pg-down` at the end.

### PROPOSITION LEDGER — C-2b fold 1 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-049 | X1 (S1): `take_url` splits the userinfo off at the last `@` before the first `/` after the scheme, before it looks for `?`, as libpq does. A password holding `?`, `@`, `#` or `:`, raw or percent-encoded, parses as that password with the host, port, database and query intact; `/` must be `%2F`. No refusal or `Debug` echoes userinfo or value text: an unknown key inside the `url` query refuses as `Spelling::UrlPart("query key")` and a malformed query value as `UrlPart("query")`. | `url.rs::the_userinfo_ends_at_the_last_at_before_the_first_slash`, `::no_userinfo_or_password_text_is_echoed_by_a_refusal` (the verifier's leaked-fragment URL and its `jdbc:` `password=…&leakedtail=x`); mutations m69, m70. | PROVEN | §9.1. |
| C-050 | X7: `redact_source_prop` masks every password the parser takes. On a Postgres URL it masks the userinfo where `take_url` splits it (the password, or the whole userinfo without `:`) and every query value whose key, percent-decoded, is secret (`pass%77ord`); then `redact_value` runs as before. | `url.rs::redaction_masks_every_password_the_parser_takes`; mutations m72, m73. | PROVEN | §9.1. |
| C-051 | S3: a `%` escape needs two ASCII hex digits, checked with `is_ascii_hexdigit` before parsing, so `%+A`, `%-1`, `% A` and a short escape refuse `UrlViolation::PercentEncoding` in the userinfo, a URL part or a query value, echoing nothing. | `url.rs::a_percent_escape_needs_two_hex_digits`; mutation m71. | PROVEN | §9.1. |
| C-052 | X2 (S1): `release_clean().await` resets the session before pooling: `RESET_SESSION` (`CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; UNLISTEN *; SELECT pg_catalog.pg_advisory_unlock_all(); DISCARD PLANS; DISCARD TEMP; DISCARD SEQUENCES`), then one simple query reading every startup pin from `pg_settings` with `statement_timestamp() = transaction_timestamp()`. Any mismatch, an open transaction, an error or the read timeout drops the connection. On `pool_max_size = 1`, after each verifier poison (`IntervalStyle`, `default_transaction_read_only`, `search_path`, an advisory lock) the next lease runs on the same backend with the interval as `1 day 02:00:00`, the pushed compare matching, the writer refused `25006` with nothing written, and the lock released. | `live_pool.rs::a_pooled_connection_is_reset_before_reuse`, `::user_types_resolve_after_a_reset`; mutations m74–m77. | PROVEN | §9.1. The ruling named `DISCARD ALL`; its `DEALLOCATE ALL` breaks the driver (§9.2 R-1, question Q1 in the hand-back). |
| C-053 | X5: a pushed scan's client is pooled only after `COMMIT`. On `pool_max_size = 1` two pushed query-mode scans run on one backend, each sees `pg_current_xact_id_if_assigned()` NULL, and between them `pg_stat_activity` shows that backend `idle`, not `idle in transaction`. | `live_pool.rs::a_pushed_scan_commits_before_its_connection_is_pooled`; V-M2 red. | PROVEN | §9.1. The xid is NULL in any read-only transaction; the state and the backend identity are the teeth (§9.2 R-7). |
| C-054 | X3: a lease dropped without `release_clean` (a `within()` timeout, an error, a dropped scan) fires `CancelToken::cancel_query` on a spawned task bounded by `connect_timeout_ms`, under the connector's TLS, and aborts the connection task. The startup packet adds `client_connection_check_interval = 1000` (`CONNECTION_CHECK_INTERVAL`; PostgreSQL 14 is the floor), which the release check reads too. Live: three read timeouts at 300 ms each leave no busy backend within 400 ms; a scan dropped while the server computes leaves none within 3 s; `SHOW` gives `1s`. | `live_pool.rs::a_timeout_or_a_drop_ends_the_server_work`, `pool.rs::query_config_pins_the_session_in_the_startup_packet`; mutations m79, m80, m81. | PROVEN | §9.1. Measured: the cancel ends the backend in 3–16 ms; the check interval alone in about 700 ms at a 300 ms timeout. |
| C-055 | X4: at connect, a server error in SQLSTATE class `28` (`28P01`, `28000`) classifies as `AuthenticationFailed`, before the `Server` fallback; the refusal echoes no password. | `live_pool.rs::a_wrong_password_is_authentication_failed`; mutation m78. | PROVEN | §9.1. Corrects C-035's note. |
| C-056 | X6: query mode resolves unqualified names as Spark's `query` does. A query-mode scan always opens `BEGIN READ ONLY` and, in the same batch, `QUERY_SEARCH_PATH` (`SET LOCAL search_path TO "$user", public`); query discovery adds it to `BEGIN_DISCOVERY`. Relation mode and the session keep the empty pin. Every generated compare is `OPERATOR(pg_catalog.op)` and every cast stays `pg_catalog`-qualified, so a user `=` on a domain and a user `<` on `int4` in `public` change no pushed result. | `live_pool.rs::query_mode_resolves_unqualified_names_through_the_role_search_path`, `::a_user_operator_cannot_shadow_a_generated_compare`, `read.rs::pushed_values_ride_set_config_never_the_statement_text`, `::query_mode_wraps_the_statement_and_an_empty_projection_selects_nothing`; mutations m82–m85. | PROVEN | §9.1. The plain `=` returned 0 rows under the query path (§9.2 R-2). |
| C-057 | S3, recorded (no code): a column retyped between resolution and scan reads through the per-column assignment cast, which rounds silently on a narrowing (`1.5` → `2` into `int4`, `1.2345` → `1.23` into `numeric(10,2)`), as sketch F-7 accepts. Registry row `CONNECT-DECL-pg-drift-cast` declares it. | The registry row; `live_pg.rs::schema_drift_between_plan_and_scan_fails_loud_or_stays_typed` pins the widening and the loud failures. | PROVEN | The rounding itself is the verifier's VL-DRIFT2 reading, recorded, not pinned. |
| C-058 | S3: an `sslrootcert` bundle with one valid CA and one certificate rustls cannot take as a trust anchor refuses `RootCertInvalid` (`ignored > 0`). | `tls.rs::a_bundle_with_one_unusable_certificate_refuses`; V-M7 red. | PROVEN | §9.1. |
| C-059 | S3: the scan checks the COPY trailer. Through `scan()` against a loopback fake backend, a one-row binary COPY with its trailer reads `[7]`, and the same stream without it fails `Disconnected`. | `scan.rs::a_copy_stream_without_its_trailer_fails_the_scan`; V-M4 red. | PROVEN | §9.1. The fake refuses the release query, so neither client is pooled. |
| C-060 | S3, recorded (no code): an idle connection whose backend dies just before checkout is handed out, because the health check is `is_closed()` alone; the scan fails `Disconnected` (the verifier's `vl_health_current_thread`). Accepted under FL-11: nothing retries inside a scan, and the failure is retryable. | FL-11 (§2); `pool.rs::closed_and_idle_expired_connections_are_never_reused` pins what the check does catch. | PROVEN | A validation round trip at checkout would close the race at a round trip per lease. |
| C-061 | Shape and files: `PoolConnection` gains `reset` (default `true`) and `canceller` (default `None`); `Canceller`, `RESET_SESSION`, `CONNECTION_CHECK_INTERVAL` and `QUERY_SEARCH_PATH` are public; `release_clean` is `async`; no `ConnectError` variant changes. File sizes: `settings/postgres.rs` 605, `pool.rs` 527, `read/postgres.rs` 420, `discover.rs` 349, `tests/it/live_pg.rs` 742, `live_pool.rs` 340, `url.rs` 209, `scan.rs` 144, all under the 1000-line gate. No dependency change. No code comments. Every gate in §9.4 passes. | §9.4. | PROVEN | `settings/postgres.rs` and `pool.rs` pass their round-1b and round-2 brief ceilings (560, 450); the mechanical gate is 1000 (§9.2 R-9). |

### 9.1 Mutations (fold 1)

Each mutation was applied alone to the committed code, the named pins run (the live ones with
`--include-ignored` against the container), and the file restored with `git checkout`. The
verifier's survivors keep their ids.

| id | clause | mutation (file) | red? | red in |
|---|---|---|---|---|
| m69 | C-049 | the round-3 split order: `?` first, then the authority (`src/settings/postgres.rs`) | RED | `the_userinfo_ends_at_the_last_at_before_the_first_slash`, `no_userinfo_or_password_text_is_echoed_by_a_refusal` |
| m70 | C-049 | an unknown `url` query key echoes its name (`src/settings/postgres.rs`) | RED | `no_userinfo_or_password_text_is_echoed_by_a_refusal` (the `leakedtail` refusal) |
| m71 | C-051 | drop the `is_ascii_hexdigit` check (`src/settings/postgres.rs`) | RED | `a_percent_escape_needs_two_hex_digits` (`%+A` accepted) |
| m72 | C-050 | match the raw query key, not decoded (`src/settings/postgres.rs`) | RED | `redaction_masks_every_password_the_parser_takes` (`pass%77ord`) |
| m73 | C-050 | the round-3 seam, `redact_value` alone (`src/settings/postgres.rs`) | RED | `redaction_masks_every_password_the_parser_takes` |
| m74 | C-052 | skip the reset batch, keep the check (`src/pool.rs`) | RED | `a_pooled_connection_is_reset_before_reuse`: the check drops the poisoned client, so the next lease runs on another backend |
| m75 | C-052 | neither reset nor check: release always pools (`src/pool.rs`) | RED | `a_pooled_connection_is_reset_before_reuse` (the interval reads in `sql_standard`) |
| m76 | C-052 | reset, then pool without the check (`src/pool.rs`) | GREEN on C-052's pins (equivalent there: the reset restores every pin) | RED with V-M2 in `a_pushed_scan_commits_before_its_connection_is_pooled` (`idle in transaction`) |
| m77 | C-052 | the ruling's literal `DISCARD ALL` (`src/pool.rs`) | RED | `user_types_resolve_after_a_reset`: `26000 prepared statement "s7" does not exist` |
| V-M2 | C-053 | `finish()` skips `COMMIT` (`src/read/postgres.rs`) | RED | `a_pushed_scan_commits_before_its_connection_is_pooled` (the release check drops the client: idle count 0); with the check's transaction column removed too, RED at the `idle` state |
| m78 | C-055 | class `28` not split: `Server` (`src/pool.rs`) | RED | `a_wrong_password_is_authentication_failed` |
| m79 | C-054 | no cancel: the lease drops its `Canceller` (`src/pool.rs`) | RED | `a_timeout_or_a_drop_ends_the_server_work` at the 400 ms bound (green under a 3 s bound alone, which the check interval meets in about 700 ms; the pin was tightened before this table was taken) |
| m80 | C-054 | no `client_connection_check_interval` pin (`src/pool.rs`) | RED | `a_timeout_or_a_drop_ends_the_server_work` (`SHOW` is not `1s`), `query_config_pins_the_session_in_the_startup_packet` |
| m81 | C-054 | neither the cancel nor the check pin (`src/pool.rs`) | RED | `a_timeout_or_a_drop_ends_the_server_work` (the backend sleeps on) |
| m82 | C-056 | no `SET LOCAL search_path` in the scan (`src/read/postgres.rs`) | RED | `query_mode_resolves_unqualified_names_through_the_role_search_path` (`42P01`) |
| m83 | C-056 | no `SET LOCAL search_path` in query discovery (`src/discover.rs`) | RED | `query_mode_resolves_unqualified_names_through_the_role_search_path` (`42P01`) |
| m84 | C-056 | a plain `=` for `CompareOp::Eq` (`src/read/postgres.rs`) | RED | `a_user_operator_cannot_shadow_a_generated_compare` (0 rows), `pushed_values_ride_set_config_never_the_statement_text` |
| m85 | C-056 | the query search path in relation mode too (`src/discover.rs`) | RED | `query_mode_wraps_the_statement_and_an_empty_projection_selects_nothing`; every live cell green, since the compares are qualified |
| V-M4 | C-059 | the scan ignores `decoder.finish()` (`src/read/postgres.rs`) | RED | `a_copy_stream_without_its_trailer_fails_the_scan` (the scan ends cleanly, the row missing) |
| V-M7 | C-058 | `trusted_roots` drops `\|\| ignored > 0` (`src/tls.rs`) | RED | `a_bundle_with_one_unusable_certificate_refuses` |

Nineteen are red, and one, m76, is equivalent on its own clause's pins and red under V-M2. Every
verifier survivor (V-M2, V-M4, V-M7) is red.

### 9.2 Readings acted on (no halt)

- **R-1, `DISCARD ALL` (X2).** The ruling's `DISCARD ALL` runs `DEALLOCATE ALL`, which drops the
  named statements `tokio-postgres` 0.7.18 keeps for type lookups (`typeinfo`,
  `typeinfo_enum`, `typeinfo_composite`), and the client has no call to forget them. The next
  query-mode discovery of a type the client has not cached failed `26000 prepared statement
  "s7" does not exist` (m77). The fold runs the documented `DISCARD ALL` sequence without
  `DEALLOCATE ALL`, and the check's `statement_timestamp() = transaction_timestamp()` keeps the
  ruling's "outside any transaction" (a multi-statement batch inside an open transaction would
  otherwise reset in place). Pins come back, advisory locks are released, temp objects,
  sequences state, cursors and plans are dropped; a prepared statement survives only if
  something made one by name, which user SQL wrapped in `SELECT * FROM (…)` cannot. Asked as
  Q1 in the hand-back, lean: keep.
- **R-2, qualified operators (X6).** Under `"$user", public` a `public` `=` on a domain shadowed
  the generated compare (0 rows instead of 1): an exact match on the domain type beats
  `pg_catalog`'s base-type operator. Same-signature operators did not shadow. Every compare now
  names `OPERATOR(pg_catalog.op)`, in relation mode too.
- **R-3, query key names (X1).** The ruling bars echoing any value. A raw `&` in a password
  (`password=abc&leakedtail=x`) makes its tail a key name, which libpq and pgjdbc split the
  same way; the refusal now names "the query key in `url`" and lists the accepted keys. Two
  `settings.rs` expectations changed with it.
- **R-4, a URL with no `/`.** The userinfo rule scans to the first `/` or the end, so
  `postgresql://h?application_name=a@b` reads `h?application_name=a` as userinfo, as libpq's
  look-ahead (to `@` or `/`) does. A URL with a path is unaffected.
- **R-5, servers older than 14.** `client_connection_check_interval` is new in 14, so an older
  server now refuses at connect with its own `Server { 42704 }` error naming the parameter,
  before discovery can name `CONNECT-DECL-pg-server-version`. The registry row says so; the
  pure pin `servers_older_than_14_are_declared` holds.
- **R-6, the cancel task.** The crate's second `tokio::spawn`, under `#[expect]` with its
  lifecycle stated: it holds no client, lock or permit, and `connect_timeout_ms` bounds it. It
  is skipped when no runtime is current (a drop outside one).
- **R-7, X5's transaction id.** `pg_current_xact_id_if_assigned()` is NULL in any read-only
  transaction, open or not, so the pin's teeth are the backend's `idle` state between scans
  and the reuse of one backend.
- **R-8, the release check's forms.** `pg_settings.setting` shows `DateStyle` as `ISO, MDY` and
  an empty `search_path` as `""`; the check compares the first `DateStyle` field and reads `""`
  as empty. Each release costs two round trips.
- **R-9, files.** `settings/postgres.rs` (605) and `pool.rs` (527) pass the round-1b and round-2
  brief ceilings; the mechanical gate is 1000. The fold's pins sit in new files (`url.rs`,
  `live_pool.rs`, `scan.rs`) so `settings.rs` stays at 599 and `live_pg.rs` at 742.
- **R-10, shared objects.** The X6 pins create a domain, two functions and two operators in
  `public` with the cell's tag and drop them after the reads; a panicking cell leaves them in
  the disposable container.

### 9.3 Four-line record (2026-10-07)

| id | date | question | Flink | Spark | default acted on |
|---|---|---|---|---|---|
| FL-16 | 2026-10-07 | Through which `search_path` does a `query` read resolve? | The JDBC connector sends the query as given; the session's path resolves it. | The `query` option is wrapped and run on the session, so the role's `search_path` (`"$user", public` by default) resolves it. | `"$user", public`, set with `SET LOCAL` inside the query's own read-only transaction (ruling X6); relation mode keeps the empty pin (FL-15). |

### 9.4 Gates (fold 1)

Run on the finished tree. Each cargo command ran under the build-slot lock.

| command | exit | output |
|---|---|---|
| `cargo deny check 2>&1 \| tail -5` | 0 | `advisories ok, bans ok, licenses ok, sources ok` |
| `cargo test -p repark-connect` | 0 | 90 passed, 21 ignored (the live cells), 0 failed (84 at round 3, plus `url.rs`'s four, `scan.rs`'s one and `tls.rs`'s one) |
| `cargo test -p repark-connect --test it live_ -- --ignored` under `make pg-up` | 0 | 21 passed (round 3's 14 and `live_pool.rs`'s 7), on six full runs of the final cells |
| `cargo build -p repark-connect --no-default-features` | 0 | the pure core builds without the driver |
| `cargo clippy -p repark-connect --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 | no diagnostics, with and without default features |
| `make rust-clippy` | 0 | workspace, all targets, no diagnostics |
| `cargo fmt --check` | 0 | no output |
| `make rust-panic-ban` | 0 | clean; the cancel spawn carries its `#[expect]` |
| `python3 scripts/check_rust_file_size.py` | 0 | 1064 files clean |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 366 maps clean |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | no output (on the docs commit) |
| `python3 scripts/check_docs_links.py` | 0 | 1340 files, 7202 links clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 303 live ledgers clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xc2b origin/main HEAD` | 0 | `comment-ban hits=0` |

## 10. C-2b fold 2 — the re-verify's S2 and S3s (2026-10-07)

**Branch:** `feat/c-2b-postgres-connection` from `e1371a8e` (fold 1, PR #982). **Model:** Claude
Opus 5.5 (`claude-opus-5-5`, high). **Scope:** the re-verify's PASS verdict on fold 1 and its
one S2 and four S3s, under the orchestrator's rulings Z1–Z5. The live cells ran against
`make pg-up` (PostgreSQL 16.15), torn down with `make pg-down` at the end.

### PROPOSITION LEDGER — C-2b fold 2 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-062 | Z1 (S2): query mode honours the configured search path. `QUERY_SEARCH_PATH` is one constant statement in the query's read-only transaction (scan and discovery) that reads the login role's `search_path` from `pg_db_role_setting`, keyed on `session_user` and `current_database()`, in the server's precedence (role in database, role, database, all roles), falls back to the built-in `"$user", public`, and applies it with `set_config('search_path', …, true)`. In a fresh database a role with `ALTER ROLE … SET search_path = app, public` reads `app.vt` over `public.vt` and the database's own `db` path; with no role setting the database's path wins; `ALTER ROLE … IN DATABASE` beats both; with none set the read is `public`'s. Each time query mode returns the rows a plain session as that login returns, and a pushed `int4` compare (`OPERATOR(pg_catalog.=)`) matches one row. | `live_pool.rs::query_mode_reads_the_configured_search_path_as_a_plain_session_does`, `read.rs::query_mode_wraps_the_statement_and_an_empty_projection_selects_nothing`; mutations m86, m87, m88. | PROVEN | §10.1. Supersedes C-056's `"$user", public` statement and FL-16's default (§10.2 R-11). |
| C-063 | Z2 (S3): the reset ends any `role` or `session_authorization` a query set. `RESET_SESSION` runs `SET SESSION AUTHORIZATION DEFAULT; RESET ROLE` (both outside `RESET ALL`, whose reach excludes them), and `SessionPins::hold` also requires `current_user` = `session_user` = the login role (`settings.user`) on every check row. On `pool_max_size = 1` a query-mode `set_config('role', <NOLOGIN role>, false)`, then a `set_config('session_authorization', …, false)` by the superuser login, each leave the next lease on the same backend reading `current_user` = `session_user` = the login and `role` = `none`. | `live_pool.rs::a_pooled_connection_is_reset_before_reuse`; mutations m89, m90, m91. | PROVEN | §10.1. The re-verify's R-M2 survivor is m89, now red. |
| C-064 | Z3 (S3): a prepared statement the driver did not make is deallocated before the client is pooled. After `RESET_SESSION`, one bound `query_typed` lists every `pg_prepared_statements` name not matching the driver's `^s[0-9]+$`, and one batch runs `DEALLOCATE <name>` per name, quoted through `PgIdent`; the check then counts names outside that form and any count but `0` drops the connection, as does a failed `DEALLOCATE`. On `pool_max_size = 1` a query-mode call of an existing plpgsql function that runs `PREPARE vprep` and `PREPARE "v Prep""q"` leaves the next lease on the same backend with neither statement listed, and the driver's own type-lookup statements survive. | `live_pool.rs::a_pooled_connection_is_reset_before_reuse`, `::user_types_resolve_after_a_reset`; mutations m92, m93, m94, m95. | PROVEN | §10.1. Closes the residue C-052 recorded beside Q1. |
| C-065 | Z4 (S3): `take_url` refuses a URL with no `/` after the scheme whose userinfo (the text before the last `@` before the first `/`) holds `?` or `=`, with `InvalidSpecification { key: UrlPart("userinfo"), reason: Url(AmbiguousUserinfo) }`; the refusal names the position only and echoes no text of the URL. `postgresql://h?user=u&password=S3@CRETpw` (the re-verify's shape, which read host `CRETpw` with no password) and its `jdbc:` form refuse; the same URL with a `/db` path, a userinfo with `%3F` and `%3D`, a trailing `/`, or a raw `@` alone parses with the password intact. | `url.rs::a_userinfo_holding_a_query_mark_without_a_path_refuses`, `::the_userinfo_ends_at_the_last_at_before_the_first_slash`; mutations m96, m97, m98, m99. | PROVEN | §10.1. Narrows C-049 for path-less URLs (§10.2 R-14). |
| C-066 | Z5 (S3), no code change: a pushed ordered compare on an enum column orders by the label's text (`m::pg_catalog.text OPERATOR(pg_catalog.>) …::pg_catalog.text`), where Spark's JDBC pushdown compares in the enum's order. Over `('sad', 'ok', 'happy')` a pushed `> 'ok'` returns `sad` and `= 'happy'` returns `happy`, in relation and query mode, while the server's own `m > 'ok'` returns `happy`. Registry row CONNECT-DIV-pg-enum-compare declares it. | `live_pool.rs::a_pushed_enum_compare_orders_by_text`; mutation m100. | PROVEN | §10.1. The result equals the `Utf8` column's own compare after the read. |
| C-067 | Shape and files: `UrlViolation` gains `AmbiguousUserinfo`; `QUERY_SEARCH_PATH` keeps its name and its place in both batches, now holding the catalog statement; `RESET_SESSION` adds `RESET ROLE`; `SessionPins` carries the login; no `ConnectError` variant and no public signature changes. File sizes: `settings/postgres.rs` 614, `pool.rs` 556, `discover.rs` 359, `tests/it/live_pool.rs` 563, `url.rs` 268, `read.rs` 233, all under the 1000-line gate. No dependency change (`query_typed` and `types::Type` are in the pinned `tokio-postgres` 0.7.18). No code comments. Every gate in §10.3 passes. | §10.3. | PROVEN | — |

### 10.1 Mutations (fold 2)

Each mutation was applied alone to the code, the named pins run with `--include-ignored`
against the container, and the file restored from a copy taken before the edit. Fourteen are
red and one, m91, is equivalent. The mutations that move a pinned identity or statement
(m89, m90, m92, m93) were run again after the reset pin's steps moved into a helper, red
again.

| id | clause | mutation (file) | red? | red in |
|---|---|---|---|---|
| m86 | C-062 | the built-in `"$user", public` always wins: it is `COALESCE`'s first argument (`src/discover.rs`) | RED | `query_mode_reads_the_configured_search_path_as_a_plain_session_does` (the role reads `public`) |
| m87 | C-062 | the precedence flipped: database before role (`ORDER BY s.setdatabase = 0, s.setrole = 0`, `src/discover.rs`) | RED | `query_mode_reads_the_configured_search_path_as_a_plain_session_does` (the role reads `db`) |
| m88 | C-062 | database-scoped rows ignored (`s.setdatabase IN (0)`, `src/discover.rs`) | RED | `query_mode_reads_the_configured_search_path_as_a_plain_session_does` (the database-level path is missed) |
| m89 | C-063 | the re-verify's R-M2 and the ruling's mutation: drop `SET SESSION AUTHORIZATION DEFAULT` and `RESET ROLE` (`src/pool.rs`) | RED | `a_pooled_connection_is_reset_before_reuse`: the check sees `current_user` = the poisoned role and drops the client, so the next lease runs on another backend |
| m90 | C-063 | m89, and `hold` no longer compares `current_user` and `session_user` (`src/pool.rs`) | RED | `a_pooled_connection_is_reset_before_reuse` (the next lease reads `current_user` = the poisoned role) |
| m91 | C-063 | drop `RESET ROLE` alone (`src/pool.rs`) | GREEN (equivalent: `SET SESSION AUTHORIZATION DEFAULT` also sets `role` back to `none`) | — |
| m92 | C-064 | skip the `DEALLOCATE` batch (`src/pool.rs`) | RED | `a_pooled_connection_is_reset_before_reuse`: the check counts two foreign statements and drops the client, so the next lease runs on another backend |
| m93 | C-064 | m92, and the check ignores the count (`src/pool.rs`) | RED | `a_pooled_connection_is_reset_before_reuse` (the next lease lists both statements) |
| m94 | C-064 | `DEALLOCATE` unquoted (`src/pool.rs`) | RED | `a_pooled_connection_is_reset_before_reuse` (`v Prep"q` is a syntax error, the reset fails and the client is dropped) |
| m95 | C-064 | the driver's form matches nothing (`^$`), so its own statements are deallocated too (`src/pool.rs`) | RED | `user_types_resolve_after_a_reset` (`26000`, as fold 1's m77) |
| m96 | C-065 | no refusal (`src/settings/postgres.rs`) | RED | `a_userinfo_holding_a_query_mark_without_a_path_refuses`, `the_userinfo_ends_at_the_last_at_before_the_first_slash` |
| m97 | C-065 | refuse `?` only, not `=` (`src/settings/postgres.rs`) | RED | `a_userinfo_holding_a_query_mark_without_a_path_refuses` (`u:S3CRET=pw@h:6543`) |
| m98 | C-065 | refuse with a path too (`src/settings/postgres.rs`) | RED | `a_userinfo_holding_a_query_mark_without_a_path_refuses`, `the_userinfo_ends_at_the_last_at_before_the_first_slash`, `no_userinfo_or_password_text_is_echoed_by_a_refusal`, `redaction_masks_every_password_the_parser_takes` |
| m99 | C-065 | the refusal names the userinfo's text as its key (`src/settings/postgres.rs`) | RED | `a_userinfo_holding_a_query_mark_without_a_path_refuses`, `the_userinfo_ends_at_the_last_at_before_the_first_slash` |
| m100 | C-066 | the pushed enum operand compares the bare column, no text cast (`src/read/postgres.rs`) | RED | `a_pushed_enum_compare_orders_by_text` (`42883`, no `enum > text` operator), `read.rs::pushed_values_ride_set_config_never_the_statement_text` |

### 10.2 Readings acted on (no halt)

- **R-11, the configured path (Z1).** The ruling's "apply with `SET LOCAL search_path`" is
  `set_config('search_path', <value>, true)`, the same transaction-local set, because the value
  comes from the catalog in the same statement and is never spliced into SQL text. The read
  and the set ride the batch that opens the transaction, so query mode adds no round trip. The
  statement is constant: the role and database are `session_user` and `current_database()`,
  which after the reset (C-063's check) are the login role and the configured database.
  Beside the ruling's three levels it reads the all-roles row (`ALTER ROLE ALL SET`), which the
  server applies last, before the built-in default. It does not see a server-wide value from
  `postgresql.conf` or `ALTER SYSTEM`: the startup pin replaces it and `pg_settings.reset_val`
  then shows the pin; `pg_file_settings` needs superuser. `pg_catalog` stays first unless the
  configured path names it later, as in a plain session; every generated cast and compare is
  `pg_catalog`-qualified either way. A pooled connection rereads the path per transaction, so
  it follows an `ALTER ROLE` made after it connected, where a long-lived plain session keeps
  its login-time path. The registry rationale (CONNECT-DECL-pg-session-sql) and `map.md` say
  this, replacing fold 1's "as Spark's `query` resolves" for the built-in path alone.
- **R-12, a role with a configured `role` (Z2).** A login whose `ALTER ROLE … SET role = x`
  runs as `x` in a plain session and on a fresh repark connection alike, but after the reset
  `current_user` is `x`, not the login, so the check drops the connection at every release:
  such a source reads correctly, without pooling. The re-verify saw that setting persist
  consistently across the reset; the ruling's check now trades its reuse for a known identity.
- **R-13, the statement sweep (Z3).** The list is one `query_typed` round trip (the unnamed
  statement, so the sweep adds no name of its own), the `DEALLOCATE`s one more only when
  something is listed; a release now costs three round trips, four after a foreign
  `PREPARE`. A function-made statement whose name has the driver's form (`s12`) is kept, as
  the ruling scopes it; its worst effect is the spurious `42P05` the re-verify named, not
  wrong rows.
- **R-14, a path-less userinfo (Z4).** Fold 1's R-4 read `postgresql://h?application_name=a@b`
  as userinfo `h?application_name=a`, as libpq does; it now refuses. So do the raw
  `?`/`=` passwords of C-049's bare forms (`postgresql://u:S3CRET?leakedfragment=1@h`), which
  that pin now expects to refuse while their `%3F`/`%3D` forms and every form with a path still
  parse. "No path" is read as "no `/` after the scheme", so a trailing `/` counts as a path.
  `redact_source_prop` is unchanged: it still masks only the userinfo of such a URL, which the
  parser now never takes.

### 10.3 Gates (fold 2)

Run on the finished tree. Each cargo command ran under the build-slot lock.

| command | exit | output |
|---|---|---|
| `cargo deny check 2>&1 \| tail -5` | 0 | `advisories ok, bans ok, licenses ok, sources ok` |
| `cargo test -p repark-connect` | 0 | 91 passed, 23 ignored (the live cells), 0 failed (90 at fold 1, plus `url.rs`'s new pin) |
| `cargo test -p repark-connect --test it -- --ignored` under `make pg-up` | 0 | 23 passed (fold 1's 21, plus the search-path and enum pins), on three full runs |
| `cargo build -p repark-connect --no-default-features` | 0 | the pure core builds without the driver |
| `cargo clippy -p repark-connect --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 | no diagnostics, with and without default features |
| `make rust-clippy` | 0 | workspace, all targets, no diagnostics |
| `cargo fmt --check` | 0 | no output |
| `make rust-panic-ban` | 0 | clean; no new spawn in product code |
| `python3 scripts/check_rust_file_size.py` | 0 | 1064 files clean |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 366 maps clean |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | no output |
| `python3 scripts/check_docs_links.py` | 0 | 1340 files, 7202 links clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 303 live ledgers clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xc2b origin/main HEAD` | 0 | `comment-ban hits=0` |

The search-path pin creates a database and a role and drops both before its assertions; a
cell that panics earlier (as the mutation runs did) leaves them, with its schema, in the
disposable container, which `make pg-down` removes with its volume.

## 11. C-2c — the provider, pushdown and the EXPLAIN boundary (2026-10-07)

**Branch:** `feat/c-2c-pushdown-explain` from `cdca8173` (C-2b merged). **Model:** Claude Opus
5.5 (`claude-opus-5-5`, high). **Scope:** sketch §2.9 (pushdown), §2.10 (the EXPLAIN boundary,
CC-1), §4's C-2c rows, §5.2, §5.3, §7's C-2c slice and §8 D-M6. D-M6 ran first, before any
provider code.

### PROPOSITION LEDGER — C-2c — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-068 | D-M6 (sketch §8, halt rule H-DF): DataFusion 54.1 hands every conjunct a provider classes `Inexact` to `TableProvider::scan` while keeping it in a `FilterExec` above the scan, and passes `limit` to `scan` only when no filter remains above it. A conjunct classed `Exact` reaches `scan` and leaves no filter. | `explain.rs::datafusion_hands_inexact_filters_to_scan_and_withholds_limit`, a recording provider (column `a` `Exact`, the rest `Inexact`): `a > 1 AND b = 'x' LIMIT 5` → `scan([a > 1, b = 'x'], None)`; `b = 'x' LIMIT 5` → `scan([b = 'x'], None)`; `a > 1 LIMIT 5` → `scan([a > 1], Some(5))`. | PROVEN | §11.1. H-DF did not fire. |
| C-069 | The provider (sketch §2.8, §2.11, FL-1, FL-7, FL-8): `PostgresSource::mount(identity, props, localiser)` returns a `PostgresCatalog` with no I/O and no validation; the first resolution parses the props on the source's door, builds the connector and the source's one `QueryPool`, and memoises the outcome, a refusal included; `PostgresSchemaProvider::table` quotes both names through `PgIdent`, runs discovery afresh, answers `Ok(None)` for `RelationNotFound` and `DataFusionError::External(ConnectError)` otherwise; listing is empty (`CONNECT-DECL-pg-listing`); `PostgresSource::table(resolved)` builds a table from an injected resolution with no network; `Debug` shows no prop. | `explain.rs::listing_a_postgres_source_is_empty_and_declared`, `::explain_never_renders_endpoint`; live: every `live_pushdown.rs` cell mounts through `PostgresSource::mount` and reads `pg.<schema>.edges` through the schema provider; `::a_missing_relation_is_table_not_found_live`. | PROVEN | §11.3 m129, m130; §11.4. |
| C-070 | The classifier (sketch §2.9, P-1…P-10 and the residual list): a column's class follows its mapping, cast and surfaced type; the null tests push on every mapped column; booleans, integers (the literal's own width; a widening cast dropped), exact decimals (a lossless widening dropped; a column whose Arrow type is not its own `(p,s)` compares cast to the Arrow type), dates, `timestamptz`, NTZ `timestamp` and UTF8 `text`/`varchar` (`COLLATE pg_catalog."C"`) push `=`, `<>`, `<`, `<=`, `>`, `>=` and `IS [NOT] DISTINCT FROM` against a literal on either side, `IN` up to 256 items, `BETWEEN`, well-formed `LIKE` and `AND`/`OR`/`NOT` over pushable children; every compare is `OPERATOR(pg_catalog.op)` and every value rides `pg_catalog.current_setting('repark.pN')::pg_catalog.<type>`; float, `bpchar`, `bytea`, the text-rendered types, enums, a placed `timestamp`, functions, arithmetic, other casts, `CASE`, `COALESCE`, `ILIKE`, regular expressions, a temporal literal outside Postgres's range and a text holding NUL stay residual. | `pushdown.rs::p01_null_tests_push` … `::p10_logic_pushes_only_exact_children`, `::r01_float_comparisons_stay_residual` … `::r04_functions_arithmetic_and_casts_stay_residual`; their live halves in `live_pushdown.rs`. | PROVEN | §11.3 m101–m117; §11.4. |
| C-071 | A filter is classed `Exact` only when it renders and DataFusion's simplifier leaves it unchanged. DataFusion keeps simplifying a scan's filters after it has removed the `Exact` ones from the plan: `qty NOT IN (1, NULL)` became a pushed `qty <> NULL`, later simplified to `NULL`, which the scan could not render and nothing applied, so the pushed scan returned five rows where the engine returns none. | `pushdown.rs::a_filter_the_optimizer_would_still_rewrite_stays_inexact`; live `live_pushdown.rs::p07_in_list_three_valued_live` (`qty NOT IN (1, NULL)` is empty both ways). | PROVEN | §11.3 m118; §11.2 R-1; §11.4. |
| C-072 | P-11: `scan` pushes `LIMIT n` only when none of the filters it receives is residual, whatever `limit` DataFusion passes. | `pushdown.rs::p11_limit_pushes_only_without_residual` (a direct `scan` call with a residual filter and `Some(5)` pushes no limit); live `::p11_limit_pushes_only_without_residual_live` (`score > 1 LIMIT 5` returns five rows over fifteen rows the filter drops first). | PROVEN | §11.3 m112 (red in the unit pin; the live half stays green because DataFusion already withholds the limit, C-068); §11.4. |
| C-073 | `pushdown_predicate = false` classes every conjunct `Inexact`: no filter pushes, and the rows are the engine's. *(Corrected by the C-2c fold 1, 2026-10-07: as first written it also said "no limit pushes"; the limit was gated on an empty residual alone, so an unfiltered `LIMIT 7` pushed. The limit now has its own switch, `pushdown_limit`, C-082; with `pushdown_predicate = false` a limit pushes only when the statement has no filter.)* | `pushdown.rs::r05_pushdown_predicate_false_pushes_nothing`; live `::r05_pushdown_predicate_false_pushes_nothing_live`, and every live cell asserts its unpushed half pushed nothing. | PROVEN | §11.3 m117; §11.4. |
| C-074 | The bounds (sketch §0 line 5): a conjunct whose rendering would bind past `MAX_PARAM_SLOTS` stays residual, an `IN` past 256 items stays residual, and a scan whose `Exact` conjuncts together bind more than 1024 values refuses the plan, naming the bound and `pushdown_predicate`, rather than drop a filter DataFusion has already removed. | `pushdown.rs::p07_in_list_three_valued` (256 bind, 257 stay residual), `::pushed_values_past_1024_fail_the_plan`. | PROVEN | §11.3 m125. |
| C-075 | The EXPLAIN boundary (sketch §2.10, §5.3, CC-1): per scan, `PostgresScanExec: source=…, relation=…` (or `query=…`), `projection=[…]`, `pushed_filters=[…]`, `residual_filters=[…]`, `pushed_limit=…`, the filter lists being what this statement pushed and left; `Verbose` adds `remote_sql=` with placeholders and `bound_values=N`, never a value; no format renders host, port, database, user, URL or a prop; the residual list is semantically equal to the `FilterExec` above the scan, and textually equal (the physical form of the list's conjunction) unless DataFusion's common-subexpression elimination rewrites that filter. *(Qualified by the C-2c fold 1, 2026-10-07, the verifier's S3: `COALESCE(qty, 0) > 2` lists `CASE WHEN CAST(qty AS Int64) IS NOT NULL …` while the `FilterExec` reads `CASE WHEN __common_expr_3@0 …` over a `ProjectionExec`; the rows match.)* | `explain.rs::explain_renders_pushed_and_residual_per_scan`, `::explain_verbose_shows_placeholders_never_values`, `::explain_never_renders_endpoint`, `::explain_residual_matches_filter_exec_above`. | PROVEN | §11.3 m121–m124. |
| C-076 | Metrics and memory (sketch §2.6, §2.10, CC-7): `PostgresScanExec` registers `output_rows`, `output_batches`, `elapsed_compute` (the decode), `bytes_received` and `time_to_first_byte`, which `EXPLAIN ANALYZE` shows, and charges every batch it yields to a `MemoryReservation` named `PostgresScan`. The scan's first poll opens the connection, so EXPLAIN opens none. | Live `live_pushdown.rs::explain_analyze_reports_rows_bytes_and_time_per_scan_live`, `::a_batch_past_the_memory_pool_is_resources_exhausted_live`; the EXPLAIN pins run with no server. | PROVEN | §11.3 m126, m127; §11.4. |
| C-077 | The localiser (sketch §2.7, §2.11): `WallClockLocaliser` is declared in connect; by default a `timestamp` column surfaces as `Timestamp(Microsecond, <zone_label>)`, read once per resolution, and each batch's wall clocks are placed by `localise`; `prefer_timestamp_ntz` keeps the wall clock; a compare on a placed column never pushes (`CONNECT-DIV-pg-timestamp-zone`). | `pushdown.rs::r03_ltz_timestamp_comparisons_stay_residual`; live `::timestamp_columns_are_placed_in_the_session_zone_live`, `::r03_ltz_timestamp_comparisons_stay_residual_live`. | PROVEN | §11.3 m115, m128; §11.4; §11.2 R-7. |
| C-078 | Text compares push under `COLLATE pg_catalog."C"` on a `UTF8` server only (`CONNECT-DIV-pg-text-collation`): code-point order and byte equality, whatever the column's collation; another encoding keeps them residual. | `pushdown.rs::p06_text_comparison_is_code_point_order`, `::p06b_text_equality_ignores_nondeterministic_collation`, `::p06c_nul_literal_stays_residual`; live halves over an ICU column and a nondeterministic one. | PROVEN | §11.3 m106, m107; §11.4. |
| C-079 | Sketch §5.2's live halves (§6), under `make pg-up`: over one seeded table holding every edge §5.2 names, every class and residual pin returns the same rows with `pushdown_predicate` on and off, with the expected pushed and residual split, including `int2 = 100000` (no error, no row), a literal past scale 18 against unconstrained `numeric`, `4714-11-24 BC` and `5874897-12-31` pushed as `date` text, the first instant pushed and one µs earlier residual, `'B' < 'a'` and the ICU and nondeterministic columns, `_` against `é`, `\%`, `-0 = 0` in `float8`, padded `char(5)` and `i64::MAX + 1`. D-M3: a pushed `current_setting` compare keeps a btree index (an index-only scan over one million rows). | The twenty-three `live_pushdown.rs` cells. | PROVEN | §11.4; every live mutation of §11.3 is red in its live half or named as unit-only. |
| C-080 | Shape and files: `src/provider.rs` and `provider/{catalog,schema,table,scan}.rs`, `src/pushdown.rs`, `tests/it/pushdown.rs`, `tests/it/explain.rs` and `tests/it/live_pushdown.rs` hold the ceilings of sketch §7; `read/postgres.rs` gains the crate-private `filter`, `bound_values`, `ScanMeter` and `scan_metered`, nothing else public changes; `async-trait` is the one dependency added; no code comments; every gate in §11.5 passes. | §11.5. | PROVEN | §11.5. |

### 11.1 D-M6, measured (2026-10-07)

| id | date | measurement | result | decides |
|---|---|---|---|---|
| D-M6 | 2026-10-07 | A minimal DataFusion 54.1 probe (`SessionContext`, one recording `TableProvider`, `EXPLAIN` and the physical plan of `SELECT a FROM t WHERE … LIMIT 5`): does `scan` receive `Inexact` filters, and does it receive `limit` while a filter remains above? | Yes to the first: the logical plan reads `TableScan: t projection=[a, b], full_filters=[t.a > Int32(1)], partial_filters=[t.b = Utf8("x")]` and `scan` receives both, unqualified. No to the second: with an `Inexact` conjunct the plan is `Limit` over `Filter` over `TableScan`, the physical `FilterExec: b@1 = x, projection=[a@0], fetch=5` carries the limit and `scan` sees `limit = None`; with only `Exact` conjuncts `scan` sees `Some(5)` and no filter remains. `OR` of two `Exact`-classed columns is one conjunct, classed as a whole. | The EXPLAIN boundary of sketch §2.10 holds: `scan` sees the full conjunct list, so it can list the residual filters, and P-11's limit arrives only when no residual remains (the scan re-checks anyway). H-DF does not fire. |

### 11.2 Readings acted on (no halt)

- **R-1, the optimizer rewrites pushed filters (C-071).** The first live run of
  `p07_in_list_three_valued_live` returned five rows pushed and none unpushed for
  `qty NOT IN (1, NULL)`. DataFusion's simplifier rewrote it to `qty <> 1 AND qty <> NULL`;
  `push_down_filter` offered both conjuncts, and both rendered (`NULL::pg_catalog.int4`), so
  both were classed `Exact` and left the plan. A later `simplify_expressions` pass, which maps a
  `TableScan`'s filters too, turned `qty <> NULL` into `NULL`; `scan` could not render that, so
  it listed it as residual while no `FilterExec` held it. The fix classes a filter `Exact` only
  when it renders **and** DataFusion's `ExprSimplifier` (over the table's unqualified schema)
  leaves it unchanged, so an `Exact` filter is a fixed point of the rewrite that runs after it.
  A filter that is not yet settled stays `Inexact`; the optimizer re-offers its settled form
  on the next pass. The guard relies on the simplifier being the only rule that rewrites a
  `TableScan`'s filters in DataFusion 54.1 (`SimplifyExpressions::optimize_internal` is; no
  other rule in the 54.1 optimizer maps them). A DataFusion upgrade re-reads that.
- **R-2, pushdown sits behind `postgres`.** Sketch §2.1 keeps the classifier outside the
  feature. It reads `ResolvedSource`, `ScanColumn` and `CastType`, which C-2b put in
  `discover.rs` behind the feature, and renders into `ScanRequest`. Moving those types out is a
  C-2b refactor this slice does not need; the classifier is still pinned with no driver
  connection and no network.
- **R-3, files outside the slice's list.** `read/postgres.rs` gains the crate-private
  `ScanRequest::filter` (the one way rendered SQL reaches the statement), `bound_values`,
  `ScanMeter` and `scan_metered` (the metrics need the chunk loop). `Cargo.toml` gains
  `async-trait` for the provider traits, already in the lock. Nothing else outside §7's C-2c
  rows changed.
- **R-4, `PostgresSource` lives at the crate root.** Sketch §2.11 writes
  `repark_connect::postgres::PostgresSource`; `repark_connect::postgres` is the type-map module.
  C-2d calls `repark_connect::PostgresSource::mount(&identity, props, localiser)`.
- **R-5, `LIKE` renders as `OPERATOR(pg_catalog.~~)`.** The `LIKE … ESCAPE '\'` keyword form
  resolves `~~` through the search path, which query mode sets to the role's; the qualified
  operator cannot be shadowed, and its escape is the backslash, as the keyword form's default
  is. So the sketch's p09 mutation, "omit `ESCAPE`", would be equivalent; m110 disables the
  escape instead (`like_escape(pattern, '')`). DataFusion 54.1 also treats the backslash as the
  escape and an escaped ordinary character as itself (`'ab' LIKE 'a\b'` is true in both). A
  pattern is pushed only when every backslash escapes `%`, `_` or a backslash; `LIKE … ESCAPE`
  with another character is refused by DataFusion itself.
- **R-6, decimals the Arrow type rounds.** An unconstrained `numeric` (and `numeric(p > 38)`)
  decodes rounded HALF_UP to its Arrow scale, so comparing the stored value would push a
  different predicate from the one the engine applies (`5e-19` reads as `1e-18`). Such a column
  compares as `"c"::pg_catalog.numeric(P,S)`, the Arrow type, which rounds the same way
  (`round_var`, half away from zero, C-009). A constrained `numeric(p ≤ 38, s)` compares bare,
  so its index stays usable. A value past the Arrow precision fails the cast in Postgres where
  the decoder would refuse it: a failure either way, with different text.
- **R-7, the localiser.** Core's session-zone localiser and its gap and overlap refusals are
  C-2d's (`session/zone_localiser.rs`); C-2c declares the trait, places each batch and names the
  column on a refusal it returns. The pins use `FixedZone` (`-05:00`), so the DST edge of r03
  is replaced by a fixed offset that moves every wall clock by five hours. `CONNECT-DECL-pg-timestamp`
  stays open, its rationale now naming C-2d as the retirement, because no door mounts the
  provider yet.
- **R-8, the memory charge is per batch.** The scan resizes its `PostgresScan` reservation to
  each batch it yields, not to the builders as they grow (C-006's `buffered_bytes()` seam would
  need the decoder inside the exec). A refused resize is DataFusion's resources-exhausted error
  naming the consumer, not the column. The metric names are DataFusion's `BaselineMetrics`
  (`output_rows`, `output_batches`, `elapsed_compute`) plus `bytes_received` and
  `time_to_first_byte`; the sketch's `batches` is `output_batches`.
- **R-9, bound values per scan.** A conjunct that would bind past 1024 values stays residual,
  but the classifier is per conjunct and stateless (DataFusion re-offers a subset on later
  passes, so a running budget would class one conjunct two ways). A scan whose `Exact`
  conjuncts together bind more than 1024 values therefore refuses the plan, naming the bound
  and `pushdown_predicate`, rather than drop a filter the optimizer has already removed.
- **R-10, EXPLAIN text.** `pushed_filters` shows the conjuncts' own displays, literals
  included, as the sketch's example does; the bound values never appear in `remote_sql`. A
  query-mode source renders `query=<the query>`.
- **R-11, D-M3.** On PostgreSQL 16, `EXPLAIN SELECT id FROM wide WHERE id OPERATOR(pg_catalog.=)
  pg_catalog.current_setting('repark.p0')::pg_catalog.int4` over one million rows plans
  `Index Only Scan using wide_id_idx on wide (cost=0.43..8.45 rows=1 width=4)` with
  `Index Cond: (id = (current_setting('repark.p0'::text))::integer)`: the carriage keeps the
  index, so Q2's lean (keep the carriage) needs nothing from the owner.
- **R-12, out of scope, observed.** DataFusion 54.1's unwrap-cast simplification rewrites
  `CAST(x AS BIGINT) > 1` on a `Decimal128(10,2)` column to `x > 1.00`, and
  `CAST(x AS DECIMAL(5,1)) > 1.5` to `x > 1.50`: over `1.50` and `1.54` the engine returns both
  rows where the cast values (`1`, `1.5`) exclude them. It happens before any provider sees the
  filter, so pushdown returns the engine's rows; it is an engine defect, not a C-2c one.

### 11.3 Mutations (C-2c)

Each mutation was applied alone to the committed code, the named pins run with
`--include-ignored` against the container, and the file restored from a copy taken before the
edit. In `live_pushdown.rs`, line 129 is the cell's `collect` failing on a server error, 136
the rows differing between pushdown on and off, and 138 the pushed and residual counts; in
`pushdown.rs`, 209 is `assert_residual` finding a pushed filter. Thirty are red; none is
equivalent.

| id | clause | mutation (file) | red? | red in |
|---|---|---|---|---|
| m101 | C-070 P-1 | render `= NULL` for `IS NULL` (`src/pushdown.rs`) | RED | `p01_null_tests_push` at `pushdown.rs:234`; live at `live_pushdown.rs:136` |
| m102 | C-070 P-2 | render `NOT c` for `IS NOT TRUE` (`src/pushdown.rs`) | RED | `p02_boolean_tests_push` at `pushdown.rs:258`; live at `:136` |
| m103 | C-070 P-3 | cast the parameter to the column's width (`src/pushdown.rs`) | RED | `p03_integer_comparison_pushes_cross_width` at `pushdown.rs:277`; live at `:129` (`22003`, smallint out of range) |
| m104 | C-070 P-4 | render the literal at the column's scale, truncating (`src/pushdown.rs`) | RED | `p04_decimal_comparison_pushes` at `pushdown.rs:334`; live at `:136` |
| m105 | C-070 P-5 | push out-of-range literals (`src/pushdown.rs`) | RED | `p05_temporal_comparison_pushes_inside_range` at `pushdown.rs:209`; live at `:129` (date out of range) |
| m106 | C-078 P-6 | drop `COLLATE "C"` (`src/pushdown.rs`) | RED | `p06_text_comparison_is_code_point_order` at `pushdown.rs:424`, `p06b_text_equality_ignores_nondeterministic_collation` at `:464`; both live at `:136` |
| m107 | C-078 P-6 | push a literal holding NUL (`src/pushdown.rs`) | RED | `p06c_nul_literal_stays_residual` at `pushdown.rs:209`; live at `:129` |
| m108 | C-070 P-7 | drop NULL items from `IN` (`src/pushdown.rs`) | RED | `p07_in_list_three_valued` at `pushdown.rs:490`; green live, where DataFusion simplifies the NULL item away before the provider |
| m109 | C-070 P-8 | render `>` / `<` for `BETWEEN` (`src/pushdown.rs`) | RED | `p08_between_renders_inclusive` at `pushdown.rs:522`; green live, where DataFusion splits `BETWEEN` into two compares first |
| m110 | C-070 P-9 | disable the escape, `like_escape(pattern, '')` (`src/pushdown.rs`; R-5) | RED | `p09_like_pushes_well_formed_patterns` at `pushdown.rs:558`; live at `:136` |
| m111 | C-070 P-10 | push the exact half of an `OR` (`src/pushdown.rs`) | RED | `p10_logic_pushes_only_exact_children` at `pushdown.rs:209`; live at `:136` |
| m112 | C-072 P-11 | push the limit regardless (`src/provider/table.rs`) | RED | `p11_limit_pushes_only_without_residual` at `pushdown.rs:625`; green live (C-068: DataFusion withholds the limit itself) |
| m113 | C-070 | class `float8` eligible (`src/pushdown.rs`) | RED | `r01_float_comparisons_stay_residual` at `pushdown.rs:209`; live at `:136` (`-0 = 0`) |
| m114 | C-070 | class `bpchar` as text (`src/pushdown.rs`) | RED | `r02_bpchar_comparisons_stay_residual` at `pushdown.rs:209`; live at `:136` |
| m115 | C-077 | class a placed `timestamp` eligible, as an instant (`src/pushdown.rs`) | RED | `r03_ltz_timestamp_comparisons_stay_residual` at `pushdown.rs:663`; live at `:136` |
| m116 | C-070 | push arithmetic: `big + 1` reads as `big` (`src/pushdown.rs`) | RED | `r04_functions_arithmetic_and_casts_stay_residual` at `pushdown.rs:698`; live at `:136` |
| m117 | C-073 | ignore `pushdown_predicate` (`src/pushdown.rs`) | RED | `r05_pushdown_predicate_false_pushes_nothing` at `pushdown.rs:708`; live at `live_pushdown.rs:471` |
| m118 | C-071 | class `Exact` whatever the simplifier would do (`src/pushdown.rs`) | RED | `a_filter_the_optimizer_would_still_rewrite_stays_inexact` at `pushdown.rs:757`; `p07_in_list_three_valued_live` at `live_pushdown.rs:339` (five rows against none) |
| m121 | C-075 | render every filter as pushed, the classifier's general support (`src/provider/scan.rs`) | RED | `explain_renders_pushed_and_residual_per_scan` at `explain.rs:58` |
| m122 | C-075 | render the bound values into `remote_sql` (`src/provider/scan.rs`) | RED | `explain_verbose_shows_placeholders_never_values` at `explain.rs:82` |
| m123 | C-075 | add `host=` to the display (`src/provider/table.rs`) | RED | `explain_never_renders_endpoint` at `explain.rs:112` |
| m124 | C-075 | class a residual `Unsupported`, so the scan never sees it (`src/provider/table.rs`) | RED | `explain_residual_matches_filter_exec_above` at `explain.rs:145` |
| m125 | C-074 | past 1024 values, scan with no pushed filter instead of refusing (`src/provider/table.rs`) | RED | `pushed_values_past_1024_fail_the_plan` at `pushdown.rs:742` |
| m126 | C-076 | count no bytes received (`src/read/postgres.rs`) | RED | `explain_analyze_reports_rows_bytes_and_time_per_scan_live` at `live_pushdown.rs:560` |
| m127 | C-076 | never resize the reservation (`src/provider/scan.rs`) | RED | `a_batch_past_the_memory_pool_is_resources_exhausted_live` at `live_pushdown.rs:593` |
| m128 | C-077 | never place `timestamp` (`src/provider/table.rs`) | RED | `r03_ltz_timestamp_comparisons_stay_residual` at `pushdown.rs:662`; live `r03_…_live` at `:138`, `timestamp_columns_are_placed_in_the_session_zone_live` at `:501` |
| m129 | C-069 | list a schema (`src/provider/catalog.rs`) | RED | `listing_a_postgres_source_is_empty_and_declared` at `explain.rs:251` |
| m130 | C-069 | `RelationNotFound` as an error, not `None` (`src/provider/schema.rs`) | RED | `a_missing_relation_is_table_not_found_live` at `live_pushdown.rs:613` |

Every sketch §5.2 and §5.3 mutation is red in at least one pin. Three are red in the unit half
alone (m108, m109, m112): DataFusion simplifies `NOT IN (…, NULL)` and splits `BETWEEN` before
the provider sees them, and withholds the limit itself, so the live half cannot reach the
mutated arm.

### 11.4 The live run (2026-10-07)

`make pg-up` (PostgreSQL 16, rootless Docker, a private `XDG_RUNTIME_DIR`), then
`cargo test -p repark-connect --test it -- --ignored`: 46 passed (C-2b's 23 and the 23
`live_pushdown.rs` cells), on three full runs of the final cells. Each cell creates and drops a
`c2_<tag>` schema, with its enum type and its two ICU collations; `make pg-down` removed the
container and its volume at the end. D-M3 is R-11.

### 11.5 Gates (C-2c)

Run on the finished tree. Each cargo command ran under the build-slot lock.

| command | exit | output |
|---|---|---|
| `cargo deny check 2>&1 \| tail -5` | 0 | `advisories ok, bans ok, licenses ok, sources ok` |
| `cargo test -p repark-connect` | 0 | 117 passed, 46 ignored (the live cells), 0 failed (91 at C-2b fold 2, plus `explain.rs`'s 6 and `pushdown.rs`'s 20) |
| `cargo test -p repark-connect --test it -- --ignored` under `make pg-up` | 0 | 46 passed |
| `cargo build -p repark-connect --no-default-features` | 0 | the pure core builds without the driver |
| `cargo clippy -p repark-connect --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 | no diagnostics, with and without default features |
| `make rust-clippy` | 0 | workspace, all targets, no diagnostics |
| `cargo fmt --check` | 0 | no output |
| `make rust-panic-ban` | 0 | clean; no new spawn |
| `python3 scripts/check_rust_file_size.py` | 0 | 1111 files clean |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 368 maps clean |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | no output |
| `python3 scripts/check_docs_links.py` | 0 | 1352 files, 7284 links clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 308 live ledgers clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xc2b origin/main HEAD` | 0 | `comment-ban hits=0` |

File sizes against sketch §7's ceilings: `provider.rs` 9/20, `provider/catalog.rs` 133/220,
`provider/schema.rs` 52/260, `provider/table.rs` 156/420, `provider/scan.rs` 267/460,
`pushdown.rs` 544/820, `tests/it/pushdown.rs` 780/900, `tests/it/explain.rs` 276/320,
`tests/it/live_pushdown.rs` 655/900 (split out of `live_pg.rs`, which stays at 742).
`read/postgres.rs` is 468 of C-2b's 600.

## 12. C-2c fold 1 — the verifier's S2s and S3s (2026-10-07)

**Branch:** `feat/c-2c-pushdown-explain` from `2c24647c` (PR #984). **Model:** Claude Opus 5.5
(`claude-opus-5-5`, high). **Scope:** the verifier's PASS verdict on C-2c and its five S2s and
three S3s, under the orchestrator's rulings L1–L5 and the S3 rulings. The live cells ran against
`make pg-up` (PostgreSQL 16), torn down with `make pg-down` at the end.

### PROPOSITION LEDGER — C-2c fold 1 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-081 | L3 (S2): `scan` never pushes a `LIMIT` above `i64::MAX`. DataFusion hands `scan` the sum `skip + fetch`; a sum that does not fit `i64` (a checked conversion) pushes no limit, so `LIMIT 9223372036854775807 OFFSET 5` over thirty rows returns twenty-five rows with `pushed_limit=None`, with pushdown on and off, where the statement used to fail with `22003` (`bigint out of range`). `LIMIT 9223372036854775807` alone still pushes. | `pushdown.rs::a_limit_past_i64_max_never_pushes` (the SQL shapes, and `scan` called with `usize::MAX`, `2^63` and `2^63 - 1`); live `live_pushdown.rs::a_limit_past_i64_max_reads_every_row_live`. | PROVEN | §12.1 m131. |
| C-082 | L4 (S2): the limit has its own switch. `pushdown_limit` (alias `pushDownLimit` on the `read_postgres` door and in a `jdbc:` URL; default `true`; a boolean) is the twenty-first `POSTGRES_KEYS` entry, and P-11 pushes `LIMIT n` only when it is set and no residual conjunct remains; `pushdown_predicate` gates the filters alone, as Spark's separate `pushDownPredicate` and `pushDownLimit` options do. `pushdown_limit = false` pushes `qty > 0` and no limit; `pushdown_predicate = false` pushes the limit of an unfiltered read and none past a residual filter. `pushDownLimit` refuses as an unknown key in `repark.toml`. | `pushdown.rs::pushdown_limit_gates_the_limit_and_pushdown_predicate_the_filters`; live `live_pushdown.rs::pushdown_limit_is_its_own_switch_live`; `settings.rs::every_endpoint_key_parses_and_unknown_keys_refuse` (its `values_refuse_naming_the_key` step refuses `pushdown_limit = 1`), `::aliases_are_case_insensitive_and_conflicts_refuse`. | PROVEN | §12.1 m132, m133, m134. Corrects C-073. |
| C-083 | S3: `output_bytes` is filled. The scan records each batch through DataFusion's `RecordOutput`, which adds the batch's rows, its `get_record_batch_memory_size` and one batch, so `EXPLAIN ANALYZE` no longer shows `output_bytes=0.0 B` and the `MetricsSet`'s `output_bytes` equals the sum over the batches the stream yields. | Live `live_pushdown.rs::the_scan_reserves_each_batch_and_counts_its_bytes_live`, `::explain_analyze_reports_rows_bytes_and_time_per_scan_live`. | PROVEN | §12.1 m136. Extends C-076. |
| C-084 | S3 (R-8 pinned): the `PostgresScan` reservation holds the current batch alone. Executing the scan with no operator above it over a 1 GiB pool, `pool.reserved()` equals the yielded batch's `get_array_memory_size()` at every yield (two batches of a 4096-row session size), and `0` once the stream is dropped. | Live `live_pushdown.rs::the_scan_reserves_each_batch_and_counts_its_bytes_live`. | PROVEN | §12.1 m135, the verifier's V3, which survived every repo pin before. |
| C-085 | L1 (S2), declared: a pushed filter sees the retyped server value while the projection casts to the planned type. Over `(id int8, qty int4)` holding `(1, 5), (2, 6)`, `SELECT id, qty … WHERE qty = 6` planned on two mounts, then `ALTER COLUMN qty TYPE numeric(10,1)` and row 1 set to `5.5` before the first poll: pushed, one `Exact` conjunct, the scan returns `(2, 6)`; with `pushdown_predicate = false` it returns `(1, 6)` and `(2, 6)`. The pushed column stays bare (no cast), so its index is usable; `CONNECT-DECL-pg-drift-cast` states both answers. | Live `live_pushdown.rs::a_pushed_filter_sees_the_retyped_value_live`; registry row `CONNECT-DECL-pg-drift-cast`. | PROVEN | §12.1 m137 (the rejected alternative, red). Needs DDL racing the statement. |
| C-086 | L2 (S2), declared: a refused value inside a pushed filter's range whose column is not projected is never decoded, so its membership follows Postgres's ordering (`NaN` above every number and equal to itself; `infinity` after every finite value, `-infinity` before). Over `c numeric(10,2)`, `d date` and `t timestamptz` holding `NaN` / `infinity` (id 1), `5` / `2024-01-01` (2), `1` / `-infinity` (3) and `2` / `2024-01-02` (4), `SELECT id` pushed returns `[1, 2, 4]` for `c > 1`, `[1, 3, 4]` for `c <> 5`, `[2, 3, 4]` for `c < 6`, `[2, 3]` for `d < DATE '2024-01-02'` and `t <` that instant, `[1, 4]` for `d > DATE '2024-01-01'` and `t >` that instant; each refuses with `pushdown_predicate = false`, naming `CONNECT-DECL-pg-numeric-special` or `CONNECT-DECL-pg-infinite-datetime`. Projected, `c > 1` refuses pushed and `c < 6` reads `[2, 3, 4]`. Both registry rows and sketch §2.9 state it. | Live `live_pushdown.rs::a_refused_value_inside_a_pushed_range_follows_postgres_order_live`; registry rows `CONNECT-DECL-pg-numeric-special`, `CONNECT-DECL-pg-infinite-datetime`. | PROVEN | §12.1 m138, m139. |
| C-087 | L5 (S2): the 1024-bound-values plan refusal (C-074, R-9) is declared. Registry row `CONNECT-DECL-pg-bound-values` names the bound (`MAX_PARAM_SLOTS`, 1024), the `pushdown_predicate` workaround and Spark's answer (the JDBC source inlines values and never refuses), and the refusal's text names the row. | `pushdown.rs::pushed_values_past_1024_fail_the_plan` (four 256-item lists bind 1024; a fifth refuses with a message holding `more than 1024 values`, `` `pushdown_predicate` to false `` and `CONNECT-DECL-pg-bound-values`). | PROVEN | §12.1 m140; m125 still covers the refusal itself. |
| C-088 | Shape and files: `src/provider/table.rs` (the `i64` check, the `pushdown_limit` gate, the row in the refusal), `src/provider/scan.rs` (`RecordOutput`) and `src/settings/postgres.rs` (`pushdown_limit`, its alias; `POSTGRES_KEYS` 21, `POSTGRES_ALIASES` 13) are the code changes; `PostgresSettings` gains the public field `pushdown_limit`, under its existing `#[non_exhaustive]`; nothing else public changes; no dependency changes; no code comments. Sizes: `table.rs` 158/420, `scan.rs` 265/460, `settings/postgres.rs` 618, `tests/it/pushdown.rs` 841/900, `tests/it/live_pushdown.rs` 891/900, `tests/it/settings.rs` 606. Every gate in §12.3 passes. | §12.3. | PROVEN | — |

### 12.1 Mutations (fold 1)

Each mutation was applied alone to the code, the named pins run with `--include-ignored`
against the container, and the file restored from a copy taken before the edit.

| id | clause | mutation (file) | red? | red in |
|---|---|---|---|---|
| m131 | C-081 | push any limit that fits `u64`: drop the `i64` check (`src/provider/table.rs`) | RED | `a_limit_past_i64_max_never_pushes` (pushed `Some(9223372036854775812)`); live `a_limit_past_i64_max_reads_every_row_live` |
| m132 | C-082 | ignore `pushdown_limit`: gate the limit on the residual alone (`src/provider/table.rs`) | RED | `pushdown_limit_gates_the_limit_and_pushdown_predicate_the_filters` at `pushdown.rs:650`; live `pushdown_limit_is_its_own_switch_live` at `live_pushdown.rs:433` |
| m133 | C-082 | C-073 as first written: gate the limit on `pushdown_predicate` too (`src/provider/table.rs`) | RED | `pushdown_limit_gates_the_limit_and_pushdown_predicate_the_filters` at `pushdown.rs:650`; live `pushdown_limit_is_its_own_switch_live` at `:433`, `a_limit_past_i64_max_reads_every_row_live` at `:466` |
| m134 | C-082 | default `pushdown_limit` to `false` (`src/settings/postgres.rs`) | RED | `every_endpoint_key_parses_and_unknown_keys_refuse` at `settings.rs:297`; the limit pins in `pushdown.rs` (`:608`, `:650`, `:669`) and live (`:412`, `:433`, `:466`) |
| m135 | C-084 | the verifier's V3: `try_resize` → `try_grow`, accumulating across batches (`src/provider/scan.rs`) | RED | `the_scan_reserves_each_batch_and_counts_its_bytes_live` at `live_pushdown.rs:700` (the second batch reads the sum of both) |
| m136 | C-083 | record rows and batches by hand, no bytes, as before the fold (`src/provider/scan.rs`) | RED | `explain_analyze_reports_rows_bytes_and_time_per_scan_live` at `live_pushdown.rs:601` (`output_bytes=0.0 B`); `the_scan_reserves_each_batch_and_counts_its_bytes_live` at `:718` |
| m137 | C-085 | the alternative L1 rejected: cast a pushed integer column to the planned type, `"qty"::pg_catalog.int4` (`src/pushdown.rs`) | RED | `a_pushed_filter_sees_the_retyped_value_live` at `live_pushdown.rs:575` (pushed now returns both rows) |
| m138 | C-086 | stop pushing `numeric` compares: class the column `NullTestOnly` (`src/pushdown.rs`) | RED | `a_refused_value_inside_a_pushed_range_follows_postgres_order_live` at `live_pushdown.rs:637` (`c > 1` refuses pushed) |
| m139 | C-086 | stop pushing `date` compares: class the column `NullTestOnly` (`src/pushdown.rs`) | RED | `a_refused_value_inside_a_pushed_range_follows_postgres_order_live` at `live_pushdown.rs:637` (`d < DATE '2024-01-02'` refuses pushed) |
| m140 | C-087 | the refusal names another row (`CONNECT-DECL-pg-listing`, `src/provider/table.rs`) | RED | `pushed_values_past_1024_fail_the_plan` at `pushdown.rs:806` |

Ten are red; none is equivalent. m137, m138 and m139 mutate toward the alternatives L1 and L2
declined (cast the pushed column; keep the compare above the scan), so each declared pin is
shown to read the behaviour it declares.

### 12.2 The live run (2026-10-07)

`make pg-up` (PostgreSQL 16.15, rootless Docker), then
`cargo test -p repark-connect --test it -- --ignored`: 51 passed (C-2c's 46 and fold 1's five
new cells: `a_limit_past_i64_max_reads_every_row_live`, `pushdown_limit_is_its_own_switch_live`,
`the_scan_reserves_each_batch_and_counts_its_bytes_live`,
`a_pushed_filter_sees_the_retyped_value_live` and
`a_refused_value_inside_a_pushed_range_follows_postgres_order_live`), on three full runs of the
finished tree. `make pg-down` removed the container and its volume at the end.

### 12.3 Gates (fold 1)

Run on the finished tree. Each cargo command ran under the build-slot lock.

| command | exit | output |
|---|---|---|
| `cargo deny check 2>&1 \| tail -5` | 0 | `advisories ok, bans ok, licenses ok, sources ok` |
| `cargo test -p repark-connect` | 0 | 119 passed, 51 ignored (the live cells), 0 failed |
| `cargo test -p repark-connect --test it -- --ignored` under `make pg-up` | 0 | 51 passed |
| `cargo build -p repark-connect --no-default-features` | 0 | the pure core builds without the driver |
| `cargo clippy -p repark-connect --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 | no diagnostics, with and without default features |
| `make rust-clippy` | 0 | workspace, all targets, no diagnostics |
| `cargo fmt --check` | 0 | no output |
| `make rust-panic-ban` | 0 | clean |
| `python3 scripts/check_rust_file_size.py` | 0 | 1111 files clean |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 368 maps clean |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | no output |
| `python3 scripts/check_docs_links.py` | 0 | 1352 files, 7284 links clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 308 live ledgers clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xc2b origin/main HEAD` | 0 | `comment-ban hits=0` |

## 13. C-2a open items — the straddling-field peak and three accounting pins (2026-10-07)

**Branch:** `fix/c-2a-open-items` from main `930ccbf3`. **Model:** Claude Opus 5.5
(`claude-opus-5-5`, high). **Scope:** the two §3 open items the C-2a re-verify left (S3, both
non-blocking): the carry's `Vec` doubling, and its surviving mutations r11, r13 and r15. No
live cell changes, so no container ran.

### PROPOSITION LEDGER — C-2a open items — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-089 | The carry is reserved once. On a field's first partial chunk (the first chunk that holds any of the field's bytes, with the carry empty) `CopyBinaryDecoder` calls `reserve_exact(length.min(MAX_FIELD_BYTES))`, so the carry's capacity equals the declared length and never grows by doubling. A 4096-byte `text` field opened with 100 bytes reads `buffered_bytes()` = 4096 (no row is buffered, so the whole charge is the carry), and still 4096 after 1000 more bytes. The re-verify's `memory.rs` shape (one `text` field of 512, 513, 700 and 1023 MiB, fed in 64 KiB chunks) peaks at 2.00x the field, down from 3.00x (2.46x at 700 MiB). Fed whole, the field still peaks at 1.00x. A length past `MAX_FIELD_BYTES` still refuses at the length word with `FieldTooLong` before any reservation (C-015 unchanged); the `min` bounds the reservation even so. | `copy_accounting.rs::carry_reserves_the_declared_length_on_the_first_partial_chunk`; mutation m141; §13.2. | PROVEN | §13.1 m141 red; §13.2 before and after. The `min` cannot fire behind the length word, so its removal is an equivalent mutation (§13.3 R-2). |
| C-090 | `buffered_bytes()` is exact for the three shapes the re-verify's survivors touched. Each expected count comes from the Arrow layout the builders hold: one `i32` offset slot (4 bytes) per variable-width row, value or NULL; the stored bytes of a variable-width value; the fixed width per fixed-width row (`int4` 4); and one validity bit per slot, ceil(rows x columns / 8); plus the carry's capacity. One buffered `abc` row and a 64-byte `text` field opened with 10 bytes read 3 + 4 + 1 + 64 = 72. Three all-NULL `text, bytea, int4` rows read 3 x (4 + 4 + 4) + ceil(9 / 8) = 38. The rows (`abc`, five bytes, `7`) and (empty, empty, `8`) read (3 + 4) + (5 + 4) + 4 + 4 + 4 + 4 + ceil(6 / 8) = 33. | `copy_accounting.rs::buffered_bytes_counts_a_mid_field_carry`, `::variable_width_nulls_charge_their_offset_slot`, `::variable_width_values_charge_bytes_and_offset`; mutations m142 (r11), m143 (r13), m144 (r15). | PROVEN | §13.1: all three re-verify survivors are now red. Extends C-016 and C-017 from bounds to exact counts. |
| C-091 | Shape and files: `src/copy_binary.rs` is the only product change (three lines, the reservation), at 476/480. The new `tests/it/copy_accounting.rs` (98 lines) holds the four pins and borrows `copy_binary.rs`'s stream builders (`Field`, `base`, `header_with`, `tuple`, now `pub(crate)`), so `tests/it/copy_binary.rs` stays at 605/700. No public signature changes, no dependency changes, no code comments. Every gate in §13.4 passes. | §13.4. | PROVEN | — |

### 13.1 Mutations (open items)

Each mutation was applied alone to the finished tree, the crate's tests run
(`cargo test -p repark-connect --no-fail-fast`) and the file restored from the copy taken before
the edit (`mutate_open.py`, the re-verify's `mutate_rv.py` shape). The tree's diff was the same
before and after the four runs.

| id | clause | mutation (file) | red? | red in |
|---|---|---|---|---|
| m141 | C-089 | grow the carry by `Vec` doubling again: drop the reservation (`src/copy_binary.rs`) | RED | `carry_reserves_the_declared_length_on_the_first_partial_chunk` at `copy_accounting.rs:43`; `buffered_bytes_counts_a_mid_field_carry` at `:56` |
| m142 | C-090 | r11: `buffered_bytes()` leaves out the carry's capacity (`src/copy_binary.rs`) | RED | `carry_reserves_the_declared_length_on_the_first_partial_chunk` at `copy_accounting.rs:43`; `buffered_bytes_counts_a_mid_field_carry` at `:56` |
| m143 | C-090 | r13: a variable-width NULL charges 0, fixed widths kept (`src/types/postgres.rs`) | RED | `variable_width_nulls_charge_their_offset_slot` at `copy_accounting.rs:70` |
| m144 | C-090 | r15: a variable-width value charges no offset bytes (`src/types/postgres.rs`) | RED | `variable_width_values_charge_bytes_and_offset` at `copy_accounting.rs:94`; `buffered_bytes_counts_a_mid_field_carry` at `:56` |

All four are red, and none is equivalent. Before this unit, r11, r13 and r15 were green in
every repo pin (the re-verify's `mutations.json`).

### 13.2 The peak, measured (2026-10-07)

The re-verify's probe (`memory.rs`, a counting global allocator, release build), copied with its
`common` module and pointed at this clone, ran under `ulimit -v 67108864` and the build-slot
lock. Each row is one `text` field between two one-byte rows, under the default limits. Peak
growth is measured from the decoder's creation.

| field | chunk | before (main `930ccbf3`) | after |
|---|---|---|---|
| 512 MiB | 64 KiB | 1535.8 MiB (3.00x) | 1024.0 MiB (2.00x) |
| 513 MiB | 64 KiB | 1536.8 MiB (3.00x) | 1026.0 MiB (2.00x) |
| 700 MiB | 64 KiB | 1723.8 MiB (2.46x) | 1400.0 MiB (2.00x) |
| 1023 MiB (`(1 << 30) - 64`) | 64 KiB | 3071.5 MiB (3.00x) | 2048.0 MiB (2.00x) |
| each of the four | whole | 1.00x | 1.00x |

Before, the carry's first extend was the chunk's remainder (65 504 bytes after the header, the
first row and the field's two words), and doubling from there passes 512 MiB only at about 1023.5 MiB, so the carry
alone nearly doubled the field. After, the carry holds the field once and the builder copy holds
it again. Every other probe line is unchanged: the `i32::MAX` length word refuses with 0 bytes
of growth, the carry is released after the flush (0.0 MiB retained), and the tiny-row and
all-NULL runs flush as before.

### 13.3 Readings acted on (no halt)

- **R-1, the up-front charge.** A stream that declares a length of up to `MAX_FIELD_BYTES` and
  sends its first byte now reserves that length at once, where before the carry grew only as
  bytes arrived. The brief's guard is the cap: a lying length reserves at most 1 GiB, the
  bound the length word already enforces, and the reservation is untouched virtual memory until
  bytes fill it. `buffered_bytes()` reports it from that chunk on, so the C-2c scan's
  `MemoryReservation` is resized to the full field before its bytes arrive and refuses early
  when the pool cannot hold it, rather than mid-field.
- **R-2, the `min`.** `field_length` refuses any length past `MAX_FIELD_BYTES` before
  `FieldValue` is entered, so `length.min(MAX_FIELD_BYTES)` always equals `length`. It stays
  as the brief's stated guard, which keeps the reservation bounded if the length check ever
  moves; its removal is an equivalent mutation and was not run.
- **R-3, no reservation on an empty chunk.** A chunk that ends just after the length word
  enters `FieldValue` with no bytes available. The reservation waits for the first byte, so
  `copy_field_at_postgres_max_is_accepted` (the maximum length with no payload) still reserves
  nothing.
- **R-4, a reused carry.** A carry kept from an earlier field (cleared, capacity within the
  byte cap, C-016) whose capacity already covers the new length keeps that capacity, because
  `reserve_exact` never shrinks. The pins run on a fresh decoder, where the capacity is exactly
  the declared length.

### 13.4 Gates (open items)

Run on the finished tree. Each cargo command ran under the build-slot lock and `ulimit -v`.

| command | exit | output |
|---|---|---|
| `cargo test -p repark-connect` | 0 | 123 passed, 51 ignored (the live cells), 0 failed |
| `cargo build -p repark-connect --no-default-features` | 0 | the pure core builds without the driver |
| `make rust-clippy` | 0 | workspace, all targets, no diagnostics |
| `cargo fmt --check` | 0 | no output |
| `make rust-panic-ban` | 0 | clean |
| `python3 scripts/check_rust_file_size.py` | 0 | 1112 files clean |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 368 maps clean |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | no output |
| `python3 scripts/check_docs_links.py` | 0 | 1354 files, 7295 links clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 309 live ledgers clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xc2ao origin/main HEAD` | 0 | `comment-ban hits=0` |
