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
- **Open item (2026-10-06, re-verify S3): the straddling-field peak is still about 3x the
  field.** The carry grows by `Vec` doubling from the first partial chunk, so its capacity nears
  twice the field before the builder copy (re-verify `memory.rs`, 512–1023 MiB fields). Reserving
  the carry at the declared field length on the first partial chunk would bring it to about 2x.
  Non-blocking; C-3 owns the gated memory benchmark.
- **Open item (2026-10-06, re-verify S3): three accounting mutations survive.** They are
  `buffered_bytes()` without the carry (r11), a variable-width NULL charged 0 (r13), and
  variable-width values charged with no offset bytes (r15)
  (the re-verify's `mutate_rv.py`). Each needs an exact-count pin on `buffered_bytes()`.
  Non-blocking.
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
| C-032 | `PooledClient` returns its connection only through `release_clean()`. Dropped any other way (cancel, error, an early `LIMIT`), its lease aborts the connection's task, so the socket closes and the server ends the backend, and the connection never re-enters the pool. No cleanup task is spawned. The only task is the driver's connection task, spawned once per connection under `#[expect(clippy::disallowed_methods)]` with its lifecycle stated: `PgConnection` holds its handle. | `a_lease_dropped_before_release_aborts_its_connection` (F-2's unit half); mutation m41. | PROVEN | §7.1 m41 red. The live F-2 (`pg_stat_activity`) is round 3. |
| C-033 | `query_config(settings)` builds the only `Config` the pool connects with: host, port, user, password, database, `application_name`, `connect_timeout`, keepalives on, `ssl_mode` `Disable` for `disable` and `Require` otherwise (verification is rustls's), and the §2.4 session pins in one `-c` options list: `client_encoding=UTF8`, `DateStyle=ISO`, `IntervalStyle=postgres`, `TimeZone=UTC`, `search_path=` (empty), `default_transaction_read_only=on`, `lock_timeout`, `statement_timeout` (= `query_timeout_ms`, `0` unlimited) and `idle_in_transaction_session_timeout` (= `read_timeout_ms`). Nothing in `pool.rs` sets a replication mode. | `query_config_pins_the_session_in_the_startup_packet`; mutations m46, m48. | PROVEN | §7.1. Whether the server honours each pin is round 3's live reading. |
| C-034 | NS-7 at connect: `PostgresConnector::connect` bounds the whole connect (TCP, the TLS negotiation and handshake, authentication) by `connect_timeout_ms` and refuses with `Timeout { which: TimeoutSetting::Connect }`, whose message names the key. The driver's own TCP timeout classifies the same way. `within(which, limit, work)` is the helper the read path wraps every request and COPY chunk in. | `connect_timeout_bounds_a_server_that_never_answers` (a listener that accepts and never answers, under `disable` and `verify-full`); mutation m47. | PROVEN | §7.1 m47 red (the 3 s guard fires). |
| C-035 | Connect failures classify without matching driver text: a server that refuses TLS under `verify-full` is `TlsRequired`, which names `sslmode=disable` as the only switch, because the handshake-tracking connector never started; a rustls certificate refusal is `TlsHandshake` with `UntrustedCertificate`, `HostNameMismatch`, `CertificateExpired` or `Handshake`; a host name rustls cannot verify is `ServerName`; any other I/O failure is `Unreachable { kind }`; a server error is `Server { sqlstate, message }`; a connection closed during startup is `Unreachable { UnexpectedEof }`; the rest, which are driver-side authentication failures (no password, SASL), is `AuthenticationFailed`. All fold to the operational class. | `plaintext_server_refuses_under_verify_full`, `verify_full_refuses_an_untrusted_or_misnamed_server_certificate`, `a_refused_port_is_unreachable`; mutations m48, m39, m49, m50. | PROVEN | §7.1. `Server` carries every server error at connect, including SQLSTATE class 28. Round 3 splits off `AuthenticationFailed` and `PermissionDenied` (`42501`) with its live pins. |
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
