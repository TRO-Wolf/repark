# Unit ledger — C-4 · Postgres writes: COPY FROM STDIN by default, row INSERT fallback

**Date:** 2026-10-08 · **Branch:** `feat/c-4-writes` · **Base:** `40fc916f` (C-1…C-3 merged) ·
**Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Order:** [c-4-writes.md](../../wo/c-4-writes.md), grade B, R-1…R-5. **Standing defaults:**
[the North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) NS-1…NS-19.
**Card:** 1.6 of [the design plan](../../roadmap/epic-term/roadmap-design-plan-2026-08-29.md).

This file closes when the unit's last step merges: it moves to `completed/` in that commit.
Step 1 (this round) is the write core in `repark-connect`. Steps 2 and 3 (the both-door
routing, the `write.path` option, the Python convenience) are another lane's; their clauses are
`OPEN` below so the gap is visible.

## 0. Rulings this round builds on (owner's delegate, 2026-10-08)

R-4 of the order is ruled, so H-FLAG does not fire:

- Row mode is the automatic fallback for any column whose type COPY BINARY cannot carry.
- The explicit override is the Spark-door writer option `write.path = bulk | row`, default
  `bulk`; `row` forces the INSERT path for every column.
- The SQL door's `INSERT INTO <source>.<schema>.<table> SELECT …` takes no flag: bulk, with the
  automatic fallback.
- The C-6 Python convenience maps onto the same option. No second path.
- Parity pin: bulk and row produce byte-identical tables for every declared type.

## 1. The design of step 1

### 1.1 What the next slice calls

The selector is a typed Rust entry point in `repark-connect`, behind the `postgres` feature.
No door parses anything in this round.

```rust
pub enum WritePath { Bulk, Row }

impl WriteRequest {
    pub fn new(resolved: &ResolvedSource) -> Result<WriteRequest>;
    pub fn columns(self, indexes: &[usize]) -> Option<WriteRequest>;
    pub fn path(&self, requested: WritePath) -> WritePath;
    pub fn carriages(&self) -> Vec<WriteCarriage>;
    pub async fn open(self, pool: &Arc<PostgresPool>, path: WritePath, options: WriteOptions)
        -> Result<PostgresWriter>;
}

impl PostgresWriter {
    pub fn path(&self) -> WritePath;
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    pub async fn commit(self) -> Result<WriteReport>;   // WriteReport { path, rows }
}
```

A door resolves the target with C-2's `discover` (unchanged), builds the request, names the
written columns in the order its batches carry them, opens the writer with the path its option
gave (`WritePath::Bulk` when it has no option), feeds it every batch of the `SELECT`, and
commits. Dropping the writer for any reason is the rollback. `path()` answers the path the
write will take before any connection, so EXPLAIN can show it. `WriteOptions::from_settings`
gives the defaults from a `PostgresSettings`. C-2's public shape is unchanged: the one addition
to an existing type is `Privilege::Insert`.

The writer is a push sink and takes no stream. A door's stream yields the engine's error type,
and a failed `SELECT` must roll the write back: the door stops calling and drops the writer.
This is the split NS-11 asks for (the data path is `write`, the commit path is `commit`).

### 1.2 One encoder, two carriages

Both paths send every value through one encoder (`types/postgres/encode.rs`,
`ColumnEncoder`), which writes the type's binary wire form: the form the type's `*_recv`
function reads. The bulk path frames those bytes as `COPY … FROM STDIN (FORMAT BINARY)`
tuples. The row path binds the same bytes as binary-format parameters of
`INSERT INTO t (cols) VALUES ($1, …)`, where the server infers each parameter's type from its
column. So for every type the bulk path can carry, the two paths hand the same bytes to the
same server function, and the parity pin measures the rest: framing, NULLs, row order, the
type modifier (COPY applies it inside `*_recv`, INSERT as an assignment), domains and enums.

A row-path alternative was rejected: binding text and letting `*_in` parse it. `float8in`
stores one canonical NaN, while the binary form keeps the payload and the sign, so the two
tables would differ for a NaN with a payload (the sample matrix carries one). That is an
H-PARITY divergence by construction, and text also costs a format and a parse per value.

### 1.3 What "COPY BINARY cannot carry" means, per type

A type is **carried** when the Arrow value determines the type's binary wire form without the
server's own parser. It is **row-only** when the Arrow side holds text that only the server's
`*_in` function parses exactly.

| Postgres type | Arrow type written | carriage | the wire value |
|---|---|---|---|
| `bool` | `Boolean` | COPY BINARY | one byte |
| `int2`, `int4`, `int8` | `Int16`, `Int32`, `Int64` | COPY BINARY | big-endian two's complement |
| `float4`, `float8` | `Float32`, `Float64` | COPY BINARY | IEEE 754 big-endian, bit for bit |
| `text`, `varchar`, `bpchar` | `Utf8` | COPY BINARY | the UTF-8 bytes |
| `bytea` | `Binary` | COPY BINARY | the bytes |
| `numeric` | `Decimal128(p,s)` of the column | COPY BINARY | `numeric_send`'s words and base-10000 digits, `dscale = s` |
| `date` | `Date32` | COPY BINARY | days since 2000-01-01 |
| `timestamp` | `Timestamp(µs, None)` | COPY BINARY | µs since 2000-01-01 |
| `timestamptz` | `Timestamp(µs, any zone)` | COPY BINARY | µs since 2000-01-01 UTC |
| `uuid` | `Utf8` | COPY BINARY | 16 bytes, parsed by `uuid_in`'s grammar |
| `json` | `Utf8` | COPY BINARY | the text (`json_recv` validates it as `json_in` does) |
| `jsonb` | `Utf8` | COPY BINARY | version byte `01`, then the text |
| an enum | `Utf8` | COPY BINARY | the label (`enum_recv` reads a label) |
| a domain over a mapped type | the base type's | COPY BINARY | the base type's (`domain_recv` checks the constraint) |
| `interval` | `Utf8` | **row only** | the text, bound as `text` and cast `::pg_catalog.interval` |
| `time` | declared (`CONNECT-DECL-pg-time`) | none | the relation does not resolve |

`interval` is the one row-only type. C-2 reads it as the server's text, so the Arrow value is
text in whatever style it was written. Its binary form (µs, days, months) needs
`interval_in`'s parser, with its four input styles, its field typmods and its overflow rules. A
second copy of that parser in the client would be a standing parity risk for no measured gain.
`uuid` is also text on the Arrow side but is carried: `uuid_in`'s grammar is twenty lines and
is ported exactly (an optional brace pair, 32 hex digits of either case, an optional hyphen
after any group of four digits except the last, nothing else), pinned form by form and against
the server.

COPY is per table, so one row-only column among the written columns puts the whole write on
the row path. A write that does not name the `interval` column stays on the bulk path.

### 1.4 The two statements

- Bulk: `COPY "s"."t" ("a", "b") FROM STDIN (FORMAT BINARY)`. The stream is the 11-byte
  signature, a zero flags word, a zero extension length, then per row a 16-bit field count and
  per field a 32-bit length (`-1` for NULL) and the value, then the `ff ff` trailer. Rows are
  buffered into chunks of `copy_chunk_bytes` (default 1 MiB); a chunk ends on a row boundary
  and holds at least one row, so a row larger than the chunk is one chunk.
- Row: `INSERT INTO "s"."t" ("a", "g") VALUES ($1, $2::pg_catalog.text::pg_catalog.interval)`,
  prepared once per write. A second statement with `rows_per_insert` tuples (default 256,
  capped so the statement stays under the protocol's 65 535 parameters) is prepared on first
  use. Encoded rows wait in a buffer; a full group goes through the multi-row statement, and
  the remainder at commit, or any group that passes `copy_chunk_bytes` before it is full, goes
  row by row through the single-row statement. Rows reach the table in input order on both
  paths (pinned by `ctid` order).

No value is ever written into SQL text. Identifiers go through `PgIdent`'s quoting. The only
type name interpolated is the map's own constant `interval`.

A statement-level trigger sees the difference between the paths and nothing else does: the
bulk path is one `COPY` statement, the row path is one `INSERT` per group or per row. Pinned.

### 1.5 Transaction and failure semantics

One write is one transaction: `BEGIN READ WRITE`, the COPY or the INSERT statements, `COMMIT`. The pool's
session pin `default_transaction_read_only=on` stays; `READ WRITE` on the `BEGIN` is the only
thing that lifts it, for that transaction. Every request and every COPY chunk waits at most
`read_timeout_ms`, as C-2's reads do.

| event | what the caller sees | what the table holds |
|---|---|---|
| `commit` returns `Ok` | `WriteReport { path, rows }`, `rows` the server's count | every row |
| a value the wire cannot carry (`UnwritableValue`: a malformed `uuid`, a date or timestamp whose shifted value is the type's `-infinity` word or overflows) | the error at that `write`, naming the column and the row in the whole stream, never the value; the writer is poisoned and `commit` answers the same error | nothing |
| the server refuses a row (a constraint, a length, a range, an encoding) | `Server { sqlstate, message }`: at `commit` on the bulk path (the server reports a COPY error only after the client's end of data), at the `write` that flushes the row or at `commit` on the row path; the SQLSTATE is the same on both | nothing |
| the role lacks `INSERT` | `PermissionDenied { relation, privilege: Insert }`: at `open` on the bulk path, at the first flush on the row path | nothing |
| the writer is dropped before `commit` | nothing; the lease's drop cancels the statement and closes the connection | nothing |
| the backend dies mid-write | `Disconnected` at the next `write` or at `commit` | nothing |
| the server answers `COMMIT` with an error (a deferred constraint) | `Server { sqlstate, message }` | nothing |
| no answer to `COMMIT` within `read_timeout_ms`, or the connection is lost while waiting | `CommitUnknown { relation }`, class `CommitStateUnknown` | every row or nothing; the caller must read before any retry |

The row path is idle in its transaction between two batches, so a `SELECT` that takes longer
than `read_timeout_ms` between batches ends the write with `Timeout { which: Read }`
(the server's `idle_in_transaction_session_timeout`, a C-2 session pin). The bulk path is
inside one COPY statement for the whole write and does not meet that timer.

### 1.6 Bounds

Client memory on the bulk path is one chunk plus one row. On the row path it is at most
`rows_per_insert` encoded rows or `copy_chunk_bytes`, whichever fills first, plus one row.
One write holds one pooled connection. Nothing is spawned.

## PROPOSITION LEDGER — C-4 — 2026-10-08

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every type the C-2 map maps encodes from its Arrow type to the binary wire form its decoder reads back to the same array, NULLs and boundary values included (the integer extremes, signed zero, a subnormal, NaN with a payload, the infinities, empty and multi-byte text, every byte value, the widest `Decimal128` at four scales, the first and last days and microseconds Postgres holds); `interval` travels as its text. `PostgresTypeRow::encode` is a thin wrapper over the same encoder, and `ConnectError::EncodeNotBuilt` is retired. | `write::every_mapped_type_encodes_to_the_wire_form_its_decoder_reads`, `postgres_types::every_mapped_row_encodes_and_a_wrong_array_names_both_types`, C-1's ten round trips. | PROVEN | §3. |
| C-002 | `numeric` encodes as `numeric_send` writes it: digit count, weight, sign word and `dscale` equal to the Arrow scale, base-10000 digits aligned on the decimal point, no leading or trailing zero digit, zero as no digit. Twelve byte anchors, and a grid of 39 scales, each over the powers of ten to 10^38, their negatives and neighbours, that decodes back exactly. | `write::numeric_encodes_as_numeric_send_writes_it`; live, the four `numeric` columns of C-006. | PROVEN | §3. |
| C-003 | `uuid` text parses by `uuid_in`'s grammar (seven accepted forms, twelve refused). A `date` or timestamp whose value shifted to the 2000 epoch overflows, or equals the word Postgres reads as `-infinity`, refuses per value (`UnwritableValue`, naming the column and the row and never the value). A `timestamptz` column takes a timestamp with any zone label and refuses one with none; a `timestamp` column the reverse. | `write::uuid_text_parses_as_uuid_in_parses_it`, `write::a_date_or_timestamp_the_wire_reads_as_infinite_or_cannot_count_refuses`; live `type_modifiers_and_text_forms_store_what_the_server_itself_parses`. | PROVEN | §3. |
| C-004 | The COPY stream is the signature, two zero words, then per row a field count and length-prefixed fields with `-1` for NULL, then the trailer; C-2's decoder reads it back to the batch; the bytes do not depend on the chunk size or on how the rows are cut into batches; a chunk ends on a row boundary and holds at least one row; no batch, or an empty one, is the header and the trailer; a refused value names its row in the whole stream. | `write::the_copy_stream_is_the_one_the_decoder_reads_whatever_the_chunking`, `write::a_copy_tuple_is_a_field_count_then_length_prefixed_fields`, `write::a_refused_value_names_its_row_in_the_whole_stream`. | PROVEN | §3. |
| C-005 | The selector: `WriteRequest::path(Bulk)` is `Bulk` unless a written column is row-only, then `Row`; `path(Row)` is `Row`; `interval` is the only row-only type in the map; a write that does not name the row-only column is bulk. The two statements quote every identifier, cast only the text-carried column, and follow the written column order. A query target, a column index outside the relation, a batch with another column count and an Arrow array of another type refuse before any byte is sent, each in its class. | `write::the_selector_falls_back_to_rows_for_a_column_copy_binary_cannot_carry`, `write::the_statements_quote_every_name_and_cast_only_the_text_carried_column`, `write::a_write_that_cannot_be_planned_refuses_before_any_connection`, `write::write_errors_fold_by_class_and_carry_no_value`. | PROVEN | §3. |
| C-006 | **The parity pin (R-2, ruled 2026-10-08).** For every type the map maps, the same Arrow batch written by the bulk path and by the row path gives two tables whose every value has the same `*_send` bytes, compared on the server row by row with no diverging row; the rows sit in input order in both; both read back to the input batch. Twenty-two columns: the eighteen mapped base types less `interval`, `numeric` at four scales, an enum and a domain; eight rows with NULLs in every column. | live `bulk_and_row_store_byte_identical_tables_for_every_declared_type`. | PROVEN | §4. |
| C-007 | An `interval` column takes the row path whether `Bulk` or `Row` was asked, and the report says `Row`. The stored values equal the server's own parse of the same text in three interval types (`interval`, `interval(2)`, `interval year to month`) for seven spellings in four input styles and NULL; a text the server cannot parse refuses with `22007` on both requests and stores nothing. | live `an_interval_column_takes_the_row_path_whatever_was_asked`. | PROVEN | §4. |
| C-008 | Type modifiers and text forms: `varchar(5)` with trailing blanks, `char(4)` padding, `timestamp(3)` and `timestamptz(0)` rounding, five `uuid` spellings, `jsonb` with duplicate keys and blanks, and `json` kept verbatim give the same bytes on both paths and the same bytes as the server's own text input of the same values. | live `type_modifiers_and_text_forms_store_what_the_server_itself_parses`. | PROVEN | §4. |
| C-009 | Streams: no batch and one empty batch store nothing and report zero rows on both paths; a stream of six batches (1000, 0, 1, 255, 257 and 4096 rows, a 300 kB value among them) stores the same rows in the same order on both paths under five option sets (the defaults, a 64-byte chunk with seven-row groups, seven-row groups, one-byte chunks with one-row groups, and a group size past the parameter limit). The bulk path is one `COPY` statement; the row path runs `floor(n / group)` multi-row statements and the rest row by row, or every row singly when the byte bound fills first. | live `empty_and_multi_batch_streams_store_the_same_rows_in_the_same_order`, `the_bulk_path_is_one_copy_and_the_row_path_bounded_inserts`. | PROVEN | §4. |
| C-010 | Failure semantics (§1.5): a write is one transaction. A server refusal (NOT NULL, a length, a date out of range, a domain check, a unique key, a NUL byte) has the same SQLSTATE on both paths and stores nothing, with good batches before and after it; a refused value poisons the writer, and `commit` answers the same error and stores nothing; a writer dropped or a backend killed before `commit` stores nothing and leaves no backend, and the pool serves the next write; a `COMMIT` the server refuses is that server error; a `COMMIT` with no answer inside `read_timeout_ms` is `CommitUnknown`, class `CommitStateUnknown`; a role without `INSERT` is `PermissionDenied` naming `INSERT` on both paths. | live `a_server_refusal_is_the_same_on_both_paths_and_stores_nothing`, `a_refused_value_poisons_the_writer_and_stores_nothing`, `a_writer_dropped_or_killed_before_commit_stores_nothing`, `a_commit_refused_is_definite_and_a_commit_unanswered_is_unknown`, `a_role_without_insert_is_refused_naming_the_privilege`. | PROVEN | §4. |
| C-011 | A write names only its columns, in its own order, and the others take their defaults; nothing shows before `COMMIT`; after it the connection is back in the pool, it is the one connection, and its session is still read-only by default. | live `a_write_returns_its_connection_clean_and_names_only_its_columns`. | PROVEN | §4. |
| C-012 | The ledger cites ConnectorX and ADBC (order R-5), with ADBC's bulk-ingest contract as the bar. | §6. | PROVEN | §6. |
| C-016 | Fold 1, the verifier's S2. A `write` future dropped before it finishes poisons the writer: every later `write` and `commit` answers `WriteRefused { Interrupted }`, nothing is committed, and dropping the writer rolls back. On the verdict's repro (100 rows, a 200 000-row write cut by a 500 µs timeout, 100 rows, commit, groups of 50) both paths store nothing and the pool serves the next write. | live `a_write_dropped_mid_flight_poisons_the_writer_and_stores_nothing`; red without the in-flight mark (`Ok(())` where the refusal is due). | PROVEN | §8.1. |
| C-017 | Fold 1, the verifier's S1 on identity and the S3 on generated columns. A write that names a `GENERATED ALWAYS` identity column refuses on both requested paths with `WriteRefused { IdentityAlways { column } }`, and one that names a generated column with `WriteRefused { GeneratedColumn { column } }`: at `open`, after one catalog read, before any byte of data, in the `Analysis` class, and nothing is stored. Left unnamed, both paths let the server fill them and store the same values. A `GENERATED BY DEFAULT` identity that is named takes the written values on both paths. | live `identity_generated_and_default_columns_agree_or_refuse_on_both_paths`; `write::a_named_identity_or_generated_column_refuses_in_one_class_and_names_the_fix`. | PROVEN | §8.2, §8.3. |
| C-018 | Fold 1, the verifier's S1s on rules and views, and every other relation property of §8.3. `open` reads the target's properties once and a `Bulk` request takes the row path when the target is a view or a materialized view, is or routes to a foreign table, has an enabled `INSERT` rule, has row-level security active for the role, or has an enabled statement-level `INSERT` trigger; `PostgresWriter::fallback()` names which (`RowFallback`), and a `Row` request reports no fallback. For each property the two requested paths give the same report, the same target rows and the same side tables: `DO INSTEAD NOTHING` (0 rows, nothing stored), `DO ALSO` (4 rows and 4 audit rows), an auto-updatable view, an `INSTEAD OF` trigger, a statement trigger (the same number of firings). A view Postgres will not insert into, a check option, and a materialized view give the server's own SQLSTATE on both. Row triggers, partitioned tables, a row with no partition, inheritance parents and children, unlogged tables, exclusion and deferred foreign-key violations, defaults and a reordered column subset stay on the bulk path and agree with the row path. Another session's temporary table is `0A000` on both. | live `rules_and_views_take_the_row_path_and_leave_the_same_tables`, `row_triggers_stay_bulk_and_statement_triggers_take_the_row_path`, `partitions_inheritance_persistence_and_constraints_agree_on_both_paths`, `a_foreign_table_or_partition_takes_the_row_path`, `another_sessions_temporary_table_is_refused_the_same_on_both_paths`, `a_statement_trigger_sees_the_same_bounded_inserts_whatever_was_asked`; `write::every_fallback_reason_says_why_copy_was_not_used`. | PROVEN | §8.3. |
| C-019 | Fold 1, row-level security and the verifier's S3 on `42501`. With a policy active for the role a `Bulk` request takes the row path; rows the policy admits are stored and a row it refuses is the server's `42501` with the server's text, on both requests, nothing stored; the table's owner, whom the policy spares, stays on the bulk path. The `INSERT` privilege is read at `open` for exactly the written columns: a role without it, or with a column grant that misses a written column, is `PermissionDenied { Insert }` on both paths before any byte, and a column grant that covers the write stores. Any `42501` after that is the server's own (`Server { sqlstate, message }`): a default's sequence without `USAGE` names the sequence and no longer claims a missing `INSERT` grant. | live `row_security_and_grants_are_judged_the_same_on_both_paths`, `a_role_without_insert_is_refused_naming_the_privilege`. | PROVEN | §8.3. |
| C-020 | Fold 1, the verifier's S3s on class and text. A row write left idle in its transaction past `read_timeout_ms` ends as `Timeout { which: Read }` at the next `write` and at `commit` (the pool now keeps the SQLSTATE that closed a connection, and `25P03` folds to the timeout), never as `Disconnected` and never as `CommitUnknown`; the bulk path does not meet that timer. A server refusal in SQLSTATE class `22` keeps the server's sentence up to the first `:` or `"` and ends in `***`, so the written value is not in the error: `22P02` for an enum, `22007` for an interval, while `22001` (no value in it) is kept whole. | live `a_row_write_idle_past_the_read_timeout_ends_as_that_timeout`, `a_server_refusal_names_no_written_value`. | PROVEN | §8.4. |
| C-021 | Fold 1, the verifier's S3 on Arrow encodings. A column takes any encoding of its planned type's values: `LargeUtf8` and `Utf8View` for a text-like column, `LargeBinary` and `BinaryView` for `bytea`, and a `Dictionary` with any key type over any of those or over the planned type itself. The encoded bytes equal the plain array's on every mapped family tried, both paths store the same table as the plain batch, and an encoding of another type still refuses with `ArrowType` naming the type that came. | `write_shapes::another_encoding_of_the_same_values_encodes_to_the_same_bytes`, `write_shapes::another_encoding_of_other_values_is_still_refused_naming_what_came`; live `other_arrow_encodings_store_the_same_table_on_both_paths`. | PROVEN | §8.4. |
| C-022 | Fold 1, the verifier's S3 on row order. Rows are sent in input order on both paths: a `bigserial` column the write does not name numbers the rows as they arrive, and ordering by it gives the input order under the default sizes and under 4 KiB chunks with seven-row groups, across six batches of varying width. Nothing is claimed about `ctid` or an unordered scan; step 1's three `ctid` assertions are removed. | live `rows_arrive_in_input_order_on_both_paths`. | PROVEN | §8.5. |
| C-013 | `INSERT INTO <source>.<schema>.<table> SELECT …` routes to the sink on the ANSI door and on the Spark door, one test row per door (order R-3). | Both doors ask bulk through the one driver and read the taken path plus the fallback sentence off the open writer (§10.1, §10.2); ANSI pins `crates/repark-sql/src/pg_insert.rs::tests` (offline routing plus 3 ignored live cells), Spark pins `crates/repark-spark/src/tests/pg_insert.rs` plus live `python/repark-parity/tests/live_db/test_c4_write.py` (14 cells). | PROVEN | §10. |
| C-014 | The Spark-door writer option `write.path = bulk \| row` reaches `WritePath`, default `bulk`, and the C-6 Python convenience maps onto the same option (ruled 2026-10-08). | The shared parser refuses an unknown value naming both values and `row` forces the INSERT path with no fallback reason; default bulk and row-forced writes agree byte for byte live; no second path exists, so the future C-6 convenience has exactly this option to map onto (§10.1, §10.6). | PROVEN | §10. |
| C-015 | DIFF-PROBE and the verifier pass (order R-1), and the unit's last live run on the C-0 container. | Step 2's live runs are recorded in §10.4; DIFF-PROBE and the verifier run on the merged tree in step 3, after this slice. | OPEN | §10.6. |
| C-023 | A table with an enabled statement-level `INSERT` trigger takes the bulk path on a `Bulk` request (no fallback) and the row path on a `Row` request: one firing per write by bulk (BEFORE, AFTER and transition-table triggers alike), one firing per statement by row, the same target rows on both. | live `a_statement_trigger_fires_once_by_bulk_and_per_statement_by_row`, `bulk_and_row_leave_the_same_target_rows_under_a_statement_trigger`. | OPEN | §11.2. |
| C-024 | A direct `postgres_fdw` foreign table takes the bulk path on a `Bulk` request (no fallback): the same remote rows and remote audit rows as the row path. Any other foreign table, and a partitioned table with any foreign leaf, keeps `RowFallback::ForeignTable`. | live `a_postgres_fdw_table_takes_the_bulk_path_and_other_wrappers_do_not`. | OPEN | §11.2. |

## 2. Four-line records (2026-10-08)

Questions no ruled row answered, each acted on under its default (North Star §1). Flink's and
Spark's answers are cited as documented; neither engine was run in this round.

| id | date | question | Flink | Spark | default acted on |
|---|---|---|---|---|---|
| FL-1 | 2026-10-08 | Is a partly sent write visible after a failure? | No, for an exactly-once sink: the JDBC sink's exactly-once mode writes inside an XA transaction that commits with the checkpoint. Its default mode is at-least-once and flushes batches as it goes, so earlier batches stay. | A JDBC write is one transaction per partition where the database supports transactions; a failed partition rolls back, and partitions that already committed stay. | One write is one transaction and stores every row or none (§1.5). RePark's write is one stream on one connection, which is Spark's one partition, so the two agree here and Flink's stricter mode agrees too. NS §2: Flink governs the guarantee, and the default is its exactly-once reading. |
| FL-2 | 2026-10-08 | What does the caller learn when the connection is lost during `COMMIT`? | A two-phase sink keeps the transaction id in the checkpoint and resolves it on recovery. | The task fails with the driver's exception; a retry may write the rows twice. | `CommitUnknown`, folded into the engine's existing `CommitStateUnknown` class (NS-4: an unknown commit outcome is an explicit state, never "probably committed"). No retry is made anywhere in the write core. The engines differ on the guarantee; the default takes Flink's side by refusing to guess, and it returns no rows a Spark workload would not, so NS §8 case 2 does not fire. Filed for the owner as a recorded difference. |
| FL-3 | 2026-10-08 | How large is a batch on the wire? | `sink.buffer-flush.max-rows` (100) and an interval. | `batchsize` (1000 rows per JDBC batch). | Bulk: 1 MiB chunks of one COPY statement, cut on row boundaries. Row: groups of 256 rows in one statement, and a byte bound of the same 1 MiB (NS-7: bounded by rows and by bytes). Both are `WriteOptions` fields, so the door slice can map Spark's `batchsize` onto `rows_per_insert` if the owner wants the name. The sizes are defaults chosen from the bounds, not measured for speed; a release-build measurement belongs to step 3 beside the benchmark. |
| FL-4 | 2026-10-08 | Which values does the client refuse before the server sees them? | The JDBC sink leaves range and syntax to the database. | The same: the driver or the database raises. | Only what the wire cannot say truthfully: a `uuid` text that does not parse (the bulk path must build 16 bytes), and a date or timestamp whose shifted value overflows or is the word the server reads as `-infinity`, which the server would store as infinity without an error. Every other range, length and constraint is the server's judgment, with its own SQLSTATE, the same on both paths. |
| FL-5 | 2026-10-08 | Does the row path bind text or binary? | Not applicable: the JDBC driver decides. | The same. | Binary, the bytes the bulk path sends (§1.2). Text binding would store a different NaN than the bulk path, which is H-PARITY by construction. |

## 3. Mutations (2026-10-08)

Each mutation was applied alone to the tree of the step-1 commit, the write pins run with the
server up (`cargo test -p repark-connect --test it write -- --include-ignored`, 22 pins: 11
without a server, 11 live), and the file restored. "Red" lists every pin that failed.

| id | clause | mutation (file) | what it would let through | red? | red in |
|---|---|---|---|---|---|
| m1 | C-002 | the sign word is always positive (`types/postgres/numeric.rs`) | negative decimals stored positive | RED | 4 pins: live `bulk_and_row_store_byte_identical_tables_for_every_declared_type`, `every_mapped_type_encodes_to_the_wire_form_its_decoder_reads`, `numeric_encodes_as_numeric_send_writes_it`, `the_copy_stream_is_the_one_the_decoder_reads_whatever_the_chunking` |
| m2 | C-002 | the weight is one too large (`numeric.rs`) | every decimal stored 10 000 times too large | RED | 4 pins: live `bulk_and_row_store_byte_identical_tables_for_every_declared_type`, `every_mapped_type_encodes_to_the_wire_form_its_decoder_reads`, `numeric_encodes_as_numeric_send_writes_it`, `the_copy_stream_is_the_one_the_decoder_reads_whatever_the_chunking` |
| m3 | C-003 | the date shift keeps the `-infinity` word (`types/postgres/temporal.rs`) | a finite Arrow date stored as `-infinity` | RED | 1 pins: `a_date_or_timestamp_the_wire_reads_as_infinite_or_cannot_count_refuses` |
| m4 | C-003 | the timestamp shift keeps the `-infinity` word (`temporal.rs`) | a finite Arrow timestamp stored as `-infinity` | RED | 1 pins: `a_date_or_timestamp_the_wire_reads_as_infinite_or_cannot_count_refuses` |
| m5 | C-003 | a hyphen is taken after any byte of a `uuid` (`types/postgres/text_like.rs`) | the bulk path stores a text `uuid_in` refuses | RED | 1 pins: `uuid_text_parses_as_uuid_in_parses_it` |
| m6 | C-001, C-004 | the `jsonb` version byte is not written (`types/postgres/encode.rs`) | a malformed `jsonb` field | RED | 5 pins: live `bulk_and_row_store_byte_identical_tables_for_every_declared_type`, live `type_modifiers_and_text_forms_store_what_the_server_itself_parses`, `a_copy_tuple_is_a_field_count_then_length_prefixed_fields`, `every_mapped_type_encodes_to_the_wire_form_its_decoder_reads`, `the_copy_stream_is_the_one_the_decoder_reads_whatever_the_chunking` |
| m7 | C-005, C-007 | `interval` is carried as COPY BINARY (`encode.rs`) | interval text sent as the binary form | RED | 4 pins: live `a_write_returns_its_connection_clean_and_names_only_its_columns`, live `an_interval_column_takes_the_row_path_whatever_was_asked`, `the_selector_falls_back_to_rows_for_a_column_copy_binary_cannot_carry`, `the_statements_quote_every_name_and_cast_only_the_text_carried_column` |
| m8 | C-005, C-007 | the selector keeps `Bulk` whatever the columns (`write.rs`) | the same, from the selector's side | RED | 3 pins: live `a_write_returns_its_connection_clean_and_names_only_its_columns`, live `an_interval_column_takes_the_row_path_whatever_was_asked`, `the_selector_falls_back_to_rows_for_a_column_copy_binary_cannot_carry` |
| m9 | C-010, C-011 | `BEGIN_WRITE` is a bare `BEGIN` (`write.rs`) | every write refused by the read-only session pin | RED | 11 pins: live `a_commit_refused_is_definite_and_a_commit_unanswered_is_unknown`, live `a_refused_value_poisons_the_writer_and_stores_nothing`, live `a_role_without_insert_is_refused_naming_the_privilege`, live `a_server_refusal_is_the_same_on_both_paths_and_stores_nothing`, live `a_write_returns_its_connection_clean_and_names_only_its_columns`, live `a_writer_dropped_or_killed_before_commit_stores_nothing`, live `an_interval_column_takes_the_row_path_whatever_was_asked`, live `bulk_and_row_store_byte_identical_tables_for_every_declared_type`, live `empty_and_multi_batch_streams_store_the_same_rows_in_the_same_order`, live `the_bulk_path_is_one_copy_and_the_row_path_bounded_inserts`, live `type_modifiers_and_text_forms_store_what_the_server_itself_parses` |
| m10 | C-010 | `commit` ignores the kept error (`write.rs`) | the rows before a refused value are stored | RED | 1 pins: live `a_refused_value_poisons_the_writer_and_stores_nothing` |
| m11 | C-010 | `write` does not keep its error (`write.rs`) | the same, and later batches are sent | RED | 1 pins: live `a_refused_value_poisons_the_writer_and_stores_nothing` |
| m12 | C-010 | an unanswered `COMMIT` stays `Disconnected` or a timeout (`write.rs`) | a caller retries a write that may be stored | RED | 1 pins: live `a_commit_refused_is_definite_and_a_commit_unanswered_is_unknown` |
| m13 | C-010 | `42501` on a write still names `SELECT` (`write.rs`) | the error names the wrong grant | RED | 1 pins: live `a_role_without_insert_is_refused_naming_the_privilege` |
| m14 | C-009 | the group size is not capped at the parameter limit (`write/row.rs`) | a group past 65 535 parameters is never run as one statement | RED | 1 pins: live `the_bulk_path_is_one_copy_and_the_row_path_bounded_inserts` |
| m15 | C-009, C-010 | `finish` does not flush the remainder (`row.rs`) | the last rows of a row-path write vanish | RED | 11 pins: live `a_commit_refused_is_definite_and_a_commit_unanswered_is_unknown`, live `a_refused_value_poisons_the_writer_and_stores_nothing`, live `a_role_without_insert_is_refused_naming_the_privilege`, live `a_server_refusal_is_the_same_on_both_paths_and_stores_nothing`, live `a_write_returns_its_connection_clean_and_names_only_its_columns`, live `a_writer_dropped_or_killed_before_commit_stores_nothing`, live `an_interval_column_takes_the_row_path_whatever_was_asked`, live `bulk_and_row_store_byte_identical_tables_for_every_declared_type`, live `empty_and_multi_batch_streams_store_the_same_rows_in_the_same_order`, live `the_bulk_path_is_one_copy_and_the_row_path_bounded_inserts`, live `type_modifiers_and_text_forms_store_what_the_server_itself_parses` |
| m16 | C-009 | the byte bound on waiting rows is dropped (`row.rs`) | wide rows gather into one `Bind` message | RED | 1 pins: live `the_bulk_path_is_one_copy_and_the_row_path_bounded_inserts` |
| m17 | C-006 | a NULL parameter is sent as an empty value (`row.rs`) | NULL stored as empty text, or refused | RED | 8 pins: live `a_server_refusal_is_the_same_on_both_paths_and_stores_nothing`, live `a_write_returns_its_connection_clean_and_names_only_its_columns`, live `a_writer_dropped_or_killed_before_commit_stores_nothing`, live `an_interval_column_takes_the_row_path_whatever_was_asked`, live `bulk_and_row_store_byte_identical_tables_for_every_declared_type`, live `empty_and_multi_batch_streams_store_the_same_rows_in_the_same_order`, live `the_bulk_path_is_one_copy_and_the_row_path_bounded_inserts`, live `type_modifiers_and_text_forms_store_what_the_server_itself_parses` |
| m18 | C-004, C-006 | a NULL field's length is `0` (`write/postgres_copy.rs`) | the same on the bulk path | RED | 9 pins: live `a_server_refusal_is_the_same_on_both_paths_and_stores_nothing`, live `a_write_returns_its_connection_clean_and_names_only_its_columns`, live `a_writer_dropped_or_killed_before_commit_stores_nothing`, live `bulk_and_row_store_byte_identical_tables_for_every_declared_type`, live `empty_and_multi_batch_streams_store_the_same_rows_in_the_same_order`, live `the_bulk_path_is_one_copy_and_the_row_path_bounded_inserts`, live `type_modifiers_and_text_forms_store_what_the_server_itself_parses`, `a_copy_tuple_is_a_field_count_then_length_prefixed_fields`, `the_copy_stream_is_the_one_the_decoder_reads_whatever_the_chunking` |
| m19 | C-004 | a chunk ends only at the end of the batch (`postgres_copy.rs`) | one chunk per batch, unbounded | RED | 1 pins: `the_copy_stream_is_the_one_the_decoder_reads_whatever_the_chunking` |
| m20 | C-005, C-007 | the text-carried parameter has no cast (`write.rs`) | interval text read as the binary form | RED | 2 pins: live `an_interval_column_takes_the_row_path_whatever_was_asked`, `the_statements_quote_every_name_and_cast_only_the_text_carried_column` |
| m21 | C-003 | a `timestamptz` column takes a zoneless timestamp (`encode.rs`) | a wall clock stored as an instant | RED | 1 pins: `a_date_or_timestamp_the_wire_reads_as_infinite_or_cannot_count_refuses` |
| m22 | C-011 | `commit` drops the connection, no `release_clean` (`write.rs`) | every write costs a connection | RED | 1 pins: live `a_write_returns_its_connection_clean_and_names_only_its_columns` |
| m23 | C-009, C-010 | a flushed group is not cleared (`row.rs`) | rows stored twice | RED | 4 pins: live `a_refused_value_poisons_the_writer_and_stores_nothing`, live `a_writer_dropped_or_killed_before_commit_stores_nothing`, live `empty_and_multi_batch_streams_store_the_same_rows_in_the_same_order`, live `the_bulk_path_is_one_copy_and_the_row_path_bounded_inserts` |
| m24 | C-001, C-006 | `float8` is encoded after `+ 0.0` (`encode.rs`) | negative zero stored as zero | RED | 3 pins: live `bulk_and_row_store_byte_identical_tables_for_every_declared_type`, `every_mapped_type_encodes_to_the_wire_form_its_decoder_reads`, `the_copy_stream_is_the_one_the_decoder_reads_whatever_the_chunking` |
| m25 | C-004, C-009 | the trailer is not sent (`postgres_copy.rs`) | a COPY stream with no end | RED | 1 pins: live `empty_and_multi_batch_streams_store_the_same_rows_in_the_same_order` |
| m26 | C-006 | the row lane records a NULL as an empty field (`row.rs`) | NULL stored as an empty value | RED | 8 pins: live `a_server_refusal_is_the_same_on_both_paths_and_stores_nothing`, live `a_write_returns_its_connection_clean_and_names_only_its_columns`, live `a_writer_dropped_or_killed_before_commit_stores_nothing`, live `an_interval_column_takes_the_row_path_whatever_was_asked`, live `bulk_and_row_store_byte_identical_tables_for_every_declared_type`, live `empty_and_multi_batch_streams_store_the_same_rows_in_the_same_order`, live `the_bulk_path_is_one_copy_and_the_row_path_bounded_inserts`, live `type_modifiers_and_text_forms_store_what_the_server_itself_parses` |

Two mutations were green on the first run and are red above because their pins were tightened
before the commit: m5 (the refused list had no hyphen after a single byte; it now has two) and
m14 (no stream reached the capped group; the statement-count cell now writes 21 850 rows of
three columns, which is one statement of exactly 65 535 parameters and five single rows).

Not mutated, and why: the lease's drop (cancel and close) is C-2b's and is pinned there; the
dropped-writer and killed-backend cells here exercise it. The client-side read timeout around
a COPY chunk has no cell: stalling a server mid-COPY-in needs a lock the cell cannot hold
without also stalling its own oracle, and the timeout wrapper is C-2's `within`, pinned there.

## 4. The live run (2026-10-08)

`DOCKER_HOST=unix:///run/user/1000/docker.sock make pg-up` (rootless Docker, Postgres 16.15,
the C-0 container), then `cargo test --locked -p repark-connect -- --include-ignored`:
**244 passed, 0 failed, 0 ignored** (3 unit, 241 in the integration binary), four consecutive
runs on the final tree (25.2 s to 33.6 s for the integration binary). The eleven `live_write`
cells are part of it. Without the server, `cargo test --locked -p repark-connect`: 173 passed,
71 ignored. A green run leaves no schema and no role behind (counted before and after).
`make pg-down` ended the container.

What the first live run showed: every cell passed on its first run, so no pin had yet been
seen red. §3's mutations are the answer to that: each pin was then made to fail by a one-line
change to the product, and the two that did not fail were tightened.

No bulk-versus-row divergence was found, so H-PARITY did not fire.

## 5. Gates (2026-10-08)

All on the final tree, real exit codes. Cargo commands ran one at a time under the lane's
build lock.

| gate | exit |
|---|---|
| `cargo fmt --check` | 0 |
| `make rust-clippy` | 0 |
| `make rust-panic-ban` | 0 |
| `cargo test --locked -p repark-connect` | 0 (173 passed, 71 ignored) |
| `cargo test --locked -p repark-connect -- --include-ignored` with `REPARK_PG_URL` | 0 (244 passed), four runs |
| `cargo check --locked -p repark-connect --no-default-features --tests` | 0, no warning |
| `python3 scripts/sync_map_md.py --check` | 0 |
| `bash scripts/check_map_md.sh --base origin/main` | 0 |
| `make check-ledger-grammar` | 0 |
| the pre-commit hook (map, DAG, crate roots, file sizes, manifest, fmt, taplo, typos) | 0 |

Disk: 563 GB free on `/` before the first build; the round added only incremental build
output under the shared `target/`. Nothing was deleted. The container and its volume are
removed.

## 6. R-5 citations (ConnectorX and ADBC)

Both are documents read on 2026-10-08. Neither library was run in this round and neither's
source was read.

- **Arrow ADBC is the bar for bulk ingest.** Its API documents bulk ingestion as a statement
  with `adbc.ingest.target_table` and `adbc.ingest.mode`, where `append` inserts into an
  existing table, answers `ADBC_STATUS_NOT_FOUND` when the table is missing and
  `ADBC_STATUS_ALREADY_EXISTS` when "the schema of the data to append" does not match
  ([statement ingestion](https://arrow.apache.org/adbc/current/cpp/api/group__adbc-statement-ingestion.html)).
  Its PostgreSQL driver page says "Bulk ingestion is supported", that the driver uses `COPY`
  "for best performance", and gives one Arrow-to-PostgreSQL table for both bound parameters and
  bulk ingestion ([PostgreSQL driver](https://arrow.apache.org/adbc/current/driver/postgresql.html)).
  **What C-4 keeps:** append into an existing table as one COPY statement; a typed refusal
  before any byte when the batch does not fit the table (`WriteRefused { ColumnCount }`,
  `ArrowType`), ADBC's schema-mismatch error; a missing table as `RelationNotFound` from C-2's
  discovery; one type table for the bulk path and the bound-parameter path, which here is one
  encoder (§1.2); and ADBC's stated rule that a timestamp past the PostgreSQL epoch's reach
  returns an error ("an error will be returned if this would be the case"), which is
  `UnwritableValue`. **Where C-4 departs, each for a recorded reason:** the target's types
  come from the table and the Arrow types must be the ones C-2 reads (Spark's dialect governs
  the surface, as in C-2), so `numeric` is written from `Decimal128` and `uuid`, `json`,
  `jsonb` and enums from text, none of which ADBC's table lists for ingestion; ADBC documents
  that "the time zone (if present) is ignored" when a timestamp is bound, where C-4 writes a
  zoned timestamp only to `timestamptz` and a zoneless one only to `timestamp`; `interval` is
  written from the server's text on the row path, where ADBC ingests Arrow's interval types;
  and the `create`, `replace` and `create_append` modes are not built, because a database
  source is read-only for DDL (`CONNECT-DECL-pg-ddl`). ADBC's page does not say how many rows
  an ingest reports; C-4 reports the server's count.
- **ConnectorX offers nothing to compare a write with.** Its documentation states its scope as
  loading "data from databases into Python", and its destinations are dataframe libraries
  ([introduction](https://sfu-db.github.io/connector-x/intro.html)); it has no load into a
  database. What C-4 takes from it is what C-2 took: the PostgreSQL binary COPY protocol with
  typed per-column codecs and no per-value allocation, run here in the write direction, and
  the read path's decoder as the oracle for the encoder (C-004).

## 7. Open items handed to the next slice

- **A write-only role cannot be resolved.** C-2's `discover` refuses a relation the role cannot
  `SELECT` from, so a role with `INSERT` and no `SELECT` gets `PermissionDenied { Select }`
  before the write core is reached. Lifting it needs a second discovery mode, which changes
  C-2's surface; it is not done here.
- **A relation with an unmapped column does not resolve**, even when the write does not name
  that column (`time`, arrays, ranges). The same discovery rule; the same owner.
- **`timestamp` and the session zone.** The write core takes a wall clock (`Timestamp(µs,
  None)`) for a `timestamp` column. C-2c's provider localises that column into the session zone
  on read unless `prefer_timestamp_ntz` is set, so the door slice must turn the instant back
  into the wall clock before it calls `write`, with the gap and overlap rules of
  `CONNECT-DIV-pg-timestamp-zone`.
- **A row over 2 GiB of COPY data** fails in the driver as one oversized message. Postgres
  stores no field over 1 GiB, so only a row of several near-limit fields reaches it. Not
  pinned: the cell would need gigabytes.
- **Sizes are not measured for speed** (FL-3).

## 8. Fold 1 (2026-10-09): the verifier's three S1, one S2 and seven S3

The Opus verifier's verdict on `dd49a376`: the per-type claim held (6 491 495 value cells
compared by `*_send`, none differing; failure semantics, quoting and the read path held; seven
hand mutants red) and the unit **failed** on relation properties (three S1), on a cancelled
write (one S2) and on seven S3. The orchestrator ruled on 2026-10-09; the rule behind the
rulings is that the two paths must leave the same table, and where Postgres gives `COPY` and
`INSERT` different meanings, `INSERT`'s meaning is the contract and bulk only optimises it.

### 8.1 S2: a cancelled write

`PostgresWriter::write` marked the writer failed only when the lane returned an error. A
future dropped at an `.await` returned nothing, so the writer stayed usable: the bulk lane had
sent a prefix of the batch, and the row lane had sent a group it had not yet cleared from its
buffer, which the next flush sent again. Now `write` sets the kept error to
`WriteRefused { Interrupted }` before the lane runs and replaces it with the lane's own
result after. A drop in between leaves the mark. `commit` takes the writer by value and
cannot be resumed, so it needs no mark. The class is `DataFusion` (operational).

### 8.2 S1: the selector reads the relation, not only the column types

Step 1's selector asked one question, whether every written column has a COPY BINARY form.
The verifier showed three relation properties where `COPY` and `INSERT` mean different things
to Postgres. The ruling makes `INSERT`'s meaning the contract. So `open` now reads the target
once, inside the write's transaction and before any data
(`write/target.rs`, `TARGET_FACTS`: one statement, three bound values, every name
`pg_catalog`-qualified), and decides three things in this order:

1. **Privilege.** `has_column_privilege(…, 'INSERT')` over exactly the written columns. Lacking
   it is `PermissionDenied { Insert }` on both paths. This replaces step 1's rule that every
   `42501` during a write meant a missing `INSERT` grant.
2. **Refusal.** A written column that is a `GENERATED ALWAYS` identity or a generated column
   refuses (`WriteRefusal::IdentityAlways`, `GeneratedColumn`), in one class, on both paths.
   `COPY` would have overridden the identity and stored the row; no `OVERRIDING SYSTEM VALUE`
   is ever sent.
3. **Route.** A `Bulk` request becomes the row path when a property in §8.3 says so, and the
   writer keeps the reason (`RowFallback`). A `Row` request is the row path and has no reason.

`WriteRequest::path` is unchanged and still answers from the column types alone, before any
connection. It can now say `Bulk` for a write that `open` routes to rows; the writer's own
`path()` and `fallback()` are the truth for a write that is open.

### 8.3 One row per relation property (measured 2026-10-09, Postgres 16.15, C-0 container)

"Bulk" and "row" are what Postgres does with `COPY … FROM STDIN` and with `INSERT` on that
relation, measured first in `psql` and then through the writer by both requested paths (the
cells of C-017…C-019). "Decision" is what a `Bulk` request does now.

| property | `COPY` (bulk) | `INSERT` (row) | decision | pin |
|---|---|---|---|---|
| `GENERATED ALWAYS` identity, named | stores the written value | `428C9` | **refuse on both**, `IdentityAlways` | `identity_always_named` |
| `GENERATED ALWAYS` identity, unnamed | server assigns | server assigns | same table; bulk | `identity_always_unnamed` |
| `GENERATED BY DEFAULT` identity, named | stores the written value | stores the written value | same table; bulk | `identity_by_default_named` |
| generated stored column, named | `42P10` | `428C9` | **refuse on both**, `GeneratedColumn` | `generated_named` |
| generated stored column, unnamed | server computes | server computes | same table; bulk | `generated_unnamed` |
| column defaults, a column subset, a reordered subset | defaults fill | defaults fill | same table; bulk | `defaults_for_the_unnamed`, `columns_in_the_write_order` |
| `INSERT` rule, `DO INSTEAD NOTHING` | rule not fired: rows stored | rule fired: nothing stored | **row fallback**, `InsertRule` | `rule_instead_nothing` |
| `INSERT` rule, `DO ALSO` | rule not fired: no audit rows | audit rows | **row fallback**, `InsertRule` | `rule_do_also` |
| a disabled rule | not fired | not fired | same table; bulk | `rule_disabled` |
| auto-updatable view | `42809` cannot copy to view | rows reach the base table | **row fallback**, `View` | `view_auto` |
| view with an `INSTEAD OF` trigger | fires the trigger (Postgres allows this COPY) | fires the trigger | **row fallback**, `View` (the ruling: every view) | `view_instead_of` |
| view Postgres will not insert into | `42809` | `55000` | **row fallback**, `View`; both answer `55000` | `view_not_insertable` |
| view `WITH CHECK OPTION`, a row outside it | `42809` | `44000` | **row fallback**, `View`; both answer `44000` | `view_check_option` |
| materialized view | `42809` cannot copy | `42809` cannot change | **row fallback**, `View`; both answer `42809` | `materialized_view` |
| `BEFORE` and `AFTER` row triggers | fire per row; a `NULL` return skips the row; the count excludes it | the same | same table, same audit rows, same count; bulk | `row_triggers` |
| statement-level `INSERT` trigger | fires once for the write | fires once per statement, so once per group or row | **row fallback**, `StatementTrigger`: both requests fire it the same number of times | `statement_trigger`, `a_statement_trigger_sees_the_same_bounded_inserts_whatever_was_asked` |
| a disabled statement trigger | not fired | not fired | same table; bulk | `statement_trigger_disabled` |
| partitioned table | rows routed | rows routed | same partitions; bulk | `partitioned` |
| a row that fits no partition | `23514`, nothing stored | `23514`, nothing stored | one refusal on both; bulk | `no_partition_for_a_row` |
| inheritance child, inheritance parent | the named table only | the named table only | same table; bulk | `inheritance_child`, `inheritance_parent` |
| unlogged table | stored | stored | same table; bulk | `unlogged` |
| another session's temporary table | `0A000` | `0A000` | one refusal on both; a session's own temporary table cannot exist here, since discovery and the write may run on different pooled sessions | `another_sessions_temporary_table_is_refused_the_same_on_both_paths` |
| foreign table (`postgres_fdw`) | stored through the wrapper | stored through the wrapper | **row fallback**, `ForeignTable`: one wrapper was measured and agreed, and no other wrapper's `COPY` can be measured here, so `INSERT`'s meaning is kept for all | `foreign_table` |
| partitioned table with a foreign partition | routed through the wrapper | routed through the wrapper | **row fallback**, `ForeignTable`, for the same reason | `foreign_partition` |
| row-level security active for the role | `0A000` COPY FROM not supported | policy checked per row | **row fallback**, `RowSecurity` | `row_security_passes` |
| a row the policy refuses | (not reached) | `42501`, the policy's text | both answer the server's `42501`; nothing stored | `row_security_refuses` |
| row-level security, the table's owner | stored (the policy spares the owner) | stored | same table; bulk | `row_security_spares_the_owner` |
| no `INSERT` grant, or a column grant that misses a written column | `42501` at `COPY` | `42501` at the first row | **refuse on both** at `open`, `PermissionDenied { Insert }` | `no_insert_grant`, `column_grant_misses_a_column` |
| a column grant that covers the write | stored | stored | same table; bulk | `column_grant_covers_the_write` |
| a default's sequence without `USAGE` | `42501` naming the sequence | the same | the server's `42501` on both | `sequence_without_usage` |
| deferred constraints | refused at `COMMIT` | refused at `COMMIT` | one refusal on both; bulk | `deferred_foreign_key`; step 1's `a_commit_refused_is_definite_…` |
| exclusion constraint | `23P01` | `23P01` | one refusal on both; bulk | `exclusion` |
| a unique violation with no `ON CONFLICT` | `23505` | `23505` | one refusal on both; bulk; no `ON CONFLICT` is ever sent | step 1's `a_server_refusal_is_the_same_…` |

**The statement-trigger row is a decision the ruling's rule makes, and it has a cost.** A
table with a statement-level `INSERT` trigger loses the bulk path, and the trigger fires once
per row-path statement, not once per write as a single `INSERT … SELECT` inside Postgres
would. The alternative (keep bulk, fire once) would leave the two requested paths with
different side tables, which is the divergence the S1 on rules was. Filed for the owner as a
recorded choice, not a halt.

**Not probed:** a rule or a policy on a partition reached through its parent (Postgres applies
neither there on either path); `FORCE ROW LEVEL SECURITY` (the probe asks
`row_security_active`, which answers for it); user-defined foreign-data wrappers other than
`postgres_fdw`.

### 8.4 S3: classes, texts and encodings fixed in this crate

- **The idle-in-transaction timeout.** The server ends an idle row write with a `FATAL`
  `25P03` while no request is pending, so the client saw only a closed connection and
  answered `Disconnected`. `PgConnection` now keeps how its connection ended
  (`closing_sqlstate`, a `watch` cell the connection task fills before it exits), and the
  writer folds `Disconnected` with `25P03` behind it into `Timeout { which: Read }`, at
  `write`, at the flush inside `commit`, and at `COMMIT` itself (where it is a definite
  failure: the server killed the session before it read the `COMMIT`). The read path is
  untouched and does not call the new method.
- **`42501`.** Closed in §8.2: the grant is read at `open`, and a later `42501` is the
  server's.
- **The written value in a server refusal.** A class `22` message is cut at its first `:` or
  `"` and ends in `***` (`repark_common::redaction::REDACTED`). The read path passes server
  text through unchanged, because a read sends no value; the write path is the one that can
  have a value echoed. The cut is by position, not by a list of messages, so a message this
  crate has never seen is cut the same way. A translated server message that puts the value
  before any `:` or `"` would escape the cut; the sessions here pin no `lc_messages`.
  Recorded as a limit.
- **Arrow encodings.** DataFusion hands out `Utf8View` by default and dictionaries from
  Parquet. Its own `INSERT` planning casts to the table's schema, but a door that passes its
  batches through would trip on a refusal, so the encoder takes them: it names the plain
  type of what came (`Dictionary` unpacked, large and view forms narrowed), checks that
  against the planned type as before, and casts only when the two encodings differ. The cast
  is Arrow's and copies the column once per batch; a plain array is not copied. A
  `LargeUtf8` or `LargeBinary` column whose values pass 2 GiB cannot narrow and answers
  `ConnectError::Arrow`. `ColumnEncoder` now owns its arrays (a clone of a typed array
  shares its buffers), since a cast result has no batch to borrow from.

### 8.5 S3: recorded, not changed

| id | date | what | the record |
|---|---|---|---|
| S3-order | 2026-10-09 | Rows did not sit in input order by `ctid` once widths varied (the verifier: 184 descents in 1M bulk rows, 1782 by row). | The claim is narrowed to what the writer controls: rows are **sent** in input order (C-022). Where the heap puts them is the server's free-space map, and an unordered scan of the two tables can differ. No reader may rely on scan order. §1.4's and C-006's "input order (pinned by `ctid`)" is withdrawn. |
| S3-query-timeout | 2026-10-09 | `query_timeout_ms` is the server's `statement_timeout`. A bulk write is one `COPY` statement, so the whole write must finish inside it, the time the `SELECT` takes between batches included; a row write is many statements and each has the full time. Unset by default, and the bulk failure is clean (`Timeout { which: Query }`, nothing stored). | Kept: Spark's `queryTimeout` is per statement, and one `COPY` is one statement. Lifting the timeout for the `COPY` would let a write outlive a bound the operator set. A door that sets `queryTimeout` on a long write should expect the bulk path to meet it; the row path is the way round. |
| S3-statement-count | 2026-10-09 | §1.4 said a statement-level trigger is the only thing that sees the difference between the paths. Rules, views, policies and identity columns also did. | Withdrawn; §8.3 is the full list, and a table with a statement trigger now takes rows on both requests. |
| S3-permission-point | 2026-10-09 | §1.5 said a missing `INSERT` grant surfaces at `open` on the bulk path and at the first flush on the row path. | Now at `open` on both, before any byte (§8.2). |
| S3-idle-class | 2026-10-09 | §1.5 named `Timeout { which: Read }` for an idle row write; the product answered `Disconnected`. | The product now answers what §1.5 says (C-020). |

### 8.6 The selector the door calls, after fold 1

Nothing step 1 published was removed or changed in shape, so a caller written against §1.1
compiles and behaves as before except where the rulings change behaviour. Added:

```rust
pub enum RowFallback { View, ForeignTable, InsertRule, RowSecurity, StatementTrigger, ColumnType }
impl PostgresWriter { pub fn fallback(&self) -> Option<RowFallback>; }
pub enum WriteRefusal { …, Interrupted, IdentityAlways { column: String }, GeneratedColumn { column: String } }
```

`WriteRefusal` is no longer `Copy`. `PostgresWriter::path()` is the path the open write
takes; `WriteRequest::path()` is still the column-type answer before any connection. A door
must not wrap `write()` in a timeout or a `select!` and then keep the writer: the writer
refuses from then on (C-016), which is safe, and the door should drop it.

### 8.7 Fold 1 mutations (2026-10-09)

Each applied alone to the fold's final product tree, the write pins run with the server up
(`cargo test -p repark-connect --test it write -- --include-ignored`), the file restored and
the tree compared byte for byte with a copy taken before the run.

| id | clause | mutation (file) | what it would let through | red? | red in |
|---|---|---|---|---|---|
| f1 | C-018 | a plain view is not a fallback reason (`write/target.rs`) | a `Bulk` request on a view answers `42809` | RED | 1 pins: `rules_and_views_take_the_row_path_and_leave_the_same_tables` |
| f2 | C-018 | a materialized view is not a fallback reason (`target.rs`) | the two requests answer different texts for a materialized view | RED | 1 pins: `rules_and_views_take_the_row_path_and_leave_the_same_tables` |
| f3 | C-018 | an `INSERT` rule is not a fallback reason (`target.rs`) | the verifier's S1: bulk stores rows the rule would have dropped | RED | 1 pins: `rules_and_views_take_the_row_path_and_leave_the_same_tables` |
| f4 | C-019 | row-level security is not a fallback reason (`target.rs`) | a `Bulk` request under a policy answers `0A000` | RED | 1 pins: `row_security_and_grants_are_judged_the_same_on_both_paths` |
| f5 | C-018 | a statement trigger is not a fallback reason (`target.rs`) | the trigger fires once by bulk and per statement by row | RED | 2 pins: `a_statement_trigger_sees_the_same_bounded_inserts_whatever_was_asked`, `row_triggers_stay_bulk_and_statement_triggers_take_the_row_path` |
| f6 | C-018 | a foreign table itself is not a fallback reason, only a foreign partition (`target.rs`) | bulk into a foreign table | RED | 1 pins: `a_foreign_table_or_partition_takes_the_row_path` |
| f7 | C-018 | a foreign partition is not a fallback reason (`target.rs`) | bulk through a partitioned parent into a foreign table | RED | 1 pins: `a_foreign_table_or_partition_takes_the_row_path` |
| f8 | C-017 | a named `GENERATED ALWAYS` identity is not refused (`target.rs`) | the verifier's S1: bulk overrides the identity, row answers `428C9` | RED | 1 pins: `identity_generated_and_default_columns_agree_or_refuse_on_both_paths` |
| f9 | C-017 | a named generated column is not refused (`target.rs`) | two SQLSTATEs for one mistake | RED | 1 pins: `identity_generated_and_default_columns_agree_or_refuse_on_both_paths` |
| f10 | C-019 | the `INSERT` grant is not read at `open` (`target.rs`) | a missing grant is the server's `42501` text, at different points per path | RED | 2 pins: `a_role_without_insert_is_refused_naming_the_privilege`, `row_security_and_grants_are_judged_the_same_on_both_paths` |
| f11 | C-017 | any identity column refuses, `BY DEFAULT` included (`TARGET_FACTS`) | a write Postgres accepts on both paths is refused | RED | 1 pins: `identity_generated_and_default_columns_agree_or_refuse_on_both_paths` |
| f12 | C-018 | a disabled rule still counts (`TARGET_FACTS`) | a needless row fallback | RED | 1 pins: `rules_and_views_take_the_row_path_and_leave_the_same_tables` |
| f13 | C-018 | a disabled trigger still counts (`TARGET_FACTS`) | a needless row fallback | RED | 1 pins: `row_triggers_stay_bulk_and_statement_triggers_take_the_row_path` |
| f14 | C-018 | a row-level `INSERT` trigger counts as a statement trigger (`TARGET_FACTS`) | every table with a row trigger loses the bulk path | RED | 1 pins: `row_triggers_stay_bulk_and_statement_triggers_take_the_row_path` |
| f15 | C-018 | a `Row` request reports a fallback reason (`target.rs`) | the door shows a fallback that did not happen | RED | 4 pins: `a_foreign_table_or_partition_takes_the_row_path`, `row_security_and_grants_are_judged_the_same_on_both_paths`, `row_triggers_stay_bulk_and_statement_triggers_take_the_row_path`, `rules_and_views_take_the_row_path_and_leave_the_same_tables` |
| f16 | C-019 | the grant is read over every column, not the written ones (`TARGET_FACTS`) | a column grant that covers the write is refused | RED | 2 pins: `row_security_and_grants_are_judged_the_same_on_both_paths`, `every_fallback_reason_says_why_copy_was_not_used` |
| f17 | C-019 | `42501` goes back through the read path's mapping (`write.rs`) | a policy or a sequence refusal is reported as a missing `SELECT` grant | RED | 1 pins: `row_security_and_grants_are_judged_the_same_on_both_paths` |
| f18 | C-020 | a class `22` message is passed whole (`write.rs`) | the written value in the error | RED | 1 pins: `a_server_refusal_names_no_written_value` |
| f19 | C-020 | the cut is at the first quote only (`write.rs`) | a value after a colon with no quote survives; the interval text differs | RED | 1 pins: `a_server_refusal_names_no_written_value` |
| f20 | C-020 | `25P03` is not folded (`write.rs`) | an idle row write answers `Disconnected` | RED | 1 pins: `a_row_write_idle_past_the_read_timeout_ends_as_that_timeout` |
| f21 | C-020 | the flush inside `commit` is not settled (`write.rs`) | the same, at `commit` | RED | 1 pins: `a_row_write_idle_past_the_read_timeout_ends_as_that_timeout` |
| f22 | C-020 | `write` is not settled (`write.rs`) | the same, at `write` | RED | 1 pins: `a_row_write_idle_past_the_read_timeout_ends_as_that_timeout` |
| f23 | C-021 | a `Dictionary` is not unpacked (`types/postgres/encode.rs`) | a dictionary column refuses by type | RED | 2 pins: `other_arrow_encodings_store_the_same_table_on_both_paths`, `another_encoding_of_the_same_values_encodes_to_the_same_bytes` |
| f24 | C-021 | `Utf8View` is not narrowed (`encode.rs`) | the engine's default string type refuses | RED | 2 pins: `other_arrow_encodings_store_the_same_table_on_both_paths`, `another_encoding_of_the_same_values_encodes_to_the_same_bytes` |
| f25 | C-021 | `LargeBinary` is not narrowed (`encode.rs`) | a large binary column refuses | RED | 2 pins: `other_arrow_encodings_store_the_same_table_on_both_paths`, `another_encoding_of_the_same_values_encodes_to_the_same_bytes` |
| f26 | C-016 | no in-flight mark (`write.rs`) | the verifier's S2 | RED | 1 pins: `a_write_dropped_mid_flight_poisons_the_writer_and_stores_nothing` |
| f27 | C-020 | the connection task records no SQLSTATE (`pool.rs`) | the writer cannot tell an idle kill from a lost connection | RED | 1 pins: `a_row_write_idle_past_the_read_timeout_ends_as_that_timeout` |
| f28 | C-020 | `Disconnected` at `COMMIT` is always `CommitUnknown` (`write.rs`) | an idle kill before `COMMIT` is reported as an unknown outcome | RED | 1 pins: `a_row_write_idle_past_the_read_timeout_ends_as_that_timeout` |

f28 was green on the first run: no case reached `COMMIT` as the first request after the idle
wait, because the remainder flush inside `commit` failed first. The idle cell now also writes
exactly one full group, so nothing is left to flush and `COMMIT` meets the closed session.

### 8.8 Fold 1 live run and gates (2026-10-09)

Rootless Docker, `make pg-up` (Postgres 16.15, the C-0 container; none was running when
the fold began). `cargo test --locked -p repark-connect -- --include-ignored`: **260 passed, 0
failed, 0 ignored** (3 unit, 257 in the integration binary), three consecutive runs on the
final tree (23.9 s to 29.1 s). Without the server: 177 passed, 83 ignored. A green run leaves
no schema, role or foreign server behind (counted before and after); `postgres_fdw` stays
installed in the disposable database.

| gate | exit |
|---|---|
| `cargo fmt --check` | 0 |
| `make rust-clippy` | 0 |
| `make rust-panic-ban` | 0 |
| `cargo test --locked -p repark-connect` | 0 (177 passed, 83 ignored) |
| `cargo test --locked -p repark-connect -- --include-ignored` with `REPARK_PG_URL` | 0 (260 passed), three runs |
| `cargo check --locked -p repark-connect --no-default-features --tests` | 0, no warning |
| `python3 scripts/sync_map_md.py --check` | 0 |
| `bash scripts/check_map_md.sh --base origin/main` | 0 |
| `make check-ledger-grammar` | 0 |
| the pre-commit hook on each of the fold's three commits | 0 |

The fold's three commits, one per finding group: the cancelled write (S2); the relation
properties, with the `42501` S3 they carry (S1); the remaining S3s.

## 9. Step 2 (2026-10-09): the both-door routing seam (Muse, before code)

Branch `feat/c-4-routing` from `dd49a376`. Step 1's `crates/repark-connect/src/write/`
and the encoder are not edited in this slice. The native module is rebuilt
(`make develop`, 1m20s). The C-0 container was already running when this slice
started (another lane's), so this slice uses `make pg-url` and never runs
`make pg-down`. `psycopg[binary]` was installed into the project's `.venv`
(the live-db extra) for the oracle side of the live cells.

### 9.1 Where each door plans an INSERT today

Both doors plan an Iceberg-target INSERT through DataFusion's DML planner
(`insert_to_plan`: column-list mapping, NULL fill for unlisted columns, a cast
of every source column to the target field type) and execute it through the
fork's `TableProvider::insert_into`. Owned paths intercept first: the ANSI
door's `session_insert.rs` (session write conf) and the Spark door's
`append_with_options.rs` (statement write options). Both owned paths plan the
whole INSERT with DataFusion, take the DML input as the source frame, stream it,
and return `read_empty()`.

A connect-source target walks the same code until the provider hook: every
door-level check passes it through (details in §9.2), DataFusion plans the DML
against `PostgresTable`, and the default `TableProvider::insert_into`
(`datafusion-catalog 54.1`, `table.rs:340`, `InsertOp` flavor) refuses. No door
implements the hook for `PostgresTable` and this slice does not add it (§9.5).

### 9.2 What a connect-source target does today (measured 2026-10-09)

Live facade session mounting the C-0 container as `pg`, table
`pg."seamprobe".t`, both doors. Every row below is a measured refusal; nothing
is written in any case.

| statement | Spark door | ANSI door |
|---|---|---|
| `INSERT INTO pg.s.t (cols) VALUES …` | `UnsupportedOperationException`: `This feature is not implemented: Insert into not implemented for this table` | same text, same class |
| `INSERT INTO pg.s.t SELECT …` | same | same |
| `INSERT OVERWRITE pg.s.t SELECT …` | same | same |
| `UPDATE pg.s.t SET …` | `UnsupportedOperationException`: `UPDATE operation on table 'pg.seamprobe.t' caused by This feature is not implemented: UPDATE not supported for Base table` | same shape |
| `DELETE FROM pg.s.t` | the DELETE shape of the same | same shape |
| `CREATE TABLE pg.s.fresh AS SELECT …` | `UnsupportedOperationException`: the `CONNECT-DECL-pg-ddl` read-only text | same text, same class |
| `MERGE INTO pg.s.t …` | the same `CONNECT-DECL-pg-ddl` text (catalog resolution) | `Unsupported SQL statement` (the ANSI door has no MERGE-into-source path; measured) |
| `df.write.jdbc(url, table, mode="append")` | `PySparkNotImplementedError`: `[NOT_IMPLEMENTED] jdbc is not implemented.` (after the save-mode check) | n/a |
| `df.write.format("jdbc").save()` (no path) | `AnalysisException`: `'path' is not specified.` | n/a |
| `df.write.format("jdbc").save(path)` | `AnalysisException`: `DATA_SOURCE_NOT_FOUND …` | n/a |

Two findings that shape the routing:

- Neither door's P11 read-only check fires for a mounted source in a live
  session: `TRUNCATE TABLE pg.s.t` answers `TABLE_OR_VIEW_NOT_FOUND`, and every
  INSERT reaches DataFusion's default hook. P11 is pinned only in unit tests
  with a hand-set read-only set (`router/tests.rs::read_only_set_reaches_p11_refusal`,
  `guards/tests.rs`). The routing keys on `CatalogRegistry::is_database_source`
  (populated live: the CTAS/MERGE texts prove the specs are visible to the
  doors), never on the read-only set, so it behaves one way whether or not the
  set is populated. The ANSI text guard is made spec-aware for the same reason
  (§9.5): a hand-set read-only set without specs keeps refusing exactly as its
  pins assert.
- `INSERT OVERWRITE` into `pg` answers the same default-hook text as append
  today (the overwrite arms' P11 checks do not fire live either). The slice
  replaces it with the named modes refusal (§9.7).

Neighbors that must not change (measured on this tree): `INSERT INTO nosuch.s.t
SELECT 1` → `AnalysisException: table 'nosuch.s.t' not found` (both spellings);
`CREATE TABLE nosuch.s.fresh AS SELECT 1` → `unknown catalog `nosuch``;
`TRUNCATE TABLE pg.s.t` → `TABLE_OR_VIEW_NOT_FOUND`; every `writer.jdbc` bad-mode
cell (`INVALID_SAVE_MODE`, caller spelling kept); the `test_c2_read.py` DDL arms.

A plain Iceberg `INSERT INTO ice.ns.t VALUES (1), (2)` on the Spark door returns
`[Row(count=2)]` (measured); the ANSI door executes the same DML plan shape
(code-derived). Owned Iceberg appends return `read_empty()` on both doors.

### 9.3 The exact function each door will call

One driver, in `repark-core` (the only crate both doors and the binding reach):

```rust
pub async fn execute_postgres_write(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    frame: DataFrame,
    write: PostgresWrite,
    session_zone: &str,
) -> datafusion::error::Result<WriteReport>
```

`PostgresWrite` carries a `PostgresWriteTarget::Mounted { source, schema, table }`
or `::Url { url, dbtable, props }`, the listed columns (`None` for all in
order), the door's case rule, and the requested `WritePath` (re-exported through
`repark-core`). `ReparkSession::write_postgres(&self, frame, write)` is the thin
`&self` wrapper (zone, context and catalog snapshot from the session) that the
binding's new `session_write_postgres` pyfunction calls. The driver is the only
caller of step 1's selector; the doors never touch `WriteRequest` directly.

- SQL doors: a new `pg` arm at the bottom of each `execute_insert_routed`
  (ANSI `crates/repark-sql/src/router.rs`, Spark
  `crates/repark-spark/src/router.rs`), where the P11 refusal is computed today:
  when the target's first segment is a Postgres-kind database source (new
  `CatalogRegistry::database_source_kind` accessor; a SQL Server/Trino mount
  falls through to today's behavior), an append plans its source SQL through the
  door's own SELECT path (Spark: `spark_ast::execute_passthrough`; ANSI:
  `delegate`), resolves the listed columns, and calls the driver with
  `WritePath::Bulk`. Overwrite and `REPLACE INTO` refuse there with the named
  rows of §9.7 and never reach the driver. The zone string comes from
  `repark_functions::session_time_zone::session_time_zone_from_options` (the
  extractors' existing accessor; both doors already depend on
  `repark-functions`).
- Writer door: `writer_jdbc` (facade `io_declared.py`) validates the effective
  save mode (the `mode` argument when given, else the writer's mode, per
  PySpark 4.1.2's `self.mode(mode)._jwrite.jdbc(url, table, jprop)`, read on
  this box), merges `writer._options` under the `properties` argument
  (case-insensitive, properties win), lifts `write.path` (default `bulk`,
  anything else an `IllegalArgumentException` naming `bulk` and `row`), and
  calls `session_write_postgres` for `append` only. Every other mode refuses
  with the modes row of §9.7. `format("jdbc").save()` routes to the same helper
  (url/dbtable from options); `format("postgres").save()` keeps today's
  `DATA_SOURCE_NOT_FOUND`, which is Spark's answer for an unknown format.

No new trait. No change to the merged C-2 provider's public shape at all: the
slice deliberately does not implement `insert_into`, so the read path the step-1
verifier is reading is untouched. Each door has one routing place. No HALT.

Why the doors route around DataFusion's DML planner instead of implementing
`insert_into`: `insert_to_plan` fills unlisted columns with NULL, which would
write NULL where the server's default belongs (step 1 C-011: a write names only
its columns and the others take their defaults). The driver projects the source
frame to the listed columns and names exactly those, so defaults survive, and it
keeps the timestamp unplacement (§9.4) explicit per array instead of inheriting
an implicit cast.

### 9.4 Batches, casts, the clock, the transaction, the return

The driver resolves the target with C-2's `discover` (unchanged: a role without
`SELECT` and a relation with an unmapped column refuse exactly as today, pinned
as the brief's owner questions), checks the source width against the listed
width (Spark's `INSERT_COLUMN_ARITY_MISMATCH` shape, core-rendered, for VALUES
and SELECT alike), then pulls the source stream batch by batch. Each batch is
cast column by column to the encoder's Arrow type with Arrow's cast kernel,
except the two timestamp columns: a zoned array into a `timestamp` column is
converted instant by instant to the wall clock in the session zone (unique per
instant, so the gap/overlap refusals of `CONNECT-DIV-pg-timestamp-zone` cannot
fire in this direction; past the calendar refuses); a zoneless array into a
`timestamp` column is already the wall clock and passes through; a zoneless
array into a `timestamptz` column is placed forward in the session zone with the
read path's gap/overlap refusals. A stream error drops the writer, which rolls
the transaction back; otherwise every batch goes through `PostgresWriter::write`
and the driver commits.

The Postgres `COMMIT` runs inside the statement's execution. There is no engine
commit and no two-phase anything: a process death after `COMMIT` leaves the rows
committed, exactly as Spark's one-partition JDBC write does (FL-18). Step 1's
`CommitUnknown` contract is unchanged.

The statement returns an empty frame (`read_empty()`) on both doors, and the
writer returns `None`. That is Spark's answer for `INSERT` (brief-given) and
both doors' owned-append convention. It differs from a plain Iceberg `INSERT`,
which returns `[Row(count=N)]` through the DataFusion-passthrough path the pg
write deliberately bypasses (§9.3): recorded difference, pinned on both doors.

Spark's JDBC `df.write.jdbc(...)` / `INSERT INTO` over a JDBC catalog was not
measured: no Postgres JDBC driver is on this box (searched `/tmp/sparkenv` and
`/`, no `postgresql-*.jar`), and the brief forbids downloading one. PySpark
4.1.2's `readwriter.py` (in `/tmp/sparkenv`) was read instead for the mode and
option-merge semantics used above.

### 9.5 Guards touched, precisely

- ANSI `refuse_read_only_catalog_dml` (text guard): a plain-`INSERT` verb whose
  target's first segment is a Postgres-kind database source with specs is let
  through to the router; every other verb, and every read-only name without
  specs, refuses exactly as today. Net door behavior for `INSERT OVERWRITE`
  stays a refusal (now the named modes error from the router). Listed in the
  hand-back under `guards_relaxed` with the Spark cells (R-3 routing; brief pins).
- Spark `execute_insert_routed`: the new `pg` arm sits where the P11 refusal is
  computed, after the write-options/owned-append arm (a pg target with statement
  write options keeps today's P11 refusal: the SQL door takes no flag, and
  options must not be silently ignored) and after `prepare_positional_insert`
  (a `PARTITION` clause on a pg target keeps today's `NON_PARTITION_COLUMN` /
  clause refusal). `routes_positional_by_name`, `refuse_insert_source_types`
  and both doors' `insert_arity` pass a pg target through untouched (verified:
  each returns early when the catalog handle is missing).
- No P11 text changes; no `insert_into` implementation, so a shape the arm
  misses still ends at DataFusion's default-hook refusal, never at a silent
  wrong write.

### 9.6 What stays refused, and the rows

- Save modes other than append and SQL `INSERT OVERWRITE`: new row
  `CONNECT-DECL-pg-write-modes` (dated 2026-10-09), named by the writer-door
  `PySparkNotImplementedError` and by both doors' `NotImplemented` texts.
- `UPDATE` and `REPLACE INTO`: new row `CONNECT-DECL-pg-write-upsert` (dated
  2026-10-09), named by a `pg` arm at the top of each door's update path and by
  the insert arm. `MERGE INTO pg…` keeps answering the `CONNECT-DECL-pg-ddl`
  catalog text through `catalog_handle` (a named error with a dated row already;
  re-routing it would touch the shared catalog lookup for no behavioral gain),
  and the upsert row says so. `DELETE FROM pg…` is untouched (not in the
  slice): DataFusion's default text, no row.
- `CREATE TABLE … AS SELECT` into Postgres: already the `CONNECT-DECL-pg-ddl`
  text on both doors (measured §9.2); pinned, not changed.
- The `CONNECT-DECL-pg-ddl` row's `INSERT, UPDATE and DELETE refuse as not
  implemented` sentence is rewritten (dated): append INSERT writes, the rest as
  above.

### 9.7 Four-line records (2026-10-09; Spark halves read, not run)

PySpark halves come from PySpark 4.1.2's `sql/readwriter.py` on this box; JDBC
server halves are documented, no value claim (no pgjdbc on the box, §9.4).

| id | date | question | Flink | Spark | default acted on |
|---|---|---|---|---|---|
| FL-6 | 2026-10-09 | What does the write statement return? | n/a (no JDBC SQL return) | `spark.sql("INSERT …")` returns an empty frame | Empty frame both doors; writer `None` (§9.4). Spark governs the surface. |
| FL-7 | 2026-10-09 | Unlisted columns in a column-list INSERT? | n/a | `DEFAULT` fill | Server defaults via `WriteRequest::columns` (§9.3). |
| FL-8 | 2026-10-09 | Zoned input into a `timestamp` column? | n/a | pgjdbc renders the instant in the JVM zone | Wall clock in the session zone (§9.4). The session zone is the JVM zone's stand-in per `CONNECT-DIV-pg-timestamp-zone`. |
| FL-9 | 2026-10-09 | Zoneless input into a `timestamp` column? | n/a | a `LocalDateTime` goes as its wall clock | Pass through as the wall clock; the encoder takes zoneless for `timestamp` (C-003). |
| FL-10 | 2026-10-09 | Zoneless input into a `timestamptz` column? | n/a | pgjdbc assumes the JVM zone | Forward-place in the session zone with the read path's gap/overlap refusals. The encoder refuses zoneless (C-003), so passing it through would refuse where Spark writes. |
| FL-11 | 2026-10-09 | Effective save mode of `df.write.jdbc(url, table, mode, properties)`? | n/a | `self.mode(mode)._jwrite.jdbc(url, table, jprop)`: the argument overrides, `None` keeps the writer's (default `error`) | The argument when given, else the writer's mode; the argument is validated case-insensitively with `INVALID_SAVE_MODE` (pinned). A bare `jdbc()` is `error` and refuses under the modes row, as Spark errors on an existing table. |
| FL-12 | 2026-10-09 | `.option()`s and the `properties` argument on `jdbc()`? | n/a | PySpark passes `properties` as `jprop` beside the writer's options; the JVM merge order was not read on this box | Merge case-insensitively, `properties` win (recorded assumption). Only `write.path` is lifted; every other key goes to `PostgresSettings::from_props`, which judges unknown keys as on the read door. |
| FL-13 | 2026-10-09 | `format("jdbc").save()` without a path? | n/a | pathless `save()` works off options | Route to the same helper (url/dbtable required). `format("postgres").save()` keeps `DATA_SOURCE_NOT_FOUND`: Spark has no `postgres` format. |
| FL-14 | 2026-10-09 | `partitionColumn` et al on a write? | n/a | forwarded to the driver, which ignores them | Lift and ignore (documented): refusing a known key as unknown would misname it, and Spark proceeds. |
| FL-15 | 2026-10-09 | `batchsize` and other unmapped Spark write options? | n/a | accepted and used | `from_props` refuses them as unknown keys under `CONNECT-DIV-pg-unknown-option` (NS rank 4). Mapping `batchsize` onto `rows_per_insert` needs the owner (step-1 FL-3 left it open); filed as an owner question, no Spark cells to justify an exemption. |
| FL-16 | 2026-10-09 | A `query`/`dbtable`-subquery write target? | n/a | the writer needs a table | `WriteRefused::QueryTarget` (step 1, unchanged). |
| FL-17 | 2026-10-09 | Missing/extra source columns vs the target? | n/a | `INSERT_COLUMN_ARITY_MISMATCH` (VALUES shape established on both doors) | That shape, core-rendered, for VALUES and SELECT alike. |
| FL-18 | 2026-10-09 | Crash between `COMMIT` and the return? | XA resolves by id | the task fails; a retry may double-write | Committed stays committed; no retry anywhere (step-1 FL-1/FL-2 carry over). |

### 9.8 Step-2 files (plan; map.md lockstep in the same commits)

New: `crates/repark-core/src/session/write_postgres.rs` (the driver) with its
tests beside `session/tests/`; `crates/repark-python/src/session_write_postgres.rs`
(the pyfunction; `session_write_options.rs` is near its ceiling);
`python/repark-parity/tests/live_db/test_c4_write.py` (the live cells).
Edited: `session.rs` (one `mod` line), `session/zone_localiser.rs` (unplace +
zoned forward-place), `catalog_state.rs` (`database_source_kind`), both
`router.rs` files (one `pg` arm each, bodies in new sibling modules if ceilings
require), both update paths (upsert arm), the ANSI text guard (§9.5),
`io_declared.py` + `writer_readwriter.py` (writer door), the two registry rows
plus the `pg-ddl` amendment, the two refusal tests of §9.2, this ledger. No
`Cargo.toml`, no `.github`, no ceiling raise.

## 10. Step 2 (2026-10-09): what the routing slice built (Muse, after code)

Branch `feat/c-4-routing`. The fold (`ae020b9f`) merged in; the doors below are
written against the folded core. The slice's §9 plan held to the file: the only
deviations are the fold adoption (§10.2) and the decisions in §10.3.

### 10.1 The shape

One driver, `repark_core::session::write_postgres::execute_postgres_write`:
resolve the target, build settings, discover, resolve the listed columns, check
the frame width (short: Spark `INSERT_COLUMN_ARITY_MISMATCH`, core-rendered;
wide: DataFusion's count text), open the selector, shape each batch (cast real
type changes, unplace session-zone timestamps for `timestamp` columns,
forward-place zoneless ones for `timestamptz`), stream every batch through
`PostgresWriter::write`, commit. Both SQL doors and the writer binding call it;
nothing else touches `WriteRequest`. Each door records the report on the
builder-installed `LastPostgresWriteReport` carrier; the take-report binding
reads it back once. Both doors return `read_empty()` (`[Row()]`, pinned), the
writer returns `None`.

- Spark door: `repark-spark/src/router/pg_insert.rs` reads `write.path` off the
  statement write options (shared core parser: absent means bulk, `row` forces
  the INSERT path, anything else refuses as `Configuration` naming both values).
- ANSI door: `repark-sql/src/pg_insert.rs` always asks bulk; the text guard lets
  a plain `INSERT` at a Postgres-kind source with specs through to the router.
- Writer door: `session_write_postgres` pyfunction (URL targets, exact column
  names) plus `session_take_postgres_write_report`; facade `writer_jdbc` (mode
  argument wins, else the writer's mode; only append writes; options merge under
  `properties` case-insensitively; `write.path` lifts; url/dbtable/path strip)
  and `format("jdbc").save()` off url/dbtable options. `save()` changed two
  lines in place (`writer_readwriter.py` holds 999 of 1000).
- Refusals: non-append modes and `INSERT OVERWRITE` under
  `CONNECT-DECL-pg-write-modes`; `UPDATE` and `REPLACE INTO` under
  `CONNECT-DECL-pg-write-upsert`; `MERGE`/`CREATE TABLE AS` keep the `pg-ddl`
  text; `DELETE` untouched. The `pg-ddl` row's DML sentence is amended (dated).

### 10.2 Fold-1 adoption

The report carries the taken path and the fallback sentence (`RowFallback`'s
Display), both read off the open writer; encoder-accepted encodings (view,
large, dictionary forms) pass through uncast while real type changes still
cast. `open()` errors (`PermissionDenied { Insert }`, `WriteRefused`,
`RelationNotFound`) flow through the existing `External` + `database source`
context. `write()` was already awaited directly, never in a timeout or
`select!`. The writer door and both SQL doors take all of this through the one
driver.

### 10.3 Decisions the build forced

- **Writer columns are exact names** (`case_insensitive: false`): Spark quotes
  the DataFrame's names into its INSERT list, so a case mismatch fails there;
  it refuses here. Recorded choice.
- **The identity/generated refusal class on the doors is `PySparkException`**,
  the shared `External` flattening (`error_map.rs` keeps `Config` and
  `NotImplemented` and flattens the rest, read door included); the core-direct
  conversion is `Analysis`. The sentence is the core's, naming the column.
  Changing the classifier would move read-door neighbors, so the door class is
  pinned as is; it is also the closer Spark shape (a failed JDBC write is an
  `ERROR`, not analysis). Recorded choice, owner-visible.
- **`REPLACE INTO` parses only on `GenericDialect`**: the ANSI door names the
  upsert row (pinned), the Spark door's parser refuses the statement first
  (pinned live); the Spark `replace_into` arm stays as sink-side defense.
- **Unconstrained numeric dscale**: the door writes the Arrow scale, so a
  server-parsed seed (`0.5`, dscale 1) differs by bytes from the door's value;
  bulk-vs-row agree byte for byte, the seed compares by value. Recorded.
- **No ANSI door from Python** (standing `repark-sql` NON-edge): the ANSI fold
  pins are `#[ignore]`d Rust live cells (`psql` setup, no new dev-deps);
  `repark.sql` keeps the default-hook refusal.
- **`batchsize` and unmapped write options still refuse as unknown keys**
  (FL-15 carried; the owner question stands).
- **Citations**: §6 stands; step 2 adds no ingest contract (the doors call the
  cited core), so no second citation.

### 10.4 Live runs (2026-10-09, the C-0 container, Postgres 16.15)

- `python/repark-parity/tests/live_db/test_c4_write.py`: 14 cells green,
  including the fold pins on the Spark door and the writer (view/rule take the
  row path and say why; the named identity column refuses with the core text),
  the byte-identical bulk-vs-row tables, the DST-zone timestamp round trip, and
  the two owner-question pins (a write-only role names the missing `SELECT`
  privilege; an unmapped column names its row even when unnamed).
- `crates/repark-sql/src/pg_insert.rs`: the 3 ignored ANSI live cells green
  with `REPARK_PG_URL`.
- `cargo test -p repark-connect -- --include-ignored`: 260 green, re-validating
  the merged fold on this branch.
- Whole `live_db/`: 46 passed, 5 xfailed (the pre-existing S0 strict xfails).
  No schema, role, publication or slot left behind (counted after; one probe
  schema and one role from a failed run removed by hand).
- Live-Spark re-measure attempted the same day: the JVM gateway will not start
  on this box (no jars offline), so the Spark halves stay read-not-run per
  §9.4; the `INVALID_SAVE_MODE` oracle cells re-ran green against
  `facade_reader_writer_oracle.json`.

### 10.5 Gates

`cargo fmt --check`; `make rust-clippy`; `make rust-panic-ban`; `cargo test
--locked` on repark-core, repark-spark, repark-sql, repark-python and
repark-connect in one invocation (lib: 1428 + 2639 + 400 + 158 + 3, all green;
integration binaries green); `make develop`; `test_io_declared_1.py` (29) plus
`test_pg_jdbc_options.py` green; `uvx ruff@0.15.22 check .` and `format --check
.`; `python3 scripts/sync_map_md.py --check`; `bash scripts/check_map_md.sh
--base origin/main`; `make check-ledger-grammar`; the pre-commit hook on every
commit. `build()` carries `#[allow(clippy::too_many_lines)]`, paid for by a
shed WHAT-comment (`session.rs` holds 1000 of 1000).

### 10.6 What stays open

C-014's C-6 half is future work mapping onto the shipped option (no second
path exists). C-015 is step 3's: DIFF-PROBE and the verifier run on the merged
tree, after this slice. The owner questions standing: a write-only role, an
unmapped column, `batchsize` onto `rows_per_insert`, and the §10.3 class
record.

## 11. Measure round (2026-10-10): COPY against INSERT for triggers and foreign tables

Branch `feat/c-4-routing`. Owner ruling 2026-10-10 (Frontier D13): the principle
"INSERT's meaning is the contract, bulk is an optimisation" is ratified, but the
two row-path routings for statement-level triggers and foreign tables (§8.3) are
NOT ratified on assertion. Measure first, then keep bulk wherever the cell shows
no divergence. Probe and verbatim output:
[c-4-measure/](c-4-measure/map.md). Server: PostgreSQL 16.15, the C-0 container.
Each cell runs the same 3 rows through one plain `INSERT` and one `COPY … FROM
STDIN (FORMAT BINARY)`.

### 11.1 The cells

| cell | INSERT | COPY | diverges? |
|---|---|---|---|
| `stmt-before-after`: `BEFORE` + `AFTER … FOR EACH STATEMENT`, each appending to an audit table | 3 stored; audit `[before, after]` | 3 stored; audit `[before, after]` | no |
| `stmt-transition`: `AFTER INSERT … REFERENCING NEW TABLE AS n`, auditing `count(*)` and the ordered ids | 3 stored; audit `(3, '1,2,3')` | 3 stored; audit `(3, '1,2,3')` | no |
| `fdw-postgres`: loopback `postgres_fdw` table over a remote table with a default, a check, and row plus statement triggers; the write omits the defaulted column | 3 stored with `n` NULL; remote audit `row,stmt` per row, 6 rows in order | 3 stored with `n` NULL; the same 6 audit rows in order | no |
| `fdw-postgres violation`: one row against the remote check | `23514` with the check text; 0 stored | the same `23514` and text; 0 stored | no |
| `fdw-file`: `file_fdw` table (the image ships it) | `0A000 cannot insert into foreign table`; 0 stored | the same `0A000` and text; 0 stored | no |

Two measured notes behind the table. The `n` NULL on both foreign paths is the
FDW layer, not the probe: a direct local `INSERT` omitting `n` stores 7, so
`postgres_fdw` sends NULL for the unlisted column on both paths alike. The
`row,stmt` interleaving on both foreign paths shows one remote `INSERT` per row
at the default options, so a remote statement trigger fires per row on both
paths at any size under those options.

### 11.2 Decisions

No cell diverges, so both routings move, each exactly where measured:

- **Statement-trigger tables go back to bulk (C-023).** COPY fires each
  statement trigger once per write, which is one `INSERT` statement's meaning,
  so a `Bulk` request takes COPY again. A `Row` request keeps firing once per
  statement (the bounded-inserts count: full groups plus one single-row
  statement per remainder row), where each statement carries the single-INSERT
  meaning, as Spark's `batchsize` batches do. C-018's "same side tables" is
  narrowed for this property only: the two requested paths now agree on the
  target rows and differ on the trigger's firing count by construction. The old
  routing forced `Bulk` requests onto the multi-fire path for agreement's sake;
  D13 withholds ratification from exactly that trade.
- **Direct `postgres_fdw` tables go back to bulk (C-024).** `TARGET_FACTS`
  resolves the wrapper name, and only `relkind = 'f'` with
  `fdwname = 'postgres_fdw'` loses the fallback. Every other foreign table
  keeps `RowFallback::ForeignTable`: `file_fdw` (measured identical, but it
  never writes, so keeping the fallback changes nothing observable), every
  unmeasured wrapper (a writable wrapper without COPY support stores by INSERT
  and refuses by COPY; routing it to bulk would be the assertion D13 forbids),
  and a partitioned table with any foreign leaf (an unmeasured combination).
  A NULL wrapper name falls back: fail-closed.

`GENERATED ALWAYS` identity named in a write still refuses on both paths
(ratified; Spark's JDBC writer names every column and Postgres refuses it too):
a record, no code.
