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
| C-013 | `INSERT INTO <source>.<schema>.<table> SELECT …` routes to the sink on the ANSI door and on the Spark door, one test row per door (order R-3). | Step 2, another lane. | OPEN | Does the routing call `WriteRequest::open` with `WritePath::Bulk` on both doors, with one pin per door? |
| C-014 | The Spark-door writer option `write.path = bulk \| row` reaches `WritePath`, default `bulk`, and the C-6 Python convenience maps onto the same option (ruled 2026-10-08). | Step 2, another lane. | OPEN | Does an unknown value refuse, and does `row` force the INSERT path for a table with no row-only column? |
| C-015 | DIFF-PROBE and the verifier pass (order R-1), and the unit's last live run on the C-0 container. | Step 3. | OPEN | Which verifier, and on which tree? |

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
