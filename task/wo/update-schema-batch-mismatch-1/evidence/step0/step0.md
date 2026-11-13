# UPDATE-SCHEMA-BATCH-MISMATCH-1, step 0 on main `dd77603a`

**Builder:** `claude-opus-5-5`, 2026-11-13. **Build:** main `dd77603a`, release profile, with a print hook that is not
part of any commit (below). **No product change.**

## The print hook

An environment-gated hook, applied to the working tree only for this run and reverted before the commit. With
`REPARK_STEP0_BATCH_LOG` set, it appends one JSON line per event to that file; unset, the code does what main does.

- In `collect_identity_update_rows` (`crates/repark-iceberg/src/write/predicate_dml.rs:387`): the statement's physical
  plan, then for every non-empty new-value batch, in arrival order, its row count, the `_file` names it carries, the
  schema `project_update_data_batch` gives it (name, type, nullable, NULL count per field) and the stream's planned
  schema.
- In `register_update_values_table` (`crates/repark-iceberg/src/write/predicate_dml/cow_commit.rs:91`): the batch
  count and each batch's nullability as `MemTable::try_new` receives them.

The probe ([probe/print_batches.py](probe/print_batches.py)) runs the first probe's cells: both layouts, the five
predicates, v2 copy-on-write, v2 merge-on-read and v3, 10 repeats each, on the Spark door and on the native door
(`repark.sql`), plus a control that sets a `NOT NULL` column to NULL. One table per attempt, two single-row INSERT statements, so
two data files. [probe/render_step0.py](probe/render_step0.py) renders [table-dd77603a.md](table-dd77603a.md) from
[raw/main-dd77603a.jsonl](raw/main-dd77603a.jsonl).

## The batches

Spark door, the cells that reach the seat (each batch shown as its set of nullable fields, in arrival order; v3 adds
`_last_updated_sequence_number` to every set, left out here):

| Statement | Layout | Kind | Batches | Order seen (count, outcome) | Refused |
|---|---|---|---|---|---|
| `d <=> 0.0` | mixed-nulls | v2 COW | 2 | `{e}` then `{i,e}` (6, refused); `{i,e}` then `{e}` (4, answered) | 6 of 10 |
| `d <=> 0.0` | mixed-nulls | v2 MOR | 2 | `{e}` then `{i,e}` (8, answered); `{i,e}` then `{e}` (2, answered) | 0 of 10 |
| `d <=> 0.0` | mixed-nulls | v3 | 2 | `{e}` then `{i,e}` (8, refused); `{i,e}` then `{e}` (2, answered) | 8 of 10 |
| `i IS NULL OR i <= 7` | mixed-nulls | v2 COW | 2 | `{e}` then `{i,e}` (5, refused); `{i,e}` then `{e}` (5, answered) | 5 of 10 |
| `i IS NULL OR i <= 7` | mixed-nulls | v2 MOR | 2 | both orders (5 and 5), all answered | 0 of 10 |
| `i IS NULL OR i <= 7` | mixed-nulls | v3 | 2 | `{e}` then `{i,e}` (3, refused); `{i,e}` then `{e}` (7, answered) | 3 of 10 |
| `i IS NULL OR i <= 7` | no-nulls | all three | 2 | `{}` then `{}` | 0 of 30 |
| `id = 4` | both | all three | 1 | one batch | 0 of 60 |
| `id IN (4, 6)` | both | all three | 0 | no batch reaches the seat | 0 of 60 |
| `flag IN ('f4', 'f6')` | both | all three | 0 | no batch reaches the seat | 0 of 60 |
| `SET r = NULL WHERE i IS NULL OR i <= 7` (`r` NOT NULL) | control | all three | 2 | NULL-free `{r}` first: mismatch text on COW and v3 (3); otherwise the writer's NOT NULL text (6) | see below |

- **The cause is the one the card names, measured.** All 258 printed batches have one row and one `_file`. The batch from the file
  that holds `i = NULL` marks `i` nullable; the other does not. `MemTable::try_new` takes the first batch's schema and
  refuses when a later batch has a nullable field the first one does not. From the printed nullability alone that rule
  predicts **25 refusals in 372 attempts, and the outcome was exactly those 25: 0 disagreements.**
- **The order is random because two things are unordered.** The fork reads the data files with
  `try_buffer_unordered` then `try_flatten_unordered` (`crates/iceberg/src/arrow/reader.rs:352-353` at fork
  `92242cb4`), and the identity plan repartitions round-robin over 10 partitions before the filter (the plans in
  the table). Either is enough to let either file's batch arrive first.
- **The `d` predicate refuses for the same reason as the `i` one.** Both rows match `d <=> 0.0`, and the row-4 batch
  carries its `i = NULL` along. `-0.0` against `0.0` plays no part; the column that differs in nullability is `i`.
- **Merge-on-read never refuses** because its batches go straight to the writer
  (`predicate_dml/mor_commit.rs:58`), which casts each batch to the declared write schema on its own
  (`merge/session_staging.rs:50`, `merge/mod.rs:576`). Nothing compares one batch with another.
- **The planned schema is the same on every batch.** In all 159 attempts that reached the seat, every batch of one
  statement carried the same planned nullability (the stream's schema); only the rebuilt per-batch schema differed.
- **The `NOT NULL` control.** On main a NULL into a required column already refuses on every attempt, but with
  either of two texts, depending on order: the mismatch text (3 of 9) or the writer's
  `Column 'r' is declared as non-nullable but contains null values` (6 of 9). The native door refuses with the fork's
  `UPDATE cannot assign NULL to required column 'r'`.

## Why `id` and `flag` never refuse

**They never reach the seat.** No batch, plan or register event was printed for `id IN (4, 6)` or
`flag IN ('f4', 'f6')`, in any cell. The Spark door sends an `UPDATE … WHERE` to the identity path only when
`try_allowed_update_in` (an `IN (subquery)`) or `try_allowed_plain_update` accepts it
(`crates/repark-spark/src/spark_ast.rs:387-410`, executed at line 435), and the plain route takes only a binary operator:
`is_scalar_comparison` (`crates/repark-iceberg/src/write/predicate_dml/plain.rs:127-135`) matches `Expr::BinaryOp`,
nested or unary forms, nothing else. An `IN` list is `Expr::InList`, so the statement falls through to DataFusion's
planned `UPDATE` and the fork's `TableProvider::update` (`crates/integrations/datafusion/src/table/mod.rs:368` at
fork `92242cb4`). That path rebuilds every batch with the table's declared schema
(`physical_plan/delete.rs:753-780`, `RecordBatch::try_new(Arc::clone(table_schema), columns)`), so batch order cannot
matter. `id = 4` is a binary operator and takes the identity path, but it matches one file, so one batch. The same
rows written as `id = 4 OR id = 6` would take the identity path; the probe did not run that form.

## The native door

`repark.sql` is stock DataFusion with no router (`crates/repark-python/Cargo.toml:56` lists `repark-sql` as a non-edge),
so every native `UPDATE` takes the fork's `TableProvider::update`: all 93 native attempts printed no identity event,
and every non-control one stored the truth rows. The native door creates only the default kind; `USING iceberg
TBLPROPERTIES` refuses at the parser, so its merge-on-read and v3 cells could not be set up (the table lists them). The
router in `crates/repark-sql/src/router.rs:252-266` does reach the seat through `commit_identity_dml`
(`router.rs:292-301`), but no door ships it; it reaches the same `execute_predicate_dml`, so a fix at the seat covers
it as well.

## Inventory

**How I searched**, on main `dd77603a`, in `crates/` (all crates) and the fork checkout at `92242cb4`
(`crates/integrations/datafusion/src`, `crates/iceberg/src/arrow`):

- data-derived nullability: `grep -rnE "(array|column|col|values|arr|a|c)\.is_nullable\(\)"`, `null_count\(\)`,
  `logical_null_count`, `with_nullable\(`, each hit outside tests read;
- schemas taken from a batch: `batches\[0\]\.schema\(\)`, `batches\.first\(\)`, `batch\.schema\(\)`, `first_batch`;
- consumers that compare batches: `MemTable::try_new`, `MemorySourceConfig::try_new_exec`, `register_scratch_provider`,
  `register_streaming_target`, and the writer entry points (`write_new_data_files_from_stream_with`,
  `write_data_files_from_stream_with_concurrency`, `cast_one_batch_to_write_schema`);
- door routing: every caller of `execute_predicate_dml`, `try_allowed_*`, `plain_identity_needs_fork`.

| Path | Where the batch schema comes from | Consumer | Mixed nullability possible? | Order-sensitive? |
|---|---|---|---|---|
| Identity `UPDATE`, copy-on-write, v2 and v3 (Spark door; router) | `project_update_data_batch`, per batch, `array.is_nullable()` (`predicate_dml/lineage.rs:162-185`, line 176), called at `predicate_dml.rs:432` | `register_update_values_table`: `MemTable::try_new(batches[0].schema(), …)` (`predicate_dml/cow_commit.rs:97-98`) | **Yes**: measured | **Yes: the bug** |
| Identity `UPDATE`, merge-on-read, v2 (and v3 when MOR) | the same batches | the writer, one batch at a time, cast to the declared schema (`predicate_dml/mor_commit.rs:58-66`; `merge/session_staging.rs:43-54`; `merge/mod.rs:556-576`) | Yes: measured | No (0 of 30 refused) |
| Identity `UPDATE … WHERE col IN (subquery)` (`try_allowed_update_in`) | the same seat, `collect_identity_update_rows` | the same two consumers | Yes, when two files' rows differ in NULLs | COW: yes, by the same rule; not measured |
| Identity `DELETE`, copy-on-write and merge-on-read | no value batches: `(_file, _pos)` pairs into a declared two-field schema (`predicate_dml.rs:457-472`); the survivors are a SQL plan over the scan (`predicate_dml/cow_commit.rs:113-136`) | `MemTable` with a declared schema; writer | No | No |
| Non-identity `UPDATE` (fork `TableProvider::update`: `IN` lists, other non-binary predicates, the native door) | the declared table schema on every batch (fork `physical_plan/delete.rs:753-780`) | the fork's writer | No | No |
| `MERGE` copy-on-write | SQL plan streams (`merge/copy_on_write.rs:26-90`); match discovery only reads | the writer, per batch | No: a plan's stream schema is fixed | No |
| `MERGE` merge-on-read | declared write-schema nullability for data fields, the stream's field for the lineage pair (`merge/merge_on_read.rs:246-262`) | the writer, per batch | No | No |
| `cow_scratch` | one-column declared schema, one batch (`merge/cow_scratch.rs:99-115`); `register_streaming_target` wraps a declared scratch schema (`merge/mod.rs:455-461`) | `MemTable` / streaming provider | No | No |
| Writer sort for a declared sort order | the first batch's schema (`distribution.rs:209-220`; `merge/row_lineage.rs:196-201`) | `MemorySourceConfig` + `SortExec` | No: every batch was already cast to the declared write schema | No |
| `INSERT OVERWRITE` positional map | declared OR the batch's NULL count (`overwrite.rs:103-136`, line 121) | the writer, per batch | Only for a required field that receives a NULL | No: no `MemTable`; the writer refuses that NULL |
| `INSERT OVERWRITE` materialized fallback (Spark door) | `tighten_batch_nullability`: NULL counts over **all** batches (`repark-spark/src/insert_overwrite.rs:611-640`) | `MemTable::try_new` (`insert_overwrite.rs:562-564`) | No: one schema for every batch | No |

**Only one path combines a per-batch, data-derived schema with a consumer that compares batches**: the identity
copy-on-write `UPDATE`. Merge-on-read sees the same mixed batches and is order-free because its consumer is not.

## The seat I intend to use

`project_update_data_batch` (`crates/repark-iceberg/src/write/predicate_dml/lineage.rs:162-185`). Each field's
nullability becomes the declared value schema's (`values_schema`, built by `update_values_schema` from the table's
current schema, `lineage.rs:58-76`) OR'd with the stream's planned field (`batch.schema().field(index + 2)`), never the
array's data. Both inputs are fixed for the statement, so every batch gets the same schema by construction, and
`register_update_values_table` accepts any arrival order. One product file.

Why the planned field is in the OR: a required column assigned NULL (`SET r = NULL`) plans a nullable field. With the
declared value alone, `RecordBatch::try_new` inside `project_update_data_batch` would refuse first, with this
function's own internal wrapper text. With the OR, the batch reaches the writer, which refuses with the text main gives
in 6 of 9 attempts (`Column 'r' is declared as non-nullable but contains null values`), now on every attempt. For an
optional column the OR changes nothing: the declared value is already true.

The same batches feed merge-on-read; the change only makes their schemas uniform, and the writer casts to the declared
schema as before. No stored value changes: the arrays are untouched, only the `Field` nullability flag.

**For the critic:** whether the planned-field OR belongs in the fix, or the fix should take the declared value schema
alone and accept the required-NULL refusal moving into `project_update_data_batch`. The brief says "from the declared
schema"; I read the planned field as declared too (by the plan, not by one batch's data).
