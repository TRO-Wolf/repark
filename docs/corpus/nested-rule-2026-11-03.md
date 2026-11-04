# Nested types: the reference rule and the measured cells (2026-11-03)

CORPUS-3, family 3. The rule for reading nested Iceberg columns by field id across schema
evolution, and Spark's semantics for the expressions that reach into them, ported from the
upstream sources the [inventory](nested-inventory-2026-11-03.md) pins: Apache Iceberg tag
`apache-iceberg-1.11.0` (commit `6976e020`) and Apache Spark tag `v4.1.2` (commit `f0bb2e6`).
Every line number below is in those checkouts. The cells that measure the rule are
[nested-cells-2026-11-03.jsonl](nested-cells-2026-11-03.jsonl), 2973 rows in the AUD-015 table
schema, measured on RePark `d2f3313e` against Spark 4.1.2 with the Iceberg 1.11.0 runtime jar. The
unit measured and fixed nothing; each defect below is a cells row group and a card candidate.

## R1. Nested types and field ids

`format/spec.md`, Nested Types, lines 177 to 183: a struct is a tuple of named fields, each with an
id unique in the table schema; a list's element and a map's key and value each carry their own id.
Map keys are required; map values may be optional. Any field type may nest, including a struct as a
map key.

## R2. Column projection by field id

`format/spec.md`, Column Projection, lines 336 to 347: data file columns are selected by field id,
never by name or position, because names and order change after a file is written. A field id
absent from a data file resolves, in order, to the identity-partition value, the name-mapping
column, the field's `initial-default`, and otherwise `null` (lines 340 to 345). The worked example
at line 347 reads a renamed column under its new name and an added column as null.

The per-format suites hold the same rule at every nesting level
(`core/src/test/java/org/apache/iceberg/avro/TestReadProjection.java`, with the Parquet and ORC
subclasses under `data/` and `spark/v4.1/spark/` named in the inventory): full and reordered
projection (line 48, line 88), rename read by id (line 182), children of structs inside structs,
map values and list elements selected by id (lines 205, 332, 476), map and list projection
(lines 276, 444), and a struct projected with no selected children reads as an empty struct, null
only where the source struct is null (lines 565 to 629).

## R3. Schema evolution of nested fields

`format/spec.md`, Schema Evolution, lines 288 to 334. Any struct, nested or top level, may add,
delete, rename and reorder fields and promote a primitive along the table at lines 296 to 302
(line 322). An added field takes a new id, so a dropped field re-added under its old name is a new
column and reads null in older files. A rename keeps the id. Grouping fields into a nested struct,
or flattening one, is not allowed (line 324). Defaults: lines 262 to 275 and 326 to 334; a field
added without a default reads null in older files.

Iceberg Java refuses any change inside a map key: `core/src/main/java/org/apache/iceberg/SchemaUpdate.java`
lines 722 and 724 raise `Cannot add fields to map keys` and `Cannot alter map keys`. Spark's
`ALTER TABLE` surfaces both through the Iceberg catalog, so a drop, rename, promotion or re-add of
a map-key struct field is a refusal on the oracle.

## R4. Spark's access semantics for the expression side

From the golden inputs `sql/core/src/test/resources/sql-tests/inputs/struct.sql` (lines 14 to 23,
field access and star over a struct), `map.sql` (lines 4 to 17, `element_at` and
`map_contains_key` over literal maps) and `array.sql` (lines 95 to 107, `element_at` and index
access), and from star expansion in
`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/unresolved.scala` line 481
(`expand`). Measured on the oracle with ANSI on, the Spark 4.1.2 default:

- `s.f` on a struct resolves the field case-insensitively; a missing field raises
  `FIELD_NOT_FOUND`, a duplicated name raises `AMBIGUOUS_REFERENCE_TO_FIELDS`. A field of a null
  parent is null.
- `s.*` expands a struct into its fields, also over a null struct (one null per field).
- `a[i]` is zero-based; an index outside the array raises `INVALID_ARRAY_INDEX`. `element_at(a, i)`
  is one-based, negative from the end, `0` raises `INVALID_INDEX_OF_ZERO`, past the end raises
  `INVALID_ARRAY_INDEX_IN_ELEMENT_AT`; `try_element_at` answers null.
- `m[k]` and `element_at(m, k)` answer the value or null for a missing key; a field access after
  either (`m['k'].f`) resolves. `map(...)` with a repeated key raises `DUPLICATED_MAP_KEY`, a null
  key raises `NULL_MAP_KEY`; `map()` is an empty map. `struct(1, 'x')` names its fields `col1`,
  `col2`.

## The cells

**Fixture kind A, constructed tables.** Spark creates each table on a Hadoop warehouse as
`(id INT, <column>)` with `write.format.default` set to Parquet, Avro or ORC, writes three rows (a
full value, a null parent, null children), applies one evolution step as `ALTER TABLE`, writes
three rows built from the evolved schema, and reads back. Six column shapes, the step applied to
the shape's struct:

| Type | Column | Step target |
|---|---|---|
| `struct` | `s STRUCT<a INT, b STRING>` | `s` |
| `map` | `m MAP<STRING, STRUCT<a INT, b STRING>>` | `m.value` |
| `list` | `l ARRAY<INT>` | the column (`l.element` for promotion) |
| `mapkey` | `mk MAP<STRUCT<a INT, b STRING>, INT>` | `mk.key` |
| `structlist` | `ls ARRAY<STRUCT<a INT, b STRING>>` | `ls.element` |
| `empty` | `e STRUCT<>` | `e` |

Eight steps: `none`; `add` (`c INT`); `drop` (`b`); `drop_last` (`b`, then `a`: the struct's last
child; for `list`, `l` then `id`); `rename` (`a` to `a2`); `readd` (drop `a`, add `a` with a new
id); `promote` (`a` INT to BIGINT); `move` (`b FIRST`). A step Spark refuses is recorded and the
table keeps its schema. Eight access operations per table: `star` (`SELECT *`), `col` (`id` and
the column), `acc_a` and `acc_b` (the child through the shape's accessor: `s.a`, `m['k1'].a`,
`l[0]`, `map_keys(mk)[0].a`, `ls[0].a`), `acc_new` (the field the step touched), `alt` (struct
star, `element_at`, `map_keys` and `map_values`, `size`), `null_parent` (the touched field where
the column is null) and `filter` (a predicate on child `b`). Each side reads through the path:
`read.format("iceberg").load(<location>)` registered as temp view `v`, then `sql`.

The second writer axis, Parquet only: RePark creates the same 48 tables, writes the same rows,
applies the same DDL, and Spark reads RePark's table through the same eight operations. The second
write uses Spark's evolved schema when RePark's DDL outcomes matched Spark's, else RePark's own.

**Fixture kind B, expression cells.** 56 literal statements over `named_struct`, `struct`, `map`,
`array`, `element_at`, subscripts, `map_keys`, star over a struct and null parents, no table, on
both RePark doors.

**Binding of the AUD-015 fields.** `cat` is `A-read`, `A-ddl` (RePark's writer DDL and DML
against Spark's), `A-write` (Spark reading RePark's table) or `B-expr`. `spelling` is the fixture
id `<type>_<format>_<step>` for kind A and the statement key for kind B. `view` is the door:
`facade-path-view`, `native-path-view` (`_native.read_iceberg_path` registered with
`create_or_replace_temp_view` on `PyReparkSession.native()`), `facade-sql-writer`,
`spark-path-view-over-repark-write`, `facade-sql`, `native-sql`. `head` is the access operation,
the write step, or the expression family. `spark_changed` and `repark_changed` are the metadata
files a read added or removed (none did), or `metadata` for a DDL that succeeded. Answers are
`cols [names] rows [...]` with rows normalised by the frame's schema and sorted; error texts carry
the Spark error class block whole, with the warehouse path written `<wh>`.

**Classes and severity.** The AUD-015 classes. Severity by the AUD-015 legend: S1 a DDL that drops
data where Spark refuses, or a value Spark reads wrong from RePark's write when RePark's writes
matched Spark's; S2 a wrong read, an answer where Spark refuses, or a state change Spark does not
make; S3 a refusal where Spark answers, a refusal text without Spark's error class, or an answer
whose values match but whose column names differ.

## Summary

**Panics: none.** No cell on either door, in reads, writes or expressions, raised a panic.

Cells per axis value:

| Axis | Values (cells) |
|---|---|
| Fixture kind | A-read 2304, A-ddl 173, A-write 384, B-expr 112 |
| Door | facade-path-view 1152, native-path-view 1152, facade-sql-writer 173, spark-path-view-over-repark-write 384, facade-sql 56, native-sql 56 |
| Type | struct 481, map 481, list 481, mapkey 481, structlist 481, empty 456 |
| Format | Parquet 1325, Avro 768, ORC 768 |
| Evolution step | none 352, add 357, drop 357, drop_last 362, rename 357, readd 362, promote 357, move 357 |
| Access operation | 336 each of star, col, acc_a, acc_b, acc_new, alt, null_parent, filter |
| Expression family | named_struct 42, map 42, array 28 |

Class counts:

| Class | A-read | A-ddl | A-write | B-expr | All |
|---|---|---|---|---|---|
| EQUAL | 346 | 152 | 217 | 7 | 722 |
| EQUAL_REFUSE | 36 | 0 | 65 | 5 | 106 |
| REFUSE_TEXT | 622 | 6 | 42 | 14 | 684 |
| REPARK_REFUSES_SPARK_ACTS | 1092 | 10 | 40 | 39 | 1181 |
| REPARK_ACTS_SPARK_REFUSES_NOOP | 4 | 0 | 2 | 5 | 11 |
| REPARK_ACTS_SPARK_REFUSES_CHANGED | 0 | 5 | 0 | 0 | 5 |
| ANSWER_DIFFERS | 204 | 0 | 18 | 42 | 264 |

Severity: S1 4, S2 104, S3 2037, none 828.

What reads equal: the full-row reads (`star`, `col`) of `struct`, `map`, `list` and `structlist`
on Parquet and Avro match Spark on every step but `drop_last` (rows, or the same refusal once the
list column is dropped), on both doors and on the writer axis: 280 of 280 cells. ORC nested reads refuse throughout (D3). The accessor operations on `map`, `mapkey`
and `structlist` are masked by D2, so their reader evidence is the full-row reads alone.

Defect groups (every non-equal cell falls in exactly one):

| D | Defect | Cells | Severity | Card |
|---|---|---|---|---|
| D1 | `ALTER TABLE` drops or renames a map-key struct field where Spark refuses `Cannot alter map keys` (R3); after the drop of both key fields the column reads null | 28 (5 DDL, 23 Spark reads of RePark's table) | S1 4, S2 13, S3 11 | F-MAP-KEY-DROP-ALTER-1 (the rename arm is new) |
| D2 | A field access after a map subscript, `element_at`, an array subscript or `map_keys(...)[0]` fails to parse: `Dot access not supported for non-string expr` | 822 | S3 | SUBSCRIPT-DOT-ACCESS-1 |
| D3 | ORC data-file reads of a nested column refuse (`only top-level primitive`) | 323 | S3 | ICE-ORC-AVRO-1 ledger row R-1 (open) |
| D4 | Star over a struct (`s.*`, also over a null struct and `s.b.*`) refuses `Invalid qualifier` | 102 | S3 | candidate |
| D5 | Native door: an array subscript is one-based; `l[0]` reads null and `l[1]` reads the first element; a filter on `l[0]` keeps no rows | 42 | S2 | candidate |
| D6 | Native door: `element_at` over a map answers a one-element list (`[13]`, `[null]`), not the value | 18 | S2 | candidate |
| D7 | Collecting a map with struct keys fails (`unhashable type: 'dict'`), and a struct with a duplicated field name fails to convert; nested struct values come back as `dict`, not `Row` (196 read answers) | 66 | S3 | candidate |
| D8 | The empty struct is not accepted: `STRUCT<>` in `CREATE TABLE`, `struct()` and `named_struct()` fail to parse; the writer axis has no table to read | 78 (12 DDL, 64 downstream, 2 expressions) | S3 | candidate |
| D9 | A zero-field read on Avro or ORC refuses: an empty struct on Avro (`StructArray::try_new_with_length`), a zero-column batch on Avro and ORC | 40 | S3 | candidate (fork; the Parquet arm was fixed under F-STRUCT-EMPTY-CHILD-PANIC-1) |
| D10 | Facade: an out-of-range array subscript or `element_at` answers null where Spark raises `INVALID_ARRAY_INDEX` and `INVALID_ARRAY_INDEX_IN_ELEMENT_AT` under ANSI | 4 | S2 | candidate |
| D11 | Routines not resolved: `array`, `size`, `try_element_at` and zero-argument `map()` on the native door, `map_contains_key` on both doors | 38 | S3 | candidate |
| D12 | Oracle divergences, not RePark defects: Spark's reader fails (`... is not in the store`) projecting a map-value struct that has no columns in a file, and collapses to null an empty struct and a struct whose projected children are all absent; RePark's answers follow R2 | 33 | S2 16, S3 17 (mechanical) | none; kept divergence, as ICE-NESTED-DDL-1 records Spark's null collapse |
| D13 | Literal struct semantics: `struct(1, 'x')` names fields `c0`, `c1` (Spark `col1`, `col2`); case-insensitive field lookup refuses; a duplicated field answers where Spark raises `AMBIGUOUS_REFERENCE_TO_FIELDS`; `m[NULL]` raises an Arrow error | 6 | S2 3, S3 3 | candidate |
| D14 | Values match, column names differ: native `v.s[a]` for `s.a`; argument rendering such as `Int64(1)` and `make_array(...)` in function-call names | 172 | S3 | candidate |
| D15 | Refusal text without Spark's error class: `Field b not found in struct` for `FIELD_NOT_FOUND`, `Schema error: No field named` for `UNRESOLVED_COLUMN` | 364 | S3 | candidate |
| D16 | Moving a map-key field: Spark's `ALTER` succeeds and leaves the key struct unchanged; RePark reorders it, and a Spark-shaped insert then refuses on a cast | 9 | S2 8, S3 1 | candidate (beside D1) |

Hours per fixture kind, from this lane's timestamps (lane start 14:21 EST): kind A 0.3 h to build
and probe the harness, 128 s of Spark seeding (144 tables), 25 s of RePark reads on both doors,
5 s of RePark writes and 33 s of Spark cross-reads; kind B 0.05 h to write the 56 statements and
25 s to run them on three doors; the write-up 0.3 h. The family took under one lane hour against
the estimate of 3 to 6.

Rows of the label draft's family-3 table these cells support:

- "No panic on the struct shapes AUD-008 and AUD-016 named": supported and widened to six nested
  shapes, three formats and eight evolution steps on both doors, the writer axis and 56
  expressions, with no panic in 2973 cells.
- "id-less struct reads by position": not measured here (Spark writes field ids); its evidence
  stays the ICE-IDLESS pins.
- A candidate row the cells support: full-row path reads of struct, map, list and list-of-struct
  columns on Parquet and Avro match Spark through add, drop, rename, re-add, promotion and move;
  ORC nested reads, the map-key ALTER (D1, D16) and the empty struct (D8, D9) are the exclusions.

## Dated updates

- 2026-11-04, D1 (the ALTER and union arms, 28 cells; the table row above is unchanged): fixed by MAP-KEY-ALTER-1 (PR #1256; ledger [map-key-alter-1-ledger.md](../../task/ledgers/completed/map-key-alter-1-ledger.md)). The DDL and union refusals now match Spark's text with the field ids masked. The fork-side drop stays open on [F-MAP-KEY-DROP-ALTER-1](../../task/roadmap/mid-term/f-map-key-drop-alter-1-card-2026-10-31.md).
- 2026-11-04, D16 (the move, 9 cells; the table row above is unchanged): fixed by MAP-KEY-ALTER-1 (PR #1256). A move of a direct key child commits nothing and leaves the key struct unchanged, as Spark's ALTER does. Spark commits a metadata file for the unapplied move and RePark commits none.
- 2026-11-04, D2 (the Spark-door arm, 822 cells; the table row above is unchanged): fixed on the Spark door by SUBSCRIPT-DOT-ACCESS-1 (PR #1259; ledger [subscript-dot-access-1-ledger.md](../../task/ledgers/completed/subscript-dot-access-1-ledger.md)). The group now reads 40 EQUAL, 152 to 153 value-equal with other column names (D14), 108 refusals with other text (D15), 100 ORC (D3), 2 D12 and 6 to 7 other; 413 native rows are unchanged, and the native door keeps refusing (lead L1, card [subscript-dot-access-native-1-card-2026-11-04.md](../../task/roadmap/mid-term/subscript-dot-access-native-1-card-2026-11-04.md)).
- 2026-11-04, D13 (a field access after a subscript or a call; verifier findings F1 and F2): the Spark door's respelling reaches DataFusion's exact-name field access, so a duplicated field name, or two names that differ only in case, answers where Spark raises `AMBIGUOUS_REFERENCE_TO_FIELDS`, and a differently cased name refuses where Spark's case-insensitive lookup answers (lead L7 of the [SUBSCRIPT-DOT-ACCESS-1 ledger](../../task/ledgers/completed/subscript-dot-access-1-ledger.md); no card file: this row's card column stays `candidate`).
