# Unit ledger — FNP-9/10 · the collections and JSON function families

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when FNP-9/10 merges, or when the owner closes the slate row.

**Unit:** FNP-9/10 · **Date:** 2026-09-05 · **Executor:** Claude (Opus 5), Actor ·
**Branch:** `feat/fnp-9-collections-json` · **Base:** `282607f5`
**Model:** opus-5
**risk_tier:** standard.

Spark is the oracle. Live PySpark 4.1.2, zulu-17, `TZ=UTC`, ANSI on and off, 2026-09-05.
The campaign charter is [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md); the unit rows are
[docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md) §7 **FNP-9**
(collections, generators, dispatch — 8 names) and **FNP-10** (JSON — 6 names). Clauses discharged
from the campaign charter: C-001 (Rust-owned expression per name), C-002 (no Python row compute),
C-007/C-009 (nothing silently absent), C-010 (no raised ceiling).

## Round 2 — the critic's twelve wrong answers, and what each one cost

Round 1 was **FAIL**: three S1 and nine S2 wrong answers measured on live PySpark 4.1.2, every
one of them inside a clause this ledger had marked `PROVEN`. That is the finding behind the
finding — the gates were green, the mutation knobs were red, and the unit still shipped a
`from_json` whose FAILFAST mode could not see a bad *record*, a decimal decoder that truncated
where Spark rounds, and a `[*]` machine that was fitted to twenty measured cells and wrong on
the twenty-first. The lesson is recorded plainly: **a rule derived by fitting a model to the
cells you happened to measure is not the rule.** Round 2 re-derived the JSONPath evaluator from
Spark's own three-style state machine and re-measured 102 cells rather than 20.

| id | sev | disposition | evidence |
|---|---|---|---|
| F1 | S1 | **FIXED** — `decode.rs` carries a per-row bad-record flag through every container; `from_json.rs` raises under FAILFAST on a bad record, not only an unparsable document | `test_from_json_failfast_raises_on_a_bad_record_not_only_a_bad_document` (6 shapes: wrong-typed leaf, fractional int, bad array element, decimal overflow, nested struct field, root array) |
| F2 | S1 | **FIXED** — `decimal_units` scales the token TEXT, rounds HALF_UP at the declared scale, refuses a value wider than the precision, and accepts the string form | `test_from_json_decimal_rounds_half_up_and_nulls_on_overflow` (6 cells incl. `1.505`→`1.51`, `-1.505`→`-1.51`, bare `DECIMAL`→`4`, overflow→NULL, `"2.50"`→`2.50`) |
| F3 | S1 | **FIXED** — `path.rs` re-derived as Spark's Raw / New / Flatten style machine: a top-level wildcard unwraps a single match, an index-then-wildcard switches to always-array, `[*][*]` flattens | `test_get_json_object_wildcard_style_machine` (15 shapes) plus the 20 round-1 cells, all re-measured |
| F4 | S2 | **FIXED** — `_corrupt_record` fills from the same bad-record flag | `test_from_json_corrupt_record_takes_a_bad_record_too` (6 cells: leaf, array, decimal, binary, date, nested struct) |
| F5 | S2 | **FIXED** — an empty or whitespace document is a NULL row, never a corrupt record, and never raises under FAILFAST | `test_from_json_empty_document_is_a_null_row_not_a_corrupt_record` |
| F6 | S2 | **FIXED** — a wrong-shaped array, struct or map NULLs the whole field (partial results stay struct-field-only), and a root object wraps for an `ARRAY<STRUCT>` schema | `test_from_json_shape_mismatch_nulls_the_container` (7 cells) |
| F7 | S2 | **FIXED** — `object_field` takes the LAST match | `test_from_json_duplicate_key_takes_the_last` |
| F8 | S2 | **FIXED** — `build_text` routes a number through `json_number_text` | `test_from_json_string_target_spells_numbers_the_java_way` |
| F9 | S2 | **FIXED** — the reader accepts single-quoted strings and keys | `test_json_family_accepts_single_quoted_documents` (4 names) |
| F10 | S2 | **FIXED** — `schema_of_json` backtick-quotes a non-identifier name and doubles an embedded backtick; `ddl.rs` parses the escape back | `test_schema_of_json_quotes_a_non_identifier_field_name` (5 names, each round-tripped through `from_json`) |
| F11 | S2 | **FIXED** — `array_insert` widens element and value through the TIGHTEST common type and refuses when there is none | `test_array_insert_widens_the_element_and_value_types` |
| F12 | S2 | **FIXED** — the pin the registry cited now exists | `test_sequence_descending_answers_empty` |
| F13 | S3 | **ROWED** — §7 `FNP10-JAVA-DOUBLE-TEXT-1`; four JDK-legacy spellings, pinned | `test_to_json_double_text_diverges_on_the_jdk_legacy_spellings` |
| F14 | S3 | **FIXED** — `write_escaped` emits upper-case `\uXXXX` | covered by the `to_json` control-character cell in the round-2 verification sweep |
| F15 | S3 | **FIXED** — empty structs pruned, duplicate keys kept, `NaN` inferred DOUBLE, an empty document infers STRING | `test_schema_of_json_prunes_empty_structs_and_keeps_duplicates` (6 cells) |
| F16 | S3 | **FIXED** — a leading zero is malformed, a non-finite renders quoted, `-0` renders `0`, a null root renders `null`, and a non-STRING argument is refused | `test_get_json_object_number_and_argument_rules` |
| F17 | S3 | **FIXED except INTERVAL** — NaN/Infinity literals and strings decode, epoch-int and minute-precision timestamps parse, a raw TAB inside a string is malformed, `NOT NULL` / `COMMENT` modifiers are accepted, `_corrupt_record INT` and a scalar root schema and a non-STRING map key are refused by condition name. `INTERVAL DAY` is §7 `FNP10-FROM-JSON-DDL-1` | `test_from_json_non_finite_and_timestamp_forms`, `test_from_json_refuses_a_non_container_schema_and_a_typed_corrupt_column`, `test_from_json_refuses_an_interval_ddl_field` |
| F18 | S3 | **FIXED** — the vacuous decimal pin now asserts `Decimal("1.50")` and the parametrized bypass is gone; a Spark-spelling `map_concat` door pin was added | `test_from_json_leaf_and_container_cells`, `test_map_concat_answers_sparks_own_map_spelling_on_the_sql_door` |
| F19 | S4 | **TRUED UP** — the ceiling is stated once and measured; the mutation table's item count is corrected; the live leg asserts a condition name instead of accepting any exception | this section, the Gates table, and `test_parity_live.py` |
| F20 | S4 | **NOTED** — `F.array_insert(F.array(lit…), …)` and `F.arrays_zip(F.array(lit…), …)` answer nullable where Spark is non-nullable; the root cause is `F.array(lit…)`'s own nullability, which the SQL-door literals do not share. A NULLABILITY-2 class question over the facade's array constructor, not these kernels; not touched | measured by the round-1 critic and re-confirmed in round 2 |

**102 cells re-measured on live Spark 4.1.2 after the fixes; 101 agree.** The one that does not is
`sequence` on the SQL door, which repark does not resolve at all — the filed
`FNP9-SEQUENCE-1` divergence, now with the pin the registry always cited.

## C-001 — the roster, measured first

Every name below was measured on live PySpark 4.1.2 and on repark (release module,
`__debug_assertions__` False) **before** any code was written. `scratch/` holds the oracle run;
the cells are transcribed into "Oracle" below. The roster is the exact set this unit takes; a
name outside it is scope creep (slate Invariant V).

| Name | Today's cell (repark, 2026-09-05) | Shipped state |
|---|---|---|
| `get_json_object` | absent both doors (`Invalid function 'get_json_object'`; no facade attribute) | **BUILT** |
| `json_array_length` | absent both doors | **BUILT** |
| `json_object_keys` | absent both doors | **BUILT** |
| `to_json` | absent both doors | **BUILT** |
| `from_json` | absent both doors | **BUILT** |
| `schema_of_json` | SQL door absent; `F.schema_of_json` raises `UnsupportedOperationException … disclosed E1` | **BUILT** |
| `create_map` | no facade attribute; Spark's SQL spelling is `map(...)`, which the Spark door already serves | **BUILT** |
| `map_concat` | absent both doors | **BUILT** |
| `array_insert` | absent both doors | **BUILT** |
| `arrays_zip` | SQL door answers DataFusion's kernel with field names `1`,`2` and a nullable outer list; `F.arrays_zip` raises `… disclosed R-FN-BATCH2` | **BUILT**, one narrowed divergence (`FNP9-ARRAYS-ZIP-NAMES-1`) |
| `posexplode` | `UnsupportedOperationException: posexplode is not supported yet (no first-class unnest-with-ordinality…)`; SQL door `SELECT item with multiple aliases is not supported` | **NOT BUILT**, §7 `FNP9-GENERATORS-1` |
| `posexplode_outer` | same refusal | **NOT BUILT**, §7 `FNP9-GENERATORS-1` |
| `inline` | no facade attribute; SQL door `Invalid function 'inline'` | **NOT BUILT**, §7 `FNP9-GENERATORS-1`, absence pinned |
| `inline_outer` | no facade attribute; absent on the SQL door | **NOT BUILT**, §7 `FNP9-GENERATORS-1`, absence pinned |
| `stack` | no facade attribute; absent on the SQL door | **NOT BUILT**, §7 `FNP9-GENERATORS-1`, absence pinned |
| `json_tuple` | SQL door answers ONE `struct<c0,c1>` column where Spark projects TWO; `F.json_tuple` raises `… disclosed E1` | **NOT BUILT**, §7 `FNP9-GENERATORS-1` |
| `call_udf` | no facade attribute | **NOT BUILT**, §7 `FNP9-BYNAME-1`, absence pinned |
| `call_function` | no facade attribute | **NOT BUILT**, §7 `FNP9-BYNAME-1`, absence pinned |
| `sequence` | **corrected 2026-09-05:** `F.sequence` EXISTS and answers ascending ranges through `generate_series`; only the SQL door is absent | **NOT BUILT**, §7 `FNP9-SEQUENCE-1` records the descending and illegal-step divergence |

**Roster correction (2026-09-05, same day, before any code shipped for it).** The first charter
row for `sequence` read "absent both doors". That was measured on the SQL door only: `F.sequence`
is exported and answers `sequence(1, 5, 2)` → `[1, 3, 5]` today, and `docs/examples/functions/`
already teaches it. Chartering it as a refusal would have REGRESSED a working name — the first
build of `functions_json.py` did exactly that and the example measurement caught it. `sequence`
leaves the build set and gets a divergence row instead: descending (`sequence(5, 1)` → `[]` where
Spark counts down) and an illegal step (`[]` where Spark raises) are both wrong answers today,
and the fix belongs with FNP-11's DATE/TIMESTAMP ± INTERVAL arm so the name closes once.

Six names share **one** seam: Spark's `posexplode` / `posexplode_outer` / `inline` /
`inline_outer` / `stack` and the facade's `json_tuple` are **multi-column generators**. The facade
select path carries at most one generator column and emits exactly one output column
(`dataframe/core.py` `_generator`), and the Spark door refuses `SELECT gen(x) AS (a, b)` outright.
Building any one of them is the same plan-shape change, so this unit files the seam once and does
not ship a one-column impostor. `call_udf` / `call_function` share a second seam: the facade
`Column` is built without a session, so a by-name lookup cannot reach the session's function
registry (the same seam that makes `F.expr("a + 1")` refuse, §7 `EX-FN-4`).

**Why the five absent names stay absent rather than becoming loud named refusals.** A named
refusal has to be exported, and an exported `F.*` name that no example covers goes on
`docs/examples/backlog.txt`, whose count `scripts/check_example_coverage.py` ratchets DOWN only.
Five new backlog rows against a baseline that may not grow is a red gate, and an example that
demonstrates a refusal is not the EX house form (a refusing name stays on the backlog with a §7
row — that is exactly what EX-25 did for `posexplode`). So the seam is filed in §7 and the
absence is pinned instead: `test_fnp9_multi_column_and_by_name_names_stay_absent` reds the day
the seam lands and the names are exported, which is the red-when-fixed obligation. The unit that
builds the generators exports the names and writes their examples in the same change.

Three adjacent cells were measured and are **filed, not fixed**, each with its reason:

| Cell | Measured | Why not this unit |
|---|---|---|
| `element_at(array, oob)` under ANSI | Spark raises `INVALID_ARRAY_INDEX_IN_ELEMENT_AT`; repark answers NULL under ANSI on **and** off | `element_at_udf()` carries `try_element_at` as an **alias** — one kernel serves both names. Making `element_at` raise means splitting the alias, which is FNP-7a's delivered contract. The divergence is already documented on `functions_collections.element_at`. |
| `map_zip_with` nullability | values Spark-equal on the Column API (`a:1/10`, `b:2/N`, `c:N/30`); Spark's map is non-nullable, repark's nullable. The SQL-door lambda spelling does not parse, which is FNP-4b's deferred dialect, not a kernel gap | NULLABILITY-2 class over the whole higher-order family, not one name. |
| array-literal element type | `slice(array(1,2,3,4), -2, 2)` answers `[3,4]` on both, but repark's element type is `int64` where Spark's is `int32` | `SparkIntegerLiteral` (TYPES-1) narrows scalar `Int64` literals; it does not reach inside `array(…)`. A TYPES-1 residue over every array literal, not a collection-function defect. It is also why `array_insert` accepts a BIGINT position (§7 `FNP9-ARRAY-INSERT-BIGINT-1`). |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The roster above is the exact set this unit takes, each name measured on live PySpark 4.1.2 and on repark before any code was written; every BUILT name reaches `repark.spark.functions` and its `__all__`, and no size ceiling is raised to get it there. | The roster table, the oracle below, `test_every_built_name_is_exported_from_the_facade`, and `functions.py` held at its exact 1985 by `check_lib_py.py`. | **PROVEN** |
| C-002 | `get_json_object`, `json_array_length` and `json_object_keys` answer Spark 4.1.2 on both Spark-facade doors — value, Arrow type and nullability — including the number spelling, the string-leaf quoting rule, and the whole `[*]` collect rule. | `test_fnp_9_collections_json.py` (44 path-grammar and wildcard cells, plus the 15 round-2 style-machine cells) + `json/scalars.rs` tests.  Round 1 was refuted here (F3): the `[*]` rule had been fitted to 20 measured cells and was wrong on six more. Round 2 re-derived it from Spark's Raw/New/Flatten machine and re-measured 35 cells; F16's number and argument rules landed with it. | **PROVEN** |
| C-003 | `schema_of_json` answers Spark's DDL string, non-nullable, with fields sorted, a non-identifier name backtick-quoted, empty structs pruned, duplicate keys kept, the null/decimal/widening rules, an empty document inferring STRING, and a raise on a malformed one. | 14 inference cells on both doors, the round-2 quoting and pruning tests + `json/schema_of.rs` tests.  Narrowed at round 2: F10 and F15 were inside this clause, and repark's own output did not round-trip through its own DDL parser. | **PROVEN** |
| C-004 | `to_json` renders STRUCT / ARRAY / MAP Spark-equally: a NULL struct field is omitted, a NULL map value is written, NaN and Infinity are JSON strings, binary is base64, timestamps use the session zone, decimals keep their scale, and a double takes `Double.toString` **except the four JDK-legacy spellings §7 `FNP10-JAVA-DOUBLE-TEXT-1` names**. | `test_to_json_*` + `json/to_json.rs` tests + `test_to_json_double_text_diverges_on_the_jdk_legacy_spellings`.  Narrowed at round 2: the round-1 clause claimed `Double.toString` outright, and F13 measured four values where JDK 17 is not the shortest repr. The clause now states the exception and pins it. | **PROVEN** |
| C-005 | `from_json` parses a DDL or `DataType` schema PERMISSIVEly: a missing field, a JSON null, a wrong-shaped value and a malformed document are all NULL; a bad RECORD (not only a bad document) fills `_corrupt_record` and raises under FAILFAST; an empty document is a NULL row; a wrong-shaped container NULLs the field; a duplicate key is last-wins; DECIMAL rounds HALF_UP and NULLs on overflow. | 16 leaf/container cells, the round-2 FAILFAST / corrupt-record / empty / shape / duplicate / decimal tests + `json/from_json.rs` tests.  Round 1 was refuted here: F1, F2, F4, F5, F6, F7, F8 and F17 were all inside this clause. | **PROVEN** |
| C-006 | `create_map`, `map_concat`, `array_insert` and `arrays_zip` answer Spark on both doors, including `INVALID_INDEX_OF_ZERO`, `NULL_MAP_KEY`, `DUPLICATED_MAP_KEY`, `MAP_CONCAT_DIFF_TYPES`, the `-1`-appends rule, NULL padding at both ends, NULL fill to the longest array, and `array_insert` widening element and value through the tightest common type. | `test_create_map_*` / `test_map_concat_*` / `test_array_insert_*` / `test_arrays_zip_*` + the four collection kernels' Rust tests.  Narrowed at round 2: F11 measured `array_insert` truncating a DOUBLE into an INT array; the widening rule and its refusal are now pinned, and F18's Spark-spelling `map_concat` door pin was added. | **PROVEN** |
| C-007 | No name this unit did not build silently half-answers: each is absent or refuses, carries a §7 row naming its seam, and carries a pin that reds when the seam closes. | `test_fnp9_multi_column_and_by_name_names_stay_absent`, `test_json_tuple_still_refuses_on_the_facade`, §7 `FNP9-GENERATORS-1` / `FNP9-BYNAME-1`. | **PROVEN** |
| C-008 | Every divergence this unit measured is filed as a §7 row with a pin, and no row claims parity it does not have. | §7 `FNP9-ARRAYS-ZIP-NAMES-1`, `FNP9-SEQUENCE-1`, `FNP10-JSON-OPTIONS-1`, `FNP10-JSON-SCHEMA-COLUMN-1`, `FNP9-ARRAY-INSERT-BIGINT-1`; `EX-FN-1` retired, `EX-FN-16` narrowed, `EX-FN-2` superseded in place. | **PROVEN** |
| C-009 | Every new pin is invertible and the gates are green on real exit codes. | The mutation table below; `make ci`, `make verify`, the facade and parity suites, the dbt suite, and the live tier under `REPARK_PARITY_LIVE=1`.  Round-1 caveat recorded: ten green knobs did not stop twelve wrong answers shipping, because a knob only inverts the cells a pin already has. The round-2 answer is more measured cells (102, not 20), not more knobs. | **PROVEN** |

## Red-first

The pin file is red on the base for every clause it carries, and the redness was **measured**
before any code was written, not assumed: on `282607f5` the facade has no `array_insert`,
`create_map`, `from_json`, `get_json_object`, `json_array_length`, `json_object_keys`,
`map_concat` or `to_json` attribute at all, and `F.arrays_zip` / `F.schema_of_json` raise
`UnsupportedOperationException`. The Spark door answers `Invalid function` for
`array_insert`, `create_map`, `from_json`, `get_json_object`, `json_array_length`,
`json_object_keys`, `map_concat`, `schema_of_json` and `to_json`. Every test in
`test_fnp_9_collections_json.py` touches at least one of those names, so the whole file is red on
the base; the two absence pins (`test_fnp9_multi_column_and_by_name_names_stay_absent`) are the
deliberate exception — they are GREEN on the base and red when the generator seam closes, which is
what a red-when-fixed pin is for.

## Oracle (live PySpark 4.1.2, 2026-09-05, zulu-17, `TZ=UTC`, `local[2]`, ANSI on and off)

Recorded verbatim from `spark.sql(...).toArrow()` — value AND Arrow type AND nullability. ANSI off
is quoted only where it differs. Spark `void` is Arrow `null`.

### JSON

| Cell | Spark answer |
|---|---|
| `from_json('{"a":1}', 'a INT')` | `struct<a:int32>` nullable, `{'a': 1}` |
| `from_json('{"a":{"b":[1,2]}}', 'a STRUCT<b: ARRAY<INT>>')` | `struct<a: struct<b: list<element: int32>>>`, `{'a': {'b': [1, 2]}}` |
| `from_json('{bad', 'a INT')` | `{'a': None}` — PERMISSIVE is the default; no raise |
| `from_json('{"z":1}', 'a INT')` | `{'a': None}` (missing field) |
| `from_json('{"a":"x"}', 'a INT')` | `{'a': None}` (type mismatch, PERMISSIVE) |
| `from_json('[{"a":1},{"a":2}]', 'ARRAY<STRUCT<a: INT>>')` | `list<element: struct<a: int32>>`, `[{'a': 1}, {'a': 2}]` |
| `from_json('{"a":1,"b":2}', 'MAP<STRING,INT>')` | `map<string, int32>`, `[['a', 1], ['b', 2]]` |
| `from_json('{bad', 'a INT', map('mode','PERMISSIVE'))` | `{'a': None}` |
| `from_json('{bad', 'a INT', map('mode','FAILFAST'))` | raises (`SparkException` through `awaitResult`) — ANSI on and off |
| `from_json(NULL, 'a INT')` | NULL |
| `to_json(struct(1 AS a, 'x' AS b))` | `string` nullable, `{"a":1,"b":"x"}` |
| `to_json(struct(NULL AS a, 'x' AS b))` | `{"b":"x"}` — a NULL field is **omitted**, not written as `null` |
| `to_json(map('a', 1))` | `{"a":1}` |
| `to_json(array(struct(1 AS a)))` | `[{"a":1}]` |
| `to_json(struct(TIMESTAMP'2021-01-02 03:04:05' AS t, DATE'2021-01-02' AS d))` | `{"t":"2021-01-02T03:04:05.000Z","d":"2021-01-02"}` |
| `to_json(CAST(NULL AS STRUCT<a:INT>))` | NULL |
| `to_json(struct(array(1,2) AS a, map('k','v') AS m, struct(1 AS z) AS s))` | `{"a":[1,2],"m":{"k":"v"},"s":{"z":1}}` |
| `to_json(struct(CAST(1.50 AS DECIMAL(5,2)) AS d, 1.5E0 AS f, true AS b))` | `{"d":1.50,"f":1.5,"b":true}` — decimal keeps its scale |
| `get_json_object('{"a":1}', '$.a')` | `string` nullable, `'1'` |
| `get_json_object('{"a":{"b":2}}', '$.a.b')` | `'2'` |
| `get_json_object('{"a":[1,2,3]}', '$.a[1]')` | `'2'` |
| `get_json_object('{"a":[{"b":1},{"b":2}]}', '$.a[*].b')` | `'[1,2]'` |
| `get_json_object('{"a":{"b":2}}', '$.a')` | `'{"b":2}'` — an object is re-serialized |
| `get_json_object('{"a":1}', '$.z')` | NULL |
| `get_json_object('{"a":1}', 'a')` | NULL — a path not starting `$` is NULL, not an error |
| `get_json_object('{"a":1}', '$')` | `'{"a":1}'` |
| `get_json_object('{bad', '$.a')` | NULL |
| `get_json_object('{"a":"hi"}', '$.a')` | `'hi'` — a string leaf is **unquoted** |
| `json_array_length('[1,2,3]')` | `int32` nullable, `3` |
| `json_array_length('[]')` | `0` |
| `json_array_length('{"a":1}')` | NULL (not an array) |
| `json_array_length('[1,')` | NULL |
| `json_object_keys('{"a":1,"b":2}')` | `list<element: string>` nullable, `['a', 'b']` — insertion order |
| `json_object_keys('{}')` | `[]` |
| `json_object_keys('[1,2]')` | NULL |
| `json_object_keys('{bad')` | NULL |
| `schema_of_json('{"a":1,"b":"x"}')` | `string` **non-nullable**, `'STRUCT<a: BIGINT, b: STRING>'` |
| `schema_of_json('[1,2]')` | `'ARRAY<BIGINT>'` |
| `schema_of_json('{"a":{"b":[1.5]}}')` | `'STRUCT<a: STRUCT<b: ARRAY<DOUBLE>>>'` |
| `json_tuple('{"a":1,"b":2}', 'a', 'b')` | **two** columns `c0`,`c1`, both `string` nullable, `'1'` / `'2'` |
| `json_tuple('{"a":1}', 'z')` | one column `c0`, NULL |
| `json_tuple('{"a":{"b":1}}', 'a')` | `'{"b":1}'` |

### Collections

| Cell | Spark answer |
|---|---|
| `arrays_zip(array(1,2), array('a','b'))` | `array<struct<0:int,1:string>>` **non-nullable**, element `struct not null`; `[{0:1,1:'a'},{0:2,1:'b'}]` |
| `arrays_zip(array(1,2,3), array('a'))` | `[{0:1,1:'a'},{0:2,1:None},{0:3,1:None}]` — NULL fill to the longest |
| `arrays_zip(a, b)` over named columns | field names are the **child names**: `struct<a:int,b:string>` |
| `arrays_zip(array(1), CAST(NULL AS ARRAY<STRING>))` | NULL (outer nullable) |
| `arrays_zip(array(), array())` | `array<struct<0:void,1:void>>`, `[]` |
| `arrays_zip(array(1,2))` | `array<struct<0:int>>` |
| `map_concat(map('a',1), map('b',2))` | `map<string,int32>` non-nullable, `[['a',1],['b',2]]` |
| `map_concat(map('a',1), map('a',2))` | raises `[DUPLICATED_MAP_KEY]` (ANSI on and off) |
| `map_concat(map('a',1), CAST(NULL AS MAP<STRING,INT>))` | NULL |
| `map_concat()` | `map<string,string>` non-nullable, `[]` |
| `array_insert(array(1,2), 1, 9)` | `array<int>` non-nullable, element nullable; `[9,1,2]` |
| `array_insert(array(1,2), 5, 9)` | `[1,2,None,None,9]` — NULL padding past the end |
| `array_insert(array(1,2,3), -1, 9)` | `[1,2,3,9]` — `-1` inserts AFTER the last element |
| `array_insert(array(1,2), 0, 9)` | raises `[INVALID_INDEX_OF_ZERO]` (ANSI on and off) |
| `array_insert(CAST(NULL AS ARRAY<INT>), 1, 9)` | NULL |
| `create_map` | not a Spark SQL routine (`UNRESOLVED_ROUTINE`); PySpark's `F.create_map` is the `map(...)` expression |
| `sequence(1, 5, 2)` / `(1,3)` / `(5,1)` | `[1,3,5]` / `[1,2,3]` / `[5,4,3,2,1]`, element **non-nullable** |
| `sequence(1, 5, -1)` | raises `IllegalArgumentException: Illegal sequence boundaries` |
| `sequence(DATE'2021-01-01', DATE'2021-01-05', INTERVAL 2 DAY)` | `array<date>`, three dates |
| `posexplode(array(10,20))` | two columns `p` `int not null`, `c` `int not null`; rows `(0,10)`, `(1,20)` |
| `posexplode(map('a',1))` | **three** columns `p`, `k`, `v` |
| `posexplode_outer(CAST(NULL AS ARRAY<INT>))` | one row `(NULL, NULL)`, both columns nullable |
| `inline(array(struct(1 AS a, 'x' AS b)))` | two columns `a`,`b` named from the struct fields |
| `inline(array(struct(1 AS a,'x' AS b), CAST(NULL AS STRUCT<a:INT,b:STRING>)))` | rows `(1,'x')`, `(NULL,NULL)`; both columns nullable |
| `inline_outer(CAST(NULL AS ARRAY<STRUCT<a:INT,b:STRING>>))` | one row `(NULL, NULL)` |
| `stack(2, 1, 2, 3, 4)` | two columns `col0`,`col1` nullable; rows `(1,2)`, `(3,4)` |
| `stack(2, 1, 2, 3)` | rows `(1,2)`, `(3,NULL)` — the short row is NULL-filled |
| `element_at(array(1,2), 5)` | ANSI on **raises** `[INVALID_ARRAY_INDEX_IN_ELEMENT_AT]`; ANSI off answers NULL |
| `element_at(array(1,2), 0)` | raises `[INVALID_INDEX_OF_ZERO]` under ANSI on **and** off |
| `slice(array(1,2,3), 0, 1)` | raises `[INVALID_PARAMETER_VALUE.START]` |
| `slice(array(1,2,3), 1, -1)` | raises `[INVALID_PARAMETER_VALUE.LENGTH]` |
| `flatten(array())` | raises `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]` — `array()` is not `ARRAY<ARRAY<T>>` |
| `flatten(array(array(1), CAST(NULL AS ARRAY<INT>)))` | NULL — a NULL inner array nulls the row |
| `flatten(array(array(1, NULL), array(3)))` | `[1, None, 3]` — a NULL *element* is kept |

## Kernels

| Name | Layer |
|---|---|
| `get_json_object` | `json/scalars.rs::SparkGetJsonObject` over `json/path.rs`. Steps: `$`, `.name`, `['name']`, `[index]`, `[*]`. A wildcard collects the remaining steps' results: none is NULL, exactly one at the top level is that result bare, otherwise a JSON array. A wildcard whose next step is a subscript flattens instead and always wraps — Spark's Hive-inherited double-wildcard rule, derived from nineteen measured cells. `["name"]`, `$..name` and `.*` do not parse, which is Spark's NULL rather than an error. |
| `json_array_length` / `json_object_keys` | `json/scalars.rs`. Nullable; a malformed document or a wrong-shaped value is NULL. Duplicate object keys are both kept, as Spark keeps them. |
| `schema_of_json` | `json/schema_of.rs`. Its own `Inferred` lattice, not Arrow's: Spark's inference sorts struct fields, merges a lone JSON null to STRING but lets a null beside a typed sibling vanish, and infers `DECIMAL(digits,0)` for an integer wider than `i64`. Non-nullable result; a malformed document raises. |
| `to_json` | `json/to_json.rs`. Recursive over Arrow. The asymmetry that matters: a NULL **struct field** is omitted, a NULL **map value** is written as `null`. Java number spellings come from `reader.rs`; binary is base64 through a local encoder; timestamps render in the session zone read from `ConfigOptions`. The facade refuses a non-empty option mapping — the same rule `from_json` and `schema_of_json` follow, because `ignoreNullFields` and `timestampFormat` change the answer and repark implements neither. |
| `from_json` | `json/from_json.rs` + `json/ddl.rs` + `json/decode.rs`. The result type comes from the foldable schema argument in `return_field_from_args`; `decode.rs` builds each Arrow column in one pass rather than row-by-row `ScalarValue`s. |
| JSON reader | `json/reader.rs`. Hand-written, and better than a generic one here: Spark keeps an integer token's text verbatim and re-renders anything with `.`/`e` through `Double.toString`, which a `serde_json::Value` round-trip destroys. It is also what keeps `Cargo.lock` untouched — `serde_json` is a workspace dependency `repark-functions` does not declare, and declaring it rewrites the lock, which this unit's brief closes. |
| `create_map` | `collection/create_map.rs`. NOT in `collection::functions()`: Spark's SQL spelling is `map(...)`, which the Spark door already serves, so the kernel reaches only the facade through `expr_fn::create_map`. Its own kernel and not DataFusion's `map(make_array, make_array)` lowering, which answers `map requires key and value lists to have the same length` when a scalar key meets a column value — measured. |
| `map_concat` | `collection/map_concat.rs`. Key and value types widen through `comparison_coercion`, so a `map<string,int32>` column joins a `map<string,int64>` literal the way Spark's does. |
| `array_insert` | `collection/array_insert.rs`. One `RowPlan` per row, then one `concat`. |
| `arrays_zip` | `collection/arrays_zip.rs`. Struct field names come from the RETURN field, not from `arg_fields`, so the array the kernel builds always matches the type the analyzer declared. |
| dispatch | `expr_fn.rs` builders + `repark-python/src/column/function_dispatch/dispatch_json.rs`, which the main arm table falls through to (that file was at 992 of its 1000-line ceiling, and the `column/dispatch/` split the campaign charter names is FNP-Z's, which the slate forbids doing piecemeal here). |
| facade | `functions_json.py` (JSON + the installer), `functions_collections.py` (`create_map`, `map_concat`, `array_insert`), `functions_expr.py` (`arrays_zip`, `schema_of_json`). |

## Design decisions taken inside the unit

| Decision | Reason |
|---|---|
| A hand-written JSON reader, not `serde_json` | The brief closes dependency and lockfile changes. `serde_json` is a workspace dependency this crate does not declare, and declaring it rewrites `Cargo.lock`. The reader is also the better fit — see the Kernels table. The design's "serde_json is already a workspace dep" note reads as an option, not a requirement. |
| `from_json` honours `mode` and `columnNameOfCorruptRecord` and REFUSES every other option; `to_json` and `schema_of_json` refuse a non-empty option mapping outright | The brief names JSON option coverage as the HALT candidate. It does not need a ruling: Spark honours around twenty options and silently ignores an unknown key, so ignoring an option repark has not implemented would change the answer silently — in a JSON parse, where a silent wrong answer is hardest to notice. Refusing is loud, recorded (§7 `FNP10-JSON-OPTIONS-1`), and reversible one option at a time. The same rule had to reach `to_json` and `schema_of_json`: the first draft ignored their options, which was the very silent divergence the `from_json` rule exists to prevent. No HALT. |
| `arrays_zip` field names are positional | Measured, twice. See §7 `FNP9-ARRAYS-ZIP-NAMES-1`: a UDF's return field must be a pure function of the argument TYPES, and both name-carrying designs failed DataFusion's own schema-stability invariant. |
| `array_insert` accepts a BIGINT position | Refusing `Int64` refused the ordinary SQL spelling `array_insert(ai, 1, 9)`, because repark's integer literals are `Int64` until `SparkIntegerLiteral` narrows them — after UDF coercion. §7 `FNP9-ARRAY-INSERT-BIGINT-1`. |
| The five unbuilt names stay absent rather than becoming exported refusals | An exported `F.*` name with no example joins `docs/examples/backlog.txt`, whose count ratchets DOWN only; five new rows is a red gate, and a refusal example is not the EX house form. The absence is pinned instead, so the pin still reds when the seam closes. |

## Mutation

**Round 1.** Ten knobs, each applied to the shipped source, measured, and reverted. The Python
column is `python/repark/tests/test_fnp_9_collections_json.py` **at 93 items when the run was
made** — the two option pins landed after it, taking the file to 94 (F19 corrected the round-1
claim of 93 against a 94-item file); the Rust column is
`cargo test -p repark-functions -- json:: collection::` at 42 items. **10 of 10 knobs red on both
suites** — and, as the round-1 critic proved, that was not enough: a knob only inverts a cell some
pin already asserts, so ten red knobs sat happily on top of twelve wrong answers.

| Knob | Python red of 93 | Rust red of 42 |
|---|---|---|
| `get_json_object`: a single wildcard result never goes bare | 5 | 1 |
| `to_json`: a NULL struct field is written instead of omitted | 1 | 1 |
| `schema_of_json`: struct fields keep document order | 2 | 1 |
| `from_json`: a fractional token decodes into an integer target | 1 | 1 |
| `array_insert`: a negative position is not counted from the end | 4 | 1 |
| `map_concat`: the duplicate-key guard is disabled | 1 | 1 |
| `arrays_zip`: zips to the SHORTEST array instead of the longest | 1 | 1 |
| `create_map`: the null-key guard is disabled | 1 | 1 |
| `java_double_text`: always the plain decimal branch, never `d.dddEn` | 1 | 1 |
| `from_json`: an unknown option is ignored the way Spark ignores it | 1 | 1 |

The `arrays_zip` row is the one the round-1 mutation run earned its keep on. Measured first, it
was **0 red of 42** on the Rust suite: `arrays_zip.rs` had tests for the field names and for
schema stability but none that read a padded NULL back. The test
(`zip_pads_the_short_argument_to_the_longest`) exists because the knob found its absence, not the
other way round.

**Round 2.** Fifteen knobs, one per fix, measured against
`test_fnp_9_collections_json.py` at 127 items.

| Knob | What it breaks | Red of 127 |
|---|---|---|
| `R2-K1` | from_json: FAILFAST and the corrupt column see only an unparsable document | 2 |
| `R2-K2` | decimal truncates instead of rounding HALF_UP | 1 |
| `R2-K3` | decimal skips the precision check | 2 |
| `R2-K4` | the top-level wildcard never unwraps a single match | 8 |
| `R2-K5` | an index before a wildcard does not switch style | 2 |
| `R2-K6` | object_field takes the first key instead of the last | 1 |
| `R2-K7` | a wrong-shaped struct keeps its partial fields | 1 |
| `R2-K8` | an empty document is a corrupt record instead of a NULL row | 1 |
| `R2-K9` | build_text writes the raw number token | 1 |
| `R2-K10` | the reader rejects single-quoted strings | 1 |
| `R2-K11` | schema_of_json never quotes a field name | 1 |
| `R2-K12` | schema_of_json keeps an empty struct field | 1 |
| `R2-K13` | array_insert casts the value to the element type | 1 |
| `R2-K14` | a leading zero is accepted as a number | 1 |
| `R2-K15` | get_json_object accepts a non-STRING argument | 1 |

**15 of 15 knobs red.**

`R2-K10` is worth its own sentence, because the first version of it was **0 red of 127** and that
was the knob's fault, not the pins'. It flipped the single-quote arm in `read_value`, which routes
a VALUE — but the document the pin uses, `{'a':1}`, has its single quotes around a KEY, and keys
go through `read_text` directly. The mutation did not break the behaviour the pin asserts, so a
green run said nothing. Re-cut against `read_text`'s own quote check it reds. A knob that passes
is either a missing pin or a knob that does not bite; telling the two apart is the work.

The distinction that matters between the two runs: round 1's knobs asked *"does some pin notice
if I break this?"* and every answer was yes. They could not ask *"is the thing I am protecting
right?"* — only Spark can answer that, and only for a cell someone thought to measure. Round 2's
102-cell re-measurement is the check that actually failed in round 1, and it is now a standing
one: the live leg re-derives sixteen of those cells from Spark on every `REPARK_PARITY_LIVE=1`
run.

## Gates (real exit codes, round 2, 2026-09-06)

| Command | Result |
|---|---|
| `make ci` | 0 |
| `make verify` | 0 (48 Rust test binaries, all ok) |
| `make check-python-conventions` | 0 |
| `make rust-panic-ban` | 0 |
| `make check-example-coverage` | 0 |
| `make check-map-sync` / `check-ledger-grammar` / `check-ledgers` / `check-docs-compaction` | 0 / 0 / 0 / 0 |
| `check_docstring_presence.py` | 0 |
| `.venv/bin/python -m pytest python/repark/tests -q -x` | 5289 passed, 234 skipped |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 624 passed |
| `VIRTUAL_ENV=$PWD/.venv make py-test-dbt` | 0 — 59 passed, 1 skipped |
| `REPARK_PARITY_LIVE=1` on `test_parity_live_fnp9.py`, `test_parity_live.py`, `test_fnp_9_collections_json.py` | 244 passed |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
| both example scripts run | 0 |

Round 2 moved one file for a ceiling, not for a rewrite: `test_parity_live.py` reached 1024 of its
1000-line default when the FNP-9/10 leg grew, so the leg moved to
`test_parity_live_fnp9.py`, which shares `conftest.py`'s session-scoped `spark_engine` and
therefore still co-collects and co-runs with `test_live_disclosure_still_diverges` in one JVM.
No ceiling was raised.

Three pins outside this unit's file moved in round 1 because the surface they described moved,
and each is named here rather than left to a reviewer's diff:
`test_fn_batch2.py::test_batch2_loud_unsupported` loses its `arrays_zip` refusal arm (no §7 row
named `arrays_zip` under `R-FN-BATCH2` — the disclosure lived in the refusal string, now gone);
`test_functions_split_identity.py::test_functions_all_matches_pre_split_inventory` gains the
FNP-9/10 block and its count (the installer chain appends `FNP9_NAMES` LAST, so the pre-split,
declared, higher-order and try blocks keep their positions); and
`test_ex_0_example_coverage.py` moves its enumerated-surface count 913 → 921.

## Disk (AGENTS.md "Resource discipline")

Checked before the first build: **862 GB free of 1.8 TB** (51% used). The lane reuses the shared
`target/` and the shared cargo registry; nothing new was downloaded. `.ivy2` (182 MB) is a copy of
`~/.ivy2.5.2` kept for the live legs and is git-excluded, as are `scratch/` and `handback.json`.
No worktree was created and no other lane's artifacts were touched.

## Delivery

| Item | Path |
|---|---|
| Kernels | `crates/repark-functions/src/json/` (7 files) and `crates/repark-functions/src/collection/{array_insert,arrays_zip,create_map,map_concat}.rs` |
| Dispatch | `crates/repark-functions/src/expr_fn.rs`, `crates/repark-python/src/column/function_dispatch/dispatch_json.rs` |
| Facade | `python/repark/src/repark/spark/functions_json.py`, `functions_collections.py`, `functions_expr.py`, and the three unchanged-length lines at the foot of `functions.py` |
| Pins | `python/repark/tests/test_fnp_9_collections_json.py` (**127 items** after round 2) + the kernels' own Rust tests (43 items) |
| Live leg | `test_parity_live.py::test_live_fnp9_collections_json` — 28 answer cells and 7 raising cells after round 2 — co-collected beside `test_live_disclosure_still_diverges` |
| Registry | §7 `FNP9-ARRAYS-ZIP-NAMES-1` (retires `EX-FN-1`), `FNP9-GENERATORS-1` (supersedes `EX-FN-2`), `FNP9-BYNAME-1`, `FNP9-SEQUENCE-1`, `FNP10-JSON-OPTIONS-1`, `FNP10-JSON-SCHEMA-COLUMN-1`, `FNP9-ARRAY-INSERT-BIGINT-1`, and the round-2 pair `FNP10-JAVA-DOUBLE-TEXT-1` and `FNP10-FROM-JSON-DDL-1`; `EX-FN-16` narrowed to `schema_of_csv` |
| Examples | `docs/examples/functions/json_family.py`, `docs/examples/functions/map_build.py`; `BACKLOG_BASELINE` 164 → 163 as `F.schema_of_json` leaves the backlog |
| Ceilings | one number, measured after the round-2 merge: `check_lib_py.py` `functions_expr.py` **2259 → 2255** (ratchets DOWN), mirrored in `test_cap_1_source_file_line_cap.py`; `functions.py` held at its exact baseline and `column/mod.rs` at 1053, which is why the new facade surface is a new module and the new dispatch arms are a child module. Round 1 stated 2256 in Delivery and 2257 in AT-8 — F19 |
| Maps | lockstep on every touched directory |

## Out of scope, observed

- `SELECT array_prepend(array(1,2), 0)` answers `array_prepend does not support type Int64` on
  the Spark door — DataFusion's own kernel against an `Int64` array literal. Pre-existing, not
  touched.
- repark's SQL parser cannot spell `CAST(NULL AS MAP<STRING,INT>)`, `array()`, `map()` or
  `struct()` with no arguments; Spark parses all four. Pre-existing; the map cells are pinned
  through `createDataFrame` instead.
- `SELECT map('a', v) FROM t` (a scalar key with a column value) answers `map requires key and
  value lists to have the same length` on the Spark door — the same DataFusion limit `create_map`
  routes around on the facade. The SQL door is unchanged by this unit.
- A `Row` from repark iterates its FIELD NAMES where PySpark's iterates its values
  (`tuple(row["parsed"])`). Noticed while writing `json_family.py`; a DataFrame-surface question,
  not a function one.
- `from_json` supports only STRING map keys. **Corrected round 2:** Spark refuses a non-STRING
  map key too, with `[DATATYPE_MISMATCH.INVALID_JSON_MAP_KEY_TYPE]`; repark now raises the same
  condition name, so this is parity, not a divergence. The round-1 note that Spark "works there"
  was wrong and is left here with its correction rather than deleted.
- **F20, noted not fixed.** `F.array_insert(F.array(F.lit(1), F.lit(2)), 1, …)` and
  `F.arrays_zip(F.array(F.lit(1)), …)` answer a NULLABLE array where Spark answers non-nullable.
  The SQL-door literals agree with Spark, so the root cause is the facade's own
  `F.array(lit …)` nullability, not these kernels — a NULLABILITY-2 class question over the
  array constructor. Measured by the round-1 critic, re-confirmed in round 2, not touched: fixing
  the constructor's nullability moves every array-returning facade name at once and belongs to
  the unit that owns that class.

## Docstrings replaced (forced, nothing stripped silently)

The no-comment rule leaves exactly one one-line docstring where the presence gate demands it, and
a pre-existing docstring is never reworded. Four were REPLACED rather than left, each because the
old text would now be false, and each is named here so no reviewer has to find it in a diff:

| Where | Old text | Why it could not stand |
|---|---|---|
| `functions_expr.py::arrays_zip` | *"Unsupported because the engine has no `arrays_zip` function."* | The engine has one now. |
| `functions_expr.py::schema_of_json` | *"Infer JSON schema as DDL (PySpark `functions.schema_of_json`). E1 type pre-check only."* | It is no longer a type pre-check; the name answers. |
| `test_examples_functions_a.py::test_arrays_zip_refuses` → `…_names_its_fields_by_position` | *"arrays_zip refuses; Spark zips element-wise with NULL fill (EX-FN-1)."* | The name answers; only the field names diverge, so the EX-FN-1 pin flips into FNP9-ARRAYS-ZIP-NAMES-1. |
| `test_examples_functions_a.py::test_schema_of_pair_refuses` → `test_schema_of_csv_refuses` | *"schema_of_csv and schema_of_json refuse; Spark infers the structs (EX-FN-16)."* | `schema_of_json` left that row. |

Everything else pre-existing is byte-identical, including
`test_functions_split_identity.py`'s *"Pre-split 360 names stay the prefix; FNP-15/16 then FNP-4c
then FNP-7 append."* and `test_ex_0_example_coverage.py`'s docstring — the FNP-9/10 facts they
carry live in `python/repark/tests/map.md` and `python/repark-parity/tests/map.md` instead. Every
`pins:` citation this unit adds lives in a directory `map.md` row, never in Python.

## Known cost, recorded not fixed

`create_map`, `array_insert` and `arrays_zip` each build their output with one small Arrow
allocation per row (`take` of a single index, or a per-row slice) and one `concat` at the end.
That is O(rows) allocations where one `arrow::compute::interleave` over a pre-built index vector
would be O(1) of them. The shape is correct and the constant is the whole cost; measuring and
changing it is a PERF unit's work, not a correctness unit's, and this unit's charter is Spark
equality. Named here so the next reader does not have to rediscover it.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fnp-9-collections-json
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every cell was recorded from live PySpark 4.1.2 BEFORE the kernel was written; twelve oracle rounds, and three rounds were run only because a hand-written expectation disagreed with the kernel and the kernel turned out to be right.
      artifacts: [task/ledgers/staging/fnp-9-collections-json-ledger.md, python/repark/tests/test_fnp_9_collections_json.py]
    - id: AT-2
      status: ATTACKED
      evidence: Controls cover empty containers, NULL rows, malformed documents, duplicate keys, non-ASCII text, escapes, wide integers, NaN/Infinity, and both ends of every index rule.
      artifacts: [python/repark/tests/test_fnp_9_collections_json.py]
    - id: AT-3
      status: ATTACKED
      evidence: The raising paths are pinned by condition name on both doors - INVALID_INDEX_OF_ZERO, NULL_MAP_KEY, DUPLICATED_MAP_KEY, MAP_CONCAT_DIFF_TYPES, PARSE_MODE_UNSUPPORTED, MALFORMED_RECORD_IN_PARSING. ANSI on and off were measured for every cell; only element_at differs between them, and that is a filed row this unit did not touch.
      artifacts: [crates/repark-functions/src/collection/map_concat.rs, crates/repark-functions/src/json/from_json.rs]
    - id: AT-4
      status: N/A
      justification: Scalar kernels with no shared mutable state; the JSON reader borrows its input and owns nothing across rows.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, dependency or lockfile change. The JSON reader is hand-written precisely so Cargo.lock stays untouched, and it bounds its own recursion at MAX_DEPTH so a hostile document cannot blow the stack.
      artifacts: [crates/repark-functions/src/json/reader.rs, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: Every facade wrapper is one _scalar call onto one kernel; no SQL text construction, no composition, no branch on argument type selecting a different engine expression. create_map's even-argument check is an argument-count guard, not an expression choice.
      artifacts: [python/repark/src/repark/spark/functions_json.py, python/repark/src/repark/spark/functions_collections.py]
    - id: AT-7
      status: ATTACKED
      evidence: The always-run pins are repark-only; Spark is behind REPARK_PARITY_LIVE=1 and the live leg co-collects with the standing disclosure leg.
      artifacts: [python/repark/tests/test_parity_live.py]
    - id: AT-8
      status: ATTACKED
      evidence: No ceiling was raised. functions_expr.py ratchets 2259 to 2255 (one number, stated once, measured after the round-2 merge); functions.py and column/mod.rs stay at their exact baselines, which is why the new facade surface lands in a new module and the new dispatch arms land in a child module.
      artifacts: [scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: N/A
      justification: No new log, metric, or tracing surface.
    - id: AT-10
      status: ATTACKED
      evidence: Every PROVEN clause is cited from a test or a map.md; seven registry rows carry their pins; the retired and narrowed EX-25 rows carry their flipped pins.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/map.md]
```
