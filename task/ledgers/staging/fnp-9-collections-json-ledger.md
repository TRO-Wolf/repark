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
| C-002 | `get_json_object`, `json_array_length` and `json_object_keys` answer Spark 4.1.2 on both Spark-facade doors — value, Arrow type and nullability — including the number spelling, the string-leaf quoting rule, and the whole `[*]` collect rule. | `test_fnp_9_collections_json.py` (44 path-grammar and wildcard cells) + `json/scalars.rs` tests. | **PROVEN** |
| C-003 | `schema_of_json` answers Spark's DDL string, non-nullable, with fields sorted, the null/decimal/widening rules, and a raise on a malformed document. | 14 inference cells on both doors + `json/schema_of.rs` tests. | **PROVEN** |
| C-004 | `to_json` renders STRUCT / ARRAY / MAP Spark-equally: a NULL struct field is omitted, a NULL map value is written, doubles take `Double.toString`, NaN and Infinity are JSON strings, binary is base64, timestamps use the session zone, decimals keep their scale. | `test_to_json_*` + `json/to_json.rs` tests. | **PROVEN** |
| C-005 | `from_json` parses a DDL or `DataType` schema PERMISSIVEly: a missing field, a JSON null, a wrong-shaped value and a malformed document are all NULL; `_corrupt_record` takes the raw text; FAILFAST raises; DROPMALFORMED refuses as Spark's does. | 16 leaf/container cells, the corrupt-record and option tests + `json/from_json.rs` tests. | **PROVEN** |
| C-006 | `create_map`, `map_concat`, `array_insert` and `arrays_zip` answer Spark on both doors, including `INVALID_INDEX_OF_ZERO`, `NULL_MAP_KEY`, `DUPLICATED_MAP_KEY`, `MAP_CONCAT_DIFF_TYPES`, the `-1`-appends rule, NULL padding at both ends, and NULL fill to the longest array. | `test_create_map_*` / `test_map_concat_*` / `test_array_insert_*` / `test_arrays_zip_*` + the four collection kernels' Rust tests. | **PROVEN** |
| C-007 | No name this unit did not build silently half-answers: each is absent or refuses, carries a §7 row naming its seam, and carries a pin that reds when the seam closes. | `test_fnp9_multi_column_and_by_name_names_stay_absent`, `test_json_tuple_still_refuses_on_the_facade`, §7 `FNP9-GENERATORS-1` / `FNP9-BYNAME-1`. | **PROVEN** |
| C-008 | Every divergence this unit measured is filed as a §7 row with a pin, and no row claims parity it does not have. | §7 `FNP9-ARRAYS-ZIP-NAMES-1`, `FNP9-SEQUENCE-1`, `FNP10-JSON-OPTIONS-1`, `FNP10-JSON-SCHEMA-COLUMN-1`, `FNP9-ARRAY-INSERT-BIGINT-1`; `EX-FN-1` retired, `EX-FN-16` narrowed, `EX-FN-2` superseded in place. | **PROVEN** |
| C-009 | Every new pin is invertible and the gates are green on real exit codes. | The mutation table below; `make ci`, `make verify`, the facade and parity suites, the dbt suite, and the live tier under `REPARK_PARITY_LIVE=1`. | **PROVEN** |

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

Ten knobs, each applied to the shipped source, measured, and reverted. The Python column is
`python/repark/tests/test_fnp_9_collections_json.py` at 93 items (the two option pins came after
the run and are counted in the Rust column only); the Rust column is
`cargo test -p repark-functions -- json:: collection::` at 42 items. **10 of 10 knobs red on both
suites.**

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

The `arrays_zip` row is the one the mutation run earned its keep on. Measured first, it was
**0 red of 42** on the Rust suite: `arrays_zip.rs` had tests for the field names and for schema
stability but none that read a padded NULL back. The test
(`zip_pads_the_short_argument_to_the_longest`) exists because the knob found its absence, not the
other way round.

## Gates (real exit codes, 2026-09-06)

| Command | Result |
|---|---|
| `make ci` | 0 |
| `make verify` | 0 (48 Rust test binaries, all ok) |
| `make check-python-conventions` | 0 |
| `make rust-panic-ban` | 0 |
| `make check-example-coverage` (and `--require-execute`) | 0 — 921 public names, 756 covered, 163 backlog, 204 examples |
| `.venv/bin/python -m pytest python/repark/tests -q -x` | 5155 passed, 227 skipped |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 574 passed |
| `VIRTUAL_ENV=$PWD/.venv make py-test-dbt` | 59 passed, 1 skipped |
| `REPARK_PARITY_LIVE=1 … test_parity_live.py test_fnp_9_collections_json.py` | 214 passed |
| `make check-map-sync` / `check-ledger-grammar` / `check-ledgers` / `check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |

Three pins outside this unit's file moved because the surface they described moved, and each is
named here rather than left to a reviewer's diff:
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
| Pins | `python/repark/tests/test_fnp_9_collections_json.py` (93 items) + the kernels' own Rust tests |
| Live leg | `test_parity_live.py::test_live_fnp9_collections_json`, co-collected beside `test_live_disclosure_still_diverges` |
| Registry | §7 `FNP9-ARRAYS-ZIP-NAMES-1` (retires `EX-FN-1`), `FNP9-GENERATORS-1` (supersedes `EX-FN-2`), `FNP9-BYNAME-1`, `FNP9-SEQUENCE-1`, `FNP10-JSON-OPTIONS-1`, `FNP10-JSON-SCHEMA-COLUMN-1`, `FNP9-ARRAY-INSERT-BIGINT-1`; `EX-FN-16` narrowed to `schema_of_csv` |
| Examples | `docs/examples/functions/json_family.py`, `docs/examples/functions/map_build.py`; `BACKLOG_BASELINE` 164 → 163 as `F.schema_of_json` leaves the backlog |
| Ceilings | `check_lib_py.py` `functions_expr.py` 2259 → 2256 (ratchets DOWN), mirrored in `test_cap_1_source_file_line_cap.py`; `functions.py` held at 1985 and `column/mod.rs` at 1053, which is why the new facade surface is a new module and the new dispatch arms are a child module |
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
- `from_json` supports only STRING map keys and refuses any other key type loudly. Spark parses
  a JSON object's keys into the declared key type, so `MAP<INT, STRING>` works there. A loud
  refusal on a rare shape, recorded rather than built.

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
      evidence: No ceiling was raised. functions_expr.py ratchets 2259 to 2257; functions.py and column/mod.rs stay at their exact baselines, which is why the new facade surface lands in a new module and the new dispatch arms land in a child module.
      artifacts: [scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: N/A
      justification: No new log, metric, or tracing surface.
    - id: AT-10
      status: ATTACKED
      evidence: Every PROVEN clause is cited from a test or a map.md; seven registry rows carry their pins; the retired and narrowed EX-25 rows carry their flipped pins.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/map.md]
```
