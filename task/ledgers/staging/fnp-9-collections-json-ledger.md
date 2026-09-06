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
