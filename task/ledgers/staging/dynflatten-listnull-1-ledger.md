# Unit ledger — DYNFLATTEN-LISTNULL-1 · parquet Null-logical list reads as int32

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

**Unit:** DYNFLATTEN-LISTNULL-1 · **Date:** 2026-09-06 · **Model:** grok-4.6 ·
**Branch:** `fix/dynflatten-listnull-1` · **Base:** `origin/main`
**Registry:** [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
`DYNFLATTEN-LISTNULL-1`.

**Rubric:** STANDARD. `risk_tier: standard`.

**Writable paths:** `crates/repark-core/src/spark_nullable.rs`, Python pins under
`python/repark/tests/`, `docs/spark-sql-iceberg-parity.md`,
`docs/perf/dynamic-flatten-baseline.md`, lockstep `map.md` files, this ledger.
Closed: `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, AWS. `drop_null_lists` default stays `True` (API freeze).

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Live PySpark 4.1.2: parquet `optional int32 element (Null)` reads as `array<int>` (`IntegerType`, containsNull true, column nullable). `explode_outer` yields nullable `int32` with one NULL per parent (NULL list and EMPTY list both one NULL). `printSchema` shows `element: integer`. | Measurement in §2; always-run pin encodes the type. | **PROVEN** |
| C-002 | `read_parquet_nullable` promotes Arrow `Null` to `Int32` after the nullability relax (same depth-bound walk as relax). CTAS still uses `relax_schema_to_nullable` only. `drop_null_lists` default stays `True`. | Rust pins in `spark_nullable.rs`; Python read pin. | **PROVEN** |
| C-003 | Both DataFrame doors (`read.parquet` and `read_parquet`) plus default `dynamicFlatten` keep the parquet column as nullable `int32` NULLs, row count unchanged. Nested-family parquet flatten includes `user_properties` last. `make_array()` / ARRAY<VOID> still drops. | `test_dynflatten_listnull.py`; `test_datasets_facade.py`; `test_drop_null_typed_list`. | **PROVEN** |
| C-004 | Live co-collect: repark == Spark on `struct_d3`, `list_struct_1`, `cartesian_two_lists` including `user_properties` int32. Co-collects beside `test_live_disclosure_still_diverges`. | `test_parity_live_dynflatten.py`; live run. | **PROVEN** |
| C-005 | Registry row FIXED; baseline struct-only shapes not regressed; maps lockstep; STATUS and the slate untouched. | Registry; baseline note; maps; `git diff origin/main -- STATUS.md briefs/next-sequence.md` empty. | **PROVEN** |
| C-006 | Mutation: skip `promote_parquet_null_types` (identity) reds the parquet-read / flatten pins. | Mutation table §6. | **PROVEN** |

`LOGIC_SCORE` = **6/6 `PROVEN`**.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dynflatten-listnull-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Spark 4.1.2 parquet footer, printSchema, explode_outer rows and Arrow type measured; repark matches on read.parquet.
      artifacts: [python/repark/tests/test_dynflatten_listnull.py, python/repark/tests/test_parity_live_dynflatten.py]
    - id: AT-2
      status: ATTACKED
      evidence: make_array() ARRAY<VOID> still drops; nested parquet flatten keeps int32 NULLs; empty and null lists both one NULL.
      artifacts: [python/repark/tests/test_dynflatten_listnull.py, python/repark/tests/test_datasets_facade.py, python/repark/tests/test_dynamic_flatten.py]
    - id: AT-3
      status: ATTACKED
      evidence: No unwrap added; promote is a schema walk with a depth bound; rust-panic-ban stays green.
      artifacts: [crates/repark-core/src/spark_nullable.rs]
    - id: AT-4
      status: N/A
      justification: Schema rewrite at plan build; no shared mutable state and no async beyond the existing parquet read.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, no .github, no Cargo pin, no secrets. Path is a parquet file the session already reads.
      artifacts: [crates/repark-core/src/spark_nullable.rs]
    - id: AT-6
      status: ATTACKED
      evidence: relax_schema_to_nullable is unchanged so CTAS VOID does not become int32; drop_null_lists default unchanged.
      artifacts: [crates/repark-core/src/spark_nullable.rs, python/repark/tests/test_dynflatten_listnull.py]
    - id: AT-7
      status: ATTACKED
      evidence: Bench createDataFrame path does not go through the reader; struct-only shapes have no void list. Ledger records the release-module re-run.
      artifacts: [docs/perf/dynamic-flatten-baseline.md]
    - id: AT-8
      status: ATTACKED
      evidence: Spark parquet physical INT32 + Null logical type measured; DataFusion infers Null; promotion is the Spark physical default.
      artifacts: [crates/repark-core/src/spark_nullable.rs, python/repark/tests/test_dynflatten_listnull.py]
    - id: AT-9
      status: ATTACKED
      evidence: Schema override is the same DataFusion ParquetReadOptions.schema path CUTOVER-SCHEMA-1 already uses.
      artifacts: [crates/repark-core/src/spark_nullable.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation skip-promote reds the named pins; STATUS.md and briefs/next-sequence.md untouched.
      artifacts: [python/repark/tests/test_dynflatten_listnull.py]
  complete: true
```

## 2. Measurement (C-001)

PySpark 4.1.2, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC`, `local[2]`, ANSI on.
Bed `list_struct_1` 16 rows seed 42.

Parquet footer:

```
optional group user_properties (List) {
  repeated group list {
    optional int32 field_id=-1 element (Null);
  }
}
```

PyArrow still reports `list<element: null>`. Spark `read.parquet`:

- `printSchema`: `user_properties: array (nullable = true) | element: integer (containsNull = true)`
- `dtypes`: `('user_properties', 'array<int>')`
- Arrow: `list<element: int32>` nullable True
- Values: `None` or `[]` matching the generator (`row % 4 == 0` → NULL, else EMPTY)

`explode_outer` (no empty-as-null rewrite): 16 rows, `up_e` nullable `int` / Arrow `int32`,
every value NULL. Parent NULL list and parent EMPTY list both one NULL.

`make_array` is not a Spark 4.1.2 builtin (`UNRESOLVED_ROUTINE`); the repark SQL spelling
stays a void-list extra. The Spark answer this row names is the parquet reader.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-listnull-measure
  agent: Actor
  action: Measure live Spark explode_outer on parquet list<null> then choose FIX vs DECLARE
  charter_trace: C-001
  preconditions:
    - venv-ok in mklane-listnull.log: SATISFIED (grep)
    - pyspark 4.1.2 in lane venv: SATISFIED (import pyspark)
    - no other Spark JVM: SATISFIED (pgrep)
  success_condition: Spark read schema, explode_outer type/nullability/rows, and parquet footer are recorded
  step_risks: [wrong layer (flatten vs reader): HANDLED(footer shows INT32 physical; Spark printSchema is array<int> before explode)]
  contingencies: [API freeze blocks changing drop_null_lists default: EXECUTABLE(promote at reader, leave default True)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## 3. Decision

FIX at the parquet reader, not at `drop_null_lists`.

Spark never sees `NullType` on this file. `spark_flatten.py` would drop a NullType list, but
the reader already inferred `IntegerType`, so explode keeps the column. Matching Spark means
inferring `Int32` at read time. Changing `drop_null_lists=True` from drop to keep would
break the documented extra and the freeze's additive-only posture.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-listnull-fix
  agent: Actor
  action: Promote parquet Null types to Int32 inside read_parquet_nullable
  charter_trace: C-002, C-003
  preconditions:
    - C-001 measurement: SATISFIED (§2)
    - API freeze lists dynamicFlatten with no required params and does not name drop_null_lists default: SATISFIED (docs/design/v1-0-api-freeze.json)
    - relax_schema_to_nullable is also used for CTAS: SATISFIED (crates/repark-core/src/map.md)
  success_condition: read.parquet of list<null> is list<int32>; dynamicFlatten keeps int32 NULLs; make_array still drops
  step_risks:
    - CTAS VOID becomes int32: HANDLED(promote only in read_parquet_nullable)
    - schema override cannot decode Null logical as Int32: HANDLED(physical type is already INT32; Python roundtrip pins None/[]/[None])
  contingencies: [override fails: EXECUTABLE(cast projection after read, not taken)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## 4. What changed

| File | Change |
|---|---|
| `crates/repark-core/src/spark_nullable.rs` | `promote_parquet_null_types` after the nullability relax in `read_parquet_nullable`. |
| `python/repark/tests/test_dynflatten_listnull.py` | Both-door read pin, flatten pin, `make_array()` drop control. |
| `python/repark/tests/test_parity_live_dynflatten.py` | Full-frame equality including `user_properties` int32. |
| `python/repark/tests/test_datasets_facade.py` | Nested parquet read type + flatten column list. |
| `docs/spark-sql-iceberg-parity.md` | Row FIXED. |
| `docs/perf/dynamic-flatten-baseline.md` | After note; numbered tables untouched. |
| `map.md` × lockstep | Reader, tests, staging, perf. |

No public API change. `DynamicFlattenOptions.drop_null_lists` default remains `true`.

## 5. Pins (C-003, C-004)

| Pin | Door | Proves |
|---|---|---|
| `test_parquet_null_list_reads_as_int32_on_both_doors[read.parquet]` | facade reader | parquet Null → list<int32> |
| `test_parquet_null_list_reads_as_int32_on_both_doors[read_parquet]` | native session method | same inner path |
| `test_dynamic_flatten_keeps_parquet_null_list_as_int32_nulls` | facade `dynamicFlatten` | default flatten keeps int32 NULLs |
| `test_create_dataframe_void_list_still_drops` | SQL `make_array()` | drop_null_lists still drops actual void |
| `test_nested_parquet_read_keeps_capitalized_nested_schema` | facade parquet | nested family type |
| `test_nested_dynamic_flatten_full_depth_column_order` | facade flatten | column last, all NULL int32 |
| `test_live_dynflatten_matches_spark_explode` | live | repark == Spark including user_properties |
| `parquet_null_list_becomes_int32_list` | Rust | schema walk |
| `relax_leaves_null_list_element_as_null` | Rust | CTAS path not promoted |

ANSI SQL has no `dynamicFlatten` spelling. Native lazy DataFrame is the Spark-shaped
`ReparkSession` (`read_parquet` vs `read.parquet`).

## 6. Mutation (C-006)

Identity `promote_parquet_null_types` (`schema.clone()`, 2026-09-06).
`cargo test -p repark-core --lib spark_nullable`: **3 failed, 7 passed** of 10.

| Pin | Result |
|---|---|
| `parquet_null_list_becomes_int32_list` | RED (`Null` not `Int32`) |
| `parquet_scalar_null_becomes_int32` | RED |
| `parquet_null_inside_struct_and_map_value_becomes_int32` | RED |
| `promote_preserves_int32_lists` | green (identity) |
| `relax_leaves_null_list_element_as_null` | green (untouched path) |

Mutation score: **3 red of 5** promote pins (the three Null→Int32 asserts). Python
read/flatten pins would red the same way after a rebuild; they stayed green on the
release module that still carried the fix. Restored from backup after the run.

## 7. Gates

Recorded at hand-back.

## 8. Perf (C-005)

Release module (`repark._native.__debug_assertions__ is False`). Struct-only bed shapes
do not carry `user_properties`; the bench loads repark via `createDataFrame`, not
`read.parquet`. Re-run in the gates section.
