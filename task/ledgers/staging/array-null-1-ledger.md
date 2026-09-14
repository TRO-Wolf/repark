# Unit ledger — ARRAY-NULL-1 · `F.array_append` / `F.array_prepend` null-preserving linear lowering — step 0

**Date:** 2026-09-14 · **Branch:** `fix/array-null-1` · **Base:** `e147685b` (`main`,
overnight-12 docs)
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card ARRAY-NULL-1 (run 12b slate): the facade's `_glue_element` builds
`when(isnull(arr), NULL).otherwise(flatten(array(arr, array(x))))` — the running
expression is embedded twice per level, so a chain of N appends references the input
2^N times. Measured on the base tree: 51 MB at depth 12, 269 MB at depth 16 — a
depth-40 chain cannot plan (red-first pin below). Step 0 measures the D-2 oracle
cells on live PySpark 4.1.2 beside today's facade AND SQL-door answers, lands the
env-gated red-first depth-40 memory pin, and measures the two D-1 candidate routes.
No product code is committed in this step; the spike tree was restored
(`git diff origin/main -- crates python/repark/src` is empty).

**Not in this step:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`, and every product file — the rewrite is step 1.

## PROPOSITION LEDGER — ARRAY-NULL-1 step 0 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The D-2 oracle cells are measured on live PySpark 4.1.2 (one `local[1]` session, ANSI default, stopped before other work) beside repark's answers on the same explicit-schema frames through the facade AND the SQL door: answer rows AND result type (element type, `containsNull`) or error class, for NULL array, NULL element, empty array, nested arrays, int→`array<bigint>`, int→`array<double>`, string→`array<int>`, NULL element into a `containsNull=False` array, and a literal array — for both `array_append` and `array_prepend`. | The measured table under Evidence + the facade oracle pins in `python/repark/tests/test_array_null_1.py`. | PROVEN | Measured 2026-09-14, PySpark 4.1.2, `spark.sql.ansi.enabled` default, zulu-17. Spark widens `containsNull` to true in every result. Facade is cell-correct today (pinned); door diverges on NULL-array append (`[4]` not NULL) and refuses every Spark-spelled `array_prepend(a, e)` (DF resolves `(element, array)`). |
| C-002 | A chain of 40 nested `F.array_append` calls plans and collects under a bounded RSS delta — bound = `max(64 MB, 2 × flat 40-append select delta)` — in a subprocess under `RLIMIT_AS = VmSize_at_apply + 3 × 8 GB`. Red on the base tree, green after the step-1 lowering. | `test_array_append_depth40_memory_linear` red output pasted in Evidence, then green. | OPEN | Pin landed env-gated (`REPARK_ARRAY_NULL_1_MEM=1`); red demonstrated 2026-09-14 — worker crossed the 64 MB bound at level 15 with 131 014 656 B delta. RSS table at depths 4/8/12/14/16 below. |
| C-003 | One D-1 route is correct on every C-001 cell and linear at depth 40: (a) a `ScalarUDF` calling DataFusion's `array_append`/`array_prepend` kernel then grafting the input's outer null buffer onto the result; (b) a Rust CASE referencing the child once through a plan-level alias. | Correctness on every cell (yes/no) + depth-12 and depth-40 RSS delta and plan/collect wall time per route, release native. | PROVEN | Route (a): all 18 cells correct (facade AND door); depth-12 delta 1.84 MB / collect 4.5 ms; depth-40 delta 1.93 MB / collect 55 ms — linear. Route (b): all cells correct but depth-12 delta 35–37 MB / collect ~1.5 s; depth-40 worker aborted (`memory allocation failed`) under the 24 GB headroom — the CASE still embeds the child twice per level, and DataFusion has no lateral plan-level alias to bind it once (measured: `Schema error: No field named x`). Route (a) is the D-1 choice. |
| C-004 | Per D-3 the SQL door routes through the same corrected arm — measured: registering `spark_array_append_udf`/`spark_array_prepend_udf` under the names `array_append`/`array_prepend` (after DF's defaults in `collection::functions()`) makes `spark.sql("SELECT array_append(a, e) FROM v")` and `array_prepend(a, e)` answer identically to the facade on every cell, in Spark arg order. | Door answers equal facade answers per cell under the spike. | OPEN | Measured under the spike: door = facade on all 18 cells, including NULL-array NULL and Spark-order `array_prepend(a, e)` — no `EXPECTED_DIVERGENCES` row needed for the arm itself. Residual: door `array(1,2)` literal stays Int64 (DF SQL integer-literal width, identical on base) — orthogonal dialect difference, not an append/prepend divergence. Door prepend arg order changes from DF `(element, array)` to Spark `(array, element)` under this route — a door-semantic change step 1 must name. |
| C-005 | Step 1 lands answer pins for every C-001 cell (value AND Arrow type, facade and door). | Pins in `python/repark/tests/` green after step 1. | OPEN | — |

## Evidence

### C-001 oracle measurement (live PySpark 4.1.2, `local[1]`, ANSI default, zulu-17 — 2026-09-14)

`array_append` — `SELECT array_append(a, e) FROM v` / `F.array_append(a, e)`:

| Cell | PySpark 4.1.2 | repark facade today | repark SQL door today |
|---|---|---|---|
| NULL array `a` + `e=4` | `[None]` `array<int>` cn=true | `[None]` `list<int32>` nullable=true | `[[4]]` `list<int32>` — **drops NULL** |
| `[1,2]` + NULL `e` | `[[1,2,None]]` `array<int>` cn=true | `[[1,2,null]]` `list<int32>` | `[[1,2,null]]` `list<int32>` |
| `[]` + `e=4` | `[[4]]` `array<int>` cn=true | `[[4]]` `list<int32>` | `[[4]]` `list<int32>` |
| nested `[[1],[2,3]]` + `[9]` | `[[[1],[2,3],[9]]]` `array<array<int>>` cn=true | `[[[1],[2,3],[9]]]` `list<list<int32>>` | `[[[1],[2,3],[9]]]` `list<list<int32>>` |
| int into `array<bigint>` | `[[1,2,4]]` `array<bigint>` cn=true | `[[1,2,4]]` `list<int64>` | `[[1,2,4]]` `list<int64>` |
| int into `array<double>` | `[[1.0,4.0]]` `array<double>` cn=true | `[[1.0,4.0]]` `list<double>` | `[[1.0,4.0]]` `list<double>` |
| string into `array<int>` | refuses `AnalysisException [DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES]` | refuses `PySparkException: simplify_expressions` (at collect) | refuses `AnalysisException` coercion `List(Int32), Utf8` |
| NULL element into `array<int>` cn=false | `[[1,2,None]]` `array<int>` cn=true (widens) | `[[1,2,null]]` `list<int32>` nullable=true | `[[1,2,null]]` `list<int32>` nullable=true |
| literal `array(1,2)` + 3 | `[[1,2,3]]` `array<int>` cn=true | `[[1,2,3]]` `list<int32>` | `[[1,2,3]]` `list<int64>` — DF literal width |

`array_prepend` — `SELECT array_prepend(a, e) FROM v` / `F.array_prepend(a, e)`:

| Cell | PySpark 4.1.2 | repark facade today | repark SQL door today |
|---|---|---|---|
| NULL array `a` + `e=4` | `[None]` `array<int>` cn=true | `[None]` `list<int32>` nullable=true | refuses `AnalysisException: array_prepend does not support type Int32` |
| `[1,2]` + NULL `e` | `[[None,1,2]]` `array<int>` cn=true | `[[null,1,2]]` `list<int32>` | refuses (same arg-order error) |
| `[]` + `e=4` | `[[4]]` `array<int>` cn=true | `[[4]]` `list<int32>` | refuses (same) |
| nested `[[1],[2,3]]` + `[9]` | `[[[9],[1],[2,3]]]` `array<array<int>>` cn=true | `[[[9],[1],[2,3]]]` `list<list<int32>>` | refuses `ArraySignature(element, array)` coercion |
| int into `array<bigint>` | `[[4,1,2]]` `array<bigint>` cn=true | `[[4,1,2]]` `list<int64>` | refuses (same) |
| int into `array<double>` | `[[4.0,1.0]]` `array<double>` cn=true | `[[4.0,1.0]]` `list<double>` | refuses (same) |
| string into `array<int>` | refuses `AnalysisException [DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES]` | refuses `PySparkException: simplify_expressions` | refuses (same arg-order error) |
| NULL element into `array<int>` cn=false | `[[None,1,2]]` `array<int>` cn=true (widens) | `[[null,1,2]]` `list<int32>` nullable=true | refuses (same) |
| literal `array(1,2)` + 3 | `[[3,1,2]]` `array<int>` cn=true | `[[3,1,2]]` `list<int32>` | refuses `array_prepend(List(Int64), Int64)` |

Notes: `cn` = `containsNull`. The facade is answer-correct on every cell today (the
`when(isnull, NULL).otherwise(flatten)` guard preserves Spark semantics; its defect
is plan size, not answers). Door divergences on the base tree: `array_append` drops
the NULL array (`[4]`, the card's documented `None`-null-buffer loss in
`generic_append_and_prepend`), `array_prepend` in Spark arg order refuses every
cell (DF's signature is `(element, array)`), and door literal arrays are Int64
(pre-existing SQL integer-literal width, present identically under the spike).
Refusal cells match Spark's shape (both refuse) with different error classes.

### C-002 red-first memory pin

Base tree `e147685b`, debug native (`make develop`). Worker: 1-row `a array<int>`
frame, warmup collect, then `RLIMIT_AS = VmSize_at_apply + 3 × 8 GB`, then a depth-N
nested `F.array_append`/`F.array_prepend` chain; RSS delta = `VmHWM` after − `VmHWM`
at baseline (build-only deltas where collect was impractical).

| Mode | Depth | Build delta | Total delta |
|---|---:|---:|---:|
| append | 4 | 131 072 B | 3 702 784 B |
| append | 8 | 131 072 B | 4 354 048 B |
| append | 12 | 19 210 240 B | 51 343 360 B |
| append | 14 | 66 834 432 B | 148 176 896 B |
| append | 16 | 269 094 912 B | 269 094 912 B (build; collect impractical >10 min) |
| prepend | 4 | 131 072 B | 3 702 784 B |
| prepend | 8 | 131 072 B | 7 716 864 B |
| prepend | 12 | 19 210 240 B | 46 710 784 B |
| prepend | 14 | 66 400 256 B | 155 086 848 B |
| prepend | 16 | 268 664 832 B | 268 664 832 B (build; collect impractical) |
| flat 40-append select | 40 | — | 4 046 848 B |

~×3/level above depth ~10 — the exponential doubling the card predicts; depth-40
cannot be reached (2^40 leaf references). The pin bound is `max(64 MB, 2 × flat
control)` = 64 MB on this box.

Armed run (`REPARK_ARRAY_NULL_1_MEM=1`), red on the base tree:

```
AssertionError: F.array_append depth-40 crossed the bound 67108864 B
(2x flat 40-append select delta 4046848 B, floor 67108864 B): 15 at delta 131014656 B;
assert 2 == 0
FAILED python/repark/tests/test_array_null_1.py::test_array_append_depth40_memory_linear
```

The worker's per-level bound check exited at level 15 (rc=2) with a 131 MB delta —
the chain never reaches 40.

### C-003 route spikes (scratch — restored before commit)

Spike shape (described; the diff was ~300 lines across four files, too large for a
patch file): `crates/repark-functions/src/collection/array_append.rs` defined
`SparkArrayAppend`/`SparkArrayPrepend` `ScalarUDFImpl`s —
`Signature::array_and_element`, `return_type` forcing the element field nullable,
`invoke_with_args` delegating to `datafusion_functions_nested::concat` `array_append_udf()`
/`array_prepend_udf()` (args swapped for prepend) and then grafting the input's
`NullBuffer` onto the kernel result via `ArrayData::into_builder().nulls(...)`
(`DataType::Null` input → all-null result of the kernel's result type);
registered after DF defaults in `collection::functions()`. `call_scalar_expr` gained
`array_append`/`array_prepend` arms building `ScalarFunction::new_udf` and
`array_append_case`/`array_prepend_case` arms building
`Case{is_null(child) → NULL, else kernel(child, e)}`. `_glue_element` selected the
route by env (`REPARK_ARRAY_NULL_1_ROUTE`); correctness ran on `make develop`
(debug), timing/memory on `maturin develop --release`.

Correctness (debug native, 2026-09-14): route (a) and route (b) both answered all
18 cells through facade AND door identically to the oracle (including the
`containsNull=False` + NULL element widening and both refusal cells).

RSS delta + wall time (release native, `VmHWM` deltas, 1-row `a array<int>` frame):

| Route | Mode | Depth | Build wall | Collect wall | Build delta | Total delta |
|---|---|---:|---:|---:|---:|---:|
| a | append | 12 | 0.2 ms | 4.5 ms | 135 168 B | 1 839 104 B |
| a | append | 40 | 0.6 ms | 55.3 ms | 147 456 B | 1 933 312 B |
| a | prepend | 12 | 0.2 ms | 4.5 ms | 135 168 B | 1 839 104 B |
| a | prepend | 40 | 0.6 ms | 55.6 ms | 143 360 B | 1 929 216 B |
| b | append | 12 | 3.4 ms | 1 463 ms | 4 399 104 B | 35 139 584 B |
| b | append | 40 | — | — | — | died: `memory allocation of 112 bytes failed` (SIGABRT under the 24 GB headroom) |
| b | prepend | 12 | 5.1 ms | 1 564 ms | 4 403 200 B | 36 950 016 B |
| b | prepend | 40 | — | — | — | died: `memory allocation of 112 bytes failed` |

Route (a) is flat at depth 40 (~1.9 MB vs ~1.8 MB at depth 12; the 55 ms collect is
analyzer/physical-plan overhead per nested UDF call — wall growth ~12× over a 3.3×
depth increase, bounded in absolute terms and flat in memory). Route (b) cannot be
linearized as written: a searched CASE needs the child in both the `when` arm and
the kernel call, and no plan-level mechanism binds an expression once for lateral
reference — a same-projection alias ref fails planning (`Schema error: No field
named x`; the alias-expression form fails type coercion). Per D-1, route (a) — the
null-buffer-grafting `ScalarUDF` — is the choice; C-003 PROVEN.

### C-004 SQL-door answer under route (a)

Under the spike (route (a) registered under `array_append`/`array_prepend` after
DF's defaults): `spark.sql("SELECT array_append(a, e) FROM v")` and
`SELECT array_prepend(a, e) FROM v` answered identically to the facade on every
cell — `[None]` for NULL array, `[null,1,2]`-order for prepend, `list<int32>` for
the literal column path. The door therefore routes through the same corrected arm
(D-3 arm 1); no `EXPECTED_DIVERGENCES` row is needed for the arm. Side effects to
name in step 1: the door's `array_prepend` argument order becomes Spark's
`(array, element)` (today only DF's `(element, array)` parses), and the door's
literal-array result stays `list<int64>` (DF SQL literal width — identical on
base, orthogonal to this arm).

## Gates

- `REPARK_ARRAY_NULL_1_MEM=1 .venv/bin/python -m pytest python/repark/tests/test_array_null_1.py -q` → red on base (pasted, C-002).
- `.venv/bin/python -m pytest python/repark/tests/test_array_null_1.py -q` → green (pin skipped by default).
- `grep -rln "array_append\|array_prepend" python/repark/tests` → `test_array_null_1.py`, `test_functions_e.py`.
- `make verify` → green on the restored tree.
