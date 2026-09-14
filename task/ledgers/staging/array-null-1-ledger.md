# Unit ledger — ARRAY-NULL-1 · `F.array_append` / `F.array_prepend` null-preserving linear lowering

**Date:** 2026-09-14 · **Branch:** `fix/array-null-1` · **Base:** `e147685b` (`main`,
overnight-12 docs)
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card ARRAY-NULL-1 (run 12b slate): the facade's `_glue_element` builds
`when(isnull(arr), NULL).otherwise(flatten(array(arr, array(x))))` — the running
expression is embedded twice per level, so a chain of N appends references the input
2^N times. Measured on the base tree: 51 MB at depth 12, 269 MB at depth 16 — a
depth-40 chain cannot plan (red-first pin below). Step 0 measured the D-2 oracle
cells on live PySpark 4.1.2 beside today's facade AND SQL-door answers, landed the
red-first depth-40 memory pin, and measured the two D-1 candidate routes; step 0
committed no product code (the spike tree was restored).

**Step 1.** Under the orchestrator's D-1 ruling (route (a) chosen), step 1 lands the
product arm: `spark_array_append_udf`/`spark_array_prepend_udf` in
`crates/repark-functions/src/collection/array_append.rs` — a `ScalarUDF` that
delegates to DataFusion's native append/prepend kernel and then grafts the input
array's outer `NullBuffer` onto the result. Registered after DF's defaults in
`collection::functions()`, so `array_append`/`array_prepend` resolve the shim on the
SQL door in Spark `(array, element)` order; the facade's `_glue_element` makes one
`_scalar` call per level through the `dispatch_json.rs` arms. The depth-40 memory
pin runs by default on both functions under `max(8 MB, 2 × flat control)`, and both
doors carry value+type pins for every D-2 cell.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`, and the files run 13 owns.

## PROPOSITION LEDGER — ARRAY-NULL-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The D-2 oracle cells are measured on live PySpark 4.1.2 (one `local[1]` session, ANSI default, stopped before other work) beside repark's answers on the same explicit-schema frames through the facade AND the SQL door: answer rows AND result type (element type, `containsNull`) or error class, for NULL array, NULL element, empty array, nested arrays, int→`array<bigint>`, int→`array<double>`, string→`array<int>`, NULL element into a `containsNull=False` array, and a literal array — for both `array_append` and `array_prepend`. | The measured table under Evidence + the facade oracle pins in `python/repark/tests/test_array_null_1.py`. | PROVEN | Measured 2026-09-14, PySpark 4.1.2, `spark.sql.ansi.enabled` default, zulu-17. Spark widens `containsNull` to true in every result. Facade was cell-correct on the base tree; door then diverged on NULL-array append (`[4]` not NULL) and refused every Spark-spelled `array_prepend(a, e)` — both closed by the step-1 arm, now pinned on both doors. |
| C-002 | A chain of 40 nested `F.array_append`/`F.array_prepend` calls plans and collects under a bounded RSS delta — bound = `max(8 MB, 2 × flat 40-append select delta)` — in a subprocess under `RLIMIT_AS = VmSize_at_apply + 3 × 8 GB`. Red on the base tree, green after the step-1 lowering, default-gated on both functions. | `test_array_append_depth40_memory_linear[append\|prepend]` red output pasted, then green with the five-run deltas below. | PROVEN | Red demonstrated 2026-09-14 on the base tree (64 MB bound crossed at level 15, 131 014 656 B delta). After the arm: five consecutive runs, deltas 1 957 888–2 035 712 B per leg vs bound 8 388 608 B — the pin is in the default suite, ungated, covering both functions. |
| C-003 | One D-1 route is correct on every C-001 cell and linear at depth 40: (a) a `ScalarUDF` calling DataFusion's `array_append`/`array_prepend` kernel then grafting the input's outer null buffer onto the result; (b) a Rust CASE referencing the child once through a plan-level alias. | Correctness on every cell (yes/no) + depth-12 and depth-40 RSS delta and plan/collect wall time per route, release native. | PROVEN | Route (a) chosen by the orchestrator's D-1 ruling and landed as `spark_array_append_udf`/`spark_array_prepend_udf` (`collection/array_append.rs`): all 18 cells correct through facade AND door; release depth-40 delta ~2.0 MB / collect ~56 ms, depth-100 delta ~2.2 MB / collect ~0.7 s — linear. Route (b) measured correct but 35–37 MB at depth 12 and alloc-abort at depth 40; DataFusion has no lateral plan-level alias (`Schema error: No field named x`). |
| C-004 | Per D-3 the SQL door routes through the same corrected arm — `spark.sql("SELECT array_append(a, e) FROM v")` and `array_prepend(a, e)` answer identically to the facade on every cell, in Spark `(array, element)` order, without shadowing a name another caller relies on. | Door answers equal facade answers per cell, pinned; name-shadow evidence recorded. | PROVEN | Door pins landed (`test_array_*_door_oracle_cells`): all 18 cells answer like the facade, including NULL-array NULL and Spark-order `array_prepend(a, e)`; the door-parity ratchet is green with no `EXPECTED_DIVERGENCES` row. Registration after DF's defaults replaces only the two primary names — DF's `register_udf` keys on name+aliases and the shim declares none, so `list_append`, `array_push_back`, `list_push_back`, `list_prepend`, `array_push_front`, `list_push_front` keep DF's kernel. PySpark 4.1.2 exposes no `list_*` spellings, so no Spark-facing name is shadowed. The door's `array_prepend` arg order becomes Spark's `(array, element)` — the only door-semantic change, named here per the card. Residual: door `array(1,2)` literal stays `list<int64>` (DF SQL integer-literal width, identical on base) — orthogonal dialect difference, pinned as actual behavior. |
| C-005 | Answer pins for every C-001 cell (value AND Arrow type AND element nullability, facade and door). | Pins in `python/repark/tests/` green after step 1. | PROVEN | `test_array_null_1.py` pins all nine cells per function per door: `to_pylist` values, `value_type`, and `value_field.nullable` (Spark's `containsNull=True` widening) via `to_arrow`, including nested arrays, both numeric coercions, the string-into-int refusal, the `containsNull=False`+NULL-element cell, and literal arrays. Rust unit tests pin the sliced-input null graft and the door SQL spellings. |

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

### Step 1 — the landed arm (route (a), product code)

`crates/repark-functions/src/collection/array_append.rs`:
`SparkArrayAppend`/`SparkArrayPrepend` `ScalarUDFImpl`s —
`Signature::array_and_element`, `return_type`/`return_field_from_args` forcing the
result element field nullable (Spark's `containsNull=True` widening) and the outer
field mirroring the input's nullability; `invoke_with_args` delegates to
`datafusion_functions_nested::concat` `array_append_udf()`/`array_prepend_udf()`
(args swapped for prepend — DF's kernel order is `(element, array)`), then grafts
the input's `NullBuffer` onto the kernel result via
`ArrayData::into_builder().nulls(...)`. A sliced input keeps its own offset
(`NullBuffer` carries offset metadata — pinned by
`null_graft_honours_a_sliced_inputs_own_offset` and
`udf_invoke_grafts_nulls_from_a_sliced_array_argument`); a `DataType::Null` input
yields an all-null result of the kernel's result type; an all-scalar call returns
a `Scalar`. Registered last in `collection::functions()` so both names replace
DF's on every door; the shim declares no aliases, leaving the DF-only
`list_*`/`array_push_*` spellings on DF's kernel.

Facade: `_glue_element` → `_scalar("array_append"/"array_prepend", array_col,
element)` — one native call per level, no CASE, no second reference to the array.
Dispatch: `array_append`/`array_prepend` arms in
`column/function_dispatch/dispatch_json.rs` (the parent table is at its 1000-line
ceiling; the FNP-9 child owns the collections arms) building
`ScalarFunction::new_udf` over the same UDFs the door registers — the parity
ratchet holds with no divergence row.

### Before/after (release)

`maturin develop --release` on the unmodified tree (before) and on the landed arm
(after); `before_after.py` workers under `RLIMIT_AS = VmSize + 3 × 8 GB`, 300 s
cap, `VmHWM` deltas on a 1-row `a array<int>` frame.

`array_append`:

| Depth | Before build wall | Before collect wall | Before total delta | After build wall | After collect wall | After total delta |
|---:|---:|---:|---:|---:|---:|---:|
| 4 | 0.4 ms | 7.1 ms | 3 743 744 B | 0.1 ms | 1.4 ms | 1 900 544 B |
| 8 | 1.2 ms | 155.2 ms | 7 958 528 B | 0.1 ms | 2.5 ms | 1 900 544 B |
| 12 | 14.0 ms | 3 965.4 ms | 58 134 528 B | 0.2 ms | 4.6 ms | 1 904 640 B |
| 14 | 66.7 ms | 23 356.2 ms | 151 711 744 B | — | — | — |
| 16 | 317.9 ms | 126 446.5 ms | 577 318 912 B | 0.2 ms | 7.3 ms | 1 912 832 B |
| 40 | — | — | — | 0.5 ms | 56.1 ms | 1 998 848 B |
| 100 | — | — | — | 1.6 ms | 676.6 ms | 2 207 744 B |

`array_prepend`:

| Depth | Before build wall | Before collect wall | Before total delta | After build wall | After collect wall | After total delta |
|---:|---:|---:|---:|---:|---:|---:|
| 4 | 0.4 ms | 7.4 ms | 3 743 744 B | 0.1 ms | 1.3 ms | 1 900 544 B |
| 8 | 1.3 ms | 157.6 ms | 7 958 528 B | 0.2 ms | 2.5 ms | 1 900 544 B |
| 12 | 15.2 ms | 4 272.4 ms | 55 201 792 B | 0.2 ms | 4.4 ms | 1 904 640 B |
| 14 | 71.0 ms | 23 075.5 ms | 150 683 648 B | — | — | — |
| 16 | 313.9 ms | 124 545.9 ms | 567 521 280 B | 0.3 ms | 7.4 ms | 1 912 832 B |
| 40 | — | — | — | 0.5 ms | 56.9 ms | 1 990 656 B |
| 100 | — | — | — | 1.6 ms | 708.7 ms | 2 203 648 B |

Before numbers are the release build of the unmodified product tree (Q-R12-2);
the base dies past depth 16 (2^N expression growth — the step-0 debug table
showed ~×3/level; depth-40 cannot plan). After numbers stay ~2 MB RSS through
depth 100.

### Depth-40 memory pin — five consecutive runs (debug native, `make develop`)

Bound = `max(8 MiB, 2 × flat 40-append control)` = 8 388 608 B each run.

| Run | Flat control delta | Append delta | Prepend delta |
|---:|---:|---:|---:|
| 1 | 1 806 336 B | 2 035 712 B | 1 957 888 B |
| 2 | 1 806 336 B | 2 027 520 B | 1 957 888 B |
| 3 | 1 810 432 B | 2 035 712 B | 1 961 984 B |
| 4 | 1 806 336 B | 2 031 616 B | 1 961 984 B |
| 5 | 1 806 336 B | 2 035 712 B | 1 957 888 B |

### Residue

Collect wall grows ~12× from depth 12 to depth 40 on the landed arm (append
4.6 ms → 56.1 ms; prepend 4.4 ms → 56.9 ms) and ~12× again to depth 100
(~0.68–0.71 s) — per-level analyzer/physical-plan overhead, roughly quadratic in
wall while RSS stays flat (~2.0 MB at 40, ~2.2 MB at 100). Bounded in absolute
terms; recorded, not fixed, per the brief.

## Gates

Step 0 (on the restored base tree, 2026-09-14):

- `REPARK_ARRAY_NULL_1_MEM=1 .venv/bin/python -m pytest python/repark/tests/test_array_null_1.py -q` → red on base (pasted, C-002).
- `.venv/bin/python -m pytest python/repark/tests/test_array_null_1.py -q` → green (pin skipped by default).
- `grep -rln "array_append\|array_prepend" python/repark/tests` → `test_array_null_1.py`, `test_functions_e.py`.
- `make verify` → green.

Step 1 (the landed arm, 2026-09-14):

- `cargo test -p repark-functions` → green (452 tests incl. 5 new `collection::array_append` tests).
- `cargo test -p repark-python door_parity` → green (4 tests; no `EXPECTED_DIVERGENCES` row added).
- `make develop` → green.
- `.venv/bin/python -m pytest python/repark/tests/test_array_null_1.py -q -p no:cacheprovider` → green, 6 tests ungated.
- `grep -rln "array_append\|array_prepend" python/repark/tests` → `test_array_null_1.py`, `test_functions_e.py`, `test_functions_split_identity.py`; all green.
- `make verify` → green.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: array-null-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-005 walked against behavior — the oracle cells were measured on live PySpark 4.1.2 beside base facade AND door answers, both D-1 routes were measured for correctness and depth-12/40 RSS+wall on a release native, the memory pin ran red-first on the base tree then green under the arm, and the before/after table came from real release builds of both trees (Q-R12-2).
      artifacts: [task/ledgers/staging/array-null-1-ledger.md, task/ledgers/staging/array-null-1-spikes/before_after.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary cells exercised on both doors — NULL array, NULL element, empty array, nested array<int> element, int into array<bigint>, int into array<double>, string into array<int>, NULL element into a containsNull=False array, literal array, CAST(NULL AS ARRAY<INT>), and a sliced input whose NullBuffer carries a non-zero offset; chain depths 1..100 measured.
      artifacts: [python/repark/tests/test_array_null_1.py, crates/repark-functions/src/collection/array_append.rs]
    - id: AT-3
      status: ATTACKED
      evidence: String-into-array<int> refuses on both doors (pinned); a non-array first argument plan-errors; a DataType::Null input yields an all-null result rather than panicking; the memory worker exits rc 2 at the bound crossing instead of aborting — the base-tree red was the bound exit at level 15.
      artifacts: [python/repark/tests/test_array_null_1.py, crates/repark-functions/src/collection/array_append.rs]
    - id: AT-4
      status: ATTACKED
      evidence: No shared mutable state — each UDF impl is a stateless ScalarUDFImpl; each pin session is created and stopped in its own fixture and the memory legs run in isolated subprocesses; the sliced-input graft test builds its own arrays per call.
      artifacts: [python/repark/tests/test_array_null_1.py, crates/repark-functions/src/collection/array_append.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, injection or deserialization surface — the dispatch arms are literal name matches and the UDF consumes typed Arrow arrays only.
    - id: AT-6
      status: ATTACKED
      evidence: Arrow value AND type AND element nullability pinned per cell on both doors via to_arrow; the door-parity ratchet re-run green with no EXPECTED_DIVERGENCES row — facade arm and door registration resolve the same ScalarUDF; the DF-only alias names are pinned to keep DF's kernel (no aliases declared on the shim).
      artifacts: [python/repark/tests/test_array_null_1.py, crates/repark-python/src/column/door_parity_tests.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The unit is the AT-7 fix — the exponential facade rewrite is replaced by one native call per level; the depth-40 pin runs in the default suite (measured 1 957 888–2 035 712 B vs the 8 388 608 B bound across five consecutive runs) and the release before/after table shows 577 MB→2.0 MB at depth 16/40; the residual collect-wall growth (~12×, 12→40) is recorded with numbers, not assumed away.
      artifacts: [python/repark/tests/test_array_null_1.py, task/ledgers/staging/array-null-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1.0 behavior verified against the crate source and by measurement — register_udf keys on name+aliases (so the shim replaces only the two primary names), the prepend kernel takes (element, array) (arg swap in the shim), generic_append_and_prepend drops the input null buffer (the graft), and no lateral plan-level alias exists (Schema error measured). File-size ratchet held — the dispatch arms landed in dispatch_json.rs because the parent is at exactly 1000; no ceiling moved.
      artifacts: [crates/repark-functions/src/collection/array_append.rs, crates/repark-python/src/column/function_dispatch/dispatch_json.rs]
    - id: AT-9
      status: N/A
      justification: No log-format or diagnosis-path change; refusal paths raise the same typed exception family as the kernels they delegate to (AnalysisException at planning, PySparkException at collect).
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held — the depth-40 pin crossed its bound at level 15 on the base tree (131 014 656 B) and the door pins would have failed on base (NULL array → [4]; Spark-spelled prepend refused outright); the bound-exit branch has a named input (the base tree at level 15), so no dead branch ships.
      artifacts: [python/repark/tests/test_array_null_1.py, task/ledgers/staging/array-null-1-ledger.md]
```
