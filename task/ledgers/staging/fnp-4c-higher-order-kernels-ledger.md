# Unit ledger — FNP-4c · the eight higher-order kernels, plus forall and reduce

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the
orchestrator's departure move). This file closes when FNP-4c merges, or when
the owner closes the slate row.

**Unit:** FNP-4c · **Date:** 2026-08-31 · **Executor:** Grok (grok-4.6), Actor ·
**Branch:** `feat/fnp-4c-higher-order-kernels` · **Base:** `60225cc427673cbc2e4bf23e90db376e602773dd`
(FNP-15/16).
**Charter:** [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) C-003 (Column
entry point) and the FNP-4c slice of C-004 (kernels registered for both doors;
SQL `x -> y` parse remains FNP-4b).
**Design:** [docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md)
§3.5 and §7 row FNP-4c.
**Proven seam:** [fnp-4a-lambda-seam-ledger.md](../archive/2026-08/2026-08-21-fnp-4a-lambda-seam-ledger.md).
**Spec:** [task/fnp-0-census/lambda-spec.md](../../fnp-0-census/lambda-spec.md).
**Slate:** [briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).

**Rubric:** STANDARD (public facade interface; new kernels; quantified
three-door claim). Floor S1. `risk_tier: standard`.

**Writable paths:** `crates/repark-functions/src/higher_order.rs` and
`higher_order/`; facade `functions_lambda.py` / `functions.py` export surface;
Rust and facade pins; maps in lockstep; this ledger. Closed: `Cargo.toml
[patch]`, lockfiles, `.github/`, `briefs/next-sequence.md`, DML-A/B/C, MAINT,
V3-*, W-0, FNP-4b dialect / write-path quoting.

FNP-4a already owns the registry both doors read, lambda-variable resolution,
and `PyColumn::lambda_variable` / `call_higher_order`. This unit builds on that
seam. It does not invent a second one. `exists` already ships as an alias of
`array_any_match`. The remaining ten Spark names land here.

## Names (design §3.5)

| Spark name | Shape |
|---|---|
| `transform` | New kernel declaring `[element, index]` (index lazy) |
| `filter` | New kernel declaring `[element, index]` (index lazy) |
| `aggregate` | New kernel: 2-ary merge, optional 1-ary finish, sequential fold |
| `reduce` | Alias of `aggregate` |
| `forall` | `NOT array_any_match(a, x -> NOT p(x))` (all-match; empty→true) |
| `zip_with` | New kernel: two arrays, 2-ary lambda, null-pad the shorter |
| `transform_keys` | New kernel: null-key runtime error + `mapKeyDedupPolicy` |
| `transform_values` | New kernel |
| `map_filter` | New kernel |
| `map_zip_with` | New kernel: ternary lambda, Spark key-union order |

These ten names were **absent** (`AttributeError`) at charter; they are not
members of the FNP-15/16 `armed_names()` roster of 62. That roster stays 62.
The FNP-4a pin now asserts RePark kernels named `transform`/`filter`, not the
unary DataFusion kernels.

SQL `x -> y` parse is FNP-4b (Databricks dialect vs engine-internal ANSI
quotes). FNP-4c registers the kernels both doors read and pins them on the
facade Column API and the native `Expr` / `SessionContext` path. Isolated
Databricks-dialect SQL tests may prove the kernel through SQL without enabling
the dialect on the production Spark door.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | `transform` is a RePark `HigherOrderUDFImpl` declaring `[element, index]`. Unary and `(element, index)` Spark arities both evaluate. Index is 0-based Int32. Index is not materialized when the lambda is unary. Values, Arrow type, null-array, empty-array, and null-element cells match Spark 4.1.2. | Facade Column API + Rust kernel pins, red-first against the FNP-4a `by_name("transform").is_none()` fixture. | **PROVEN** |
| C-002 | `filter` is a RePark kernel declaring `[element, index]`, same arity and index contract as C-001. Null predicate drops the element (false). Return type equals the input array type. | Facade + Rust pins, red-first vs `by_name("filter").is_none()`. | **PROVEN** |
| C-003 | `aggregate` is a RePark kernel: two value args (array, initial), 2-ary merge, optional 1-ary finish, sequential left-to-right fold. Empty array yields initial (then finish). Null array yields null. Merge/finish null handling matches the oracle. | Facade + Rust pins including finish-present and finish-absent. | **PROVEN** |
| C-004 | `reduce` is an alias of the `aggregate` kernel. PySpark signatures are byte-identical. No second kernel. | `by_name("reduce")` resolves the aggregate kernel; facade `F.reduce` matches `F.aggregate` on a shared fixture. | **PROVEN** |
| C-005 | `forall` is the De Morgan rewrite of `exists` / `array_any_match`: false if any element is false; else null if any predicate is null; else true. Empty array → true. Null array → null. Not a nested higher-order call. | Facade + Rust pins on the five census cases. | **PROVEN** |
| C-006 | `zip_with` pairs two arrays with a 2-ary lambda. The shorter array is null-padded to the longer length before the lambda runs. Result length is `max(len(left), len(right))`. Null either array → null. | Facade + Rust pins including unequal lengths. | **PROVEN** |
| C-007 | `transform_keys` applies a 2-ary `(k, v)` lambda to produce new keys. A null produced key is a runtime error. Duplicate produced keys raise Spark's `DUPLICATED_MAP_KEY` / `mapKeyDedupPolicy` EXCEPTION text (RePark's existing default). | Facade + Rust pins: happy path, null key, duplicate key. | **PROVEN** |
| C-008 | `transform_values` applies a 2-ary `(k, v)` lambda to produce new values. Keys are unchanged. Null values are allowed. | Facade + Rust pins. | **PROVEN** |
| C-009 | `map_filter` keeps entries whose 2-ary predicate is true. Null predicate drops the entry. Return type equals the input map type. | Facade + Rust pins. | **PROVEN** |
| C-010 | `map_zip_with` takes two maps and a ternary `(k, v1, v2)` lambda. Key set is the union: map1 keys in order, then map2-only keys. A key absent from one map yields a null value argument. | Facade + Rust pins including map2-only keys. | **PROVEN** |
| C-011 | One kernel per Spark name (except `reduce` as alias). Both doors resolve the same table (`higher_order::functions` / `by_name`). Python does not compute rows. Nested higher-order remains the FNP-4a loud refusal. | Registry tests; facade builds through `call_higher_order`; the FNP-4a nested-HOF pin still reds. | **PROVEN** |
| C-012 | Per-name lambda arity is refused at the facade with Spark's class (`PySparkValueError`) before a plan error. A lambda that does not return a `Column` is refused. Kernel-side extra lambda params vs declared params remain a plan error. Measured Spark error text is the oracle; incidental controls are measured too. | Facade pins per name; at least one kernel-side arity/type pin. | **PROVEN** |
| C-013 | The FNP-4a seam is the only seam: no second `call_higher_order`, no alias of Spark `transform`/`filter` onto unary `array_transform`/`array_filter`. `exists` remains the `array_any_match` alias. | The FNP-4a registry tests, updated so `transform`/`filter` resolve RePark kernels whose `name()` is not `array_transform`/`array_filter`. | **PROVEN** |
| C-014 | Docs and maps stay in lockstep. File-size ceilings ratchet down only. `functions.py` does not raise its 1985 baseline. New Rust files stay at or under the 1000-line default. | `make check-map-sync`; `check_lib_py` / `check_rust_file_size` green. | **PROVEN** |
| C-015 | Gates before done: `make verify`, `make check-map-sync check-ledger-grammar`, `python3 scripts/ledger_lifecycle.py check --base 60225cc427673cbc2e4bf23e90db376e602773dd`, full `make py-test`, `make py-test-facade` for facade tests added. Real exit codes. | Recorded at close. | **OPEN** |

## Sequence

1. This ledger (grammar-gate clean, verdicts OPEN) — this commit.
2. Measure Spark 4.1.2 oracle cells (values, Arrow types, NULL/empty, errors).
3. Shared `lambda_utils` + `transform` / `filter` / `forall`.
4. `aggregate` / `reduce` + `zip_with`.
5. Map family: `transform_keys`, `transform_values`, `map_filter`, `map_zip_with`.
6. Facade wrappers, `__all__` install, pins, registry retirement of the
   `by_name is none` fixtures.
7. Gates. Ledger verdicts flip when the pins exist.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-fnp-4c-charter
  agent: Actor
  action: File the FNP-4c staging ledger and lockstep maps, no kernel code yet
  charter_trace: FNP-0 C-003; this unit C-001..C-015
  preconditions:
    - AGENTS.md read path and design §3.5/FNP-4c row: SATISFIED (docs/design/spark-function-parity.md:218-246, :420)
    - FNP-4a seam delivered: SATISFIED (task/ledgers/archive/2026-08/2026-08-21-fnp-4a-lambda-seam-ledger.md)
    - Branch is feat/fnp-4c-higher-order-kernels at 60225cc: SATISFIED (git)
    - Disk headroom: SATISFIED (306 G free of 1.8 T)
  success_condition: staging ledger exists, staging/map.md links it, check-ledger-grammar accepts OPEN clauses
  step_risks:
    - Chartering SQL parse as in-scope would fork into FNP-4b: HANDLED(C-011 states FNP-4b owns parse; kernels register on the existing table)
    - Growing functions.py ceiling: HANDLED(C-014; install_into pattern, ratchet down only)
  contingencies:
    - Revert this commit if grammar-red: EXECUTABLE(additive git revert)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-fnp-4c-kernels
  agent: Actor
  action: Land the ten Spark names on the FNP-4a table and the facade Column API
  charter_trace: FNP-4c C-001..C-014
  preconditions:
    - Charter commit 6e9e6c8: SATISFIED
    - Live PySpark 4.1.2 cells measured 2026-08-31: SATISFIED (c26-oracle)
    - functions.py stays 1985: SATISFIED (install_into; extra import; two docstring lines dropped)
  success_condition: registry resolves ten names; facade pins match the oracle; nested HOF still refused
  step_risks:
    - Second seam: HANDLED(call_higher_order + by_name only; exists stays array_any_match)
    - DataFusion drops unused lambda params: HANDLED(_keep_lambda_params struct wrap)
    - SQL x->y parse: HANDLED(FNP-4b; this unit registers kernels both doors read)
  contingencies:
    - Revert the kernel commit: EXECUTABLE
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Disk (AGENTS.md "Resource discipline")

Checked 2026-08-31 at pickup: `/` 306 G free of 1.8 T (83% used). No worktree.
Incremental `target/` reuse. No `cargo clean`. Spark oracle at
`/home/john/grok-trees/c26-oracle` (PySpark 4.1.2) is read-only measurement.

## Oracle

Live PySpark **4.1.2** via `/home/john/grok-trees/c26-oracle`. Cells recorded
in the execution record as they are measured. Hand-computed expectations are
not an oracle.
