## 1. PySpark 4.1.2 — the eleven higher-order functions

Source: `<pyspark-4.1.2-sdist>/src/pyspark/sql/functions/builtin.py`

### Shared dispatch machinery (lines 23255–23336)

All eleven funnel through three private helpers:

- `_get_lambda_parameters(f)` (23255) — `inspect.signature(f)`; **validates arity is 1..3 and nothing else**, raising `WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION`; rejects `*args`/`**kwargs`/keyword-only via `UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION`.
- `_create_lambda(f)` (23285) — binds params positionally to the fixed names `argnames = ["x", "y", "z"]`, calls `f(*args)` once at build time, requires a `Column` back (`HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN`), and wraps as a Catalyst `LambdaFunction`.
- `_invoke_higher_order_function(name, cols, funs)` (23316) — `PythonSQLUtils.fn(name, cols ++ lambdas)`.

**Key consequence for a re-implementation:** Python enforces only the generic 1..3 bound. Per-function arity (e.g. rejecting a 2-arg lambda for `exists`) is enforced *server-side by Catalyst*, not in `builtin.py`. Lambda param names are always `x`/`y`/`z` regardless of what the user wrote.

### Per-function spec

| # | Exact signature (verbatim) | Lambda arity | Param meaning |
|---|---|---|---|
| 1 | `def transform(col: "ColumnOrName", f: Union[Callable[[Column], Column], Callable[[Column, Column], Column]]) -> Column` (23354) | **1 or 2** | `(x)` element; `(x, i)` element + **0-based index** |
| 2 | `def filter(col: "ColumnOrName", f: Union[Callable[[Column], Column], Callable[[Column, Column], Column]]) -> Column` (23508) | **1 or 2** | `(x)` element; `(x, i)` element + **0-based index** |
| 3 | `def exists(col: "ColumnOrName", f: Callable[[Column], Column]) -> Column` (23412) | 1 only | element |
| 4 | `def forall(col: "ColumnOrName", f: Callable[[Column], Column]) -> Column` (23453) | 1 only | element |
| 5 | `def aggregate(col: "ColumnOrName", initialValue: "ColumnOrName", merge: Callable[[Column, Column], Column], finish: Optional[Callable[[Column], Column]] = None) -> Column` (23565) | merge **2**, finish **1** (optional) | `merge(acc, x)`; `finish(acc)` |
| 6 | `def reduce(col, initialValue, merge, finish=None) -> Column` (23641) — signature byte-identical to `aggregate` | same | same |
| 7 | `def zip_with(left: "ColumnOrName", right: "ColumnOrName", f: Callable[[Column, Column], Column]) -> Column` (23714) | 2 | `(x1, x2)` positionally-paired elements |
| 8 | `def transform_keys(col: "ColumnOrName", f: Callable[[Column, Column], Column]) -> Column` (23769) | 2 | `(k, v)` |
| 9 | `def transform_values(col: "ColumnOrName", f: Callable[[Column, Column], Column]) -> Column` (23809) | 2 | `(k, v)` |
| 10 | `def map_filter(col: "ColumnOrName", f: Callable[[Column, Column], Column]) -> Column` (23849) | 2 | `(k, v)` predicate |
| 11 | `def map_zip_with(col1: "ColumnOrName", col2: "ColumnOrName", f: Callable[[Column, Column, Column], Column]) -> Column` (23912) | **3** | `(k, v1, v2)` — the only ternary |

`transform` and `filter` are the only two with `@overload` stubs (23343–23351, 23497–23505); `filter` shadows the Python builtin.

`aggregate`/`reduce` branch on `finish`: `[merge, finish]` vs `[merge]` are passed as the `funs` list, so the wire form carries **two lambdas** only when a finish is supplied.

### Return-type rules

1. **transform** → `ArrayType(<lambda body type>, containsNull = <lambda body nullable>)`. Element type comes from the lambda, not the input.
2. **filter** → identical type to the input array (element type unchanged).
3. **exists / forall** → `BooleanType`.
4. **aggregate / reduce** → type of `finish` body if present, else type of `merge` body. Catalyst requires `merge` to return the same type as `initialValue` (docstring: *"returning expression of the same type as `initialValue`"*) — i.e. the accumulator type is fixed by the zero value, and DataFusion-style *cyclic* accumulator inference is not needed for Spark's contract, though Spark does coerce `initialValue` up to the merge output.
5. **zip_with** → `ArrayType(<lambda body type>)`, length = `max(len(left), len(right))`.
6. **transform_keys** → `MapType(<lambda body type>, <original value type>)`.
7. **transform_values** → `MapType(<original key type>, <lambda body type>)`.
8. **map_filter** → identical type to the input map.
9. **map_zip_with** → `MapType(<key type>, <lambda body type>)`.

### Null semantics

Docstring-confirmed, plus Catalyst behavior (the sdist is Python-only, so items marked † are from Catalyst's `higherOrderFunctions.scala` semantics, not quotable from this tree):

- **Null input collection → null result** for all eleven. †
- **transform**: null *elements* are passed into the lambda as null; the result array keeps the same length/offsets. †
- **filter**: predicate returning **null is treated as false** (element dropped). †
- **exists**: three-valued — `true` if any element true; `null` if no element true but some predicate result is null; `false` otherwise (including empty array). Governed by `spark.sql.legacy.followThreeValuedLogicInArrayExists` (default `true`). †
- **forall**: `false` if any element false; else `null` if any null; else `true`. Empty array → `true`. †
- **aggregate/reduce**: a null `initialValue` is legal; nulls flow into `merge` normally; empty array → `initialValue` (then `finish`). †
- **zip_with**: **documented** — *"If one array is shorter, nulls are appended at the end to match the length of the longer array, before applying the function."* The lambda therefore sees explicit nulls for the padded slots (docstring example produces `[foo_1, bar_2, 3]`).
- **transform_keys**: a lambda producing a **null key is a runtime error**; duplicate produced keys are governed by `spark.sql.mapKeyDedupPolicy` (`EXCEPTION` default / `LAST_WIN`). †
- **transform_values**: null values allowed; keys untouched.
- **map_filter**: null predicate → entry dropped. †
- **map_zip_with**: **documented** via Example 3 — key set is the **union** of both maps (map1 order first, then map2-only keys); a key absent from one map yields a **null** for that value argument, and the example output `[('A', 1), ('B', 5), ('C', None)]` shows the lambda receiving `v2 = null`. Duplicate keys in an input map raise. †

---

## 2. Rust inventory — `datafusion-functions-nested-54.1.0`

Path: `<cargo-registry>/datafusion-functions-nested-54.1.0/src/`

**The entire higher-order roster in DataFusion 54.1.0 is three kernels.** `lib.rs:204 all_default_higher_order_functions()` returns exactly:

```rust
array_any_match::array_any_match_higher_order_function(),
array_filter::array_filter_higher_order_function(),
array_transform::array_transform_higher_order_function(),
```

There is **no** `array_all_match`, no reduce/fold, no zip-with, and **no map higher-order function of any kind**. Also checked: `datafusion-spark-54.1.0/src/function/lambda/mod.rs` is an empty stub — `pub fn functions() -> Vec<Arc<ScalarUDF>> { vec![] }` — so the Spark-compat crate contributes zero lambda functions.

### `array_transform.rs` (293 lines)

- Registered as `array_transform`, alias `list_transform`. Expr fn: `array_transform(array: Expr, lambda: Expr) -> Expr`.
- Signature: `HigherOrderSignature::exact(vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())], Volatility::Immutable)` — exactly one value then one lambda.
- `coerce_value_types` → `coerce_single_list_arg`: normalizes `ListView`/`FixedSizeList` → `List`, `LargeListView` → `LargeList`, `Null` → `List(Null)`; anything else is a plan error.
- `lambda_parameters` → `single_list_lambda_parameters`, which returns:
  ```rust
  Ok(LambdaParametersProgress::Complete(vec![vec![Arc::clone(field)]]))
  ```
  **One parameter — the list element field. Nothing else.**
- `return_field_from_args`: builds `List(Field::new(LIST_FIELD_DEFAULT_NAME, lambda.data_type(), lambda.is_nullable()))` (or `LargeList`), nullable = input list nullability. This is exactly Spark's rule #1.
- `invoke_with_args`: flattens the list values, evaluates the lambda once over the flat values array, reassembles with `adjust_offsets_for_slice` and the original null buffer. Captured outer columns are spread with `list_values_row_number` + `take_arrays`.
- Its own tests prove two behaviors that match Spark: the lambda is **not** evaluated on values behind null list rows, and slicing is handled.

**Answer to the specific question: no. `array_transform` does NOT support the two-parameter `(element, index)` form.** It declares exactly one lambda parameter. Passing a 2-param lambda fails at *planning* time in `datafusion-expr/src/higher_order_function.rs:1284`:

```rust
if lambda.params.len() > lambda_params.len() {
    return plan_err!(
        "{} lambda defined {} params ({}), but only {} supported",
        func.name(), lambda.params.len(), display_comma_separated(&lambda.params), lambda_params.len()
    );
}
```

The converse *is* permitted — a kernel may declare more params than a given lambda uses (`std::iter::zip` takes the prefix), and `LambdaArgument::evaluate` takes `&[&dyn Fn() -> Result<ArrayRef>]` closures that are **lazily invoked** (`merge_captures_with_variables` only calls `variables[..params.len()]`). So a single RePark kernel can declare `[element, index]` and serve both Spark arities with zero cost when the index is unused — the index array is never materialized.

### `array_filter.rs` (464 lines)

- Registered as `array_filter`, alias `list_filter`; same `exact(Value, Lambda)` signature, same `coerce_single_list_arg`, same `single_list_lambda_parameters` — **also one-parameter-only, no index**.
- `return_field_from_args`: returns the input list field unchanged (Spark rule #2). ✔
- `invoke_with_args`: evaluates the predicate over flat values; has a **scalar short-circuit** (`x -> true` returns the input array; `x -> false`/`null` returns `empty_filtered_list`, which zeroes offsets while preserving the null bitmap); otherwise downcasts to `BooleanArray` (erroring `"{name} lambda must return boolean"`) and calls `filter_list_values`, which recomputes per-sublist offsets and uses `arrow_filter`.
- Null predicate handling, verbatim from the doc comment: *"Null predicate values are treated as false."* This **matches Spark exactly**.

### `array_any_match.rs` (462 lines)

- Registered as `array_any_match`, aliases `any_match` and `list_any_match`.
- `lambda_parameters` inlines the same single-element-field logic (does not call the shared helper).
- `return_field_from_args` → `Field::new("", DataType::Boolean, list.is_nullable())`.
- Core semantics in `any_match_for_range(predicate, start, end) -> Option<bool>`: `Some(true)` if any true; `None` if none true and any null; `Some(false)` if all false or the range is empty. Row nullability is then `NullBuffer::union(list_array.nulls(), predicate_nulls)`.
- Its own doc string: *"Returns true if one or more elements match, false if none match (including empty arrays), and null if the predicate returns null for some elements and false for all others."*

**This is bit-for-bit Spark `exists` under the default three-valued-logic config.**

### `lambda_utils.rs` (192 lines)

Shared helpers, all `pub(crate)` — **RePark cannot import them and must reimplement**:
- `value_lambda_pair(name, args) -> Result<(&V, &L)>`
- `coerce_single_list_arg(name, arg_types) -> Result<Vec<DataType>>`
- `single_list_lambda_parameters(name, fields) -> Result<LambdaParametersProgress>`
- `enum ListValuesResult { EarlyReturn(ColumnarValue), Values(ArrayRef) }` + `extract_list_values(list_array, return_type)` — the fast paths: all-null input → null scalar; all sublists empty and non-null → default empty-list scalar.
- `mod test_utils` (`create_i32_list`, `eval_hof_on_i32_list`, `v`) — the reusable test harness, also `pub(crate)`.

Roughly **90 lines of the utils are directly re-needed** by any new RePark kernel and must be re-written locally.

### `macros_lambda.rs` (107 lines)

Two macros, both crate-local:
- `make_higher_order_function_expr_and_func!(UDF, EXPR_FN, args..., DOC, HOF_FN [, CTOR])` — emits the fluent `pub fn <expr_fn>(arg…: Expr) -> Expr` returning `Expr::HigherOrderFunction(HigherOrderFunction::new(HOF_FN(), vec![args]))`, plus
- `create_higher_order!(UDF, HOF_FN [, CTOR])` — emits a `LazyLock` singleton `Arc<HigherOrderUDF>` via `HigherOrderUDF::new_from_impl`.

RePark would need a ~40-line local copy of these (or hand-write the two functions per kernel).

---

## 3. `HigherOrderUDFImpl` — full required surface

File: `<cargo-registry>/datafusion-expr-54.1.0/src/higher_order_function.rs` (1684 lines). Trait declared at line 458.

```rust
pub trait HigherOrderUDFImpl: Debug + DynEq + DynHash + Send + Sync + Any {
```

Supertraits are satisfied in practice by `#[derive(Debug, PartialEq, Eq, Hash)]` on the impl struct (all three shipped kernels do exactly that).

### Methods with NO default body — must be implemented (5)

```rust
// 460
fn name(&self) -> &str;
```
Returns the function's canonical name. Used for registry lookup, error text, and `PartialOrd` ordering.

```rust
// 493
fn signature(&self) -> &HigherOrderSignature;
```
Must return a stored `HigherOrderSignature` = `{ type_signature: HigherOrderTypeSignature, volatility: Volatility, lambda_parameters_max_iterations: usize }` (default 256). `HigherOrderTypeSignature` variants: `UserDefined`, `VariadicAny`, `Any(usize)`, `Exact(Vec<ValueOrLambda<(), ()>>)`. All three shipped kernels use `HigherOrderSignature::exact(vec![Value(()), Lambda(())], Volatility::Immutable)`.

```rust
// 658
fn lambda_parameters(
    &self,
    step: usize,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> Result<LambdaParametersProgress>;
```
Must return, **for every lambda argument, the full list of parameter fields that lambda may bind** — the doc is explicit: *"If a lambda support multiple parameters, all should be returned, regardless of whether they are used or not on a particular invocation."* Field **names are ignored**. Called repeatedly with `step` incremented from 0 until `Complete` is returned (capped by `lambda_parameters_max_iterations`, else `plan_err!`):

```rust
pub enum LambdaParametersProgress {
    Partial(Vec<Option<Vec<FieldRef>>>),   // some lambdas' params still unknown
    Complete(Vec<Vec<FieldRef>>),          // all known; not called again
}
```
The `Vec` length must equal the number of lambda args or it is a plan error. `Partial` exists for accumulator-style functions whose lambda param type depends on that lambda's own output — and the doc's **worked example for `Partial` is literally `array_reduce`**, i.e. Spark `aggregate`.

```rust
// 716
fn return_field_from_args(
    &self,
    args: HigherOrderReturnFieldArgs,
) -> Result<FieldRef>;
```
Where:
```rust
pub struct HigherOrderReturnFieldArgs<'a> {
    pub arg_fields: &'a [ValueOrLambda<FieldRef, FieldRef>],
    pub scalar_arguments: &'a [Option<&'a ScalarValue>],
}
```
Crucially, **for a lambda argument `arg_fields` already holds the field of the lambda *body's* output**, evaluated against the params returned by `lambda_parameters`. That is what makes Spark's "element type comes from the lambda" rule expressible. Field name is ignored except inside structured types.

```rust
// 743
fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue>;
```
Where:
```rust
pub struct HigherOrderFunctionArgs {
    pub args: Vec<ValueOrLambda<ColumnarValue, LambdaArgument>>,
    pub arg_fields: Vec<ValueOrLambda<FieldRef, FieldRef>>,
    pub number_rows: usize,
    pub return_field: FieldRef,
    pub config_options: Arc<ConfigOptions>,
}
```
The lambda is invoked via:
```rust
pub fn evaluate(
    &self,
    args: &[&dyn Fn() -> Result<ArrayRef>],
    spread_captures: impl FnOnce(&[ArrayRef]) -> Result<Vec<ArrayRef>>,
) -> Result<ColumnarValue>
```
Two obligations here: (a) the `args` closures must produce arrays matching, positionally, the params returned by `lambda_parameters` — and are **lazy**, so unused params cost nothing; (b) `spread_captures` must reshape captured outer columns (one value per outer row) to the batch shape the kernel actually evaluates over — for list flattening that is `take_arrays(arrays, &list_values_row_number(&list_array)?, None)`. If the lambda captured nothing, `spread_captures` is never called.

### De-facto sixth required method

```rust
// 806
fn coerce_value_types(&self, _arg_types: &[DataType]) -> Result<Vec<DataType>> {
    not_impl_err!("Function {} does not implement coerce_value_types", self.name())
}
```
It *has* a default, but the default is an error. Both `HigherOrderTypeSignature::Exact` and `UserDefined` cause DataFusion to call it, so any kernel using those signatures **must** override it. All three shipped kernels do.

### Methods with usable defaults (may be skipped)

| Method (line) | Default |
|---|---|
| `fn aliases(&self) -> &[String]` (471) | `&[]` |
| `fn schema_name(&self, args: &[Expr]) -> Result<String>` (478) | `"{name}({comma-separated args})"` |
| `fn coerce_values_for_lambdas(&self, _fields: &[ValueOrLambda<DataType, DataType>]) -> Result<Option<Vec<DataType>>>` (685) | `Ok(None)` — needed for accumulator coercion (the `array_reduce` case: coercing an integer `initialValue` up to a float merge output) |
| `fn clear_null_values(&self) -> bool` (729) | `true` — DataFusion pre-cleans non-empty null sublists with `remove_list_null_values` before invoking |
| `fn short_circuits(&self) -> bool` (754) | `false` |
| `fn conditional_arguments<'a>(&self, args: &'a [Expr]) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)>` (777) | derived from `short_circuits()` |
| `fn documentation(&self) -> Option<&Documentation>` (817) | `None` |

**Minimum viable new kernel = 6 methods** (`name`, `signature`, `coerce_value_types`, `lambda_parameters`, `return_field_from_args`, `invoke_with_args`) + `#[derive(Debug, PartialEq, Eq, Hash)]` + a singleton constructor.

---

## 4. Cost anchor — what one higher-order kernel costs

| File | Total lines | Impl (before `#[cfg(test)]`) | Tests |
|---|---|---|---|
| `array_transform.rs` | **293** | 223 | 70 |
| `array_filter.rs` | **464** | 284 | 180 |
| `array_any_match.rs` | 462 | 242 | 220 |

Reading the impl sections: ~17 lines license header, ~35 lines `#[user_doc]`/macro invocation, ~45 lines struct + `new()` + the five trivial trait methods. **Genuinely novel logic is ~80 lines for `array_transform` and ~150 for `array_filter`** (the latter carries the offset-recomputation helper `filter_list_values` and the `empty_filtered_list` short-circuit).

Planning figure: **~250–300 lines of implementation plus 100–200 lines of tests per simple array kernel**, once shared utilities exist. The first RePark kernel additionally pays ~130 lines of one-time infrastructure (local copies of `lambda_utils` + `macros_lambda`, both `pub(crate)` upstream and therefore not importable). `aggregate`/`reduce` is materially more expensive than this anchor — it is the only one needing two lambdas, `Partial` multi-step parameter resolution, `coerce_values_for_lambdas`, and a sequential per-position fold rather than a single flat lambda evaluation; budget 2–3× (`~600–800` lines).

---

## 5. Verdict per function

| # | Spark function | Verdict | Detail |
|---|---|---|---|
| 3 | **`exists`** | ✅ **Pure alias** — `array_any_match` | Only unary lambdas exist in Spark, which is exactly what the kernel supports. Three-valued null logic (`Some(true)` / `None` / `Some(false)`), empty-array→false, null-array→null all match Spark's default `followThreeValuedLogicInArrayExists=true`. Zero Rust. |
| 1 | **`transform`** | ⚠️ **Alias covers the unary form only; the `(x, i)` form needs a new kernel** | `array_transform` declares one lambda param; a 2-param lambda is a hard plan error. Return-type rule already matches Spark. Recommended: one RePark `HigherOrderUDFImpl` declaring `[element, index]` (index closure is lazy → free when unused) and alias Spark `transform` to it wholesale, rather than splitting the two arities across two functions. **~250 lines.** |
| 2 | **`filter`** | ⚠️ **Same as `transform`** | `array_filter` is unary-only. Null-predicate-as-false and the identical-return-type rule already match Spark. Add an index param → **~300 lines** (predicate path is the more complex offset recomputation). |
| 4 | **`forall`** | 🔄 **Neither — expression rewrite, no new kernel** | `forall(a, p)` ≡ `NOT array_any_match(a, x -> NOT p(x))`. Verified against `any_match_for_range` for all four cases: any-`p`-false → any_match true → `false` ✔; none false + some null → `None` → `null` ✔; all true → `Some(false)` → `true` ✔; empty array → `Some(false)` → `true` ✔ (Spark: empty → true); null array → null ✔. RePark owns the lambda body construction, so wrapping in `NOT` is trivial. A dedicated `ArrayAllMatch` kernel (~240 lines, mirroring `array_any_match`) is the fallback if the rewrite proves awkward in the Column builder. |
| 5 | **`aggregate`** | 🔴 **New kernel — the expensive one** | No DataFusion equivalent. Two lambdas (`merge` 2-ary, optional `finish` 1-ary), sequential fold, and it is the *only* one of the eleven needing `LambdaParametersProgress::Partial` + `coerce_values_for_lambdas`. Notably, the DataFusion trait docs use a hypothetical `array_reduce` as the *worked example* for both — the API was designed for this and the kernel simply was not shipped. **~600–800 lines.** |
| 6 | **`reduce`** | ✅ **Alias of #5** | Signature and docstring are byte-identical to `aggregate` in PySpark; Catalyst registers `reduce` as an alias of `ArrayAggregate`. Register as an alias on the same kernel. Zero extra Rust. |
| 7 | **`zip_with`** | 🔴 **New kernel** | Two array values + one 2-ary lambda; requires max-length pairing with **null padding of the shorter array before the lambda runs** (docstring-mandated). No existing kernel takes two value args. Desugaring via `range` + `array_element` + captures is theoretically possible but fragile on the padding semantics and generates a per-row range; not recommended. **~350 lines.** |
| 9 | **`transform_values`** | 🟡 **New kernel preferred; desugaring possible** | DataFusion ships zero map HOFs. Desugarable as `map(map_keys(m), array_transform(map_entries(m), e -> body[k:=e.key, v:=e.value]))` — RePark controls the lambda body so the `(k,v)` → struct-field substitution is mechanical. Risk: `map()`'s null-map-row behavior and its own key rules differ from Spark's. **~300 lines as a kernel.** |
| 8 | **`transform_keys`** | 🟡 **New kernel preferred; desugaring possible** | Mirror of #9: `map(array_transform(map_entries(m), e -> body), map_values(m))`. But Spark's null-key runtime error and `spark.sql.mapKeyDedupPolicy` (EXCEPTION/LAST_WIN) semantics must be enforced explicitly, which pushes toward a real kernel. **~350 lines.** |
| 10 | **`map_filter`** | 🟡 **New kernel preferred** | Desugarable as `array_filter(map_entries(m), e -> pred)` then re-splitting via two `array_transform`s into keys/values and `map(...)` — but that evaluates the entries three times (CSE-dependent) and reads badly. **~350 lines as a kernel.** |
| 11 | **`map_zip_with`** | 🔴 **New kernel** | Ternary lambda `(k, v1, v2)`, two map values, key **union with Spark's specific ordering** (map1 keys in order, then map2-only keys), and null for absent values. Desugaring would need `array_distinct(array_concat(map_keys, map_keys))` (order-preservation not guaranteed) plus per-key `map_extract` inside a lambda; the ordering contract makes this the least desugarable of the eleven. **~450 lines.** |

### Roll-up

- **Free (alias only): 2** — `exists`, `reduce`.
- **Free-ish (expression rewrite): 1** — `forall`.
- **Partly served — existing kernel is correct but arity-deficient: 2** — `transform`, `filter` (unary works today via `array_transform`/`array_filter`; the Spark-mandated index form does not).
- **Requires new `HigherOrderUDFImpl`: 6** — `aggregate`, `zip_with`, `transform_keys`, `transform_values`, `map_filter`, `map_zip_with`.

Rough total new Rust: **~2,700–3,200 impl lines + ~1,200 test lines across 6–8 kernels**, front-loaded with ~130 lines of one-time shared-utility re-implementation (upstream's `lambda_utils`/`macros_lambda` are `pub(crate)` and cannot be imported).

RePark currently contains **zero** higher-order-function plumbing — a repo-wide grep for `HigherOrder|array_transform|array_filter|any_match` hits only `python/repark/src/repark/spark/ml/feature/_transformers.py` and its `map.md`, which are unrelated ML transformers. DataFusion pin is `54.1.0` (`Cargo.toml:88`), matching the crate versions inventoried above, so this is the live surface.