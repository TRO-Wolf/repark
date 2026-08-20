# DESIGN — the Spark function parity campaign

**Settled 2026-08-20.** The campaign after V2 Engine Hardening's function waves: close the
`pyspark.sql.functions` gap, and move the semantics behind every name out of Python and into
Rust. Status while it runs lives in [STATUS.md](../../STATUS.md); the execution slate is
[briefs/spark-function-parity.md](../../briefs/spark-function-parity.md); the approval gate is
`task/fnp-0-charter-ledger.md`.

Owner decisions taken at kickoff (2026-08-20): **one pull request** for the campaign; the lambda
seam and the SQL-door dialect land as **one unit**; "Rust-owned" means **owning the semantics**,
not merely avoiding row compute; the coverage target is **everything reachable without a JVM**.

## 1. Goal and definition of done

RePark's PySpark facade is a real drop-in for `pyspark.sql.functions`, and the semantics behind
every name are RePark's own — in Rust, on the engine, not assembled in Python.

Three things are true at close, each checkable:

1. **Nothing is silently absent.** Every name PySpark 4.1.2 exports, RePark exports — working, or
   raising `UnsupportedOperationException` that names a divergence-registry section. The set
   difference is empty and a test asserts it.
2. **Python decides nothing semantic.** No facade function builds SQL text, composes two or more
   engine calls, or branches on argument shape to pick a different engine expression.
3. **Lambdas work.** All eleven Spark higher-order functions, through the Column API with a Python
   lambda and through the Spark SQL door with `x -> y`, on every lambda arity Spark accepts.

What this is not: a rewrite of DataFusion, a fork of it, or a change to the native/ANSI door. The
native door's dialect, semantics and tests are out of scope and are protected by a clause.

## 2. Ground truth (measured 2026-08-20)

Evidence: [task/fnp-0-census/](../../task/fnp-0-census/map.md) — a nine-agent read-only census of
the facade surface, the PySpark gap, the lambda machinery and the kernel ownership map. Every
number below is measured, not estimated.

### 2.1 The declared surface

| Quantity | Measured |
|---|---|
| PySpark 4.1.2 `__all__` | **506** names (one `__all__`, AST-parsed) |
| RePark `repark.spark.functions.__all__` | **333** names |
| Absent from RePark entirely | **181** |
| Exported but raising unconditionally | **35** |
| Exported but raising on some argument paths | **7** |
| **Real gap** | **216** — so RePark meets ~**57%** of the declared surface, not the 66% a raw `__all__` diff suggests |

### 2.2 How the 333 exported names reach the engine

| Class | n | Compliant with §4.1? |
|---|---|---|
| `THIN_SCALAR` — one `_scalar(name, …)` call | 126 | yes |
| `ALIAS` — module-level binding to another name | 33 | yes |
| `COLUMN_METHOD` — delegates to a `Column` method | 29 | yes |
| `NATIVE_DIRECT` — calls a `PyColumn` constructor | 27 | yes |
| `REFUSE` / `CONDITIONAL_REFUSE` | 35 / 7 | n/a — stubs |
| `OTHER` | 16 | reviewed per name |
| **`PY_COMPOSED`** — assembles from 2+ engine calls or branches on argument shape | **47** | **no** |
| **`SQL_STRING`** — builds SQL text for the engine to parse | **6** | **no** |
| **`PY_COMPUTE`** — computes values in Python | **2** | sanctioned exception (`udf`, `pandas_udf`) |

**55 functions are non-compliant** and are the repatriation target. The remaining 5 exported names
(`PandasUDFType`, `PythonUDFColumn`, `UserDefinedFunction`, `UserDefinedTableFunction`, `udtf`)
are classes and decorators, not functions, and are out of scope for §4.

The rule *"Python builds plans; Python never touches rows"* **holds today**: the only `PY_COMPUTE`
rows are the sanctioned user-UDF path. The gap is not row compute — it is ownership of semantics.

### 2.3 Kernel ownership

**435 distinct callable spellings** are reachable on the Spark door:

| Provenance | n |
|---|---|
| `REPARK_OWNED` | 123 — 39 `repark-functions` scalar spellings, 3 aggregate, 81 `repark-ta` window UDFs |
| `DATAFUSION_SPARK` | 73 registered and surviving (14 overwritten by RePark shims) |
| `DATAFUSION_CORE` | 239 registered and surviving (30 overwritten) |

`repark-spark` owns **zero** kernels — it owns statement-level surface. Adding a kernel never
touches it.

**Cost anchor:** `crates/repark-functions/src` is 15,132 lines, 5,110 of them tests (33.8%). The
median new kernel is **~287 lines fully loaded**, about a third of it tests. A thin wrapper over an
upstream kernel is 185–263 lines; a re-kernel from the Spark/Java source of truth (the
`java.net.URI` port, the four-decimal-width aggregate, session-zone localization) is 450–1,300.

### 2.4 The finding that reframes the campaign — the two-door asymmetry

**Nineteen names resolve to a different kernel depending on which door you enter through.** The
facade's `call_scalar_expr` table lowers them to DataFusion-core while the SQL door resolves the
`datafusion-spark` or RePark shim.

Seventeen are latent (`ascii`, `base64`, `unbase64`, `ceil`/`ceiling`, `floor`, `round`, `length`,
`like`, `ilike`, `elt`, `size`/`cardinality`, `sec`, `csc`, `slice`, `array_repeat`,
`array_contains`, `date_part`). **Two are semantically live:**

- **`to_timestamp`** — the facade calls DF-core `expr_fn::to_timestamp`, bypassing
  `instant_ts::SparkToTimestamp` and therefore the TZ-4 PR-1 `Timestamp(µs, UTC)` typing and PR-2
  session-zone localization. `F.to_timestamp(x)` and `spark.sql("SELECT to_timestamp(x)")` are not
  the same kernel. Adjacent to the open TZ-4 / TZ-7 rows.
- **`avg`** — the facade's `unary_aggregate_udaf("avg")` returns DF-core `avg_udaf`, bypassing
  `SparkAvgWithRetract`. The delta is narrower than it looks (DF-core `Avg` already carries the
  `(p+4, s+4)` decimal rule) but the Spark i64-count / null-on-empty arm is not on the facade path.
  This is also the kernel behind FLOAT-AGG-2.

FN-GT1/GT2 deliberately closed this seam for sixteen names under the policy *"both doors resolve
the same UDF."* `to_timestamp` and `avg` are the two remaining holes in a policy the project
already adopted. This is a silent-wrong-results class and it is the campaign's first correctness
unit.

*Correction (2026-08-20, FNP-1).* Two claims above were measured wrong by the census and are
corrected here rather than rewritten away. **The latent set is 18, not 19** — `cardinality`
already resolved the same kernel on both doors, caught by the ratchet test on its first run. And
**`avg` is behaviourally latent, not semantically live**: no input was found on which DF-core
`Avg` and `SparkAvgWithRetract` disagree (all-null, empty frame, and the sliding-window retract
path all agree), so its fix rests on the two-doors-one-kernel policy rather than on a demonstrated
wrong answer. `to_timestamp` is live as described and was proven so both ways. Detail:
[task/fnp-1-two-door-asymmetry-ledger.md](../../task/fnp-1-two-door-asymmetry-ledger.md).

## 3. The higher-order (lambda) seam

### 3.1 What was actually wrong

Not a missing engine capability, and not a RePark defect. DataFusion 54.1 ships a complete
higher-order function machinery — `Expr::HigherOrderFunction`, `Expr::Lambda`,
`Expr::LambdaVariable`, the `HigherOrderUDFImpl` trait, a registry on `SessionState` separate
from scalar UDFs, and three working kernels (`array_transform`, `array_filter`,
`array_any_match`). A stock session has all three registered.

The SQL front end never reaches them because `Dialect::supports_lambda_functions()` defaults to
`false` (`sqlparser-0.62.0/src/dialect/mod.rs:520`) and DataFusion parses with `Dialect::Generic`.
`x -> x + 1` therefore parses as the PostgreSQL JSON arrow operator:

```text
BinaryOp { left: Identifier(x), op: Arrow, right: BinaryOp { left: Identifier(x), .. } }
```

so `x` resolves as a column and planning fails with `FieldNotFound { name: "x" }`. Measured
identically on a bare `SessionContext::new()` and on a RePark session.

The facade never reaches them either, for an unrelated reason: `_scalar` → `PyColumn.call_scalar`
carries a name and a `Vec<Expr>`, and a lambda is not an `Expr` argument the caller can supply —
it is a *body* that must be built against a synthetic parameter.

Two independent causes, two independent fixes.

### 3.2 The facade path — expr API, no parser involved (MEASURED)

`F.transform(col, lambda x: x + 1)` never needs SQL. The whole construction is public expr API:

```rust
Expr::HigherOrderFunction(HigherOrderFunction::new(
    array_transform_udf,
    vec![col("arr"), lambda(["x"], lambda_var("x") + lit(1))],
))
```

Variables built this way are unresolved (`LambdaVariable::field == None`) and must be resolved
against the input schema before the plan is built — `Expr::resolve_lambda_variables(&schema)`.

Measured end to end on a real frame (spike, 2026-08-20, reverted): input `[1,2,3]` / `[10,20]`
→ `resolve_lambda_variables => OK` → `select().collect()` → `[2,3,4]` / `[11,21]`.

The Python side becomes: mint a placeholder `Column` for each lambda parameter, call the user's
Python callable with it, take the returned `Column`'s inner `Expr` as the body, and hand
`(function name, value args, parameter names, body)` to a new binding entry point. Python decides
nothing semantic — it binds names to positions, which is the callable's own signature.

### 3.3 The SQL path — dialect per door, never per session (MEASURED)

`SessionState::sql_to_statement(sql, &Dialect)` and `sql_to_expr_with_alias(sql, &Dialect)` take
the dialect **as a parameter**. `create_logical_expr_from_sql_expr(sql_expr, df_schema)` is public
and takes an already-parsed expression. So the Spark door can parse Spark without the session
config changing at all.

That distinction is load-bearing. Setting `sql_parser.dialect = Databricks` session-wide was
measured against the workspace suite and **regresses the native door**:

```text
ParserError("Expected: joined table, found: { at Line: 1, Column: 36")
```

on `SELECT {'f1': value, 'f2': value} AS s …` — DuckDB-style struct literals that `Generic`
accepts and `Databricks` does not. Two `repark-core` DEFECT-2 guard tests fail on it.

A session-wide flip also blends the two doors, which
[ADR-0002](../adr/0002-two-sql-doors.md) forbids in as many words: *two honest SQL doors, no
blended parser*. The native door is ANSI/Trino-flavoured and stays that way.

So: the Spark door names its dialect at its own parse sites; the native door is not touched; the
session config option keeps its current value and stops being the thing that decides.

Sites that must name the Spark dialect (all facade-reachable):
- `repark-spark::spark_ast::execute_passthrough` — `spark.sql(...)`
- the facade's SQL-expression entry points — `F.expr(...)`, `DataFrame.filter("...")` — which
  today reach `DataFrame::parse_sql_expr` and therefore the session config

Once the parser emits `SQLExpr::Lambda`, nothing else is needed: `datafusion-sql`'s function
planner (`expr/function.rs`) already lowers it to `Expr::Lambda` + `HigherOrderFunction` with
variables **already resolved**, so the SQL path skips the resolution step the expr path needs.

### 3.4 Registration and Spark spellings

`SessionContext::register_higher_order_function(Arc<HigherOrderUDF>)` is public, and
`HigherOrderUDF::with_aliases` attaches alternate names. `repark-functions::register_all` gains a
higher-order loop beside its existing UDF/UDAF/UDWF loops, with the same overwrite-by-name
property: DataFusion defaults first, RePark last, RePark wins.

Spark spellings map onto DataFusion kernels by alias where the semantics match
(`transform`→`array_transform`, `filter`→`array_filter`, `exists`→`array_any_match`) — measured
working last session. Where they do not match, the alias is not used and a RePark kernel owns the
name outright.

Because `PyColumn` builds expressions without a session in hand, the binding needs a
session-independent name → `Arc<HigherOrderUDF>` lookup. `repark-functions` exposes one, and the
session registry is populated from the same table, so the two cannot drift.

### 3.5 What each of the eleven actually costs (measured)

DataFusion 54.1's entire higher-order roster is three kernels, and `datafusion-spark`'s
`function/lambda/mod.rs` is an empty `vec![]`. Per-function verdict:

| Spark name | Verdict | Cost |
|---|---|---|
| `exists` | **Pure alias** of `array_any_match` — three-valued null logic, empty→false and null-array→null all already match Spark's default | 0 lines |
| `reduce` | **Alias** of `aggregate` (Catalyst registers it as an alias of `ArrayAggregate`; the PySpark signatures are byte-identical) | 0 lines |
| `forall` | **Expression rewrite** — `NOT array_any_match(a, x -> NOT p(x))`, verified against `any_match_for_range` on all five cases including empty-array→true and null-array→null | ~0 lines |
| `transform` | **New kernel.** `array_transform` declares one lambda parameter; Spark's `(element, index)` form is a hard plan error against it | ~250 |
| `filter` | **New kernel**, same reason | ~300 |
| `aggregate` | **New kernel** — the expensive one. Two lambdas (2-ary merge, optional 1-ary finish), sequential fold, and the only one of the eleven needing `LambdaParametersProgress::Partial` + `coerce_values_for_lambdas` | ~600–800 |
| `zip_with` | **New kernel** — two array values, 2-ary lambda, null-padding of the shorter array before the lambda runs | ~350 |
| `transform_keys` | **New kernel** — Spark's null-key runtime error and `spark.sql.mapKeyDedupPolicy` must be enforced explicitly | ~350 |
| `transform_values` | **New kernel** | ~300 |
| `map_filter` | **New kernel** | ~350 |
| `map_zip_with` | **New kernel** — ternary lambda, key union in Spark's specific order (map1 keys, then map2-only keys) | ~450 |

Roll-up: **2 free by alias, 1 free by rewrite, 8 new `HigherOrderUDFImpl` kernels** —
~2,700–3,200 impl lines plus ~1,200 test lines, front-loaded with ~130 lines of one-time shared
utilities (upstream's `lambda_utils` / `macros_lambda` are `pub(crate)` and cannot be imported).

One mechanism makes `transform` and `filter` cheap: **a kernel may declare more lambda parameters
than a given call uses.** `LambdaArgument::evaluate` takes `&[&dyn Fn() -> Result<ArrayRef>]`
closures and `merge_captures_with_variables` only invokes `variables[..params.len()]`, so a single
RePark kernel declaring `[element, index]` serves both Spark arities and never materializes the
index array when the lambda does not take one. One kernel per name, both arities, no cost when
unused.
## 4. Repatriating the semantics

### 4.1 What "owned in Rust" means, negatively

A facade function is compliant when its Python body contains none of:

- **SQL text construction** — an f-string or concatenation handed to the engine to parse. The
  weakest tier: the semantics live in a string, the parser owns them, and nothing type-checks.
- **Multi-call composition** — assembling the result from two or more engine calls. Every such
  body is a small unwritten kernel, and its Spark fidelity is asserted by whoever wrote the
  composition rather than pinned by a kernel that owns the name.
- **Semantic branching** — choosing a different engine expression based on an argument's type,
  arity, or value. This is the `lit_indices` family: a pure-Python decision about whether an
  argument is a column reference or a literal, which is a *semantic* decision about the function.

Compliant and untouched: type gates that only raise, arity validation that only raises, docstrings,
and passing arguments through. The user-supplied UDF path (`udf`, `pandas_udf`) is the sanctioned
exception and does not move.

### 4.2 Why these three, and why now

The GT1-FIX sweep already showed what the `lit_indices` tier costs: 38 wrappers had the wrong
literal/column decision, and every one of them was a silent wrong answer rather than an error.
That failure class — a Python-side decision that looks like plumbing and is actually semantics —
is exactly what moving the decision into the kernel eliminates. A kernel that owns its own
signature cannot disagree with itself about which argument is a pattern.

### 4.3 The shape of the move

Per compliant-in-Rust function the change is the same three steps:

1. The name gets a dispatch arm that owns its full signature — including which arguments are
   literals — so the Python call is `name` plus positional arguments and nothing else.
2. Where the composition was not expressible as an existing DataFusion expression, the kernel is
   written in `repark-functions` against `ScalarUDFImpl`, registered by `register_all`, and wins
   the name by the existing overwrite-by-name rule.
3. The Python body collapses to the thin-wrapper shape the facade already uses for the majority
   of its names.

The dispatch table grows past the 1500-line file ceiling in the process and splits into
`crates/repark-python/src/column/dispatch/`, one module per family, plain `mod` declarations — no
`#[path]`, per AGENTS.md.

### 4.4 The 55 names

`PY_COMPOSED` (47) —
`abs`, `array_append`, `array_prepend`, `array_sort`, `arrays_overlap`, `bin`, `broadcast`,
`btrim`, `cbrt`, `count`, `count_distinct`, `count_if`, `date_from_unix_date`, `date_sub`,
`dayname`, `degrees`, `e`, `expm1`, `isnotnull`, `left`, `lit`, `log1p`, `log2`,
`make_dt_interval`, `make_interval`, `map_contains_key`, `monthname`, `nullif`, `nullifzero`,
`nvl2`, `overlay`, `parse_url`, `pmod`, `quote`, `radians`, `rand`, `randn`, `replace`, `right`,
`rint`, `sequence`, `shuffle`, `str_to_map`, `try_parse_url`, `unix_millis`, `unix_seconds`,
`zeroifnull`.

`SQL_STRING` (6) — `days`, `hours`, `months`, `pi`, `uuid`, `years`. The four partition transforms build a
`transform(source)` SQL fragment for `partitionedBy`; `pi` and `uuid` are SQL-text constants.

`PY_COMPUTE` (2) — `pandas_udf`, `udf`. Sanctioned; unchanged.

Two of these carry named divergence pins already worth calling out:

- **`abs`** is not an engine `abs` at all. Python builds `when(col < 0, lit(0) - col).otherwise(col)`
  — a `CASE` expression — then **overwrites the display and SQL text to read `abs(child)`**. The
  native plan and the SQL text are different expressions with only presumed-equal semantics; they
  differ at minimum on `MIN_VALUE` integer overflow. The docstring records the choice as
  deliberate ("no new Rust"). It is the clearest single argument for this campaign.
- **`concat`** implements Spark's any-null→NULL rule **three times**: once in Rust
  (`PyColumn::concat`, a `Case` + `Cast` to `Utf8`), once again as Python-built SQL text for
  `sql_expr`, and a third spelling in `join_sql_expr` that carries **no null guard at all** — so a
  join `ON` embedding `concat` gets DataFusion skip-null semantics instead of Spark's.

### 4.5 Explicitly out of this scope

`lit_indices` — the Python-side declaration of which argument positions are literals — **stays in
Python** under the owner's chosen scope. It is worth recording that this is where FN-GT1 found 38
wrong wrappers, each a silent wrong answer rather than an error, and that 27 exported names carry
one today. Moving the declaration next to the kernel would remove the failure class. Raised here,
not acted on; it needs its own owner decision.
## 5. Decisions (dated)

**D-1 (2026-08-20) — The Spark dialect is named per door, never set on the session.**
Measured: `sql_parser.dialect = Databricks` session-wide fails 8 workspace tests — 2 in
`repark-core` (DuckDB struct literals `{'f1': …}` that `Generic` accepts and `Databricks`
rejects) and 6 in `repark-sql/tests/cross_door.rs`, the suite whose entire job is asserting the
two doors agree. `sql_to_statement` / `sql_to_expr_with_alias` take the dialect as a parameter,
so naming it at the Spark door's own parse sites costs nothing and leaves the native door
untouched. Consistent with ADR-0002 ("no blended parser"). The session config option keeps its
current value.

**D-2 (2026-08-20) — Higher-order kernels RePark cannot alias are RePark-owned, written against
`HigherOrderUDFImpl`.** This is not a DataFusion fork and does not touch ADR-0001: the ADR's own
boundary rule puts "anything engine-flavored (SQL dialects, function semantics, session policy,
facades)" in RePark. A `HigherOrderUDFImpl` is an ordinary upstream extension point, the same
shape as the ~40 scalar kernels `repark-functions` already owns.

**D-3 (2026-08-20) — Spark spellings arrive by alias only where semantics match.** Where Spark's
`transform` / `filter` / `exists` agree with DataFusion's `array_transform` / `array_filter` /
`array_any_match`, an alias is the whole implementation. Where they diverge — including Spark's
two-parameter `(element, index)` lambda forms — the alias is dropped and a RePark kernel owns the
name outright. A divergence absorbed silently behind an alias is worse than no alias.

**D-4 (2026-08-20) — "Rust-owned semantics" is defined negatively, so it is checkable.** A facade
function is compliant when its Python body contains: no SQL text construction, no composition of
two or more engine calls, and no branch on argument type / arity / value that selects a different
engine expression. Type gates, arity validation that only raises, and passing arguments through
are compliant. The user-supplied UDF path (`udf`, `pandas_udf`) is the one sanctioned exception
and stays as it is.

**D-5 (2026-08-20) — Nothing is silently absent at close.** Every name in PySpark 4.1.2's
`__all__` is exported: working, or raising `UnsupportedOperationException` with a matching
divergence-registry section. `dir(repark.spark.functions)` becoming a superset-equal of PySpark's
is a checkable clause, and it converts every remaining gap into a disclosed one.

**D-6 (2026-08-20) — File-size ceilings are met by splitting into the canonical module tree, never
by raising a ceiling.** `function_dispatch.rs` is at 906 of a 1500-line default and cannot absorb
the new arms; it becomes `column/dispatch/` with one module per family. Per AGENTS.md the split
uses plain `mod`/directory layout — no `#[path]`.

**D-7 (2026-08-20) — The four sub-project families are declared, collation and crypto are built.**
Owner ruling; the full text and the reasoning are §8. It closes charter clause C-007 and sets the
actionable target at 160 names. Note what it does in *both* directions: it declines four builds,
and it resolves registry row G15's held implement-or-keep-absent decision as **implement**, which
retires a DECLARED divergence rather than adding one.

## 6. Risks

**R-1 — One PR, very large diff.** The owner chose a single PR. Mitigation: units land as
separate commits on one branch, each with its own `task/` ledger and its own Actor–Critic cycle,
so review can proceed commit by commit and a defect in one unit is attributable. This does not
remove the risk that a late red gate blocks everything; it makes the blockage locatable.

**R-2 — The dialect change is still a parser change for the Spark door.** Even scoped to one
door, Databricks differs from Generic on backtick identifiers, struct literals, `!` as NOT, and
`->` no longer being the JSON arrow. Every Spark-door suite and the facade cohort run against it
before the unit converges, and any behaviour that moves is either fixed or gets a registry row.
Registry row BL-2 (backtick-quoted identifiers in a filter string) is adjacent and is re-checked
in the same unit.

**R-3 — Repatriating semantics can change behaviour silently.** Moving a decision from Python to
Rust is exactly the failure class the binding manifest calls `s0_fresh_execution`: engine-vs-
reference divergence that a passing test suite can sit on top of. Each repatriated function is
pinned on the Arrow path (value AND type) per the entry-point matrix before its unit converges,
and the Critic executes at least one novel input per unit through the public surface.

**R-4 — JVM-only classification is a judgement that can be wrong in our favour.** Declaring a
function unreachable is the cheap way out of implementing it. The partition is an artifact
attacked at the gate, and each JVM-only claim carries the specific mechanism (Java hash code,
JVM regex dialect, Java locale data) rather than the word "JVM".

**R-5 — Four `#[path]` module inclusions predate the rule that now forbids them.**
`repark-functions/src/url.rs` includes `java_uri.rs`, and `collection.rs` includes `str_to_map.rs`,
`shuffle.rs` and `map_from_entries.rs`, all via `#[path]`. AGENTS.md now requires the canonical
module tree. This campaign adds eight kernels to the same crate, so the conversion lands here
rather than accruing. (The two remaining `#[path]` sites, in `repark-iceberg`'s
`predicate_dml.rs`, are test-file inclusions and are the documented exception.)

## 7. The plan — seventeen units, one branch, one PR

Ordering weighs three things: is the kernel already compiled into the shipping wheel, how many
census rows flip out of the gap bucket, and does the work reuse a seam RePark already owns.

| Unit | Scope | n | Why here |
|---|---|---|---|
| **FNP-1** | **Two-door asymmetry.** Close `to_timestamp` and `avg`; audit the other 17 latent names and close or register each. | 19 | Correctness before features. A live silent-wrong-results class, and the project already adopted the "both doors resolve the same UDF" policy. |
| **FNP-2** | **Free names.** `asc_nulls_last`, `desc_nulls_first`, `column`, `negate`, `session_user`. | 5 | Closes the 2-of-4 `asc_nulls_*`/`desc_nulls_*` asymmetry. *Corrected 2026-08-20:* `sha` moved to FNP-3 (it aliases `sha1`, which is still a stub) and `typeof` to FNP-12 (no kernel exists, and a standalone `Column` has no schema to read) — and the unit was not free after all: the two new corners exposed a live ordering defect. See [../task/fnp-2-free-names-ledger.md](../../task/fnp-2-free-names-ledger.md). |
| **FNP-3** | **De-stub what already ships.** `sha` lands here with `sha1`. 14 of the 35 raising stubs have an exact or near kernel already linked into the wheel: `crc32`, `format_string`, `from_utc_timestamp`, `json_tuple`, `map_from_arrays`, `sha1`, `soundex`, `to_utc_timestamp`, `xxhash64`, `arrays_zip`, plus `datediff` (arg-order over the existing `date_diff`), `regexp_extract`, `try_to_timestamp`, `unix_timestamp`. | 14 | These are `split`, `hash`, `regexp_extract` — names real PySpark code uses constantly. Fixing a loud stub on `split` buys more than adding `theta_intersection_agg`. |
| **FNP-4a** | **The higher-order seam.** The registry both doors read, `PyColumn::lambda_variable` / `call_higher_order`, lambda-variable resolution at every site that hands a column to DataFusion, and `exists` — the one Spark higher-order function needing no new kernel. | 1 | Proves the whole pipeline on the cheapest function before any kernel is written. |
| **FNP-4b** | **The Spark-door dialect**, and making the engine's own generated SQL dialect-independent. *Split out 2026-08-20 on owner ruling* — the dialect works (all three DataFusion kernels become SQL-reachable) but costs 5 cross-door DML tests, because `write/idents.rs::quote_ident_spark` emits ANSI double quotes that a Spark dialect reads as string literals. That is a write-path change and is judged on its own evidence. Adjacent to registry row BL-2, which understates the scope. | — | Measured evidence in [../../task/fnp-4a-lambda-seam-ledger.md](../../task/fnp-4a-lambda-seam-ledger.md). |
| **FNP-4c** | **The eight new kernels**, plus `forall` (an `array_any_match` rewrite) and `reduce` (an alias of `aggregate`). Per §3.5: one RePark kernel per name declaring `[element, index]`, so both Spark arities are served and the index costs nothing when unused. | 10 | The bulk of the lambda work; ~2,700–3,200 impl lines. |
| **FNP-5** | **Wire-only aggregates.** The nine `regr_*`, `sum_distinct`, `grouping`, `approx_count_distinct`, `listagg`, `listagg_distinct`, `string_agg`, `string_agg_distinct`. | 16 | All already in `all_default_aggregate_functions()` and registered on every session. Sixteen names for roughly the cost of one. |
| **FNP-6** | **Reuse RePark's own kernels.** `regexp_extract_all`, `regexp_substr`, `randstr`, `uniform`, `validate_utf8`, `try_validate_utf8`, the three `bitmap_*_agg`, `assert_true`. | 10 | The hard semantics — Java regex dialect, Spark's PRNG, the bitmap layout — were already paid for. |
| **FNP-7** | **The `try_*` sweep.** | 12 | One systematic pass over the ANSI seam RePark already owns (`SparkAnsiConfig`, DEC-6/DEC-7). One rule, twelve names, one ledger — not twelve builds. |
| **FNP-8** | **Repatriation.** The 55 non-compliant functions of §4. | 55 | The owner's "own the semantics" decision. Sized against the ~287-line median kernel. |
| **FNP-9** | **Collections, generators, dispatch.** `create_map`, `map_concat`, `array_insert`, `inline`, `inline_outer`, `stack`, `call_udf`, `call_function`. | 8 | `inline` is `dynamicFlatten` machinery that already shipped in v0.5.0. |
| **FNP-10** | **JSON.** `from_json`, `to_json`, `get_json_object`, `json_array_length`, `json_object_keys`, plus the stubbed `schema_of_json`. | 6 | `serde_json` is already a workspace dep and `types.py` already parses DDL schemas. A real build, high user value. |
| **FNP-11** | **Timestamp / TIME.** | 18 | Half the kernels exist and RePark owns `TimeType` and the session-zone carrier — but the family is entangled with the open TZ-4 residues and TZ-6/TZ-7/TZ-8, so it needs a design pass, not a wiring pass. |
| **FNP-12** | **Remaining aggregates + numeric formatting.** `max_by`, `min_by`, `product`, `grouping_id`, `any_value`, `percentile`, `histogram_numeric`, `count_min_sketch`; `to_char`, `to_varchar`, `to_number`, `to_binary`, `conv`, `bround`, `typeof`. | 15 | DataFusion's `to_char` is a false friend — its own doc says numeric formatting is unsupported, while Spark's is numeric. Do not reuse it. |
| **FNP-13** | **Collation (D-7).** `collate`, `collation`, and the G15 retirement: every refusal site G15 armed — `repark-spark/src/collation.rs`, the type-position guard, `DataFrame.filter_sql`, the facade `AttributeError`s — comes down as real support lands. Registry row G15 moves DECLARED → FIXED. | 2 + G15 | Reaches much further than two names. Needs its own design note before it opens; it is the only unit that retires a declared divergence. |
| **FNP-14** | **Crypto and masking (D-7).** `aes_encrypt`, `aes_decrypt`, `try_aes_decrypt`, `mask`. | 4 | Introduces one new workspace dependency (a cipher crate). The crate choice is a unit decision, recorded in the ledger, and passes `cargo-deny` and `make audit` like any other. `mask` ships without it. |
| **FNP-15** | **Unreachable — register, do not build.** `java_method`, `reflect`, `try_reflect`, `unwrap_udt`; plus `input_file_block_start` / `input_file_block_length` until `input_file_name` is itself de-stubbed. | 6 | A divergence-registry section each with the specific mechanism, plus a loud-refusal pin. |
| **FNP-16** | **Declared families — register (D-7).** The 56 names of §8: sketches (32), CSV/XML/XPath (11), VARIANT (8), geospatial (5). Refusing stub plus a registry section per family. | 56 | Same mechanism as FNP-15, different reason — these are reachable and deferred by cost, not unreachable. The registry language must say so; conflating the two would misreport the engine's limits. |
| **FNP-Z** | **Close-out.** `__all__` completion so C-009 holds, census re-run, STATUS truth-up, the `#[path]` conversion (R-5), the dispatch-table module split (D-6). | — | The unit that owns the closed paths. |

## 8. D-7 — the sub-project families (owner ruling, 2026-08-20)

Fifty-six names are reachable without a JVM but each family behind them is a sub-project. They
are **declared absent-and-loud** under the G15 pattern — a divergence-registry section and a
refusing stub per name — which satisfies "nothing is silently absent" without four sub-projects
and brings the actionable target from 216 to **160**.

| Family | n | Why declared, not built |
|---|---|---|
| Sketches (HLL / theta / KLL) | 32 | Needs a byte-compatible Rust port of Apache DataSketches. DataFusion's internal `hyperloglog.rs` is a different format and cannot serve the blob even for the HLL subset. 18% of the headline gap for close to the least value per name. |
| CSV / XML / XPath | 11 | The nine `xpath_*` need an XPath 1.0 engine matching `javax.xml.xpath`. No crate vendored. `datafusion-spark`'s `csv` and `xml` modules are both empty `vec![]`. |
| VARIANT | 8 | Spark's VARIANT is a specific value/metadata binary encoding; nothing in the registry implements it. RePark has a `VariantType` shell with nothing behind it. |
| Geospatial | 5 | GEOGRAPHY/GEOMETRY have no Arrow representation and no vendored WKB codec. Effectively its own sub-project. |

**Two families the same ruling puts IN scope, and they are built:**

- **Collation** (`collate`, `collation`) — this was blocked by policy, not capability. **Registry
  row G15's implement-or-keep-absent decision is hereby resolved as implement**, so G15 moves from
  DECLARED to a fix and the loud refusals across `collation.rs`, the type-position guard and the
  facade come down as real support lands. This is the one ruling in the campaign that *retires* a
  declared divergence rather than adding one, and it reaches further than two function names —
  every refusal site that G15 armed is in scope.
- **Crypto and masking** (`aes_encrypt`, `aes_decrypt`, `try_aes_decrypt`, `mask`) — a cipher
  crate is accepted as a new workspace dependency. It enters through `cargo-deny` and the audit
  gate like any other, and the choice of crate is a unit decision recorded in the FNP ledger, not
  a design-time pick. `mask` is trivial string work and does not need the dependency.

## 9. Non-goals

- The native/ANSI door. Its dialect, semantics and tests do not change; a clause protects them.
- Forking DataFusion. Owning a `HigherOrderUDFImpl` is an upstream extension point, not a fork.
  ADR-0001's boundary rule already places function semantics in RePark.
- Raising any file-size ceiling. Growth is met by splitting into the canonical module tree.
- `lit_indices` repatriation (§4.5) — recorded, not scoped.
