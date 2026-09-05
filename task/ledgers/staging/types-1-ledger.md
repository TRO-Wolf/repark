# Charter ledger — TYPES-1 · the SQL door's Arrow types follow Spark

**Date:** 2026-09-05 · **Branch:** `fix/types-1` · **Base:** `origin/main`
`6eaccd5e` · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `V3-COV-8` (width half) BACKLOG → **FIXED**; `BL-8` BACKLOG → **FIXED**;
`G5-RANK-TYPE-1/2/3` BACKLOG → **FIXED**; `UNIX-1` BACKLOG → **FIXED**; `TY-3` re-measured
residue; residues filed honestly, never absorbed.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Four registry rows pin the same class: the Spark SQL door hands back
DataFusion's integer/unsigned/timestamp types where Spark hands back INT/BIGINT/STRING.
A bare `1` types `Int64` (Spark: INT), so CTAS stores `long` where Spark stores `int`
(`V3-COV-8`, width half); `regr_count`/`approx_distinct` answer `UInt64`, which Spark
reads back from Parquet as `decimal(20,0)` (`BL-8`); `rank()`/`row_number()`/`ntile()`
answer `UInt64` (Spark: INT); `from_unixtime` answers TIMESTAMP (Spark: STRING).

**Not in this unit:** nullability derivation in any form (CUTOVER-SCHEMA-1 settled it on
`main` — `VALUES`/`UNION` nullability residue stays residue); the ANSI door (stock
DataFusion by design); `unix_timestamp` and `to_timestamp` (DATE-FN-1 left them);
public API names (the v1.0 freeze binds); any dependency or lockfile change.

## PROPOSITION LEDGER — TYPES-1 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A bare integer literal in INT range types `Int32` on the Spark door — `SELECT 1`, `VALUES (1)`, CTAS `SELECT 1 AS x`, `df.select(lit(1))`, `withColumn('x', lit(1))` all agree; out of range stays `Int64`; CTAS stores Spark's `int`. | `test_types_1.py` literal section, red on the base; live legs under `REPARK_PARITY_LIVE=1`. | **OPEN** | Base probed 2026-09-05: SQL door `SELECT 1`/`VALUES`/`1+1` are `int64`; facade `lit(1)` is already `int32` (the door the SQL side must join). |
| C-002 | Integer arithmetic follows Spark: INT+INT→INT, INT+BIGINT→BIGINT; overflow errors under ANSI on and wraps under ANSI off, both modes measured on both doors. | `test_types_1.py` arithmetic section + ANSI-mode pins; live legs. | **OPEN** | Base: `i + 1` is `int32` on the SQL door (ExprPlanner), `2147483647 + 1` is `int64` (pure literals skip the overflow rule). |
| C-003 | Count-like aggregates return `BIGINT`/`Int64` on the SQL door: `count(*)`, `count(x)`, `count(DISTINCT x)`, `approx_count_distinct`, `regr_count`, `count_if`; Arrow type AND `collect()` Python type pinned. | `test_types_1.py` aggregate section; `DOOR_RETURNS_UNSIGNED` ratchets to empty. | **OPEN** | Base: `count(*)` already `int64` (DF core returns Int64); `regr_count`/`approx_count_distinct` are `uint64`; SQL-door `count_if` does not resolve (facade has a shim). |
| C-004 | `sum(INT)`/`sum(BIGINT)` → `BIGINT`, `sum(DECIMAL)` → Spark's widened decimal, `bit_length`/`length`/`char_length` → INT, `grouping` → INT — pinned on value, Arrow type, and `collect()` type. | `test_types_1.py` sum/length section. | **OPEN** | Base: `sum(int)` is `int64`, `sum(decimal(10,2))` is `decimal128(20,2)`, `bit_length`/`length`/`char_length`/`grouping` already Spark-equal — pin, do not change. |
| C-005 | `rank()`, `dense_rank()`, `row_number()`, `ntile(n)` → `Int32` on both doors, with and without partitions, and inside a CTAS; `percent_rank`/`cume_dist` → `Float64`. | `test_types_1.py` rank section; window-corpus rank rows flip to equality. | **OPEN** | Base: rank family is `uint64` on the SQL door, `int32` on the facade; `percent_rank`/`cume_dist` already `float64`. |
| C-006 | `from_unixtime` returns session-zone STRING `yyyy-MM-dd HH:mm:ss` with the optional format argument, measured under UTC and a non-UTC session zone; `unix_timestamp`/`to_timestamp` unchanged. | `test_types_1.py` from_unixtime section; UNIX-1 pin flips. | **OPEN** | Base: SQL door returns `timestamp[s]`, facade returns STRING (facade keeps its 1-arg shape; the format argument is new on the SQL door). |
| C-007 | Placement: literal narrowing runs pre-coercion (a `FunctionRewrite`), unsigned casts run post-coercion (an `AnalyzerRule`), both in `repark-functions` and installed by `SparkExtension` only; `EXPLAIN` pins prove the plan carries the casts; no public name changes; nullability untouched. | `test_types_1.py` EXPLAIN pins; ANSI-door control pins stay `int64`/`uint64`-free per stock DataFusion. | **OPEN** | Custom analyzer rules run after `TypeCoercion` (read off DF 54.1 vendored source); coercion upcasts `Int32` into `Exact(Int64)` signatures, so narrowing breaks no signature. |
| C-008 | No regressions: `make verify` green, the full facade suite green with every flipped pin classified (Spark-answer flip with citation, or fixed regression), the cutover battery re-run, mutation score per rule recorded. | The suites; §8. | **OPEN** | Baselines on the unchanged lane 2026-09-05: `make verify` exit 0; facade suite 4765 passed, 208 skipped. |
| C-009 | Docs: every flipped row FIXED with date and unit id, every residue an honest row, crate maps carry the design note and pins line, `STATUS.md` and `briefs/next-sequence.md` untouched. | The gates. | **OPEN** | Rows owned: `V3-COV-8`, `BL-8`, `G5-RANK-TYPE-1/2/3`, `UNIX-1`; neighbours read: `TY-3`, `TY-4`. |

VERDICT: 9 clauses, 0 PROVEN, 9 OPEN, 0 REJECTED.

## 6. What changed

| File | Change |
|---|---|
| `task/ledgers/staging/types-1-ledger.md` | This ledger. |
| `task/ledgers/staging/map.md` | The ledger row (same commit). |

## 7. Design

### 7.1 Where each rule lives

Literals narrow in a `FunctionRewrite` (`datafusion_expr::expr_rewriter::FunctionRewrite`),
which the DF 54.1 analyzer runs before `TypeCoercion` (read off the vendored
`analyzer/mod.rs`: `ApplyFunctionRewrites` chains ahead of every rule). Pre-coercion
placement is what makes `int_col + 1` unify to INT while `CAST(x AS BIGINT) + 1` keeps
its explicit BIGINT — post-coercion the two are indistinguishable. The rewrite mirrors
the facade's `PyColumn::literal` exactly (`i32::try_from`, Int32 on fit, Int64 past it,
`-(2^31)` folds to `Int32::MIN`).

Unsigned results cast in an `AnalyzerRule` after `TypeCoercion` (casts are explicit, so
no re-coercion is needed). It mirrors the facade's `cast_unsigned_count_to_signed`
exactly: probe the UDF return type with `Int64` args, wrap in `CAST AS BIGINT` iff
unsigned. Window UDWFs probe the same way and wrap in `CAST AS INT`. The rule is a
fixpoint under repeated analysis via `transform_down` + `Stop` on already-wrapped casts.

`from_unixtime` is a scalar UDF overwriting DF core's (later registration wins),
reusing the `date_format` Java-pattern compiler and the session-zone carrier.
`count_if` is a one-arg boolean aggregate UDF returning `Int64` (the SQL door lacks
the name entirely; the facade's shim already answers `Int64`).

### 7.2 Signatures surveyed, not assumed

`coerced_from` in `datafusion-expr` 54.1 coerces `Int32` into `Exact(Int64)` (widening
only), so narrowing breaks no `Exact` signature; `date_add` (`Exact(Date32, Int32)`,
no `Int64` arm) is unplannable with an `Int64` literal on the base and becomes
plannable after narrowing. `ntile` accepts every int width; `lead`/`lag` are `Any`;
`length` (datafusion-spark) already returns `Int32`.

## 8. Mutations

Recorded here as run (one rule disabled at a time; the pins each reds).

## 9. Live oracle

Banner, zone, and per-shape readings recorded here when a JVM slot frees (two sibling
lanes hold one each as of 2026-09-05; this lane polls with `pgrep` and never starts a
second JVM).
