# TZ-5 — `CAST(TIMESTAMP AS <numeric>)` returns epoch SECONDS

Unit ledger. **Base:** `origin/main` @ `9acb566` (frozen). **Branch:**
`hardening/tz5-timestamp-cast-seconds`. **Date:** 2026-08-11/12.

The unit closes divergence-registry row **TZ-5**: repark returned epoch **nanoseconds** where
Spark returns epoch **seconds** for `CAST(TIMESTAMP AS BIGINT)` — a 10⁹ factor, correctly signed,
on the one shape a migrated job writes to get an epoch. §6 hands the orchestrator paste-true text
for the registry row; this ledger never edits `docs/spark-sql-iceberg-parity.md`.

## 1. Where the bug actually lived

Not in a cast shim — there was none. `repark-spark`'s cast surface owns only
`cast_may_fail_at_runtime` (the INSERT OVERWRITE wipe guard), and `datafusion-spark` ships no
timestamp→numeric cast. The SQL planner lowered `CAST(ts AS BIGINT)` straight onto DataFusion's
own cast, and arrow reinterprets a `Timestamp(Nanosecond, _)`'s backing `i64` buffer as `Int64`.
So the divergence was an **absence**: the Spark expression-semantics layer
(`crates/repark-functions/src/analyzer.rs`, `SparkExprSemantics`) had arms for `/`, `%`, `[]`,
`substr` and `overlay`, and none for `Cast`.

That is the seam the fix uses — the same one the other Spark-vs-DataFusion operator divergences
already run through, installed by the Spark door's `SessionExtension` on every session.

## 2. Probe transcript — live Spark 4.1.2 (the oracle)

Recorded 2026-08-12T00:47Z under the shared JVM lock (`/tmp/grok-jvm-record.lock`, acquired
00:46:20Z, released 00:47:56Z), on the corpus basis: `local[2]`, `spark.sql.ansi.enabled=true`,
`spark.sql.shuffle.partitions=2`, UI off. Every query was run under **three** session zones
(`America/New_York`, `Asia/Tokyo`, `UTC`); the answers were identical under all three, which is
the finding recorded as "zone-independent" throughout.

### 2a. Forward — `TIMESTAMP` → numeric

| # | Input instant | Target | **Spark 4.1.2** | repark @ 9acb566 | Verdict |
|---|---|---|---|---|---|
| A1 | `1969-12-31T23:30:00Z` | `BIGINT` | `-1800` (int64, null=T) | `-1800000000000` | **silent ×10⁹** |
| A2 | `1969-12-31T23:59:59.5Z` | `BIGINT` | **`-1`** | `-500000000` | **silent** |
| A3 | `1969-12-31T23:59:58.75Z` | `BIGINT` | **`-2`** | `-1250000000` | **silent** |
| A4 | `1970-01-01T00:00:00.75Z` | `BIGINT` | `0` | `750000000` | **silent** |
| A5 | `2024-06-15T12:00:01.999999Z` | `BIGINT` | `1718452801` | `1718452801999999000` | **silent** |
| A6 | `1970-01-01T00:00:00Z` | `BIGINT` | `0` | `0` | agree (degenerate) |
| A7 | `2024-06-15T12:00:00Z` | `BIGINT` | `1718452800` | `1718452800000000000` | **silent** |
| A8 | `NULL` | `BIGINT` | `NULL` | `NULL` | agree |
| C1 | `1969-12-31T23:30:00Z` | `INT` | `-1800` (int32) | LOUD refusal | refused, not silent |
| C2 | " | `SMALLINT` | `-1800` (int16) | LOUD refusal | refused, not silent |
| C3 | " | `TINYINT` | `CAST_OVERFLOW` | LOUD refusal | both refuse |
| C4 | `2400-01-01T00:00:00Z` | `INT` | `CAST_OVERFLOW` | LOUD (ns range) | both refuse |
| D1 | `1969-12-31T23:59:59.5Z` | `DOUBLE` | **`-0.5`** | `-500000000.0` | **silent ×10⁹** |
| D2 | `1969-12-31T23:30:00Z` | `DOUBLE` | `-1800.0` | `-1800000000000.0` | **silent** |
| D3 | " | `FLOAT` | `-1800.0` | `-1799999979520.0` | **silent** |
| D4 | `1969-12-31T23:59:59.5Z` | `DECIMAL(20,6)` | `-0.500000` | `-500000000.000000` | **silent** |
| B1 | `1969-12-31T23:30:00Z` | `LONG` (keyword) | `-1800` | `Unsupported SQL type LONG` | LOUD; residual R-3 |

### 2b. THE FLOOR-EDGE VERDICT

**Spark FLOORS (`Math.floorDiv`); it does NOT truncate toward zero.** Rows A2 and A3 are the
proof and they are the whole reason this fix is a UDF rather than a two-line arrow cast hop
through `Timestamp(Second, _)`:

| Input | floor (Spark) | truncate-toward-zero (the plausible fix) |
|---|---|---|
| `-0.5 s` | **`-1`** | `0` |
| `-1.25 s` | **`-2`** | `-1` |
| `+0.75 s` | `0` | `0` (agree) |
| `+1.999999 s` | `1718452801` | same (agree) |
| `-1.0 s` (whole) | `-1` | `-1` (agree) |

Truncation agrees with Spark on every positive instant AND on every whole negative second. It
disagrees only on a **negative fractional** second — i.e. only before 1970, sub-second, which is
exactly where nobody looks. Both engines' arithmetic is otherwise identical, so a corpus without a
negative fractional row would have gone green over the wrong implementation.

### 2c. Reverse — numeric → `TIMESTAMP` (probed, and deliberately NOT changed)

| # | Expression | **Spark 4.1.2** | repark @ 9acb566 | Verdict |
|---|---|---|---|---|
| E1 | `CAST(-1800L AS TIMESTAMP)` | `1969-12-31 23:30 UTC`, `timestamp[us, tz=UTC]`, null=F | `1969-12-31 23:30`, `timestamp[ns]`, null=F | **value AGREES**; type = TZ-4 |
| E2 | `CAST(1L AS TIMESTAMP)` | `1970-01-01 00:00:01 UTC` | same instant | value agrees |
| E3 | `CAST(-1800 AS TIMESTAMP)` (INT) | `1969-12-31 23:30 UTC` | same instant | value agrees |
| E5 | `CAST(NULL AS BIGINT)` → `TIMESTAMP` | `NULL` | `NULL` | agrees |
| E4 | `CAST(-0.5D AS TIMESTAMP)` | `1969-12-31 23:59:59.5 UTC` | parser refuses `-0.5D` | LOUD; residual R-4 |

**The reverse direction was already Spark-correct and was left alone.** DataFusion reads an
integer→timestamp cast as SECONDS, exactly as Spark does. A symmetric "fix" would have
*introduced* the divergence this unit removes. That is pinned as a fence, not assumed:
`the_reverse_direction_still_reads_seconds_and_round_trips` (Rust) and
`bigint_to_timestamp_reads_seconds` (facade disclosure). The residual type gap
(`timestamp[ns]` vs `timestamp[us, tz=UTC]`) is registry row **TZ-4**, not this unit's.

### 2d. Neighbouring casts probed to bound the class

* `CAST(DATE AS INT)` and `CAST(INT AS DATE)` — Spark **refuses both**
  (`DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION`, "use `UNIX_DATE`"). No scaling class there.
* `CAST(TIMESTAMP '1969-12-31 23:30:00' AS BIGINT)` (a **zoneless** literal) — Spark answers
  `16200` under `America/New_York` (it reads the digits as a session wall clock); repark answers
  `-1800`. That divergence is registry row **TZ-7**, the zoneless-input class, and it is why no
  row in this unit's corpus is built on a zoneless literal.
* `unix_timestamp(ts)` — Spark `-1800`; repark has no such function (`Invalid function`). LOUD,
  a missing-function unit, residual R-2.

## 3. The fix

Two files of engine change, both in `repark-functions` (crate-DAG tier 3, no internal deps).

**`crates/repark-functions/src/timestamp_cast.rs` (new).** Two embedded UDFs:

* `__repark_epoch_seconds_floor__` → `Int64`, `ticks.div_euclid(ticks_per_second(unit))`. Exact
  integer floor division; `div_euclid` IS floor division for a positive divisor and
  `ticks_per_second` is positive by construction, so it cannot panic.
* `__repark_epoch_seconds_real__` → `Float64`, `ticks as f64 / per_second as f64`. Spark computes
  its own `TIMESTAMP → DECIMAL` cast through a double, so the double hop is the oracle's own
  mechanism rather than an approximation of it.

Both are per-`TimeUnit` (a `createDataFrame` column arrives as `timestamp[us]`, a `to_timestamp`
literal as `timestamp[ns]` — the fix must not assume one), both propagate the argument's
nullability through `return_field_from_args`, and both are **embedded, never registered**, so they
are not user-callable.

**Why two UDFs and not one** — this is the load-bearing design note:
* one `Decimal128`-returning UDF cannot serve the integer target, because arrow's decimal→integer
  cast truncates toward zero and loses the floor edge again;
* one `Float64`-returning UDF cannot serve it either — f64 resolves ~2·10⁻⁷ s at present-day
  epochs, so a sub-microsecond instant can floor to the wrong second (row A5 is that shape).

**`crates/repark-functions/src/analyzer.rs`.** A new `Expr::Cast` arm in `rewrite_expr`:

```
CAST(<Timestamp> AS Int64|Int32|Int16|Int8)                  -> CAST(floor_udf(ts) AS <target>)
CAST(<Timestamp> AS Float64|Float32|Decimal128|Decimal256)   -> CAST(real_udf(ts)  AS <target>)
everything else                                              -> untouched
```

The scaling is pushed **under** the user's cast, so the outer cast still applies the requested
width — this rewrite owns the *scale*, not the cast-failure surface (that is the concurrent X-1
lane's, see §7).

**Idempotency** is structural, not asserted: the arm matches on the **source** type being a
timestamp, and its own output casts an `Int64`/`Float64`. Pinned by
`the_timestamp_cast_rewrite_is_idempotent`, which analyzes twice and asserts both the plan
fixpoint and "exactly one scaling UDF".

**Deliberately untouched** (each pinned from outside so the boundary is not a claim):
`CAST(ts AS DATE/STRING/TIMESTAMP)` (no scaling involved), unsigned integer targets (Spark SQL
cannot spell one, so a rewrite would invent semantics), and the reverse direction (§2c).

## 4. Test rows

Every expectation below is live-Spark-4.1.2-recorded, never hand-computed.

### 4a. Engine layer (Rust) — 24 new pins

| File | Cell | Pins |
|---|---|---|
| `crates/repark-functions/src/timestamp_cast.rs` | kernel | 4 — floor-vs-truncation, per-`TimeUnit` divisors, `i64` extremes without panicking, non-timestamp arg is LOUD |
| `crates/repark-functions/src/analyzer.rs` | rewrite | 9 — seconds, floor both signs, NULL+type, narrower ints, real targets, idempotency, non-numeric casts untouched, reverse untouched, round trip, column path with nulls |
| `crates/repark-spark/tests/timestamp_cast_seconds.rs` (new) | Spark door + native DataFrame API | 9 |
| `crates/repark-sql/tests/timestamp_cast_ansi_door.rs` (new) | ANSI door | 2 |

`timestamp_cast_ansi_door.rs` lives in `repark-sql` because `scripts/check_crate_dag.py` allows
`repark-sql -> repark-spark` as a dev edge and nothing the other way — the same reason
`session_timezone_ansi_door.rs` lives there. Its second pin is the honest negative: a **bare**
(extension-free) session still returns `-1800000000000`, which is both the correct behaviour for a
non-Spark session and the revert-red evidence for the whole class.

### 4b. Facade layer (Python) — the divergence-class flip

**The flip.** `python/repark/tests/test_session_timezone_parity.py` row
`pre_1970_timestamp_cast_to_bigint` moves from DISCLOSURE to EQUALITY: repark-expected
`-1800000000000` → `None` (engines agree on Spark's `-1800`). Three consequential edits in the same
module, each a visible one: the module docstring's disclosure enumeration (twelve → eleven), the
named disclosure set in `test_the_extraction_class_converged_and_the_residue_is_named`, and its
equality count (`17` → `18`, with the reason on the assertion).

**The class corpus.** `python/repark/tests/test_timestamp_cast_parity.py` (new, 19 rows) +
`python/repark/tests/_record_timestamp_cast_goldens.py` (new), following the tz corpus's
recorded-oracle contract exactly: the driver imports `ROWS` from the committed module and runs each
row's own `run_row`, so the recorded golden and the asserted recipe cannot drift apart.

It is a corpus of its own rather than more G16 rows because the class is **zone-independent** — its
rows do not belong in a budget that documents timezone semantics. The tz corpus keeps the one row
that recorded the divergence, as the flip evidence.

| Entry point | Rows |
|---|---|
| `sql` (facade Spark-dialect door) | 16 — whole/modern/zero, floor edge ×3 both signs, NULL, INT/SMALLINT/DOUBLE/FLOAT/DECIMAL, three zones, reverse-direction fence |
| `dataframe_api` (`F.col("ts").cast("long")` over a real tz-aware column) | 2 (New York + Tokyo), three targets each |
| `expr` (`F.expr("CAST(… AS BIGINT)")`) | 1, on the floor edge |

`test_the_class_is_covered_per_entry_point_and_per_edge` pins the SHAPE: all three spellings
present, the negative-fractional rows present, a positive-fractional row present, every named cast
target present, three zones at the SQL door and two at the DataFrame door, and exactly one
remaining disclosure. That is what stops the corpus decaying into "one representative case".

### 4c. Record-mode evidence (the oracle contract, discharged)

Both recorders were run under the shared JVM lock (acquired 2026-08-12T01:10:25Z, released
01:11:08Z), against live PySpark 4.1.2:

```
python/repark/tests/_record_timestamp_cast_goldens.py   -> exit 0; 19 rows re-derived, 0 mismatch(es)
python/repark/tests/_record_session_timezone_goldens.py -> exit 0; 29 rows re-derived, 0 mismatch(es)
```

Every `spark` half in both corpora reproduces bit-for-bit (schema name/type/nullability, then
values). The tz recorder was re-run because this unit edited that module.

## 5. Residuals (honest, declared — NOT silent skips)

Each is a LOUD refusal today, so none of them is a silently-wrong answer. None is in this unit's
class (the *scaling*); each is named for the unit queue.

| # | Residual | Today | Owner |
|---|---|---|---|
| R-1 | `CAST(ts AS TINYINT)` — Spark raises `CAST_OVERFLOW`; repark refuses with a DataFusion optimizer error | both refuse, messages differ | cast-FAILURE semantics (X-1's G6 corpus) |
| R-2 | `unix_timestamp(ts)` / `to_unix_timestamp` missing | `Invalid function` | missing-function unit |
| R-3 | `CAST(x AS LONG)` — the `LONG` keyword spelling | `Unsupported SQL type LONG` | parser vocabulary, not scaling |
| R-4 | `CAST(-0.5D AS TIMESTAMP)` — the `D` double-literal suffix | `ParserError` | parser vocabulary |
| R-5 | `F.expr("CAST(ts AS BIGINT)")` referencing a COLUMN — repark resolves `F.expr` eagerly against an empty schema and refuses | `Schema error: No field named ts` | `F.expr` binding gap; the corpus's `expr` row uses a self-contained expression and says so |
| R-6 | `CAST(ts AS INT)` out-of-range — Spark raises `CAST_OVERFLOW`; repark's `to_timestamp` rejects the out-of-ns-range instant first, so the shapes are not comparable yet | both refuse | blocked on TZ-4 (µs representation) |

## 6. §6 HANDOFF — paste-true registry text (orchestrator applies; this unit does NOT edit the file)

Replace the whole `### TZ-5 — CAST(TIMESTAMP AS BIGINT) returns nanoseconds` row in
`docs/spark-sql-iceberg-parity.md` (~:653) with:

> ### TZ-5 — `CAST(TIMESTAMP AS <numeric>)` returns epoch seconds — **FIXED**
>
> **Status: fixed** (2026-08-12, `hardening/tz5-timestamp-cast-seconds`). repark returned epoch
> **nanoseconds** where Spark returns epoch **seconds** — a 10⁹ factor, correctly signed, on
> `CAST(ts AS BIGINT)`. The same wrong scaling reached `DOUBLE`, `FLOAT` and `DECIMAL(p,s)`; `INT`
> and `SMALLINT` were refused outright.
>
> repark now matches Spark 4.1.2 on the whole numeric-target family, **including the floor edge**:
> Spark uses `Math.floorDiv`, so `1969-12-31T23:59:59.5Z` is `-1` (not `0`) and
> `1969-12-31T23:59:58.75Z` is `-2` (not `-1`). Truncation toward zero agrees with Spark on every
> positive instant and every whole negative second, so that edge is the only thing separating the
> two implementations. Float and decimal targets keep the fraction (`-0.5`). The class is
> zone-independent on both engines — a cast reads the instant, never a wall clock.
>
> The **reverse** direction (`CAST(<integer> AS TIMESTAMP)`) was probed and was already correct:
> DataFusion reads it as seconds, exactly as Spark does. Its remaining gap is the Arrow export
> **type** (`timestamp[ns]`, no zone, vs Spark's `timestamp[us, tz=UTC]`) — that is row TZ-4, not
> this one.
>
> **Fix:** `repark_functions::timestamp_cast` (two embedded scaling UDFs — exact `i64` floor for
> integer targets, `f64` for real targets) driven by the `Expr::Cast` arm of
> `repark_functions::analyzer::SparkExprSemantics`.
> **Pins:** `crates/repark-functions/src/{timestamp_cast,analyzer}.rs`,
> `crates/repark-spark/tests/timestamp_cast_seconds.rs`,
> `crates/repark-sql/tests/timestamp_cast_ansi_door.rs`,
> `python/repark/tests/test_timestamp_cast_parity.py` (19 recorded rows across three facade
> spellings), and the flipped row `pre_1970_timestamp_cast_to_bigint` in
> `python/repark/tests/test_session_timezone_parity.py`.
> **Residuals** (all LOUD refusals, none silent): `TINYINT` overflow-message parity, the `LONG`
> keyword spelling, `unix_timestamp`, the `D` double-literal suffix, and `F.expr` over a column
> reference — `task/tz5-cast-seconds-ledger.md` §5.

## 7. Coordination

**Grok lane X-1 (cast-failure corpus)** probed `timestamp→int` TONIGHT against the same frozen base
`9acb566`. Its corpus may pin the pre-fix nanosecond behaviour, or the pre-fix LOUD refusal for
`INT`/`SMALLINT` — both are correct per its charter against that base. The morning re-pass
reconciles: X-1's rows for the *scaling* flip when this PR lands; its rows for the *failure
messages* are unaffected and remain its own. This lane did not coordinate with, wait on, or read
X-1's worktree.

## 8. Gates

| Gate | Exit | Notes |
|---|---|---|
| `make verify` | 0 | `/tmp/opus-o3-verify.log` |
| `make py-test-facade` | 0 | `/tmp/opus-o3-py-test-facade.log` |
| `make preflight` | 0 | `/tmp/opus-o3-preflight.log` |
