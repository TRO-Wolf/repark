# Unit ledger — G5b / H-2 gap G5: temporal `RANGE` window frames

**Unit:** H-2 gap **G5**, second unit (`briefs/v2-engine-hardening.md` G5 row: "one for the
temporal-`RANGE` implementation and its pins") · **Date:** 2026-08-11 · **Lane:** overnight
O-2 · **Worktree:** `/tmp/opus-o2` · **Branch:** `hardening/g5b-temporal-range` ·
**Base:** `origin/main` `9acb566` (frozen)

**Companion unit:** W-4 (#52) landed the ROWS / numeric-`RANGE` value matrix in
`python/repark/tests/test_window_parity.py`. This unit **appends** to that corpus and never
rewrites one of its rows.

**Out of scope per charter:** the ROWS / numeric-`RANGE` matrix (W-4 owns it), the registry file
`docs/spark-sql-iceberg-parity.md` (§6 below is the paste-true handoff, not an edit), dependency
bumps of any kind.

---

## 0. Recon — MANDATORY, and it changed the unit

The charter's premise was that a temporal `RANGE` frame over a timestamp/date order key is
**"rejected outright"**. That premise is **false at the frozen base**, and finding out was the
single most valuable thing this unit did.

### 0.1 The charge's premise, tested directly

Twelve spellings through the Spark facade `.sql()` door on `9acb566`
(`/tmp/opus-o2-probe.py`, transcript `/tmp/opus-o2-recon.log`). The canonical Spark spelling
**works, and is already correct**:

```
SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING
                        AND CURRENT ROW) AS s FROM t ORDER BY id
OK schema=[('id', 'int64', True), ('s', 'int64', True)]
OK rows={'id': [1, 2, 3, 4, 5], 's': [10, 30, 60, 90, 90]}
```

So there is **no lowering to implement**. The frame class is supported; what was missing was any
test at all, and a correct *envelope* around it.

### 0.2 What the pinned DataFusion version can express

`datafusion = 54.1.0` (workspace `Cargo.toml`; unchanged — no bump was made or needed).

- `datafusion_expr::window_frame::convert_frame_bound_to_scalar_value` carries a `RANGE` bound as
  `ScalarValue::Utf8` (`"1 DAY"`, or `"1"` for a unit-less offset).
- `datafusion_optimizer::analyzer::type_coercion::extract_window_frame_target_type` maps **any**
  datetime order key to `Interval(MonthDayNano)` and casts the bound text into it.
- `datafusion_expr::window_state::WindowFrameStateRange::calculate_index_of_row` does the range
  search with `ScalarValue::add_checked` / `sub_checked`, which supports `Timestamp ± Interval`.

Temporal `RANGE` is therefore fully expressible at the pin. **The §0 escape hatch (HALT with a
deferral) does not apply** — it was written for the case where the pin cannot express the frame,
and the pin can.

### 0.3 The live Spark oracle

PySpark **4.1.2**, corpus basis (`local[2]`, `spark.sql.ansi.enabled=true`,
`spark.sql.shuffle.partitions=2`, UI off), zulu-17, under the shared JVM record lock
(`/tmp/opus-o2-oracle.py`, transcript `/tmp/opus-o2-oracle.log`). Same seed rows on both engines.

| # | Spelling (order key) | Spark 4.1.2 | repark @ `9acb566` | Verdict |
|---|---|---|---|---|
| 1 | `INTERVAL '1' DAY PRECEDING` (ts, asc) | `[10,30,60,90,90]` | same | MATCH |
| 2 | `INTERVAL '1' DAY PRECEDING` (ts, desc) | `[60,50,30,90,90]` | same | MATCH |
| 3 | `INTERVAL '0' DAY PRECEDING` (ts, ties) | `[10,20,30,90,90]` | same | MATCH |
| 4 | `INTERVAL '1' DAY PRECEDING` (ts, NULL keys) | `[10,60,40,60]` | same | MATCH |
| 5 | `INTERVAL '1' DAY PRECEDING` (**date**) | `[10,30,30]` | same | MATCH |
| 6 | `INTERVAL '1' DAY` both sides (ts) | `[60,60,60,90,90]` | same | MATCH |
| 7 | `INTERVAL '12' HOUR PRECEDING` (ts) | `[10,30,50,90,90]` | same | MATCH |
| 8 | partitioned + `INTERVAL '1' DAY` (ts) | `[10,20,40,40,50]` | same | MATCH |
| 9 | `INTERVAL '1' MONTH PRECEDING` (ts) | `[10,30,60,150,150]` | same | MATCH |
| 10 | `INTERVAL '1 day'` string form (ts) | `[10,30,60,90,90]` | same | MATCH |
| 11 | **`1 PRECEDING` (ts)** | **raises** `DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE` | `[10,30,60,150,150]` | **DIVERGE — silent** |
| 12 | **`1 PRECEDING` (date)** | `[10,30,30]` (one **day**) | `[10,30,60]` (one **month**) | **DIVERGE — silent** |
| 13 | `INTERVAL 1 DAY PRECEDING` unquoted (ts) | `[10,30,60,90,90]` | raises `Execution error` | DIVERGE — loud |
| 14 | `INTERVAL '1 12:00:00' DAY TO SECOND` (ts) | `[10,30,60,90,90]` | raises Arrow parse error | DIVERGE — loud |
| 15 | `INTERVAL '-1' DAY PRECEDING`, `sum` (ts) | `[NULL×5]` | **Rust panic** | DIVERGE — crash |
| 16 | `INTERVAL '-1' DAY PRECEDING`, `count(*)` (ts) | `[0,0,0,0,0]` | **`[-1,-1,0,0,0]`** | **DIVERGE — silent** |
| 17 | `1 DAY FOLLOWING … 2 DAY FOLLOWING` (ts) | `[30,NULL,90,NULL,NULL]` | `[30,NULL,**120**,…]` | **DIVERGE — silent** |
| 18 | `INTERVAL '1' DAY PRECEDING` (**int** key) | `[10,20,30,40,50]` | raises Arrow cast error | DIVERGE — loud |

Verbatim, the two worst:

```
# 11 — Spark refuses; repark answers a window nobody asked for
RAISE pyspark.errors...AnalysisException: [DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE] Cannot
resolve "(ORDER BY ts ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)" due to data
type mismatch: The data type "TIMESTAMP" used in the order specification does not support the
data type "INT" which is used in the range frame. SQLSTATE: 42K09

# 15 — repark, debug build
thread '<unnamed>' panicked at datafusion-functions-aggregate-54.1.0/src/sum.rs:502:9:
attempt to subtract with overflow
```

**Root cause of 11/12** (one cause, two arms): a unit-less `RANGE` offset over a datetime key is
coerced to `Interval(MonthDayNano)`, and Arrow's interval parser reads a bare `"1"` as **one
month**. Confirmed by construction: `1 PRECEDING`, `2 PRECEDING` and `30 PRECEDING` over the
three-day seed all return the running total, exactly as `INTERVAL '1' MONTH` does.

### 0.4 Entry points that can express the frame (charter §0.3)

`Window.rangeBetween(start, end)` takes **numeric offsets only** in both PySpark 4.1.2 and the
repark facade (`crates/repark-python/src/column.rs` builds an `Int64` bound for `RANGE`), so a
temporal frame is reachable **only through SQL**. The differential rows are scoped to the facade
`sql()` door accordingly, and the DataFrame-API door is correctly out of this family — not
omitted, unreachable.

### 0.5 Existing refusal pins for this class

Searched (`RANGE_FRAME`, `temporal.*range`, `interval.*preceding`) across tests and docs. There
is **no** existing refusal pin to flip: the only `RANGE` refusals in the tree are W-4-era facade
checks for `RANGE_FRAME_WITHOUT_ORDER` / `RANGE_FRAME_MULTI_ORDER`
(`python/repark/tests/test_g2_window_rand_sampleby.py`) and the DataFrame-API numeric-order-key
guard in `python/repark/src/repark/dataframe/core.py`, none of which touch this class. The
temporal-`RANGE` path had **zero** tests before this unit — a supported behaviour with no pins.

### 0.6 Verdict — what this unit therefore is

Not "implement the frame" (it works) and not the HALT deferral (the pin can express it), but the
third outcome the recon uncovered: **close the silent-wrong-answer arms of an already-working
path, pin the whole path, and record every residual.** Recorded here rather than quietly
re-scoped.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Engine change | [`crates/repark-spark/src/window_range.rs`](../crates/repark-spark/src/window_range.rs) | Spark's bare-offset rules for `RANGE` over a datetime key |
| Wire-in | [`crates/repark-spark/src/spark_ast.rs`](../crates/repark-spark/src/spark_ast.rs) | `conform_temporal_range_frames` between planning and analysis |
| Rust pins | [`crates/repark-spark/src/tests/window_temporal_range.rs`](../crates/repark-spark/src/tests/window_temporal_range.rs) | 5 tests (4 pin classes + the NULL-key twin) |
| Differential rows | [`python/repark/tests/test_window_parity.py`](../python/repark/tests/test_window_parity.py) | +15 rows, +2 facade tests, budget + family pins |
| This ledger | `task/g5b-temporal-range-ledger.md` | linked from [`task/map.md`](map.md) |

### 1.1 The fix, precisely

Two arms, each **exactly** what Spark does — no invented semantics, no touched peer/tie handling:

| Order key | unit-less `RANGE` offset | Behaviour after this unit |
|---|---|---|
| `TIMESTAMP` | Spark refuses | refuse, carrying Spark's `DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE` |
| `DATE` | Spark reads **days** | restate the bound as `INTERVAL '<n>' DAY` and re-plan |
| numeric / anything else | ordinary value offset | **untouched** |

**Why two mechanisms.** A window expression's schema name embeds its frame
(`datafusion_expr::expr`'s `SchemaDisplay` writes `" {window_frame}"`), so rewriting a bound on
the planned `LogicalPlan` renames the Window node's output field and strands every parent
`Expr::Column`. Refusing needs no rewrite, so the TIMESTAMP arm reads the planned tree; the DATE
arm restates the **AST** and re-plans, where the whole plan is rebuilt consistently.

**Cost.** A cheap AST probe (`statement_has_bare_range_bound`) gates everything: a statement with
no unit-less `RANGE` bound — effectively every statement — keeps the single-plan path untouched.

**Deliberate narrowness.** The DATE restatement is statement-wide-or-nothing, because the AST
carries no resolved order-key type. A statement mixing a DATE-keyed and an INT-keyed bare-number
frame is therefore left alone: the DATE frame keeps its recorded divergence rather than the INT
frame acquiring a new one. A narrower fix, never a wider bug — pinned by
`temporal_range_numeric_order_keys_are_untouched`.

### 1.2 Rust pins (charter: 4) — `crates/repark-spark/src/tests/window_temporal_range.rs`

| # | Test | Class |
|---|---|---|
| 1 | `temporal_range_bare_offset_over_timestamp_key_refuses_like_spark` | the refuse arm (3 offsets × PRECEDING/FOLLOWING/shorthand) |
| 2 | `temporal_range_bare_offset_over_date_key_means_days` | the restate arm (1 day vs 30 days vs the spelled-out interval) |
| 3 | `temporal_range_interval_bounds_still_match_spark` | the already-correct path undisturbed: asc, desc, ties, HOUR≠DAY |
| 4 | `temporal_range_numeric_order_keys_are_untouched` | scope: numeric keys + the mixed-statement fallback |
| + | `temporal_range_null_order_keys_match_spark` | NULL order keys (split from 3 for its own fixture) |

### 1.3 Revert-red proof (`docs/testing.md`, divergence-class rule 3)

The `conform_temporal_range_frames` call was removed from `spark_ast.rs`, the suite re-run, and
the call restored. Verbatim:

```
test tests::window_temporal_range::temporal_range_bare_offset_over_timestamp_key_refuses_like_spark ... FAILED
test tests::window_temporal_range::temporal_range_null_order_keys_match_spark ... ok
test tests::window_temporal_range::temporal_range_bare_offset_over_date_key_means_days ... FAILED
test tests::window_temporal_range::temporal_range_numeric_order_keys_are_untouched ... ok
test tests::window_temporal_range::temporal_range_interval_bounds_still_match_spark ... ok

panicked at crates/repark-spark/src/tests/window_temporal_range.rs:208:32:
`SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM wt
 ORDER BY id` must refuse, not answer a one-month window
panicked at crates/repark-spark/src/tests/window_temporal_range.rs:265:5:
assertion `left == right` failed: `1 PRECEDING` over a DATE key is Spark's ONE DAY, not
DataFusion's one month

test result: FAILED. 3 passed; 2 failed; 0 ignored; 0 measured; 369 filtered out
```

Both fix arms are covered by a pin that reds when reverted; the three that stay green are the
scope pins, which *must* stay green (they assert the fix changed nothing there). Restored:
`test result: ok. 5 passed; 0 failed`. The provocation was never committed.

### 1.4 Differential rows (+15) — `python/repark/tests/test_window_parity.py`

Family `temporal_range`, appended after W-4's 27 rows. Corpus total **42**; budget ceiling raised
`28 → 45` (the floor, the equality floor and the disclosure ceiling are unchanged). No W-4 row
was edited.

**Equalities (9)** — repark == Spark on value AND Arrow type AND nullability:

| Name | What it pins |
|---|---|
| `temporal_range_ts_asc_interval_day` | ascending ts key, one-day trailing frame |
| `temporal_range_ts_desc_interval_day` | DESC — different values, so direction is real |
| `temporal_range_ts_peer_group_zero_interval` | ties: zero-width interval == the peer group |
| `temporal_range_ts_null_order_keys` | NULL keys are their own peer group |
| `temporal_range_date_order_key_interval_day` | DATE order key |
| `temporal_range_ts_partitioned` | the interval must not cross partitions |
| `temporal_range_ts_both_sides_interval` | centred window (both bounds intervals) |
| `temporal_range_ts_hour_unit` | HOUR ≠ DAY, so the unit is honoured |
| `temporal_range_bare_offset_over_date_key_means_days` | **the fix's facade-side evidence** |

**Recorded residual divergences (6)** — each names what flips it:

| Name | Spark | repark | Class |
|---|---|---|---|
| `temporal_range_unquoted_interval_literal` | `[10,30,60,90,90]` | raises | spelling |
| `temporal_range_day_to_second_literal` | `[10,30,60,90,90]` | raises | spelling |
| `temporal_range_negative_offset_sum` | `[NULL×5]` | raises (panic) | defect |
| `temporal_range_negative_offset_count` | `[0,0,0,0,0]` | `[-1,-1,0,0,0]` | **defect, silent** |
| `temporal_range_following_to_following_window` | `[30,NULL,90,NULL,NULL]` | `[30,NULL,120,…]` | **value, silent** |
| `temporal_range_interval_bound_over_int_key` | `[10,20,30,40,50]` | raises | error class |

Two facade tests carry what the `WindowRow` shape cannot (it forbids pinning *both* engines
raising): `test_temporal_range_bare_offset_over_timestamp_refuses` and
`test_temporal_range_bare_offset_over_date_key_is_days_not_months`.

### 1.5 Record mode

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_window_goldens.py
```

Run under the shared JVM record lock (`/tmp/grok-jvm-record.lock`, FIFO, released immediately
after). Result is in §3.

---

## 2. Honest residuals

1. **Five divergence classes remain open** (§1.4 disclosures). None is silently absorbed; each is
   a pinned row that reds if it changes.
2. **The negative-offset panic is a wrong-answer class, not only a crash class.** `attempt to
   subtract with overflow` is a debug-build arithmetic check; a release wheel wraps instead. The
   `count(*)` row (`-1`) is what shows the silent half.
3. **The mixed-statement fallback is a deliberate hole** (§1.1): one statement carrying both a
   DATE-keyed and a numeric-keyed unit-less `RANGE` bound leaves the DATE frame diverging.
   Pinned, not hidden.
4. **The refusal message is not byte-identical to Spark's.** Spark quotes its own resolved plan
   text inside the message; we quote our rendering of the window spec. The error class and the
   semantic sentence are verbatim; the quoted spec is ours.
5. **The charter asked for an implementation of a frame class that already worked.** The unit
   delivered the correctness envelope instead. Recorded here rather than re-scoped quietly.

---

## 3. Record mode — result

All **42** Spark halves re-derive bit-for-bit against live PySpark 4.1.2, including all 15 rows
this unit added:

```
[temporal_range] temporal_range_ts_asc_interval_day PASS
[temporal_range] temporal_range_ts_desc_interval_day PASS
[temporal_range] temporal_range_ts_peer_group_zero_interval PASS
[temporal_range] temporal_range_ts_null_order_keys PASS
[temporal_range] temporal_range_date_order_key_interval_day PASS
[temporal_range] temporal_range_ts_partitioned PASS
[temporal_range] temporal_range_ts_both_sides_interval PASS
[temporal_range] temporal_range_ts_hour_unit PASS
[temporal_range] temporal_range_bare_offset_over_date_key_means_days PASS
[temporal_range] temporal_range_unquoted_interval_literal PASS
[temporal_range] temporal_range_day_to_second_literal PASS
[temporal_range] temporal_range_negative_offset_sum PASS
[temporal_range] temporal_range_negative_offset_count PASS
[temporal_range] temporal_range_following_to_following_window PASS
[temporal_range] temporal_range_interval_bound_over_int_key PASS

record mode: 42 spark halves re-derived, 0 mismatch(es)
```

Exit code 0. W-4's 27 rows also re-derive unchanged, so this unit's edits did not disturb them.

---

## 4. Files touched

- `crates/repark-spark/src/window_range.rs` (new)
- `crates/repark-spark/src/spark_ast.rs`
- `crates/repark-spark/src/lib.rs`
- `crates/repark-spark/src/tests/window_temporal_range.rs` (new)
- `crates/repark-spark/src/tests/mod.rs`
- `crates/repark-spark/map.md`, `crates/repark-spark/src/tests/map.md`
- `python/repark/tests/test_window_parity.py`, `python/repark/tests/map.md`
- `task/g5b-temporal-range-ledger.md` (this file), `task/map.md`

---

## 5. Gate results

Each run as `cmd > /tmp/opus-o2-<gate>.log 2>&1; echo $?` — a real exit code, never a pipe's.

| Gate | Command | Exit | Result |
|---|---|---|---|
| verify | `make verify` | **0** | 1342 Rust tests pass (33 binaries); lint / fmt / clippy / the six structure gates clean |
| facade | `make py-test-facade` | **0** | **2724 passed**, 61 skipped, in 115.76s |
| preflight | `make preflight` | **0** | verify + `cargo deny` + `pip-audit` + `zizmor` — "No findings to report" |

The window suites specifically: `test_window_parity.py` **45 passed** (42 differential rows + the
budget/shape pin + the 2 G5b facade tests) and `crates/repark-spark` `window_temporal_range` **5
passed**.

---

## 6. Handoff for the registry (paste-true; this unit does NOT edit the registry)

> **Temporal `RANGE` window frames — supported, with a corrected bare-offset envelope (G5b).**
> A `RANGE` frame bounded by an interval over a `TIMESTAMP` or `DATE` order key
> (`RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW`) matches Spark 4.1.2 on value and
> Arrow type through the facade `sql()` door — ascending and descending order, ties on the order
> key, NULL order keys, `DATE` keys, partitioned frames, and sub-day units. It was already
> correct before this unit; it now has pins.
>
> A **unit-less** offset over a datetime order key (`RANGE BETWEEN 1 PRECEDING`) no longer
> silently means one *month*: over a `TIMESTAMP` key the door refuses with Spark's
> `DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE`, and over a `DATE` key it means days, as in Spark.
>
> **Recorded divergences (open):**
> 1. `INTERVAL 1 DAY PRECEDING` — the unquoted interval literal is refused as a frame bound
>    (accepted everywhere else); use `INTERVAL '1' DAY`.
> 2. `INTERVAL '1 12:00:00' DAY TO SECOND PRECEDING` — a field-qualified interval literal is
>    refused as a frame bound.
> 3. A **negative** interval offset (`INTERVAL '-1' DAY PRECEDING`) is a wrong answer, not an
>    empty frame: `count(*)` returns **-1** and `sum` fails. Spark returns an empty frame.
> 4. A frame with **both** bounds `FOLLOWING` includes the current row, which lies outside it
>    (`INTERVAL '1' DAY FOLLOWING AND INTERVAL '2' DAY FOLLOWING` sums 120 where Spark sums 90).
> 5. An interval bound over a **numeric** order key raises a raw Arrow cast error rather than a
>    Spark error class or Spark's table.
>
> Entry points: SQL only — `Window.rangeBetween` takes numeric offsets in PySpark and in the
> facade, so a temporal frame is not reachable from the DataFrame API in either engine.
> Pins: `crates/repark-spark/src/tests/window_temporal_range.rs` (Spark door) and the
> `temporal_range` family in `python/repark/tests/test_window_parity.py` (facade differential).

**Unit-queue rows this unit hands forward** (one follow-up unit, five classes — all in the same
DataFusion frame-bound seam, so they should be scoped together, not one PR each):

| Row | Class | Note |
|---|---|---|
| G5b-R1 | unquoted `INTERVAL n UNIT` frame bound | AST normalisation before planning |
| G5b-R2 | `DAY TO SECOND` qualified literal | same seam as R1 |
| G5b-R3 | negative interval offset | wrong answer + panic; needs a Spark-shaped empty frame |
| G5b-R4 | `FOLLOWING`-to-`FOLLOWING` off-by-one | range-search boundary, upstream-shaped |
| G5b-R5 | interval bound over a numeric key | error-class alignment only |

## Landing note (L-1, 2026-08-12)

Supported + corrected envelope classified **LANDED** as a FIXED-style registry note. G5b-R1…R5
classified **LANDED** as OPEN pinned BACKLOG rows (no live-mirror — window-frame recipes are
not in the L-1 live-tier both-halves set). G5 slate cell dated-corrected in
`briefs/v2-engine-hardening.md`.
