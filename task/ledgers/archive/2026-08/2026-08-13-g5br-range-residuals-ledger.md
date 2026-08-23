# Unit ledger — G5b-R / Y-1: window-RANGE residual classes

**Unit:** G5b-R (follow-up to G5b) · **Date:** 2026-08-12 · **Lane:** overnight Y-1 ·
**Worktree:** `/tmp/grok-y1` · **Branch:** `grok/y1-g5br-range-residuals` ·
**Base (frozen):** `a985edf7e22b68ea720cb2a8e08fca6cdd1a33b7` (PR #65 / L-1)

**Charter:** `planning/grok/BRIEF-y1-g5br-range-residuals.md`. Writable set: `window_range.rs`,
`tests/window_temporal_range.rs`, `test_window_parity.py`, `_record_window_goldens.py`, plus
same-crate lowering only if §0 named it. `spark_ast.rs` and `column.py` are **not** in the set
(A8 / A10). No `Cargo.lock` bumps. Registry / `_live_parity.py` / live pins are orchestrator-only.

**R3 hard rule:** wrapping-wrong must not survive in any form. Prefer Spark-matching values at
the pinned DataFusion; else loud refuse at the seam. Never still wrapping.

---

## 0. Recon

Re-derived at the frozen base on the Spark door (`crates/repark-spark` `execute`), the ANSI
door (`repark-sql` `ReparkSession` + `AnsiDialect`, throwaway probe deleted), and live PySpark
4.1.2 under `/tmp/grok-jvm-record.lock` (MARKER=`y1-g5br`; FIFO after Y-6's marker-verified
release). Seed = G5b timestamp rows (ids 1–5, values 10/20/30/40/50).

### 0.1 Per-class reproduction at base

| # | Class | Spark door @ base | ANSI door @ base | Mechanism |
|---|---|---|---|---|
| R3 | `INTERVAL '-1' DAY PRECEDING` + `count(*)` | `[-1,-1,0,0,0]` (silent wrap) | same wrap | sliding `count` retracts an inverted range; `i64` goes negative |
| R3 | same + `sum` | debug **panic** `attempt to subtract with overflow` at `sum.rs:502` (`u64` count retract) | same panic | same inverted range; release wheels wrap |
| R3 | DATE + negative | `[0,0,0]` / `sum` NULL (already Spark-empty) | n/a (no date seed) | DATE ± interval does not invert the search the same way |
| R1 | unquoted `INTERVAL 1 DAY` | first-plan `INTERVAL expression cannot be Value(Number("1"))` | same | `convert_frame_bound_to_scalar_value` accepts only `SingleQuotedString` |
| R2 | `INTERVAL '1 12:00:00' DAY TO SECOND` | analyze `Invalid input syntax for type interval: "1 12:00:00 DAY"` | same | convert concatenates leading field only; Arrow rejects |
| R2 | `INTERVAL '1 0:0:0' DAY TO SECOND` | same Arrow error (`"1 0:0:0 DAY"`) | — | same |
| R4 | `1 DAY FOLLOWING` … `2 DAY FOLLOWING` | `[30,NULL,120,NULL,NULL]` | panic/same class (not collected past R3) | DF range-search / sliding window includes current; plan frame is correctly typed |
| R5 | `INTERVAL '1' DAY` over `INT` key | Arrow `Cannot cast string '1 DAY' to value of Int64 type` | same | coerce Utf8 interval text onto the numeric key type |

### 0.2 Spark 4.1.2 (live, recorded)

Same recipes as the Python corpus rows. Recorded halves in
`python/repark/tests/test_window_parity.py` re-derived bit-for-bit (see §3). Extra probes:

- R3 `INTERVAL '-1' DAY PRECEDING` + `sum` → `[NULL×5]`; + `count(*)` → `[0,0,0,0,0]`.
- R3 `CURRENT ROW AND INTERVAL '-1' DAY FOLLOWING` → empty (same inverted frame).
- R2 both DAY TO SECOND spellings → `[10,30,60,90,90]`.
- R4 → `[30,NULL,90,NULL,NULL]` (count `[1,0,2,0,0]`).
- R5 → `[10,20,30,40,50]` (each unique `v` sees only itself).
- R1 unquoted → same table as quoted `INTERVAL '1' DAY`.

### 0.3 Rewrite candidates (Spark door, at base)

| Candidate | Result | Used? |
|---|---|---|
| `INTERVAL '36' HOUR` / `INTERVAL '1 day 12 hours'` | `[10,30,60,90,90]` | R2 restates to the spelled-out form |
| `INTERVAL '1 12:00:00'` (no qualifier) | Arrow still rejects | no |
| `1 DAY FOLLOWING AND CURRENT ROW` (sign-normalized R3) | DF refuses: start FOLLOWING > end CURRENT | inverted after flip → empty frame, not this refuse |
| inverted `ROWS` / numeric `RANGE` | DF refuses start > end | not a value path |
| `10000 YEAR FOLLOWING AND 10000 YEAR FOLLOWING` | sum NULL, count 0 | **R3 empty-frame restatement** |
| DATE + negative (no rewrite) | already empty | left on the single-plan path |

### 0.4 Why R1 / R4 / R5 are not closed here

- **R1** fails inside `statement_to_plan` **before** `classify_planned_range_frames`. A fix is
  a pre-plan AST rewrite. That call site lives in `spark_ast.rs`, which A8/A10 assign to Y-7
  tonight. Already loud (not wrapping). **Defer.**
- **R4** plans a correctly-typed
  `IntervalMonthDayNano { days: 1 } FOLLOWING … { days: 2 } FOLLOWING` frame. The 120 vs 90
  is DataFusion's range-search / sliding accumulator at the pin. No AST rewrite at this seam
  produces Spark's table without inventing row-level compensation. No `Cargo.lock` bump.
  **Defer** (silent-wrong stays a recorded disclosure).
- **R5** is error-class alignment (Spark answers a table; we raise Arrow). Matching Spark
  would mean restating the interval to a numeric `0`/`CURRENT ROW` after resolving the key
  type, with a ties oracle we do not have a rewrite home for without widening the DATE-arm
  restatement onto numeric keys. **Defer.**

### 0.5 Entry points

Temporal / interval `RANGE` is SQL-only (`Window.rangeBetween` is numeric in both engines).
The ANSI door does **not** call `window_range.rs`. R3 wrapping therefore **remains on the
ANSI door** — out of this unit's writable set; named in §6. Facade `.sql()` rides the Spark
door and gets the fix.

### 0.6 Adjacent lowering

§0 did **not** name a same-crate lowering file. All engine edits are in `window_range.rs`.
`spark_ast.rs` is unchanged: the existing `statement_has_bare_range_bound` probe +
`RangeFrameVerdict::RestateBareBoundsAsDays` + `rewrite_bare_range_bounds_to_days` path is
generalized in place (the probe now also fires for a negative or field-qualified interval).

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Engine | [`crates/repark-spark/src/window_range.rs`](../../../../crates/repark-spark/src/window_range.rs) | R3 empty-frame restatement; R2 DAY TO SECOND restatement; mixed negative+numeric refuse |
| Spark-door pins | [`crates/repark-spark/src/tests/window_temporal_range.rs`](../../../../crates/repark-spark/src/tests/window_temporal_range.rs) | +5 tests (R3 value, R2 value, R1/R4/R5 still-open) |
| Facade corpus | [`python/repark/tests/test_window_parity.py`](../../../../python/repark/tests/test_window_parity.py) | R2 + both R3 rows flipped to equality; disclosure floor 5→3 |
| This ledger | `task/g5br-range-residuals-ledger.md` | linked from [`task/map.md`](../../../map.md) |

### 1.1 R3 fix (HIGH)

At plan time the bound is still `Utf8("-1 DAY")`. DataFusion's start≤end check looks at bound
**kind**, not the sign inside the interval, so a negative PRECEDING is not seen as FOLLOWING
and the sliding window wraps (`count(*)` = -1; `sum` panics in debug).

The existing AST-restate + re-plan path now:

1. Flips `INTERVAL '-n' UNIT PRECEDING` ↔ `INTERVAL 'n' UNIT FOLLOWING`.
2. If the **signed positions** are then inverted (kind **or** same-kind magnitude),
   restates the window as `FILTER (WHERE false)` over `RANGE BETWEEN CURRENT ROW AND
   CURRENT ROW`. DataFusion never executes the inverted search.

Half-B (2026-08-12) dropped the `INTERVAL '10000' YEAR FOLLOWING` ×2 pair: that pair is
not Spark-empty (a peer at `ts+10000y` would match; year overflow is undefined). DATE +
negative already answered empty and is left on the single-plan path (classify is
TIMESTAMP-only for this site).

A statement that mixes a negative TIMESTAMP interval with a **numeric** unit-less bound cannot
use the statement-wide restatement (same mixed-statement rule as G5b). That mix is **refused**
(`UNSUPPORTED.NEGATIVE_RANGE_OFFSET`) so wrapping cannot ride it.

### 1.2 R2 fix

`INTERVAL '1 12:00:00' DAY TO SECOND` becomes Utf8 `"1 12:00:00 DAY"` (leading field only).
Arrow rejects it. The restatement clears `leading_field` / `last_field` and spells
`'1 days 12 hours 0 minutes 0 seconds'`, which Arrow accepts. Same for `'1 0:0:0'`.

### 1.3 Probe

`statement_has_bare_range_bound` (name kept: `spark_ast.rs` is not ours) now also returns
true for a negative interval or a field-qualified interval (`last_field`). Ordinary
`INTERVAL '1' DAY` stays on the single-plan path.

---

## 2. Dispositions

| Class | Disposition | Evidence |
|---|---|---|
| **R3** | **FIX** (Spark empty frame) | wrapping gone; `count` `[0,0,0,0,0]`, `sum` `[NULL×5]` |
| **R2** | **FIX** | `[10,30,60,90,90]` for both DAY TO SECOND spellings |
| **R1** | **DEFER** | needs pre-plan rewrite in `spark_ast.rs` (Y-7 tonight) |
| **R4** | **DEFER** | DF range-search off-by-one at the pin; still 120 vs 90 |
| **R5** | **DEFER** | error-class alignment; still Arrow cast |

R3 is not wrapping in any form on the Spark door / facade `.sql()` **after the Half-B
fix** (kind-or-magnitude invert). The ANSI door still wraps — named residual, not
silently absorbed. See §7.

---

## 3. Record mode

FIFO: Y-6 (`MARKER=y6-g10-boundary`) released the lock at ~19:24:57; Y-1 acquired
`MARKER=y1-g5br` (no stale removal). Released immediately after record.

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_window_goldens.py
```

```
[temporal_range] temporal_range_unquoted_interval_literal PASS
[temporal_range] temporal_range_day_to_second_literal PASS
[temporal_range] temporal_range_negative_offset_sum PASS
[temporal_range] temporal_range_negative_offset_count PASS
[temporal_range] temporal_range_following_to_following_window PASS
[temporal_range] temporal_range_interval_bound_over_int_key PASS

record mode: 42 spark halves re-derived, 0 mismatch(es)
```

Exit 0. The three flipped rows keep their recorded Spark halves; only the repark
pin / `repark_raises` flag moved. Extra live probes (`/tmp/y1-spark-oracle.log`)
confirmed R3 empty frame, both R2 spellings, R4 = 90, R5 table + ties as peer groups.

The three flipped rows keep their recorded Spark halves; only the repark pin/`repark_raises`
flag moved. R1/R4/R5 Spark halves are unchanged.

---

## 4. Files touched

- `crates/repark-spark/src/window_range.rs`
- `crates/repark-spark/src/tests/window_temporal_range.rs`
- `crates/repark-spark/src/map.md`, `crates/repark-spark/src/tests/map.md`
- `python/repark/tests/test_window_parity.py`, `python/repark/tests/map.md`
- `task/g5br-range-residuals-ledger.md` (this file), `task/map.md`

Half-B added pins in `window_temporal_range.rs` + the
`temporal_range_negative_both_preceding_count` corpus row; no `spark_ast.rs`,
`column.py`, registry, `_live_parity.py`, live pins, or `Cargo.lock`.

---

## 5. Gate results

Each run as `cmd > /tmp/y1-<gate>.log 2>&1; echo $?` — a real exit code.

| Gate | Command | Exit | Result |
|---|---|---|---|
| verify | `make verify` | **0** | `/tmp/y1-verify.log` — lint / fmt / clippy / structure gates / Rust workspace tests clean |
| preflight | `make preflight` | **0** | `/tmp/y1-preflight.log` — verify + facade suite **2822 passed**, 71 skipped + `cargo deny` / `pip-audit` / zizmor "No findings to report" |
| record | `_record_window_goldens.py` | **0** | 42 Spark halves re-derived, 0 mismatches |

Spark-door `window_temporal_range`: **10** passed (5 G5b + 5 G5b-R).

---

## 6. Handoff for the registry (paste-true; this unit does NOT edit the registry)

> **G5b-R window-RANGE residuals (Y-1, 2026-08-12).** Two of the five G5b residual classes
> close on the Spark door / facade `.sql()`; three stay OPEN. Pins:
> `crates/repark-spark/src/tests/window_temporal_range.rs` and the `temporal_range` family in
> `python/repark/tests/test_window_parity.py`.
>
> **G5b-R3 — FIXED (Half-B).** Invert is kind **or** same-kind magnitude after
> sign-normalize. Kind invert vs CURRENT ROW (`INTERVAL '-1' DAY PRECEDING AND CURRENT
> ROW`) is Spark's empty frame (`count(*)` 0, `sum` NULL) via `FILTER (WHERE false)` over a
> current-row frame. Same-kind magnitude invert (`-2 PRECEDING AND -1 PRECEDING`, direct
> `2 FOLLOWING AND 1 FOLLOWING`) refuses at classify with Spark's
> `SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON` (live 4.1.2). The previous silent-wrong
> (`count(*)` = **-1** in release wheels; `sum` panics in debug) is gone on the **Spark
> door / facade `.sql()` after this fix**. The far-future `10000 YEAR FOLLOWING` pair is
> gone (not Spark-empty). DATE + negative already answered empty and is unchanged. The ANSI
> door does not call this seam and still wraps — named residual, not silently absorbed. A
> statement that mixes a negative TIMESTAMP interval with a numeric unit-less `RANGE` bound
> is refused (`UNSUPPORTED.NEGATIVE_RANGE_OFFSET`) so wrapping cannot ride the mixed-statement
> hole (Q-003, pinned).
>
> **G5b-R2 — FIXED.** `INTERVAL '1 12:00:00' DAY TO SECOND` (and `'1 0:0:0'`) as a frame bound
> matches Spark 4.1.2 on value and Arrow type. The door restates the qualified literal as an
> Arrow-accepted interval string (`1 days 12 hours 0 minutes 0 seconds`) and re-plans.
>
> **G5b-R1 — still OPEN.** Unquoted `INTERVAL 1 DAY` as a frame bound is still refused at
> first plan (`INTERVAL expression cannot be Value(Number…)`). Spark accepts it. Fix needs a
> pre-plan AST rewrite in `spark_ast.rs` (not this unit's writable set). Use
> `INTERVAL '1' DAY`.
>
> **G5b-R4 — still OPEN.** Both-bounds-`FOLLOWING`
> (`INTERVAL '1' DAY FOLLOWING AND INTERVAL '2' DAY FOLLOWING`) still includes the current
> row (120 vs Spark 90). The planned frame is correctly typed; the miss is DataFusion's
> range-search / sliding window at the pin. No dependency bump.
>
> **G5b-R5 — still OPEN.** An interval bound over a numeric order key still raises a raw
> Arrow cast error (`Cannot cast string '1 DAY' to value of Int64 type`). Spark returns a
> table in which every unique key sees only itself. Error-class alignment only.

**Unit-queue rows this unit hands forward:**

| Row | Class | Note |
|---|---|---|
| G5b-R1 | unquoted `INTERVAL n UNIT` frame bound | pre-plan rewrite in `spark_ast.rs` |
| G5b-R4 | `FOLLOWING`-to-`FOLLOWING` off-by-one | DataFusion range-search; needs a pin bump or an upstream fix |
| G5b-R5 | interval bound over a numeric key | error-class alignment; Spark answers a table |
| G5b-R3-ANSI | negative interval wrap on the ANSI door | same DF defect; this seam is Spark-door only |

---

## 7. Half-B (OPEN queue, 2026-08-12)

Cycle-1 critic findings against the first R3 restatement. **Spark-door wrapping is gone
after this fix.** ANSI still wraps (named residual above). STATUS / registry stay
orchestrator-owned — this unit does not edit them.

| ID | Sev | Disposition |
|---|---|---|
| Q-001 / L-001 / SAF-001 | S1 | **FIXED.** Invert is signed-position (kind **or** same-kind magnitude) after sign-normalize. Same-kind magnitude invert (`-2 PRECEDING AND -1 PRECEDING`, direct `2 FOLLOWING AND 1 FOLLOWING`) refuses at classify with Spark's `SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON` — never executed, never wraps. |
| Q-002 / L-002 | S1/S2 | **FIXED.** Dropped `INTERVAL '10000' YEAR FOLLOWING` ×2. Kind invert vs CURRENT ROW restates to `FILTER (WHERE false)` over a current-row frame (Spark-empty). Magnitude invert refuses loud (Spark 4.1.2 live under MARKER=`y1-g5br-fix`). |
| Q-003 / L-003 | S2 | **FIXED.** Mixed negative-TS + numeric-bare refuse is pinned (`temporal_range_mixed_negative_timestamp_and_numeric_bare_refuses` in Rust + the facade module-level twin). |
| CL-001 | S2 | **FIXED (this ledger only).** Spark-door wrapping gone after this fix; ANSI still wraps. STATUS / registry not edited. |

Pins added: Rust `temporal_range_value_inverted_frames_do_not_wrap` (spellings 2–4 refuse
like Spark) + the mixed refuse; Python
`test_temporal_range_negative_both_preceding_refuses_like_spark` (spelling 2; both engines
raise — not a value row). Live Spark probe under `/tmp/grok-jvm-record.lock`
MARKER=`y1-g5br-fix`.
