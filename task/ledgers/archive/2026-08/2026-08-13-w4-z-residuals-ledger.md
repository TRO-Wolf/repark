# Unit ledger — W-4: Z-wave residuals (R1 / R5 close, R4 recon, ns pins, Q-002)

**Unit:** W-4 Z-wave residuals · **Date:** 2026-08-13 · **Lane:** grok W-4 ·
**Worktree:** `/tmp/grok-w4` · **Branch:** `grok/w4-z-residuals` ·
**Base (frozen):** `c7e6589088111ded62848751a30a45adfea0973a` (#79 tip)

**Charter:** `planning/grok/BRIEF-w4-z-residuals.md` + conductor-6 A5 / A6.
**SEPMO:** STANDARD, acc + C4. Writable: `spark_ast.rs`, `window_range.rs`,
`tests/window_temporal_range.rs`, `test_window_parity.py` + record driver,
`functions.py`, `test_g4b_semi_join.py`, maps, this ledger. **CLOSED:**
`column.py`, `core.py`, `cross_door.rs` + DML valve files (W-3), tz/decimal
surfaces, G13 arithmetic, registry, `_live_parity.py`.

---

## 0. Recon

### 0.1 Tree

`git rev-parse HEAD` = `c7e6589088111ded62848751a30a45adfea0973a`. DataFusion
54.1.0. sqlparser 0.62.0 (`WindowFrame` still `// TBD: EXCLUDE`).

### 0.2 R1 — unquoted `INTERVAL 1 DAY`

Z-4 live Spark 4.1.2: accepts; same table as quoted `INTERVAL '1' DAY`
(`[10, 30, 60, 90, 90]`). DataFusion `convert_frame_bound_to_scalar_value`
accepts only `SingleQuotedString` (`datafusion-expr-54.1.0/src/window_frame.rs`).
Pre-plan quote at `spark_ast` altitude (file freed by #78).

### 0.3 R4 — both-bounds-FOLLOWING 120 vs 90

Re-verified on this tree: rust pin still
`[30, NULL, 120, NULL, NULL]`. Planned frame is correctly typed
`IntervalMonthDayNano { days: 1 } FOLLOWING … { days: 2 } FOLLOWING`.
sqlparser 0.62 `WindowFrame` has `// TBD: EXCLUDE`. In-place plan rewrite
strands parent `Expr::Column` (G5b module doc). No `Cargo.lock` bump.

`WindowFrameStateRange::calculate_range` (DF 54.1.0) is the miss; no AST
restatement at this seam produces Spark's table without inventing row-level
compensation. Spark answers a table — refuse-loud would invent a class.

### 0.4 R5 — interval bounds over a numeric order key

Z-4 magnitude probe (ties seed `(1,10),(2,20),(3,20),(4,21),(5,30)`):

| Bound | Spark sum |
|---|---|
| `INTERVAL '0' DAY PRECEDING` | `[10, 40, 40, 21, 30]` (peer group) |
| `INTERVAL '1' DAY PRECEDING` | `[10, 40, 40, 61, 30]` |
| `INTERVAL '1' HOUR` / `'1' MONTH` / unquoted `INTERVAL 1 DAY` | same as 1 DAY |
| `RANGE BETWEEN 1 PRECEDING AND CURRENT ROW` | same as 1 DAY |
| `INTERVAL '10' DAY PRECEDING` | `[10, 50, 50, 61, 91]` (numeric 10) |

Spark 4.1.2 treats `INTERVAL 'n' UNIT` over INT as **numeric `n` RANGE**,
**unit ignored**. Y-1's unique-`v` seed (gaps of 10) could not see the
magnitude and recorded `[10,20,30,40,50]` — consistent with numeric `n=1`,
not a distinct "self-group" class. Not version-fragile on 4.1.2. Fix, not
refuse.

Type-aware: classify on the first plan (key type resolved); restate only when
every interval `RANGE` site is numeric-keyed. Mixed datetime-interval +
numeric-interval stays on the first plan (statement-wide rewrite cannot tell
the sites apart).

### 0.5 Window ns type pins

#79 is µs e2e. Temporal seed fixtures were already `TimestampMicrosecondArray`.
Tighten: rust `SELECT ts` type pin + facade `timestamp.unit == "us"`. No
corpus golden projected `ts`, so no Spark re-record of existing rows.

### 0.6 Q-002 aggregate origin

`sum` / `count` (non-star) / `avg` / `min` / `max` all built a fresh `Column`
and dropped `_origin_plan_id` / `_join_sql_expr` — the same hole as Z-4
`F.abs`. `count_distinct` is the same path (including multi-arg: first origin
is left, `join_sql` must carry the right QCOL). `first` / `last` use the same
`_aggregate_argument` builder and drop origin the same way — §0 reds the
hole; they ride along (A6 allowed). `column.py` untouched.

### 0.7 A5 ride-along

`spark_ast.rs` L69 comment said "IN … only". The attach is already
`try_allowed_delete_in` → `execute_predicate_dml` (spelling-generic). One-line
refresh; W-3 stays byte-identical.

### 0.8 JVM lock

Waited for `/tmp/grok-w1-first-released` (present 2026-08-13T13:25). One hold,
released on EXIT (marker-verified `rm`):

| Hold | Marker | Acquired | Released | Driver |
|---|---|---|---|---|
| 1 | `MARKER=w4-residuals` pid=2494892 | 2026-08-13T13:29:07-04:00 | trap EXIT 13:30:00 | `/tmp/w4-spark-probe.py` via `/tmp/grok-w4/.venv` (PySpark 4.1.2) |

Lock was free after W-1's first release; no local `pyspark`/`SparkSubmit`.
No stale-rm. No second hold (R1+R5+Q-002 fit one session). `uv.lock` /
`Cargo.lock` untouched (`uv sync --locked --extra record`).

Live 4.1.2 (ANSI on, `local[2]`, zulu-17, `SPARK_LOCAL_IP=127.0.0.1`):

| Probe | Spark |
|---|---|
| R1 quoted / unquoted | `[10, 30, 60, 90, 90]` both |
| R4 FOLLOWING-to-FOLLOWING | `[30, None, 90, None, None]` |
| R5 unique `v` `INTERVAL '1' DAY` | `[10, 20, 30, 40, 50]` |
| R5 ties `0` / `1 DAY` / `1 HOUR` / `1 MONTH` / `1 PRECEDING` | `[10, 40, 40, 21, 30]` / `[10, 40, 40, 61, 30]` ×4 |
| R5 ties `10 DAY` | `[10, 50, 50, 61, 91]` |
| Q-002 `F.sum`/`count`/`avg`/`min`/`max`/`count_distinct`/`first`/`last(right["k"])` after leftsemi | all `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` |

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Pre-plan quote + R5 verdict | `crates/repark-spark/src/spark_ast.rs` | R1 quote before first plan; R5 restate arm; A5 comment |
| Classify + rewrite | `crates/repark-spark/src/window_range.rs` | `quote_unquoted_interval_range_bounds`; `RestateIntervalBoundsAsNumeric`; probe fires for any interval |
| Spark-door pins | `crates/repark-spark/src/tests/window_temporal_range.rs` | R1/R5 value pins + magnitude + µs type; R4 still 120 |
| Facade corpus | `python/repark/tests/test_window_parity.py` | R1/R5 flipped to equality; R4 disclosure note; magnitude + µs pins |
| Origin thread | `python/repark/src/repark/functions.py` | Q-002 on required aggs + `count_distinct` + `first`/`last` |
| Pins | `python/repark/tests/test_g4b_semi_join.py` | `test_right_ref_agg_*` family |
| This ledger | `task/w4-z-residuals-ledger.md` | linked from `task/map.md` |

`column.py`, `core.py`, registry, `_live_parity.py`, `Cargo.lock`, `uv.lock`
untouched.

---

## 2. Dispositions

| Item | Disposition | Evidence |
|---|---|---|
| **R1** unquoted `INTERVAL 1 DAY` | **FIX** | rust `temporal_range_unquoted_interval_literal_matches_quoted`; corpus row equality |
| **R4** FOLLOWING-to-FOLLOWING 120 vs 90 | **DEFER** | rust pin still 120; sqlparser EXCLUDE TBD; no pin bump |
| **R5** interval over numeric key | **FIX** | numeric `n` (unit ignored); unique-key row + ties magnitude pin |
| **Window ns** | **FIX** (tighten) | rust + facade µs type pins; no golden projected `ts` |
| **Q-002** aggregate origin | **FIX** | `test_right_ref_agg_*` + left / inner / distinct-name / `count_distinct` left-then-right |
| **A5** `spark_ast` comment | **FIX** (ride-along) | L69 refresh; named here |
| **G5b-R5 registry** | **HANDOFF** (text) | §6; numeric-`n` RANGE, not Y-1 self-group |

---

## 3. Record mode

R1/R5 Spark halves were already the live 4.1.2 tables (Z-4 / Y-1 record).
Only the repark pin / `repark_raises` flag moved — no golden rewrite.
Optional re-record under lock confirms the halves still bit-match (no
classifier edit). New facade magnitude / µs pins are repark-only (not
`WindowRow`).

---

## 4. Files touched

- `crates/repark-spark/src/spark_ast.rs`
- `crates/repark-spark/src/window_range.rs`
- `crates/repark-spark/src/tests/window_temporal_range.rs`
- `crates/repark-spark/src/map.md`, `crates/repark-spark/src/tests/map.md`
- `python/repark/src/repark/functions.py`
- `python/repark/src/repark/map.md`, `python/repark/src/repark/dataframe/map.md`
- `python/repark/tests/test_window_parity.py`, `python/repark/tests/test_g4b_semi_join.py`
- `python/repark/tests/map.md`
- `task/w4-z-residuals-ledger.md` (this file), `task/map.md`

No `column.py`, `core.py`, registry, `_live_parity.py`, `Cargo.lock`, `uv.lock`.

---

## 5. Gate results

Filled after the in-worktree runs. Real exit codes (`cmd > log 2>&1; echo $?`).

| Gate | Command | Exit | Result |
|---|---|---|---|
| targeted rust | `cargo test -p repark-spark temporal_range` | **0** | 15 passed (R1/R5 value + magnitude + mixed-loud + µs; R4 still 120) |
| targeted facade | `pytest python/repark/tests/test_g4b_semi_join.py python/repark/tests/test_window_parity.py` | **0** | **176 passed** |
| verify | `make verify` | **0** | `/tmp/w4-verify.log` — lint / fmt / clippy / structure gates / Rust workspace tests clean |
| preflight | `make preflight` | **0** | `/tmp/w4-preflight.log` — verify + facade **2978 passed**, 71 skipped + `cargo deny` / `pip-audit` / zizmor "No findings to report" |

---

## 6. Handoff for the registry (paste-true; this unit does NOT edit the registry)

> **W-4 Z-wave residuals (2026-08-13).** R1 and R5 close on the Spark door /
> facade `.sql()`. R4 stays OPEN. Q-002 aggregate origin-thread FIX. Window
> timestamp type pins tightened to µs. Pins:
> `crates/repark-spark/src/tests/window_temporal_range.rs`, the `temporal_range`
> family in `python/repark/tests/test_window_parity.py`, and
> `python/repark/tests/test_g4b_semi_join.py` (`test_right_ref_agg_*`).

### G5b-R1 — FIXED

- **repark** — unquoted `INTERVAL 1 DAY` as a frame bound is quoted to
  `INTERVAL '1' DAY` before first plan and matches the quoted table.
- **Apache Spark** — accepts both spellings (same table). Oracle: Z-4 live
  PySpark 4.1.2; rust/facade pins on this tree.
- **Pin** —
  `temporal_range_unquoted_interval_literal_matches_quoted` and the
  `temporal_range_unquoted_interval_literal` corpus row (now equality).

### G5b-R5 — FIXED (Spark half is numeric-`n` RANGE, not Y-1 self-group)

- **repark** — `INTERVAL 'n' UNIT` over a numeric order key is restated to
  unit-less `n` RANGE (unit ignored). Unique-key seed `[10,20,30,40,50]` is
  `n=1` on gaps of 10. Ties seed distinguishes magnitude
  (`1 DAY` → `[10,40,40,61,30]`; `10 DAY` → `[10,50,50,61,91]`;
  `0 DAY` → peer group).
- **Apache Spark** — the same tables (Z-4 live 4.1.2 under MARKER=`z4-residuals-r5`;
  W-4 re-probe under `w4-residuals` when the JVM lock is free).
- **Y-1's `[10,20,30,40,50]` "each unique key sees only itself"** is the
  unique-`v` special case of numeric `n=1`, not a distinct class. W-5 should
  land the row as FIXED with the numeric-`n` wording, not the self-group
  wording.
- **Pin** —
  `temporal_range_interval_bound_over_int_key_is_numeric_n`,
  `temporal_range_interval_bound_over_int_key_uses_numeric_magnitude`,
  corpus `temporal_range_interval_bound_over_int_key` (now equality),
  facade `test_temporal_range_interval_over_int_uses_numeric_magnitude`.

### G5b-R4 — still OPEN

- Both-bounds-`FOLLOWING` still includes the current row (120 vs Spark 90).
  DF 54.1.0 range-search at the pin. sqlparser 0.62 `EXCLUDE` is TBD. No
  dependency bump. Silent-wrong stays a recorded disclosure.

### G4b D6 residual Q-002 — FIXED

- **repark** — after `left.join(right, on, "leftsemi"|"leftanti")`,
  `select(F.sum|count|avg|min|max|count_distinct|first|last(right[…]))`
  raises `AnalysisException` carrying Spark 4.1.2's
  `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` (same-name) or
  `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT` (distinct-name).
  Left refs and inner-join refs still resolve. `count_distinct(left, right)`
  still raises on the unemitted right (join_sql QCOL scan, not first-origin-only).
- **Pin** —
  `python/repark/tests/test_g4b_semi_join.py::test_right_ref_agg_raises_missing_attributes_same_key`
  (and left / inner / distinct-name / `count_distinct` left-then-right siblings).

**Unit-queue rows this unit hands forward:**

| Row | Class | Note |
|---|---|---|
| G5b-R4 | `FOLLOWING`-to-`FOLLOWING` off-by-one | DataFusion range-search; pin bump or upstream |
| G5b-R3-ANSI | negative interval wrap on the ANSI door | G13 / ANSI-knob campaign body |

---

## 7. Authorship

Commits authored **TRO-Wolf** (`64240326+TRO-Wolf@users.noreply.github.com`) with the
`Authored-By: Grok (grok-4.5) <noreply@x.ai>` trailer, per-command `-c` identity only.
No co-author trailers, no session ids or URLs. `%ae` checked after every commit.

---

## 8. ACC + C4

**Risk tier:** standard. **Nearest AGENTS.md:** repo root. **Cycles used:** 1 / 1.
**Convergence:** `ACC-CONVERGED` (Critic-1 remediating cycle + Critic-2 + C4 clean ≥ S1).

### Actor build summary

- Slice: R1/R5 FIX; R4 DEFER after DF 54.1.0 + sqlparser 0.62 recon; µs type
  pins; Q-002 origin-thread; A5 comment; G5b-R5 numeric-`n` handoff.
- Files: `spark_ast.rs`, `window_range.rs`, rust/python window pins, `functions.py`,
  `test_g4b_semi_join.py`, maps, this ledger.
- Tests: 15 rust temporal_range; facade g4b+window 176; preflight 2978 / 71 skipped.
- Verify: `make verify` 0; `make preflight` 0.

### Critic-1 (Quality) — context break executed; attacking artifacts, not memory.

| ID | Sev | Claim | Disposition |
|---|---|---|---|
| Q-001 | S1 | `test_left_agg` required a non-NULL cell; `F.last` on leftanti (NULL key + k=2) can yield NULL. | **REMEDIATED.** Assert one global-agg row, not a non-NULL payload. |
| Q-002 | S1 | `test_inner_join_sum` on a condition join of two `k` columns is DataFusion-ambiguous on the native aggregate handle (`unqualified field k`). Not a semi-raise pin. | **REMEDIATED.** Name-key inner only; comment names the condition-join residual. |
| Q-003 | S2 | Mixed TIMESTAMP-interval + INT-interval would silently restate the datetime site if R5 were statement-blind. | **REMEDIATED.** `datetime_interval_sites` blocks R5; pin `temporal_range_mixed_datetime_and_numeric_interval_leaves_numeric_loud`. |
| Q-004 | S3 | `first`/`last` were not in A6's required set. | **ACCEPTED.** §0: same `_aggregate_argument` hole as `sum`. A6 allows them when §0 reds. Each rides the same parametrized pin. |

Coverage skeptic: drop `quote_unquoted_interval_range_bounds` → R1 rust + corpus red; drop `RestateIntervalBoundsAsNumeric` → R5 rust/magnitude red; drop `_thread_origin` on `sum` → `test_right_ref_agg[sum]` red; drop `join_sql_expr` only on `count_distinct` → `test_count_distinct_left_then_right_*` red.

Null reports: R4 still 120 at DF 54.1.0; `column.py` / `core.py` not in diff; map lockstep present.

Verdict after cycle 1: **CLEAN** (floor S1).

### Critic-2 (Security/Safety) — context break executed; attacking artifacts, not memory.

Attacked: secrets/credentials (diff has none); injection (`join_sql_expr` interpolates existing `join_sql_part()` QCOL tokens — same contract as `F.abs`); production panics (none added); unsafe (none); destructive ops (none); supply-chain (`uv.lock` / `Cargo.lock` / `.github/` untouched); numeric safety (R5 restates to the leading number Spark already uses; no new arithmetic kernel).

Null report: no SEC/SAF finding. **CLEAN.**

### Critic-4 (Claims/Record) — context break executed; attacking the paper.

| Class | Attack | Result |
|---|---|---|
| CL-MANDATE | R1/R5 value pins + Q-002 red-first pins in `test_g4b_semi_join.py`; R4 still disclosure; no registry edit | holds |
| CL-QUANT | required set named (`sum`/`count`/`avg`/`min`/`max`); `count_distinct` + `first`/`last` named as §0 same-hole, not "every wrapper" | holds |
| CL-STALE | R4 still OPEN in rust pin + corpus disclosure | holds |
| CL-TRANSCRIPT | verify 0 / preflight 0 / facade 2978 replayed | holds |
| CL-COUNT | 2922 (Z-4) → 2978 (Q-002 parametrize + 2 window pins) | holds |
| CL-GHOST | ledger citations resolve (`test_g4b_semi_join.py`, `window_temporal_range.rs`) | holds |
| CL-IDENTITY | checked after commit (`%ae` byte-exact) | pending commit |
| CL-RATIONALE | R5 is numeric `n` (Z-4 live + this-tree magnitude pin), not Y-1 self-group; G5b-R5 §6 says so | holds |
| CL-DUALHOME | rust + facade halves share one oracle for R1/R5/R4 | holds |
| CL-VACUOUS | R1/R5 equality rows have Spark tables; R4 disclosure still diverges | holds |

Corpus-failure taxonomy: two WindowRows flipped disclosure→equality (R1, R5) by moving `repark_raises` / keeping recorded Spark halves. No hollow flip.

Verdict: **CLEAN** pending post-commit CL-IDENTITY.

### High-tier role verdicts

n/a (standard tier).
