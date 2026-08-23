# Unit ledger — Z-4: Y-wave residuals (R1 / R4 / R5 + post-semi `F.abs`)

**Unit:** Z-4 Y-wave residuals · **Date:** 2026-08-13 · **Lane:** grok Z-4 ·
**Worktree:** `/tmp/grok-z4` · **Branch:** `grok/z4-y-residuals` ·
**Base (frozen):** `9b2dce3c73af402e8705923135d7de014da5501f` (#72 / G5b-R)

**Charter:** `planning/grok/BRIEF-z4-residuals.md` + conductor-5 A5 / A8 / A9 / A10.
**SEPMO:** STANDARD, acc + C4. Writable: `window_range.rs` +
`tests/window_temporal_range.rs` + `test_window_parity.py` + its record driver,
`python/repark/src/repark/dataframe/core.py`, `functions.py`, `test_g4b_semi_join.py`,
maps, this ledger. **CLOSED:** `spark_ast.rs` + `cross_door.rs` (Z-1 / A8), `column.py`
(A9), tz/decimal surfaces, G13 arithmetic, registry, `_live_parity.py`.

---

## 0. Recon (live Spark 4.1.2 under lock)

Two lock holds, both released on exit (marker-verified `rm`):

| Hold | Marker | Acquired | Released | Driver |
|---|---|---|---|---|
| 1 | `MARKER=z4-residuals` pid=3833058 | 2026-08-13T08:14:17-04:00 | trap EXIT after probe | `/tmp/z4-spark-probe.py` via `/tmp/grok-z2/.venv` (PySpark 4.1.2) |
| 2 | `MARKER=z4-residuals-r5` pid=3856897 | 2026-08-13T08:15:24-04:00 | trap EXIT after R5 magnitude | inline script, same venv |

Waited for `/tmp/grok-z2-probe-released` (present 2026-08-13T08:03:26-04:00). Lock was
free; no local `pyspark`/`SparkSubmit` (containerized HiveThrift ignored). Used Z-2's
existing venv so this tree's `uv.lock` was not touched. Basis: `local[2]`, ANSI on,
`spark.sql.shuffle.partitions=2`, UI off, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
`SPARK_LOCAL_IP=127.0.0.1`.

### 0.1 R1 — unquoted `INTERVAL 1 DAY`

- **Spark 4.1.2:** accepts; same table as quoted `INTERVAL '1' DAY`: `[10, 30, 60, 90, 90]`.
- **repark @ pin:** first-plan `INTERVAL expression cannot be Value(Number("1"))`
  (`datafusion-expr` `convert_frame_bound_to_scalar_value` — `SingleQuotedString` only).
- **Seam:** `rewrite_bare_range_bounds_to_days` runs only *after* `statement_to_plan`
  succeeds (`spark_ast.rs` `conform_temporal_range_frames`). The probe
  `statement_has_bare_range_bound` is `&Statement → bool`; it cannot rewrite or refuse.
  Quoting the Number before first plan is a call in `spark_ast.rs` (Z-1's file, A8).
- **Disposition:** **DEFER** (named reason: needs pre-plan rewrite in closed `spark_ast.rs`).
  Already loud. Use `INTERVAL '1' DAY`.

### 0.2 R4 — both-bounds-FOLLOWING 120 vs 90

- **Spark 4.1.2:** sum `[30, NULL, 90, NULL, NULL]`; count `[1, 0, 2, 0, 0]`.
- **repark @ pin:** still `[30, NULL, 120, NULL, NULL]`
  (`temporal_range_following_to_following_still_includes_current_row` green).
- **Seam:** planned frame is correctly typed
  `IntervalMonthDayNano { days: 1 } FOLLOWING … { days: 2 } FOLLOWING`. DataFusion 54.1.0
  `WindowFrameStateRange::calculate_range` includes the current row for id 3. sqlparser
  `WindowFrame` has `// TBD: EXCLUDE` — no `EXCLUDE CURRENT ROW` restatement. In-place
  plan rewrite of the bound renames the Window field and strands parent `Expr::Column`
  (G5b module doc). No `Cargo.lock` bump.
- **Disposition:** **DEFER** (DF range-search at the pin; not expressible on this seam).
  Silent-wrong stays a recorded disclosure. Refuse-loud was considered and rejected:
  Spark answers a table; inventing a refuse would be a new split, not Spark's class.

### 0.3 R5 — interval bounds over a numeric order key

Y-1 guessed Spark returns "each unique key sees only itself" (CURRENT ROW / peer group)
from the unique-`v` seed (gaps of 10). **Falsified.**

Ties seed `(id, v) = (1,10), (2,20), (3,20), (4,21), (5,30)`:

| Bound | Spark sum |
|---|---|
| `INTERVAL '0' DAY PRECEDING` | `[10, 40, 40, 21, 30]` (peer group / CURRENT ROW) |
| `INTERVAL '1' DAY PRECEDING` | `[10, 40, 40, 61, 30]` |
| `INTERVAL '1' HOUR PRECEDING` | same as 1 DAY |
| `INTERVAL '1' MONTH PRECEDING` | same as 1 DAY |
| `INTERVAL 1 DAY PRECEDING` (unquoted) | same as 1 DAY |
| `RANGE BETWEEN 1 PRECEDING AND CURRENT ROW` | same as 1 DAY |
| `INTERVAL '10' DAY PRECEDING` | `[10, 50, 50, 61, 91]` (numeric 10) |
| `CURRENT ROW AND CURRENT ROW` | `[10, 40, 40, 21, 30]` |

Spark 4.1.2 treats `INTERVAL 'n' UNIT` over INT as **numeric `n` RANGE**, **unit
ignored**. Matching it is: restate the interval to a unit-less `n` and re-plan.

- **repark @ pin:** still Arrow `Cannot cast string '1 DAY' to value of Int64 type`.
- **Seam:** classify *has* the key type, but the AST rewrite is type-blind and
  statement-wide. Converting every `INTERVAL 'n' UNIT` to `n` would turn the working
  TIMESTAMP path into a unit-less bound (G5b TIMESTAMP refuse). A new verdict /
  type-aware invoke lives in `spark_ast.rs` (closed). In-place plan rewrite strands names.
- **Disposition:** **DEFER** (Spark answers a table, not a refuse; value fix needs
  closed `spark_ast.rs`). Not refuse-loud: that would invent a class Spark does not raise.

### 0.4 `F.abs(right["k"])` after semi (Y-5 SAF-001)

- **Spark 4.1.2:** `select(F.abs(right["k"]))` after name-key leftsemi raises
  `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` (`Resolved attribute(s) "k"`).
  Distinct-name `F.abs(right["rk"])` raises
  `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT`.
  `F.abs(left["k"])` still resolves.
- **repark @ pin:** `functions.abs` built a fresh `Column(...)` and dropped
  `_origin_plan_id` / `_join_sql_expr`, so the CASE bound the left `k`.
- **Disposition:** **FIX** (A9 required). `column.py` untouched.

### 0.5 G13 handoff

Text only. See §6. No engine arithmetic edit.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Origin thread | `python/repark/src/repark/functions.py` | `_thread_origin` + `join_sql_expr` on `abs`, `_scalar`, `_date_fn`, `coalesce`, `concat`, `add_months`, `date_add` |
| Pins | `python/repark/tests/test_g4b_semi_join.py` | `F.abs` same-name / distinct-name / left / inner; `_scalar` `F.lower` ride-along |
| R1/R4/R5 comments | `window_range.rs`, `window_temporal_range.rs`, `test_window_parity.py` | Z-4 recon; no value flip |
| This ledger | `task/z4-residuals-ledger.md` | linked from `task/map.md` |

`core.py` was not edited — `_rebind_origin_column` already raises once origin is threaded.
`spark_ast.rs`, `column.py`, registry, `_live_parity.py` untouched.

---

## 2. Dispositions

| Item | Disposition | Evidence |
|---|---|---|
| **R1** unquoted `INTERVAL 1 DAY` | **DEFER** | first-plan DF error; rewrite is post-plan; `spark_ast.rs` closed |
| **R4** FOLLOWING-to-FOLLOWING 120 vs 90 | **DEFER** | DF 54.1.0 still 120; EXCLUDE TBD; no pin bump |
| **R5** interval over numeric key | **DEFER** | Spark = numeric `n` (unit ignored); type-aware rewrite needs `spark_ast.rs` |
| **`F.abs` post-semi** | **FIX** | threads origin; pins raise Spark's `MISSING_ATTRIBUTES.*` |
| **G13 ANSI RANGE wrap + F-Y10-1** | **HANDOFF** (text) | §6; no arithmetic edit |

---

## 3. Record mode

No window corpus flip (R1/R4/R5 stay disclosures; R2/R3 already equality). Record driver
not re-run. F.abs is a non-differential pin in `test_g4b_semi_join.py`, not a `WindowRow`.

---

## 4. Files touched

- `python/repark/src/repark/functions.py`
- `python/repark/tests/test_g4b_semi_join.py`
- `python/repark/src/repark/map.md`, `python/repark/src/repark/dataframe/map.md`,
  `python/repark/tests/map.md`
- `crates/repark-spark/src/window_range.rs` (module doc + recon only)
- `crates/repark-spark/src/tests/window_temporal_range.rs` (comments only)
- `crates/repark-spark/src/map.md`, `crates/repark-spark/src/tests/map.md`
- `python/repark/tests/test_window_parity.py` (disclosure notes only)
- `task/z4-residuals-ledger.md` (this file), `task/map.md`

No `spark_ast.rs`, `column.py`, `core.py`, registry, `_live_parity.py`, `Cargo.lock`,
`uv.lock`.

---

## 5. Gate results

Filled after the in-worktree runs. Real exit codes (`cmd > log 2>&1; echo $?`).

| Gate | Command | Exit | Result |
|---|---|---|---|
| targeted rust | `cargo test -p repark-spark temporal_range` | **0** | 12 passed (R1/R4/R5 still-open pins hold) |
| targeted facade | `pytest python/repark/tests/test_g4b_semi_join.py` | **0** | **73 passed** after the Q-001 negative-key pin |
| verify | `make verify` | **0** | `/tmp/z4-verify.log` — lint / fmt / clippy / structure gates / Rust workspace tests clean |
| preflight | `make preflight` | **0** | `/tmp/z4-preflight.log` — verify + facade then post-Q-001 re-run `/tmp/z4-facade-rerun.log` **2922 passed**, 71 skipped + `cargo deny` / `pip-audit` / zizmor "No findings to report" |

---

## 6. Handoff for the registry (paste-true; this unit does NOT edit the registry)

> **Z-4 Y-wave residuals (2026-08-13).** One FIX, three DEFERs, one text handoff.
> Pins: `python/repark/tests/test_g4b_semi_join.py` (`test_right_ref_abs_*` family) and
> the still-open `temporal_range_*` rows / Rust twins.

### G4b D6 residual SAF-001 — FIXED

- **repark** — after `left.join(right, on, "leftsemi"|"leftanti")`,
  `select` / `filter` / `withColumn` of `F.abs(right[…])` raise `AnalysisException`
  carrying Spark 4.1.2's `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION`
  (same-name) or `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT`
  (distinct-name). `F.abs(left[…])` and inner-join `F.abs(right[…])` still resolve.
  The same origin-thread rides `functions._scalar` (e.g. `F.lower`) and the other
  named Column wrappers in `functions.py`.
- **Apache Spark** — the same classes (oracle: live PySpark 4.1.2, 2026-08-13,
  MARKER=`z4-residuals`).
- **Pin** —
  `python/repark/tests/test_g4b_semi_join.py::test_right_ref_abs_raises_missing_attributes_same_key`
  (and the left / inner / distinct-name / `F.lower` siblings).

### G5b-R1 / R4 / R5 — still OPEN (Z-4 recon supersedes Y-1's R5 guess)

- **G5b-R1 — still OPEN.** Unquoted `INTERVAL 1 DAY` as a frame bound is still refused
  at first plan (`INTERVAL expression cannot be Value(Number…)`). Spark accepts it
  (same table as quoted). Fix needs a pre-plan AST rewrite in `spark_ast.rs`.
- **G5b-R4 — still OPEN.** Both-bounds-`FOLLOWING` still includes the current row
  (120 vs Spark 90). DF 54.1.0 range-search at the pin. sqlparser `EXCLUDE` is TBD.
  No dependency bump.
- **G5b-R5 — still OPEN (recon corrected).** An interval bound over a numeric order
  key still raises a raw Arrow cast. Spark 4.1.2 does **not** refuse and does **not**
  collapse to "self only": `INTERVAL 'n' UNIT` is numeric `n` RANGE, unit ignored
  (`1 DAY` = `1 HOUR` = `1 MONTH` = `1 PRECEDING`; `10 DAY` = `10 PRECEDING`;
  `0 DAY` = peer group). Y-1's unique-key seed could not see the magnitude. Matching
  Spark is a type-aware restatement invoked from `spark_ast.rs`.

### G13 campaign-body handoff (text only — Z-4 does not touch engine arithmetic)

> **ANSI-door RANGE wrap + F-Y10-1 integer wrap belong to the DEC/ANSI-knob campaign
> body, not this residual lane.**
>
> 1. **ANSI-door RANGE wrap (G5b-R3-ANSI).** Y-1 closed wrapping on the Spark door /
>    facade `.sql()` (`FILTER (WHERE false)` over a current-row frame; same-kind
>    magnitude invert refuses `SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON`). The ANSI
>    door does not call `window_range.rs` and still wraps (`count(*)` = −1 / debug
>    panic on inverted `INTERVAL '-1' DAY`). That is a named residual of the ANSI
>    door's arithmetic / sliding-window retract path — same campaign body as G13,
>    not a Spark-door seam fix. Pin already named in
>    `task/g5br-range-residuals-ledger.md` §6.
> 2. **F-Y10-1 integer wrap (both doors).** Y-10 observed
>    `CAST(2147483647 AS INT) + CAST(1 AS INT)` → Int32 `-2147483648` on **both**
>    doors (two's complement). Standard SQL shall raise. This is the integer analog
>    of DEC-6. Do not pin wrap as intended; do not "fix" one door to match the
>    other overnight. Carry onto G13 / the DEC campaign body with Y-10's §6 text
>    (`task/y10-ansi-door-ledger.md`). F-Y10-2 (ANSI float `/0` is IEEE Inf) stays
>    a FINDING, not this lane.
>
> Z-4 did not edit `crates/repark-functions` arithmetic, `analyzer.rs`, or any
> decimal/integer kernel.

**Unit-queue rows this unit hands forward:**

| Row | Class | Note |
|---|---|---|
| G5b-R1 | unquoted `INTERVAL n UNIT` frame bound | pre-plan rewrite in `spark_ast.rs` |
| G5b-R4 | `FOLLOWING`-to-`FOLLOWING` off-by-one | DataFusion range-search; pin bump or upstream |
| G5b-R5 | interval bound over a numeric key | restate `INTERVAL 'n' UNIT` → unit-less `n`; `spark_ast.rs` |
| G5b-R3-ANSI | negative interval wrap on the ANSI door | G13 / ANSI-knob campaign body |
| F-Y10-1 | integer arithmetic overflow wraps both doors | G13 integer analog of DEC-6 |

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

- Slice: four Y-wave residuals + G13 text handoff. F.abs origin-thread FIX; R1/R4/R5 DEFER
  after live Spark 4.1.2 + DF 54.1.0 recon; G13 paragraph only.
- Files: `functions.py`, `test_g4b_semi_join.py`, window comment/note updates, maps, this ledger.
- Tests: `test_right_ref_abs_*` family + lower / coalesce / string-name / negative-key inner.
- Verify: `make verify` 0; `make preflight` 0; facade **2922** passed / 71 skipped.

### Critic-1 (Quality) — context break executed; attacking artifacts, not memory.

| ID | Sev | Claim | Disposition |
|---|---|---|---|
| Q-001 | S1 | Inner-join `F.abs(right["k"])` on `k=1` stays green if `join_sql` is dropped and `_rebind` replaces the CASE with the leaf (`abs(1)==1`). | **REMEDIATED.** `test_inner_join_abs_keeps_the_abs_on_a_negative_key` (`k=-3` → `3`; filter `abs > 0`). |
| Q-002 | S3 | Aggregate builders (`F.sum` / `F.count` / …) still drop origin. `F.sum(right["k"])` after semi can name-bind left. | **ACCEPTED_FLAGGED.** A9 required `F.abs` and allowed other wrappers to ride along; aggregates were not in the cheap set. Named residual, not a silent claim of "every wrapper". |

Coverage skeptic: revert `_thread_origin` / `join_sql_expr` on `abs` → `test_right_ref_abs_*` reds; drop `join_sql` only → Q-001 pin reds; first-origin-only without join_sql → `test_coalesce_left_then_right_*` reds.

Null reports: logic (R1/R4/R5 still-open pins still hold at DF 54.1.0); closed files (`spark_ast.rs` / `column.py` / `core.py` not in diff); map lockstep present.

Verdict after cycle 1: **CLEAN** (floor S1).

### Critic-2 (Security/Safety) — context break executed; attacking artifacts, not memory.

Attacked: secrets/credentials (diff has none); injection (`join_sql_expr` interpolates existing `join_sql_part()` QCOL tokens — same contract as `column.py` `_binary`); production panics (Rust edits are comments only); unsafe (none); destructive ops (none); supply-chain (`uv.lock` / `Cargo.lock` / `.github/` untouched); numeric safety (no arithmetic kernel).

Null report: no SEC/SAF finding. **CLEAN.**

### Critic-4 (Claims/Record) — context break executed; attacking the paper.

| Class | Attack | Result |
|---|---|---|
| CL-MANDATE | F.abs pin + `MISSING_ATTRIBUTES` in `test_g4b_semi_join.py`; G13 text in §6; no `spark_ast.rs` | holds |
| CL-QUANT | "other wrappers" is named (`_scalar`, `_date_fn`, coalesce, concat, add_months, date_add), not "every" | holds; Q-002 flags aggregates |
| CL-STALE | R1/R4/R5 still OPEN in rust pins + corpus disclosures | holds |
| CL-TRANSCRIPT | verify 0 / preflight 0 / facade 2922 replayed | holds after count fix |
| CL-COUNT | 2920 → 2922 after Q-001 (2 parametrized cases) | **REMEDIATED** in §5 |
| CL-GHOST | ledger citations resolve (`test_g4b_semi_join.py`, `window_temporal_range.rs`) | holds |
| CL-IDENTITY | checked after commit (`%ae` across `grok/z4-y-residuals`) | pending commit |
| CL-RATIONALE / CL-DUALHOME / CL-VACUOUS | R5 defer reason re-derived from live probe (numeric `n`, not Y-1 self-only); rust pin still Arrow-cast | holds |

Corpus-failure taxonomy null: no new WindowRow; no classifier/golden edit; no hollow equality flip.

Verdict: **CLEAN** pending post-commit CL-IDENTITY.

### High-tier role verdicts

n/a (standard tier).
