# Charter ledger — SEM-0 · the two silently wrong answers

**Retires (line added 2026-08-23 by DL-1's backfill):** this charter moves to `../completed/` when the tabled `LOG-1` row (SEM-2) is either chartered and merged or closed by an owner ruling — its measured scope lives here until then.

**Date:** 2026-08-21 · **Branch:** `fix/spark-semantics` · **Base:** `8c660f6` (`main`, post-#191) ·
**Rows in scope:** `RE-1` — **CLOSED by SEM-1**, so its row is retired from the registry; and
[`LOG-1`](../../../docs/spark-sql-iceberg-parity.md#log-1--sql-door-log-is-base-10-sparks-is-natural) —
**TABLED by the owner**, so its row stands

The low-risk sweep found two ordinary calls on common functions that return a plausible wrong
number, registered both as BACKLOG rows under its own invariant (no working query changes its
answer), and stopped. This ledger is the scope audit for the change that lifts that
invariant deliberately. **The gate was ruled on 2026-08-21** — see below; `RE-1` closes, `LOG-1`
is tabled.

Everything here was measured on `7d14a6f` against the live PySpark 4.1.2 oracle
([design §7](../../../docs/design/low-risk-sweep.md)), not inferred from the registry rows.

## The two units

### SEM-1 — `regexp_extract_all` defaults to capture group 1

**One knob, both doors.** `crates/repark-functions/src/spark_regexp.rs` `extract_rows`, the `None`
arm of the group resolution: `None => 0` → `None => 1`. The facade omits the third argument entirely
rather than defaulting it itself (`functions_expr.py` `regexp_extract_all`), so the Rust default is
the only default; there is no second site to keep in step.

Two things already exist and are not part of the work:

- **`regexp_substr` shares `extract_rows` and cannot be disturbed** — it binds the group as
  `_group` and never reads it, always returning `regex.find(text)`. The oracle confirms
  `regexp_substr('a1b2','([a-z])([0-9])')` is `'a1'` on Spark and on repark today.
- **The out-of-range raise is already written** (`invoke_extract_all`, the
  `group >= regex.captures_len()` arm) and already fires for `idx=3` on two groups and for `idx=1`
  on zero groups. The unit relies on it; it does not add it.

**The collateral is the work, and two of the three sites are not flagged anywhere in the repo.**

| Site | How it goes red |
|---|---|
| `python/repark/tests/test_lrs6_regexp_divergences.py::test_re1_extract_all_two_argument_form_returns_group_zero` | **By design** — the RE-1 pin. *(Outcome: both RE-1 pins were retired from that file rather than flipped; `test_sem1_extract_all_group_default.py` owns the assertions now, and the row left the registry.)* |
| `python/repark/tests/test_fnp6_regexp.py::…` (the `[0-9]*` stepping test, line ~137) | **A runtime error, not an assertion.** The pattern has zero capture groups, so an `idx` of 1 now raises. Wants an explicit `idx=0` — it tests the stepping walk, not the group default. |
| `python/repark/tests/test_fnp_critic_remediation.py::…` (the empty-pattern test, line ~97, **all four parametrizations**) | Same mechanism, same fix. |

Plus two docstrings that state the old default (`spark_regexp.rs` module docs, `functions_expr.py`
`regexp_extract_all`), the `RE-1` row (BACKLOG → deleted, per §6: a closed row leaves the registry),
and the STATUS line that names it.

**Land the adjacent defect in the same unit.** `F.regexp_extract_all(s, pattern, "1")` raises
`AnalysisException: No field named "1"` where Spark, repark's own SQL door, and repark's own
`F.regexp_instr(s, pattern, "0")` all accept the string. `regexp_instr` carries
`lit_indices=frozenset({} if isinstance(idx, Column) else {2})`; `regexp_extract_all` carries none.
It is a regression from the F-FNP6A-1 remediation, which stripped `lit_indices` entirely instead of
narrowing `{1, 2}` to `{2}` — see [fnp-6a-regexp-ledger.md](../archive/2026-08/2026-08-21-fnp-6a-regexp-ledger.md) and the
STATUS entry. Same function, same test pass, cheapest place to catch it.

**Known residual, in or out by the owner's call:** repark's out-of-range message is a generic
execution error where Spark's is `[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX]`. Pre-existing, but
this change makes it reachable from an ordinary two-argument call, so it stops being theoretical.

### SEM-2 — a Spark-semantics `log` kernel

**Not a one-liner, and not the shape the row first implied.** `datafusion-spark` 54.1.0 ships no
`log` at all, so the Spark door inherits DataFusion's base-10 `LogFunc`, registered by
`with_default_features()` in `crates/repark-core/src/session/df_guards.rs` before `register_all`
runs. The facade is right only because `crates/repark-python/src/column/function_dispatch.rs`'s
`"log" | "ln"` arm hardcodes `expr_fn::ln(...)` and never consults the registry — which is both why
the two disagree and why fixing the door cannot break the facade.

The work is a new `ScalarUDFImpl` (`SparkLog`) with `Signature::one_of` over one and two arguments,
registered by name over DataFusion's — the same overwrite-a-builtin pattern `SparkRand` already
uses in `crates/repark-functions/src/random.rs`, which is the template to copy. No ExprPlanner and
no analyzer rule are involved: the analyzer's only name-keyed rewrite is `substr`, for an unrelated
problem.

**The null-guard must cover both arities.** Measured 2026-08-21 — repark `log(0,8)` → `-0.0`,
`log(-2,8)` → `NaN`, `log(10,0)` → `-inf`, `log(10,-1)` → `NaN`, `log(0)` → `-inf`, `log(-1)` →
`NaN`; Spark returns **NULL** for all six (`Logarithm.nullSafeEval`). `log(1,8)` → `inf` on both.
A fix that redirects the one-argument form to `ln` and leaves DataFusion's two-argument formula in
place closes half the row and leaves the other half silently open.

**Two mechanical consequences to plan for, not discover:**

- `EXPECTED_DIVERGENCES` in `crates/repark-python/src/column/door_parity_tests.rs` loses its `log`
  row and its `len()` assertion moves 24 → 23, in the same commit, with the reason written there.
- The C-012 guard checks **kernel identity**, not just value. If the door's new kernel is not the
  same instance the facade resolves, the guard reds even though the numbers now agree. The clean
  answer is to point the facade's `"log"` arm at the new kernel too, which also closes the second
  adjacent defect below.

**Land the adjacent defect in the same unit.** `F.log` has no two-argument form. PySpark's
signature is `log(arg1, arg2=None)` (two-argument form is `log(base, x)`); repark's is `log(col)`,
and the dispatch arm has no two-argument case, so `F.log(2.0, col)` fails in Python before reaching
Rust. Once `SparkLog` exists it is what both the door and the overload need.

**Scope boundary:** the Spark facade's SQL door only. The native ANSI door (`repark.sql()`) is a
separate contract per [ADR-0002](../../../docs/adr/0002-two-sql-doors.md), where base-10 `log` is
defensible; changing it needs its own decision and is **out**.

## Propositions

| # | Clause | State | Evidence |
|---|---|---|---|
| C-101 | **Both defects reproduce on this tree.** | PROVEN | Measured on `7d14a6f` against the built wheel and pinned in-tree: `test_lrs6_regexp_divergences.py::test_re1_…` (RE-1) and `test_lrs4_door_domain.py::test_log1_sql_door_log_is_base_ten` + `::test_log1_the_two_argument_form_diverges_on_non_positive_operands` (LOG-1). |
| C-102 | **Every expected value is Spark's, taken from an independent oracle.** | PROVEN | Live PySpark 4.1.2 + JDK 17, the same oracle the LRS used; every value quoted above is transcribed from it, none read back out of repark. |
| C-103 | **RE-1's default has exactly one site.** | PROVEN | `extract_rows`'s `None` arm is the only default; the facade passes no third argument when the caller omits one, and the SQL door reaches the same kernel. Read on this tree, not assumed. |
| C-104 | **RE-1's collateral is enumerated, not estimated.** | PROVEN | Three test sites named above, two of which fail as runtime errors rather than assertion diffs and appear in no RE-1 document in the repo before this ledger. Found by reading every `regexp_extract_all` call site in `python/repark/tests/`. |
| C-105 | **LOG-1 cannot be fixed by redirecting to `ln`.** | PROVEN | The six non-positive measurements above: the two-argument form diverges on every one of them. The registry row's original "only the one-argument form diverges" claim was written from a single positive sample and was corrected in `7d14a6f`. |
| C-106 | **Fixing the door cannot break the facade.** | PROVEN | `function_dispatch.rs`'s `"log" \| "ln"` arm lowers to `expr_fn::ln` without consulting the registry, so a registry-level change is invisible to it. The reverse is the risk, and C-107 names it. |
| C-107 | **Closing LOG-1 moves a ratchet, and the move is planned.** | PROVEN | `EXPECTED_DIVERGENCES.len()` is asserted at 24; removing the `log` row requires editing the number in the same commit, which is where the reason must be written. The kernel-identity check is the second consequence and is named above. |
| C-108 | **This slate breaks the LRS invariant on purpose, and only there.** | PROVEN | Both units change a computed answer — that is the point, and it is why the LRS excluded them rather than deferring for effort. No other value changes: SEM-1 touches one default and one argument-coercion set; SEM-2 registers one name the Spark door currently resolves to a different function. |
| C-109 | **No forbidden surface.** | PROVEN | No AWS credential or environment, no `Cargo.toml [patch]`, no `.github/` change, no lockfile change, no secret in any output. |
| C-110 | **Ceiling risk is known before the edit.** | OPEN | `crates/repark-functions/src/lib.rs` sits at 173 of 175 lines, so SEM-2's `pub mod` line for the new module has two lines of headroom and the registration list must fit. Provable only when the module name and the registration shape are fixed, which is SEM-2's first step. |

**OPEN clauses: C-110.** It does not hold the gate — it is a mechanical constraint SEM-2 resolves
in its own first commit — but it is recorded so it is not discovered at pre-commit.

## APPROVAL_GATE

**RULED 2026-08-21.** First queued on the owner's instruction ("add that to the short term task
work"), which scheduled the work without authorizing the value change. The owner then ruled:

> "Lets table the LOG-1 issue and then fix the others as soon as possible"

Read against the three questions below:

1. **The value change is authorized for `RE-1` and refused for `LOG-1`.** `RE-1` closes and its row
   leaves the registry; `LOG-1` stays a BACKLOG row with its pins intact, and SEM-2 is **tabled,
   not dropped** — the scope in §SEM-2 above stands as written for whenever it is untabled.
2. **The adjacent defects go ahead**, minus one. The string-`idx` regression is SEM-3. The missing
   `F.log` two-argument overload is **tabled with SEM-2**: the only kernel available for it today
   is DataFusion's, the one without Spark's null-guard, so shipping the overload alone would trade
   a crash for an answer that is silently wrong on six edges — the exact failure mode this campaign
   exists to remove. Raised with the owner before starting.
3. **The `REGEX_GROUP_INDEX` message is its own unit**, SEM-4, and was sequenced FIRST rather than
   last: SEM-1 makes that refusal reachable from an ordinary two-argument call, so its pins had to
   assert the final wording rather than one about to change.

Delivery is one branch, one PR, matching the last two campaigns.

**Owner ruling 2026-08-31 (gate pass):** both RE-1 and LOG-1 are fixed to Spark
semantics. The 2026-08-21 tabling of LOG-1 is lifted. Delivery is
[sem-1-spark-answer-parity-ledger.md](sem-1-spark-answer-parity-ledger.md). The
measured scope in §SEM-1 and §SEM-2 above stands.

## Unit roster, as ruled

| Unit | Scope | State |
|---|---|---|
| **SEM-4** | The regexp refusals say Spark's words; the four kernels name themselves | Delivered |
| **SEM-1** | `RE-1` — the two-argument `regexp_extract_all` defaults to group 1 | Delivered |
| **SEM-3** | The string-`idx` regression from F-FNP6A-1 | Queued |
| **SEM-2** | `LOG-1` + the `F.log` overload | **TABLED** by the owner |

The three questions the gate was held on, kept for the record:

1. **Do these two answers change?** `regexp_extract_all(s, p)` starts returning group 1, and
   `spark.sql("SELECT log(x)")` starts returning the natural log. Any caller relying on today's
   behavior sees a different number with no error. Both rows argue the change is right — Spark is
   the contract, and a plausible wrong number is worse than a loud one — but it is a break, and
   the registry's rule is that a BACKLOG row closes on a decision, not on convenience.
2. **Do the two adjacent defects ride along?** Recommended yes for both: same functions, same test
   passes, and SEM-2's kernel is what the missing `F.log` overload needs anyway.
3. **Is the `REGEX_GROUP_INDEX` error-message residual in or out?** Recommended out of SEM-1 as its
   own small unit — it is a message, not a value, and bundling it widens a unit that is otherwise
   one line plus its collateral.

Delivery is **manual PR** per standing process: the orchestrator prepares and reviews, the owner
merges.

## Sizing

SEM-1 is an afternoon — one line, three test sites, two docstrings, one row retired, one adjacent
defect. SEM-2 is a day — a new kernel with a real semantics table, the ratchet move, and the
kernel-identity question. They are independent and can land in either order.

## A scoping caveat, recorded

The investigation behind this ledger ran five parallel lenses and **one failed** — the dedicated
oracle-truth-table lens hit its structured-output retry cap and returned nothing. Its measurements
were covered by the other four, and every value quoted here was independently re-run against both
engines afterwards. Recorded because "five lenses agreed" would be the wrong summary: four did, and
one produced no output.
