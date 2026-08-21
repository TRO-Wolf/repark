# Unit ledger — Critic round 1 · adversarial review of the whole branch

**Date:** 2026-08-20 · **Branch:** `feat/spark-function-parity` · **Base:** `5d69153` ·
**Charter:** [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) ·
**Mode:** two independent Critic passes, **hard context break** — separate fresh agents (R3(d)'s
preferred form, not the procedural break this repo defaults to), Opus tier per the standing owner
grant, distinct primary lenses (correctness/parity, and claims/failure-modes/safety).

Both passes returned **NOT_CONVERGED**. 25 findings, **10 at or above the S1 floor**, one **S0**.

## What the break bought

Neither agent wrote the code. Both were restricted to the charter, the diff, the tests and the
taxonomy, and both filed their findings **before** opening any unit ledger — so the narrative that
produced the defects could not prime the search for them. Three of the ten blockers were in work
this branch had already called done and gated green, and the S0 was in the unit whose own test
file claimed the seam was proven.

That is the case for the hard break, made concretely: `make preflight` was green over every one of
these.

## S0 — and where it led

**F-CSP-1: nested higher-order functions returned an exactly inverted boolean, silently.**

`_build_lambda` named parameters purely by arity, so an inner lambda minted the same plan name as
the outer one and `resolve_lambda_variables` bound the inner body to the OUTER variable.

```
exists(a, x -> exists(b, y -> y > 4))   over ([1,2],[5,6]) / ([9],[1])
  observed [False, True]      Spark [True, False]
```

Chasing it produced a finding neither critic could have reached from the diff alone. Giving the
parameters unique plan names fixed the binding and surfaced an **upstream DataFusion 54.1 limit**:
a nested lambda over real columns cannot be evaluated at all, and fails identically through
DataFusion's own SQL planner —

```
SELECT array_any_match(a, x -> array_any_match(b, y -> y > 4)) FROM t
  => Field of physical LambdaVariable with index 0 doesn't match batch field
     Field { "y": Int32 } != Field { "x": Int32 }
```

so no way of building the expression avoids it. Remediation is therefore both halves: unique plan
names (which removes the silent wrongness) **and** a loud refusal naming the upstream limit, so a
user gets a sentence instead of a physical-plan error. Registry row and an upstream report are
FNP-Z items.

## Blocking findings and dispositions

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| F-CSP-1 | **S0** | Nested higher-order silently inverted | `REMEDIATED` — unique plan names + loud refusal; upstream limit measured |
| F-CSP-2 / F-CFS-4 | S1 | `ascending=True` discarded an explicit null marker | `REMEDIATED` — PySpark's `_sort_cols` re-marks only on a **falsy** flag; all four combinations pinned |
| F-CSP-3 / F-CFS-3 | S1 | Empty pattern: `regexp_count` said 4, `regexp_extract_all` said `[]` | `REMEDIATED` — `collect_matches` now mirrors the counting walk's empty-pattern arm |
| F-CSP-4 / F-CFS-2 | S1 | `PyDataFrame::aggregate` never bound lambda variables | `REMEDIATED` — routed through `bound`; `groupBy`/`agg` pinned |
| F-CSP-5 / F-CFS-9 | S1 | `xxhash64` unary where PySpark is variadic | `REMEDIATED` — signature was the only thing blocking it |
| F-CFS-1 | S1 | `randstr` had no length cap — SIGABRT, not an error | `REMEDIATED` — bounded, refused loudly, pinned |
| F-CFS-5 | S1 | `approx_count_distinct` materialized `UInt64` while `schema` said bigint; arithmetic became `DECIMAL(21,0)` | `REMEDIATED` — cast to `Int64` at the expression level |

## Three of these were in work already called done

Worth naming rather than filing quietly:

1. **The `ascending=` fix from FNP-2 was wrong, and wider than either critic stated.** Its ledger
   recorded that it had been applied *before* the evidence arrived and was "right by luck". It was
   not right. PySpark's `_sort_cols` re-marks a column only in the falsy branch; the premise
   written into the code comment and the ledger was invented, and both critics cited the upstream
   source independently.

   Fixing it properly showed the defect reached further than the finding: `_apply_ascending_override`
   was *also* changing DIRECTION on a truthy flag, which PySpark treats as a complete no-op. So
   `orderBy("v", ascending=False)` — about as ordinary a PySpark call as exists — was returning
   nulls in the wrong position. `_sort_specs` now mirrors `_sort_cols` line for line: a falsy entry
   replaces the column's marker with `desc()` (descending, nulls last), a truthy entry changes
   nothing at all. Five cases pinned.
2. **The `bound` totality claim was false.** Its docstring said "every method that hands a
   `PyColumn` to DataFusion goes through here" while `aggregate` did not — and the FNP-4a test was
   honest about covering four of five sites while missing that a sixth existed. The claim is what
   stopped the gap being looked for; it now names the sites and the grep that defines the domain.
3. **The C-012 ratchet guard was half vacuous.** Every row needing more than one argument fell
   through a `continue`, so about half the table was never checked — a guard manufacturing
   confidence. Rewritten to classify each row by measured shape (`Kernel(arity)` vs `Composed`)
   and to FAIL on a row it cannot check. It immediately found `array_has` already agreeing, a
   second stale row after `cardinality`. **The live divergence set is 19, not the 18 reported.**

## Below the floor — shipped flagged (R6)

Fixed anyway because they were cheap or touched verification integrity: F-CSP-10 / F-CFS-11 (five
working functions still documented "Unsupported"), F-CSP-11 / F-CFS-8 (the ratchet above).

Carried as `ACCEPTED_FLAGGED`, each in the PR description: F-CSP-6 (three Spark spellings reachable
only through the facade), F-CSP-7 (unseeded `randstr`/`uniform` are deterministic — seed 0),
F-CSP-8 (`map_from_arrays` last-wins on duplicate keys where Spark raises), F-CSP-9 (`listagg`
rejects `Column` delimiters), F-CFS-6 (the higher-order builder splices an unquoted display string
into `sql_expr`), F-CFS-7 (a join whose condition holds a higher-order function fails), F-CFS-10
(`grouping` docstring vs behaviour), F-CFS-12/13 (error-message naming; window-key tri-state).

## Undischarged claims

Both critics ran the step-4 audit. Beyond the three above, the recurring one: six S2 divergences
across FNP-3/5/6a/6c are dispositioned *"registry row handed to FNP-Z"*, and **no registry row
ships in this PR** — a promise, not an artifact. FNP-Z owns them; until it runs, the disposition
overstates what exists. Charter C-003/C-004 were marked `PROVEN` as *enumeration complete*, not as
*delivered* — only `exists` of the eleven ships — and a reader can take `PROVEN` for shipped.

## Gates after remediation

| Gate | Result |
|---|---|
| `cargo test --workspace --no-fail-fast` | **45 binaries, 1,990 passed, 0 failed**, cargo exit 0 |
| `make ci` | exit **0** |
| facade pytest (full) | **3,531 passed, 70 skipped, 0 failed** |
| remediation pins | 15, in `test_fnp_critic_remediation.py`, each red before its fix |

## The remediation made the code smaller

Two of the mechanical gates forced the fix toward a better shape rather than merely permitting it:

* `check_lib_py` reded at `core.py` 7233/7225. The cause was the now-dead
  `_apply_ascending_override`, left behind when `_sort_specs` was rewritten to mirror PySpark.
  Deleting it: **7215**.
* `check_rust_file_size` reded at `column/mod.rs` 1207/1200. The nested-lambda guard did not
  belong in the `#[pymethods]` module anyway; moving it to `expr_build.rs`, where the other
  expression-construction helpers live: **1172**.

Neither ceiling was raised. Both tables ratchet down only, and in both cases the sanctioned out —
split the module — was also the right design.
