# Unit ledger — SEM-1 · Spark-answer parity for RE-1 and LOG-1

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the
orchestrator's departure move). This file closes when SEM-1 merges, or when the
owner closes the slate row.

**Unit:** SEM-1 · **Date:** 2026-08-31 · **Executor:** Grok (grok-4.6), Actor ·
**Branch:** `feat/sem-1-spark-answer-parity` · **Base:** `be2d754066e4ab3a5d61b4ec32418a10b8a31804`
**Charter:** [sem-0-charter-ledger.md](sem-0-charter-ledger.md) (measured scope).
**Registry:** [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
`RE-1` (already retired by the 2026-08-21 SEM-1) and `LOG-1`.
**risk_tier:** standard.

**Owner ruling, 2026-08-31:** both RE-1 and LOG-1 are fixed to Spark semantics.
SEM-0 was held at its approval gate for this dated ruling; it is now given.
The 2026-08-21 ruling that tabled LOG-1 is superseded for LOG-1 only. RE-1's
value change was already authorized 2026-08-21 and delivered as
`task/ledgers/archive/2026-08/2026-08-21-sem-1-extract-all-group-default-ledger.md`
(PR #193). This unit re-measures that default against live PySpark 4.1.2, then
closes LOG-1.

This unit changes what a working query returns on the Spark door.

## Scope (charter, followed exactly)

1. **RE-1** — `regexp_extract_all` two-argument default is capture group 1.
   One site: `extract_rows` `None` arm in `crates/repark-functions/src/spark_regexp.rs`.
   On this tree that arm is already `None => 1`. Collateral the charter named:
   the RE-1 pin, the `[0-9]*` stepping test, the empty-pattern critic test
   (four parametrizations). Adjacent string-`idx` defect is SEM-3 (already
   delivered). `REGEX_GROUP_INDEX` wording is SEM-4 (already delivered).
2. **LOG-1** — Spark-door `log` is natural log, dual-arity, Spark null-guard
   on both arities. New `ScalarUDFImpl` (`SparkLog`), not a redirect to `ln`.
   Ratchet: drop `log` from `EXPECTED_DIVERGENCES`, `len()` 24 → 23. Point the
   facade `"log"` arm at the same kernel. Land `F.log(base, expr)`. Native
   ANSI `repark.sql()` `log` stays base 10 (ADR-0002; out).

Oracle: live PySpark 4.1.2. Measure every assertion, including incidental
controls, before flipping a pin. HALT on unmeasured collateral.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Owner ruling 2026-08-31 authorizes both value changes to Spark semantics. | Dated ruling in this ledger and in the LOG-1 registry row. | **OPEN** |
| C-002 | RE-1 default is capture group 1 on both Spark doors. Explicit `idx=0` still returns the whole match. A pattern with no groups raises `REGEX_GROUP_INDEX` on the two-argument form. Null input propagates. `regexp_substr` is untouched. | Live-oracle table plus facade and Spark SQL pins. | **OPEN** |
| C-003 | The three charter collateral sites match Spark after the default. No unnamed `regexp_extract_all` call site in `python/repark/tests/` goes red. | Named tests plus a call-site grep recorded here. | **OPEN** |
| C-004 | Spark-door `log(expr)` is the natural log. `log(base, expr)` is log at that base. Both arities return NULL on the Spark domain edges (zero, negative, null, base 1, base <= 0) measured on live PySpark 4.1.2. Not a redirect of one-arg `log` to `ln` that leaves DataFusion's two-arg formula in place. | Kernel plus Spark SQL and facade pins, value AND Arrow type. | **OPEN** |
| C-005 | `EXPECTED_DIVERGENCES` drops its `log` row in the same commit as the kernel. `len()` moves 24 → 23 with the reason written there. | `door_parity_tests.rs`. | **OPEN** |
| C-006 | Facade `"log"` arm embeds the same `SparkLog` instance the Spark SQL door registers. `F.log` accepts PySpark's `log(arg1, arg2=None)`. `F.ln` stays `ln`. | Kernel-identity pin plus two-arg facade pin. | **OPEN** |
| C-007 | Native ANSI `repark.sql()` `log` stays DataFusion base-10. This unit does not register `SparkLog` on the extension-less session. | Native-door pin of `log(8)`. | **OPEN** |
| C-008 | Registry `LOG-1` moves to FIXED with the 2026-08-31 ruling date and the measured evidence. Prior LOG-1 pins that asserted the divergent answer flip red-first to the Spark answer. `RE-1` stays retired (already gone). STATUS / docs truth-up only for what this unit changed. | Registry row + flipped pins + STATUS SEM / `F.log` lines. | **OPEN** |
| C-009 | Gates before done: `make verify`, `make check-map-sync check-ledger-grammar`, `python3 scripts/ledger_lifecycle.py check --base be2d754066e4ab3a5d61b4ec32418a10b8a31804`, `make py-test`, and `make py-test-facade` because the facade is touched. Real exit codes. | Recorded at close. | **OPEN** |
| C-010 | Live PySpark 4.1.2 measurements (RE-1 edges, LOG-1 both arities, both reachable Spark doors, native-door control, incidental `log2`/`log1p`/`ln`) are transcribed here before any pin flip. | Oracle transcript in this ledger. | **OPEN** |

## Sequence

1. This ledger (grammar-gate clean, verdicts OPEN) — this commit.
2. Measure live PySpark 4.1.2. Record C-010. HALT on unnamed collateral.
3. RE-1: re-confirm the default and incidental controls; flip remaining
   divergent pins only if any still assert group 0.
4. LOG-1: `SparkLog` kernel, register_all overwrite, facade arm, `F.log`
   two-arg, ratchet, red-first pin flip.
5. Registry FIXED + STATUS truth-up. Gates. Verdicts flip when pins exist.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-sem-1-ledger
  agent: Actor
  action: File the SEM-1 staging ledger and lockstep staging/map.md, no kernel code yet
  charter_trace: SEM-0 SEM-1 and SEM-2 scope; owner ruling 2026-08-31
  preconditions:
    - AGENTS.md read path and engineering-method: SATISFIED
    - Branch is feat/sem-1-spark-answer-parity at be2d754: SATISFIED (git)
    - Disk headroom: SATISFIED (452 G free of 1.8 T)
    - RE-1 default already None => 1 on this tree: SATISFIED (spark_regexp.rs extract_rows)
    - LOG-1 still DataFusion base-10 on the Spark SQL door: SATISFIED (EXPECTED_DIVERGENCES log row)
  success_condition: staging ledger exists, staging/map.md links it, check-ledger-grammar accepts OPEN clauses
  step_risks:
    - Re-breaking the already-delivered RE-1 default: HANDLED(C-002 re-measure; do not edit the None arm unless it diverges)
    - Redirecting one-arg log to ln and leaving two-arg unguarded: HANDLED(C-004 forbids it)
    - Changing native ANSI log: HANDLED(C-007 out)
    - Widening past charter collateral: HANDLED(HALT on unnamed red)
  contingencies:
    - Revert this commit if grammar-red: EXECUTABLE(additive git revert)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```
