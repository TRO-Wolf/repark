# PLAN-DEPTH-1 — deep filter chains segfault the process (stack overflow)

**Filed: 2026-09-30 under owner ruling OD-3 (ATTR-ID-1 work order).** Verifier finding **RC5-5**,
severity **S3**, pre-existing (identical on base and head). **Target:** mid-term.
**Covered by PR #892 (DEEP-FILTER-CHAIN-CRASH-1); this card closes when #892 merges.**
Its remaining work is to re-run the repro below, with string and Column filters under both
`caseSensitive` settings, on main after that merge.

## The divergence it records

`count()` over 200 or more chained `DataFrame.filter` calls kills the process with SIGSEGV
(exit code 139, stack overflow). 150 chained filters pass.

Repro, verbatim from the verifier hand-back
(`/tmp/oc-worker/direct/wo/reverify4-cs2-opus-handback.json`):

```python
f = base
for i in range(200):
    f = f.filter(f'a >= {i%7-10}')
f.count()
```

150 filters pass; string or Column filters, both settings. Script:
`/tmp/oc-worker/direct/wo/reverify4-cs2/segv.py`.

Measured by the fourth CASESENS-2 re-verify (Spark 4.1.2, base `adc26586`, head `ca4ac687`,
2026-09-29): Spark answers; base exits 139 at 200, 300 and 1000 chained filters; head exits
139 at the same three depths. The cause is plan recursion depth in optimizer or physical
planning, not the CASESENS-2 binder.

## Orchestrator measurement, 2026-09-30

The PR #892 head (`cb8c8b8d`, DEEP-FILTER-CHAIN-CRASH-1) answers 50 at both 200 and 1000
chained string filters. Base v1.5.1 (`adc26586`) dumps core on both.

## The verifier's suggested fix direction (a suggestion, not a decision)

Convert the recursive plan walks, or run execution on a large-stack thread. File filed
separately at the verifier's request; the unit that owns it is PR #892.

## Clauses (draft)

- **C-001** `count()` over 200 and 1000 chained string filters answers Spark's value.
- **C-002** The same holds for chained Column filters, under both `caseSensitive` settings.
- **C-003** No `EQUAL` cell in the gate matrix moves.

## Pointers

- Up: [map.md](map.md) · Verifier hand-back (evidence location):
  `/tmp/oc-worker/direct/wo/reverify4-cs2-opus-handback.json` (finding RC5-5) · Repro script
  (evidence location): `/tmp/oc-worker/direct/wo/reverify4-cs2/segv.py`
