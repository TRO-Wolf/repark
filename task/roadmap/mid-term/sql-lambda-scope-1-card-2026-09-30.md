# SQL-LAMBDA-SCOPE-1 — the spark.sql door scopes lambdas to the wrong rows

**Filed: 2026-09-30 under owner ruling OD-3 (ATTR-ID-1 work order).** Verifier finding **RC5-6**,
severity **S3**, pre-existing (identical on base and head). **Target:** mid-term. The
`DataFrame.filter` forms of the same predicates answer Spark; only the `spark.sql` path is
wrong. ATTR-ID-1 §3.8 names the SQL door's own ambiguity audit as out of scope and keeps its
mechanism, so this card is separate work.

## The divergence it records

Several `spark.sql` predicates over higher-order functions answer the wrong rows silently.
The SQL router's lambda scoping is outside the CASESENS-2 DataFrame binder, which the
verifier's fourth re-verify (Spark 4.1.2, base `adc26586`, head `ca4ac687`, 2026-09-29)
confirmed by measuring base and head byte-identical on every cell below.

Repro, verbatim from the verifier hand-back
(`/tmp/oc-worker/direct/wo/reverify4-cs2-opus-handback.json`):

```sql
SELECT id FROM lt WHERE exists(sa, s -> s.a > 2)          -- (F/T)
-- ... exists(arr, T -> T > 4) OR T > 5                   -- (F/T)
-- ... exists(arr, V -> v > 4)                            -- (T)
-- ... exists(arr, `X` -> X > 4)                          -- (F)
```

Measured behaviour (base and head identical; `(F)` / `(T)` are `caseSensitive=false/true`):

| Predicate | Spark | RePark base and head |
|---|---|---|
| `exists(sa, s -> s.a > 2)` (F/T) | `[1,2]` | `[2]` |
| `exists(arr, T -> T > 4) OR T > 5` (F/T) | `[1,5]` | `[1]` |
| `exists(arr, V -> v > 4)` (T) | `[1,2,5]` | `[1,5]` |
| `` exists(arr, `X` -> X > 4) `` (F) | `[1,5]` | `[5]` |

Probe cells: `p4` `{F,T}_lam_sql_16`, `F_lam_sql_17`, `{F,T}_lam_sql_52`, `T_lam_sql_33`,
`F_lam_sql_46`. Recorded in the CASESENS-2 ledger as residue R-CS2-21 (OPEN 2026-09-29).

## The verifier's suggested fix direction (a suggestion, not a decision)

A SQL-router lambda-scoping card: the `DataFrame.filter` forms are FIXED by the CASESENS-2
fold, so the fix belongs to the SQL door's own lambda scoping, not the DataFrame binder.

## Oracle cells to re-measure on the day

The four predicates above under both `caseSensitive` settings, plus the `DataFrame.filter`
equivalents as controls (they already answer Spark and must not move).

## Clauses (draft)

- **C-001** Each predicate above answers Spark's rows on the `spark.sql` door, both settings.
- **C-002** The `DataFrame.filter` equivalents still answer Spark's rows.
- **C-003** R-CS2-21 closes.

## Pointers

- Up: [map.md](map.md) · Verifier hand-back (evidence location):
  `/tmp/oc-worker/direct/wo/reverify4-cs2-opus-handback.json` (finding RC5-6)
