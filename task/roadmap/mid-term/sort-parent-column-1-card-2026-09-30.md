# SORT-PARENT-COLUMN-1 — orderBy(parent Column) sorts by the output twin on case twins

**Filed: 2026-09-30 under owner ruling OD-3 (ATTR-ID-1 work order).** Verifier finding **RC5-7**,
severity **S3**, pre-existing (identical on base and head). **Target:** mid-term. Home is the
R-CS2-18 family (hidden sort keys through projections). Recorded in the CASESENS-2 ledger as
residue R-CS2-22 (OPEN 2026-09-29).

## The divergence it records

Under `caseSensitive=true`, `orderBy` with a parent `Column` on a case-twin frame sorts by
the output twin instead of the hidden parent attribute, returning rows in the wrong order.

Repro, verbatim from the verifier hand-back
(`/tmp/oc-worker/direct/wo/reverify4-cs2-opus-handback.json`):

```python
dd.select((dd.v + 1).alias('V'), (dd.v * -1).alias('v'), dd.id).orderBy(dd.v)
# also after filter/limit, and desc
```

Measured by the fourth CASESENS-2 re-verify (Spark 4.1.2, base `adc26586`, head `ca4ac687`,
2026-09-29): Spark sorts by the hidden parent `v` and answers
`[[null,null,2],[null,null,5],[11,-10,3],[21,-20,4],[31,-30,1]]`; base and head sort by the
output twin `v*-1` (reversed). Probe cells: `p4` `T_so casetw{,_filt,_lim} parent{,_desc}`
(6 cells).

## The verifier's suspected cause and suggested fix direction (a suggestion, not a decision)

The Column sort key rebinds by display name to the output `v` instead of the parent
attribute. The verifier suggests a separate card in the R-CS2-18 family: bind a parent
Column sort key to the attribute it was taken from, not to the display name it spells.

## Oracle cells to re-measure on the day

The six `T_so casetw` cells above (plain, after filter, after limit, each ascending and
descending), plus the string-key equivalents as controls.

## Clauses (draft)

- **C-001** `orderBy(dd.v)` on the repro frame answers Spark's rows in Spark's order.
- **C-002** The same holds after `filter` and after `limit`, ascending and descending.
- **C-003** R-CS2-22 closes.

## Pointers

- Up: [map.md](map.md) · Verifier hand-back (evidence location):
  `/tmp/oc-worker/direct/wo/reverify4-cs2-opus-handback.json` (finding RC5-7)
