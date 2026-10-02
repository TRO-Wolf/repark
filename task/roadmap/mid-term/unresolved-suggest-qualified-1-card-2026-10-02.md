# UNRESOLVED-SUGGEST-QUALIFIED-1 — Spark's UNRESOLVED suggestion lists are qualifier-qualified

**Filed: 2026-10-02 from ATTR-ID-1 S3e** (source:
`/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`,
`out_of_scope_observed[3]` — "owner: suggestion-list follow-up card";
verified present 2026-10-02). **Severity:** error-text divergence, not
yet rated (owner). **Status:** pre-existing, unchanged from main.
**Target:** mid-term.

## The divergence it records

Spark's UNRESOLVED suggestion lists are qualifier-qualified; RePark
echoes bare names.

## Repro

Not yet measured: the source names the shape but carries no
Spark-versus-RePark error dump. Repro to run on the day, against live
Spark 4.1.2:

```python
both = left.alias("l").join(right.alias("r"), "k")
both.select("zzz").collect()
```

Compare the full suggestion-list text: Spark qualifies each candidate
with its alias, RePark echoes bare names.

## Scope

Qualifier-qualified candidates in UNRESOLVED suggestion lists, with
Spark's exact text.

## Out of scope

- Which candidates are suggested (ranking is settled elsewhere; only
  the qualification moves).
- Star error classes
  ([star-error-shape-1-card-2026-10-02.md](star-error-shape-1-card-2026-10-02.md)).

## Clauses (draft)

- **C-001** The repro's suggestion text matches Spark exactly.
- **C-002** Unqualified frames suggest exactly as today.
- **C-003** One test row per door.

## Pointers

- Up: [map.md](map.md) · Evidence (evidence location):
  `/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`
