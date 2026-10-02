# QUALIFIED-DISPLAY-NAME-1 — Spark strips qualifiers in compound display names, RePark keeps them

**Filed: 2026-10-02 from ATTR-ID-1 S3e** (source:
`/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`,
`out_of_scope_observed[1]` — "owner: display-naming follow-up card";
verified present 2026-10-02). **Severity:** naming divergence, not yet
rated (owner). **Status:** pre-existing, on main-identical display paths.
**Target:** mid-term.

## The divergence it records

Spark strips qualifiers in compound display names (`(v + v)`) and
`selectExpr` (`(v + 1)`); RePark keeps the written qualifier.

## Repro

Not yet measured: the source names the shapes but carries no
Spark-versus-RePark display dump. Repro to run on the day, against live
Spark 4.1.2:

```python
both = left.join(right, "k")
both.select(left["v"] + right["v"]).columns
both.selectExpr("v + 1").columns
```

Spark answers `(v + v)` and `(v + 1)`; RePark keeps the written
qualifiers.

## Scope

Qualifier stripping in compound and `selectExpr` display names, matching
Spark exactly.

## Out of scope

- The S3e qualified-name binding semantics (which attribute a name binds
  is settled; only the displayed name moves).
- Star expansion display (STAR-ERROR-SHAPE-1 owns the star shapes).

## Clauses (draft)

- **C-001** The repro columns read `(v + v)` and `(v + 1)`.
- **C-002** Qualification that Spark keeps is kept.
- **C-003** Bound attributes are unchanged (display only).

## Pointers

- Up: [map.md](map.md) · Evidence (evidence location):
  `/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`
