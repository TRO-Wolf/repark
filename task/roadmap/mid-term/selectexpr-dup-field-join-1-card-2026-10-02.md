# SELECTEXPR-DUP-FIELD-JOIN-1 — selectExpr over duplicate-field join frames fails at the scratch-view scan

**Filed: 2026-10-02 from ATTR-ID-1 S3e** (source:
`/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`,
`out_of_scope_observed[0]` — "owner: view/engine follow-up card"; verified
present 2026-10-02). **Severity:** failure where Spark answers, not yet
rated (owner). **Status:** pre-existing, on main-identical untouched paths.
**Target:** mid-term.

## The divergence it records

`selectExpr` over duplicate-field join frames fails at the scratch-view
scan, on paths S3e did not touch (main-identical).

## Repro

Not yet measured: the source names the shape but carries no
Spark-versus-RePark cell dump. Repro to run on the day, against live Spark
4.1.2:

```python
both = left.join(right, "k")
both.selectExpr("v + 1").collect()
```

with `left` and `right` each carrying a field `v`, so the joined frame
holds duplicate fields.

## Scope

The view/engine follow-up: the scratch-view scan (and whatever plans it)
must carry duplicate-field join frames through `selectExpr` as Spark does.

## Out of scope

- The S3e qualified-name binding cutover (shipped; its 56 facade pins
  stay green).
- Display naming of the resulting columns
  ([qualified-display-name-1-card-2026-10-02.md](qualified-display-name-1-card-2026-10-02.md)).

## Clauses (draft)

- **C-001** The repro answers Spark's rows.
- **C-002** `selectExpr` over distinct-field frames is unchanged.
- **C-003** The S3e qualified-bind pins stay green.

## Pointers

- Up: [map.md](map.md) · Evidence (evidence location):
  `/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`
