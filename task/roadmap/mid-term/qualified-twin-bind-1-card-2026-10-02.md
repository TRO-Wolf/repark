# QUALIFIED-TWIN-BIND-1 — record: single-id multi-hit qualified binds against Spark's two-attribute refusal

**Filed: 2026-10-02 from ATTR-ID-1 S3e** (source:
`/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`,
`out_of_scope_observed[5]` — the last residue; verified present
2026-10-02). **Status:** settled S1/C-038 select-dup model; the S3b pin
changed contract with record. **Target: revisit if a cell disagrees.**
This is a record card only; it charters no work.

## The divergence it records

Single-id multi-hit qualified binds (select/filter/sort) against Spark's
two-attribute refusal, under the settled S1/C-038 select-dup model from
S3d. The S3b pin that changed contract carries the record of the
decision.

## Repro

Not yet measured as a standalone cell set: the record lives in the S3b
pin that changed contract. Repro to run if a cell ever disagrees, against
live Spark 4.1.2:

```python
one = df.select("v", "v")
one.select("v").collect()
one.filter(F.col("v") > 0).collect()
one.orderBy("v").collect()
```

where the frame holds a single attribute id under two display hits;
compare Spark's two-attribute refusal with the settled bind.

## Scope

The record only: the S1/C-038 select-dup model and the S3b pin's
contract change, kept findable.

## Out of scope

- Everything: any bind change needs a new card with a disagreeing cell.
- Creation-dup multi-id unions
  ([createdataframe-dup-names-1-card-2026-10-02.md](createdataframe-dup-names-1-card-2026-10-02.md)).

## Clauses (draft)

- **C-001** The record names the S3b pin and the C-038 decision.
- **C-002** No code moves under this card.

## Pointers

- Up: [map.md](map.md) · Evidence (evidence location):
  `/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`
