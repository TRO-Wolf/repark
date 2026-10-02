# CREATEDATAFRAME-DUP-NAMES-1 — createDataFrame refuses duplicate column names where Spark mints distinct ids

**Filed: 2026-10-02 from ATTR-ID-1 S3d** (source:
`/tmp/oc-worker/direct/wo/attr-id-1/s3d/final-handback.json`, residue 2;
verified present 2026-10-02). **Severity:** refusal where Spark answers,
not yet rated (owner). **Status:** pre-existing creation-door gap.
**Target:** mid-term.

## The divergence it records

RePark's `createDataFrame` refuses duplicate column names where Spark mints
distinct ids. The S3d probe `s3d18` (creation-dup union) is unbuildable in
RePark, so the multi-id-union refusal was pinned via alias-dup instead.

## Repro

Not yet measured: the source names the shape but carries no
Spark-versus-RePark cell dump. Repro to run on the day, against live Spark
4.1.2:

```python
df = createDataFrame([(1, 2)], ["v", "v"])
df.columns
df.union(df).collect()
```

Spark builds the frame with two distinct attribute ids; RePark refuses at
creation.

## Scope

Duplicate-name creation on the `createDataFrame` door, with Spark's
distinct-id minting, through the downstream union that `s3d18` probes.

## Out of scope

- The C-038 select-dup single-id model (settled in S3d; untouched).
- The alias-dup pins that stand in for `s3d18` (they stay until this
  unit re-pins creation-dup directly).

## Clauses (draft)

- **C-001** The repro builds and answers Spark's rows and ids.
- **C-002** `s3d18` is re-pinned through creation-dup, not alias-dup.
- **C-003** Distinct-name creation is unchanged.

## Pointers

- Up: [map.md](map.md) · Evidence (evidence location):
  `/tmp/oc-worker/direct/wo/attr-id-1/s3d/final-handback.json`
