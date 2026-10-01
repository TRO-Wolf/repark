# UNICODE-CASE-VERSION-1 — case folding follows Rust's Unicode tables, not the deployment JDK's

**Filed: 2026-09-30 under owner ruling OD-3 (ATTR-ID-1 work order).** Verifier finding **RC3-5**
(second CASESENS-2 re-verify) / residue **R-CS2-17**, severity **S3**. **Target:** mid-term.
Recorded in the CASESENS-2 ledger as R-CS2-17 (OPEN 2026-09-29): "case pairs added in
Unicode 14 to 16 fold in RePark, whose `names.rs` uses Rust's Unicode 16 tables, but not in
Spark on JDK 17 (Unicode 13)". ATTR-ID-1 §3.8 files it here as a separate card, as the
verifier already said.

## The divergence it records

Case folding uses Rust's Unicode 16 tables, while JDK 17 Spark uses Unicode 13. Code points
that gained case mappings in Unicode 14 to 16 fold in RePark but stay distinct in Spark, so
`drop`, `withColumn`, USING join keys, `unionByName`, `withColumns` duplicates,
`dropDuplicates` and `orderBy` answer or refuse differently. `select` / `col` / `get` on the
same pairs already folded on main (a GAP, not a regression).

Repro, verbatim from the verifier hand-back
(`/tmp/oc-worker/direct/wo/reverify2-cs2-opus-handback.json`):

```python
df = createDataFrame([(1, 2)], ['\U00010570', 'v'])  # Vithkuqi; also U+A7C0/U+A7C1, U+1C89/U+1C8A, U+A7CB/U+0264
df.drop('\U00010597')
df.withColumn('\u1c8a', lit(0))
df.join(other, ['\U00010597'])
df.withColumns({'\U00010570': lit(7), '\U00010597': lit(8)})
df.orderBy('\u1c8a')
```

Measured by the second CASESENS-2 re-verify (Spark 4.1.2 on JDK 17.0.15, 2026-09-28/29):
base keeps the column / appends a new one / refuses the USING key (matching Spark); head
drops the column / replaces it / joins / raises `COLUMN_ALREADY_EXISTS`. Spark keeps the
column, appends a new one, refuses with `UNRESOLVED_USING_COLUMN_FOR_JOIN`, and answers
`withColumns` with three columns. Cells: 14 REGRESSION + 20 WORSE-VERDICT,
`F_uni_{vith,u14a,u16a,u16b}_*`.

The boundary moves with the deployment JDK: on JDK 21 (Unicode 15) the Unicode 14 pairs
would fold but the Unicode 16 pairs would not.

## The verifier's suspected cause and suggested fix direction (a suggestion, not a decision)

Suspected cause: `crates/repark-common/src/names.rs` (`lookup_equal` via `to_lowercase`,
`java_char_equal` via `char::to_uppercase`/`to_lowercase`, `folded_duplicate` via
`to_lowercase`) uses the Rust std Unicode tables (16.0), while the JVM Spark runs on uses an
older version. Suggested fix direction: mask case mappings whose code points gained case in
Unicode 14 or later — a short exclusion table keyed on the deployment JDK's Unicode version —
or file it as a dated residue with the JDK dependence stated. Pin one Unicode 14 and one
Unicode 16 pair.

## Oracle cells to re-measure on the day

One Unicode 14 pair and one Unicode 16 pair across `drop`, `withColumn`, USING join,
`unionByName`, `withColumns` duplicates, `dropDuplicates` and `orderBy`, under
`caseSensitive=false`, on the deployment JDK of the day.

## Clauses (draft)

- **C-001** The pinned Unicode 14 and Unicode 16 pairs answer the deployment JDK's Spark.
- **C-002** Pre-Unicode-14 folding still matches Spark (no over-masking).
- **C-003** R-CS2-17 closes or becomes a dated residue naming the JDK.

## Pointers

- Up: [map.md](map.md) · Verifier hand-back (evidence location):
  `/tmp/oc-worker/direct/wo/reverify2-cs2-opus-handback.json` (finding RC3-5) · Ledger residue
  (read-only): `/tmp/xs-cs1/task/ledgers/staging/casesens-2-ledger.md` (R-CS2-17)
