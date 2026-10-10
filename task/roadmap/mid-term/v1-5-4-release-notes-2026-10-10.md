# v1.5.4 release notes (2026-10-10)

Not tagged yet. The tag and the PyPI upload are the owner's; this file prepares them. Nothing
here is a go decision.

v1.5.4 is the fourth patch on 1.5.0. It follows v1.5.3 (`7a8fcf1a`, notes
[v1-5-3-release-notes-2026-10-07.md](v1-5-3-release-notes-2026-10-07.md)). It carries a
cross-join condition fix, two write-path fixes for nanosecond timestamps, the encryption-key
refusal for keyed tables, `COUNT(*)` on the changelog, incremental and lineage readers, the
pyarrow dependency floor, and the SQL door's `timestamp_seconds` family.

Tag target: the merge commit of this release pull request on `main` (the workspace version
moves to 1.5.4 in it). The last product commit is `e845b73b` (#1012). The tag and the PyPI
publication are the owner's.

## Cross-join condition (#1010, `153b6cb5`)

- A cross join with a condition answers as the inner join, as Spark does. Before this release
  the condition was dropped: `df.join(other, cond, "cross")` returned the Cartesian rows. The
  ledger records a 12-row answer where live Spark 4.1.2 answers 2.
- Every join door refuses a non-deterministic condition (for example `rand()`) and an untyped
  NULL condition, for every join type, with Spark's error class, condition and text:
  `INVALID_NON_DETERMINISTIC_EXPRESSIONS` for the first, `JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE`
  for the second. The DataFrame door and the SQL door both refuse.
- Ledger: [cross-join-condition-1-ledger.md](../../ledgers/staging/cross-join-condition-1-ledger.md)
  (C-001, C-004, C-008).

## Dependency floor (#1015, `4224aeee`)

- The pyarrow floor rises to `pyarrow>=25.0.1`. pyarrow 25.0.0 bundles a mimalloc that
  segfaults a first threaded `collect()`, `take()`, `head()`, `first()` or `toLocalIterator()`
  when pyarrow is not yet loaded on the main thread (five of six doors measured). The fault is
  upstream (apache/arrow GH-50471), fixed in 25.0.1.
- Ledger: [threaded-collect-segv-1-ledger.md](../../ledgers/staging/threaded-collect-segv-1-ledger.md).

## `COUNT(*)` over the changelog, incremental and lineage readers (#1017, `ca5a062a`)

- `COUNT(*)`, `COUNT(1)`, `SELECT 1`, `df.count()` and `EXISTS` answer the row count on the
  changelog reader, the incremental reader and the lineage reader. Before this release they
  failed with an engine-internal rebuild error. The shared batch conform now serves an empty
  projection, as the metadata-columns copy already did.
- The lineage `EXISTS` refusal is unchanged. Filtered counts, empty results and the plain, v3,
  snapshot and metadata counts answer as before.
- Ledger: [empty-projection-count-1-ledger.md](../../ledgers/completed/empty-projection-count-1-ledger.md).

## Nanosecond wall on every write door (#1018, R-007, `58bf67e3`)

Every write door stores the same session wall into a top-level `timestamp_ns` column, at full
nanosecond precision, equal to what `INSERT` stores. Before this release, in a session whose
zone is not UTC, `INSERT OVERWRITE`, every `MERGE` arm, `UPDATE SET`, `overwritePartitions` and
`insertInto(overwrite=True)` stored the UTC wall where `INSERT` stored the session wall. Thirty-eight
of 321 measured cells stored a wrong value.

### Behaviour changes

- **A nested `timestamp_ns` leaf refuses by name.** A write that supplies a value for a column
  whose type holds a naive `timestamp_ns` leaf below the top level (a struct field, an array
  element, a map key or value) refuses before any file is written, with
  `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]`. Before this release such a write stored a
  value, often the UTC wall instead of the session wall. Omit the column, or supply NULL for
  it. CTAS, RTAS and `CALL system.add_files` are not affected. Follow-up:
  [ice-tsns-nested-1-card-2026-10-09.md](ice-tsns-nested-1-card-2026-10-09.md)
  (ICE-TSNS-NESTED-1, parity row R-015).
- **`UPDATE` with no `WHERE` refuses a value narrowed from nanoseconds.** When the `SET` value
  was narrowed to microseconds, for example `array(c, NULL)[0]` or a `CASE` whose `ELSE` branch
  is an untyped `NULL`, the statement refuses by name. Before this release one such spelling
  stored a wrong value. The refusal is the narrow fix; the nanosecond-aware typing that turns
  these refusals back into stores is card
  [ice-tsns-coercion-1-card-2026-10-10.md](ice-tsns-coercion-1-card-2026-10-10.md). See also
  [ice-tsns-narrow-refuse-1-card-2026-10-10.md](ice-tsns-narrow-refuse-1-card-2026-10-10.md)
  and parity row R-017.
- **A malformed write on a table holding a nested leaf reports the nested refusal.** The
  parser's message is replaced by the refusal above. Lifting the refusal restores the parser's
  text (ledger question Q13).

Ledger: [ice-tsns-merge-wall-1-ledger.md](../../ledgers/staging/ice-tsns-merge-wall-1-ledger.md).

## Encryption key on a table (ENC-1, #1019, `9b230aed`)

- A table carrying `encryption.key-id` refuses its first write with
  `UnsupportedOperationException`. The message names the table, the property and ENC-1, and
  never echoes the key id. The refusal covers every write path of the survey: INSERT, INSERT
  OVERWRITE, `replaceWhere`, CTAS and RTAS, MERGE, UPDATE, DELETE, TRUNCATE, the DataFrame
  writers, the streaming sink, the CALL file writers and WAP publish.
- RePark is deliberately stricter than Spark 4.1.2 with Iceberg 1.11.0. Iceberg ignores the key
  on format v3 and writes plaintext. RePark refuses the first write on every format version.
  CREATE and scans keep working. This is a dated divergence row in the parity document.
- Metadata-only operations run: expiry, orphan sweep, pointer moves, `ALTER`, `SELECT`, and
  `UNSET` of the key.
- Known residue (card ENC-1-RESIDUE-1,
  [enc-1-residue-1-card-2026-10-09.md](enc-1-residue-1-card-2026-10-09.md)): a pointer move can
  publish rows that were staged in plaintext before the key was set; a table handle loaded before
  the key was added can stage one orphan file before its commit refuses.
- Ledger: [enc-1-ledger.md](../../ledgers/completed/enc-1-ledger.md).

## `timestamp_seconds`, `timestamp_millis`, `timestamp_micros` on the SQL door (#1012, `e845b73b`)

- The SQL door answers `timestamp_seconds`, `timestamp_millis` and `timestamp_micros` as Spark
  does.
- A decimal argument is exact at every boundary Spark answers, and the argument types Spark
  rejects refuse with `DATATYPE_MISMATCH`; overflow refuses as Spark does.
- Ledger: [sql-epoch-constructors-1-ledger.md](../../ledgers/staging/sql-epoch-constructors-1-ledger.md).

## Known issues carried to v1.5.5

- **R-008** (parity row ICE-TSNS-SQL-1-R-008): a `timestamptz_ns` target stores a wall as UTC on
  the overwrite and MERGE doors.
- **R-017** (card [ice-tsns-narrow-refuse-1-card-2026-10-10.md](ice-tsns-narrow-refuse-1-card-2026-10-10.md)):
  `coalesce(ns, NULL)` and similar spellings are cut to microseconds before the store.
- **R-011** (card [ice-ntz-nested-wall-1-card-2026-10-10.md](ice-ntz-nested-wall-1-card-2026-10-10.md)):
  a nested microsecond `TIMESTAMP_NTZ` stores the UTC wall through INSERT and the session wall
  through field assignment.

## Matrix

The 842-cell matrix on a fresh release build of `e845b73b` (scoreboard 2026-10-10, the v1.5.2
harness and compare rules; the version bump follows it and changes no behaviour) reads
**705 EQUAL / 132 SPARK-CANNOT / 5 REFUSED-REGISTERED / 0 DIFFERENT**, with no per-family
count moved against v1.5.3.
