# Card UNICODE-CASE-VERSION-1 — case folding follows Rust's Unicode tables; Spark's follow the JDK

**Date:** 2026-09-30 · **Filed by:** a Claude session (claude-fable-5-1) on the owner's ruling of the same day (ATTR-ID-1 OD-3) · **Source:** the second re-verify of PR #881, finding RC3-5 (S3), recorded as residue R-CS2-17 in `task/ledgers/staging/casesens-2-ledger.md` on the PR #881 branch (`feat/casesens-2-s4`, not yet on main) (34 cells: 14 regressions and 20 worse verdicts against main, all `F_uni_{vith,u14a,u16a,u16b}_*`).

## What was measured

`caseSensitive=false`. Column names built from code points that gained a case mapping in Unicode 14 or later: Vithkuqi `U+10570` / `U+10597`, `U+A7C0` / `U+A7C1`, `U+1C89` / `U+1C8A`, `U+A7CB` / `U+0264`.

| Operation on a frame with column `U+10570` | Spark 4.1.2 on JDK 17 | RePark (#881 head) |
|---|---|---|
| `df.drop('\U00010597')` | keeps the column | drops it |
| `df.withColumn('ᲊ', lit(0))` | appends a new column | replaces |
| `df.join(other, ['\U00010597'])` | `UNRESOLVED_USING_COLUMN_FOR_JOIN` | joins |
| `df.withColumns({'\U00010570': lit(7), '\U00010597': lit(8)})` | answers with three columns | `COLUMN_ALREADY_EXISTS` |

Cause (`crates/repark-common/src/names.rs`, `lookup_equal`, `java_char_equal`, `folded_duplicate`): Rust's `char::to_lowercase` / `to_uppercase` use the Unicode 16 tables. Spark's `equalsIgnoreCase` runs on the JVM's tables: Unicode 13 on JDK 17, Unicode 15 on JDK 21. So the set of case pairs that fold is a property of the deployment JDK, not of Spark, and RePark folds more pairs than any JDK Spark runs on today. On main the widening reached only `select` / `col` / `getitem`; #881 extended the fold to `drop`, USING keys, `unionByName`, `withColumns` duplicates, `dropDuplicates` and `orderBy`, which is why 14 cells regressed there.

## Why it is a card and not a fold

The answer depends on which JDK the reference Spark runs on, which is an owner decision about what RePark declares parity against, not a code fix that can be made inside a PR.

## Decisions (proposed)

| id | decision |
|---|---|
| D-1 | Declare the reference JDK for parity (the oracle box runs 17.0.15, Unicode 13). Record it in `docs/spark-sql-iceberg-parity.md` next to the Spark and Iceberg versions. |
| D-2 | `NameRule::IgnoreCase` masks case mappings for code points whose case was added after the declared JDK's Unicode version: a short exclusion table in `repark-common/src/names.rs`, generated from the Unicode `DerivedAge.txt` for the affected code points, with the Unicode version it targets in its name. |
| D-3 | Pin one Unicode 14 pair and one Unicode 16 pair under both settings on every API the CASESENS-2 ledger lists for R-CS2-17; the 34 cells replay EQUAL. |
| D-4 | If the owner would rather not track the JDK, the alternative is a dated residue stating the JDK dependence, and the 34 cells stay a named carve-out of the v1.5.0 parity gate. |

## Done condition

D-1 recorded; either D-2 + D-3 EQUAL, or D-4 ruled and written into the parity registry.
