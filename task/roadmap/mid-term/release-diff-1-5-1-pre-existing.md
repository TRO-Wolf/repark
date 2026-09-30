# RELEASE-DIFF-1-5-1-PE: pre-existing wrong answers found by the v1.5.1 release differential

**Filed:** 2026-09-28 by the orchestrating session, from the Opus-high whole-release differential (v1.5.0 9392dbc3 against main adc26586, about 5,330 statements, verdict CLEAN_FOR_TAG).
**Target:** v1.5.2, and the post-v1.5.1 differential audit of main.
**Evidence:** the hand-back `release-1-5-1-diff-opus-handback.json`; the probes and per-engine results under the release lane's `target/release-diff/`.

All of these answer the same on v1.5.0 and on the v1.5.1 candidate; none were introduced by v1.5.1. PE-1 and PE-2 (S1, silent TIMESTAMP ↔ TIMESTAMP_NTZ walls on BY NAME, OVERWRITE and MERGE) are not listed here: they are being fixed as NTZ-STORE-DOORS-1.

| Id | Severity | Difference | Spark |
|---|---|---|---|
| PE-3 | S2, silent | CSV and JSON path writes of TIMESTAMP in a non-UTC session emit the UTC wall; CSV carries no offset | `2024-03-10T03:30:00.000-04:00` |
| PE-4 | S2, silent | `nvl(DATE, TIMESTAMP)` answers STRING and drops the time (`coalesce` is right) | TIMESTAMP |
| PE-5 | S2, silent | Under caseSensitive=true, DataFrames built from tables bind names ignoring case, so `withColumn("ID", …)` overwrites `Id` | appends a new column |
| PE-6 | S2 | Under caseSensitive=true, `writeTo().append()` accepts respelled or extra columns | refuses `EXTRA_COLUMNS` |
| PE-7 | S2 | Reading partitioned parquet (local and s3a) drops the partition column | partition column present |
| PE-8 | S3 | RePark refuses where Spark answers: `WITH … INSERT`; `UPDATE SET` from a scalar subquery; `MERGE … WHEN NOT MATCHED BY SOURCE`; `DIV`; `timestamp_seconds`/`timestamp_millis`/`nvl` stored into TIMESTAMP; struct-field case (`info.city` against `City`); `F.col("l.id")` after an alias join; `SELECT id, id`; MERGE with exact names under true; case-twin CREATE/CTAS under true (the last one is RP-56, #879) | answers |
| PE-9 | S3 | RePark stores what Spark refuses: TIMESTAMP into BIGINT, and STRING into DATE through VALUES | refuses |
| PE-10 | S1 | A double-quoted string with a backslash-escaped quote, `SELECT "x\\""y"`, returns `x\""y` on RePark; Spark returns `x\"y` (found by the #882 re-verify 3) | `x\"y` |
| PE-11 | S3 | UPDATE, DELETE and MERGE whose SQL holds a backslash before a quote (`'x\\''y'`) refuse with a TokenizerError, even on a STRING-only table (found by the #882 re-verify 3; site not located) | runs |
| PE-12 | S1 | Mixed STRING/DECIMAL coercion widens to DECIMAL where Spark uses DOUBLE: `CASE`/`nvl` of `'1.123456789012345678'` and a DECIMAL into DECIMAL(38,18) stores 1.123456789012345678; Spark stores 1.123456789012345700 (10 cells; found by the #885 re-verify, RT-5) | Spark's DOUBLE-rounded value |
| PE-13 | S1 | 89 mixed-type coercion cells return wrong values, for example `coalesce('1.5', CAST(… AS DECIMAL(38,0)))` stores 2.0 where Spark stores 1.5 (#885 re-verify, RT-8; overlaps NVL-TYPE-COERCION-1) | 1.5 |
| PE-14 | S2 | STRING-source store assignment: STRING into DATE/numeric through VALUES and writers stores where Spark refuses, and mixed STRING conditionals type as STRING where Spark widens. Removed from #885 to ship v1.5.1; card STORE-STRING-ASSIGN-1 (v1.5.2), which should land after NVL-TYPE-COERCION-1 | refuses / widens |
| PE-15 | S2 | `count()` after 200 or more chained `filter` calls segfaults the process on main (150 passes); found by the #881 re-verify 4 (RC5-5, `segv.py` in `reverify4-cs2/`) | answers |
| PE-16 | S1 | `CAST('2024-03-10 02:30:00' AS TIMESTAMP_NTZ)` shifts a New York DST-gap wall to 03:30 even outside VALUES; an NTZ wall has no zone and must stay 02:30 (#886 re-verify 2; card NTZ-DST-GAP-CAST-1) | 02:30 |
| PE-17 | S3 | CSV writes leave an empty non-null string unquoted (`1,`) where Spark writes `1,""`, so empty and NULL collapse in the file bytes (#889 DIFF-PROBE W180, residue R-3; card CSV-EMPTY-QUOTE-1) | `1,""` |
| PE-18 | S1 | `INSERT INTO t SELECT a, b FROM VALUES (1, TIMESTAMP'…') AS v(a, b)` (and `SELECT * FROM (VALUES …)`) stores epoch seconds into BIGINT where Spark refuses; the VALUES-node door #885 does not gate (#885 re-verify 3, RT3-2) | refuses |
| PE-19 | S1 | Double literals beyond 2^53 misround: `9007199254740993.0D` does not produce Spark's double (#891 verifier) | Spark's double |
| PE-20 | S2 | Storing a DOUBLE into MAP<STRING,INT> panics in Rust; STRUCT/ARRAY integral overflows give raw Arrow text instead of Spark's error class (#891 verifier) | Spark's error class |
| PE-21 | S2 | `-NULL` beside a TIMESTAMP_NTZ in an inline table stores NULL where Spark refuses INVALID_INLINE_TABLE. Main already stores it on VALUES, INSERT SELECT and writeTo().append(); on BY NAME, OVERWRITE and MERGE main failed only through an Arrow crash, which #886 fixes. Cause: the #885 gate fires only on a Null-typed column (`negated_null_store.rs:62`); #886 re-verify 4, RD5-1 | refuses |
| PE-22 | S1 | STRING targets on the BY NAME, OVERWRITE and MERGE doors store timestamp text as ISO `2024-03-10T02:30:00`; Spark stores `2024-03-10 02:30:00` (#886 re-verify 4, RD5-4) | space-separated text |
| PE-23 | S1 | `DATE + INTERVAL '1' HOUR` types DATE and drops the hour; Spark gives a TIMESTAMP (#886 re-verify 4, RD5-2) | TIMESTAMP with the hour |
| PE-24 | S1 | `-NULL` into a TIMESTAMP column through `INSERT OVERWRITE … PARTITION (p='a') VALUES` stores NULL where Spark refuses; #885 does not gate that door (v1.5.1 release delta) | refuses |
| PE-25 | S2 | `- -N`, with a space between the signs, fails to parse on UPDATE, MERGE, BY NAME and every OVERWRITE form; INSERT VALUES and INSERT SELECT accept it, and `-(-N)` works everywhere (v1.5.1 release delta) | parses |
| PE-26 | S2 | DEFAULT fails in MERGE, UPDATE and inline tables; `'a\\\'b'` fails to tokenize on BY NAME, UPDATE and OVERWRITE PARTITION VALUES; `coalesce(ntz, ts)` into TIMESTAMP/NTZ refuses where Spark stores (v1.5.1 release delta) | stores |

Also noted:
- **Test-harness gap:** `tests::common::setup` (the native-door runner) does not register the NTZ cast function or the fractional-division rule, so native UPDATE/MERGE into NTZ fail there. The facade and the real session door are correct.
- **S3 write gaps:**
  - a case-twin write under true fails with an internal temp-table error;
  - SQL `parquet.\`s3a://…\`` reads fail;
  - partition directories keep the frame's spelling (`Grp=x`) where Spark lowercases (`grp=x`).
- **Performance (from #885):** a VALUES body of non-literal cells (for example 2,000 rows × 7 columns of `abs(i)` into BIGINT) plans one probe per cell: 1.71× on a debug build. Fix: plan the VALUES once and read each column's type (RT3-1).
- **Performance:** large VALUES bodies take 5–14 s on both sides. A 2,000-row VALUES with integer division takes 3.85 s against 1.16 s for plain INT.

**Done condition:** each row gets a unit card or an owner ruling. The silent rows (PE-3, PE-4, PE-5, PE-10, PE-12, PE-13, PE-16, PE-18, PE-19, PE-22, PE-23, PE-24) come first.
