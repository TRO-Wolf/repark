# v1.5.7 release notes (2026-11-22)

Every change on `origin/main` after the `v1.5.6` tag (`ae60c359`), one line per user-visible change, on `913fbd53`.
The range holds 129 commits: 42 fixes and one feature, two dependency or supply-chain changes, and 84 docs, test, CI and harness commits grouped at the end. Each line names its unit and PR; the unit's card or registry row holds the detail.

## Data-safety fixes

- **A partitioned path overwrite under dynamic mode refuses (PLAIN-DYNAMIC-OVERWRITE-GUARD-1, #1402).** With `partitionOverwriteMode=dynamic`, a partitioned `overwrite` to a plain path (Parquet, JSON, CSV or text) replaced the whole directory: partitions the write did not touch were deleted, and an empty input emptied it. Every effective DYNAMIC partitioned path overwrite now refuses before any effect. This includes the forms 1.5.6 answered as Spark does (an absent target, an empty directory, an input covering every partition). Workaround: use an Iceberg table, or overwrite one partition's own directory path.
- **Casts over Parquet statistics no longer answer wrong or drop rows (CAST-STATS-SILENT-1, #1405).** The `max` and `min` of a cast that reorders values (a STRING id to INT, BIGINT to STRING or BOOLEAN) no longer come from the Parquet footer; on 1.5.6, ids `10, 3, 25, 7` gave `7, 10` for `25, 3`. A narrowing float cast (to an integer type, or DOUBLE to FLOAT) no longer prunes a row group that holds a NaN, so `CAST(d AS INT) = 0` and `CAST(d AS FLOAT) > 100` return the NaN row. Workaround on 1.5.6, for `max` and `min` only: `try_cast` in place of `CAST`, or `datafusion.execution.collect_statistics` set to false.
- **Two loud internal errors on casts over statistics are gone (CAST-STATS-INTERVAL-1, #1405).** A cast's carried column statistics no longer fail a filter with an internal interval assertion, and a full-range BIGINT bound no longer breaks filter selectivity.
- **`unix_timestamp` and `unix_seconds` round as Spark does before 1970 (UNIX-TIMESTAMP-TRUNCATE-1, #1408).** `unix_timestamp` truncates, and `unix_seconds` and `unix_millis` floor, so an instant before 1970 with a fraction of a second is no longer off by one; a job that stored whole minutes from `(unix_timestamp(end) - unix_timestamp(start)) / 60` stored a value one minute off. `F.unix_seconds` and `F.unix_millis` no longer round through DOUBLE after about 2242. The one-argument `unix_timestamp` answers as its two-argument form with the default pattern does.
- **A native-door DataFrame write that would store nothing refuses before any effect (NATIVE-DATAFRAME-WRITE-SILENT-1, #1331).** A `repark.sql(...)` frame written to an existing Iceberg table, an in-memory table, or a Parquet, CSV or JSON path returned without an error and stored nothing, and a path overwrite deleted the rows already there. The write now refuses and names the working route; the 1.5.6 notes give those routes.
- **Struct comparisons pair fields by position, as Spark does (STRUCT-COMPARE-BY-POSITION-1, #1292; STRUCT-COMPARE-SEAT-FAMILY-1, #1295).** A comparison of a struct column with a struct whose fields are declared in another order paired the fields by name, so `SELECT`, `UPDATE`, `DELETE` and `MERGE` acted on the wrong rows. The 1.5.6 workaround (write the literal in the column's field order) is no longer needed.
- **A held Iceberg frame or view re-binds to the session's table after a same-session write (ICE-HELD-FRAME-SNAPSHOT-1, #1390 and #1391).** A frame or view held across the session's own write answered the snapshot bound at the hold and stored old rows, silently. It now takes its table at each build.
- **A lazy cache refills after a same-session commit to a table it read (ICE-HELD-FRAME-SNAPSHOT-1 unit 2, #1394).**
- **Signed zeros and NaN store as Spark does (ZERO-PARTITION-MERGE-1 #1332, NAN-WRITE-CANONICAL-1 #1325, NAN-PARTITION-VALUE-1 #1319).** `-0.0` and `0.0` rows keep two FLOAT or DOUBLE partitions on every write door; RePark stores every NaN as Spark does, per data-file format; an INSERT's NaN or infinite FLOAT or DOUBLE partition value is stored as itself.

## Stored values and reads that change

- **Plain Parquet NaN reads as NaN (NAN-MINMAX-1 part 1, #1333).** Float `max` and `min` no longer answer from the file footer.
- **A FLOAT or DOUBLE stored into a DECIMAL column rounds its JDK 17 text half up (DOUBLE-DECIMAL-STORE-ROUND-1, #1335).** Stores only.
- **`round` and `bround` of a FLOAT or DOUBLE round the value's JDK 17 text (ROUND-DOUBLE-EXACT-1, #1337).**
- **Float comparisons against zero and two constant zeros match both signs (SIGNED-ZERO-NORMALIZE-1 part A, #1298; SIGNED-ZERO-CONST-COMPARE-1, #1336).** Flat float keys group, join and deduplicate with one zero and one NaN (SIGNED-ZERO-NORMALIZE-1 part B part 1, #1326).
- **A narrowing cast compared with a constant keeps its cast, as in Spark (CAST-LITERAL-UNWRAP-1, #1345).**
- **A decimal sum and `try_sum` stay exact in every phase and check once at the end (SUM-DEC-I128WRAP-1, #1360).**
- **A decimal `avg` and `mean` round half up at scale plus four and follow the ANSI flag (AVG-DECIMAL-ROUND-1, #1355).**
- **An integer literal beside a decimal takes its digits in every operator (DECIMAL-DIVIDE-LITERAL-1, #1361).**
- **Decimal pairs past 38 digits take Spark's least common type on the Spark door (DECIMAL-COMMON-TYPE-1, #1350).**
- **A DECIMAL beside a FLOAT or DOUBLE unifies to double (DECIMAL-DOUBLE-UNION-COERCION-1 parts 1 and 2, #1339, #1341).** A mixed DECIMAL and DOUBLE `VALUES` is double, unions of three or more branches widen, and `IN` lists of doubles plan as decimals.
- **`hour`, `minute` and `second` of a STRING read the text as the instant, as Spark does (STRING-HOUR-MINUTE-SECOND-1, #1370).**
- **A CSV schema's TIMESTAMP field parses under the session zone (CSV-SCHEMA-TIMESTAMP-ZONE-1, #1367).**
- **CSV fields are read as text and parsed by Spark's field converters, paired by position (CSV-FIELD-KERNEL-1, #1384; CSV-STRING-FIELD-TEXT-1 wiring, #1388).**
- **A JSON or CSV reader's parse runs under the session that reads it (TEMP-VIEW-FRAME-READER-PARSE-1 unit 1, #1373).**
- **An `F.expr` `ORDER BY`, and a string filter's subquery `ORDER BY`, place NULLs as Spark does (FEXPR-WINDOW-NULL-ORDER-1, #1316; FILTER-STRING-NULL-ORDER-1, #1321).**
- **A copy-on-write `UPDATE` answers in any batch order (UPDATE-SCHEMA-BATCH-MISMATCH-1, #1327).**
- **A Hadoop drop and recreate reads and writes the new table (LOCAL-DROP-RECREATE-STALE-WRITE-1, #1313).**
- **A CTAS whose query fails leaves no metadata behind (CTAS-QUERY-FAILURE-CLEANUP-1, #1303; AUD-025).**
- **A key-name join on a non-hashable key joins on it instead of returning the cross product (LISTVIEW-INNER-JOIN-KEY-1, #1304).**
- **A lazy `cache()` fills at its first use, under the live settings (TEMP-VIEW-CACHE-LAZY-PARSE-1 unit A1, #1386).**

## Behaviour changes

- **SQL `INTERSECT ALL`, `EXCEPT ALL` and `MINUS ALL` refuse at the statement seat (SQL-SETOP-ALL-REFUSE-1, #1314).**
- **Set operations refuse struct operands whose field names differ in order or set (STRUCT-SETOP-REORDER-REFUSAL-1, #1283), and the set-operation name check is kind-invariant across Arrow list kinds (STRUCT-SETOP-TYPE-FAMILY-1, #1290).**
- **`repark.sql()` refuses a CTAS into an Iceberg table (NATIVE-CTAS-ICEBERG-REFUSE-1, #1317).**
- **A SQL temporary view answers under the settings captured at `CREATE` (TEMP-VIEW-SESSION-SETTINGS-1 part 1, #1359), and a DataFrame temporary view under its frame's own settings (part 2, #1362).**
- **A held frame over a view takes its table at its build (ICE-HELD-FRAME-SNAPSHOT-1 unit 1a follow-up D1, #1391).**

## Lock-only and supply-chain changes

- **`url` 2.5.9 for RUSTSEC-2026-0592 (lock only, #1401).**
- **Three unmaintained advisories ignored for chrono, chrono-tz and humantime, with reasons in `.cargo/map.md` (#1351).**

## Docs, tests, CI and harness (no user-visible change)

64 docs and records commits, 16 test commits (among them the acceptance and parity pins for the units above), one CI runner change for the Rust test job (#1309), and three harness fixes to the map-size ratchet and the lockstep warnings (#1374, #1375, #1376).

## Known issues carried to the next release

- **Spark-written INT96 timestamps read as zone-less (PARQUET-INT96-AS-NTZ-1, S1, open).** Values and dates shift outside a UTC session; in UTC only the type differs. Card: `parquet-int96-as-ntz-1-card-2026-11-22.md`.
- **A sorted DataFrame is written in its source order (WRITE-ORDER-DROPPED-1, S2, card, not dispatched).** A sort before a Parquet or Iceberg write is dropped; values and row counts are equal. Card: `write-order-dropped-1-card-2026-11-22.md`.
- **FNP-8 residuals (BACKLOG).** Nullability and higher-order result metadata that retains inherited nullable fields.
- **Identifier case folding (DECLARED, 2026-08-10).** Registry row ID-1.
- **The session-timezone family.** TZ-8 is partly fixed; `last_day` and `date_add` over a TIMESTAMP stay BACKLOG; TZ-4 is in progress; F-V4-1 and F-V4-2 are DECLARED and fork-routed. Registry TZ rows.
- **decimal128.** DEC-9 and the DEC-5 nullability stay BACKLOG; TY-3 is DECLARED.
- **Temporal RANGE frames.** G5b-R4 is OPEN (FOLLOWING to FOLLOWING); the ANSI-door wrap is a named residual with no pin.
- **DELETE and UPDATE subquery predicates.** The `IN`, `NOT IN` and `EXISTS` forms execute on both doors; `UPDATE NOT IN`, `[NOT] EXISTS`, correlated `UPDATE IN`, `ANY` and `ALL` stay valved. Registry G3-E8 rows.
- **`bin` and `rint` over-accept BOOLEAN (BL-6, BACKLOG).**
- **`bit_length` and `octet_length` stringify DOUBLE values differently (Infinity, E-notation) (BL-7, BACKLOG).**
- **Regexp match-counting residual families (GT1-FIX #180, no disposition).** Four narrow families still diverge from Java's matcher, and ANSI-off conditional semantics are not modelled.
- **Numeric implicit-cast breadth on string-function arguments (no disposition).** Spark casts a numeric first argument to a string and a non-integer numeric index; RePark refuses both at planning.
