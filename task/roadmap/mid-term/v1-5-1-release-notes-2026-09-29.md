# v1.5.1 release notes (2026-09-29)

v1.5.1 is the first patch on 1.5.0. Every change is additive under the API freeze.

## S3 path writes (U12, 7bbb8e95)
- DataFrame path writes land on `s3://` and `s3a://` with Spark's save modes.
- The s3a endpoint keys are honoured: `fs.s3a.endpoint`, `.endpoint.region`, `.path.style.access` and `.connection.ssl.enabled`.
- Verified live on the release candidate: the tier-2 AWS acceptance run on main 8568e57a (run 36601549520, 2026-09-29) passed 11 of 11, plus the dbt gold stage on Glue. It covers Glue and S3 Tables, and the U12 scratch-prefix path write, which stores 6 rows after an append, as Spark does. An earlier run on 5fb38051 (run 36558939045) passed the same set.

## Name resolution follows spark.sql.caseSensitive (CASESENS-1, 5323ec45, 2fa34a5b)
Both the SQL and DataFrame doors now resolve names by the session's case rule. This covers:
- case twins;
- nested SQL scopes;
- CTE outputs;
- catalog views;
- Iceberg DDL names.

## TIMESTAMP_NTZ and TIMESTAMP store assignment
- **NTZ-1** (3777e0de, f710d885): stores into TIMESTAMP_NTZ columns keep Spark's walls and refusals.
- **LTZ-STORE-INT-1** (13df1635): an INT stored into a TIMESTAMP column refuses `CANNOT_SAFELY_CAST`. It no longer writes epoch microseconds.
- **LTZ-STACKED-SIGN-1** (#882, d415da76): a numeric literal with stacked unary signs refuses into TIMESTAMP and TIMESTAMP_NTZ. String cells that hold a backslash before a quote store exactly.
- **STORE-TS-TO-NUMERIC-1** (#885, e6144f5f): TIMESTAMP, TIMESTAMP_NTZ and DATE sources into numeric or BOOLEAN columns refuse, as Spark does, on every write door. They no longer store epoch values. `-NULL` into TIMESTAMP, DATE and BOOLEAN refuses too.
  - The merged commit subject also names STRING sources. The STRING refusals were removed before merge; they are carded for v1.5.2 as STORE-STRING-ASSIGN-1.
- **NTZ-STORE-DOORS-1** (#886, 8568e57a): BY NAME, the INSERT OVERWRITE family, overwritePartitions, the MERGE arms and VALUES that mix TIMESTAMP and TIMESTAMP_NTZ now store through the session zone, as Spark does.

## Arithmetic (INTDIV-1, adc26586)
Integer division stays fractional, as in Spark. That holds in the same scope and through derived tables, CTEs and views.

## Fork
RP-55 (87316e78): a staged replace writes its metadata once. This fixes the S3 Tables AWS acceptance 412.

## Performance note
A large VALUES body whose cells are expressions rather than literals, for example a 10,000-row VALUES of computed cells, plans one probe per cell. It is up to 1.3–1.7× slower to write than on 1.5.0; plain literal VALUES and INSERT … SELECT are unchanged. Carded for v1.5.2.

## Release differential
The v1.5.0 → adc26586 differential (5,331 statements) and the adc26586 → 8568e57a delta (1,768 facade statements, 30 on S3 via moto, 2,612 corpus cells, and 346 new statements that combine the merged units) found no regression of any severity against Spark 4.1.2. Every new refusal is one that Spark also makes.

## Matrix
The fresh-build rerun of the 842-cell Spark + Iceberg matrix on the release head (main 8568e57a, 2026-09-29) reads **705 EQUAL / 132 SPARK-CANNOT / 5 REFUSED-REGISTERED / 0 DIFFERENT**. That is unchanged from v1.5.0. The five refused cells are still the dated owner carve-outs C-1, C-2 and C-4.

## Known follow-ups (v1.5.2)
- CASESENS-2 (#881) and RD-1/RD-2 (#884).
- The Polars `is_duplicated` door (#883).
- The nvl-family typing (#888).
- CSV/JSON timestamps in the session zone (#889).
- String-literal escapes (#890).
- Overflow cast on insert (#891).
- The deep-chain crash (#892).
- The pre-existing gaps PE-3 to PE-26, each with a Spark answer and a card, in [release-diff-1-5-1-pre-existing.md](release-diff-1-5-1-pre-existing.md). The silent rows come first.
