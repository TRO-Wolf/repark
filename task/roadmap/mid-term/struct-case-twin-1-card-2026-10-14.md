# Card STRUCT-CASE-TWIN-1: a BY NAME struct write with case-twin source fields refuses

**Date:** 2026-10-14. **Filed by:** the docs clerk lane, from ICE-STRUCT-PAIRING-1
step 0 (U-6) and the owner's ruling on question 37 of 2026-10-14.

**Status:** open. Unscheduled, not in the closeout. **2026-10-14:** the measured shape closes in
ICE-STRUCT-PAIRING-1 build round 2 (hint H-2: the by-name walk refuses a target field that two
source fields match), pinned by `test_ice_struct_pairing_1.py` cell `by_name_case_twins`; row
R-021 is CLOSED. What the card adds beyond that cell (format v3, the other by-name doors and
containers) is for the owner to keep or retire.

**Related:** parity row ICE-TSNS-SQL-1-R-021 in
[docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).
Case twins outside structs (top-level columns and every reference form) are already
covered by [ID-1a](../../../docs/spark-sql-iceberg-parity.md#id-1a--ascii-case-twin-columns-refuse-every-reference).

## Why

U-6 step 0 measured a new divergence on Spark 4.1.2 with Iceberg 1.11 against main's
module. Table `(id INT, st STRUCT<a: INT, b: INT>)`, format v2, seed `(10,20)`,
written `(1,2)`.

A BY NAME write whose source struct carries two fields differing only in letter case
(`INSERT INTO t BY NAME SELECT 2 AS id, named_struct('a', 1, 'A', 2) AS st`) is
stored by RePark: `[[1,10,20],[2,1,2]]`. Spark refuses it with
`AnalysisException: [INCOMPATIBLE_DATA_FOR_TABLE.AMBIGUOUS_COLUMN_NAME] Cannot write
incompatible data for the table `ora`.`ns`.`step0_twins`: Ambiguous column name in
the input data `st`.`a`. SQLSTATE: KD000`.

## Measured 2026-10-14 (U-6 step 0)

One diverging cell (`by_name_case_twin_fields`): RePark stores `[[1,10,20],[2,1,2]]`;
Spark refuses as above. The neighbouring cell with uniformly different-case fields
(`named_struct('A', 1, 'B', 2)`) stores `[[1,10,20],[2,1,2]]` on both engines: no
divergence, and the fix must keep storing it.

## The ask

- Refuse a BY NAME struct write whose source struct carries two fields differing only
  in letter case, as Spark does: class `INCOMPATIBLE_DATA_FOR_TABLE.AMBIGUOUS_COLUMN_NAME`,
  condition `Ambiguous column name in the input data `st`.`a``, SQLSTATE `KD000`, with
  Spark's message text. The refusal fires before anything is stored.
- Measure Spark 4.1.2 on the shape if the refusal text needs more than the recorded
  sentence, and record the class and text in the ledger. The refusal's text follows
  Spark's measured answer.

## Recorded, not fixed by this card

- Format v3: not measured. The step-0 table is format v2 only.
- Case twins outside structs are already covered by ID-1a (top-level twin columns
  refuse every reference); this card is the struct-input twin only.
- The merge-schema surplus struct field (`EXTRA_STRUCT_FIELDS`) is parity row R-022
  and belongs to ICE-STRUCT-PAIRING-1 (U-6) by the same ruling; no card here.

## Gates

- The case-twin BY NAME struct write refuses with Spark's sentence before anything is
  stored, and the table holds the rows it held before the statement.
- The uniformly different-case BY NAME write keeps storing `[[1,10,20],[2,1,2]]`, as
  both engines do today.
- An Opus verifier on the product pull request.
