# Charter ledger — FNP-GEN-1 · generators and semi-structured parsers answer PySpark 4.1.2

**Date:** 2026-09-15 · **Branch:** `feat/fnp-gen-1` · **Base:** `bee2cde3` · **Model:**
muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `EX-FN-2`, `EX-FN-6`, `EX-FN-8`, `EX-FN-16`, `EX-FN-22`, `FNP9-GENERATORS-1`
and the FNP-Z `json_tuple` residue flip at unit close, not in step 1.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 1.5 Spark-parity campaign chartered nine generator and semi-structured
names (`inline`, `inline_outer`, `posexplode`, `posexplode_outer`, `json_tuple`,
`from_csv`, `schema_of_csv`, `from_xml`, `schema_of_xml`). The live PySpark 4.1.2
oracle recording of 2026-09-14 already holds their answers; this unit builds the Rust
kernels and facade wrappers against those cells. Run 16a step 1 (this round) lays the
ledger, the fixture subset and the red-first pins only: no product code, no cargo.

**Not in this round:** any engine or facade change; registry flips; `LATERAL VIEW`
spellings (run 16c's parser work, D-2/D-7); re-recording the oracle.

## Decisions

| Id | Ruling | Applied |
|---|---|---|
| D-1 | Orchestrator: kernels in `crates/repark-functions/src/` (new modules in its `map.md`), registered in `register_all`; generators follow the existing `explode` / `stack` plumbing (`StackRewrite` in `crates/repark-core`, `functions_stack.py`). Facade wrappers in `functions_json.py` / `functions_collections.py` / new `functions_generators.py`. | Steps 2–5. |
| D-2 | Orchestrator: `LATERAL VIEW` spellings are run 15c's parser work; pin SELECT-list SQL cells only. | C-003; blocked list below. |
| D-3 | Orchestrator: never edit `dataframe/**`, `column.py`, `session/**`, `catalog.py`, `types.py`, the SQL parser/dialect, or any dependency manifest; HALT at the seam instead. | Whole unit. |
| D-4 | Orchestrator: flip `EX-FN-2`, `EX-FN-6`, `EX-FN-8`, `EX-FN-16`, `EX-FN-22`, `FNP9-GENERATORS-1` and the FNP-Z `json_tuple` residue to FIXED (or DECLARED) with the new pins; one section-7 row per residual divergence. | Step 6. |
| D-5 | Orchestrator: pins in `python/repark/tests/test_fnp_gen_1.py` on both doors over the oracle cells, red first with the red summary in the ledger; Rust unit tests beside each kernel. | This round + steps 2–5. |
| D-6 | Owner ruling Q-15B-1, 2026-09-15: XML stays a dated DECLARED refusal in 1.5 (only the ORC reader gained a dependency). `from_xml` / `schema_of_xml` raise Spark's own unsupported-operation error with a dated registry row joining `FNP-16-csv-xml-xpath`; pins assert the refusal and the fixture keeps Spark's answers for a later release. One-line reason: an XML parser needs a new dependency, which this unit may not add. | C-002, C-003, step 5. |
| D-7 | Orchestrator: `LATERAL VIEW` cells are run 16c's work; list them as `blocked on 16c`, do not pin them. | C-003. |

## Step-1 inputs

- Fixture `python/repark/tests/fnp_gen_1_spark_oracle.json`: 62 cells whose `name`
is one of the nine card names plus their 9 `signatures`, copied verbatim from
`/tmp/oc-worker/pa-gen/o245_spark_oracle.json` (live PySpark 4.1.2, recorder
`/tmp/oc-worker/pa-gen/o245.py`); `spark_version` 4.1.2 kept. Nothing re-recorded.
- No cell projects the `ts` column (checked: zero exprs mention it), so the
driver-local America/New_York timestamp note needs no localization in this unit.
- Blocked on 16c (unpinned, D-7): the three ansi-True `LATERAL VIEW` cells —
`SELECT id, x, y FROM FRAME LATERAL VIEW inline(arr_s) v AS x, y`,
`SELECT id, p, v FROM FRAME LATERAL VIEW OUTER posexplode(arr_i) t2 AS p, v`,
`SELECT id, a, b FROM FRAME LATERAL VIEW json_tuple(js, 'a', 'b') j AS a, b`
(plus their three ansi-False twins).
- Step-2 obligation found while pinning: RePark spells the posexplode parameter
`column`, Spark spells it `col`; the signature pin reds until the rename lands.
- Spark behavior the pins lock in: single-field `json_tuple(...).alias('x')`
answers column `c0` (Spark ignores the alias); the FAILFAST and uninferable-literal
`schema_of_csv` Spark errors record as unclassified `Py4JJavaError` with a truncated
message, so those pins assert the raise shape and step 4 records the exact class.

## Proposition ledger

| Clause | Claim | Verdict | Evidence |
|---|---|---|---|
| C-001 | All nine names sit on `repark.spark.functions` with PySpark 4.1.2's parameter names and defaults. | OPEN | Red on base `bee2cde3`: `test_nine_names_present_on_the_facade` fails (`inline`/`inline_outer` absent); signature pins fail for `inline`, `inline_outer`, `posexplode`, `posexplode_outer` (missing; `column` vs Spark's `col`). The five stub signatures (`json_tuple`, `from_csv`, `schema_of_csv`, `from_xml`, `schema_of_xml`) already match and pass — the stubs were written against PySpark's shapes. pins: fnp-gen-1/C-001 |
| C-002 | The Python door answers the oracle cells for `inline`, `inline_outer`, `posexplode`, `posexplode_outer` (array, map, multi-alias), `json_tuple`, `from_csv`, `schema_of_csv`, and refuses `from_xml` / `schema_of_xml` per D-6. | OPEN | Red on base: 13 failed — `inline*` raise `AttributeError`; `posexplode*`/`json_tuple`/`from_csv`/`schema_of_csv` raise the `UnsupportedOperationException` stub refusals; the XML pins fail the `FNP-16-csv-xml-xpath` match (stub text names no registry row). pins: fnp-gen-1/C-002 |
| C-003 | The SQL door answers the SELECT-list cells for the same names and refuses the XML pair per D-6; `LATERAL VIEW` cells stay unpinned per D-7. | OPEN | Red on base: 11 failed — `Invalid function 'inline'` / `'posexplode'` / `'from_csv'` / `'schema_of_csv'`; `json_tuple` answers one `struct<c0>` column where Spark answers `c0..cN-1`; the XML pins raise `AnalysisException`, not `UnsupportedOperationException`. pins: fnp-gen-1/C-003 |
| C-004 | Error cells: `from_csv` FAILFAST raises the parse error, `schema_of_csv` on a non-foldable column raises `DATATYPE_MISMATCH.NON_FOLDABLE_INPUT`, the uninferable literal raises. | OPEN | Red on base: 3 failed — FAILFAST raises the stub refusal (`"not supported yet"` still in the message); the non-foldable cell raises `Invalid function` with no `DATATYPE_MISMATCH` match; the uninferable-literal cell raises `Invalid function`. pins: fnp-gen-1/C-004 |
| C-005 | NULL / empty / outer rows: `inline_outer` and `posexplode_outer` keep a NULL row for NULL and empty arrays; NULL `js` answers all-NULL fields; NULL `csvrow` answers NULL struct and `',,'` an all-NULL struct. | OPEN | Red on base through the C-002/C-003 outer pins (same failures: absent names, stub refusals, `Invalid function`). pins: fnp-gen-1/C-005 |
| C-006 | No regression: the touched suites stay green at unit close. | OPEN | No product code in step 1; gates run at close. pins: fnp-gen-1/C-006 |
| C-007 | Registry rows flip with the new pins and every touched `map.md` stays in lockstep. | OPEN | Step-1 maps (tests fixture row, staging row) land in the step-1 commit; row flips land in step 6. pins: fnp-gen-1/C-007 |

VERDICT: 7 clauses, 0 PROVEN, 7 OPEN, 0 REJECTED.

## Red summary (base `bee2cde3`, `PYTHONPATH=python/repark/src .venv/bin/python -m pytest python/repark/tests/test_fnp_gen_1.py -q`)

37 tests: 32 failed, 5 passed. The 5 passes are signature pins on existing stubs
whose parameter names and defaults already equal Spark's (`json_tuple`, `from_csv`,
`schema_of_csv`, `from_xml`, `schema_of_xml`). Per-name red causes: `inline` /
`inline_outer` — `AttributeError` on the facade, `Invalid function 'inline'` on SQL;
`posexplode` / `posexplode_outer` — `UnsupportedOperationException` stub on the
facade, `Invalid function 'posexplode'` on SQL, plus the `column`/`col` parameter
divergence; `json_tuple` — stub refusal on the facade, single-`struct<c0>` shape on
SQL; `from_csv` / `schema_of_csv` — stub refusals on the facade, `Invalid function`
on SQL, FAILFAST carrying the stub text; `from_xml` / `schema_of_xml` — stub text
without the `FNP-16-csv-xml-xpath` marker on the facade, `Invalid function` on SQL.
