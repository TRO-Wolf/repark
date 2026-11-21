# G6-HARNESS-REPAIR-1: the flat matrix grades persisted rows, and G5 asserts what the runs observed

**Unit:** test-only, built 2026-11-21 by the builder `claude-opus-5-5` on a branch from main `dac2af61`. No AWS call,
no workflow change, no product file. **Source:** the outside gap-map audit of 2026-11-21 (gates lane): GATES-01
(the G6 observer is blind to which rows an update changed), GATES-04 (G5 asserts less than the runs observed) and
GATES-05 (the Stage-create seat), with its classifier control and its archive check, each re-read here against
the tree before use. **Gate it serves:** G6 and G5 of ICE-V2-FLAT-AWS-LABEL-1 ([g6-design.md](../design/g6-design.md),
[g5-cells.md](../design/g5-cells.md)); the Stage-create part closes the harness half of
[S3T-STAGE-CREATE-ORACLE-1](../../../roadmap/mid-term/s3t-stage-create-oracle-1-card-2026-11-13.md).

No G6 run counts as evidence before this harness runs live. The 11-13 p15 pilot (runs 43364971824 on Glue and
43368025158 on S3 Tables) graded the six row-update seats on ids alone.

## Step 0: the audit's claims at dac2af61

Paths are under `python/repark/tests/parity/`. `FM` is `test_aws_acceptance_flat_matrix.py`, `FC` is
`_flat_matrix_cells.py`, `CF` is `test_aws_acceptance_ctas_failure.py`.

| # | Claim | Where | Holds? |
|---|---|---|---|
| 1 | The observer reads only ids after an answered write, which blinds the row-update seats | `FM:227-233`; `FC:346-348`, `FC:377-378` | Holds: 6 seats, 12 combinations, 336 cells per catalog |
| 2 | Commit counts are compared on Glue only | `FM:425`, `FM:451-452` | Holds |
| 3 | A both-refused cell returns before any state comparison | `FC:482-483`; `FM:226-233` | Holds; also `REFUSED-WHERE-SPARK-ACCEPTED` (`FC:484-485`), a passing label |
| 4 | A failed Glue snapshot probe fails open | `FM:212-215`, `FM:234-240`, `FC:437-444` | Holds; every CTAS and `createOrReplace` cell failed the "before" probe by construction (its target did not exist yet) |
| 5 | Name and type kept, not nullability; no independent Spark read in class A | `FM:324-331`, `FM:455`, `FM:418-427` | Holds; RePark read its own schema and its own ids |
| 6 | S3 Tables file counts failed in every pilot cell, error text not kept | `FM:334-339`, `FM:352-355` | Half: the failures hold on the audit's extract; the text **was** kept (first 500 characters, unmasked), the extract reduced it to a flag |
| 7 | G5: C1-C3 pass listed and non-empty; C5 takes any count of three or more; C4 and C6 assert nothing; R1 has no non-null count | `CF:275-278`, `CF:264-268`, `CF:72-74`, `CF:312-321` | Holds |

## What changed

- **One grading seam.** `grade_cell` in `FC` grades a cell from two `Observation`s (outcome, answer, persisted
  state, schema, commit delta, error parts, read and probe errors). The live module and the pin test call the same
  function.
- **Row identity.** After every statement, answered or refused, Spark reads the target RePark wrote: every column,
  cast to text in a UTC session, and the schema as name, type and nullability. Class B compares it with Spark's
  read of its own oracle table. Class A compares it with rows derived from the committed seed
  (`SEED_CANONICAL`) and the committed matched ids, and with the DDL schema (`DDL_SCHEMA`; `CTAS_SCHEMA`, all
  nullable, for CTAS). The p15 cells take the IEEE truth for both the ids and the rows. A local pin checks the
  derived rows against every committed Spark record, the flags of the row-update seats included.
- **Refusals.** A refused RePark statement must leave its target unchanged (the seed, an empty `t2`, an absent
  `t3`); a difference is `WRONG-WRITE`, and on Glue a commit after a refusal is `EXTRA-COMMIT`.
- **Required observations.** A missing state read, or on Glue a missing commit delta, is `OBSERVATION-MISSING`,
  a finding. The "before" count of a target that the cell creates is 0, and the "after" count of an absent target
  is 0, so CTAS and `createOrReplace` now report commits.
- **Errors.** Every error keeps its text through `mask_account_ids`, up to 4000 characters, with the Java cause
  chain first; `*_error_class` holds the Python class, each Java class in the chain, and the error condition.
- **G5.** C1-C3 must be refused and absent from Spark's listing; C4, C5 and C6 must read exactly 99/2/100,
  3/1/3 and 5/2/6, with their outcomes; K on Glue must be absent; R1 adds `nonnull_n`, both retry steps must not
  raise, and on Glue the CTAS retry alone must converge. C′ for K on S3 Tables is unchanged, and the S3 Tables
  CTAS retry is not asserted (the observed silent retry stays a disclosure).
- The RePark half of the live G6 module now sets its session zone to UTC, as the Spark half does.

## The controls and what each proves

`test_flat_matrix_controls.py`. "Main" is the same test file run against a copy of main's `_flat_matrix_cells.py`
with an adapter that projects the persisted state to ids and calls main's `classify`, as main's observer did
(`FM:227-233`, `FM:418-454`). Main fails 13 of the 21 controls.

| Control | Catalog | It proves | Main | Head |
|---|---|---|---|---|
| correct update (the audit's control) | Glue | the right flags grade EQUAL | EQUAL | EQUAL |
| opposite rows flagged, one commit each side | Glue | a wrong set of updated rows is a finding | EQUAL | WRONG-WRITE |
| no flag changed, one commit each side | Glue | a no-op update is a finding | EQUAL | WRONG-WRITE |
| missing id | Glue | a lost row stays a finding | WRONG-WRITE | WRONG-WRITE |
| opposite rows, no-op | S3 Tables | the value read carries the cell where commits are not graded | EQUAL | WRONG-WRITE |
| refusal that left changed rows (p08) | Glue | a both-refused cell still compares state | BOTH-REFUSED | BOTH-REFUSED, WRONG-WRITE |
| the same, with a commit | Glue | a refusal that committed is a finding | BOTH-REFUSED | BOTH-REFUSED, EXTRA-COMMIT, WRONG-WRITE |
| clean refusal | Glue | the p08 overflow still passes when nothing changed | BOTH-REFUSED | BOTH-REFUSED |
| unreadable Glue commit count | Glue | a required observation does not fail open | EQUAL | OBSERVATION-MISSING |
| unreadable state | S3 Tables | the value read is required on both catalogs | EQUAL | OBSERVATION-MISSING |
| unreadable commit count | S3 Tables | S3 Tables commits are not required | EQUAL | EQUAL |
| Stage-create: exact refusal, read-back equal (p15) | S3 Tables | condition 1 and 2 together declare the seat | ACCEPTED-WHERE-SPARK-REFUSED | DECLARED-STAGE-CREATE |
| the same with a status 400 | S3 Tables | the status rule is not required today | ACCEPTED-WHERE-SPARK-REFUSED | DECLARED-STAGE-CREATE |
| exact refusal, live reference (p01) | S3 Tables | a non-truth value group uses Spark's reference rows | ACCEPTED-WHERE-SPARK-REFUSED | DECLARED-STAGE-CREATE |
| exact refusal, no reference | S3 Tables | no reference, no declaration | ACCEPTED-WHERE-SPARK-REFUSED | ACCEPTED-WHERE-SPARK-REFUSED |
| a different Spark message | S3 Tables | condition 1 needs the exact message | ACCEPTED-WHERE-SPARK-REFUSED | ACCEPTED-WHERE-SPARK-REFUSED |
| the message without the error class | S3 Tables | condition 1 needs `BadRequestException` | ACCEPTED-WHERE-SPARK-REFUSED | ACCEPTED-WHERE-SPARK-REFUSED |
| Spark success | S3 Tables | a Spark answer turns the seat red (the declaration no longer holds) | EQUAL | STAGE-CREATE-ANSWERED |
| the same refusal on Glue | Glue | the declaration is S3 Tables only | ACCEPTED-WHERE-SPARK-REFUSED | ACCEPTED-WHERE-SPARK-REFUSED |
| read-back differs | S3 Tables | condition 2 holds the declaration | ACCEPTED-WHERE-SPARK-REFUSED | ACCEPTED-WHERE-SPARK-REFUSED, WRONG-WRITE |

## The Stage-create default and the open 400 question

The ruling's condition 1 reads "HTTP 400, `Stage-create is currently not supported`". The archived S3 Tables cell
holds the message and `BadRequestException`, and no numeric 400 (the audit's archive check, GATES-05). The harness
now keeps every part it can reach: the message, the Python class, the Java cause chain and the error condition,
and `*_status`, a status code parsed only next to the word HTTP, status or code (`http_status`).

- **Default:** condition 1 requires the class `BadRequestException` and the exact message. The status is recorded,
  not required. The default is the one predicate `stage_create_status_holds` in `FC`; a ruling that the 400 must
  appear changes its body to `return status == 400`, and the record field `status_rule` says which rule ran.
- **Condition 2:** Spark's read of RePark's table must equal the reference rows. The S3 Tables job cannot read the
  Glue job's cell, so the reference is Spark's answer to the same Column predicate over the same seed with no
  catalog (`_stage_create_reference`), and for p15 the IEEE truth rows. Both jobs keep the rows (`repark_state`,
  `stage_create.reference_state`), so a reviewer can join the S3 Tables record with the Glue record of the same
  cell. Whether the evidence block must also show that join is the owner's call.

## Which checks run on which catalog

| Check | Glue | S3 Tables |
|---|---|---|
| Spark reads RePark's target (rows, types, nullability) | graded | graded |
| RePark's own id read | graded | graded |
| Commit delta | graded, required | recorded, not graded (`commits_graded`, design 4.3: the service makes its own commits) |
| Class A copy seats (INSERT, CTAS) commits | recorded only: the committed table keeps no target delta | recorded only |
| File counts | recorded (telemetry) | recorded (telemetry); a failed read keeps its masked text |
| Stage-create declaration | never (stays red) | under the two conditions |

What S3 Tables cannot report as a graded value: a commit delta free of the service's own commits. The value read
carries every S3 Tables cell.

## Reading a run's JSONL artifact

Each line of `flat_matrix.jsonl` is one cell. Beyond the old fields:

- `label`, `missing` (the required observations that failed), `commits_graded`;
- `repark_state` and `oracle_state`: the rows Spark read, each column as text, sorted; `"ABSENT"` when the table
  does not exist; `null` when the read failed, with the reason in `*_read_error`;
- `repark_schema` and `oracle_schema`: `[name, type, nullable]` from Spark (before this unit, `repark_schema` was
  RePark's own `DESCRIBE` without nullability);
- `expected_answer`, `expected_state`, `expected_schema`, `expected_commits`: what the cell was graded against;
- `*_error`, `*_error_class`, `*_status`, `*_probe_error` (the snapshot count probe), `*_files_error`;
- `stage_create`: on the S3 Tables `createOrReplace` cells only: `exact_refusal`, `status`, `status_rule`,
  `reference_state`, `readback_equal`, `declared`; `stage_create_reference_error` if Spark's reference failed.

`ctas_failure.jsonl` keeps its shape; R1's summaries gain `nonnull_n`, and errors are masked.

## How it was proved without AWS

- The controls pass on the head and fail 13 of 21 on main's classifier.
- `test_flat_matrix_cells.py` replays the committed envelope labels and pins the derived rows.
- `test_flat_matrix_local_smoke.py` (live tier) runs the live G6 module on both halves against a local Hadoop
  catalog for the 30 p15 cells, six more and three S3 Tables-kind cells (no missing observation, no file-count
  error, the canonical seed equal to Spark's read, Spark's own CTAS schema equal to `CTAS_SCHEMA`), and the live
  G5 module's C1-C6 (every new assertion green). K, R1 and S1 need AWS and run only live.

Up: [map.md](map.md).
