# ATTR-VIEW-SEMANTICS-1 — SQL aliases and SQL-defined temp views mint attribute ids; a foreign column raises MISSING_ATTRIBUTES

grade: B (design sketch first) · sketch engine: Claude Opus 5.5 (`claude-opus-5-5`), high · slice engine: Muse Spark 1.3 contributor, max · release: the minor the ATTR-ID-1 stack ships in

Card: [attr-view-semantics-1-card-2026-10-04.md](../../roadmap/mid-term/attr-view-semantics-1-card-2026-10-04.md), with the owner's rulings of 2026-10-05.

Measurement, kept beside this order because `/tmp` is wiped at boot:
- [matrix.md](matrix.md), the 28-row matrix, measured 2026-10-04;
- [views_probe.py](views_probe.py), the probe;
- the three engine outputs it read: [spark.json](spark.json) (Spark 4.1.2), [repark152.json](repark152.json) (the 1.5.2 wheel) and [stack.json](stack.json) (ATTR-ID-1 stack `d5c97862`).

## 0. Why, and what is out of scope

On the ATTR-ID-1 stack an attribute id is carried through a SQL alias (`SELECT v AS v FROM tv`) and through a SQL-defined temp view (`CREATE TEMP VIEW sv AS SELECT * FROM tv`). Spark 4.1.2 mints a new id at both. A column taken from another frame and used against such a result therefore answers on RePark, where Spark raises `MISSING_ATTRIBUTES`. These are shapes V3, V4, V9, V12 and V15 in the matrix.

This unit makes those mint points and that refusal match Spark.

Out of scope:
- **V6b** (`t1.join(t2, "id").select(t1.v)`). It is checked against the self-join (SJ) slices and not added here (owner, 2026-10-05).
- **V10 and global temp views**, under carve-out F16.
- **Item (d)**, the V-3a and V-3b internal errors. That is a stack regression, already fixed on the stack by PR #951 (`b9db3536`): the SQL temp-view provider is born clean (`strip_schema_ids`), and the frame carries the definition's ids above the scan (`copy_attribute_ids`). This unit builds on that fix and must keep V-3a, V-3b and the #951 pins green.

## 1. Rulings already made

| id | ruling |
|---|---|
| R-0 | Grade B with an Opus design sketch (owner Q1). The sketch is the first and only Opus round. The slices run on Muse at max, one item per round, each with one commit, a DIFF-PROBE and one scoped Opus verifier per product-Rust PR. |
| R-1 | Release split (owner Q2): (a), (b) and (c) land as their own PR on main, right after the stack-to-main merge. Nothing folds into the stack. Until then the slices live on a branch cut from `wip/feat/attr-id-1-2026-10-01`. |
| R-2 | (c) breaks code and ships that way (owner Q3). No warning-first release. The release note in the stack's minor names the V3, V4, V9, V12 and V15 shapes. It is the same rule as self-join option A: Spark 4.1.2 already refuses every one of them. |
| R-3 | Every new refusal carries Spark's condition, `MISSING_ATTRIBUTES` (`[MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION]` or the subclass Spark prints), measured on the live oracle. A refusal raised under a different condition is a wrong answer. |
| R-4 | No new refusal fires where Spark answers. Every cell the matrix records as EQUAL must stay EQUAL, and so must every self-join pin (`python/repark/tests/test_attr_id_1_sj*.py` on the stack). |
| R-5 | Single source of truth for identity stays `crates/repark-core/src/session/df_guards/attr_id.rs` on the stack (`stamp`, `attribute_ids`, `resolve`). No new carrier, no name-based fallback, no Python-side identity decision. |
| R-6 | No new crate, no Cargo change, no `.github` change. No comments in any code file (markdown may explain). Ceilings only ratchet down. |

## 2. The sketch round (Opus 5.5, high): what it must produce

Write `task/wo/attr-view-semantics-1/design.md` on a branch `docs/attr-view-semantics-1-design` cut from `origin/main`, opened as a docs PR. Measure on the stack (`wip/feat/attr-id-1-2026-10-01`) and on the oracle, and change no code. It must answer:

1. **The mint points, measured, not assumed.** For each of these, record what Spark 4.1.2 does (new exprId or kept, via `queryExecution.analyzed.output`) and what the stack does today (`repark._native.attribute_ids`):
   - SQL `SELECT v AS v`, `SELECT v AS w`, `SELECT t.v`, `SELECT *`;
   - `CREATE TEMP VIEW … AS SELECT *`, and with a column list;
   - `CREATE OR REPLACE TEMP VIEW`;
   - a view over a view;
   - `spark.table(view)` read twice;
   - the DataFrame door: `df.select(df.v.alias("v"))` and `df.withColumnRenamed`;
   - `createOrReplaceTempView` of a DataFrame (which Spark keeps).

   The table goes into `design.md`. Any row where the DataFrame door and the SQL door differ in Spark must be called out. Do not unify them by assumption.
2. **Where each mint happens in RePark.** Name the function and file for each, with signatures:
   - (a) the SQL temp-view registration. Today the carry is in `crates/repark-spark/src/view_ddl/execute.rs`, after #951, so state what replaces `copy_attribute_ids` there;
   - (b) the SQL alias. That means the `spark.sql` result stamp and how a `Projection` of `Alias(Column)` coming from the SQL planner is told apart from one coming from the DataFrame door.
3. **(c) the refusal.** State where a column bound to an id absent from the plan's input raises, and with which exact Spark text. Measure V3, V4, V9, V12 and V15 on the oracle. Show that `resolve` already distinguishes "absent id" from "ambiguous" or "missing name", or name the smallest change that makes it do so.
4. **Blast radius, measured.** On a stack dev build with a throwaway local patch of the proposed mint points (never committed), run:
   - the facade suite;
   - the replay and its like-for-like comparison ([attr-id-1-s0-replay.md](../attr-id-1/attr-id-1-s0-replay.md) names the harness);
   - the self-join pins.

   List every cell that moves, classified as toward Spark, away from Spark, or same class. Any cell that moves away from Spark is a HALT for the owner, not a design decision.
5. **Slices.** Give one slice per item (a), (b) and (c), in landing order. For each slice give:
   - the files and their ceilings;
   - the pins, named and each paired with the mutation that turns it red;
   - the Spark answer per pin;
   - the gate commands;
   - the halt rules.

   The proposed pin names in the card are proposals. Keep or rename them, and say which.
6. **Release-note text** for R-2: the five shapes, one line each, with the Spark condition.

## 3. Gates for the sketch round

- `design.md` passes `bash scripts/check_map_md.sh --base origin/main`.
- `test -e` holds for every cited path.
- Every Spark answer in it comes from the live oracle (`systemd-run --user --scope --slice=repark.slice env JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 /tmp/sparkenv/bin/python`) and is recorded with the date.

## 4. Halt rules

- **H1.** A mint point would change an answer that Spark gives (R-4). Stop, and report the cell and both answers.
- **H2.** The SQL door and the DataFrame door differ in Spark, and the stack has only one stamp path for both. Stop with options; do not pick one.
- **H3.** (c) needs a name-based fallback, or a Python-side identity decision (R-5).
- **H4.** Any replay cell moves away from Spark.
- **H5.** The #951 pins (`test_attr_view_semantics_1.py`, `temp_view_attr_ids`) would go red.

## 5. Hand-back

End with one fenced JSON object, and write the same object to `design-handback.json` beside this order in the orchestrator's work directory:

```json
{"status": "DONE|HALT", "design_pr": "<n>", "mint_table_rows": "<n>", "slices": ["a", "b", "c"], "blast_radius": {"toward": "<n>", "away": "<n>", "same_class": "<n>"}, "halt": null, "questions": []}
```

Standing rules from [../TEMPLATE.md](../TEMPLATE.md) apply.
