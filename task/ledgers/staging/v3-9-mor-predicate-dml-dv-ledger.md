# Charter ledger — V3-9 · merge-on-read predicate DML writes deletion vectors on v3

**Date:** 2026-09-02 · **Branch:** `feat/v3-9-mor-subquery-where-dv` · **Base:** `origin/main`
`ca9c007` · **Model:** claude-opus-5 (medium) · **Policy:**
[../../../AGENTS.md](../../../../AGENTS.md) · **Path:** STANDARD.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** V3-7 lifted MoR MERGE onto deletion vectors and V3-8 measured the MoR
subquery-`WHERE` cells but left them refused: `resolve_write_mode` gated merge-on-read
predicate DML to `format_version == V2`. That gate is the last v3 MoR refusal.

**Scope correction (measured, 2026-09-02).** The brief's premise that plain-`WHERE` MoR DML on
v3 sits on the same gate is **false**. Only allow-listed subquery shapes reach
`execute_predicate_dml` (`try_allowed_delete_in` / `try_allowed_update_in` return `None` for a
plain predicate — `crates/repark-sql/src/router.rs` `Statement::Delete | Statement::Update`);
plain `WHERE` goes through `delegate` and has written DVs since RP-6. Plain `WHERE` is pinned
here as an incidental control, not a lift.

**Not in this unit:** subquery spellings outside the allow-listed hole (`G3-E8`); V1 delete
files; equality deletes; fork repin; `.github/`.

## PROPOSITION LEDGER — V3-9 — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Reproduce the refusal before any fix: MoR subquery-`WHERE` DELETE and UPDATE on a v3 table refuse with `resolve_write_mode`'s V2-only text on the Spark door, the ANSI door and the facade. | A red run naming the message. | **PROVEN** | Facade: `UnsupportedOperationException: … merge-on-read DELETE writes Parquet position deletes, which require a V2 table (this table is V3) — use write.delete.mode = 'copy-on-write' instead` from the new facade cell on the pre-lift binary. Spark door: the retired `created_v3_merge_on_read_subquery_dml_refuses_on_the_v2_delete_file_gate`. Restore-the-gate mutation below re-runs it across all three doors. |
| C-002 | Measure the live oracle (PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `local[1]`, `coalesce(1)` single-file seed) on MoR v3 `DELETE … WHERE` plain / `IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` and `UPDATE … WHERE` plain / `IN`, plus `write.delete.granularity` = `file` / `partition`, recording rows, lineage triples, next-row-id, first-row-id, added rows, snapshot summary, delete-entry content / format / `referenced_data_file` / record count and data-file counts. | Oracle transcript table. | **PROVEN** | Nine cells below. Every delete entry is `POSITION_DELETES` in a `PUFFIN` file naming its data file — file-scoped; granularity changes nothing on v3. |
| C-003 | Lift `resolve_write_mode` from `!= V2` to `< V2` (the shape `resolve_merge_mode` already uses), so v3 predicate DML reuses `prepare_row_delta_deletes` → `close_touched_dv_containers`; no new DV code. Engine equals Spark on every measured cell across the Spark, ANSI and facade doors, created and adopted; shared-Puffin closure keeps both siblings live and file-scoped. | Three-door pins with absolute values; a shared-Puffin sibling pin. | **PROVEN** | 14 Spark-door cells in `crates/repark-spark/src/tests/v3_mor_dml.rs`, the repurposed cell in `v3_subquery_dml.rs`, the shared-Puffin cell in `v3e4.rs`, the ANSI twin in `crates/repark-sql/src/v3/cow.rs`, the facade twin in `python/repark/tests/test_v3_cow_dml.py`. |
| C-004 | The incidental controls hold: v2 MoR predicate DML still writes Parquet position deletes with no `referenced_data_file` (RDF-1's file-scoped bounds pins stay green); COW v3 unchanged; MoR MERGE pins green; a subquery DELETE matching nothing writes no delete file. Mutation-proof the lift (restore the gate) and the lineage carry. | Control pins; mutation N red of M. | **PROVEN** | Controls in `v3_mor_dml.rs`. Mutation numbers below. |
| C-005 | Registry `docs/spark-sql-iceberg-parity.md` MoR predicate DML rows say FIXED with the measured clause; north star §3 "Write: MOR DML via deletion vectors" → ✅; `docs/design/format-v3-track.md` §5 one line; STATUS v3 bullet; the byte tripwire re-recorded citing this unit; a `REPARK_PARITY_LIVE` cell; maps in lockstep; this ledger `move`d to `completed/` last. | `make check-map-sync`, `check-ledger-grammar`, `check-ledgers`, `check-docs-compaction`. | **PROVEN** | Citation: `python/repark/tests/test_v3_live_oracle.py`, `crates/repark-spark/src/tests/v3_lineage.rs`. |
| C-006 | The v3 create opt-in refusal stops claiming what V3-9 makes false: its parenthetical "v3 tables cannot yet do merge-on-read row-level writes" is removed on both refusal sites (`crates/repark-sql/src/create_table.rs`, `crates/repark-functions/src/cardinality.rs`), leaving only the conf name and the v2 default. No pin asserted the removed substring as a Spark-measured value, so no HALT. | Three-door pins asserting the message names the conf and no longer says `merge-on-read`. | **PROVEN** | Citations: `crates/repark-sql/src/v3/create.rs`, `crates/repark-spark/src/tests/create_table.rs`, `crates/repark-spark/src/tests/ctas.rs`, `python/repark/tests/test_v3_create_opt_in.py`. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3-9-mor-predicate-dml-dv
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Seven MoR shapes Spark-equal on rows, lineage triples, next-row-id, first-row-id, added rows, data-file count and the single file-scoped Puffin DV; UPDATE advances next-row-id by the appended count only.
      artifacts: [crates/repark-spark/src/tests/v3_mor_dml.rs, crates/repark-spark/src/tests/v3_subquery_dml.rs, crates/repark-sql/src/v3/cow.rs, python/repark/tests/test_v3_cow_dml.py]
    - id: AT-2
      status: ATTACKED
      evidence: Created and adopted v3; IN, NOT IN, EXISTS, NOT EXISTS, UPDATE IN, plain WHERE; delete granularity partition; a subquery DELETE matching nothing; the shared-Puffin partitioned fixture.
      artifacts: [crates/repark-spark/src/tests/v3_mor_dml.rs, crates/repark-spark/src/tests/v3e4.rs]
    - id: AT-3
      status: ATTACKED
      evidence: v2 MoR predicate DML still writes Parquet position deletes with no referenced_data_file; shapes outside the allow-listed hole still refuse G3-E8; a no-match subquery DELETE writes no delete file and leaves the seed.
      artifacts: [crates/repark-spark/src/tests/v3_mor_dml.rs, crates/repark-spark/src/tests/v3_cow.rs]
    - id: AT-4
      status: N/A
      justification: No new shared mutable engine state; the DV close path is V3-7's, reused.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, or secret handling. One format-version comparison changed; no new IO.
      artifacts: [crates/repark-iceberg/src/write/predicate_dml.rs]
    - id: AT-6
      status: N/A
      justification: No Catalog trait change.
    - id: AT-7
      status: N/A
      justification: No new recursion or unbounded allocation.
    - id: AT-8
      status: N/A
      justification: No dependency pin change.
    - id: AT-9
      status: ATTACKED
      evidence: The registry's MoR predicate DML residual is FIXED and the north-star MOR DML row is ✅; the remaining v3 refusals named are not MoR; the v3 opt-in refusal no longer states a false MoR limitation.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md, crates/repark-sql/src/create_table.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Five clauses pinned; maps lockstep; two mutations red then restored; the V3-COW-1 byte tripwire re-recorded.
      artifacts: [crates/repark-spark/src/tests/v3_lineage.rs]
  complete: true
```

## Oracle transcript (C-002)

Live oracle: PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
`local[1]`, `coalesce(1)` single-file seed `(id,name,_row_id,seq) =
(1,a,0,1),(2,b,1,1),(3,c,2,1)`, next-row-id 3, 1 data file, 0 delete files. Source table holds
one row, `id = 2`. Every target is `format-version = 3` with `write.delete.mode` /
`write.update.mode` / `write.merge.mode` = `merge-on-read`, each on a fresh table.
Interpreter `<pyspark-4.1.2-oracle>`. Transcript: this table.

| Cell | After (id,name,_row_id,seq) | next | first | added | data files | delete files | delete entry | DV records |
|---|---|---|---|---|---|---|---|---|
| MoR DELETE plain `id = 2` | (1,a,0,1),(3,c,2,1) | 3 | 3 | 0 | 1 | 1 | POSITION_DELETES / PUFFIN, file-scoped | 1 |
| MoR DELETE … IN | (1,a,0,1),(3,c,2,1) | 3 | 3 | 0 | 1 | 1 | POSITION_DELETES / PUFFIN, file-scoped | 1 |
| MoR DELETE … NOT IN | (2,b,1,1) | 3 | 3 | 0 | 1 | 1 | POSITION_DELETES / PUFFIN, file-scoped | 2 |
| MoR DELETE … EXISTS | (1,a,0,1),(3,c,2,1) | 3 | 3 | 0 | 1 | 1 | POSITION_DELETES / PUFFIN, file-scoped | 1 |
| MoR DELETE … NOT EXISTS | (2,b,1,1) | 3 | 3 | 0 | 1 | 1 | POSITION_DELETES / PUFFIN, file-scoped | 2 |
| MoR UPDATE plain `id = 2` | (1,a,0,1),(2,m,1,2),(3,c,2,1) | 4 | 3 | 1 | 2 | 1 | POSITION_DELETES / PUFFIN, file-scoped | 1 |
| MoR UPDATE … IN | (1,a,0,1),(2,m,1,2),(3,c,2,1) | 4 | 3 | 1 | 2 | 1 | POSITION_DELETES / PUFFIN, file-scoped | 1 |
| MoR DELETE … IN, `write.delete.granularity` = `file` | (1,a,0,1),(3,c,2,1) | 3 | 3 | 0 | 1 | 1 | POSITION_DELETES / PUFFIN, file-scoped | 1 |
| MoR DELETE … IN, `write.delete.granularity` = `partition` | (1,a,0,1),(3,c,2,1) | 3 | 3 | 0 | 1 | 1 | POSITION_DELETES / PUFFIN, file-scoped | 1 |

Snapshot summary: DELETE cells are `operation = delete`, `added-delete-files 1`,
`added-dvs 1`, `added-position-deletes` = the DV record count, no `added-records`; UPDATE
cells are `operation = overwrite`, `added-records 1`, `added-data-files 1`,
`added-delete-files 1`, `added-dvs 1`, `added-position-deletes 1`. Every delete entry's
`referenced_data_file` is the seed data file and `content_offset` is 4. `write.delete.granularity`
has no effect on v3 — a deletion vector is file-scoped by construction. The IN / NOT IN / EXISTS /
NOT EXISTS / UPDATE-IN rows reproduce V3-8's MoR transcript exactly.

## Engine after the lift (C-003, C-004)

Same seed; created **and** adopted (`register_table`) v3.

| Cell | Engine after | next / first / added | data files | delete entry | Verdict |
|---|---|---|---|---|---|
| MoR DELETE … IN / EXISTS / plain | (1,a,0,1),(3,c,2,1) | 3 / 3 / 0 | 1 | 1 Puffin, POSITION_DELETES, file-scoped, 1 record | Spark-equal |
| MoR DELETE … NOT IN / NOT EXISTS | (2,b,1,1) | 3 / 3 / 0 | 1 | 1 Puffin, 2 records | Spark-equal |
| MoR UPDATE … IN / plain | (1,a,0,1),(2,m,1,2),(3,c,2,1) | 4 / 3 / 1 | 2 | 1 Puffin, 1 record | Spark-equal |
| MoR DELETE … IN, granularity `partition` | (1,a,0,1),(3,c,2,1) | 3 / 3 / 0 | 1 | 1 Puffin, file-scoped | Spark-equal |
| MoR DELETE … IN matching nothing | seed unchanged | next 3 | 1 | none | no delete file, no lineage move |
| v2 MoR DELETE / UPDATE … IN (control) | Spark's rows | — | — | 1 Parquet, POSITION_DELETES, `referenced_data_file` NULL | unchanged by the lift |
| shared-Puffin partitioned fixture, MoR DELETE … IN | (3,c,0),(4,d,1),(6,f,1) | — | 2 | 2 file-scoped DVs in one new shared Puffin | untouched sibling survives |

**The route.** `resolve_write_mode`'s `format_version != FormatVersion::V2` became
`< FormatVersion::V2`, with `resolve_merge_mode`'s message wording. Nothing else changed:
`execute_predicate_dml` already hands its `(_file, _pos)` pairs to `commit_row_delta_kind`,
which calls `dv_close::prepare_row_delta_deletes` — V2 → `write_position_deletes`, V3 →
`close_touched_dv_containers`. The MoR UPDATE data batch already carried the v3 lineage pair
(V3-8's `predicate_dml/lineage.rs`), so `_row_id` survives and `_last_updated_sequence_number`
is written NULL and reads back as the new sequence number, exactly as Spark's does.

**Shared-Puffin closure.** Closing a touched container rewrites **every** blob it holds into a
new container, so the untouched sibling's DV changes path but keeps its positions and its
`referenced_data_file`. Pinned by
`subquery_delete_on_the_shared_puffin_v3_table_keeps_the_untouched_sibling` (both referenced
data files keep a live DV, the two blobs stay in one container, the container path moves).

**Mutation.** Gate restored to `!= FormatVersion::V2`: **17 red** — 12 of the 14
`v3_mor_dml.rs` cells, the `v3e4.rs` shared-Puffin cell, the repurposed `v3_subquery_dml.rs`
cell, the ANSI twin, the facade twin and the live cell. The two `v3_mor_dml.rs` cells that
stay green are exactly the controls that must: `created_v3_mor_plain_where_dml_matches_the_subquery_cell`
(plain `WHERE` never routed through this gate) and
`v2_mor_subquery_dml_still_writes_parquet_position_deletes`. Restored. Second mutation —
`resolve_write_mode` returns `CopyOnWrite` for v3 after the granularity parse: **16 red** —
11 `v3_mor_dml.rs` cells, the shared-Puffin cell, the repurposed cell, the ANSI twin, the
facade twin and the live cell; the no-match cell also stays green here because a COW rewrite
of nothing is indistinguishable. Restored. Both mutations leave the v2 control green, so the
pins distinguish the deletion-vector path rather than merely "a delete happened".

**Opt-in message (C-006).** Orchestrator addendum, 2026-09-02. `parse_format_version` on
both doors refused a v3 CREATE with "(v3 tables cannot yet do merge-on-read row-level writes;
default create stays format v2)". After the lift the parenthetical's first half is false on
every door. Grep over `crates/` and `python/` found **no** pin asserting that substring — the
existing refusal pins assert only `repark.sql.allowCreateFormatVersion3` and the property name
— so nothing was a Spark-measured value and no HALT was owed. The message now reads
"… = true (default create stays format v2)", and all three doors plus the facade assert the
error no longer contains `merge-on-read`.

**Facade interpreter.** `.venv/bin/pytest`'s shebang points elsewhere. Facade gates in this
unit run as `.venv/bin/python -m pytest …` after `make develop`. The live cell needs the
`record` extra, then `make develop`, then `REPARK_PARITY_LIVE=1
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1`.

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-v3-9-mor-predicate-dml-dv
  pr_unit: v3-9-mor-predicate-dml-dv
  criteria:
    blast_radius: FAIL (predicate DML write path on every v3 MoR table)
    reversibility: PASS (one revert commit; no migration)
    size: PASS (one comparison plus pins, registry and maps)
    novelty: PASS (reuses V3-7's DV close path; no new dependency)
    sensitivity: FAIL (write/commit path)
    clarity: PASS (charter frozen 2026-09-02; five clauses)
  path: STANDARD
  recorded_by: Actor
```
