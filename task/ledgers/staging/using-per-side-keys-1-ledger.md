# Unit ledger — USING-PER-SIDE-KEYS-1 · coalesced USING star and per-side key fields

**Date:** 2026-10-07 · **Branch:** `fix/using-per-side-keys-1` · **Base:** `3fbcb2ca` (`main`)
· **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: high** (changes shown values on two doors).

**Order:** the orchestrator's brief USING-PER-SIDE-KEYS-1, the row of that name in
[the v1.5.3 card](../../roadmap/mid-term/v1-5-3-card-2026-10-04.md), following
[attr-id-1-ledger.md](attr-id-1-ledger.md) C-066 (R-R6-1..R-R6-3).

**State: HALT after step 1.** Step 0 (the measured grid) and step 1 (the design note) are
filed here. No product code changed. The brief stops the unit when the design needs a change
outside `repark-core` and `repark-spark`; §3 shows that the DataFrame door does, and why. Every
clause below is `OPEN` and names the question or the work that closes it.

**Starting point.** The card names `d66de2e3` (the reverted coalesce). That commit is not in this
clone and `git fetch origin d66de2e3` finds no such ref (it was squashed away), so it was read
through C-066's account only: it put the coalesce in the merged projection in place of the left
key, the left key left the output, and every left-side reference main answers Spark-exact began
to refuse.

## PROPOSITION LEDGER — USING-PER-SIDE-KEYS-1 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The acceptance grid is recorded on live Spark 4.1.2 and on RePark `main` `3fbcb2ca`: 1,090 cells per engine, both legs reproduced byte-identical on a second run. The card's 2026-10-06 table agrees with the oracle on every cell it names. | Value pins for every cell that changes, on both doors. | OPEN | §1, §2; `using-per-side-keys-1-probes/`. Closes when the changed cells are pinned. |
| C-002 | DataFrame door: the shown key of a `right` `USING` join is the right key and of a `full` join is `coalesce(left, right)`; star, the unqualified key, `frame["id"]`, `withColumn`, `groupBy`, `distinct`, `union` and a second `USING` join read that value. | One value pin per shape and join type; mutation "remove the coalesce". | OPEN | §2.1, §2.5. Blocked on Q1. |
| C-003 | DataFrame door: `l.id` and `r.id` (strings, `col`, `selectExpr`, getitem on the joined-in frame, stale references) answer the per-side key in select, filter, sort and join condition on `inner`/`left`/`right`/`full`; `semi`/`anti` keep refusing the right side. | Value pins on every choke the R6 refusal pins hold today; mutation "drop a hidden field". | OPEN | §2.1, §2.2. Blocked on Q1: the binding sites are in the facade and `repark-python`. |
| C-004 | A hidden per-side key is never a field of the frame's plan output, so `columns`, `schema`, writes and exports cannot show it. | Pins on `columns`, `schema`, a parquet write and `toPandas`; mutation "let a hidden field leak into `columns`". | OPEN | §3.2. Blocked on Q1. |
| C-005 | Attribute ids stay stable: `inner`/`left` keep the left key's id on the shown key, `right` shows the right key's id, `full` mints one id for the coalesce; per-side ids are unchanged. | The attr-id suites identical; a pin per join type on the shown key's id. | OPEN | §3.2. The `full` rule follows Spark's analyzed plan (§1). Blocked on Q1. |
| C-006 | SQL door: star and the unqualified key over `right`/`full` `USING` joins read the coalesced key in select, filter, sort, group-by and a later join condition. | Rust pins in `repark-core` plus facade pins. | OPEN | §2.3, §2.4, §3.3. Not blocked by Q1; held for Q2. |
| C-007 | SQL door: an unqualified `USING` key in `WHERE` answers on every join type. `main` refuses all eight measured cells with `AMBIGUOUS_REFERENCE`. | Pins on six join types. | OPEN | §2.3. Not in the card; Q3 asks whether it rides with this unit. |
| C-008 | A chained `full` `USING` join matches on the coalesced key. `main` matches on the left key and answers wrong rows on both doors. | Value pins on both doors. | OPEN | §2.5 `chain · full · using-again`, §3.3. Follows from C-002 and C-006. |
| C-009 | Non-`USING` joins and `on=` expression joins are unchanged. | Control pins. | OPEN | §2.5 control rows equal on Spark and `main` today. |
| C-010 | Mixed `INT`/`STRING` keys keep `main`'s left-key type (`INT`); Spark answers `STRING` on `right` and `BIGINT` on `full`. | A declared-divergence pin. | OPEN | §2.5 `mixed` rows. The card says `BIGINT` only; the oracle adds `right` = `STRING`. |

## 1. Step 0 — how the grid was measured

Probe: [using-per-side-keys-1-probes/grid.py](using-per-side-keys-1-probes/grid.py), one script
for both engines. Spark leg: `/tmp/sparkenv/bin/python` (PySpark 4.1.2, Zulu 17, `local[1]`).
RePark leg: the `main` build of `3fbcb2ca` (`make develop`). Frames: left `(1,a) (2,b) (3,c)` as
`id INT, s STRING`; right `(2,x) (3,y) (4,z)` as `id INT, t STRING`; a third frame `q` with the
right frame's ids for the join-condition cells.

Spark's analyzed plan for the `full` join, which the design follows:

```
Project [coalesce(id#0, id#2) AS id#4, s#1, t#3]
+- Join FullOuter, (id#0 = id#2)
   :- SubqueryAlias l
   +- SubqueryAlias r
```

The shown key is a new attribute (`id#4`); both side keys stay below the projection and are
reachable by qualifier and by held reference. On `right` the shown key is the right key itself.

**Counts.** Of 1,090 cells, 549 are byte-identical and 130 more refuse with the same condition.
The other 411 differ:

| Class | Cells | In this unit |
|---|---|---|
| Rows differ (shown key `NULL` where Spark coalesces; `r.*`; sort order) | 134 | yes |
| `main` refuses with the R6 `USING` refusal, Spark answers | 76 | yes |
| `main` raises `MISSING_ATTRIBUTES`, Spark answers (stale right key in a join condition) | 10 | yes |
| `main` raises `AMBIGUOUS_REFERENCE`, Spark answers (SQL `WHERE id`) | 8 | Q3 |
| Both refuse, different error text or class | 82 | no |
| Rows equal, column name differs (`(l.id + 1)` for Spark's `(id + 1)`, `count(*)`) | 63 | no |
| `main` raises a duplicate-name error on `SELECT id, l.id, r.id` | 8 | no (also on `SELECT l.id, l.id` over an `ON` join) |
| `main` raises `Invalid qualifier` on `tl.*` / `tr.*` over unaliased views | 10 | no (also over an `ON` join) |
| `main` raises `type_coercion` on `filter(col("l.id") > 2)` | 7 | no (also over an `ON` join) |
| Spark refuses a stale right key in `sort` over `semi`/`anti`, `main` answers | 8 | no |
| Spark raises `INTERNAL_ERROR` on `select(frame["l.id"])` / `frame["r.id"]`, `main` answers or refuses | 3 | no |
| Schema of mixed-type keys | 2 | declared (C-010) |

**The card against the oracle.** The card's acceptance values hold: `full` `r.id` = 2,3,4,NULL;
`left` `r.id` = 2,3,NULL; `right` `r.id` = 2,3,4; left-side rows unchanged; `main` answers the
left key on the SQL door. The oracle adds four facts the card does not carry:

1. A chained `full` `USING` join answers wrong rows on `main`, not only a `NULL` key (C-008).
2. The SQL door refuses an unqualified `USING` key in `WHERE` on every join type (C-007).
3. Mixed keys answer `STRING` on `right` (C-010).
4. Spark itself fails with `INTERNAL_ERROR` ("Hit an invalid Dataset column reference") on
   `select(frame["l.id"])` over `right`/`full` and `select(frame["r.id"])` over
   `inner`/`left`/`full`, while the same getitem answers in filter, sort and a join condition.
   The engine should answer the per-side value there, not copy the failure.

## 2. The grid

One row per cell, one column per join type. A plain cell is the answer both engines give. A bold
cell is `Spark ‖ main`. Select cells list the one column's values; filter (`> 2`), sort and
join-condition cells list the shown key of each answered row, in row order for sort. `∅` is
`NULL`. `refuse` is the R6 refusal. `E:42703` is `UNRESOLVED_COLUMN`; the other `E:` marks are
the error classes of §1. The full answers are in the two JSON files.

### 2.1 DataFrame door, frames aliased `l` and `r`

`L[id]` / `R[id]` are getitem on the aliased frames that were joined; `stale-left[id]` /
`stale-right[id]` are getitem on the frames before `.alias`.

| Cell | inner | left | right | full | semi | anti |
|---|---|---|---|---|---|---|
| `columns` | ['id', 's', 't'] | ['id', 's', 't'] | ['id', 's', 't'] | ['id', 's', 't'] | ['id', 's'] | ['id', 's'] |
| `schema` | `struct<id:int,s:string,t:string>` | `struct<id:int,s:string,t:string>` | `struct<id:int,s:string,t:string>` | `struct<id:int,s:string,t:string>` | `struct<id:int,s:string>` | `struct<id:int,s:string>` |
| `star` | (2,b,x) (3,c,y) | (1,a,∅) (2,b,x) (3,c,y) | **(2,b,x) (3,c,y) (4,∅,z) ‖ (2,b,x) (3,c,y) (∅,∅,z)** | **(1,a,∅) (2,b,x) (3,c,y) (4,∅,z) ‖ (1,a,∅) (2,b,x) (3,c,y) (∅,∅,z)** | (2,b) (3,c) | (1,a) |
| `select l.id, r.id` | (2,2) (3,3) | **(1,∅) (2,2) (3,3) ‖ refuse** | **(2,2) (3,3) (∅,4) ‖ refuse** | **(1,∅) (2,2) (3,3) (∅,4) ‖ refuse** | E:42703 | E:42703 |
| `select l.*` | (2,b) (3,c) | (1,a) (2,b) (3,c) | (2,b) (3,c) (∅,∅) | (1,a) (2,b) (3,c) (∅,∅) | (2,b) (3,c) | (1,a) |
| `select r.*` | (2,x) (3,y) | **(2,x) (3,y) (∅,∅) ‖ (1,∅) (2,x) (3,y)** | **(2,x) (3,y) (4,z) ‖ (2,x) (3,y) (∅,z)** | **(2,x) (3,y) (4,z) (∅,∅) ‖ (1,∅) (2,x) (3,y) (∅,z)** | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** |
| `selectExpr l.id + 0` | 2,3 | 1,2,3 | 2,3,∅ | 1,2,3,∅ | 2,3 | 1 |
| `selectExpr r.id + 0` | 2,3 | **2,3,∅ ‖ refuse** | **2,3,4 ‖ refuse** | **2,3,4,∅ ‖ refuse** | E:42703 | E:42703 |
| `select col(l.id) + 1` | 3,4 | 2,3,4 | 3,4,∅ | 2,3,4,∅ | 3,4 | 2 |
| `select col(r.id) + 1` | 3,4 | **3,4,∅ ‖ refuse** | **3,4,5 ‖ refuse** | **3,4,5,∅ ‖ refuse** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** |
| `select id` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | 2,3 | 1 |
| `select l.id` | 2,3 | 1,2,3 | 2,3,∅ | 1,2,3,∅ | 2,3 | 1 |
| `select r.id` | 2,3 | **2,3,∅ ‖ refuse** | **2,3,4 ‖ refuse** | **2,3,4,∅ ‖ refuse** | E:42703 | E:42703 |
| `select col(id)` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | 2,3 | 1 |
| `select col(l.id)` | 2,3 | 1,2,3 | 2,3,∅ | 1,2,3,∅ | 2,3 | 1 |
| `select col(r.id)` | 2,3 | **2,3,∅ ‖ refuse** | **2,3,4 ‖ refuse** | **2,3,4,∅ ‖ refuse** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** |
| `select L[id]` | 2,3 | 1,2,3 | 2,3,∅ | 1,2,3,∅ | 2,3 | 1 |
| `select R[id]` | 2,3 | **2,3,∅ ‖ refuse** | **2,3,4 ‖ refuse** | **2,3,4,∅ ‖ refuse** | E:missing-attr | E:missing-attr |
| `select frame[id]` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | 2,3 | 1 |
| `select frame[l.id]` | 2,3 | 1,2,3 | **E:internal ‖ 2,3,∅** | **E:internal ‖ 1,2,3,∅** | 2,3 | 1 |
| `select frame[r.id]` | **E:internal ‖ 2,3** | **E:internal ‖ refuse** | **2,3,4 ‖ refuse** | **E:internal ‖ refuse** | E:42703 | E:42703 |
| `select stale-left[id]` | 2,3 | 1,2,3 | 2,3,∅ | 1,2,3,∅ | 2,3 | 1 |
| `select stale-right[id]` | 2,3 | **2,3,∅ ‖ refuse** | **2,3,4 ‖ refuse** | **2,3,4,∅ ‖ refuse** | E:missing-attr | E:missing-attr |
| `filter id` | 3 | 3 | **3,4 ‖ 3** | **3,4 ‖ 3** | 3 | (none) |
| `filter l.id` | 3 | 3 | 3 | 3 | 3 | (none) |
| `filter r.id` | 3 | **3 ‖ refuse** | **3,4 ‖ refuse** | **3,4 ‖ refuse** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** |
| `filter col(id)` | 3 | 3 | **3,4 ‖ 3** | **3,4 ‖ 3** | 3 | (none) |
| `filter col(l.id)` | **3 ‖ E:type-coercion** | **3 ‖ E:type-coercion** | **3 ‖ E:type-coercion** | **3 ‖ E:type-coercion** | **3 ‖ E:type-coercion** | **(none) ‖ E:type-coercion** |
| `filter col(r.id)` | **3 ‖ E:type-coercion** | **3 ‖ refuse** | **3,4 ‖ refuse** | **3,4 ‖ refuse** | **E:42703 ‖ E:type-coercion** | **E:42703 ‖ E:type-coercion** |
| `filter L[id]` | 3 | 3 | 3 | 3 | 3 | (none) |
| `filter R[id]` | 3 | **3 ‖ refuse** | **3,4 ‖ refuse** | **3,4 ‖ refuse** | E:missing-attr | E:missing-attr |
| `filter frame[id]` | 3 | 3 | **3,4 ‖ 3** | **3,4 ‖ 3** | 3 | (none) |
| `filter frame[l.id]` | 3 | 3 | 3 | 3 | 3 | (none) |
| `filter frame[r.id]` | 3 | **3 ‖ refuse** | **3,4 ‖ refuse** | **3,4 ‖ refuse** | E:42703 | E:42703 |
| `filter stale-left[id]` | 3 | 3 | 3 | 3 | 3 | (none) |
| `filter stale-right[id]` | 3 | **3 ‖ refuse** | **3,4 ‖ refuse** | **3,4 ‖ refuse** | E:missing-attr | E:missing-attr |
| `sort id` | 2,3 | 1,2,3 | **2,3,4 ‖ ∅,2,3** | **1,2,3,4 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort l.id` | 2,3 | 1,2,3 | **4,2,3 ‖ ∅,2,3** | **4,1,2,3 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort r.id` | 2,3 | **1,2,3 ‖ refuse** | **2,3,4 ‖ refuse** | **1,2,3,4 ‖ refuse** | E:42703 | E:42703 |
| `sort col(id)` | 2,3 | 1,2,3 | **2,3,4 ‖ ∅,2,3** | **1,2,3,4 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort col(l.id)` | 2,3 | 1,2,3 | **4,2,3 ‖ ∅,2,3** | **4,1,2,3 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort col(r.id)` | 2,3 | **1,2,3 ‖ refuse** | **2,3,4 ‖ refuse** | **1,2,3,4 ‖ refuse** | E:42703 | E:42703 |
| `sort L[id]` | 2,3 | 1,2,3 | **4,2,3 ‖ ∅,2,3** | **4,1,2,3 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort R[id]` | 2,3 | **1,2,3 ‖ refuse** | **2,3,4 ‖ refuse** | **1,2,3,4 ‖ refuse** | **E:missing-attr ‖ 2,3** | **E:missing-attr ‖ 1** |
| `sort frame[id]` | 2,3 | 1,2,3 | **2,3,4 ‖ ∅,2,3** | **1,2,3,4 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort frame[l.id]` | 2,3 | 1,2,3 | **4,2,3 ‖ ∅,2,3** | **4,1,2,3 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort frame[r.id]` | 2,3 | **1,2,3 ‖ refuse** | **2,3,4 ‖ refuse** | **1,2,3,4 ‖ refuse** | E:42703 | E:42703 |
| `sort stale-left[id]` | 2,3 | 1,2,3 | **4,2,3 ‖ ∅,2,3** | **4,1,2,3 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort stale-right[id]` | 2,3 | **1,2,3 ‖ refuse** | **2,3,4 ‖ refuse** | **1,2,3,4 ‖ refuse** | **E:missing-attr ‖ 2,3** | **E:missing-attr ‖ 1** |
| `joincond id` | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous |
| `joincond l.id` | 2,3 | 2,3 | 2,3 | 2,3 | 2,3 | (none) |
| `joincond r.id` | 2,3 | **2,3 ‖ refuse** | **2,3,4 ‖ refuse** | **2,3,4 ‖ refuse** | E:42703 | E:42703 |
| `joincond col(id)` | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous |
| `joincond col(l.id)` | 2,3 | 2,3 | 2,3 | 2,3 | 2,3 | (none) |
| `joincond col(r.id)` | 2,3 | **2,3 ‖ refuse** | **2,3,4 ‖ refuse** | **2,3,4 ‖ refuse** | E:42703 | E:42703 |
| `joincond L[id]` | 2,3 | 2,3 | 2,3 | 2,3 | 2,3 | (none) |
| `joincond R[id]` | **2,3 ‖ E:missing-attr** | **2,3 ‖ refuse** | **2,3,4 ‖ refuse** | **2,3,4 ‖ refuse** | E:missing-attr | E:missing-attr |
| `joincond frame[id]` | 2,3 | 2,3 | **2,3,4 ‖ 2,3** | **2,3,4 ‖ 2,3** | 2,3 | (none) |
| `joincond frame[l.id]` | 2,3 | 2,3 | 2,3 | 2,3 | 2,3 | (none) |
| `joincond frame[r.id]` | 2,3 | **2,3 ‖ refuse** | **2,3,4 ‖ refuse** | **2,3,4 ‖ refuse** | E:42703 | E:42703 |
| `joincond stale-left[id]` | 2,3 | 2,3 | 2,3 | 2,3 | 2,3 | (none) |
| `joincond stale-right[id]` | **2,3 ‖ E:missing-attr** | **2,3 ‖ refuse** | **2,3,4 ‖ refuse** | **2,3,4 ‖ refuse** | E:missing-attr | E:missing-attr |

### 2.2 DataFrame door, frames not aliased

`l.id` and `r.id` name no frame here, so both engines refuse them; the held references carry the
cells.

| Cell | inner | left | right | full | semi | anti |
|---|---|---|---|---|---|---|
| `columns` | ['id', 's', 't'] | ['id', 's', 't'] | ['id', 's', 't'] | ['id', 's', 't'] | ['id', 's'] | ['id', 's'] |
| `schema` | `struct<id:int,s:string,t:string>` | `struct<id:int,s:string,t:string>` | `struct<id:int,s:string,t:string>` | `struct<id:int,s:string,t:string>` | `struct<id:int,s:string>` | `struct<id:int,s:string>` |
| `star` | (2,b,x) (3,c,y) | (1,a,∅) (2,b,x) (3,c,y) | **(2,b,x) (3,c,y) (4,∅,z) ‖ (2,b,x) (3,c,y) (∅,∅,z)** | **(1,a,∅) (2,b,x) (3,c,y) (4,∅,z) ‖ (1,a,∅) (2,b,x) (3,c,y) (∅,∅,z)** | (2,b) (3,c) | (1,a) |
| `select l.id, r.id` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `select l.*` | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** |
| `select r.*` | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** |
| `selectExpr l.id + 0` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `selectExpr r.id + 0` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `select col(l.id) + 1` | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** |
| `select col(r.id) + 1` | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** |
| `select id` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | 2,3 | 1 |
| `select l.id` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `select r.id` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `select col(id)` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | 2,3 | 1 |
| `select col(l.id)` | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** |
| `select col(r.id)` | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** |
| `select L[id]` | 2,3 | 1,2,3 | 2,3,∅ | 1,2,3,∅ | 2,3 | 1 |
| `select R[id]` | 2,3 | **2,3,∅ ‖ 1,2,3** | **2,3,4 ‖ 2,3,∅** | **2,3,4,∅ ‖ 1,2,3,∅** | E:missing-attr | E:missing-attr |
| `select frame[id]` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | 2,3 | 1 |
| `select frame[l.id]` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `select frame[r.id]` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `select stale-left[id]` | 2,3 | 1,2,3 | 2,3,∅ | 1,2,3,∅ | 2,3 | 1 |
| `select stale-right[id]` | 2,3 | **2,3,∅ ‖ 1,2,3** | **2,3,4 ‖ 2,3,∅** | **2,3,4,∅ ‖ 1,2,3,∅** | E:missing-attr | E:missing-attr |
| `filter id` | 3 | 3 | **3,4 ‖ 3** | **3,4 ‖ 3** | 3 | (none) |
| `filter l.id` | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** |
| `filter r.id` | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** |
| `filter col(id)` | 3 | 3 | **3,4 ‖ 3** | **3,4 ‖ 3** | 3 | (none) |
| `filter col(l.id)` | **E:42703 ‖ E:type-coercion** | **E:42703 ‖ E:type-coercion** | **E:42703 ‖ E:type-coercion** | **E:42703 ‖ E:type-coercion** | **E:42703 ‖ E:type-coercion** | **E:42703 ‖ E:type-coercion** |
| `filter col(r.id)` | **E:42703 ‖ E:type-coercion** | **E:42703 ‖ E:type-coercion** | **E:42703 ‖ E:type-coercion** | **E:42703 ‖ E:type-coercion** | **E:42703 ‖ E:type-coercion** | **E:42703 ‖ E:type-coercion** |
| `filter L[id]` | 3 | 3 | 3 | 3 | 3 | (none) |
| `filter R[id]` | 3 | 3 | **3,4 ‖ 3** | **3,4 ‖ 3** | E:missing-attr | E:missing-attr |
| `filter frame[id]` | 3 | 3 | **3,4 ‖ 3** | **3,4 ‖ 3** | 3 | (none) |
| `filter frame[l.id]` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `filter frame[r.id]` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `filter stale-left[id]` | 3 | 3 | 3 | 3 | 3 | (none) |
| `filter stale-right[id]` | 3 | 3 | **3,4 ‖ 3** | **3,4 ‖ 3** | E:missing-attr | E:missing-attr |
| `sort id` | 2,3 | 1,2,3 | **2,3,4 ‖ ∅,2,3** | **1,2,3,4 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort l.id` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `sort r.id` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `sort col(id)` | 2,3 | 1,2,3 | **2,3,4 ‖ ∅,2,3** | **1,2,3,4 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort col(l.id)` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `sort col(r.id)` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `sort L[id]` | 2,3 | 1,2,3 | **4,2,3 ‖ ∅,2,3** | **4,1,2,3 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort R[id]` | 2,3 | 1,2,3 | **2,3,4 ‖ ∅,2,3** | **1,2,3,4 ‖ ∅,1,2,3** | **E:missing-attr ‖ 2,3** | **E:missing-attr ‖ 1** |
| `sort frame[id]` | 2,3 | 1,2,3 | **2,3,4 ‖ ∅,2,3** | **1,2,3,4 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort frame[l.id]` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `sort frame[r.id]` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `sort stale-left[id]` | 2,3 | 1,2,3 | **4,2,3 ‖ ∅,2,3** | **4,1,2,3 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort stale-right[id]` | 2,3 | 1,2,3 | **2,3,4 ‖ ∅,2,3** | **1,2,3,4 ‖ ∅,1,2,3** | **E:missing-attr ‖ 2,3** | **E:missing-attr ‖ 1** |
| `joincond id` | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous |
| `joincond l.id` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `joincond r.id` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `joincond col(id)` | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous |
| `joincond col(l.id)` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `joincond col(r.id)` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `joincond L[id]` | 2,3 | 2,3 | 2,3 | 2,3 | 2,3 | (none) |
| `joincond R[id]` | **2,3 ‖ E:missing-attr** | **2,3 ‖ E:missing-attr** | **2,3,4 ‖ E:missing-attr** | **2,3,4 ‖ E:missing-attr** | E:missing-attr | E:missing-attr |
| `joincond frame[id]` | 2,3 | 2,3 | **2,3,4 ‖ 2,3** | **2,3,4 ‖ 2,3** | 2,3 | (none) |
| `joincond frame[l.id]` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `joincond frame[r.id]` | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 | E:42703 |
| `joincond stale-left[id]` | 2,3 | 2,3 | 2,3 | 2,3 | 2,3 | (none) |
| `joincond stale-right[id]` | **2,3 ‖ E:missing-attr** | **2,3 ‖ E:missing-attr** | **2,3,4 ‖ E:missing-attr** | **2,3,4 ‖ E:missing-attr** | E:missing-attr | E:missing-attr |

### 2.3 SQL door, `tl l … JOIN tr r USING (id)`

| Cell | inner | left | right | full | semi | anti |
|---|---|---|---|---|---|---|
| `star` | (2,b,x) (3,c,y) | (1,a,∅) (2,b,x) (3,c,y) | **(2,b,x) (3,c,y) (4,∅,z) ‖ (2,b,x) (3,c,y) (∅,∅,z)** | **(1,a,∅) (2,b,x) (3,c,y) (4,∅,z) ‖ (1,a,∅) (2,b,x) (3,c,y) (∅,∅,z)** | (2,b) (3,c) | (1,a) |
| `select l.*` | (2,b) (3,c) | (1,a) (2,b) (3,c) | (2,b) (3,c) (∅,∅) | (1,a) (2,b) (3,c) (∅,∅) | (2,b) (3,c) | (1,a) |
| `select r.*` | (2,x) (3,y) | (2,x) (3,y) (∅,∅) | (2,x) (3,y) (4,z) | (2,x) (3,y) (4,z) (∅,∅) | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** |
| `select l.id, r.id` | (2,2) (3,3) | (1,∅) (2,2) (3,3) | (2,2) (3,3) (∅,4) | (1,∅) (2,2) (3,3) (∅,4) | E:42703 | E:42703 |
| `select id, l.id, r.id` | **(2,2,2) (3,3,3) ‖ E:dup-name** | **(1,1,∅) (2,2,2) (3,3,3) ‖ E:dup-name** | **(2,2,2) (3,3,3) (4,∅,4) ‖ E:dup-name** | **(1,1,∅) (2,2,2) (3,3,3) (4,∅,4) ‖ E:dup-name** | E:42703 | E:42703 |
| `select id` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | 2,3 | 1 |
| `select l.id` | 2,3 | 1,2,3 | 2,3,∅ | 1,2,3,∅ | 2,3 | 1 |
| `select r.id` | 2,3 | 2,3,∅ | 2,3,4 | 2,3,4,∅ | E:42703 | E:42703 |
| `select+1 id` | 3,4 | 2,3,4 | **3,4,5 ‖ 3,4,∅** | **2,3,4,5 ‖ 2,3,4,∅** | 3,4 | 2 |
| `select+1 l.id` | 3,4 | 2,3,4 | 3,4,∅ | 2,3,4,∅ | 3,4 | 2 |
| `select+1 r.id` | 3,4 | 3,4,∅ | 3,4,5 | 3,4,5,∅ | E:42703 | E:42703 |
| `filter id` | **3 ‖ E:ambiguous** | **3 ‖ E:ambiguous** | **3,4 ‖ E:ambiguous** | **3,4 ‖ E:ambiguous** | 3 | (none) |
| `filter l.id` | 3 | 3 | 3 | 3 | 3 | (none) |
| `filter r.id` | 3 | 3 | **3,4 ‖ 3,∅** | **3,4 ‖ 3,∅** | E:42703 | E:42703 |
| `sort id` | 2,3 | 1,2,3 | **2,3,4 ‖ ∅,2,3** | **1,2,3,4 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort l.id` | 2,3 | 1,2,3 | **4,2,3 ‖ ∅,2,3** | **4,1,2,3 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort r.id` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** |
| `joincond id` | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous |
| `joincond l.id` | 2,3 | 2,3 | 2,3 | 2,3 | 2,3 | (none) |
| `joincond r.id` | 2,3 | 2,3 | **2,3,4 ‖ 2,3,∅** | **2,3,4 ‖ 2,3,∅** | E:42703 | E:42703 |
| `groupby id` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | 2,3 | 1 |
| `groupby l.id` | 2,3 | 1,2,3 | 2,3,∅ | 1,2,3,∅ | 2,3 | 1 |
| `groupby r.id` | 2,3 | 2,3,∅ | 2,3,4 | 2,3,4,∅ | E:42703 | E:42703 |

### 2.4 SQL door, `tl … JOIN tr USING (id)` (qualifiers `tl` and `tr`)

| Cell | inner | left | right | full | semi | anti |
|---|---|---|---|---|---|---|
| `star` | (2,b,x) (3,c,y) | (1,a,∅) (2,b,x) (3,c,y) | **(2,b,x) (3,c,y) (4,∅,z) ‖ (2,b,x) (3,c,y) (∅,∅,z)** | **(1,a,∅) (2,b,x) (3,c,y) (4,∅,z) ‖ (1,a,∅) (2,b,x) (3,c,y) (∅,∅,z)** | (2,b) (3,c) | (1,a) |
| `select l.*` | **(2,b) (3,c) ‖ E:qualifier** | **(1,a) (2,b) (3,c) ‖ E:qualifier** | **(2,b) (3,c) (∅,∅) ‖ E:qualifier** | **(1,a) (2,b) (3,c) (∅,∅) ‖ E:qualifier** | **(2,b) (3,c) ‖ E:qualifier** | **(1,a) ‖ E:qualifier** |
| `select r.*` | **(2,x) (3,y) ‖ E:qualifier** | **(2,x) (3,y) (∅,∅) ‖ E:qualifier** | **(2,x) (3,y) (4,z) ‖ E:qualifier** | **(2,x) (3,y) (4,z) (∅,∅) ‖ E:qualifier** | **E:star ‖ E:qualifier** | **E:star ‖ E:qualifier** |
| `select l.id, r.id` | (2,2) (3,3) | (1,∅) (2,2) (3,3) | (2,2) (3,3) (∅,4) | (1,∅) (2,2) (3,3) (∅,4) | E:42703 | E:42703 |
| `select id, l.id, r.id` | **(2,2,2) (3,3,3) ‖ E:dup-name** | **(1,1,∅) (2,2,2) (3,3,3) ‖ E:dup-name** | **(2,2,2) (3,3,3) (4,∅,4) ‖ E:dup-name** | **(1,1,∅) (2,2,2) (3,3,3) (4,∅,4) ‖ E:dup-name** | E:42703 | E:42703 |
| `select id` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | 2,3 | 1 |
| `select l.id` | 2,3 | 1,2,3 | 2,3,∅ | 1,2,3,∅ | 2,3 | 1 |
| `select r.id` | 2,3 | 2,3,∅ | 2,3,4 | 2,3,4,∅ | E:42703 | E:42703 |
| `select+1 id` | 3,4 | 2,3,4 | **3,4,5 ‖ 3,4,∅** | **2,3,4,5 ‖ 2,3,4,∅** | 3,4 | 2 |
| `select+1 l.id` | 3,4 | 2,3,4 | 3,4,∅ | 2,3,4,∅ | 3,4 | 2 |
| `select+1 r.id` | 3,4 | 3,4,∅ | 3,4,5 | 3,4,5,∅ | E:42703 | E:42703 |
| `filter id` | **3 ‖ E:ambiguous** | **3 ‖ E:ambiguous** | **3,4 ‖ E:ambiguous** | **3,4 ‖ E:ambiguous** | 3 | (none) |
| `filter l.id` | 3 | 3 | 3 | 3 | 3 | (none) |
| `filter r.id` | 3 | 3 | **3,4 ‖ 3,∅** | **3,4 ‖ 3,∅** | E:42703 | E:42703 |
| `sort id` | 2,3 | 1,2,3 | **2,3,4 ‖ ∅,2,3** | **1,2,3,4 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort l.id` | 2,3 | 1,2,3 | **4,2,3 ‖ ∅,2,3** | **4,1,2,3 ‖ ∅,1,2,3** | 2,3 | 1 |
| `sort r.id` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | **E:42703 ‖ E:nofield** | **E:42703 ‖ E:nofield** |
| `joincond id` | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous | E:ambiguous |
| `joincond l.id` | 2,3 | 2,3 | 2,3 | 2,3 | 2,3 | (none) |
| `joincond r.id` | 2,3 | 2,3 | **2,3,4 ‖ 2,3,∅** | **2,3,4 ‖ 2,3,∅** | E:42703 | E:42703 |
| `groupby id` | 2,3 | 1,2,3 | **2,3,4 ‖ 2,3,∅** | **1,2,3,4 ‖ 1,2,3,∅** | 2,3 | 1 |
| `groupby l.id` | 2,3 | 1,2,3 | 2,3,∅ | 1,2,3,∅ | 2,3 | 1 |
| `groupby r.id` | 2,3 | 2,3,∅ | 2,3,4 | 2,3,4,∅ | E:42703 | E:42703 |

### 2.5 Chains, controls, mixed keys, two keys, `NATURAL`

`=` in the last column means `main` gives Spark's answer.

| Cell | Spark 4.1.2 | RePark main | |---|---|---|
| `chain · full · alias-x-r.id` | E:42703 | **refuse** |
| `chain · full · alias-x.id` | (1) (2) (3) (4) | **(1) (2) (3) (∅)** |
| `chain · full · distinct` | (1,a,∅) (2,b,x) (3,c,y) (4,∅,z) | **(1,a,∅) (2,b,x) (3,c,y) (∅,∅,z)** |
| `chain · full · drop-id` | (∅,z) (a,∅) (b,x) (c,y) | = |
| `chain · full · filter-then-r.id` | (2) (3) (4) (∅) | **refuse** |
| `chain · full · groupBy-id` | (1,1) (2,1) (3,1) (4,1) | **(1,1) (2,1) (3,1) (∅,1)** |
| `chain · full · select-then-r.id` | (2) (3) (4) (∅) | **refuse** |
| `chain · full · union` | (1,a,∅) (1,a,∅) (2,b,x) (2,b,x) (3,c,y) (3,c,y) (4,∅,z) (4,∅,z) | **(1,a,∅) (1,a,∅) (2,b,x) (2,b,x) (3,c,y) (3,c,y) (∅,∅,z) (∅,∅,z)** |
| `chain · full · using-again` | (1,a,∅,∅) (2,b,x,p) (3,c,y,q) (4,∅,z,r) | **(1,a,∅,∅) (2,b,x,p) (3,c,y,q) (∅,∅,∅,r) (∅,∅,z,∅)** |
| `chain · full · withColumn` | (1,a,∅,2) (2,b,x,3) (3,c,y,4) (4,∅,z,5) | **(1,a,∅,2) (2,b,x,3) (3,c,y,4) (∅,∅,z,∅)** |
| `chain · right · alias-x-r.id` | E:42703 | **refuse** |
| `chain · right · alias-x.id` | (2) (3) (4) | **(2) (3) (∅)** |
| `chain · right · distinct` | (2,b,x) (3,c,y) (4,∅,z) | **(2,b,x) (3,c,y) (∅,∅,z)** |
| `chain · right · drop-id` | (∅,z) (b,x) (c,y) | = |
| `chain · right · filter-then-r.id` | (2) (3) (4) | **refuse** |
| `chain · right · groupBy-id` | (2,1) (3,1) (4,1) | **(2,1) (3,1) (∅,1)** |
| `chain · right · select-then-r.id` | (2) (3) (4) | **refuse** |
| `chain · right · union` | (2,b,x) (2,b,x) (3,c,y) (3,c,y) (4,∅,z) (4,∅,z) | **(2,b,x) (2,b,x) (3,c,y) (3,c,y) (∅,∅,z) (∅,∅,z)** |
| `chain · right · using-again` | (2,b,x,p) (3,c,y,q) (4,∅,z,r) | **(2,b,x,p) (3,c,y,q) (∅,∅,∅,r) (∅,∅,z,∅)** |
| `chain · right · withColumn` | (2,b,x,3) (3,c,y,4) (4,∅,z,5) | **(2,b,x,3) (3,c,y,4) (∅,∅,z,∅)** |
| `control-expr · full · show` | (1,a,∅,∅) (2,b,2,x) (3,c,3,y) (∅,∅,4,z) | = |
| `control-expr · left · show` | (1,a,∅,∅) (2,b,2,x) (3,c,3,y) | = |
| `control-expr · right · show` | (2,b,2,x) (3,c,3,y) (∅,∅,4,z) | = |
| `control-on · full · columns` | ['id', 's', 'id', 't'] | = |
| `control-on · full · l.id,r.id` | (1,∅) (2,2) (3,3) (∅,4) | = |
| `control-on · full · show` | (1,a,∅,∅) (2,b,2,x) (3,c,3,y) (∅,∅,4,z) | = |
| `control-on · left · columns` | ['id', 's', 'id', 't'] | = |
| `control-on · left · l.id,r.id` | (1,∅) (2,2) (3,3) | = |
| `control-on · left · show` | (1,a,∅,∅) (2,b,2,x) (3,c,3,y) | = |
| `control-on · right · columns` | ['id', 's', 'id', 't'] | = |
| `control-on · right · l.id,r.id` | (2,2) (3,3) (∅,4) | = |
| `control-on · right · show` | (2,b,2,x) (3,c,3,y) (∅,∅,4,z) | = |
| `mixed · full · l.id` | (1) (2) (∅) | = |
| `mixed · full · r.id` | (2) (3) (∅) | **refuse** |
| `mixed · full · schema` | `struct<id:bigint>` | **`struct<id:int>`** |
| `mixed · full · star` | (1) (2) (3) | **(1) (2) (∅)** |
| `mixed · inner · l.id` | (2) | = |
| `mixed · inner · r.id` | (2) | = |
| `mixed · inner · schema` | `struct<id:int>` | = |
| `mixed · inner · star` | (2) | = |
| `mixed · left · l.id` | (1) (2) | = |
| `mixed · left · r.id` | (2) (∅) | **refuse** |
| `mixed · left · schema` | `struct<id:int>` | = |
| `mixed · left · star` | (1) (2) | = |
| `mixed · right · l.id` | (2) (∅) | = |
| `mixed · right · r.id` | (2) (3) | **refuse** |
| `mixed · right · schema` | `struct<id:string>` | **`struct<id:int>`** |
| `mixed · right · star` | (2) (3) | **(2) (∅)** |
| `natural · full · l.id,r.id` | (1,∅) (2,2) (3,3) (∅,4) | = |
| `natural · full · star` | (1,a,∅) (2,b,x) (3,c,y) (4,∅,z) | **(1,a,∅) (2,b,x) (3,c,y) (∅,∅,z)** |
| `natural · right · l.id,r.id` | (2,2) (3,3) (∅,4) | = |
| `natural · right · star` | (2,b,x) (3,c,y) (4,∅,z) | **(2,b,x) (3,c,y) (∅,∅,z)** |
| `two · full · columns` | ['a', 'b', 's', 't'] | = |
| `two · full · l.a,r.b` | (1,∅) (2,2) (∅,3) | **refuse** |
| `two · full · star` | (1,1,a,∅) (2,2,b,x) (3,3,∅,y) | **(1,1,a,∅) (2,2,b,x) (∅,∅,∅,y)** |
| `two · right · columns` | ['a', 'b', 's', 't'] | = |
| `two · right · l.a,r.b` | (2,2) (∅,3) | **refuse** |
| `two · right · star` | (2,2,b,x) (3,3,∅,y) | **(2,2,b,x) (∅,∅,∅,y)** |

## 3. Step 1 — design note

### 3.1 What `main` builds, and where a reference is bound

**DataFrame door.** `frame_names::join_on_named_keys`
(`crates/repark-core/src/session/df_guards/case_bind.rs`) builds

```
Projection [l.id, l.s, r.t]        the right key is filtered out of the list
+- Join <type>, (l.id = r.id)      both keys are physically here
```

and `FrameNode::join` records the lineage. The left key is in the output on every join type
(C-066 R-R6-1). The right key exists only below the projection.

A written reference is not bound in `repark-core`. The facade binds it:

| Reference | Binding site |
|---|---|
| `"l.id"` in select / getitem | `python/repark/src/repark/spark/column_fields.py` (`_refuse_using_key_name`, then `_native.resolve_display_name`) |
| `"l.id"` in sort | `qualified_names._resolve_sort_qualified_name` |
| `col("l.id")` and compounds | `qualified_names._rebind_qualified_refs` → `_native.bind_qualified_free_refs` |
| `selectExpr` / `filter` text | `filter_quote._quote_select_expr_dotted` and `column_fields.py` — a regex over dotted tokens that substitutes engine field names |
| `"l.*"` | `qualified_names._expand_qualified_star` → `_native.qualifier_star_positions` |
| held references (`L["id"]`, stale) | `__REPARK_ATTR_` tokens, `dataframe/unemitted_ids.py`, `_native.prepare_join_condition` |
| join condition | `dataframe/core.py` `_join_on_condition_h1`: both frames become scratch temp views and the join is one built SQL statement over them |

Every one of these resolves a name to **a position in the plan's output schema**, through
`_frame_qualifiers` (output id → qualifier names, filled from `join_output_sources`, which pairs
the merged key with both sides) and the natives in `crates/repark-python/src/dataframe_names.rs`
and `frame_lineage.rs`. Python then builds `PyColumn.column(<engine field>)` itself. So today
`r.id` can only ever mean "output position 0", and R6's refusals (`_using_state`, the five
`_refuse_using_key_*` helpers) sit in the facade at exactly these sites.

**SQL door.** Stock `datafusion-sql` 54.1 plans `USING`:
`Projection: l.id, l.s, r.t` over `Full Join: Using l.id = r.id`. Both keys stay in the join's
schema, so `l.id` and `r.id` already answer Spark's values in select, filter, sort, group-by and
a join condition (§2.3). The star expansion drops the right key and never coalesces, and an
unqualified key resolves to the left one. In `WHERE`, `repark-core`'s `column_resolution`
ambiguity audit does not know a `USING` key is merged and refuses it.

### 3.2 DataFrame door — the design, and why it leaves the two crates

**The merged plan.**

```
Projection [<shown key> AS id, l.s, r.t]
+- Join <type>, (l.id = r.id)
```

| Join type | Shown key | Its attribute id | Hidden per-side keys |
|---|---|---|---|
| `inner`, `left` | `l.id` (as today) | the left key's (as today) | `r.id` |
| `right` | `r.id` | the right key's | `l.id` |
| `full` | `coalesce(l.id, r.id)` | a fresh one, as Spark mints | `l.id`, `r.id` |
| `semi`, `anti` | `l.id` (as today) | the left key's | none; the right side stays `UNRESOLVED_COLUMN` |

The left key stays physical in the `Join` node on every type, which is what C-066 holds. What
changes is that on `right`/`full` it is no longer the shown column, and that is exactly the step
that broke `d66de2e3`: with the binding of §3.1, a key that is not an output position cannot be
referenced at all.

**Hidden fields are not output fields.** A hidden key is a column of the `Join` node, recorded
in the frame's lineage node (`FrameNode::join` gains, per hidden key: its attribute id, its
side, its qualified column below the projection). It is never a field of the plan's output
schema. `columns`, `schema`, `collect`, writes, exports, `distinct`, `union` and every positional
path of the facade read the output schema, so none of them can see it, by construction and
without a filter that could be forgotten. The leak mutation ("append the hidden key to the
merged projection") then turns the `columns` pin red.

**Answering a reference to a hidden key** needs three things:

1. *Resolution reports it.* `attr_id::resolve` and the lineage token resolver answer a new
   outcome, "hidden key `<attr id>`", when the qualifier or the held id names a hidden key of
   the frame. `repark-core`.
2. *Exposure.* `frame_names::expose_hidden_keys(plan, ids)` rewrites the merged projection to
   append `<side>.id AS __repark_using_<attr id>` and carries the column up through the
   pass-through nodes above it (filter, sort, limit, alias-free column projections), returning
   the widened plan and the column to reference. `repark-core`.
3. *The operation runs on the exposed plan and narrows again.* `select` narrows by itself.
   `filter` and `sort` wrap the result in a projection of the original output. A join condition
   registers the exposed plan as the scratch view; the built statement already lists the columns
   it keeps. This is in the operation entry points: `PyDataFrame.select` / `filter` / `sort` in
   `crates/repark-python/src/dataframe/mod.rs`, the four resolvers in `dataframe_names.rs`,
   `prepare_join_condition` in `frame_lineage.rs`, and in the facade the seven binding sites of
   §3.1, where "raise the R6 refusal" becomes "bind the exposed column", including the two text
   binders that substitute engine field names by regex.

Step 3 is the reason for the HALT. Items 1 and 2 are `repark-core`. Item 3 is `repark-python`
and non-trivial facade logic, because the facade, not the engine, owns reference binding on the
DataFrame door. Deleting the refusals alone gives back `main` before R6: `r.id` answers the
shown key silently.

**Propagation, as measured (§2.5).** A hidden key is reachable after `filter` and
`select` of the frame (`select("id", "s").select("r.id")` answers on Spark), and not after
`.alias("x")`. The first cut covers the frame itself and `filter`/`sort` above it; a hidden key
past a narrowing `select` refuses with the R6 message, a declared divergence to pin.

**Routes not taken.**

- *Hidden keys as physical output columns, hidden by the facade.* About forty thousand lines of
  facade assume plan fields are `columns`, position by position (`_stamped_frame_id_snapshot`,
  `_display_names` / `_engine_names`, every `len(displays) != len(engine_names)` guard).
  `distinct`, `union`, `exceptAll`, writes and exports would each need a filter, and one missed
  site is a silent wrong answer or a leaked `__repark_` column in a file.
- *A marker function resolved by an analyzer rule.* The facade emits
  `__repark_using_key('<attr id>')` where it refuses today and a `repark-core` analyzer rule does
  the exposure at plan time, so `repark-python` is untouched. It still needs the facade sites,
  attribute ids do not survive optimization and are stripped from materialized temp views (the
  join-condition path goes through scratch views), and the marker's type is unknown when the frame is built.
  More moving parts for the same facade change.
- *Routing `USING` through the `ON` path.* An `ON` join already keeps both keys with per-side
  qualifiers, but it plans through built SQL over temp views. The perf guard forbids adding a
  SQL re-plan to the names join, which plans natively today.

**Work per call, if Q1 is granted.** The names join adds one `coalesce` expression on `full`
and one lineage entry per key. No door gains a SQL re-plan. An operation that references a
hidden key pays one plan rewrite (exposure) and one extra projection; an operation that does
not pays one lookup in the lineage node.

### 3.3 SQL door — the design (inside `repark-core`, not started)

A statement rewrite on the parsed AST, before planning, in the scope-aware fold that
`repark-core/src/column_resolution/fold.rs` already runs (per-SELECT relation scopes, projection
alias slots, CTE levels). For each `SELECT` whose `FROM` carries a `USING` join:

- the merged key of the join chain is an expression built left to right: the left key on
  `inner`/`left`/`semi`/`anti`, the right key on `right`, `coalesce(<merged so far>, <right>.k)`
  on `full`. A chained `USING` join then matches on the merged key (C-008);
- an unqualified reference to the key in that scope becomes that expression (aliased to the key
  name when it is a bare select item), which also clears the `WHERE` refusal (C-007);
- a bare `*` gets the merged key in place of the left key.

Open points, measured so they are not guessed later: `SELECT * REPLACE (…)` does not parse on
the Spark door, so the star needs the wildcard option set on the AST or an explicit expansion
from the fold's known fields; an alias that shadows the key wins in `ORDER BY`
(`SELECT s AS id … ORDER BY id` sorts by `s` on Spark); `x.id` over a derived table of the join
must read the coalesced key (`main` reads the left key). No path gains a re-plan: the rewrite
runs once on the AST of a statement that is planned once.

## 4. Questions for the ruling

- **Q1 (scope).** The DataFrame door needs `crates/repark-python` (the operation entry points
  and four resolvers) and facade binding logic, beyond `repark-core` and `repark-spark`. Lean:
  grant both, with §3.2's design — hidden keys stay out of the output schema, exposure is a
  `repark-core` plan rewrite, and the facade sites change from "refuse" to "bind the exposed
  column".
- **Q2 (order).** May the SQL door (§3.3) land before the DataFrame door? Today both doors show
  the left key, so they agree; SQL-first makes them disagree on `right`/`full` star until the
  DataFrame door follows. Lean: one branch, one commit per door, merged together.
- **Q3 (C-007).** The `WHERE id` refusal on the SQL door is a `USING` defect on every join type
  and is not in the card. Lean: in, it falls out of §3.3's rewrite.
- **Q4 (propagation).** Is "hidden keys reachable on the frame and through `filter`/`sort`,
  refused past a narrowing `select`" enough for the first cut? Spark answers past the `select`.
  Lean: yes, pinned as a declared divergence.

## 5. Seen and left alone

Each is reproduced without a `USING` join, so fixing it here would change an answer outside the
unit:

- `frame.filter(col("l.id") > 2)` over any join frame raises `type_coercion` / "No field named
  l.id" (an `ON` join too). `filter("l.id > 2")` and `filter(l_frame["id"] > 2)` answer.
- `SELECT l.id, l.id FROM … ON …` raises "Projections require unique expression names".
- `SELECT tl.* FROM tl JOIN tr ON …` over temp views raises "Invalid qualifier tl".
- Expression display names: `(l.id + 1)` for Spark's `(id + 1)` on the DataFrame door,
  `l.id + Int64(1)` and `count(*)` on the SQL door.
