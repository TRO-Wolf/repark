# Owner rulings of 2026-11-20 (morning): #1386, ICE-HELD-FRAME-SNAPSHOT-1 confirmed, ENC-1 conditions, and Fable's guidance on the cache and ICE-HELD step 0s

**Window.** This record adds the owner's morning rulings to the window of about 00:22 UTC on 2026-11-20, whose terms are in [rulings-2026-11-20.md](rulings-2026-11-20.md). Fable (`claude-fable-5-2`) replies are recorded as guidance under the owner's delegation, never as owner rulings: where Fable names a question as the owner's, it is acted on under the delegation and recorded as Fable's guidance. Each section copies the owner's words verbatim, then records what was ruled. Recorded by the records clerk (`claude-haiku-5-5`). The previous record is [rulings-2026-11-20.md](rulings-2026-11-20.md).

## 1. The owner's morning rulings

> Owner rulings, 2026-11-20 morning:
> 1. #1386: the 5x selective-child row is accepted as Spark's contract (release
>    note line). 11 product files accepted. The first-count and view rows are NOT
>    ruled: finish the ten rounds, commit them, attribute both with instruction
>    counts, then ask again. Verifier also checks: the C-006 mutation goes red;
>    C-007's changed meaning is named in the PR and ledger; which frames take
>    _bind_lazy's fallback and whether cells cover them.
> 2. ICE-HELD-FRAME-SNAPSHOT-1: S1, label-blocking, confirmed.
> 3. ENC-1: the two catalog types may join the allowlist on these conditions:
>    they wrap the guarded handle only; a refused commit records nothing; a
>    map-served table behaves as a fresh load under the guard, with cells.
>    1a runs as an ENC-1 unit (10-29 split, two verifiers, no merge grant).
> 4. 1a: C-012 measured before merge; say whether the 8 stored cells are all 1b's.
> 5. Live Glue and S3 Tables run: after 1a merges, before 1b builds.
> 6. Window continues on the same terms. No new build until A1's series and 1a's
>    cost sample are in; add a turnstile to the measure lock.

- **Ruling 1, #1386.** The 5x selective-child row is accepted as Spark's contract, with a release-note line. Eleven product files are accepted. The first-count and view rows are not ruled. The owner's instruction is to finish the ten rounds, commit them, attribute both rows with instruction counts, and then ask again. The verifier also checks three things: the C-006 mutation goes red; C-007's changed meaning is named in the PR and the ledger; and which frames take `_bind_lazy`'s fallback, and whether cells cover them.
- **Ruling 2.** ICE-HELD-FRAME-SNAPSHOT-1 is S1 and label-blocking, confirmed.
- **Ruling 3.** The two catalog types, `SessionTablesCatalog` and `OneTableCatalog`, in `crates/repark-iceberg/src/catalog/session_tables/mod.rs` on branch `fix/ice-held-frame-snapshot-1a` (not yet on main), may join the ENC-1 allowlist on three conditions: they wrap the guarded handle only; a refused commit records nothing; and a map-served table behaves as a fresh load under the guard, with cells. Unit 1a runs as an ENC-1 unit: the 10-29 split, two verifiers, no merge grant.
- **Ruling 4.** Unit 1a of ICE-HELD-FRAME-SNAPSHOT-1 (see §3) has its C-012 measured before it merges. The owner asks whether the 8 stored cells all belong to 1b. This record does not answer that question.
- **Ruling 5.** A live run on Glue and S3 Tables happens after 1a merges and before 1b builds.
- **Ruling 6.** The window continues on the same terms. No new build starts until A1's series and 1a's cost sample are in. A1 is the first unit of TEMP-VIEW-CACHE-LAZY-PARSE-1 (see §2). A turnstile is added to the measure lock, which is a box-local `measure.sh shared|exclusive` outside the repository.

## 2. Fable's guidance on the TEMP-VIEW-CACHE-LAZY-PARSE-1 step 0 (Q1 to Q12)

Fable's guidance under the owner's delegation, not an owner ruling. Source: Fable's reply titled `Fable reply — TEMP-VIEW-CACHE-LAZY-PARSE-1 step 0 2026-11-20`, kept outside the repository. Its answers to the sketch's questions are under its headings Q1 to Q12 (the questions are in [sketch.md](../../temp-view-cache-lazy-parse-1/sketch.md) §6); its build and seat B sections are its §14 and §15.

- **Q1, seat A's memo at the fill: adopted (a), with two additions.** Fable: "Rule (a) is right for the frame's own memo." The additions are: any frame's first own-plan action that reads a lazy table adopts that fill's settings, not only the cached frame's; and after `unpersist` or `clearCache` the table has a third state, a pass-through in which each scan is decided under the acting frame's memo if one is settled, or live otherwise. Five measured shapes join A1's must-hold set.
- **Q2, the SQL door after `CACHE TABLE`: adopted (a).** Re-point the temp view and restore it on `UNCACHE`. Fable names this as CACHE-HONESTY-1 (already filed, F3 in the sketch): A2 closes it, with its six cells in A2's bar. Add cells for `createOrReplaceTempView` and `DROP VIEW` over a cached view, and for both a DataFrame view and a `CREATE TEMP VIEW … AS SELECT` view.
- **Q3: withdrawn; no section in Fable's reply.** The sketch withdraws Q3 (seat B's door coverage moves to the seat B re-sketch with Q9). Fable's short answer covers "Q1 to Q12" as a block and names no Q3.
- **Q4, F7, a held frame joined after a `SET`: adopted (a), no new card.** The 20 `N` cells and the 8 `selfjoin` cells go to TEMP-VIEW-FRAME-CROSSJOIN-1's evidence. Changed bar: A1's required bar is 188 cells, and the 128 cached-join cells are reported as moved or not. If they do not move, they stay with CROSSJOIN-1, and that is not an A1 failure.
- **Q5, F5, the object-identity cache: adopted (a) for the card and registry row,** as a named residual at S3. The label line is the owner's. Fable's lean for the owner: name it in the label text as a kept divergence.
- **Q6, F8, `writeTo().create()` of a cached frame: adopted (a),** folded into A1. Addition: measure the other `writeTo` verbs (`append`, `createOrReplace`, `overwritePartitions`) on a cached frame before choosing where the lineage read sits.
- **Q7, the `mapInPandas` `STRING` refusal: adopted (a),** a new card, not label-blocking, with the pandas 3 caveat for the Spark side. Fable: "Do not let the caveat lower its priority."
- **Q8, the rating: adopted (a).** A1, A2 and seat B all block the label.
- **Q9, seat B's cost under Spark's per-plan rule: adopted (a),** with B2 measured first. The over-1.15x row goes to the owner; Fable does not waive it, and its lean for the owner is to accept.
- **Q10, the ANSI-parse divergence: adopted (a).** READER-TYPED-SCAN-1 takes the 18 cells as evidence. The CSV half closes with the CSV wiring unit; the JSON half stays on READER-TYPED-SCAN-1.
- **Q11, a held Iceberg frame stale after a write: adopted (a),** a new card, ICE-HELD-FRAME-SNAPSHOT-1, S1, label-blocking. This is the item that raised ICE-HELD-FRAME-SNAPSHOT-1: Fable ran 13 held-frame shapes and found 12 silently wrong on main. See §3.
- **Q12, `CACHE [LAZY] TABLE … AS SELECT`: adopted (b),** with no new name: it is already carded as TEMP-VIEW-ALTER-AND-CACHE-AS-1 (S3, owner, 2026-11-17). The 16 cells are added to that card. A2 takes it only if its bar has room.
- **Order (Fable §15): changed.** Within the two-agent cap: (1) now, the A1 build, beside the ICE-HELD-FRAME-SNAPSHOT-1 step 0; (2) then the ICE-HELD-FRAME-SNAPSHOT-1 build, beside seat B's re-sketch; (3) then A2, then seat B's build. Fable: the ICE-HELD build goes ahead of A2, and seat B's re-sketch no longer waits for A2, because ICE-HELD "has the widest reach of anything open here" and seat B "is the long pole". Fable's §14 says A1 may start now; ruling 6 of §1 says no new build starts until A1's series and 1a's cost sample are in.
- **The selective-child cost row: Fable's lean is to accept.** Under the standing ruling of 2026-11-19 ("nothing over 1.15x without the owner"), Fable does not waive it. If the row measures over, the build finishes, the verifier runs, and the PR waits unmerged for the owner on that row. Fable's reason: it is Spark's contract for `cache()`, and the cost of children sharing the cache at all. Fable also asks that the cost rows include the second child of the same cached frame, which reads the source today and the cache under seat A, so the owner sees the pair.
- **Added items for the A1 brief (Fable §14, twelve items):** (1) fill at execution, never in `scan`, so `explain` does not fill; (2) Q1's two additions and the five must-hold shapes; (3) `clearCache` is `unpersist`, not an emptied cache, and `REFRESH` re-arms the table; (4) a deep-lineage cell run in a subprocess; (5) the physical ordering and single-partition flag; (6) budgets, stated and disclosed; (7) a failed or abandoned fill keeps nothing, and two concurrent first uses fill once; (8) no strong cycle, so the session frees after `stop`; (9) Q4's bar split and Q6's `writeTo` verbs; (10) `take(1)` fills the whole cache and `limit(0)` fills nothing; (11) the gates; (12) cost rows.

## 3. Fable's guidance on the ICE-HELD-FRAME-SNAPSHOT-1 step 0 (Q1 to Q11, then §12 to §14)

Fable's guidance under the owner's delegation, not an owner ruling. Source: Fable's reply titled `Fable reply — ICE-HELD-FRAME-SNAPSHOT-1 step 0 2026-11-20`, kept outside the repository. The questions are in the sketch's [§7](../../ice-held-frame-snapshot-1/sketch.md). Fable changes four defaults: Q2, Q6 and Q9, and a check inside Q1 and Q3.

- **Q1, the seat: adopted (a), seat A.** Seat B would pay a catalog call on every held action on the label's catalogs, and makes a held frame fresher than Spark's; seat A does neither. Condition: every load and every commit passes through the decorator, and the wrap goes before `session.rs` line 472, so the fork's schema provider holds the decorated catalog. The brief must also say where the decorator sits relative to `EncryptionGuardCatalog`.
- **Q2, what else the map records: changed, adopted (b) with two changes.** Order by what the session observed, not by `last-updated-ms`: re-bind when the provider's metadata location differs from the entry's, with a session counter as the epoch. Guard the one race: read the epoch before a load, and record only if it has not moved. Consequence to write down: on Glue and S3 Tables a held frame follows the session's last fresh read, so it can show another writer's commit before a refresh, where Spark does not. Fable prefers (b) anyway.
- **Q3, the own-plan rule: adopted (a).** It is measured on all six actions; (b) would return a deleted row where Spark does not (22 cells per build). The table decision is taken in the synchronous rule and carried in the wrapper instance, because the memo is a thread-local and the wrapper's async `scan` may run on another thread. Open point with a cell: whether `checkpoint`'s child shares the table slot, as it shares the memo.
- **Q4, the path read: adopted (b),** a separate card at S2, ICE-HELD-PATH-READ-1, filed with this record. Condition: the label's kept-divergences file must name the path read before the label is declared. Fable: "the 27 cells are silent and 4 of them store old rows".
- **Q5, refs and metadata tables: adopted (a).** Spark refreshes them, and the 16 cells are silent. They are the detachable part of 1a: if the round runs tight, they are the first thing to hand back. The 68 pinned time-travel cells must hold, including `VERSION AS OF '<ref>'` staying pinned while `t.branch_b` moves.
- **Q6, writes of a held frame: changed, adopted (a) with the unit boundary moved.** The command scope moves from 1b into 1a, so 1a is 12 files, not 11. Reason: with the command scope in 1b, 1a checks writes under the query rule, which allows an added column, and about nine cells then write the frame's old columns silently. 1b is the re-plan alone. Fable's re-plan rule (a bare frame's `writeTo` is re-planned on every write) is from Fable's memory of Spark's source, not read from the jar. Before 1b pins anything, it measures on Spark the session changes between hold and write: `USE` another namespace, a temp view created with the table's name, a `spark.sql` frame with args, a text that reads a since-dropped temp view, and ANSI, case sensitivity or session time zone changed.
- **Q7, the rating: adopted (a),** S1, label-blocking.
- **Q8, the order: adopted (a).** No 1a cell involves a cache. Freeze the epoch's signature, a per-`(catalog, identifier)` counter on the session table state, so unit 2 and A1 code against one name.
- **Q9, the re-bound provider: changed, adopted (a),** the fork-free wrapper, with the scoped mode replaced or proven. `refreshed()` does not carry the table, so both of its calls must be served the decided table. Prefer an explicit carrier, a one-table shim catalog, to an ambient mode. Proof: a counting catalog shows zero `load_table` calls on a held action with no write, and on one after a same-session write. Fallback: if the wrapper fails the `alter_add` cells or the zero-call proof, the builder stops and hands back.
- **Q10, `REFRESH TABLE` as an epoch source: adopted (a).** Without it a held frame on Glue or S3 Tables stays old after a refresh, where Spark answers 3, 3. The two local cells stay WRONG on `type=hadoop` under either option; Fable records them as the local catalog's existing divergence.
- **Q11, the `CALL` procedures: adopted (a).** They commit through the decorated handle and need no file. Caution: 1a fixes `expire_snapshots` only for an action that re-binds. Measure `collect`, `INSERT OVERWRITE`, `expire_snapshots`, `collect` on Spark and record it; do not chase it in 1a.
- **§12, the build: unit 1a may start now,** on an Opus 5.5 xhigh builder with an Opus 5.5 medium verifier, beside CSV-STRING-FIELD-TEXT-1's wiring build. The brief carries: cost first (main's baseline under the exclusive lock before product code); one Spark cell by name or field id before the check is written; the other schema changes measured and pinned as Spark answers; the frozen epoch signature; session table state owned by one session; the line ceilings (`session.rs` 1,000, `core.py` 3,461); and the builder gate, the whole facade tree plus `make rust-panic-ban`. Ruling 6 of §1 allows no new build until A1's series and 1a's cost sample are in.
- **§13, the loud cells in the gap: Fable's reading, not a measurement.** 1a may merge before 1b, and they need not merge together. Fable's count of the 113 unit-1b cells (ANSI on): in 53, after a drop, rename, recreate or drop-then-create, Spark writes the current table and RePark raises under either split; 13 write after an added column; 35 raise `COLUMNS_MISMATCH`; 12 raise a target or text error. With the command scope in 1a, every gap cell is loud. Fable recommends that, and 1b as the next slot; whether 1b blocks the label is the owner's. (The section heading says "53 loud cells"; its text says the gap is larger than 53 and not all loud.)
- **§14, the AWS gap: adopted (a).** The label is not declared without a live held-frame run on both `glue` and `s3tables`, which runs after 1a merges because the leg accepts only main. Nothing was contacted; the dispatch is the owner's. Fable gives eight cells, each to run on both catalogs. It numbers them and names none:
  1. hold `spark.table(t)`; `INSERT`; held `count` and `show` answer the new rows;
  2. hold; `DELETE`; `held.writeTo(other).append()` into a table made before the write; read it back: the deleted row is absent;
  3. a DataFrame view of the held frame; `INSERT`; SQL over the view answers the new rows;
  4. `collect`; `INSERT`; `collect` keeps the first rows; `count` answers the new count;
  5. a held `VERSION AS OF <id>` frame; `INSERT`; it stays;
  6. hold; `ALTER TABLE … DROP COLUMN`; held `count` raises `COLUMNS_MISMATCH`; hold, drop and recreate; held `count` raises `TABLE_ID_MISMATCH`;
  7. the external writer (a second session with its own catalog handle), in both orders: held `count`, fresh `count`, held `count`, `REFRESH TABLE t`, held and fresh (under Q2 (b): 2, 3, 3, then 3, 3); and held `count`, `REFRESH TABLE t`, held `count` with no fresh read between (2, then 3), which decides Q10;
  8. a same-session `INSERT` after the external one: held and fresh both answer 4.

## 4. The label-blocking set

[ICE-HELD-FRAME-SNAPSHOT-1](../../../roadmap/mid-term/cards-2026-11-19/ice-held-frame-snapshot-1-card-2026-11-20.md) joins the label's blocking set (S1, confirmed by the owner 2026-11-20).

The last full list is the one in [rulings-2026-11-17.md](rulings-2026-11-17.md) ("The open label-blocking list, later the same evening"). It has not been re-issued since. The items added or closed after it are in [rulings-2026-11-18.md](rulings-2026-11-18.md), [rulings-2026-11-19.md](rulings-2026-11-19.md) and [rulings-2026-11-20.md](rulings-2026-11-20.md). A re-issued list, with each item's status checked against main, is owed.

## Filed with this record

- Two cards in [cards-2026-11-19/](../../../roadmap/mid-term/cards-2026-11-19/map.md), each in its own file: ICE-HELD-FRAME-SNAPSHOT-1 (`ice-held-frame-snapshot-1-card-2026-11-20.md`) and ICE-HELD-PATH-READ-1 (`ice-held-path-read-1-card-2026-11-20.md`), with one row each in the folder's map.
- The row for this file in [map.md](map.md).
- Not edited by this record: the label text, `kept-divergences.md`, the two step 0 work orders, and the cards that Fable's guidance asks to take extra cells (TEMP-VIEW-ALTER-AND-CACHE-AS-1 and CACHE-HONESTY-1).

Up: [map.md](map.md).
