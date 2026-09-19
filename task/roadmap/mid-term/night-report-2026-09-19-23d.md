# Run 23d report — 2026-09-18 → 19 night run: the silent top of the parity slate (RePark side)

**Run:** 23d (unit `night-23d`, lanes `/tmp/nd-build` and `/tmp/nd-git`). **Window:** 23:00 → 05:30 EDT. **Orchestrator:** claude-opus-5.
**Actor:** Muse Spark 1.3 contributor on one unit, in a single session (`01a0b7b6-84b3-7c02-82ef-faf16d7190ef`)
with five launches: r1, the r1 resume after the 400-step cap, r2, r2-close + r3, and the r3 resume after a
provider 503. **Reviewer:** Grok 4.6 (one logic critic). **Beside:** 23a (fork lane) and 23b (RePark; owns the
bumps). **Box:** one build clone (`/tmp/nd-build`) and one git-only clone (`/tmp/nd-git`). Every gate went through
`build-slot.sh` with `CARGO_BUILD_JOBS=6`. I never had more than two cargo-running things at once.

## 1. Slate rows — before / after

| Row | Before (measured on main) | After | Where |
|---|---|---|---|
| IPI-01: reader `versionAsOf` / `timestampAsOf` are ignored, so RePark reads the current snapshot | 78 of 94 re-recorded cells differ from Spark 4.1.2 + Iceberg 1.11.0 (IPI-01 and IPI-02 together, v2 and v3) | **DRAFT, not merged.** At r1 (04ecea52) the local gate was green on all 94 cells. The logic critic then found 2 P1 and 6 P2. R3 is in progress: item 1 of 8 is committed (f3259cbf) | `fix/ice-tt-resolve-1`, draft PR (see §2) |
| IPI-02: `TIMESTAMP AS OF <expr>` reads the current snapshot, and `<int>` is read as ms where Spark reads seconds | included above | same branch | same |
| IPI-04: `DROP NAMESPACE` on a non-empty namespace | 20 of 26 re-recorded cells differ. 14 are silent orphaning drops (7 statement shapes × InMemory and Hadoop, CASCADE included); 4 are nested-namespace cells that already fail loud; 2 are the missing-namespace message | **not started.** The one build clone stayed on unit 1. Claimed at 01:2x and released at 04:30 | truth `/tmp/oc-worker/nd-ns/spark-ns.json`, Muse brief `nd-ns/brief-1.md` |
| IPI-03: static-mode `INSERT OVERWRITE … PARTITION (col)` and writer `overwrite-mode=dynamic` | 24 of 60 re-recorded cells differ. DML-1's "declared residue" is in fact silent at runtime. See §3 | **not started, never claimed** | truth `nd-ow/spark-ow.json`, Opus-high brief `nd-ow/brief-1.md` |
| IPI-06 / IPI-07 / IPI-05 | not measured by 23d | not started | 23c briefs |

## 2. Pull requests

| PR | Unit | Actor, rounds | Reviews (Grok 4.6) | Comment gate | State |
|---|---|---|---|---|---|
| draft (opened at the end; the number is in the state file) | ICE-TT-RESOLVE-1 (IPI-01 + IPI-02) | Muse: r1 (799 steps over two launches), r2 (212), r2-close + r3 (144 before a 503; resumed) | logic **NEEDS_REMEDIATION**: P1 `random()`/alias non-determinism missed; P1 date-only `…Z` read as a session wall clock; P2 aliases swallowed, `tag_` selector message, native-door mutation proof, season-fragile NY pins, legacy error texts. $1.53, 39 turns. No verification critic yet | 0 at every accepted head (3413c537 … f3259cbf) | **DRAFT.** Branch head f3259cbf (or later; see the state file). R3 items 2–8 remain |

Muse r1 hit the 400-step cap with the whole Rust fix uncommitted, even though the brief said "commit after every
step". The resume committed it. R2 raised four size ceilings and added a `check_lib_rs` exception; I sent that back.
R2-close ratcheted all of them below main's values: `session.rs` 1127 → 1123, `session_core.py` 2290 → 2287.
One launch died on a Muse backend 503 after 10 retries.

Local gate on 04ecea52 (r1): `CB=0 R=0 T=0 U=0 L=0`.
- 40 Rust time-travel tests passed (repark-core 2, repark-spark 17, repark-sql 21).
- 394 passed and 5 skipped offline, over 17 test files.
- 399 passed live.

It has not been re-run since r2. The next orchestrator re-runs it on the final head.

## 3. What the measurements showed

All Spark legs ran on PySpark 4.1.2 + Iceberg 1.11.0 (InMemoryCatalog, plus HadoopCatalog for namespaces), re-recorded
tonight from new cell files that go past 23c's briefs. The files are under `/tmp/oc-worker/nd-meas/` and copied to
each unit's directory.

- **Time travel (94 + 21 cells).**
  - Integers and decimals in `TIMESTAMP AS OF` are epoch **seconds**. RePark main read them as ms.
  - Strings, on the reader and in SQL, are cast in the **session zone**. RePark main parsed naive strings as UTC, so
    a New York session errored or picked the wrong snapshot.
  - Spark's cast rules are wider than a hand parser:
    - Accepted: `'2020'`, `'2020-6-1 1:2:3'`, `+0000`, and `HH:mm` with no seconds.
    - Refused (INPUT): `'2020-06-01Z'` and `'…T00:00Z'`.
    - DST: a New York gap wall is shifted forward; an overlap takes the earlier offset.
  - Refusals:
    - `rand()`, `random()` and `uuid()` refuse NON_DETERMINISTIC.
    - A `WITH … rand()` subquery is an INTERNAL_ERROR in Spark.
  - Answered: a constant scalar subquery, `current_timestamp()`, and a table alias after the clause.
  - Selector interplay:
    - `versionAsOf` on `t.tag_x` → "Can't time travel using selector and Spark time travel spec at the same time".
    - On `t.branch_x`, or combined with the `branch` option → "Can't time travel in branch".
  - Spark 4.1 refuses the legacy options `snapshot-id` / `as-of-timestamp` / `tag` when they are used alone. That is
    IPI-18, kept out of this unit.
- **Namespaces (26 cells).**
  - Iceberg refuses to drop a non-empty namespace for every statement shape, CASCADE and IF EXISTS included
    (NamespaceNotEmptyException through Py4J; the InMemory message names the table count).
  - Empty namespaces drop.
  - RePark main drops a non-empty namespace, and its tables become unreadable. That is silent data loss.
- **Overwrite (60 cells).**
  - In static mode (the default), `PARTITION (cat)` with no value replaces the **whole table**. RePark takes the
    dynamic path.
  - Empty static wipes the table. RePark refuses.
  - Mixed `PARTITION (cat='x', sub)`: in static mode it replaces all of cat=x; in dynamic mode only (x, p). RePark
    refuses both.
  - `PARTITION (cat)` on an unpartitioned table raises `[NON_PARTITION_COLUMN]`. RePark silently overwrites.
  - Writer `overwrite-mode=dynamic` on `insertInto` keeps untouched partitions. RePark wipes them.
  - `saveAsTable(overwrite)` data is equal, but Spark's history is a table REPLACE. That is RTAS, not IPI-03.

## 4. Rulings (G-2)

- **Q-23d-1.** When the legacy reader options `snapshot-id` / `as-of-timestamp` / `branch` / `tag` are used alone,
  they keep their resolution. Their not-found errors now carry Spark's IllegalArgumentException texts through the
  shared resolver. Accepted. IPI-18 owns their refusal.
- **Q-23d-2.** `VERSION AS OF <id> + 0` stays a ParseException, which is Spark's class. Accepted as P3.
- **Q-23d-3.** Muse r2 Q3: `#[allow(clippy::too_many_arguments)]` on the 9-argument `read_iceberg_table` PyO3 method.
  This is the repo idiom (dataframe_fill.rs, cdf_infer.rs, ml.rs). Accepted.
- **Q-23d-4.** Every string becomes a timestamp through the engine's own Spark `CAST(<string> AS TIMESTAMP)`; the
  hand parser is deleted (r3 item 2). A cast that disagrees with a measured cell is a cast finding, pinned as a
  strict xfail, never special-cased in time travel.
- **Q-23d-5.** Non-determinism is decided from the planned expression's volatility, not from a name list (r3 item 1,
  committed f3259cbf).

## 5. Owner questions (with recommendations)

- None blocking. **IPI-18** (Spark 4.1 refuses the legacy `snapshot-id` / `as-of-timestamp` / `tag` reader options,
  which RePark accepts) is a compatibility call. Recommendation: keep accepting them and file a DECLARED row. Refusing
  them would break existing RePark users for no correctness gain, since both engines resolve the same snapshot.

## 6. Next run, in order

1. **ICE-TT-RESOLVE-1.**
   - Resume the Muse session (`--resume 01a0b7b6-84b3-7c02-82ef-faf16d7190ef`, prompt `followup-3b.md`), or start a
     fresh session on `followup-3.md`: that session is past five launches.
   - Finish r3 items 2–8.
   - Local gate: `local-gate.sh` on the build lane with crates "repark-core:time_travel,repark-spark:time_travel,repark-sql:time_travel"
     -n 4 <the 17 files>`.
   - Grok verification critic, mutation first.
   - Take the draft PR out of draft, then queue it.
2. **ICE-DROP-NS-1** (IPI-04), Muse, brief ready. The smallest silent data-loss fix left on the slate.
3. **ICE-OVERWRITE-MODE-1** (IPI-03), Opus high, brief ready. The brief carries the headless foreground rule (23b's
   Opus actor exited mid-build).
4. IPI-06/07, then IPI-05 (WAP), per 23c's order.

## 7. STATUS.md

No edit. No unit merged.

## 8. Rust-first roll-call

ICE-TT-RESOLVE-1 is Rust:
- one resolver in `repark-core::time_travel` (`resolve_reader_spec`, `evaluate_sql_timestamp_asof`), used by
  `repark-spark` and `repark-sql`;
- PyO3 takes the raw option strings;
- `reader.py` only forwards `versionAsOf` / `timestampAsOf` (and still validates the legacy ints, as on main).

## 9. Scripts changed

- a review-launcher copy of `nb-review.sh` for this run's review lanes, under `/tmp/oc-worker/_lib/`.

## 10. Disk

- `/tmp/nd-build` is 46 G. Keep it: it is the TT unit's lane and holds a built release native.
- `/tmp/nd-git` is 0.3 G.
- `/tmp/oc-worker/nd-meas` is 0.3 G. It includes a 270 M snapshot of the main-built package, used to measure main
  while the lane was edited. Delete `nd-meas/mainpkg` once the unit merges.
- The Grok critic clone `/tmp/nd-rv-ttlogic` was removed at 02:58.
- `/` has 1.1 T free.
