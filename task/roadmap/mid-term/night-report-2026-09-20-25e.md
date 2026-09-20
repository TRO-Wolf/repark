# Run 25e report — the plan packets for the large parity units (2026-09-19 → 20)

**Run:** 25e (unit `night-25e`, lane prefix `qe-`). **Window:** 19:42 → 20:50 EDT.
**Orchestrator:** claude-opus-5. **Beside:** 25a (bumps, perf remainder, RePark halves),
25b (fork lane one), 25c (parity RePark), 25d (fork lane two).

**Mandate:** write **no product code, no fork PR, no cargo build**. Read the two code bases, measure
Spark where a design question needs a fact, and write one self-contained plan packet per large
parity unit so that a single headless Opus session at maximum effort can execute it end to end
without rediscovering anything.

**The owner's question behind the run, verbatim:** *"If we planned a few well documented and
organized out plans for running Opus Max sessions, would that speed up development time?"*

---

## 1. Deliverable

**Thirteen packets + a shared rules file + an index**, in `/tmp/oc-worker/plan-packets/`:

The brief named ten units; the run had time for three more. **11 and 12 were written at 25c's
explicit request** (`claims.txt` 19:49: *"Packets for any of those are welcome — 25c will use them
as the unit brief"*); **13** is the best cells-per-effort unit left on the slate.

| File | Lines | Slate rows | Cells | Split |
|---|---|---|---|---|
| `COMMON.md` | 204 | — | — | the rules every packet inherits |
| `INDEX.md` | ~150 | — | — | sizes, splits, **all 22 owner decisions in one table**, a suggested wave order |
| `ipi-41-orc-avro.md` | 545 | IPI-41 | 13 | fork 75 % / RePark 25 %, bump |
| `ipi-40-views.md` | 564 | IPI-40 | 13 (+2) | RePark ~100 % |
| `ipi-22-incremental-changelog.md` | 559 | IPI-22 | 11 | fork 40 % / RePark 60 %, bump |
| `ipi-19-56-37-schema-evolution-write.md` | 532 | IPI-19, -56, -37 | 12 | **RePark only** |
| `ipi-05-wap.md` | 490 | IPI-05 | 6 | RePark ~85 % + one small fork ask |
| `ipi-26-27-parser-ddl.md` | 413 | IPI-26, -27 | 34 | **RePark only** |
| `ipi-20-23-metadata-cols-time-travel.md` | 442 | IPI-20, -23 | 26 | fork 40 % / RePark 60 %, bump |
| `ipi-30-31-procedures.md` | 373 | IPI-30, -31 | 25 | fork 45 % / RePark 55 %, bump |
| `ipi-51-error-conditions.md` | 353 | IPI-51 | 45 of 118 | **RePark only** |
| `ipi-43-rdf-sort-zorder.md` | 280 | IPI-43 | 3 | **RePark only** (fork half = 25d's #323) |

**222 inventory cells**, every one currently non-EQUAL and every one a cell Spark answers.

Each packet carries: the goal as the exact cells (statement, **Spark's recorded answer**, RePark's
message today); the facts measured for it with the probe key beside each; **the design DECIDED**,
every fork ruled with the measurement or Java fact behind it; the owner's decisions listed
separately; the file map in both repositories with **line counts and ceiling status**; ordered steps
with the pin bump in place; a red-first test list cell by cell **plus the pins no cell requires**;
a mutation list naming the pin each item must turn red; the exact local gate; the done condition as
harness cells; and the unit-specific traps.

## 2. Method

1. Read the two trees read-only: `/tmp/qe-fork` (fork `main` `457fd2c`) and `/tmp/qe-repark`
   (RePark `origin/main` `ff5a13c3`, a `git archive` extract — the mirror's worktree is stale at
   `3bd667e6`).
2. Built a **cell dumper** (`/tmp/oc-worker/qe/dump.py`): any cell id → its statement source, both
   engines' recorded answers, verdict and registry hint. Every packet's §1 is generated from it.
3. Built a **javap helper** (`/tmp/oc-worker/qe/jp.sh`) over the 1.11.0 Spark runtime jar; extracted
   listings into `/tmp/oc-worker/qe/jp/`.
4. **Measured Spark three times under the JVM lock** where the cells did not answer a design question:
   - `probe/p1.json` — ORC/Avro delete-file formats on v2 and v3, the `write.delete.format.default`
     override, all-type round trips, file extensions, and the **HadoopCatalog** view surface;
   - `probe/p2.json` — the **InMemoryCatalog** view surface end to end, and the whole
     schema-evolution matrix (`mergeSchema` with and without `accept-any-schema`, missing columns,
     type widening, `MERGE WITH SCHEMA EVOLUTION`, and the four (property × conf) combinations of
     `spark.sql.iceberg.merge-schema` on `INSERT … BY NAME`);
   - `probe/p3.json` — the ref `IF [NOT] EXISTS` grammar, `REPLACE TABLE` on a missing table, and
     the whole `USE` / `SHOW CATALOGS` / `SHOW COLUMNS` / `CACHE` / `table-default`-vs-`override`
     surface. **This one answered two rulings my own packet had flagged as unmeasured, and reversed
     one of them** (§5.11).
5. Derived `/tmp/oc-worker/qe/both_refuse.json` — all 118 both-refuse cells with both engines'
   exception class and condition — for IPI-51.
6. Eleven read-only code surveys (Sonnet tier, no edits, no builds) for the file maps.
7. **A Grok 4.6 critic per packet**, one question: *"what will the executor have to stop and ask?"*
   Findings folded in; every design-changing claim **re-verified by hand** before it was written
   down (run-24 rule: a reviewer's claim about Spark or Iceberg is measured before it becomes a
   ruling).

## 3. The answer to the owner's question

**Yes — and the measurable reason is that planning found things the executing session would have
hit as a wall mid-round.**

The critic on the very first packet refuted its central design ruling. The draft said ORC write
needs no `Cargo.toml` edit because `orc-rust` 0.8 is already a dependency with an `ArrowWriter`.
**Verified by hand in the crate source, that writer cannot produce an Iceberg ORC file at all:**

- `attributes: vec![]` on every serialized type (`arrow_writer.rs:175` and each arm) — **no
  `iceberg.id`**, and the fork's own ORC reader *requires* it and errors loudly
  (`arrow/orc_reader.rs:77-78`; `reader.rs:8815-8817` says so in as many words);
- `_ => unimplemented!("unsupported datatype")` (`:217`, and `writer/stripe.rs:187`) — **DATE,
  TIMESTAMP, DECIMAL, ARRAY, MAP and STRUCT all panic**;
- `statistics: vec![]` (`:239`) and `compression: None` (`:251`);
- 0.9.0 adds Date/Timestamp but **still** `attributes: vec![]`.

An Opus Max session would have written a writer, produced files its own reader rejects, and stopped
to ask the owner a question mid-round. The packet now carries the footer-splice design (mirroring
what the fork already does on the **read** side) and the one blocking owner decision, up front.

All ten critics ran; **nine of the ten packets had at least one design ruling overturned by a
measurement, and six had a *central* one** (§5). Total critic cost: **$9.30**.

**The second measurable finding: four of the ten units need little or no fork work, and the slate
says otherwise.** Those are the cheapest units on the whole slate, and nothing in the campaign's
documents would have told an executor:

| Unit | The slate's "likely home" | What is actually on fork `main` |
|---|---|---|
| **IPI-05 WAP** | *"RePark session conf **+ fork (staged commit)**"* | `SnapshotProducer::with_stage_only`, public `FastAppendAction::stage_only()`, the entire `CherryPickAction` including `validate_wap_publish` and `is_wap_id_published`, **and five round-trip tests**. Only a one-line provider ask is missing (§5.2) |
| **IPI-19 schema evolution** | *"… **fork UpdateSchema**"* | `UpdateSchemaAction::union_by_name_with(Schema)` — a Java-parity port with promotion and required→optional relaxation — and one `Transaction` already carries a schema update **and** an append in a single commit |
| **IPI-43 sort / zorder** | *"held by 23a"* | 25d's **#323** has the whole fork half. The gap is a **string parser** in RePark — the fork API takes `SortOrder` / `ZOrderSpec` values, and nothing anywhere parses `'zorder(id, data)'` |
| **IPI-40 views** | *"RePark **+ fork (view catalog)**"* | the fork's view model is complete, **and RePark's `catalog/provider.rs:550-637` already forwards all seven `Catalog` view methods.** Only the read path and the SQL door are missing |

**Scheduling consequence:** those four can run with no fork-queue and (bar IPI-05's one ask) no
pin-bump contention — the scarcest resource in the campaign, since only one bump may be open across
all runs at a time.

## 4. Cost

| | |
|---|---|
| Grok 4.6 critics | **10 runs, 306 turns, $9.30 total, $0.93 average** (`/tmp/grok-worker/runs.tsv`, lanes `qe-rv-*`) |
| Read-only code surveys | 10, Sonnet tier, no builds |
| Spark | 2 probe sessions, both under `jvm-lock.sh`, both short |
| cargo / builds | **none** — the run's mandate |
| AWS | **none** |
| Clerk rounds | none needed (no code, no gates) |

## 5. What the critics overturned — the five design reversals

Each was re-verified by hand before it entered a packet.

1. **IPI-41 — the ORC writer.** Above. Owner decision now blocking the fork half.
2. **IPI-05 — the unit is fork-touching after all.** The cells' `INSERT … VALUES` path goes
   `router.rs:279-289` → the fork's `IcebergCommitExec`, which calls
   `tx.fast_append().set_snapshot_properties({operation-id})` with **no** `stage_only()`. The
   packet's RePark-only staged-write design reached a commit path the cells never take. Fixed: a
   small fork ask (`with_stage_only` on the provider, the exact analogue of `with_commit_branch`)
   and a bump.
3. **IPI-05 — Java REFUSES both WAP confs set.** The packet ruled "branch wins". Verified in the
   jar: `SparkTableUtil` carries
   `Cannot set both WAP ID and branch, but got ID [%s] and branch [%s]`. Shipping branch-wins would
   have been a new silent divergence in a unit whose whole point is removing silent divergences.
4. **IPI-22 — the changelog delete-file refusal is Spark's too.** The packet ruled "always pass
   `with_row_level_deletes(true)` because Spark answers merge-on-read tables". Verified:
   `BaseIncrementalChangelogScan` in the jar contains
   `Delete files are currently not supported in changelog scans` — the **same string** as the fork.
   The fork already matches Spark; the correct action is to **pin the refusal**, not build around it.
5. **IPI-19 — `ICE-WRITE-OPTIONS-1` is FIXED on the opposite contract.** The packet's trap T-5 told
   the executor to make unknown write options refuse "as Spark does". Verified:
   `write_options.rs:215-220` is `fn unknown_keys_ignored_like_spark()`, and the registry records
   *"unknown option keys are ignored"* measured 2026-09-17. T-5 struck.

6. **IPI-30/31 — `ancestors_of` emits NEWEST-FIRST.** The packet said oldest-first because the
   inventory recorded `S0, S1, S2`. That ordering is a **harness artefact** —
   `harness.py:265-277` does `sorted(out, key=repr)` after aliasing the ids. Java
   (`SnapshotUtil.ancestorIdsBetween`) emits head-first, and the fork's own helper is already
   documented as *"head first and inclusive"*. **The inventory replay sorts both engines, so it
   cannot catch an order bug — only a pytest can.** A packet that told the executor to reverse the
   fork helper would have shipped a silent wrong answer that the gate would have passed.
7. **IPI-20/23 — `_partition` cannot be a constant, and `_deleted` cannot be written where the
   ceiling allows.** `constant_fields` is `HashMap<i32, Datum>` and `Datum` is primitive-only, so a
   struct cannot ride it; `partition_field()` builds a **REQUIRED** field while the measured answer
   on an unpartitioned table is **NULL**. And the delete verdict lives in `reader.rs`, which is at
   an exact 10139 baseline — the packet had told the executor not to grow it and to put the code
   somewhere that cannot see the mask. Both now have rulings, including an extract-and-ratchet step
   before the first feature commit.
8. **IPI-51 — the parser as specified would not have fired at all.** `error_map.rs:99-101` preserves
   DataFusion's `Error during planning: ` prefix into Python, while the packet required the
   `[CONDITION]` bracket at column 0. And the packet's headline claim ("twelve of the 45 already
   carry the answer") was wrong: re-derived by hand, it is **two** at column 0, four after a prefix,
   and **one** carrying a SQLSTATE at all.
9. **IPI-26/27 — `CLUSTERED BY` is not parsed on this door at all.** The packet was right that
   nothing reads `CreateTable.clustered_by`, but `parse_optional_clustered_by` only runs for
   `HiveDialect | GenericDialect` and RePark's CREATE uses `DatabricksDialect` — so the field is
   never populated and "read it" is not the fix. It is a token rewrite.
10. **IPI-43 — the load-bearing order pins would have been vacuous.** The packet's own §6 was built
    to prevent a no-op implementation from passing, and its observation method (`SELECT id` with no
    `ORDER BY`) could not observe file order, because `harness.py` sorts. The fix reuses a pattern
    already shipped in this tree (`test_ice_sorted_insert_1.py` reads the parquet files directly).

11. **IPI-21/25/42 — my own measurement reversed my own ruling.** The packet said
    `CREATE OR REPLACE BRANCH IF NOT EXISTS` needed measuring before a message was authored.
    Measured: **Spark's parser rejects it** —
    `mismatched input 'NOT' expecting {<EOF>, 'AS', 'RETAIN', 'WITH'}` — so `IF NOT EXISTS` attaches
    **only** to the plain `CREATE BRANCH|TAG` form. The same probe also showed the four recorded
    `IF [NOT] EXISTS` cells all happen to exercise the **no-op** branch, so the packet's four cell
    pins alone would have passed an implementation that *always* no-ops. Three more pins were added.
12. **IPI-32 — the survey found TWO existing "current catalog" notions that disagree.** DataFusion's
    `datafusion.catalog.default_catalog` (which actually resolves SQL names, and is already
    runtime-settable — `crates/repark-sql/src/v3/cow.rs:540-545` does it) and a Python-only
    `_catalog_state()` behind `spark.catalog.setCurrentCatalog`, which **does not move the first**.
    The packet now rules that `USE` must update both and pins that they agree — closing a latent
    wrong-answer path that exists on main today. The same survey found that `CACHE TABLE` and
    `REFRESH TABLE` **already work through the Python method API** (`catalog.py:699-834`), so the
    packet's "implement them as a no-op" ruling was withdrawn in favour of routing the SQL forms to
    the existing implementation.

Plus a long tail of corrections folded in as addenda: a wrong cell verdict (`V-ALTER-UNSET` is
NOT-PARSED, not REFUSED-REGISTERED), a wrong `format-version` rendering (`Display` gives `v1`;
Spark reports `1`), a missing `Created By` row in DESCRIBE EXTENDED, a `SHOW TBLPROPERTIES`
single-key form the packet's grammar omitted, three RePark refusal sites that turned out to be in
**Python** rather than Rust, and four Python pins encoding refusals the units remove.

## 6. Gate items found while planning — cells nobody owns and nobody has filed

Per the owner's ruling 3 of 2026-09-19, these go in a numbered list rather than as footnotes.
**None is a residue of work done tonight; each is a hole the planning surfaced.**

1. **`CLUSTERED BY (col) INTO n BUCKETS` is SILENTLY DROPPED.** sqlparser parses
   `CreateTable.clustered_by` and **neither `create_table.rs` nor `ctas.rs` ever reads it** (grep:
   zero hits). Spark maps it to a real bucket spec (`D-X-CLUSTERED-BY`:
   `[["id_bucket","bucket[4]","id"]]`). Cell `D-X-CLUSTERED-BY` currently fails earlier on clause
   ordering, so the matrix does not show it — but the plain form would silently lose the spec.
2. **`ADD COLUMNS (s STRUCT<a:INT, b:STRING>)` is mangled by a rewrite bug.**
   `split_top_level_comma_segments` (`alter.rs:633-662`) tracks paren depth but **not angle-bracket
   depth**, so the comma inside a nested type splits the column list. Cells `D-ADD-COL-STRUCT`,
   `D-X-ADD-COL-MAP-KEY-STRUCT`.
3. **`_spec_id`, `_partition` and `_deleted` plan successfully and fail at READ time.** The fork's
   `TableScanBuilder::select` accepts any metadata column name (`scan/mod.rs:459-460`) but the
   transformer has no synthesis path for those three (`record_batch_transformer.rs:468-474`).
4. **`describe_show.rs:355-363` advertises all five metadata columns** in
   `DESCRIBE TABLE EXTENDED` while every query using one fails. RePark is telling users a feature
   exists that does not.
5. **`_LEGACY_ERROR_TEMP_2330 / _0034 / _3060 / _1008`** — four cells where Spark's "condition" is
   an internal placeholder Spark itself renames between releases. Copying them would pin RePark to a
   Spark internal (IPI-51 owner decision 16).
6. **The 20 `Py4JJavaError` both-refuse cells** — Spark surfaces a raw JVM exception; RePark has no
   JVM. The inventory's §5 already rules them out of class-for-class parity, but they remain
   non-EQUAL on the matrix and someone has to decide whether the gate tolerates that.
7. **`REST-4` in `task/roadmap/mid-term/rest-catalogs-intake-2026-09-12.md` still says the memory
   catalog refuses views.** The fork's memory catalog implements all seven view methods
   (`catalog/memory/catalog.rs:620-701`). A stale claim in a document agents are expected to trust.
8. **`V-DESCRIBE-EXTENDED` is mis-titled** in the inventory: `cells_misc.py` runs
   `SELECT count(*)`, not `DESCRIBE EXTENDED`. The cell name will mislead whoever closes IPI-40.
9. **`R-DF-INCREMENTAL-OVERWRITE-ERR` is mis-titled**: Spark does **not** raise; it returns
   `[[3,"c","x"]]`. An executor trusting the title would implement a refusal Spark does not make.
10. **`crates/integrations/datafusion/src/physical_plan/delete.rs:61`** imports
    `ParquetWriterBuilder` and never uses it (the writers live in `delete_position_deletes.rs` and
    `row_lineage.rs`). Harmless, but it is what makes a naive grep for the ORC write sites land in
    the wrong file.

## 6b. A scope miss of my own

Run 25c posted its ownership list in `claims.txt` at **19:49** — five minutes after I read the file
at 19:44, and I did not re-read it before writing. Run rule 12 says to read `claims.txt` at the top
of every loop; I read it at the top of the run and then at 20:32. Two packets were affected and both
now carry a scope banner quoting 25c verbatim:

- **IPI-22** — 25c has the brief, 37 fresh Spark cells and a lane running tonight. Packet 3 is a
  reference unless that lane died; its addendum still matters, because it corrects two design
  rulings against Iceberg 1.11 bytecode.
- **IPI-30** — 25c owns four of the five procedures. Packet 7 is `add_files` + IPI-31 + the fork
  halves, and its "13 of 25 cells with no fork PR" checkpoint is void and must be recounted.

25c also **asked** for packets on IPI-19, IPI-56, IPI-32, IPI-21, IPI-25, REF-2, IPI-23/MT-1 and
IPI-20, to use as unit briefs. Packets 4 and 10 cover four of those.

## 7. Owner questions

**One is blocking.** All 22 are in `INDEX.md` with a recommendation each; the three that most want
an answer before v1.5.0:

1. **BLOCKING — IPI-41's ORC type coverage.** (a) in-tree encoders for date / timestamp / decimal /
   nested, (b) **primitives only + card the rest**, or (c) an owner-gated `Cargo.toml` patch.
   *Recommendation: (b)* — every IPI-41 cell uses `BIGINT` and `STRING`, so (b) closes the whole
   gate; refuse other column types loudly by type name and say in the PR that ORC write covers the
   primitive set only.
2. **IPI-40 — do Glue and S3 Tables need to store views?** No cell measures it and Spark's own Glue
   catalog cannot. *Recommendation: not in this unit.* **But the owner's stated reason for the
   campaign is production pipelines at work, which will use Glue** — so this is the one question
   worth answering before the tag rather than after it.
3. **IPI-19/56 — once Spark's `{short}.id = s.id` MERGE qualifier works, does RePark keep answering
   `target.` / `source.`?** Spark raises on those; `EX-DF-9` records them as working and a shipped
   facade test requires them. *Recommendation: keep both, narrow `EX-DF-9`.* Going Spark-exact
   breaks shipped tests and anyone who copied the RePark docs.
4. **IPI-20/23 — `R-MT-POSITION-DELETES-TT` cannot reach EQUAL in that unit.** The fork's
   `PositionDeletesTable::scan` is `FeatureUnsupported`; the cell belongs to IPI-45, its row
   `V3-COV-6` is DECLARED, and the fork ask `F-POSDEL-SCAN-1` is 25b's. **Does the gate demand
   26/26 in that PR?** *Recommendation: 25/26 plus a named gate item* — demanding 26/26 blocks the
   unit on another run's fork port.

**Three of the 22 were CLOSED by their own critics**, without an owner turn: the fork's
`with_commit_branch` does create a missing branch (so IPI-05 needs no fork change for that);
`REF-6` is already FIXED; and #323's own ledger already rules IPI-43's tuning options.

## 8. STATUS.md

No edit. No STATUS clause names these units.

## 9. Rust-first roll-call

No code was written. Every packet's design is Rust-first by construction: the one place a packet
puts a decision in Python is IPI-19's DataFrame `mergeSchema` projection, and only because the
measured failure is raised there (`writer_readwriter.py:769-774`) before any SQL is issued — the
packet says so and confines it.

## 10. Machine and disk

- **No cargo build, no maturin, no gate run** — the run's mandate.
- Two Spark sessions, both under `/tmp/oc-worker/_lib/jvm-lock.sh`, both short.
- Read-only trees: `/tmp/qe-fork` (39 M, a `--filter=blob:none` clone), `/tmp/qe-repark` (241 M, a
  `git archive` extract), `/tmp/qe-critic` (a small git repo holding the packets, which the critics
  read). Probe warehouses under `/tmp/oc-worker/qe/probe/wh1`, `wh2`. **All four are removable.**
- Every Grok critic ran `--sandbox read-only`, so none could write to any tree.

## 11. Durable artefacts

| What | Where |
|---|---|
| The packets | `/tmp/oc-worker/plan-packets/` (`COMMON.md`, `INDEX.md`, ten `ipi-*.md`) |
| The cell dumps, one per packet | `/tmp/oc-worker/qe/cells/*.txt` |
| The cell dumper | `/tmp/oc-worker/qe/dump.py` |
| The javap helper and its listings | `/tmp/oc-worker/qe/jp.sh`, `/tmp/oc-worker/qe/jp/*.txt` |
| The Spark measurements | `/tmp/oc-worker/qe/probe/p1.{py,json}`, `p2.{py,json}` |
| The 118 both-refuse cells with both engines' classes and conditions | `/tmp/oc-worker/qe/both_refuse.json` |
| The critic briefs and logs | `/tmp/oc-worker/qe/critics/`, `/tmp/grok-worker/qe-rv-*/` |
| State | `/tmp/oc-worker/run25/state-25e.md` |

## 12. For the orchestrating session

1. **File the packets.** They are **already durable** — `/tmp/oc-worker` is a symlink to
   `~/repark-lanes/campaign/oc-worker`, so they survive a reboot. But they still belong under
   `task/roadmap/mid-term/` or `docs/design/` on `origin/main`, so the next run reads them from git
   rather than from a path it has to be told about. The brief says no docs PR tonight, so this is
   the orchestrating session's.
2. **Answer owner decision 1 (ORC type coverage) before anyone opens IPI-41's fork lane.**
3. **Schedule wave A first** (`INDEX.md`): IPI-19/56/37, IPI-43, IPI-51 and IPI-05's RePark half —
   four units, 66 cells, no fork-queue contention.
4. **Tell 25c and 25d** the four "no fork work needed" findings (§3) — two of them are on their
   lists.
5. **The ten gate items in §6** are not any unit's residue; they need owners.
