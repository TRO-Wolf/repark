# Overnight report — run 16b of 2026-09-15 (DAY run: the facade surfaces and IO of the 1.5 Spark-parity campaign)

**Session:** one Opus orchestrator (overnight-16b), 2026-09-15 07:59 → 20:15 EDT · **Orchestrator:** Claude (claude-opus-5) · **Lanes:** `qb-` ·
**Builders:** Muse Spark 1.3 contributor (owner ruling for the day) · **Reviewers:** Grok 4.6 (critic-logic and S2-21 Rust perf, read-only
clones) · **Mechanical:** GLM 5.3 Flash (one pins round) · **Beside:** run 16a (functions), run 16c (SQL door) · **Grants:** G-1 yes, G-2
yes, G-3 stop opening lanes 19:30 / done 20:15, G-4 Muse builders + Grok reviewers + GLM mechanical, G-5 yes. **Procedure:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md). STATUS.md, tags and the release pipeline
untouched; no file owned by run 16a or 16c edited outside the seams named below.

## 1. Census slice — before / after

`census.py` over run 15's PySpark 4.1.2 surface dump. **Before:** main 11ae1595 + #605/#607/#608 at 08:14 on a release native. **After:**
main 230468c5 plus IO-TEXT-1 (#630's content, merged as 3c8f5c09; release native built at c97293e1, every later commit tests / docs only).

| Surface | PySpark | Present before | Present after | Missing after | Closed today |
|---|---|---|---|---|---|
| DataFrame | 115 | 106 | 108 | `asTable` `exists` `freqItems` `lateralJoin` `metadataColumn` `scalar` `transpose` | `inputFiles` `semanticHash` (#626) |
| DataFrameReader | 13 | 10 | 13 | — | `orc` `xml` declared (#610), `text` (#630) |
| DataFrameWriter | 18 | 14 | 18 | — | `orc` `xml` `jdbc` declared (#610), `text` (#630) |
| DataFrameNaFunctions | 3 | 2 | 3 | — | `replace` (#610) |
| Column, GroupedData, DataFrameWriterV2, Catalog, SparkSession, Row, types | — | complete | complete | — | — |

17 names missing at start, 7 at the end — all DataFrame, all carried over with oracles and briefs (§6). `sameSemantics` also changed
answer: it now reports Spark's plan equality (EX-DF-11 FIXED, #626).

## 2. Per-PR table

| PR | Unit | Builder rounds | Reviews (Grok 4.6, verdict, cost) | Merged |
|---|---|---|---|---|
| #610 | IO-DECLARED-1 (run 15b carry-over): ORC / XML / JDBC reader-writer names as dated refusals, `spark.read.jdbc` keeps PostgreSQL reads, `na.replace` | GLM 3 rounds (run 15b); orchestrator rebase onto #604–#609 (inventory union, writer_readwriter 1105 → 1104, EX-0 1035 → 1041) | critic 2 rounds (run 15b) | 7bcb68de tree-equal 09:12 |
| #621 | REGISTRY-16B-1: CONF-UNSET-1, CONF-WAP-1, IO-JDBC-FORMAT-1 BACKLOG rows with pins | GLM 1 round $0.04 (pins); orchestrator rows and ledger | none (docs + pins) | d976e34c tree-equal 10:36 |
| #626 | DF-PLAN-INTROSPECT-1 (run 15b carry-over): `inputFiles`, `semanticHash`, plan-equality `sameSemantics` via a canonical plan walk in Rust | Muse r2 144, r3 166, r4 277 steps; orchestrator R-19 disposition and three rebases | logic: r3 NEEDS_REMEDIATION $0.60 (L-201 P1 qualifier strip false equality) → r4 **PASS** $0.70 (L-301 P3 residue); Rust perf: r3 **PASS** $0.64, r4 **PASS** $0.51 | e9ea03c7 tree-equal 12:30 |
| #630 | IO-TEXT-1 (run 15b carry-over): `read.text` / `write.text`, Rust scan, one-scan partitioned writer, partition discovery, user-schema overlay, streaming spilling fallback sort | Muse r3 574, r4 226, r5 293, r6 244, r7 233 steps; orchestrator R-39 and R-42 pins, six rebases, trailer repairs, a CI time-zone fix to one pin (R-16b-44) | logic: re-check $0.83 NEEDS_REMEDIATION (L-101 P1 silent row drop) → r3 $0.95 → r4 $0.74 → r5 $0.71 PASS → r6 $0.60 NEEDS_REMEDIATION (L-501) → r7 **PASS** $0.55; Rust perf: re-check NEEDS_REMEDIATION (P1 one scan per key) → r3 $0.56 → r4 $0.54 → r5 $0.58 (P1 fallback never spilled) → r6 $1.08 (P2 pool) → r7 **PASS** $0.92 with one P2 recorded as IO-TEXT-PART-POOL-1 | 3c8f5c09 tree-equal 17:10 (first CI run red on one machine-zone pin, fixed; second run green, merged on the first queue pass) |

**Worker spend (runs.tsv, run 16b lanes).** Muse Spark 1.3 contributor: 8 builder rounds, 2157 steps (subscription, $0 metered). Grok 4.6:
16 reviewer rounds, $11.13. GLM 5.3 Flash: 1 round, $0.04.

**Oracle batches recorded before ruling (live PySpark 4.1.2, one short JVM each):** planintro_l102, planintro_cast, planintro_r4 (plan
hashing); iotext_probe3..probe7 (text IO: escaping, discovery, overlay types, New York session); conf_jdbc (with repark main's answers);
pool reproduction (repark); and, recorded for the carry-overs, dfrust3 (freqItems / transpose / metadataColumn), dfsubq (scalar / exists /
lateralJoin / asTable), orc (with Spark-written ORC fixtures). Eleven review claims were decided by a live cell; four were overturned
(L-201 `k=007` inference, L-205 bare glob, the mixed-layout "error", run 15c's "unset restores the builder value").

## 3. Rulings taken under G-2 (orchestrator ids R-16b-1..44)

- **R-16b-1/2** — #610 lands first because it shares reader.py, writer_readwriter.py and the reader/writer oracle with IO-TEXT-1; IO-TEXT-1
  rebased `--onto` its squash.
- **R-16b-3** — plan hashing measured on Spark: a DataFrame filter equals the same SQL `WHERE`; an in-order all-columns select equals the frame.
- **R-16b-4/5/6** — DF-RUST-3 is freqItems + transpose; metadataColumn becomes DF-METADATA-COL-1 (a hidden `_metadata` column on file scans);
  freqItems `support` accepts ints like Spark Connect; neither name has a Spark SQL spelling.
- **R-16b-7, -16, -17, -24, -25, -27, -28, -29, -34, -35, -37, -40, -42** — the IO-TEXT-1 review rounds, each measured live before ruling
  (ruling ids U-1..U-11, V-1/V-2, W-1..W-5, X-1..X-5, Y-1/Y-2, Z-1/Z-2, R-39, R-42 in the unit ledger).
- **R-16b-12, -13, -18, -19, -22** — the DF-PLAN-INTROSPECT-1 review rounds (R-6..R-19 in the unit ledger).
- **R-16b-15, -23** — disk rule Q-15a-6: from 12:18 run 16b kept ONE build clone; no new Rust unit opened at or under 250 G.
- **R-16b-21** — `_ddl_type` → `ARRAY<INT>` measured on main (fails the Iceberg CREATE TABLE type mapping); granted to run 16c's FNP-4B as a
  one-line seam edit instead of a standalone PR.
- **R-16b-26** — REGISTRY-16B-1 merged with its ledger in staging/; IO-TEXT-1's departure moved it.
- **R-16b-30, -31, -36, -39** — time budget: DF-RUST-3 did not open (IO-TEXT-1 could not merge by 15:30); round 7 was IO-TEXT-1's last
  remediation round; loud-only findings ship as pinned BACKLOG rows, silent wrong values block.
- **R-16b-41** — the tight-pool refusal reproduced in 0.2 s at a 16 MiB pool; pinned as today's loud answer with no destination or staging left.
- **R-16b-44** — #630's first CI run failed one pin that read a recorded Python datetime in the machine zone (green on this box, red on UTC
  runners); the product answer was right on both, and the pin now reads the wall clock in the probe's zone.

Owner rulings applied: **Q-15B-1** (orc-rust approved; IO-ORC-1 brief uses 0.8.0), **Q-15B-3** (`inputFiles` answers `[]` on Iceberg, #626),
**Q-15B-4** (`format("jdbc")` URL dispatch recorded as IO-JDBC-FORMAT-1, #621), **Q-15a-2** (merge queue file), **Q-15a-6** (second build
clone only above 250 G).

## 4. Owner questions (with recommendations)

- **Q-16b-1 — DF-PLAN-INTRO-CAST-1.** The Spark door types integer literals as BIGINT before coercion, so `semanticHash` / `sameSemantics`
  cannot separate `CAST(x AS BIGINT) > 1` from `x > 1` there (the DataFrame door matches Spark). *Recommendation:* no new unit; run 16c's
  typed-literal work (FNP-4B) flips the pin and retires the row when it lands (told 16c).
- **Q-16b-2 — build-clone disk budget.** Three orchestrators held `/` near the 250 G line all day (low 242 G), so run 16b ran every Rust unit
  serially on one clone behind IO-TEXT-1's review rounds, and three units with oracles and briefs ready did not open. *Recommendation:* free
  ~150 G before the next day run (timeshift snapshots, stale lane clones), or let orchestrators share one `CARGO_TARGET_DIR` per worktree
  family so a second clone costs ~2 G instead of ~40 G.
- **Q-16b-3 — LOGICAL-WIDTH-1 / DF-TO-BINARY-1.** The facade still reports smallint / tinyint as `int`, float as `double` and binary as
  `string` (engine Arrow types are exact). IO-TEXT-1 now pins four more instances. *Recommendation:* schedule the `arrow_type_key` width fix as a
  1.5 unit; it touches `fillna` width-preserving casts and the census pins, so it needs the full facade suite as its blast-radius gate.
- **Q-16b-4 — IO-TEXT-PART-POOL-1.** A partitioned write past 256 keys refuses loudly under a tight session memory pool at DataFusion's default
  64-way partitioning. *Recommendation:* a small follow-up that caps `target_partitions` (or coalesces in-flight) for the fallback, measured
  against the 128 MiB default-partition case.

## 5. Rust-first roll-call (Python-only logic left in Python, with reason)

| Unit | Left in Python | Why it may stay there |
|---|---|---|
| IO-DECLARED-1 (#610) | `orc` / `xml` / non-PostgreSQL `jdbc` refusals, `write.jdbc`'s save-mode check, `na.replace` → `DataFrame.replace` | declared refusals and argument checks are API plumbing; `na.replace` delegates to the existing plan builder |
| REGISTRY-16B-1 (#621) | nothing (rows and pins) | — |
| DF-PLAN-INTROSPECT-1 (#626) | the `NOT_DATAFRAME` gate, the per-handle `inputFiles` memo dictionary, cache-view lineage gathering | argument checks and session bookkeeping the engine does not own; every walk, hash and comparison is Rust |
| IO-TEXT-1 (#630) | option and argument checks (lineSep, compression, mode spelling, recursiveFileLookup), the path-list union, save-mode staging and cleanup, `_integral.attach_error_condition` | API plumbing and exception attributes; every scan, split, glob, discovery, overlay, escape, sort and write is Rust |

## 6. Carry-overs for the next run (oracles and briefs recorded)

| Unit | Names | State | Brief |
|---|---|---|---|
| DF-RUST-3 | `freqItems` (DataFrame and stat), `transpose` | not opened (time budget) | `/tmp/oc-worker/qb-build2/brief-dfrust3-1.md`, oracle `dfrust3_probe_2026-09-15.json` (+ tuple cells in `dfsubq_probe`) |
| DF-SUBQUERY-1 | `scalar`, `exists`, `lateralJoin`, `asTable` | not opened (disk rule + time) | `/tmp/oc-worker/qb-oracle/brief-dfsubq-1.md`, oracle `dfsubq_probe_2026-09-15.json` |
| IO-ORC-1 | read-only ORC scan on orc-rust 0.8.0 (Q-15B-1) | not opened (disk rule + time) | `/tmp/oc-worker/qb-oracle/brief-ioorc-1.md`, oracle `orc_probe_2026-09-15.json` + Spark-written fixtures `orc-fixtures/`; reuse IO-TEXT-1's `partition_discovery.rs` and globs |
| DF-METADATA-COL-1 | `metadataColumn` (hidden `_metadata` struct on file scans) | not carded | oracle cells `metadata_*` in `dfrust3_probe_2026-09-15.json` |

All oracle files are under `/tmp/oc-worker/qb-oracle/`; copy them before the box reboots (`/tmp` is cleared on boot).

## 7. Lessons

- **A Muse round can commit Rust edited after its last release build.** Check `find crates -name '*.rs' -newer _native.abi3.so` before gates or
  copying a native into reviewer clones; round 3 of DF-PLAN-INTROSPECT-1 needed an orchestrator rebuild.
- **Muse round commits kept losing their `Authored-By` trailer** (the line was present but not separated by a blank line). Every brief now says
  so explicitly; audits read `%(trailers:key=Authored-By)`, not grep.
- **Measure every UNMEASURED review cell before ruling.** It overturned four claims and turned "the mixed layout errors" into "Spark ignores the
  root file".
- **A perf reviewer is the only thing that catches "spill-capable" that does not spill.** Round 5's fallback drained the tail into memory;
  logic review passed it.
- **Parallel Bash calls share one working directory**: absolute paths and `git -C` only.
- **A merge chain that looks hung may already have merged.** `gh pr checks --watch` merged #621 seconds before a manual attempt; read the PR
  state first (the merge script now does).
- **The staging map is sorted on main now.** Rebases resolve it by taking main's side and re-placing the unit's entry at its sorted position.
- **opencode / GLM cannot read files outside the lane clone**; put oracle files inside the clone or the brief.
- **A pin built from a locally rendered Python datetime must fix the zone.** The probe's Python process renders Spark instants in the box's
  America/New_York zone, and CI runs in UTC; `time.mktime` on the recorded repr passed here and failed #630's smoke job. Compare strings in the
  session zone, or read the recorded wall in the probe's zone explicitly, and run new timestamp pins once under `TZ=UTC` before pushing.
- **One build clone serializes every Rust unit.** IO-TEXT-1's seven review rounds held the only clone from 12:18 to 16:23.

## Pointers

- Ledgers: `task/ledgers/completed/{io-declared-1,registry-16b-1,df-plan-introspect-1,io-text-1}-ledger.md`.
- Registry rows added or changed today: IO-ORC-1, IO-XML-1, IO-JDBC-1 (#610); CONF-UNSET-1, CONF-WAP-1, IO-JDBC-FORMAT-1 (#621);
  DF-PLAN-INTRO-1, DF-PLAN-INTRO-CAST-1, EX-DF-11 FIXED (#626); IO-TEXT rows, IO-TEXT-PARTDISC-1 FIXED, IO-TEXT-PART-POOL-1, LOGICAL-WIDTH-1 and
  DF-TO-BINARY-1 pin lines (#630).
