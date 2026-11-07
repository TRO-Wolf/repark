# Hand-off to the next orchestrator — Iceberg format-v2 label closeout, 2026-11-07 (~15:00 UTC)

The previous orchestrator session ended with ANSI-SUBSCRIPT-AND-AMBIGUOUS-FIELD-1 (PR #1267) paused on the
owner's condition after its fourth verifier round. This document is the whole hand-off: where main stands, what is
in flight, the queue in order, the mechanics the repo does not carry, and the questions the owner still owns. The
session-local locations (scratch clones, briefs, verifier outputs, reproduction runs) are not in the repo; they are in
the project memory directory under `handoff-2026-11-07-paths.md` and `ansisub-pause-2026-11-07.md`.

## 0. Read first, then obey

1. [AGENTS.md](../AGENTS.md) from `origin/main` and its read path; [STATUS.md](../STATUS.md) for state;
   [task/lessons.md](lessons.md) for the dated owner rulings. The project memory directory carries the dated rulings
   as one file each; read `MEMORY.md` there, then `ansisub-pause-2026-11-07.md`, then
   `map-policy-evaluation-2026-10-29.md` (the running log of every round since 2026-10-29).
2. **Standing rules, in effect** (each has a dated lessons entry or memory file):
   - Anthropic models write NO comments in code (Rust `//`, `///`, `//!`; Python, YAML, shell, TOML `#`). Markdown
     prose is fine. Attributes such as `#[derive]` are not comments. Fork exceptions: the verbatim Apache licence
     header and a one-line doc comment under `#![deny(missing_docs)]`.
   - Commit trailer, exactly and last: `Authored-By: Claude (<model-id>) <noreply@anthropic.com>`; Muse lanes
     `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`. Never `Co-Authored-By`, never a
     session link, never a scratch or home path, in commits or PR bodies. The owner's rule overrides the tool's
     attribution reminder.
   - No account id, bucket name, region, ARN, catalog or table name from the owner's bug report in repo or
     artifact text.
   - Lanes, clerks and verifiers never touch the owner's code directory; every lane runs in a scratch clone under
     the session scratchpad with the `TRO-Wolf` identity, push URL disabled; only the orchestrator pushes
     (`git push https://github.com/TRO-Wolf/<repo> HEAD:<branch>`) and opens PRs; agents never merge; merges only on
     the owner's word naming the PR; the label PR needed an explicit word.
   - **Opus means `claude-opus-5-5` only**, verified from the task transcript
     (`grep -oE '"model":"[^"]+"' tasks/<id>.output`) at every hand-back. Builder lanes are Opus 5.5. Clerks are
     Haiku 5.5 at effort xhigh or Muse. Frontier models write no map rows, ledger rows or docs prose (briefs and PR
     bodies excepted; mechanical path, row, baseline-number and formatting fixes with the orchestrator's trailer
     are allowed).
   - Muse MAX is the standing second verifier on every product PR; merge on the union of PASS. Since 2026-11-06 the
     order is Muse first, Opus only after a Muse PASS (usage).
   - CI green before any verifier (push the builder head first; watch by commit sha with
     `gh api repos/TRO-Wolf/repark/commits/<sha>/check-runs` and wait until at least 20 checks are registered).
     Every verifier finding is reproduced by the orchestrator before a fix round. A failed test is never re-run
     without the owner's word; a job cancelled at its time cap with no failed test may be re-run once.
   - One-round design (2026-11-03): the clause is a measured table; cap one build, one critique, one delta, one
     re-check, then park. A panic, a wrong read or a wrong write blocks the label regardless. Cards are for
     refusal text, over-refusal and refuse-with-hint only. Ruling 76(a): no hint or refusal names a data-deleting
     action.
   - Rules adopted 2026-11-05: before any divergence is ruled a card it is measured inside DELETE and UPDATE
     predicates and an INSERT source (the DML-reach rule); a special-values row (NaN, -0.0, '', null, extremes)
     per family; audit probe cells are permanent in the corpus; verifier briefs carry the standing claim "the same
     expression in DML".
   - Builder briefs name the full facade suite (`pytest python/repark/tests -n 8`, one process), the pre-commit
     script list by hand (`scripts/check_lib_py.sh`, `check_lib_rs.sh`, `check_rust_file_size.sh`,
     `check_docstring_presence.sh`, `python3 scripts/sync_map_md.py --check`, `check_map_md.sh`,
     `check_crate_dag.sh` with `cargo metadata` piped in, `check_manifest.sh`,
     `python3 scripts/check_docs_compaction.py`, `uvx typos@1.47.2`, ruff check and format),
     `pytest python/repark-parity/tests -n 8` (it mirrors the `check_lib_py` baselines in
     `test_cap_1_source_file_line_cap.py`), `cargo fmt --all -- --check` before retiring any size row (fmt can grow
     a file), `make rust-panic-ban` and `make rust-clippy`.
   - Baseline rows are lowered by hand to the exact size only, never raised, never `--update-baselines`; map rows
     may be condensed, never removed while the file exists. The STATUS line `_Last updated: 2026-09-12._` is pinned
     by `python/repark-parity/tests/test_v1_gate_docs.py` and never changes.
   - Workers `taskset -c 0-25`, `CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8`; heavy runs under
     `systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet`; one Spark 4.1.2 oracle JVM at a time
     (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`); fork lanes `flock /tmp/fork-build.lock` and `CARGO_INCREMENTAL=0`;
     the rm guard needs `"${S:?}/${d:?}/target"` spelling; no foreground `sleep` chains; one CI watcher per batch.
   - Never amend or force-push a pushed head (merge main, never rebase); never base a PR on another PR's branch;
     ledgers already on main are append-only; verdict words are PROVEN, OPEN, REJECTED only.
   - Usage: one lane at a time (owner, 2026-11-02; at 76 % weekly on 2026-11-05). The owner's usage reset is the
     morning of 2026-11-07. Subagent prompt-cache TTL is one hour; keep the orchestrator's turns minimal and read
     only tails of long outputs.

## 1. Where main stands

- **Merged this cycle (all with records):** HADOOP-DISCOVERY-RULE-1 (#1250, #1251), the five-family corpus
  (#1252 to #1255), MAP-KEY-ALTER-1 (#1256), SUBSCRIPT-DOT-ACCESS-1 (#1259, #1260), STAGED-PUBLISH-CLEANUP-1
  (#1261, #1262), the label text (#1263), the audit records (#1265), DQ-IDENTIFIERS-CONFIG-REFUSE-1 (#1266,
  #1268).
- **The label** (`docs/spark-sql-iceberg-parity.md`, "Format-v2 label: supported behaviour (2026-11-05)"): status
  PROPOSED for PRODUCTION, maturity WITHDRAWN (ledger C-072) until the re-audit re-dates it. The 2026-11-05
  corpus audit's verdict is LABEL DOES NOT STAND (ice-v2-label-1 ledger C-097). Of its five findings: AUD-021 fixed
  (#1266, C-098 closed); AUD-022 and AUD-023 are on PR #1267 (C-099 open); AUD-024 NaN under Iceberg scan filters is
  a kept divergence by the owner's ruling (C-114); AUD-025 is unit 3, not started.
- **Corpus:** `docs/corpus/` holds the five families (discovery 3904, names 2362, nested 2973, delete files 6464,
  commit 364 cells) plus the 450 re-audit cells, and on #1267 the 1031-cell struct-domain table
  (`python/repark/tests/sql/data/struct_domain_cells.jsonl`, generator and driver beside it).

## 2. In flight: PR #1267, PAUSED at `8e929978`

- **History.** Round 1: both verifiers FAIL on the DataFrame Column-index subscript answering null under ANSI
  (reproduced); the pre-existing `struct(...)` naming wrong write folded into delta 1. Round 2 (`df0cf3aa`): Muse
  PASS, Opus FAIL on the string-subscript naming arm, a struct-comparison regression and DataFrame `F.struct`
  naming. Delta 2 (`66ecb273`, owner cap exception): Muse PASS, Opus FAIL on MERGE seats skipping the naming pass
  and the positional rename stopping at arrays. Delta 3 (`8e929978`, owner "go with option 2"): the measured
  struct-domain table first (1031 cells, 21 seats), one naming entry point in the SQL router, a pairing rule that
  recurses into lists and maps, a duplicate-name expression planner, and an empty-input DELETE/UPDATE guard; Muse
  PASS (all five claims), Opus FAIL. Every finding of every round was reproduced by the orchestrator.
- **Proven on the branch:** ANSI out-of-range subscript raises on both doors for literal and Column indexes;
  `F.get`, `try_element_at` and ANSI off answer null; `struct(...)` names fields as Spark does on both doors;
  duplicate-field lookups refuse with nothing written; structs pair by position (including arrays, maps and
  MERGE) with the measured field-type rule; the duplicate-name comparison fold; a null-struct DELETE writes
  nothing. Against the table: 988 of 1031 cells equal, 2 wrong writes left and both are Spark's own internal
  error on an Iceberg struct literal (oracle defect), every other differing cell reasoned in the pins.
- **Open (round 4, reproduced):** F9, DataFrame string expressions (`filter(str)`, `where(str)`, `F.expr`) bypass
  the naming pass, whose only call site is the SQL router, so mismatched structs pair there and `insertInto` or an
  overwrite writes where Spark refuses (pre-existing on main; the fix is the same call in the DataFrame
  expression-string path). F10, the duplicate-name planner covers binary comparisons only; `IN`, `NOT IN` and
  `isin` with duplicate-name struct operands that need widening reach DataFusion's duplicate-name assertion, a
  caught panic on dev and CI wheels and an ordinary answer on the release wheel, where the assertion is compiled
  out. Cards C1 to C15 plus the builder's three (DataFrame `F.struct` over a map value by string key; Spark's
  mismatch text rendering; the facade cast-type parser has no struct types) are listed in the PR comments and in
  the pause memory file.
- **The owner's condition (2026-11-06):** one more failure pauses the unit for a reorganisation after the usage
  reset. The pause is in force: no delta 4, no new lane. The reorganisation proposal is in
  `ansisub-pause-2026-11-07.md` (memory) and reproduced in §3.

## 3. The queue, in order

1. **The owner's reorganisation choice for #1267.** Proposal: extend the struct table with a door axis (SQL,
   DataFrame typed API, DataFrame string expressions) and cross duplicate names with every operator (`=`, `<=>`,
   `!=`, `<`, `IN`, `NOT IN`, `isin`, `IN (subquery)`) and every DML seat, re-measure on Spark; one naming entry
   point shared by the SQL router and the DataFrame expression-string path, with a unit test asserting both call
   sites; the duplicate-name planner generalised to `InList` and `InSubquery`; the Rust table test run under the
   release profile as well as dev, pins failing any panic on either; then one re-check round, Muse first, Opus on
   a Muse PASS. A further failure parks the unit and the label waits for v1.5.7. Alternative: close unit 2 on the
   ANSI subscript half (proven in every round) and move the struct domain to its own unit from main carrying the
   branch's commits. Both are the owner's call.
2. **The unit-2 records clerk** after #1267 merges (Haiku 5.5 xhigh): the ledger over the rounds, the card
   closure, one residue card for the fifteen-plus cards, the C-099 closure line in the staging ledger, the label
   section's dated line, and two owed doc lines (the ANSI sentence in `docs/guide/session-and-conf.md`; the
   label section's double-quoted-identifiers exclusion row, which still reads the pre-audit "a card, not a wrong
   write" and needs a dated correction citing #1266).
3. **Unit 3, CTAS-QUERY-FAILURE-CLEANUP-1** (AUD-025: a CTAS whose query fails leaves a metadata file and RePark's
   path read answers an empty table where Spark refuses). Card
   `task/roadmap/mid-term/ctas-query-failure-cleanup-1-card-2026-11-05.md`; branch from main; one Opus 5.5 lane.
4. **The narrow second re-audit** (the five findings plus DML-reach probes on their classes), then the records
   pass that re-dates the label (status PRODUCTION, maturity line), then **v1.5.6** (the v2 label only).
5. Dependabot PRs #1264 and #1240 and the older parked product PRs (#1071, #1048, #1038, #1037, #1014, #1011,
   #1000) are untouched by this cycle and keep their earlier rulings.

## 4. Mechanics the repo does not carry

- **Lanes and clones.** Each unit builds in a scratch clone of the repo; verifiers get their own clones checked
  out at the PR head with the head wheel installed (`uvx maturin@1.14.1 develop` from `python/repark`). The
  previous session's clones, briefs and probe outputs are listed with their absolute locations in the memory
  file `handoff-2026-11-07-paths.md`; they live under `/tmp` and survive until a reboot or a cleanup.
- **Brief patterns.** Builder briefs: findings with reproduce keys and measured Spark answers, the delta in
  numbered steps, the cards not to touch, the gate list of §0, the hand-back JSON in an untracked
  `handback-notes.md`. Verifier briefs: a common file plus one per verifier, claims numbered, each PROVEN or a
  finding with a reproduce command, severity S1 in DML, S2 in a SELECT, text differences as cards, attestation
  categories from `.agents/skills/sepmo/references/05-critic.md`, hand-back as a verdict file (Opus) or the
  launcher's `handback.json` (Muse). Clerk briefs follow the DQ-IDENTIFIERS-CONFIG-REFUSE-1 records brief.
- **Muse launcher.** `~/.claude/skills/muse-worker/muse-worker.sh --lane <lane> --repo <clone> --brief <file>
  --role critic --effort max --max-steps 160`; runs land under `/tmp/muse-worker/<lane>/<utc>/handback.json`;
  verify the identity file and the `runs.tsv` row at every hand-back.
- **Spark oracle.** Spark 4.1.2 with Iceberg 1.11.0 on this box; drivers take an absolute warehouse and write
  JSON; the audit's `spark_util.spark_session(warehouse, name)` helper; one JVM at a time; the hadoop catalog
  for DML readbacks.
- **Known environment noise on the clones (not findings):** the full facade suite can fail
  `io/test_mapinarrow_oracle.py` and `io/test_pandas_udf_oracle.py` when the clone's PySpark worker lacks
  pyarrow or pandas (the files pass alone and CI is green), and `perf/test_t2_spill_reach.py` can hang a full run
  near the end (run the file alone). DataFusion's duplicate-struct-field check is a debug assertion: prove "no
  panic" on the dev profile and re-check answers on the release wheel.
- **Memory hygiene.** The memory index must stay under its size limit; keep index lines short and put detail in
  topic files. The running log for this cycle is `map-policy-evaluation-2026-10-29.md`.

## 5. Open questions for the owner (ask in plain text; do not block on them)

1. Which reorganisation for #1267: the complete-table delta 4 on the branch, or the split into an ANSI-only
   close and a new struct-domain unit?
2. Does unit 3 start before or after #1267 resolves, given one lane at a time?
3. Are the fifteen-plus unit-2 cards accepted as one residue card, or do any of them block the label (none is a
   wrong write; two are refusals of statements Spark answers in DML)?
4. v1.5.6 timing once the re-audit passes.
