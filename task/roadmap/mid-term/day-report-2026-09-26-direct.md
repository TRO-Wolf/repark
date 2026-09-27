# Direct orchestration — day report (2026-09-26 06:00 → 2026-09-27 02:30 EDT)

The orchestrating session ran the v1.5.0 remainder directly, with the owner's rulings of the day
applied as they came: the guided-execution bands (executors under 50 on Terminal-Bench 4.0 run
one cell per round with every ruling pre-made), the usage-conservation plan (Opus 5.5 only for
design-heavy work and one scoped medium verifier per product-Rust PR; docs and test folds by the
orchestrating session), and a cap of four concurrent Muse lanes. This report lists what merged,
what the scoreboard says, every owner ruling, the dispositions, the process lessons, and the
spend picture.

## Merged (RePark, all tree-equal through the merge driver, in merge order)

#846 (the executor band table, handbook rule, Muse addendum, task card), #844 (U9 PR2 — `VOID`
columns are Iceberg `unknown` on v3, `uuid` columns present as strings on every door), #847
(PARTNAME-1 — partition-field names pinned per door; the U9 uuid bucket step compares the same
door; the fork's redundancy divergence recorded), #848 (RP50-A — the v1 ref-write guard the fork
now owns is gone: v1 branches, tags, branch writes, WAP and fast-forward commit like Spark), #849
(RP-53 — fork repin to `96582bc2`), #850 (RP50-B — the replace re-key call the fork now owns is
gone; partition-spec order pinned EQUAL), #843 (U11-EDGE-1 — output columns keep the query's
spelling, partition sources bind case-sensitively; a debug-build stack overflow on the nested-view
recursion fixed by boxing the new futures), #851 (CATALOG-1 part 1 — the session catalog is
`spark_catalog` and `spark.sql.defaultCatalog` moves the current catalog until `USE` pins it),
#852 (TBLPROPS-1 — `SHOW TBLPROPERTIES` on an Iceberg table answers Spark's rows).

Fork, squash-merged tree-equal with the Docker fixture jobs red on the quay.io `minio` pull
(owner option 1): #358 (F-UUID-STATIC-2 — the `uuid_as_string` flag survives `deregister_table`;
static-provider pins; one advertisement path).

Overnight (Muse-only executors after the 18 % usage re-evaluation, one scoped Opus 5.5 medium
verifier per product-Rust PR, folds through guided Muse rounds and, on one lane, Devin SWE-2),
in merge order: #854 (F-ROW-LINEAGE-ORDER-1 — a delegated `INSERT` commits its files in Spark's
fanout-writer order through the fork's commit-order hook; `R-MC-ROW-ID-V3` EQUAL in six runs),
#855 (CATALOG-1 part 2 — `type=memory` refuses at first use on both doors, both-keys refusal,
`USE` forms, `listDatabases`, toml `session.default_catalog`; the facade leaves a two-part name on
a refused catalog to the native refusal, matched case-sensitively like Spark), #856 (NTZ-1 slice
1 — `TIMESTAMP_NTZ` literals and `CAST … AS TIMESTAMP_NTZ` on both doors; the invalid-literal
window counted in characters after a multibyte panic surfaced), #858 (TZ-ASOF-1 — an expression
named like its ORDER BY key plans; the binding rules measured on live Spark), #857 (RP-54 — fork
repin to `0d3f2b4f`; eleven pins moved to the fork-owned shapes). Fork, squash-merged tree-equal
with the Docker fixture jobs red on the quay.io `minio` pull, each verified by log: #359
(F-HADOOP-STAGED-CREATE-1), #360 (F-DELETE-GRANULARITY-1), #361 (F-MANIFEST-MERGE-1).

Open for the owner: #853 (the orchestrator doctrine package — latitude ladder with PROPOSED usage
thresholds, the Muse adapter, the lane contract, the dated rulings file, `claude_usage.py`).

## Scoreboard

| | 09-25 | 09-26 06:34 (`f28a122f`) | 09-26 14:05 (`82e8e871`) | 09-27 02:25 (`9aa1c185`) |
|---|---|---|---|---|
| EQUAL | 668 | 684 | 688 (with carve-out C-3) | 704 |
| Non-EQUAL cells Spark answers | 45 | 28 | 24 | 6 |

The overnight run gained seventeen cells (the thirteen the five PRs targeted plus
`CAT-CURRENT-CATALOG`, `E-CASE-SELECT` and the two `D-SHOW-TBLPROPERTIES` cells from #852) and
lost one: `L-INSERT-OVERWRITE`, a pre-existing source-scan-order nondeterminism on the owned
overwrite path (the main wheel answered non-EQUAL in four of five runs before any of tonight's
branches; one of six on identical code during the row-lineage round) — candidate scope for
R-FILEORDER-2, not a regression. What remains: the three streaming cells (carve-out C-1),
`D-NS-NESTED` (C-2), `TY-VARIANT-V3` (the sizing decision), and that flake.

## Owner rulings (dated, all in `run29/claims.txt`)

1. Catalog default = Option A (`spark_catalog` like Spark; `spark.sql.defaultCatalog` honoured;
   toml `session.default_catalog`), no carve-out.
2. Guided-execution bands on Terminal-Bench 4.0 (50 / 25) accepted; weekly re-pull.
3. CAT-TYPE-MEMORY: Spark's refusal on the bare `type=memory`, the toml rewrite to the long
   form, a RePark opt-in on the builder door.
4. Usage-conservation plan at 8 % weekly usage (Opus executors only for design; no Opus verifier
   on docs/tests-only PRs; one scoped Opus medium verifier on product-Rust PRs; no Opus fixers for
   S2/S3; fewer orchestrator turns); the tick-driven distributed mode stays off.
5. At most four Muse lanes at once (box load).
6. Carve-out C-3: `P-RDF-PARTIAL-PROGRESS` — Spark's partial-progress commit order is
   JVM-identity-hash dependent; the harness compares that cell's commit sequence
   order-insensitively.
7. Usage re-evaluation at 18 % (19:40): overnight Muse-only, event-driven orchestration, no Opus
   design rounds until the morning, verifiers unchanged; 19 % at 21:00.
8. The orchestrator-doctrine package (from a Muse review of the session transcript): a
   single-homed state file, a latitude ladder, a Muse adapter, a lane contract, the dated
   rulings, a token-accounting script — #853 for the owner's review.

## Dispositions and measurements worth keeping

- Spark 4.1.2 loads `org.apache.iceberg.inmemory.InMemoryCatalog` through `catalog-impl` end to
  end; the bare `type=memory` refuses at first use; both keys set refuse with
  `Cannot create catalog <name>, both type and catalog-impl are set: …`.
- Partition-field names are already Spark-equal per door (CREATE omits the width, the UPDATE
  door keeps it); the U9 residue compared different doors. The UPDATE-door redundancy rule
  differs (fork card F-PARTSPEC-REDUNDANT-1).
- v1 branches, tags, WAP and fast-forward commit on Spark; a tag on an empty v1 table refuses.
- The v3 row-lineage cell's Spark answer is stable across three runs; the rewrite
  partial-progress order is not (carve-out C-3).
- The catalog Opus round ran out of its 300-turn budget after two of five cells; per the handbook
  the remainder was split into guided Muse rounds (CATALOG-1B, 1C), which landed the rest.
- Spark looks catalog names up case-sensitively (`C_MEM.t` is a different, unregistered name that
  Spark answers from the session catalog); RePark's session catalog resolves namespace case
  exactly where Spark folds it (residue R-6 → CASESENS-1).
- Spark binds an unqualified ORDER BY key to the OUTPUT column (`CAST(id AS STRING) … ORDER BY
  id` sorts as strings); a qualified key binds to the source column; DISTINCT with an
  un-projected key refuses on both engines.
- The NTZ cast is nullable on Spark in both ANSI modes; the NTZ literal is not. The invalid
  literal's `== SQL` window and carets count characters.
- The fork's DataFusion `Overwrite` commit arm is unreachable from both RePark doors (every
  overwrite goes through owned stage-then-swap), so the row-lineage order covers `INSERT` only.
- The verifiers independently recomputed every fanout-order golden from the stated algorithm and
  measured Spark for every disputed shape; six of the night's seven verifier rounds found real S1
  defects the executors' own gates had passed.

## Process lessons (ledgered in `scripts/coordinator/lessons.md` at the next run's start)

- After a merge-driver conflict or branch update, lease a force-push against the fetched REMOTE
  head, not the last local push; `--force-with-lease` needs the full sha.
- A local gate must include every test file that greps for the feature keyword, and session or
  catalog-default PRs must run `scripts/check_example_coverage.py --require-execute` locally.
- Muse provider faults (stream idle timeout, transport timeout, network) hit four rounds; each was
  resumed with a finish order or relaunched. None was an engine weakness.
- Guided halts worked as designed: TBLPROPS-1 (a module placement contradiction), RTP-1 (a wrong
  diagnosis in the order), F-RDF-PARTIAL-1 (a refuted design premise), F-ROW-LINEAGE-ORDER-1 (an
  unreachable pin target) and RP-54 (pins the fork now owns, three halts) each stopped with the
  exact question.
- Three CI-required checks the local gate did not run, each caught late: the `lib.rs` thinness
  guard (`scripts/check_lib_rs.sh`), the CAP-1 moved-symbol hash pins
  (`test_production_file_size.py`), and `cargo clippy --all-targets` over test targets. All three
  are now in the gate mechanics of the orchestrator state file.
- The gate scripts take each pytest path as its own argument; a quoted list runs nothing (`U=4
  L=4`) while the Rust phases pass. Never pipe a rebase or gate through `tail` inside an `&&`
  chain (the exit code is masked); `pkill -f <script>` matches the orchestrator's own shell.
- When several PRs queue at once, the later ones' update-branch merges report CONFLICT after the
  first lands even when a local rebase is clean: rebase, full gate (main carries new code), push
  with a lease against the fetched remote head, requeue.
- Muse transport faults (`body-truncated`, five or six failed attempts over twelve minutes) hit one
  lane three times, on a resumed session and on a fresh one, at different steps with small
  outputs while three other Muse sessions ran normally; the fold moved to Devin SWE-2, which
  concluded it in one round with a clean audit.

## Spend

Day: Opus 5.5 one 300-turn high executor round (about 120 M cached tokens), two design rounds
(about 15 M and 23 M), one short design round. Night: seven scoped Opus 5.5 medium verifier rounds
(about 4 M each), no Opus executors or fixers; Muse Spark (flat rate) fifteen guided rounds and
follow-ups; Devin SWE-2 (free tier) two rounds. Weekly Claude usage: 8 % at 11:30, 18 % at 19:40,
19 % at 21:00 — the orchestrator session itself was the largest line item until its context was
compacted at 19:40. `scripts/coordinator/claude_usage.py` (#853) reproduces the breakdown.

## Next

`TY-VARIANT-V3` (variant sizing on the morning usage number), NTZ-1 slices 2 and 3, the
CASESENS-1 slices (now carrying the namespace-case residue R-6), a card for the
`L-INSERT-OVERWRITE` scan-order flake under R-FILEORDER-2, a card for the `uuid_cast.rs`
byte-offset window bug (same class as the NTZ fix), the three-part upper-case catalog text (R-7),
and the owner's review of #853.
