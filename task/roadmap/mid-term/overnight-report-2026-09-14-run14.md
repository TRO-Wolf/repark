# Run 14 report — the Iceberg production-cutover slate (2026-09-14, day run)

**Unit:** overnight-14 · **Window:** 12:53–20:43 local (G-3 stop 23:30) · **Charter:**
[docs/cutover/production-iceberg-status-2026-09-14.md](../../../docs/cutover/production-iceberg-status-2026-09-14.md)
§7 and §10 (docs PR #585), cards in
[ice-cutover-slate-2026-09-14.md](ice-cutover-slate-2026-09-14.md) (#586) · **Beside:** run 14b (ARRAY-NULL-1,
FACADE-4) on its own lanes.

Workers: Devin SWE-2 (`swe-2-high`) actors; Grok 4.6 read-only critic-logic per PR. No product code changed in
RePark in units 1–3, so the S2-21 perf reviewers did not run for them; for unit 4, which changed Rust and the Python binding, one Grok S2-21 reviewer covered both languages (CLEAN).

## Per-unit results

| # | Unit | PR | Result | Rounds | Critic |
|---|---|---|---|---|---|
| 0 | Slate cards (six cards) | #586 | merged `95640874` (tree-equal) | orchestrator | — |
| 1 | F-GLUE-REPLACE-1 (fork) | TRO-Wolf/iceberg-rust #282 | merged **`edc38c6a`** (tree-equal) | Devin ×3 + one orchestrator spelling commit | Grok critic-logic, 49 turns, $1.73: 0 P1, 2 P2 (fixed in round 3), 3 P3 (ledger residues) |
| 2 | RP-20 + ICE-GOLD-TWICE-1 | #587 | merged `0b33f5b7` (tree-equal); live run green | Devin ×2 | Grok critic-logic, 30 turns, $1.14: 0 P1, 5 P2 + 1 P3 (all fixed in round 2) |
| 3 | ICE-SPARK-TABLE-1 | #589 | merged `293fbcf6` (tree-equal) | Devin ×2 + orchestrator owner-scrub commit, two rebases | Grok critic-logic, 23 turns, $0.66: 0 P1, 6 P2 + 8 P3 (P2s and the cheap P3s fixed in round 2) |
| 4 | ICE-COMMIT-UNKNOWN-1 | #590 | merged `acbb6a8e` (tree-equal) | Devin ×2 + orchestrator date commit, one rebase | Grok critic-logic, 39 turns, $1.17: **1 P1** (service-managed CTAS dropped the table and swallowed the class on an unknown commit), 2 P2, 2 P3 — round 2 |

### 1. F-GLUE-REPLACE-1 — fork `edc38c6aa5cdbe132235f4fefc01d3066d6cff23`

- `GlueCatalog::publish_replace_table`: a pointer-only GetTable, then the expected-base check, then a read-back of the
  staged metadata with a uuid match, then `UpdateTable` through the existing `GlueCommitTransport` with the Glue
  `version_id` as the optimistic lock.
  - A stale base returns a retryable `CatalogCommitConflicts` before any send.
  - A missing or foreign staged file returns `DataInvalid` with zero sends.
  - A lost response returns `CommitStateUnknown` after one attempt, keeping the staged file.
  - A service conflict is retryable; an access denial is terminal.
  - Pins: 9 offline commit-transport tests. The recording transport also pins the `version_id` sent and
    `previous_metadata_location`.
- R158 residue (1), no read-validation before the pointer swap, is closed for Glue. S3 Tables still swaps without it
  (named).
- R158 residue (2), metadata versioning restarting at v0, is closed: the staged replace continues version N → N+1 with a
  fresh uuid.
  - Orchestrator audit P1 in round 2: a Hadoop-named base (`vN.metadata.json`) would have staged a shared
    `v(N+1)` name across concurrent replaces. Fixed and pinned.
- New named residues:
  - The staged replace runs no `CommitStateUnknown` reconciliation (Glue and S3 Tables alike).
  - An `i32::MAX` version wraps.
  - `publish_replace_table(table, None)` is a blind replace by trait contract.
  - Replace rebuilds the Glue `TableInput`, dropping Glue-only parameters, as `update_table` already does.
- Gates (orchestrator re-run): Glue lib 50, `iceberg --lib staged` 23, S3 Tables lib 39, `make check` clean. One typos
  red on the PR (a spelling variant in the ledger and `task/todo.md`, markdown only) was fixed by an orchestrator commit.

### 2. RP-20 (repin `3ebf7d36` → `edc38c6a`) + ICE-GOLD-TWICE-1

- The pin bump rode an orchestrator commit: five `rev` lines, the `docs/fork-sync.md` row, the root map sentence.
- `CREATE OR REPLACE TABLE … AS` twice legs on Glue and S3 Tables (`_acceptance_replace.py`, offline twin on the memory
  catalog). They pin rows, Arrow types, `append` operations and snapshot-history retention.
- The gold test uses per-run stems, runs `dbt run` twice (each model's history +1), then `dbt test`.
- `aws-acceptance.yml` (owner-approved change):
  - dbt pins install before the OIDC credentials step.
  - A `dbt gold acceptance` step runs after the silver step (`if: !cancelled()`).
  - Per-leg `-rA` output; `timeout-minutes` 60 → 90 (critic L-005).
- **Live proof — aws-acceptance run 34901483202** (`workflow_dispatch` on `main` `0b33f5b7`, 21:55:59Z → 22:27:04Z,
  conclusion success, no environment wait):

| Step | Leg | Result |
|---|---|---|
| acceptance module | `test_process_silver_acceptance_against_glue` | PASSED |
| | `test_process_silver_acceptance_against_s3tables` | PASSED |
| | `test_mor_merge_compact_expire_against_glue` | PASSED |
| | `test_mor_merge_compact_expire_against_s3tables` | PASSED |
| | `test_v3_dv_dml_maintenance_against_glue` | PASSED |
| | `test_v3_dv_dml_maintenance_against_s3tables` | PASSED |
| | `test_sql_harden_cutover_against_glue` | PASSED |
| | `test_sql_harden_cutover_against_s3tables` | PASSED |
| | `test_create_or_replace_twice_against_glue` | PASSED (new) |
| | `test_create_or_replace_twice_against_s3tables` | PASSED (new) |
| | total | **10 passed in 308.55 s** |
| dbt gold acceptance | `test_gold_stage_on_glue` (two `dbt run`, `dbt test` 10 blocks) | **1 passed in 36.19 s** (new) |

### 3. ICE-SPARK-TABLE-1

- **Mechanism (measured):** Spark 4.1.2 with Iceberg 1.11.0 on a module-private **Hadoop** catalog creates and seeds
  the table. `InMemoryCatalog` writes no local bytes, so it was rejected. RePark adopts it with
  `CALL … system.register_table`.
- **What Spark creates:** the production v2 copy-on-write properties, plus Spark's own CoW `MERGE` (`overwrite` history),
  `write.distribution-mode = 'hash'`, and in the live cell a second partition spec.
- **What RePark runs:** the production `UPDATE SET * / INSERT *` MERGE twice (20 → 30 → 35 rows, five columns, values and
  Arrow types pinned at each step), then the weekly CALLs with load-bearing arguments:
  - `expire_snapshots`: `older_than` between two snapshots.
  - `remove_orphan_files`: removes one planted pre-dated orphan.
  - `rewrite_data_files` binpack: pinned as admitted-and-no-op under the min-input floor.
- **What Spark reads back:** `EXCEPT ALL` empty both ways against the independently derived rows, count and
  per-partition counts, and readable `.snapshots` / `.history` / `.files` with zero delete files.
- **PR CI:** two always-run cells run adopt → MERGE ×2 → CALLs over a checked-in 67.8 KB Spark-written fixture.
  Nightly: two live cells in the parity-live tier. Co-collected live run: 9 passed.
- **Differences named (ledger C-007, registry `ICE-SPARK-TABLE-1`):**
  - Spark-stamped `owner`, codec and `write.distribution-mode` are carried verbatim (seven-key property equality pinned).
  - The codec is honoured at write time: ZSTD on the main table, SNAPPY on a `snappy`-stamped table (footer pins).
  - RePark continues the adopted `vN` metadata naming.
  - Mixed summary vocabularies are read fine by Spark.
  - `statistics` keys are dropped on RePark commits.
  - The evolved partition spec is honoured (RePark writes `spec_id 1` files).
  - Spark's **table cache** serves the pre-RePark snapshot until `refreshTable`. A stale-cache Spark `INSERT`
    scan-forwards and commits cleanly (measured; the critic had predicted a `CommitFailedException`). **The C4
    action:** refresh Spark's table cache after RePark commits. This is catalog-agnostic, and C4 is Glue, which has no
    version hint.
  - A transform sort order (`bucket(4, id)`) refuses the MERGE loudly and commits nothing:
    `WRITE-ORDER-TRANSFORM-1`, a stated residual.
  - **Residue:** RePark has no exists-fail on a Hadoop `vN` collision (fork R167).
- **Critic** (Grok 4.6 critic-logic, $0.66): no P1; six P2s fixed in round 2 (hollow row and CALL oracles, the
  stale-hint narrative, unpinned property and codec preservation, missing G-3 shapes).
- **Orchestrator hygiene:**
  - Spark stamps the OS user as table `owner`; the checked-in fixture now carries a neutral value.
  - My first gate script pointed the ivy redirect inside the clone, touching a tracked `.ivy2/` file; it was restored
    before commit. `.ivy2/` has 12 tracked files, accidentally committed in #416 — cleanup card suggested.
- **Gates** (orchestrator, rebased head `9b88d45c`): offline 2 passed / 2 skipped; live co-collected 9 passed;
  `make verify` 0; parity 757 passed / 2 skipped / 12 xfailed; docs, ledger and comment scans clean.

### 4. ICE-COMMIT-UNKNOWN-1

- **Class:** `repark.errors.CommitStateUnknownException(PySparkException)` carrying `operation_id`. Routing goes
  through a new `Error::CommitStateUnknown` variant and `ErrorClass`. The message is byte-identical to today's, and
  definite kinds are unchanged.
- **Operation id:** minted at six RePark commit sites (append and service-managed CTAS, MERGE and predicate DML,
  `INSERT OVERWRITE` / `TRUNCATE`, partition overwrite). A wrapper `CommitStateUnknownError` carries it and `error_map`
  downcasts it first.
  - Surfaces with `operation_id = None`: fork-provider `INSERT INTO` / `DELETE` / `UPDATE`, staged CTAS on **Glue**,
    CALL procedures.
  - Stamping Glue's staged CTAS needs a fork change (snapshot properties on `StagedTableTransaction`). That is out of
    this card's scope (no fork change), named as owner question Q-R14-2.
- **Data-safety fix found by the critic (P1):** service-managed CTAS (S3 Tables, both SQL doors) used to **drop the
  just-created table** on any commit failure, including an unknown outcome where the table may have landed. It also
  stringified the error, so Airflow would have seen the base class. On `CommitStateUnknown` it now keeps the table and
  returns the class with its id. Definite failures keep the drop-and-explain abort. Pinned red-first on both doors.
- **Keep-set pins:** the MERGE "keep written files on an unknown commit" carve-out (`abort.rs`) is now mutation-proven
  with real files.
- **Docs:** registry row `ICE-COMMIT-UNKNOWN-1` (statement-shape table, Airflow guidance: alert on the class; retry MERGE
  and CTAS IF NOT EXISTS only). Inventory §8 ruling 8 names the class and `exc.operation_id`. The API freeze register
  gains exactly one name (888 → 889).
- **Critic** (Grok 4.6 critic-logic, $1.17): 1 P1 (above), 2 P2 (keep-set unpinned; Glue CTAS id overclaimed in docs),
  2 P3 (`setattr` failure swapped the class; dates) — all fixed in round 2.
- **Orchestrator notes:** Devin's round-1 hand-back reported `check-ledger-grammar` green, but `make verify` on the
  rebased head failed it (no COVERAGE_ATTESTATION block, one uncited PROVEN clause); fixed in round 2. The unit was dated
  2026-09-15 in a dozen places; an orchestrator commit corrected them.
- **S2-21 perf reviewer:** Grok 4.6, 15 turns, $0.30 — **CLEAN**, no P1/P2. All new branches are on the commit-failure or raise path (success `map_err` is a no-op, the extra downcast runs only for a failed statement's `External` error, import cost ~310 ns). One P3: one extra 36-byte UUID `String` clone per successful commit, recorded in the ledger at departure.
- **Gates** (orchestrator, `ac057eb2`): targeted pins 9 + 1 + 4 + 3 + 75 passed; `pytest -k 'error or exception or commit or ctas'` 404 passed / 13 skipped; `make verify` 0; parity 757 passed / 2 skipped / 12 xfailed; docs and ledger gates clean.

## C0–C6 after run 14

| Step | State after run 14 | What changed today | Still blocking |
|---|---|---|---|
| C0 | Canaries recorded for 1.0.1 only | — | A canary record for the release that ships the cutover (1.4.1 is on `main`) |
| C1 | Silver CoW MERGE green on Glue and S3 Tables at the current pin | aws-acceptance run 34901483202 on `0b33f5b7` (fork `edc38c6a`): all 8 silver legs passed | — |
| C2 | Not started (pipeline side) | G-3 covered in this repository (ICE-SPARK-TABLE-1); ruling 6: Spark creates the shadow tables (card SHADOW-1 change) | `SHADOW-1` in the cutover pipeline; the Spark `refreshTable` step after RePark commits |
| C3 | Not started | — | C2 |
| C4 | Not started | G-3 measured (Spark-created table: MERGE ×2, CALLs, Spark read-back); G-6 closed (CommitStateUnknownException, merge ##590) | C3; rollback rehearsal; check 4 with Trino (ICE-TRINO-READ-1, parked); G-5 memory sizing |
| C5 | Not started | — | C4; the migrated script passes `dry_run => false` to `remove_orphan_files` (ORPHAN-2; the procedure refuses an `older_than` within 24 h) |
| C6 | **Unblocked on the engine side**: Glue replace publish live-proven; the gold module runs twice nightly in aws-acceptance | F-GLUE-REPLACE-1 (fork #282) + RP-20 (#587); gold module green in run 34901483202 | C4 holding (inventory ordering) |

## Decisions taken under G-2

- **F-GLUE-REPLACE-1.**
  - D-3 was scoped to `begin_replace` with a test-expectation guard.
  - Audit P1 in round 2: a Hadoop-named base (`vN.metadata.json`) would have staged a shared, uuid-less `v(N+1)` name.
    Fixed with a fresh uuid.
  - The critic's two P2 hollow oracles went back to Devin before merge.
  - The fork PR's typos red (a spelling variant in markdown) was fixed by an orchestrator commit.
  - Devin's round-3 commit carried the clone's local author name; I amended it to the repository identity with the tree
    unchanged.
- **RP-20 / ICE-GOLD-TWICE-1.**
  - The replace-twice helper was measured on the memory catalog, and its exact 1/2/3 counts were used on Glue.
  - S3 Tables uses relaxed counts plus retention pins (critic L-001).
  - The job `timeout-minutes` went 60 → 90 (critic L-005, inside the owner-approved `.github` change).
  - The dispatch fired automatically on the tree-equal merge.
- **ICE-SPARK-TABLE-1.**
  - A live Hadoop-catalog loop was chosen over a fixture-only fallback, with the fixture added for PR CI.
  - The critic's P2-3 (predicted `CommitFailedException`) was re-measured, and the measurement won: the Spark
    stale-cache write scan-forwards.
  - Spark's OS-user `owner` stamp in the fixture was replaced with a neutral value.
  - Two rebases resolved inventory rows 5/6 and the staging ledger map by keeping both sides.
- **ICE-COMMIT-UNKNOWN-1.**
  - The class name `CommitStateUnknownException` and attribute `operation_id` were fixed in the brief (D-4); see
    Q-R14-1.
  - The critic's P1 (service-managed CTAS drop on an unknown commit) was ruled in scope as a data-safety fix, following
    the existing `abort.rs` rule.
  - Glue staged-CTAS id stamping was ruled out of scope (fork change), with the docs corrected.
  - The unit's dates were corrected to 2026-09-14 by an orchestrator commit.
- **Cards and ledgers.** RP-20, ICE-GOLD-TWICE-1 and ICE-SPARK-TABLE-1 ledgers moved to `completed/` in this report PR,
  after the live proof. The assessment's C2 and D2 rows are marked PROVEN with the run id.

## Owner questions (with recommendations)

- **Q-R14-1 — the public name.** `CommitStateUnknownException` with `exc.operation_id` joined the frozen API (888 → 889
  names). **Recommendation:** keep it. It mirrors Iceberg Java's `CommitStateUnknownException` and the fork's
  `ErrorKind`, and subclasses `PySparkException`, so nothing that catches the base breaks.
- **Q-R14-2 — Glue CTAS carries no `operation_id`.** Stamping `engine.operation-id` on the staged create/replace path
  needs snapshot properties on the fork's `StagedTableTransaction`. **Recommendation:** open a small fork card
  (F-STAGED-SUMMARY-1) and a repin; low urgency, because Q-ICE-2 keeps silver on CTAS IF NOT EXISTS + MERGE, and the
  class alone is enough for the Airflow alert and retry policy.
- **Q-R14-3 — the C4 runbook step.** Spark's table cache serves the pre-RePark snapshot until `REFRESH TABLE` /
  `spark.catalog.refreshTable`, on any catalog. **Recommendation:** add a refresh after every RePark commit on tables
  Spark jobs read during the shadow and after the switch. Record it in SHADOW-1 and the C4 runbook.
- **Q-R14-4 — residues on Spark-created tables.**
  - Transform sort orders refuse the MERGE loudly (WRITE-ORDER-TRANSFORM-1).
  - RePark has no exists-fail on a Hadoop `vN` collision (fork R167).
  - `rewrite_data_files` binpack is a no-op under the min-input floor, because the options map is refused.

  **Recommendation:** confirm the production silver tables carry no transform sort order before C4 (one `SHOW CREATE
  TABLE` per table). Keep the other two as backlog, since production is Glue.
- **Q-R14-5 — fork staged-replace residues.**
  - No `CommitStateUnknown` reconciliation on the staged replace path (Glue and S3 Tables).
  - S3 Tables replace publish does not read-validate the staged metadata.

  **Recommendation:** one fork card for both, after the cutover; gold replaces are idempotent, so an unknown outcome
  there is recoverable by a re-run.
- **Q-R14-6 — `.ivy2/` is tracked in the repository** (12 files, accidentally committed in #416), so ivy redirects
  inside a clone modify tracked files. **Recommendation:** a small hygiene PR removing them and adding `.ivy2/` to
  `.gitignore`.

## Housekeeping

- `/tmp/repark-main` is shared with run 14b and was moved to a stale local `main` mid-run. Lanes read from their own
  clones from then on.
- Attribution: orchestrator commits carry `Authored-By: Claude (claude-opus-5) <noreply@anthropic.com>` — the model
  that actually wrote them (this session ran on Opus 5). The launch prompt's example named `claude-fable-5-1`.
- Cost: Grok critics $1.73 + $1.14 + $0.66 + $1.17 + $0.30 = $5.00; Devin rounds free (SWE-2).

## Pointers
- Up: [map.md](map.md) · Cards: [ice-cutover-slate-2026-09-14.md](ice-cutover-slate-2026-09-14.md)
