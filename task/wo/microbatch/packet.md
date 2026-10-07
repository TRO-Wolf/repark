# The micro-batch packet — decisions, order, fork asks, open questions (2026-10-05)

> **North Star briefing (owner, 2026-10-05).** Every executor and verifier on this order reads [the CDC and micro-batch North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) first: NS-1…NS-19, the authority order in §2 (Flink governs guarantees, Spark the surface, Iceberg's own sinks the commits, else refuse with a dated row), the Rust placement in §4, and the production-grade lists in §5. The North Star sits below every ruled O/D/CC/ES/CL row and above the agent's judgment. For a question no ruled row answers, write the four lines (the question, Flink's answer, Spark's answer, the NS default), act on the default, and record the four lines as a dated ledger row. Halt only for the three §8 cases.


The plan is
[task/roadmap/epic-term/microbatch-cdc-sink-plan-2026-10-04.md](../../roadmap/epic-term/microbatch-cdc-sink-plan-2026-10-04.md)
(§3 D-1…D-8, §5 slices, §6 waves, §7 O-1…O-10, §8 order, §9 rules, §10
backfill, §11 moves). The O-9a amendment and the two fork cards are on branch
`docs/microbatch-fork-cards-2026-10-05` (PR #953) until that PR merges. The
v1.6.0 card's step 0, which MB-0 records, is
[task/roadmap/mid-term/ice-streaming-1-6.md](../../roadmap/mid-term/ice-streaming-1-6.md).
The contracts are CC-1, CC-6 and CC-9 in
[task/roadmap/epic-term/contracts-ahead-of-code-2026-10-01.md](../../roadmap/epic-term/contracts-ahead-of-code-2026-10-01.md).
Every Spark claim below is cited from the plan or marked "to record in MB-0".

## 1. The design, one line per decision

- **D-1** (plan §3): offsets live in the Silver snapshot summary plus the same
  vector as a table property in the same commit; no checkpoint directory for
  an Iceberg sink; the `checkpointLocation` accept-or-refuse is decided by
  the oracle — to record in MB-0.
- **D-2** (plan §3): a batch source over the append scan splits oversized
  windows per Spark's max-files / max-rows options and fails loud on an
  overwrite or delete snapshot in the window — error classes to record in
  MB-0.
- **D-3** (plan §3): a Session-owned tokio driver task, one batch in flight,
  `availableNow` and `processingTime` triggers, under CC-1's four rules.
- **D-4** (plan §3): idempotent replay — the epoch check before commit, the
  silver-s0 C-008 walk on unknown outcome, OCC fencing for accidental second
  drivers.
- **D-5** (plan §3): deterministic event id and version id (UUIDv5/v8 over
  canonical typed bytes) computed by a vectorised Arrow kernel; the Bronze
  contract carries them now; pipeline-minted values never feed the id.
- **D-6** (plan §3): catalog-only state; a shared catalog is required and a
  local filesystem catalog refuses a streaming query.
- **D-7** (plan §3): two transform doors, `foreachBatch` first and the
  declarative `SilverPlan` second; vector offsets for multi-input Silver.
- **D-8** (plan §3): the facade — `readStream.format("iceberg")`,
  `writeStream`, `trigger`, `foreachBatch`, `StreamingQuery`; Rust first,
  Python forwards builders.

## 2. The rulings O-1…O-10 plus O-9a

| id | decision | status |
|---|---|---|
| O-1 | offsets in the Silver snapshot summary and a table property; no checkpoint directory for an Iceberg sink | **ruled 2026-10-04** (owner: "Go with recommendations") |
| O-2 | the event id and version id enter the Bronze contract now, before the capture producer; existing tables take the §10 backfill | **ruled 2026-10-04** (owner: "Go with recommendations") |
| O-3 | catalog-only state; no control database before multi-writer; a shared catalog is required and a local filesystem catalog refuses | **ruled 2026-10-04** (owner: "Catalog only") |
| O-4 | release slot 1.7 beside `repark-cdc` | **ruled 2026-10-04** (owner: "Sink waits for 1.6", confirmed on the question) |
| O-5 | a Bronze table that receives row-level deletes: refuse; Bronze is append-only by contract; a delete or overwrite snapshot inside a window is a contract violation; no opt-in skip | **ruled 2026-10-04** (owner: "No deletes in bronze") |
| O-6 | a genuinely keyless source table: refuse with a dated registry row; a surrogate rule is a demand-triggered card | **ruled 2026-10-04** (owner: "Go with recommendations") |
| O-7 | the Opus slot order (STAMP-2 re-measure, then the TA single-series sketch, then the micro-batch sketch): keep the order | **ruled 2026-10-04** (owner: "TA first") |
| O-8 | a second Opus lane for this build: no; one Opus orchestrator slot for every unit | **ruled 2026-10-04** (owner: "Keep everything in one opus orc slot") |
| O-9 | the 1.7 sink half waits for the 1.6 connectors | **ruled 2026-10-04** (owner: "Sink waits for 1.6") |
| O-10 | an HTML artifact copy under `docs/artifacts/` in the ruling change | **ruled 2026-10-04** (owner: default) |
| O-9a | O-9 amended: the sink slices no longer wait for C-1 | **ruled 2026-10-05** (owner): "the sink slices no longer wait for C-1. After the micro-batch design sketch in the Opus slot, MB-1 and MB-2a may run on Muse in parallel with C-1. Count lanes against the cap of four before every launch. Release slot stays 1.7 beside repark-cdc; packaging order is unchanged." The same day: the packet PR is docs-only with no Opus verifier; the two fork requests file today; MB-0 runs on Muse after the bisect timing run |

Under O-5 the fork card's pair of skip opt-ins stays fork-side only: RePark
exposes no skip for a Bronze stream, and the default refuses both kinds.

## 3. Merge order and waves as amended by O-9a

packet → MB-0 (after the bisect timing run) → the micro-batch design sketch
in the Opus slot → MB-1 and MB-2a on Muse beside C-1, under the cap of four
lanes → the harness → MB-2c → MB-3 → MB-4 → MB-5 → acceptance.

The sketch fixes names and signatures (the plan's *Contracts* content), so no
separate contracts PR opens; every slice builds against the sketch. The fork
PR (a separate repository, free parallelism) and the RP-N pin bump merge
before the slice that consumes each ask: the F-APPEND pin before MB-1, the
F-COMMIT pin before MB-2c. The *Identity* slice and *Acceptance* keep their
plan §5 shape and run outside this packet's orders. A lane that finishes early
waits in the queue rather than rebasing repeatedly.

## 4. The fork asks (filed 2026-10-05, owner: "Fork requests file today")

- **F-APPEND-WINDOW-FAILLOUD-1**
  (`task/roadmap/mid-term/f-append-window-failloud-1-2026-10-05.md` on PR #953):
  an opt-in fail-loud mode on `IncrementalAppendScan` for a window holding a
  non-append snapshot. Default `false`; batch behaviour unchanged. Consumer:
  MB-1. Whether a `replace` snapshot counts as non-append follows MB-0's
  Spark cell — to record in MB-0.
- **F-COMMIT-OFFSET-PROPERTY-1**
  (`task/roadmap/mid-term/f-commit-offset-property-1-2026-10-05.md` on PR #953):
  prove the combined commit (row delta or overwrite plus the property update
  in one transaction) under retry, fix it only if it fails, and measure the
  same-key race. Consumers: MB-2a and MB-2c. Measurement (3) decides where
  the same-key protection lives (open question Q8 below).
- **F-APPEND-PIN-BASE-1**
  ([`task/roadmap/mid-term/f-append-pin-base-1-2026-10-07.md`](../../roadmap/mid-term/f-append-pin-base-1-2026-10-07.md),
  filed 2026-10-07 under OQ-4 after DM-6 failed, ruling Q1; revised in MB-2c
  fold 1): `fast_append` and `merge_append` gain `validate_from_snapshot` and
  `validate_no_concurrent_snapshot_with_summary(key, value)`, default off, so
  a pinned append fails non-retryably when a newer snapshot on the ref carries
  this query's `repark.cdc.query-id`, and unrelated writers still land; the
  operation stays `append`. The validation sits on the append action itself
  (DM-6 C-001 (d), A′ rejected).
  Consumer: MB-2c's closing slice (step 3, the fence; harness pins 2 and 3;
  the 5-passed gate). The owner's fork work.

## 5. Open decisions the design sketch must close

Each is phrased as a question. The sketch answers from MB-0's recorded cells,
never from documentation.

- **Q1.** For an Iceberg sink, does the facade accept `checkpointLocation`
  and ignore it, or refuse it — and with which error class?
- **Q2.** Is the `Once` trigger supported or refused — and with which error
  class? (D-3 names `availableNow` and `processingTime` only.)
- **Q3.** Is `complete` output mode supported on an Iceberg sink, or refused —
  and with which error class?
- **Q4.** Does a `replace` (rewrite/compaction) snapshot inside a window fail
  loud or skip silently? (O-5 rules delete/overwrite only; MB-0's Spark cell
  decides.)
- ~~**Q5.**~~ **Ruled (owner, 2026-10-05):** a query always starts from the sink's
  offsets, and a Spark checkpoint is never parsed. Migration is a one-time explicit
  start-snapshot option on the first run, documented as a dated difference row.
- **Q6.** What are the exact snapshot-summary key names, the table-property
  key, and the offset encoding — single input and vector form?
- **Q7.** What are the `StreamingQuery` states, the progress shape, and the
  `awaitTermination` / `stop` semantics under CC-1's four rules?
- **Q8.** Where does the same-key property-race protection live: a fork-side
  typed conflict error, or RePark-side generation fencing in MB-2c?
  (Pending the F-COMMIT measurement (3).)
- **Q9.** What is the `foreachBatch` callable contract — its arguments, its
  failure and retry semantics, and what the callable observes per batch?
- ~~**Q10.**~~ **Ruled (owner, 2026-10-05):** the slices carry no maintenance hooks.
  The driver never runs expiry or compaction implicitly. The only thing shipped is the
  guard that offsets are read from the current snapshot's summary. Driver-scheduled
  maintenance is a later card.
- ~~**Q11.**~~ **Ruled (owner, 2026-10-05):** the Iceberg source is the product feature.
  The rate source is a test-only helper, outside the public surface. A file source is a
  later, dated card.

**Sequencing (owner, 2026-10-05):** the design sketch runs after MB-0's recordings
land.

**Q8 input (fork #366, merged `267370b7`):** a retried commit silently clobbers a
same-key property set by a racer. No `TableRequirement` can assert a property value,
so the protection lives in RePark: MB-2c's generation fencing (CC-9).
