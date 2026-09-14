# Overnight report — run 12b of 2026-09-13 (cache and expressions)

**Session:** one Opus orchestrator (overnight-12b), 2026-09-13 13:33 → 20:34 local, beside run 12 (overnight-12, f- lanes).
**Grants:** G-1 (squash-merge on green), G-2 (bounded decisions), G-3 (stop when the list is exhausted or by 21:30),
G-4 **Devin** actor, **Grok** critic-logic and S2-21 reviewers (Grok actor only as fallback; no Muse, no GLM), G-5 yes
(card authoring for the two residue units). STATUS.md, tags and the release pipeline untouched. **Procedure:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md).

## 1. What landed

| Unit | PR | Merged | Rounds |
|---|---|---|---|
| EAGER-BUDGET-1: `repark.cache.max_total_bytes` refuses (never evicts), distinct-buffer `repark.cache.retained_bytes`, incremental admission | #569 | `30d57ef9` | Devin 5 (step 0, step 1, step 1 recovery after an OOM kill, step 2, review round; one session, $0), Grok critic-logic 1 ($0.89), Grok S2-21 Rust+Python perf reviewer 1 ($1.28) |
| REPLACE-LINEAR-1 step 0: card, 26 live PySpark 4.1.2 oracle cells, red-first 40-entry memory pin; **step 1 parked on Q-R1** | #571 | `cb614235` | Devin 1 (step 0, $0) |

Cards filed: REPLACE-LINEAR-1 (#571), ARRAY-NULL-1 and ANSI-DOOR-1 (this PR). Nothing else is parked.

## 2. EAGER-BUDGET-1

### Step 0: retention under a memory cap (release natives, 1e6 × 25-column TA fixture, 30 iterations)

Base = EAGER-OWN-1's base `8936346a` (bare `eager()` leaks); main = `acc9b550` (bare `eager()` releases; "retained"
appends each result to a list). `systemd-run --scope -p MemoryMax=<cap> -p MemorySwapMax=0`. Raw JSON:
`docs/perf/eager-budget-1-2026-09-13/`.

| cap | cell | outcome | last iter | VmHWM | registrations |
|---|---|---|---|---|---|
| 2G | base, bare | OOM kill | 5 | 2,025 MB | 6 |
| 2G | main, bare | completed | 29 | 1,384 MB | 0 |
| 2G | main, retained | OOM kill | 5 | 2,051 MB | 6 |
| 4G | base, bare | OOM kill | 16 | 4,139 MB | 17 |
| 4G | main, bare | completed | 29 | 1,361 MB | 0 |
| 4G | main, retained | OOM kill | 15 | 4,054 MB | 16 |
| 8G | base, bare | completed | 29 | 6,695 MB | 30 leaked |
| 8G | main, bare | completed | 29 | 1,369 MB | 0 |
| 8G | main, retained | completed | 29 | 6,767 MB | 30 held |

**The owner's per-call slowdown does not reproduce under any cap.** Completed cells stay flat within noise, and
`ru_majflt` is 0 in every cell (swap off). With too much retained, the process gets OOM-killed rather than slowing down.
The only slow iterations were the last two before the 2 GB base kill (0.80 → 2.39 s, reclaim at the cap edge). Main
with retained results died at the same iteration with flat wall time. Wall time differs across cells (0.8–2.9 s) with
box load, so only the shape within a cell is comparable. Retained-buffer probe on one result: `Table.nbytes`
199,008,178 vs a distinct-`buffer.address` sum of 188,196,432 (385 buffers, 0.9457): per-result sums double-count about
5.4 %.

### Before and after

| Reading | Before (`main` at `9efb6a65`) | After (`30d57ef9`) |
|---|---|---|
| session-wide cache budget | none | `repark.cache.max_total_bytes`: refuses, never evicts |
| readable retained bytes | none (RSS only; allocator keeps freed arenas) | `spark.conf.get("repark.cache.retained_bytes")`, distinct buffers, read-only |
| when `max_bytes` is checked | after the full `collect()` | per batch while streaming; same measure, message and boundary as before (312 / 684,576-byte cells identical) |
| refused budget, one partition, 1e6 × 7 | n/a | refused after 25 ms with 87 MB HWM growth, vs a full collect's 68 ms and 131 MB (reviewer) |
| unbudgeted `cache()` / `eager()` cost | baseline | within noise (1e6 × 25 admit 56.3 ms unbudgeted vs 53.0 ms budgeted) |
| `getAll` with 100 live views | baseline | +56 µs (one native walk) |
| registration after a refusal or mid-collection failure | n/a | none, and no handle (pinned on `cache()` and `eager()`) |

### Reviews

- **Grok critic-logic** (34 turns, $0.89): NEEDS_REMEDIATION, 0 P1, 3 P2, 1 P3. L-001 `max_bytes` used a different
  measure from main and admitted where main refused; L-002 with both budgets set, `max_bytes` skipped bytes shared with
  live views; L-003 non-lowercase budget keys were ignored at materialize. All three were fixed in one Devin round and
  re-measured by the orchestrator against main's release native. L-004 (SQL `SET repark.cache.*` fails with DataFusion's
  namespace error) is pinned as-is. Checked and held: the D-2 walk (nested types, prefix collisions, released handles,
  shared views), retry after a refusal, and mid-stream failure; five scratch mutations turned their pins red.
- **Grok S2-21 Rust + Python perf reviewer** (41 turns, $1.28): PASS, no P1, no P2. P3s filed in the ledger:
  `to_data()` rebuilds `ArrayData` when budgeted; retained-byte walks are O(live buffers) per budgeted admission and per
  `getAll`; multi-partition prefetch before a refusal lands.
- `make preflight` caught one more thing: the `test_dfcore_1_exports.py` snapshot of `core`'s imported names (fixed as a
  declared delta).

## 3. REPLACE-LINEAR-1

Card authored under G-5 and filed on the unit's own PR (#571). **Step 0 landed; step 1 is parked on owner question
Q-R1.** Devin ran one round ($0): the card, the live PySpark 4.1.2 oracle (one `local[1]` session, ANSI on, 26 cells),
today's repark answers beside them, and the red-first memory pin (armed only via `REPARK_REPLACE_LINEAR_1_MEM=1` until
the rewrite).

### Memory (base tree, debug native, `RLIMIT_AS = VmSize_at_apply + 3 × 8 GB`)

| dict entries | RSS delta |
|---|---|
| 4 | 3.2 MB |
| 8 | 3.2 MB |
| 12 | 21.9 MB |
| 14 | 77.6 MB |
| 16 | 277.1 MB |
| flat 40-column select (control) | 1.7 MB |

At 40 entries the worker dies under the cap: `case_when` panics with `PyObject pointer is null`, the same shape
ABS-EXPR-1 recorded. After the rewrite the target is one searched CASE per column, linear in entries.

### Oracle vs today

16 cells match, including NULL values, NaN keys, int keys on double columns, every subset shape, backticked names and
`{}`. The expected D-3 cell differs: `{1: 2, 2: 3}` is `[2, 3, 3]` on Spark (simultaneous) and `[3, 3, 3]` on repark
(sequential). **Nine more cells diverge**, which halted the round under D-3:

| cell | PySpark 4.1.2 | repark today |
|---|---|---|
| `replace({None: 5})` | refuses `MIXED_TYPE_REPLACEMENT` | silent no-op |
| `replace(None, 5)` | refuses `NOT_BOOL_OR_DICT_…` | silent no-op |
| `replace([1, 2], [3, 4])` | `[3, 4, 3]` | `TypeError: unhashable type: 'list'` |
| `replace([1, 2], 9)` | `[9, 9, 3]` | `TypeError` |
| `replace("a", "b")` over string + int columns | string column replaced, int untouched | engine cast error at collect |
| `replace(1, 2)` on a boolean column | unchanged | `type_coercion` error at collect |
| `replace(1, 2.5)` on an int column | `[2, 2, None]` int | `[2.5, 2.0, None]` double |
| `replace(1, 9, subset="missing")` | refuses `UNRESOLVED_COLUMN` | silent no-op |
| `replace("a", "b", ["x"])` on an int column | unchanged | engine cast error at collect |

Three more cells refuse on both sides with a different error class. The pins hold today's answers for the divergent
cells and the oracle's for the matching ones, so step 1 flips them explicitly.

Gates at the pushed head (before the rebase onto #569): `make preflight` rc 0 (facade 6025 passed), parity suite 757
passed / 12 xfailed, replace pins 11 passed / 1 skipped, `make verify` rc 0; after the rebase, `make develop`,
the replace + budget pins and `make verify` re-ran before the merge.

## 4. Decisions under §6

- R12b-D-1 The D-2 readback is the conf key `repark.cache.retained_bytes` (computed, read-only). The card allowed a
  catalog method or a conf readback; a conf key adds no class method, so the EX-0 public-surface count stays put.
- R12b-D-2 Step 0's slowdown claim stays unreproduced as measured. Under a 2/4/8 GB cgroup cap retention ends in an
  OOM kill; wall time per call is flat in every completed cell and `ru_majflt` is 0.
- R12b-D-3 The conf intercepts live in `builder_conf.RuntimeConfig` and `session_configuration.py` (the real conf
  seam), outside the card's first Home list.
- R12b-D-4 `repark.cache.max_bytes` keeps main's measure: this result's `get_array_memory_size` sum, per result, message
  byte-identical (critic L-001/L-002).
- R12b-D-5 Both cache budget keys resolve case-insensitively, like `repark.cache.retained_bytes` (critic L-003).
- R12b-D-6 The S2-21 Rust and Python perf questions ran as one Grok reviewer beside one Grok critic (run 9b's shape).
- R12b-D-7 Orchestrator fix commits on the unit branch, each mechanical: removed reworded `///` lines (comment ban) and
  the multi-line docstring Devin added to `_cache_conf_lookup` (its rule moved to the dataframe map); ratcheted
  `repark-python/src/session.rs` 1128 → 1127; corrected note dates; declared the `_dfcore_1_expected.py` export delta
  (`core` re-imports only `_resolve_cache_budgets`); stripped the co-author message trailers naming Devin that the pre-push
  hook bans (tree unchanged).

## 5. Harness lessons

- A Devin unit capped at `MemoryMax=32G` was OOM-killed inside its own `make verify` 40 minutes in. The launcher dies
  with it, so no `exit` file and no `runs.tsv` row appear, and an exit-file wait never ends. Rust rounds now run at 64 G
  with `CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8`, and the wait checks the unit's journal for `oom-kill`.
- Devin CLI adds a co-author trailer naming its GitHub bot account to some commit messages, and one of its commits
  had no `Authored-By:` trailer. The pre-push scan covers messages; the audit now does too.
- `make preflight` caught what the actor's gates did not: the `test_dfcore_1_exports.py` snapshot of `core`'s names.

## 6. Owner questions — recommendations

- **Q-B1 (the readback name)** `repark.cache.retained_bytes` as a read-only conf key, or a `spark.catalog` method? —
  **Recommend the conf key (as built).** It reads like the two budget keys beside it and adds no method to a Spark class.
- **Q-B2 (multi-partition prefetch)** A budgeted refusal on a multi-partition plan still sees ~90 MB of batches
  prefetched through `CoalescePartitionsExec` before the refusal lands. — **Recommend leave as is**; the refusal still
  comes before the full peak, and bounding channel prefetch is a DataFusion executor change for its own card.
- **Q-R1 (REPLACE-LINEAR-1 step 1 scope)** The instruction said the rewrite keeps today's semantics, but today differs
  from Spark in nine cells: three silent no-ops where Spark refuses, a silent int → double widening, list
  `to_replace` crashing, and engine errors where Spark leaves a column untouched. Should step 1 bring all nine to
  Spark's answers? — **Recommend yes, in the same rewrite.** A per-column searched CASE already has to decide key/column
  type compatibility and value casting, so matching Spark is nearly free; keeping today's answers would mean rebuilding
  silent wrong answers on purpose. Resume with Devin on `task/ledgers/staging/replace-linear-1-ledger.md` step 1.
- **Q-R2 (order after Q-R1)** ARRAY-NULL-1 and ANSI-DOOR-1 are filed as cards only; neither was started. — **Recommend
  ARRAY-NULL-1 next** (a user-reachable exponential plan, same shape as REPLACE), then ANSI-DOOR-1 (door-only
  divergence, with the facade already correct).

## 7. Cards filed

- [replace-linear-1-card-2026-09-13.md](replace-linear-1-card-2026-09-13.md): on PR #571 (step 0 landed, step 1 parked on Q-R1).
- [array-null-1-card-2026-09-13.md](array-null-1-card-2026-09-13.md): on this PR; not started (the list ran out of time).
- [ansi-door-1-card-2026-09-13.md](ansi-door-1-card-2026-09-13.md): on this PR, card only as instructed.
- [eager-budget-1-card-2026-09-13.md](eager-budget-1-card-2026-09-13.md): the seeded card with the owner's Q-E2 ruling folded in, on #569.
