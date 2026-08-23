# Slate — the low-risk sweep (LRS)

**Design:** [../docs/design/low-risk-sweep.md](../docs/design/low-risk-sweep.md) ·
**Charter:** [../task/lrs-0-charter-ledger.md](../task/ledgers/archive/2026-08/2026-08-21-lrs-0-charter-ledger.md) ·
**Branch:** `fix/low-risk-sweep` off `feat/spark-function-parity` @ `8a28057`

**Rebased 2026-08-21:** `feat/spark-function-parity` squash-merged as [#190](https://github.com/TRO-Wolf/repark/pull/190) / `65bacdf`, whose tree is byte-identical to `8a28057`, so this branch was replayed onto `main` with zero conflicts and a byte-identical result tree. The base commit named above is the one the work was actually done on; it is unreachable from `main` post-squash, which is this repo's normal squash-merge outcome.

This is the orchestration contract. The design says *what* and *why*; this says *how the work is
run*. Both are binding.

## The one invariant

**No query that works today returns a different value tomorrow.** Every unit here changes a failure
into a better failure, an argument contract, a registry entry, or a file's location. If a unit finds
itself about to change a computed answer, it stops and the item moves to the excluded table in the
design with its reason — that is a scope decision, not a judgement call to make mid-edit.

## Per-unit contract

Each unit is one Actor–Critic cycle and one commit.

1. **Reproduce first.** The defect is measured on this tree before anything is edited, and the
   measurement goes in the ledger. A finding inherited from a Critic round is still re-measured —
   round 2 refuted two round-1 dispositions that had looked settled.
2. **Write the pin before the fix**, and watch it go red. A pin that was never red proves nothing.
3. **Fix narrowly.** Smallest readable change, existing abstractions, no refactor to make testing
   easier, no semantic-adjacent tidying while inside a sensitive path.
4. **Gate alone.** Each gate runs by itself and its own `$?` is captured immediately. Never
   `cmd | grep | head` — that reports the exit status of `head`, which is how a 45-binary run once
   got read as green from 30 of its result lines, and how a passing suite once reported red.
5. **`map.md` lockstep** in the same commit as the code. The pre-commit hook enforces it; do not
   discover it at commit time.
6. **Ledger** at `task/lrs-<n>-<name>-ledger.md`, indexed in `task/map.md` in the same commit.

## The testing contract, restated because it is a hard block

From [../docs/testing.md](../docs/testing.md), and it applies here without exception:

- A test asserts a **measured** value, never one read back out of the code under test.
- An oracle is **independent** — PySpark's own source, Python's `re`, a closed form, the other door.
  Repark agreeing with repark is not evidence.
- A test that cannot check a row **fails on that row**. It does not `continue`, skip, or narrow its
  own domain to stay green. The C-012 ratchet failed exactly this way and reported green over half
  its table.
- A ratchet **ratchets down**. Adding a row to a sanctioned-out list to make a test pass is a
  decision that gets made in the ledger, in the same commit, with a reason — not a convenience.

## Disk discipline

Per AGENTS.md "Resource discipline". This branch builds Rust and rebuilds the wheel repeatedly.

- `df -h .` **before** the first build of a session, and again at each unit boundary and before any
  broad validation (`make preflight`, the full facade suite).
- Working baseline measured at open: **446G free, `target/` at 63G.** If free space drops below
  ~100G, stop and reclaim before the next artifact-heavy command.
- Reclaim only **task-owned** artifacts. Never another task's worktree, never uncommitted files.
- The shared `target/` and the shared cargo registry are used as-is — no per-unit worktree, so no
  duplicated build artifacts. That is the cheapest correct choice here because the units are
  sequential and none of them conflict.
- Report the checks, anything reclaimed, and anything retained with the reason, at handoff.

## Sequencing

LRS-5 → LRS-1 → LRS-2 → LRS-3 → LRS-4. The reason is in design §4: LRS-5 is pure motion, so doing it
first means no later unit edits a file that is about to move. LRS-4 is last because it is the only
unit whose scope is not fully known until it runs.

Units are sequential and single-agent in the main thread. No fan-out unless the owner asks for it.

## Unit notes that are easy to get wrong

- **LRS-1, the value-argument guard.** Putting the walk on the assembled `HigherOrderFunction`
  covers both positions by construction; putting it on `value_args` covers only the position that
  was measured. Prefer the former, and pin **both** shapes either way.
- **LRS-1, the SQL-text paths.** `cube` / `rollup` / join conditions fail because a `Column` is
  lowered to SQL text the Spark dialect cannot read back. The refusal belongs at the facade, before
  the text is built — refusing after the parser has already failed just reformats the same error.
- **LRS-2, `xxhash64()`.** Check what PySpark actually returns for the zero-argument form before
  choosing between accepting it and refusing by name. Do not infer it from the signature.
- **LRS-3, the alias.** `with_aliases` overwrites by name in DataFusion's registry, and
  `register_all` is order-sensitive — the comment at the top of that function says why. Add the
  alias where `percentile_approx` is added, not somewhere new.
- **LRS-4.** Widening the domain will surface divergences nobody has adjudicated. Each is closed or
  registered **with a reason**. If the count is large, ship the measurement and the widened guard
  behind justified rows, and hand the rest forward — do not stuff the sanctioned-out table.
- **LRS-5.** `foo.rs` may sit beside `foo/` in Rust 2018; nothing needs renaming to `mod.rs`, and
  the crate-root ceilings must come out unchanged. If a move would push a file over its ceiling,
  that is a signal to stop, not to edit the ceiling.

## Delivery

Manual PR. The orchestrator prepares the branch, runs its own review, and summarizes. **The owner
merges.** Nothing is pushed without being asked.
