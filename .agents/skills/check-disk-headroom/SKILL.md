---
name: check-disk-headroom
description: >-
  Check free disk before starting an operation that consumes tens of
  gigabytes, and reclaim space safely when it is short. Use this skill before
  fork work (a second iceberg-rust checkout plus a full rebuild), before a
  clean release or wheel build, before generating Spark oracle fixtures, and
  whenever a build dies with "No space left on device" or a gate fails for no
  reason the diff explains. It names this repo's real consumers with measured
  sizes, what is safe to delete versus what is expensive or destructive to
  delete, and the reclaim order to work through. Do NOT use it for ordinary
  incremental builds, which need no headroom check.
---

# Skill: check-disk-headroom — know the cost before you pay it

An agent-facing runbook for one question: *is there room to do this?* It records measured sizes
and a safe reclaim order; it defines no policy. On any conflict the spine wins —
[AGENTS.md](../../../AGENTS.md) (hard rules, especially what must never be deleted) and
[DEVELOPMENT.md](../../../DEVELOPMENT.md) (what each build target actually produces).

**Deleting is a real action.** Everything in §3 is reversible by rebuilding, and nothing in it
touches the repository, a catalog, or cloud state. Anything outside that list gets confirmed with
the owner first. AGENTS.md's rule stands unchanged: never drop a Glue table, an S3 Tables table,
or S3 data.

## 1. Check first, and check the right filesystem

```bash
df -h /            # the partition that holds the repo, ~/.cargo and /tmp on this box
du -sh target      # almost always the answer
```

On this machine `/`, `/home` and `/tmp` are the **same** partition (`/dev/nvme1n1p1`), so freeing
`/tmp` genuinely helps a build. Do not assume that elsewhere — check `df` output for the mount
each path resolves to before planning a reclaim.

## 2. What actually costs, measured 2026-08-21

| Path | Size | What it is |
|---|---:|---|
| `target/debug` | **72 G** | the incremental debug tree — the dominant consumer, by an order of magnitude |
| `target/release` | 2.0 G | release artifacts |
| `~/.cargo/registry` | 2.6 G | downloaded crate sources and caches, shared across every project |
| `/tmp/repark_ctas` | 2.7 G | **a leak, not a cache** — see §4 |
| `.venv` | 1.1 G | the facade virtualenv (`uv sync` + maturin develop) |
| Spark oracles | ~1.1 G | `c26-oracle` (4.1.2) + `spark40-oracle` (4.0.1), each with a JVM toolchain |
| `~/.cargo/git` | 207 M | git checkouts including the owned `iceberg-rust` fork |

**Budget for the operation you are about to run**, not for the repo at rest:

- **A full clean rebuild** of this workspace regenerates most of `target/` — budget on the order
  of the debug tree above, and expect it to take a long time. This is why `cargo clean` is a last
  resort rather than a first move.
- **Fork work** (a second `iceberg-rust` checkout built against this workspace) costs its own
  `target/` on top of the repo's. Two debug trees is the shape to plan for, which is what makes
  this check worth running *before* starting rather than after.
- **Spark oracle fixtures** are small per fixture (megabytes) but each one is a fresh warehouse
  directory that nothing cleans up. They add up across a session.

## 3. Reclaim order — cheapest and safest first

Work down this list, re-checking `df -h /` after each step. Stop as soon as you have room.

1. **`/tmp/repark_ctas`** — free, and it should not exist (§4). Nothing live depends on it.
2. **Session scratch directories** — the fixture warehouses and logs written under the session's
   scratchpad. Yours to delete; regenerating them costs a Spark run.
3. **`cargo clean -p <crate>`** — drops one crate's artifacts and keeps the rest of the
   incremental tree. Far cheaper to recover from than a full clean.
4. **`target/release`** — 2 G, and only rebuilt when a release or wheel build needs it.
5. **`cargo clean`** — the whole debug tree. Frees the most, costs a full rebuild. Say so before
   doing it, because the next gate run will be slow.

**Not on this list, and not to be deleted casually:** `~/.cargo/registry` (shared with every other
project on the machine, and re-downloading needs the network), the Spark oracles (a reinstall is a
long JVM-toolchain setup, and the 4.1.2 one is pinned), and `.venv` (cheap to rebuild but every
facade gate depends on it, so rebuild it in the same breath).

## 4. `/tmp/repark_ctas` is a bug's exhaust, not a cache

Tables created in a namespace with no `location` property fall back to
`<temp_dir>/repark_ctas/<catalog>/<namespace>/<table>` (`ctas.rs::resolve_create_location`, the
E-4 fallback). That path is keyed by **names alone** — nothing process-specific — so every session
that ever used `mem.ns.events` wrote into one shared directory.

Measured 2026-08-21: **2.7 G across 22 catalog names**, `mem` alone holding 1.9 G, and one table
directory holding 139,179 files. It is safe to delete in full, and it will come back.

Two consequences worth knowing rather than rediscovering:

- **`remove_orphan_files` refuses to run on a table sitting under that root.** One session's
  "orphan" is another session's live file, and that procedure has no undo. The refusal is
  deliberate (MW-3); it names the hazard and tells the caller to create the namespace with an
  explicit `LOCATION`.
- **The underlying behaviour is open as roadmap A13**
  ([task/roadmap-intake-2026-08-21.md](../../../task/roadmap-intake-2026-08-21.md)) — a warehouse
  argument that is silently ignored. Until that closes, this directory regrows.

Avoid feeding it: create namespaces with an explicit `LOCATION` in tests and scratch work.

## Gotchas

- **Check before, not after.** A build that dies on `No space left on device` can leave a
  partially written `target/`, and the next build may fail in ways that look like a code defect.
  If a gate fails inexplicably right after a big operation, check `df` before reading the diff.
- **`du -sh target` on a 72 G tree takes a while.** `df -h /` answers "is there room" instantly;
  reach for `du` only once you know you need to reclaim.
- **Freeing space under `/tmp` only helps if `/tmp` shares the partition.** On this box it does.
  Confirm rather than assume.
- **A clean tree is not free.** `cargo clean` trades minutes of disk for a long rebuild on the
  next gate run. Prefer `cargo clean -p <crate>` when one crate is the problem.
