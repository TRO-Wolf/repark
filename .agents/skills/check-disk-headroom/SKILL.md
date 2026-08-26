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

### Measured again 2026-08-25 — the machine, not just the repo

A full-disk sweep found the largest consumers sat **outside** the repo's `target/`:

| Path | Size | What it is |
|---|---:|---|
| `timeshift` snapshots | **840 G** | rsync snapshots on the **same** partition; its config includes `~` (`/home/<user>/**`), so every `cargo target/` is snapshotted |
| fork workspace `target/` | 207 G | the second `iceberg-rust` checkout (deps 161 G + incremental 45 G) |
| per-unit worktree `target/` | 23–51 G each | one debug tree per in-flight SEPMO unit worktree |
| `Trash` | 12 G | desktop trash |
| systemd coredumps | 4 G | `/var/lib/systemd/coredump` |
| 24 stale kernels | ≈ 6.8 G | across `/boot` + `/lib/modules` |

The lesson: `du -sh target` answers the repo's cost; a full disk needs the machine's. Timeshift
covering `~` means every build tree is counted a second time in its snapshots.

## 3. Reclaim order — cheapest and safest first

Work down this list, re-checking `df -h /` after each step. Stop as soon as you have room.

1. **Merged-unit worktree `target/` trees** — a unit whose PR merged no longer needs its
   worktree's debug tree; **151 G was freed this way on 2026-08-25**, after a read-only refute
   pass. Confirm the unit merged first, and never touch a worktree still in flight — its
   uncommitted state is someone else's unit, not garbage (AGENTS.md "Resource discipline").
2. **`/tmp/repark_ctas`** — free, and it should not exist (§4). Nothing live depends on it.
3. **Session scratch directories** — the fixture warehouses and logs written under the session's
   scratchpad. Yours to delete, but **refute first** (Gotchas): a scratch directory can hold the
   only copy of ledger-cited evidence. Regenerating fixtures costs a Spark run.
4. **`cargo clean -p <crate>`** — drops one crate's artifacts and keeps the rest of the
   incremental tree. Far cheaper to recover from than a full clean.
5. **`target/release`** — 2 G, and only rebuilt when a release or wheel build needs it.
6. **`cargo clean`** — the whole debug tree. Frees the most, costs a full rebuild. Say so before
   doing it, because the next gate run will be slow.

**Not on this list, and not to be deleted casually:** `~/.cargo/registry` (shared with every other
project on the machine, and re-downloading needs the network), the Spark oracles (a reinstall is a
long JVM-toolchain setup, and the 4.1.2 one is pinned), and `.venv` (cheap to rebuild but every
facade gate depends on it, so rebuild it in the same breath).

## 4. `/tmp/repark_ctas` is a bug's exhaust, not a cache

Until A13, tables created in a namespace with no `location` property fell back to
`<temp_dir>/repark_ctas/<catalog>/<namespace>/<table>` (`ctas.rs::resolve_create_location`).
That path was keyed by **names alone**, so every session that used `mem.ns.events` wrote into
one shared directory. Measured 2026-08-21: **2.7 G across 22 catalog names**.

**A13** (`register_memory_catalog`) sets the fallback root to the supplied warehouse, so new
writes land at `<warehouse>/repark_ctas/…`. A leftover `/tmp/repark_ctas` from older sessions
and from the `CatalogRegistry::from` test helper (no warehouse argument) may still exist. It is
safe to delete in full.

`remove_orphan_files` still refuses a table sitting under the catalog's fallback root — two
processes that pass the same warehouse and the same names still share a directory, and
memory-catalog metadata is process-local.

Prefer an explicit namespace `LOCATION` in tests and scratch work so the table owns its
directory.

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
- **Refute a scratch directory before `rm -rf`.** A scratch directory can hold the **only** copy
  of evidence a committed ledger cites — the MW-6 Critic files were in session scratch after their
  ledger had already archived; a blind delete would have stranded the citations. Their durable
  home is now `task/mw-6-critic-evidence/`. Read what a scratch tree holds before removing it.
- **`sudo`-tier reclaim is owner-run.** The kernels, the journal, the coredumps and the timeshift
  snapshots in §2 need root and touch machine state outside any worktree — hand them to the owner
  with the measured sizes; do not run them from an agent session.
