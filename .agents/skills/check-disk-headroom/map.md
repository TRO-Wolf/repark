# map — .agents/skills/check-disk-headroom/

## Purpose

One skill: answer "is there room to do this?" before an operation that costs tens of gigabytes,
and reclaim space safely when the answer is no. It records **measured sizes and a safe reclaim
order**, not policy — what must never be deleted is [AGENTS.md](../../../AGENTS.md)'s rule, and
what each build target produces is [DEVELOPMENT.md](../../../DEVELOPMENT.md)'s.

## Contents

- [SKILL.md](SKILL.md) — the runbook: check `df` before `du` and on the right filesystem, the
  measured consumer table (`target/debug` dominates at 72 G), how to budget for the specific
  operation rather than the repo at rest, the five-step reclaim order with what is explicitly
  **not** on it, and why leftover `/tmp/repark_ctas` (pre-A13 and the `CatalogRegistry::from`
  test helper) is exhaust rather than a cache.

## Pointers

- Up: [../map.md](../map.md)
- Related: roadmap **A13** in
  [../../../task/roadmap-intake-2026-08-21.md](../../../task/roadmap-intake-2026-08-21.md) —
  closed: `register_memory_catalog` uses the warehouse as the fallback root. §4 records the
  leftover directory.
- Authoritative: [../../../AGENTS.md](../../../AGENTS.md),
  [../../../DEVELOPMENT.md](../../../DEVELOPMENT.md)

## Debug

| Symptom | First check |
|---|---|
| A gate fails right after a large build | `df -h /` before reading the diff — a full disk fails in ways that look like a code defect |
| The measured sizes look stale | Re-measure and update §2 in the same PR; the numbers are dated on purpose |
| `/tmp/repark_ctas` is back | Pre-A13 leftover or `CatalogRegistry::from` tests — safe to delete |
