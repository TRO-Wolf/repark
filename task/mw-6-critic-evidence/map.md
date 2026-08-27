# map — task/mw-6-critic-evidence/

## Purpose

The home of the Critic-round evidence the **MW-6** archived ledger cites by path. The ledger moved
to the immutable archive on 2026-08-24 while these files still lived only in a session scratch
directory (PROC-1, 2026-08-25); a `rm -rf` of that scratch would have stranded a committed
ledger's citations. This directory is that evidence's durable home. **Recorded evidence, never
hand-edited** — it is excluded from `ruff` (`pyproject.toml` `[tool.ruff] extend-exclude`) and from
`typos` (`.typos.toml` `extend-exclude`) for the same reason `task/census/` is: a linter would
rewrite verbatim oracle output.

The citing ledger is
[../ledgers/archive/2026-08/2026-08-24-mw-6-rewrite-manifests-ledger.md](../ledgers/archive/2026-08/2026-08-24-mw-6-rewrite-manifests-ledger.md)
(immutable — the DL-1 rule; nothing here rewrites it).

## Contents

The `COVERAGE_ATTESTATION` rows of the citing ledger name each file:

- `test_critic_shapes.py` — the Critic's shape probes (`rewrite_manifests` counts, argument
  refusals, post-`expire_snapshots` and post-`add_column` cases). Ledger lines 221, 230, 239, 257,
  285 (named tests: `test_h_nonexistent_table`, `test_k_argument_refusals`,
  `test_i_rewrite_merge_rewrite`, `test_c_after_expire_snapshots`, `test_l_exactly_one_delete_manifest`,
  `test_d_after_add_column`, `test_a_two_data_one_delete`).
- `test_critic_shapes2.py`, `test_critic_bytes.py` — the second shape batch and the byte-count
  probe. Ledger line 221.
- `oracle_critic.py` / `oracle_critic.log` — the primary oracle script and its recorded run
  (line 221; the `.py` is the `.log`'s provenance).
- `oracle_k2.py` / `oracle_k2.log` — the argument-refusal oracle and its run (lines 221, 276).
- `oracle_r2.py` / `oracle_r2.log` — the rewrite/merge oracle and its run (lines 221, 276).
- [jar/](jar/map.md) — `rmsa.txt`, `rmp.txt`: the disassembled Java procedure surfaces
  (lines 209, 276). See its map.

**Substitution (PROC-1).** Two classes of machine-local literal were neutralised so the evidence
satisfies the repository's content-hygiene classes ([../../briefs/map.md](../../briefs/map.md)
"Import gate" — no personal identifiers, local absolute paths or session identifiers; the
owner-local pre-push hook enforces them locally). Every file is otherwise byte-identical to the
scratch original; nothing beyond these tokens changed.

- **Home-directory path (2026-08-25).** `oracle_k2.log` carried an absolute home-directory path
  inside a captured PySpark traceback (two occurrences on one line); each was replaced with the
  neutral `/home/<user>` placeholder (**+4 B**).
- **Spark start-up banner (PROC-1 cycle 2, 2026-08-25).** Line 3 of `oracle_critic.log`,
  `oracle_k2.log` and `oracle_r2.log` carried the Spark boot banner naming this box: the hostname
  → `<host>`, both IP addresses → `<ip>`, the interface → `<iface>` (**−22 B each**).

## I want to...

| ...do this | go to |
|---|---|
| Read the ruling these files support | [../ledgers/archive/2026-08/2026-08-24-mw-6-rewrite-manifests-ledger.md](../ledgers/archive/2026-08/2026-08-24-mw-6-rewrite-manifests-ledger.md) |
| See the disassembled Java surfaces | [jar/map.md](jar/map.md) |
| Understand why evidence is excluded from lint | this Purpose (same rationale as `task/census/`) |

## Pointers

- Up: [../map.md](../map.md)
- Authoritative: [../../AGENTS.md](../../AGENTS.md) "Markdown document lifecycle" (the archive is
  immutable; evidence is added, never rewritten).
- The divergence registry these runs fed: [../../docs/spark-sql-iceberg-parity.md](../../docs/spark-sql-iceberg-parity.md).
