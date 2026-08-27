# map — task/mw-6-critic-evidence/jar/

## Purpose

Disassembled Java procedure surfaces that the MW-6 Critic round compared the fork's behaviour
against. Regenerated 2026-08-25 with `javap -p -c` (OpenJDK 11) from
`iceberg-spark-runtime-4.0_2.13-1.10.0.jar` — recorded output, never hand-edited (excluded from
`ruff`/`typos` with the rest of `../` — see [../map.md](../map.md)).

## Contents

- `rmsa.txt` — the `RewriteManifestsSparkAction` disassembly (the private `-p` members and `-c`
  bytecode). Cited by the MW-6 ledger at lines 209 and 276.
- `rmp.txt` — the `RewriteManifestsProcedure` disassembly. Cited at line 209.

## Pointers

- Up: [../map.md](../map.md)
- The ruling these surfaces support:
  [../../ledgers/archive/2026-08/2026-08-24-mw-6-rewrite-manifests-ledger.md](../../ledgers/archive/2026-08/2026-08-24-mw-6-rewrite-manifests-ledger.md).
