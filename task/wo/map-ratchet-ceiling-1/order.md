# MAP-RATCHET-CEILING-1 — map size rows become ceilings      grade: A   engine: Opus 5.5 high   release: next

## 0. Why, and what is out of scope

The map size ratchet compared an excepted map with its `(lines, bytes)` row exactly, so a map that
shrank failed until its row was lowered, and a records PR that lowered one row ran the whole code
roster. Out of scope: the source-size gates (`check_rust_file_size.py`, `check_lib_py.py`), the CAP-1
mirror (harness item H) and `EXPECTED_SYMBOL_HASHES` (item J).

## 1. Rulings already made

- Owner, 2026-11-18 ([task/lessons.md](../../lessons.md), "size baselines are ceilings"): growth past
  the row fails, shrinkage passes, rows are lowered only by a periodic clerk sweep; a unit adds or
  changes a map row only when it adds, removes or renames a file.
- Owner, 2026-11-19, "Yeah proceed": Opus 5.5 high builder, Opus 5.5 medium verifier; the clerk sweep
  runs `--update-baselines` at each release tag.
- Orchestrator: the "baseline is within both ceilings" error stays.

## 2. Files

- `scripts/check_map_size.py`: the exact-match branch in `check_map` is gone; docstring rule 2 is
  deleted (comments are shed, never reworded) and the growth message drops "exact".
- `python/repark-parity/tests/test_map_governance_1.py`: shrink without a row edit is green; growth in
  bytes alone and in lines alone each red; the shrink and retire tests now expect green before
  `--update-baselines`.
- Prose: `docs/maps-policy.md`, `AGENTS.md`, `DEVELOPMENT.md`, `scripts/map.md`,
  `python/repark-parity/tests/map.md`.
- Left for the owner: the `Makefile` help text of `check-map-size` and the `check_map_size.py`
  docstring summary still say "exact-baseline"; Anthropic models may not reword comments, so the
  owner decides whether a person rewords them.
- `scripts/ledger_lifecycle.py`: unchanged; a row moved by a link rewrite stays at or above its map.

## 3. Proof (row for `docs/map.md` untouched, `(373, 31128)`; map restored after each step)

| step | map | `make check-map-size` | script exit |
|---|---|---|---|
| 1. shrink one line | 372 lines, 30,854 B | 0, "703 maps clean" | 0 |
| 2. one byte more, equal lines | 373 lines, 31,129 B | 2 (make), "grew to 373 lines and 31129 bytes" | 1 |
| 3. one line more, equal bytes | 374 lines, 31,128 B | 2 (make), "grew to 374 lines and 31128 bytes" | 1 |

Make reports a failed recipe as exit 2; the script's own exit is 1.

## 4. Gates

Each under `flock -s` on the measure lock, `nice -n 10 taskset -c 32-47`.

| gate | exit | line |
|---|---|---|
| `make check-map-size` | 0 | 703 maps clean; 49 exceptions, 5 exempt |
| `make check-map-sync` | 0 | 703 maps clean (strict=off) |
| `make check-docs-links` | 0 | 3060 files, 13596 links checked, clean |
| `make check-ledgers` | 0 | 602 ledgers in bins, 1400 ledger links resolve |
| `make check-ledger-grammar` | 0 | clean |
| `make check-owner-ruling`, `make check-model-ban` | 0, 0 | present; no Opus 5.0 id |
| `python3 scripts/check_docs_compaction.py` | 0 | `AGENTS.md` 29,047 B of 30,000 |
| `make py-test` (parity harness, isolated `uv` env) | 0 | 1043 passed, 17 skipped |
| `pytest -n 6` over `test_map_governance_1.py` and `test_map_governance_1_lifecycle.py` | 0 | 75 passed |
| comment gate (`comment_ban.py . origin/main HEAD`) | 0 | no added comment |
| `uvx typos@1.47.2` | 0 | clean |
| `uvx ruff@0.15.22 check .`, `format --check .` | 0, 0 | all checks passed; 1829 files formatted |

No file under `python/repark/tests` pins the map ratchet, so no facade test ran.

## 5. Hand-back

The orchestrator's scratchpad, `<scratchpad>/map-ceiling-1/ready.md`.
