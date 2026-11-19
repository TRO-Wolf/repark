# Card CSV-GLOB-LITERAL-RESIDUAL-1: an existing CSV spelling whose own name holds `*`, `?` or `[` takes main's local all-text pre-scan again, so F1 and F2 return for it

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling of 2026-11-19 (ruling 3, about 14:45 UTC). **Source:** the delta 3 hand-back of unit 0 and the second re-check verdict on PR #1367, both held outside the repository (`csv-ts-out/delta3-ready.md`; `verify-csvts/recheck2/verdict.md`, its residual table and edge table); unit 0's [delta 3 bar](../../../wo/csv-schema-timestamp-zone-1/bar.md#delta-3-2026-11-19-the-glob-spelling).

**Status:** filed 2026-11-19; not dispatched.

**Severity: S3, not label-blocking** (owner, 2026-11-19, about 18:00 UTC, verbatim: "merge 1371 on green, N7 S1, residual S3, carry on"). Accepted under ruling 3 (owner, about 14:45 UTC, verbatim: "go with delta 3"), which accepts delta 3 of [CSV-SCHEMA-TIMESTAMP-ZONE-1](csv-schema-timestamp-zone-1-card-2026-11-19.md) with this residual named.

## Finding

- **The residual.** Delta 3 takes the `file://` route in `_load_csv` only for a spelling with none of `*`, `?` or `[` (`reader.py:510-511`). An existing spelling whose own name holds one of them takes main's local all-text pre-scan again (`read_options.rs:295-311`). So F1 and F2 return for that spelling alone, as on the first head. The delta 3 hand-back notes that it is loud: a `DECIMAL` over exponent text raises.
- **Counts against main, from the second re-check** (`verdict.md`, residual table): 32 EQUAL → RAISE (all UTC, `full` and `f1` on the residual spellings), 40 WRONG → RAISE, and 2 RAISE → WRONG (`a\*b.csv`, F2's shape, Los Angeles). The builder's own matrix, in the delta 3 hand-back, counts 24 EQUAL → RAISE and 48 WRONG → RAISE over a different probe set. Both are recorded.
- **Class (c) glob notes, the same on main.** A glob character in a directory part (`g*d/x.csv`) refuses with `No files found`, where Spark expands it. Brace alternation `x{a,b}.csv` is not a glob in DataFusion's set, so RePark reads the literal file if it exists, or refuses. An escaped `\*` is not honoured. An unclosed `[` raises on every build.
- **Pins.** The five G1 shapes over plain text are pinned (the delta 3 pins). The residual is named in the records and is not pinned (the second re-check, Check 5).

## First step

None is given in the source.

Up: [map.md](map.md).
