# SPARK-DOOR-SCOREBOARD-1 — the Spark-door audit harness, its corpus and its skill, in the repository

**Kind:** measurement tooling, no product change. **Base:** main `9df269acb`. **Opened:** 2026-11-27, on the
owner's request to make the 2026-11-21 whole-door audit "an extremely detailed skill for this repo".
This card closes when its PR merges; later scoreboards go on the cards of the units that measure them.

## What landed

- The harness, ported with its behaviour kept:
  [python/repark-parity/src/repark_parity/scoreboard/](../../../python/repark-parity/src/repark_parity/scoreboard/map.md)
  (replay, record, diff, controls, flaky, summary, compare, inspect, ingest, check-corpus).
- The corpus: 17,264 cell definitions in 51 lanes with their Spark 4.1.2 answers, 619 frozen
  exclusions, 1.5 MB gzipped:
  [data/](../../../python/repark-parity/src/repark_parity/scoreboard/data/map.md).
- The procedure: [.agents/skills/spark-door-scoreboard/SKILL.md](../../../.agents/skills/spark-door-scoreboard/SKILL.md).
- Tests that need no Spark and no package:
  [python/repark-parity/tests/spark_door_scoreboard/](../../../python/repark-parity/tests/spark_door_scoreboard/map.md).

## Measured (2026-11-27)

| Scoreboard | Revision | Comparable | EQUAL | Share |
|---|---|---:|---:|---:|
| [2026-11-21 baseline](../../../python/repark-parity/src/repark_parity/scoreboard/data/scoreboards/2026-11-21-dac2af61.md) | `dac2af61` | 16,645 | 8,583 | 51.57% |
| [2026-11-27 main](../../../python/repark-parity/src/repark_parity/scoreboard/data/scoreboards/2026-11-27-9df269ac.md) | `9df269acb` | 16,645 | 9,033 | 54.27% |

The baseline was rebuilt from the audit's own diff files and equals its counts exactly. Main was replayed on a
release-fast package, three times (two batch sizes): 0 flaky cells. The
[comparison](../../../python/repark-parity/src/repark_parity/scoreboard/data/reports/2026-11-27-dac2af61-to-9df269ac.md):
454 cells moved onto Spark, **4 moved off**, 62 changed but still unlike.

**Reproduction check.** The audit's revision was rebuilt twice and the whole comparable corpus replayed through
the port on each build. With the audit's own `ci` profile, 16,640 of 16,645 cells reproduce its recorded RePark
answers field for field (outcome, value, schema, error class, SQLSTATE, stored write), and every class reproduces.
The other five differ only in `physical_write.part_files` (the same counts on every build and core count tried;
cause not established), which the classifier does not read. With `release-fast`, twelve more `nested-pairs`
cells differ: the `ci` profile keeps Rust overflow checks, so a timestamp overflow is a caught refusal there and a
wrapped value in a release build. So 12 of the 62 "changed" cells in the comparison are the build profile, not a
code change; against a `release-fast` replay of `dac2af61`, main moved 454 on, 4 off and changed 50. The controls
lane passes `--strict` on the audit package.

## Moved off Spark (each needs its own card; none fixed here)

All four are `dates`, ANSI on, `WRONG-VALUE`, in the `expr-literal` seat:
`dates.followup-unix_timestamp-ntz.on.{Kolkata,Los_Angeles}.expr-literal` and
`dates.followup-to_unix_timestamp-ntz.on.{Kolkata,Los_Angeles}.expr-literal`.
`F.expr("unix_timestamp(cast('2024-01-01 02:00:00' as timestamp_ntz))")`: Spark answers `1704074400` (the wall read
as UTC); main answers the wall localized in the session zone (`1704054600` in Kolkata). The ANSI-off siblings moved
from `NULLABILITY-ONLY` to `WRONG-VALUE` (4 more). Rank S1 until a stored-write witness says otherwise.

## Largest movers and worse transitions

- **Moved on**, by lane: dates 275, rewrites-prior 69, strings-collections 32, numbers 21, reference 14.
  By former class: WRONG-VALUE 267, REFUSED-WHERE-SPARK-ANSWERS 134, WRONG-TYPE 29, NULLABILITY-ONLY 24.
- **Worse while still unlike** (a refusal became an answer Spark does not give):
  `num.bigint.negate.on.UTC.typed-column` and its sibling, and `prior.SIM-CONST-DEAD.cast_and.fire.UTC.on` and its
  sibling, `BOTH-REFUSED` to `ACCEPTED-WHERE-SPARK-REFUSED` under ANSI; `error.cast.0`,
  `REFUSED-WHERE-SPARK-ANSWERS` to `WRONG-TYPE`.

## Review round 1: the seven minor findings (2026-11-28)

The fresh verifier passed the unit with 0 class (a), 0 class (b) and 7 class (c) findings. Each fix
has a test that turns red on its mutant; the scoreboard tests went from 32 to 37.

- **c1.** An empty `--cells` list selects no cell. It used to replay all 16,645.
- **c2.** `replay` and `record` check every engine setting before they create the destination.
- **c3.** `ingest` refuses a lane name outside lowercase letters, digits and hyphens.
- **c4.** `ingest` and `check-corpus` refuse a cell or answer that holds a machine path, and skill
  section 7 names the scrub.
- **c5.** Kept as recorded: two messages end mid-path at the audit's 800-character cut, which
  names nothing.
- **c6.** `data/checksums.json` holds the SHA-256 of every stored file; `check-corpus` fails on any
  difference, and `ingest` rewrites it.
- **c7.** The `part_files` wording is fixed. The two meanings of `definitions` are explained in skill
  section 4.4 rather than changed, so the committed scoreboards stay byte-identical.

## Follow-ups (named, not opened)

- A card for the four moved-off `unix_timestamp` NTZ cells and the two ANSI `ACCEPTED` transitions above.
- **ICEBERG-SCOREBOARD-1**: the same replay over local Iceberg catalog tables, with snapshot, schema and file
  witnesses; until then Iceberg parity lives in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
  and the AWS acceptance matrix.
