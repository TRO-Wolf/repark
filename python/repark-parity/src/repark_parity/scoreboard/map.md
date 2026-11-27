# map — python/repark-parity/src/repark_parity/scoreboard

## Purpose

The Spark-door scoreboard harness (SPARK-DOOR-SCOREBOARD-1, 2026-11-27): replay a RePark package
against the recorded Spark 4.1.2 answers of the 2026-11-21 whole-door audit corpus, classify every
cell, and compare two scoreboards cell by cell. Ported from the audit's own harness with its
behaviour kept. The procedure is
[../../../../../.agents/skills/spark-door-scoreboard/SKILL.md](../../../../../.agents/skills/spark-door-scoreboard/SKILL.md);
the corpus and its provenance are [data/map.md](data/map.md).

Run it as `PYTHONPATH=python/repark-parity/src python -m repark_parity.scoreboard <command>` under
any Python 3.12 with pyarrow and pydantic. Engine processes start their own interpreters.

## Contents

| File | Role |
|---|---|
| `cli.py`, `__main__.py` | the commands: `replay`, `record`, `diff`, `controls`, `flaky`, `summary`, `compare`, `inspect`, `ingest`, `check-corpus` |
| `settings.py` | the machine settings, each a flag or an environment variable; a missing one fails with its flag and variable named |
| `encoding.py` | `encode_line` (sorted-key ASCII JSON, no NaN), `cell_digest` (SHA-256 of that line), `read_records` (duplicate id fails), `canonical` (float hex with NaN as one class, decimals as text, bytes as hex) |
| `classify.py` | `classify_pair` and `diff_rows`: equal id sets, digest and engine tag per answer, then the class |
| `engine.py` | one engine process over one case file: settings per cell, setup, operation, `store` write and cold read, observation, refusal capture |
| `launch.py` | the engine environment, CPU pin, nice 19, memory floor, Spark driver slots, batches of 192 |
| `corpus.py` | read the stored lanes and exclusions, choose cells, stage a replay, ingest a lane |
| `replay.py` | the RePark half over a staged corpus on a process pool; the Spark half for a case file |
| `controls.py` | the controls lane check, six classifier mutations, float edges |
| `summary.py` | scoreboard, comparison, flaky detection, Markdown tables |
| `witness.py` | joined cell, verdict and answers for one class, name or id |
| `grid.py` | the six-seat scalar grid for new cases |
| [cellpath/](cellpath/map.md) | modules the stored case code imports by bare name |
| [data/](data/map.md) | the corpus, exclusions, scoreboards, reports |

## Invariants and reasons

- **Class order.** Both answered: a type or name difference (nullability removed) is `WRONG-TYPE`,
  then a value difference is `WRONG-VALUE`, then a schema difference left is `NULLABILITY-ONLY`.
  A panic outranks an exclusion; an exclusion outranks every product class.
- **Digest.** An answer binds to the SHA-256 of its cell's sorted-key ASCII JSON. A changed
  definition, a missing id, a duplicate id or a wrong engine tag fails the diff; nothing is
  re-matched by position.
- **Never overwrite answers.** `replay` and `record` need a new destination inside the output root;
  answers go to `.partial` or `.incomplete` and are renamed only when whole; `ingest` refuses a
  stored lane without `--replace`.
- **Rendered timestamps.** A top-level timestamp column also carries its SQL text. Text counts only
  when both engines rendered the same columns; otherwise only the Arrow values (nanoseconds on the
  Arrow timeline; for NTZ the wall timeline) compare.
- **`store: true`.** The result is written to fresh Parquet, read back through the engine, and every
  committed part is cold-read with PyArrow into `physical_write`. `physical_write` is an
  observation: the classifier does not read it, and its `part_files` count differs between builds.
- **Exclusions.** `EXCLUDED` at run (setup failure, observation conversion, settings read-back) and
  the frozen recorded exclusions both leave the comparable count; a flaky cell leaves it too.
- **The engine imports no pydantic.** The Spark interpreter has none; only `settings.py` uses it.
- **`grid.scalar_grid` arguments.** `expressions` is the SQL literal form, the SQL form over the
  input column and the typed Column form; `seed` is trusted Python that yields a PyArrow table;
  `predicate` holds `{v}` for the expression and must depend on the answer.

## Pointers

- Up: [../map.md](../map.md)
- Tests: [../../../tests/spark_door_scoreboard/map.md](../../../tests/spark_door_scoreboard/map.md)
- One surface against live Spark instead: [../../../../../.agents/skills/audit-repark-parity/SKILL.md](../../../../../.agents/skills/audit-repark-parity/SKILL.md)
