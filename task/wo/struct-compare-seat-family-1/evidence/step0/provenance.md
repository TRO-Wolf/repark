# Provenance: family step-0 table

## The clerk's table (2026-11-10)

- Clerk model id: `claude-haiku-5-5` (verified from the session environment before the first action).
- Brief: the orchestrator's `haiku-family-step0-table.md` (scratch, not committed).
- Memo: the frontier review memo of 2026-11-10, committed as `../frontier-review-memo-2026-11-10.md`.
- Fable's probes `probe.py` and `dml.py`, copied from the memo probe folder. Their memo outputs (`spark.jsonl`,
  `head.jsonl`, `base.jsonl`, `dml-results.txt`) are copied unchanged into `memo-probes/`, so `--render` can
  reproduce the "Fable section 1 reproduction" section from this directory.
- Legs:
  - `spark`: `<oracle-venv>/bin/python -I`, Spark 4.1.2, `JAVA_HOME` a Zulu 17 JDK, Iceberg 1.11.0 runtime jar via
    `spark.jars`.
  - `head`: `<clone>/.venv/bin/python -I`, `repark.__file__` under the lane clone at #1292's head (release build,
    `__debug_assertions__` False). `raw/head.identity.json`.
  - `base`: the same interpreter without `-I`, `PYTHONPATH=<base-site>`, the Opus verifier's main build before #1292.
    `raw/base.identity.json`.
- One run per leg, inside one `systemd-run --user --slice=repark.slice` unit, every Spark JVM under the shared
  `spark-jvm.lock` flock. The run script ran, in order: Fable's `probe.py` and `dml.py` on the three legs, then
  `probe_family.py --leg spark`, `--leg head` and `--leg base`. Exit 0.
- v3 DML re-run (orchestrator): `rerun_v3.py head` and `rerun_v3.py base`, with
  `repark.sql.allowCreateFormatVersion3=true`, as a `repark.slice` unit, both exits 0. The results replaced the v3
  records in `raw/head.jsonl` and `raw/base.jsonl`; `raw/head-v3.jsonl` and `raw/base-v3.jsonl` are the re-run
  records. The superseded pre-merge copies are not committed.

## The builder's commit (item 1, 2026-11-10)

- Builder model id: `claude-opus-5-5`, verified from the session environment before the first action.
- `table.md` up to byte 969744 is the clerk's table (sha256 `ca543a36…`). The section after it is the completeness
  additions, rendered by `probe_family_add.py --render`.
- **Lint.** `probe_family.py`, `probe.py`, `dml.py` and `rerun_v3.py` were brought to `ruff check` and
  `ruff format --check` clean (ruff 0.15.22). The edits are:
  - type annotations;
  - lower-case names for `F`, `L`, `R`, `S`, `N`, `P`, `M`, `AR` and `l`, renamed by token;
  - long strings split into implicit concatenations;
  - `zip(…, strict=False)`;
  - `Path.open`, `contextlib.suppress`, `[*row[:4], 77]`, `_oracle`, `_name`;
  - the unused `subprocess` import dropped.
- **Behaviour proof.** `probe_family.py --render` over the clerk's raw legs gives sha256 `ca543a36…` before the
  lint edits and after them, byte-identical to the clerk's `table.md`. A normalized AST comparison (annotations
  dropped, the rename map applied, `strict=` dropped) shows that `dml.py` is identical, and that every other
  difference is one of the edits listed above.
- **Paths scrubbed:**
  - the Iceberg jar becomes `<ivy-jar>` (override with `STRUCT_FAMILY_IVY_JAR`);
  - the memo probe folder becomes `memo-probes/` here (override with `STRUCT_FAMILY_MEMO_PROBES`);
  - `rerun_v3.py` finds `probe_family` beside itself;
  - the identity records name `<clone>`, `<base-site>` and `<oracle-venv>`.
- **Capture.** `attempt` and `try_call` catch `BaseException`, so a pyo3 `PanicException` is recorded as a cell
  outcome. This affects runs only; the render is unchanged.
- **The additions** (`probe_family_add.py`, ids `X0001`–`X0148` and `Y0001`–`Y0006`) ran from a scratch copy of
  this directory, as one `repark.slice` unit:
  - `--leg spark` on the oracle interpreter, under `spark-jvm.lock`;
  - then `--leg head` on `<clone>/.venv/bin/python -I`, the same release build as the `head` leg, which is this
    unit's base.
  - Both exits were 0. The records are `raw/spark-add.jsonl` and `raw/head-add.jsonl`, with their `.identity.json`
    and `.setup.json` files.
- Run a leg with the target tree's own interpreter from a scratch copy, never in this directory: a run writes
  `raw/` and `work/` beside the script.
