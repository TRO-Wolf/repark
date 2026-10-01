# ATTR-ID-1 S0: the replay that gates everything (fresh Muse session; lane /tmp/xattr, branch feat/attr-id-1 at origin/main db3a1f37; no product edits)

Read first:
- AGENTS.md, from this clone;
- the work order `/tmp/oc-worker/direct/wo/attr-id-1-design.md`, all of it. This round is its §4 "S0", and §0–§3 explain what the replay has to judge;
- the six verifier hand-backs on PR #881: `/tmp/oc-worker/direct/wo/verify-cs2-opus-handback.json`, and `reverify-cs2` … `reverify5-cs2-opus-handback.json` in the same folder.

## Task (the owner adopted the work order on 2026-09-30)
Build `/tmp/oc-worker/direct/wo/attr-id-1/replay/`. It needs one driver, `replay.py <engine> <out-dir>`, in the shape of the verifiers' `cs_probe5.py`, with every cell keyed by a stable id: the corpus name, then the cell name.

**Cells come from:**
- `/tmp/oc-worker/direct/wo/cs2-verify-evidence/probe*.py`;
- `reverify-cs2-evidence/`, `reverify2-cs2-evidence/`, `reverify3-cs2-evidence/`, `reverify4-cs2/corpus/` and `reverify5-cs2/`;
- `python/repark/tests/casesens_2_spark_oracle.json`, read from `/tmp/xs-cs1`, the #881 lane at 121c4bd0. Main does not have that file.

Port the cells into the one driver. Do not import the verifiers' scripts in place, and never modify or delete anything in those folders.

**Record three answer sets in `replay/`:**
- `spark.json`, run once through `/tmp/oc-worker/_lib/jvm-lock.sh /tmp/sparkenv/bin/python replay.py spark <dir>` (JAVA_HOME=/usr/lib/jvm/zulu-17-amd64). If the JVM lock is held for more than 20 minutes, wait again. Never kill a process you did not start.
- `main.json`, from `/tmp/xattr/.venv/bin/python`, a fresh debug `make develop` of origin/main db3a1f37 that the orchestrator built. Confirm the .so mtime is later than `/tmp/oc-worker/xattr-setup.done`'s creation, or that the setup log shows `develop rc=0`.
- `pr881.json`, from `/tmp/xs-cs1/.venv/bin/python` (debug, head 121c4bd0; its .so is newer than the commit).

**Each answer is a normalized result:**
- rows, as sorted or ordered per cell, whichever the source cell specifies;
- column names;
- or the error class plus the first line of the message.

Make sure the same cell run twice gives byte-identical JSON. Nondeterministic cells are marked and excluded from the counts.

**Print and save `replay/summary.txt`:**
- total cells;
- cells where main ≠ Spark;
- cells where 881 ≠ Spark;
- cells where 881 ≠ main;
- the cells EQUAL on main that move away on 881. These are the S1s the folds introduced, and each should map to an RC finding id where one exists.
- the wall-clock time of the whole replay on main and on 881, as the median of 3 runs, because S2's 1.2x halt rule is measured against it.

Classify every non-EQUAL cell in `replay/classes.json` by RC finding id where the hand-backs name one, or as `unnamed`.

## Halt when
- Fewer than 600 cells assemble.
- A corpus needs Spark for more than 10 % of its cells and Spark is unavailable.
- Two runs of the same build disagree on more than 1 % of cells.

## Output
- `/tmp/xattr/handback.json`, with the counts, the timings, the per-corpus cell counts and the path of every file written.
- No commits, and no edits in `/tmp/xattr` or `/tmp/xs-cs1` beyond `handback.json`: this round builds evidence only.

## Rules
- Work only in `/tmp/oc-worker/direct/wo/attr-id-1/` and read-only in `/tmp/xattr` and `/tmp/xs-cs1`.
- No cargo, maturin, make or pip.
- Never touch `~/CodeRepos`. Never use AWS credentials. Do not push.
- Write no code comments in the driver.
