# ATTR-ID-1 S2: the seam (fresh Muse session, guided; lane /tmp/xattr, branch feat/attr-id-1, head ce7fe8e2)

Read first:
- AGENTS.md, from this clone;
- the work order `/tmp/oc-worker/direct/wo/attr-id-1-design.md`: §1–§3, §4 "S2", §5, and **§9, S1's outcome and the orchestrator's rulings Q1–Q4**;
- S1's hand-back `/tmp/xattr/handback.json` and the S1 commits `git log origin/main..HEAD`; `crates/repark-core/src/session/df_guards/attr_id.rs` and the three pyfunctions in `crates/repark-python/src/dataframe_names.rs`;
- the S0 replay in `/tmp/oc-worker/direct/wo/attr-id-1/replay/` (`replay.py <engine> <out-dir>`, `compare.py`, `main.json`, `summary.txt`).

## Orchestrator corrections to §4 S2 (they override the work order where they differ)
- **The baseline is `main.json`, not `pr881.json`.** The branch comes from main (OD-1), so the S2 facade is main's facade. The replay on this branch must be byte-identical to `main.json` on every judged cell. The timing bar is 1.2x of main's 498 s median, which is 597 s at most, measured as the median of 3.
- **Stamp at the single construction point.** Main builds facade `DataFrame`s in about 17 places: `core.py` `_spawn`, `catalog_surface.py`, the `session_core.py` readers and sql, `reader_*.py` and `repark/__init__.py`. So put `stamp_attribute_ids` in `DataFrame.__init__`, where the native frame is wrapped, rather than in `_spawn` alone. Every path is then covered by construction. `stamp` is idempotent, as S1 pinned. Show with a grep that no path builds a facade frame without going through `__init__`.
- **`Column._attr_id`:** add the attribute (default `None`), and set it at the bind sites that exist **on main** from §3.6's list: `_column_of`, and wherever main's `__getitem__`/`__getattr__` bind a Column to a frame's field. `_bind_engine_display_column` and `_shared_origin_column` are #881 names; if main lacks them, skip them, since S3 covers them. Nothing reads `_attr_id` yet.

## Also in S2 (rulings from §9)
- **Q2: strip `repark.attr` from every output.** No `repark.attr` metadata key may reach a written file or an export. Strip it at every write sink: parquet, CSV, JSON, Iceberg (SQL and DataFrame writers, `saveAsTable`, `insertInto`, `writeTo`) and s3a. Strip it at every export: `toArrow`, `toPandas`, `collect`, `df.schema` and `printSchema`/`_repr`.
  - Pins must show that no `repark.attr` key survives in any written file's footer or schema, or in any exported Arrow schema. Include an Iceberg table's stored schema and a parquet footer.
  - If a sink would need a change inside DataFusion or iceberg-rust, HALT with the site.
- **Q3: the USING-join re-mint.** Add the re-mint of colliding right-side ids in `join_on_keys`, as S1 did in `requalify_join_sides`. Pin it with a self-join through USING, asserting distinct ids for the non-key columns.

## Proof
- Rebuild the lane with `make develop` (debug), inside `repark.slice` with `CARGO_BUILD_JOBS=6`.
- Replay on this build: `/tmp/xattr/.venv/bin/python /tmp/oc-worker/direct/wo/attr-id-1/replay/replay.py <engine-name-as-in-S0-for-main> /tmp/oc-worker/direct/wo/attr-id-1/s2/`. Use the engine name S0 used for the main run; read `replay.py`'s `main()`.
  - Compare with `main.json` through `compare.py`. **Target: 0 changed judged cells**, with the 7 nondet cells excluded.
  - Any changed cell is a HALT with the cell. It names a place where a projection added by `stamp` was silently load-bearing.
  - Timing: median of 3 at or under 597 s.
- The Q2 and Q3 pins above.
- A mutation: stop stripping at the parquet sink, and the footer pin goes red. Revert, and show `git status` clean.
- Rerun the facade files `test_casesens_1*.py` and every `test_*join*`, `test_*write*` and `test_*iceberg*` file whose runtime allows it. List what you ran.

## Halt when
- A replay cell changes against `main.json`.
- The replay exceeds 597 s.
- Stripping needs a change inside DataFusion or iceberg-rust.
- Any path needs to treat a missing id as anything but a bug.

## Commit (commit first after each block, and keep `handback.json` provisional)
- Commits:
  - `feat(attr-id-1): every facade frame is stamped at construction; Column carries its attribute id`;
  - `feat(attr-id-1): repark.attr never reaches a written file or an export; USING joins re-mint colliding ids`.
- Identity: `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
- Exactly one trailer: `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`.
- **No code comments** in any source file, doc comments, docstrings added to new functions and `noqa` prose included. Update `map.md` in lockstep. Extend the ledger `task/ledgers/staging/attr-id-1-ledger.md` with S2 clauses.
- Every Rust file stays at or below 1,000 lines, and every Python file stays within its CAP-1 row. Never raise a ceiling.
- Gate: `bash /tmp/xattr/gate.sh`. Also run the facade test files you touched with `.venv/bin/python -m pytest -n 8 -q -p no:cacheprovider …`.
- Hand-back: `/tmp/xattr/handback.json`, with the replay counts, the timings, the pins and the product-line and test-line counts.

## Rules
Work only in `/tmp/xattr` and `/tmp/oc-worker/direct/wo/attr-id-1/s2/`. Never modify anything else under `/tmp/oc-worker/direct/wo/`. Never touch `~/CodeRepos`. Never use AWS credentials. Do not push.
