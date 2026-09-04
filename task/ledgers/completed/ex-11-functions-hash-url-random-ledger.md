# Unit ledger — EX-11 · v0.7 example backfill, `F.*` hash + URL + random

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the orchestrator's departure move). This file closes when EX-11 merges, or when the owner closes the slate row.

**Unit:** EX-11 · **Date:** 2026-09-03 · **Model:** muse-spark-1.2-contributor (continuation of glm-5.3-flash) · **Branch:** `feat/ex-11-functions-hash-url-random` · **Base:** `84c1801`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), batch roster row (27 names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v0.7 — Full example documentation".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`, lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`, `python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The roster is the 27 `F.*` hash, URL and random names that were backlog rows at the base `84c1801`. Five files cover the twenty names the oracle confirms; seven stay on the backlog with measured divergences or refusals.

**Roster (27):** `F.md5`, `F.sha`, `F.sha1`, `F.sha2`, `F.crc32`, `F.hash`, `F.xxhash64`, `F.hex`, `F.unhex`, `F.bin`, `F.uuid`, `F.url_decode`, `F.url_encode`, `F.try_url_decode`, `F.parse_url`, `F.try_parse_url`, `F.rand`, `F.randn`, `F.random`, `F.uniform`, `F.randstr`, `F.try_mod`, `F.try_to_binary`, `F.try_to_number`, `F.try_reflect`, `F.reflect`, `F.java_method`.

**Grouping (5 files, 4–8 allowed, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `hashing.py` | `F.md5`, `F.sha`, `F.sha1`, `F.crc32`, `F.xxhash64` | Digests and checksums: `md5`, the `sha`/`sha1` alias pair (one digest, two spellings, asserted equal), `crc32`, and `xxhash64` — one idea, one frame, NULL included. |
| `hex_binary.py` | `F.hex`, `F.unhex`, `F.bin`, `F.try_to_binary` | Hex and binary encoding: `hex` spelling integers and strings, `unhex` back (bad input NULL), `bin` base-two, and `try_to_binary` answering NULL on a bad charset. |
| `url.py` | `F.url_encode`, `F.url_decode`, `F.try_url_decode`, `F.parse_url`, `F.try_parse_url` | URL codec and part extraction: encode/decode round trip, `try_url_decode` answering NULL where strict raises, `parse_url` part extraction with `try_parse_url` answering NULL on a malformed URL. |
| `random_values.py` | `F.uuid`, `F.rand`, `F.randn`, `F.random` | Random generators: `uuid` shape, `rand`/`random`/`randn` range and shape only — never a value — the one thing these four share. |
| `try_fallbacks.py` | `F.try_mod`, `F.try_to_number` | `try_*` NULL fallbacks: modulo-by-zero and format-mismatch, each exercised on a column and on literals with format arguments that are foldable literals. |

`F.col` and `F.lit` are already covered by `abs.py`; they are listed where genuinely used and do not move the ratchet.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Five files under `docs/examples/functions/` land runnable local examples for the twenty roster names the live oracle confirms, every asserted value measured against PySpark 4.1.2 + Iceberg 1.11.0 before it was written; those twenty leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly twenty, 842 → 822, with no other `scripts/` change; the seven others (`F.sha2`, `F.hash`, `F.uniform`, `F.randstr`, `F.reflect`, `F.java_method`, `F.try_reflect`) stay backlog rows with both values recorded in the oracle table below, and no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (27 findings before, 0 after), oracle table (27 rows, one per roster name, Spark value + repark value + kept/dropped + file), the five scripts each exit 0, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured at `a0cd39e` (dispatch base `84c1801`, before any example file
existed). At that base — 27 roster rows still in `docs/examples/backlog.txt`,
`BACKLOG_BASELINE=842` — `python3 scripts/check_example_coverage.py` and the
same with `--require-execute` both exit **0** (`913 public names; 69 covered;
842 backlog; 2 exceptions; 15 examples`). **Provocation:** delete the 27 roster
rows from `backlog.txt` and lower `BACKLOG_BASELINE` to 815 (`842 − 27`) with
no example files present; the same command exits **1** with 27 findings, one
per roster name and no others. With the five files present, the twenty kept
names removed and `BACKLOG_BASELINE=822`, the same command exits **0**; the
seven dropped names remain backlog rows so the gate does not name them as
uncovered.

## Oracle (live PySpark 4.1.2 + Iceberg 1.11.0, JDK 17, warehouse `/tmp/oc-ex11-oracle/`)

Measured with `_live_parity.build_spark_iceberg_engine(Path(tmpdir)).session` at `/tmp/oc-ex11/.venv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` and `PYTHONPATH=/tmp/oc-ex11/python/repark/tests`, one throwaway script under `/tmp/oc-ex11-oracle/` printing per name Spark and repark values for identical inputs. Inputs: `s` in `[("hello",), ("hello world",), ("",), (None,)]`; `n` in `[(13,), (255,), (0,), (-1,), (None,)]`; `u` in `[(URL,), ("%ZZ",), (None,)]` where `URL=https://spark.apache.org/docs/latest/api.html?q=spark+sql#example`; `mods` `[(6,3),(7,0),(-7,3),(None,3)]`; `try_to_number` literals `999.99`/`999`/`$999`. `pins: ex-11-functions-hash-url-random/C-001`

| Name | Spark value (repr) | repark value (repr) | Kept / dropped | File | Note |
|---|---|---|---|---|---|
| `F.md5` | `['5d41402abc4b2a76b9719d911017c592', '5eb63bbbe01eeed093cb22bb8f5acdc3', 'd41d8cd98f00b204e9800998ecf8427e', None]` | same | kept | `hashing.py` | |
| `F.sha` | `['aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d', '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed', 'da39a3ee5e6b4b0d3255bfef95601890afd80709', None]` | same | kept | `hashing.py` | alias of `sha1`, asserted equal |
| `F.sha1` | same as `F.sha` | same | kept | `hashing.py` | |
| `F.sha2` | SQL `SELECT sha2('hello',256)` Spark hex `2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824` (StringType) and `512` hex `9b71d224...dec043` (StringType); facade `F.sha2(col,256)` Spark same hex string | facade `F.sha2(col,256)` repark raw bytes `b'\x2c\xf2M\xba_\xb0\xa3\x0e&\xe8;*\xc5\xb9\xe2\x9e...'` (BinaryType len 32, hex `2cf24dba...9824`); SQL `SELECT sha2('hello',256)` repark hex string `2cf24...9824` (StringType, matches Spark); facade `512` repark `UnsupportedOperationException: functions.sha2(numBits=512) only 256 is supported`, SQL `512` repark hex `9b71...043` (matches Spark) | **dropped** | — | facade returns raw bytes not hex; SQL door now matches Spark (see `FN-SHA2-1`) |
| `F.crc32` | `[907060870, 222957957, 0, None]` | same | kept | `hashing.py` | |
| `F.hash` | facade `F.hash(col)` / SQL `SELECT hash('hello')` Spark `-1008564952` (Int32) | facade `F.hash(col)` repark `UnsupportedOperationException: functions.hash is not supported yet (engine gap; disclosed R-FN-BATCH1)`; SQL `SELECT hash('hello')` repark `AnalysisException: Invalid function 'hash'. Did you mean 'tanh'?` (nondeterministic `tanh`/`cosh` suggestion) | **dropped** | — | facade refusal is the roster name; SQL door is a separate mis-suggestion |
| `F.xxhash64` | `[-4367754540140381902, 7620854247404556961, -7444071767201028348, 42]` | same | kept | `hashing.py` | `NULL → 42` (seed) |
| `F.hex` | `int: ['D','FF','0','FFFFFFFFFFFFFFFF',None]; str: '68656C6C6F'` | same | kept | `hex_binary.py` | |
| `F.unhex` | `b'hello', None, None` for `68656C6C6F`/`nothex`/NULL | same | kept | `hex_binary.py` | |
| `F.bin` | `['1101','11111111','0','111...64×1',None]` | same | kept | `hex_binary.py` | `-1 → 64 ones` |
| `F.uuid` | 32 values, all match `^[0-9a-f]{8}-...-4...-[89ab]...` | same shape, versions `{'4'}` | kept | `random_values.py` | shape/range only |
| `F.url_decode` | `'hello world'` for `hello+world`, `'a/b'` for `a%2Fb`, `RAISED IllegalArgumentException: [CANNOT_DECODE_URL]` for `%ZZ` | `'hello world'`, `'a/b'`, `RAISED PySparkException: Invalid percent-encoding` | kept | `url.py` | strict raises on both; example asserts only good input, bad input via `try_` |
| `F.url_encode` | `'hello+world+%26+more'`, `'a%2Fb%3Fc%3Dd'` | same | kept | `url.py` | |
| `F.try_url_decode` | `('a/b', None, None)` for `a%2Fb`/`%ZZ`/NULL | same | kept | `url.py` | answers NULL on `%ZZ` |
| `F.parse_url` | `protocol https, host spark.apache.org, path /docs/latest/api.html, query q=spark+sql, ref example, file /docs/latest/api.html?q=spark+sql, authority spark.apache.org, port None` | same | kept | `url.py` | requires foldable string parts; bad URL raises `INVALID_URL` on both |
| `F.try_parse_url` | same parts as `parse_url` on good URL, `None` on `%ZZ` | same | kept | `url.py` | |
| `F.rand` | `float in [0,1)`, `min~0.01 max~0.99`, `rand(42)` deterministic | same range, deterministic | kept | `random_values.py` | never asserts a value |
| `F.randn` | `float finite, e.g. -2.5…1.6` | finite | kept | `random_values.py` | shape only |
| `F.random` | `float in [0,1)` alias of `rand` | same | kept | `random_values.py` | |
| `F.uniform` | working `uniform(2.5,7.5)` Spark Decimal `2.9` / `3.1` in `[2.5,7.5)` (sampled); NULL arm `uniform(NULL,3)` Spark `Row(v=None)` (via `CAST(NULL AS DOUBLE)`) | working `uniform(2.5,7.5)` repark `float 6.30…` in `[2.5,7.5)` (double, works); NULL arm `F.uniform(CAST(NULL AS DOUBLE), lit 7.5)` / SQL `SELECT uniform(NULL,7.5)` repark `RAISED PySparkException: uniform min must be a non-null numeric constant` | **dropped** | — | working arm works on both; NULL arm: Spark None vs repark raises |
| `F.randstr` | working `randstr(8)` Spark `chgd2bjB` length 8 charset `0-9A-Za-z`; NULL arm `randstr(NULL)` Spark `''` (empty string) | working `randstr(8)` repark `Lw5jIgey` length 8 charset `0-9A-Za-z` (works); NULL arm `F.randstr(CAST(NULL AS INT))` / SQL `SELECT randstr(NULL)` repark `RAISED PySparkException: randstr length must be a non-null integer constant, got NULL` | **dropped** | — | working arm works on both; NULL arm: Spark '' vs repark raises |
| `F.try_mod` | `[0, None, -1, None]` for `(6,3),(7,0),(-7,3),(None,3)` | same | kept | `try_fallbacks.py` | |
| `F.try_to_binary` | `try_to_binary('hello','utf-8')→b'hello'`, `bad-cs→None`, `round_trip hex 68656C6C6F` | same | kept | `hex_binary.py` | Spark and repark both support foldable `fmt`; column-wise `fmt` not exercised |
| `F.try_to_number` | foldable `try_to_number('123.45','999.99')` Spark `Decimal('123.45')` (kept) and `'$123','$999'` `Decimal('123')`; column-wise `try_to_number(col('s'), col('f'))` on `[('123.45','999.99')]` Spark `RAISED AnalysisException: [DATATYPE_MISMATCH.NON_FOLDABLE_INPUT]` | foldable same `Decimal('123.45')` (kept); column-wise `F.try_to_number(col('s'), col('f'))` repark `Decimal('12345')` silently (wrong, filed `FN-TRYTONUMBER-1`) | kept (foldable) / dropped (column-wise) | `try_fallbacks.py` | foldable literals kept; column-wise mismatch filed as `FN-TRYTONUMBER-1` |
| `F.try_reflect` | `try_reflect('no.such.Class',...) → AnalysisException: class no.such.Class not found` (analysis), `try_reflect('java.lang.String','valueOf','hello')→hello` | `UnsupportedOperationException: try_reflect is unreachable: it is reflect with exception-to-NULL, and still needs a live JVM` | **dropped** | — | needs JVM |
| `F.reflect` | `reflect('java.lang.String','valueOf','hello')→hello` | `UnsupportedOperationException: reflect is unreachable: it is Spark's CallMethodViaReflection spelling of java_method, which needs a live JVM` | **dropped** | — | needs JVM |
| `F.java_method` | `java_method('java.lang.String','valueOf','hello')→hello` | `UnsupportedOperationException: java_method is unreachable: it loads a Java class by name and invokes a static method by reflection, which needs a live JVM` | **dropped** | — | needs JVM |

## Gates (2026-09-03, on this tree)

| Command | Exit |
|---|---|
| `python3 scripts/check_example_coverage.py` (static half) | **0** |
| `python3 scripts/check_example_coverage.py --require-execute` | **0** |
| `python3 scripts/sync_map_md.py --check` | **0** |
| `python3 scripts/check_ledger_grammar.py` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base a0cd39e` | **0** |
| `uv run --no-sync ruff check docs/examples` | **0** |
| `uv run --no-sync ruff format --check docs/examples` | **0** |

Counts line (both legs, after `make develop` the native module is importable, every example executed):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 89 covered; 822 backlog; 2 exceptions; 20 examples`

Before this unit: `913 public names; 69 covered; 842 backlog; 2 exceptions; 15 examples` (at `84c1801` after LOG1P-1). After: `89 covered; 822 backlog; 20 examples` — exactly the twenty kept names.

## Cost

Throughput in EX-2's shape: GLM leg started 2026-09-03 ~00:45 UTC, produced
the five example files and the ledger draft, then died on transport errors
before commit; Muse Spark continuation (~15 min, free tier) measured the
oracle divergences, repaired the ledger, filed `FN-SHA2-1` and
`FN-TRYTONUMBER-1` with pins, wrapped the five `map.md` rows, and committed.
Base `a0cd39e` (dispatch base `84c1801`).

## Registry rows filed

`FN-SHA2-1` (BACKLOG) and `FN-TRYTONUMBER-1` (BACKLOG) in
`docs/spark-sql-iceberg-parity.md` §7, each with its pin
(`test_sha2_facade_bytes_divergence_is_pinned` in
`python/repark/tests/test_fn_batch4.py`, `test_try_to_number_non_foldable_divergence_is_pinned`
in `python/repark/tests/test_fnp7_try_inversions.py`) and `pins:` citations
in `python/repark/tests/map.md`. Their current wrong values are pinned so the
fix reds on purpose.

## Disk

Pickup: `df -h` free > 400 GB (measured `526 GB free of 1.8 TB` at last charter). No worktree; unit works in the main clone. `.venv` + `target/` reused if a native build is needed; the native module is already built for this base, so `make develop` is not run.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job (`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke `python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is installed. EX-11 moves only the inventory/backlog ratchet and example files; it moves no wire, and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: EX-11
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 20 hash/URL/random names are covered by five new example files and the 7 dropped names stay backlog rows with both values in the oracle table.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/functions/hashing.py, docs/examples/functions/hex_binary.py, docs/examples/functions/url.py, docs/examples/functions/random_values.py, docs/examples/functions/try_fallbacks.py]
    - id: AT-2
      status: ATTACKED
      evidence: A COVERS name on a wrong receiver is unused and red; the widened backlog is an exact baseline 822.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-3
      status: ATTACKED
      evidence: A missing class, missing nested class, or module with no __all__ raises a hard RuntimeError; there is no silent skip on shape drift.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-4
      status: N/A
      justification: The gate is a read-only process over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the five local examples; example children drop AWS_* and PYTHONPATH, exceptions ratchet is unchanged.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill is a walk of public names that already exist.
    - id: AT-7
      status: N/A
      justification: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed.
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the new examples; the walk adds no import of the facade.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The pin file cites C-001 of this unit alongside the prior units.
      artifacts: [scripts/map.md, docs/examples/functions/hashing.py, docs/examples/functions/hex_binary.py, docs/examples/functions/url.py, docs/examples/functions/random_values.py, docs/examples/functions/try_fallbacks.py]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Sibling: [ex-0-example-drift-gate-ledger.md](../archive/2026-09/2026-09-02-ex-0-example-drift-gate-ledger.md), [ex-1-class-surfaces-ledger.md](../archive/2026-09/2026-09-02-ex-1-class-surfaces-ledger.md), [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md)
