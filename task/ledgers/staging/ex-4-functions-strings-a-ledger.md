# Unit ledger — EX-4 · v0.7 example backfill, `F.*` string basics (batch a)

**Retires:** this ledger moves to `../completed/` in the family's last commit
(the orchestrator's departure move). It closes when the `F.*` string-basics
family PR merges, or when the owner closes the slate row.

**Unit:** EX-4 · **Date:** 2026-09-03 · **Model:** grok-4.6 (continuation of
glm-5.3-flash) · **Branch:** `feat/ex-4-functions-strings-a` ·
**Base:** `887695cb45d64def26743c82cfbb8e6344c04986` (dispatch base `d7e2c4a`)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md).
**Ruling:** owner, 2026-08-31, v0.7 example documentation.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the
`BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`, lockstep
`map.md` files, this ledger with its `staging/map.md` row, and — for the one
silent value divergence — registry §7 plus its pin test. Closed: `crates/`,
`python/repark/src/`, `.github/`, `STATUS.md`, every other ledger.

## Scope

The family is the 34 `F.*` string-basics names dispatched as backlog rows at
`origin/main` `d7e2c4a`. This unit is batch a.

**Roster as dispatched (34 names):**

`F.ascii F.base64 F.unbase64 F.btrim F.char F.chr F.char_length
F.character_length F.concat F.concat_ws F.contains F.endswith F.startswith
F.encode F.decode F.format_number F.format_string F.printf F.initcap F.instr
F.lcase F.ucase F.lower F.upper F.left F.right F.length F.levenshtein F.locate
F.lpad F.rpad F.ltrim F.rtrim F.trim`

**As landed after critic round 2: twenty-seven.** Seven names stay on the
backlog — see the oracle table. The first landing taught 30; `F.initcap` and
`F.chr`/`F.char` came off after the live oracle showed a silent miss on
non-space word breaks and on `n >= 256`.

## Grouping

| File | `COVERS` (batch names) |
|---|---|
| `case.py` | `F.lcase`, `F.lower`, `F.ucase`, `F.upper` |
| `concat.py` | `F.concat`, `F.concat_ws` |
| `edges.py` | `F.left`, `F.right` |
| `format.py` | `F.format_string`, `F.printf` |
| `length.py` | `F.length`, `F.char_length`, `F.character_length`, `F.ascii` |
| `matching.py` | `F.contains`, `F.startswith`, `F.endswith`, `F.instr`, `F.locate`, `F.levenshtein` |
| `padding.py` | `F.lpad`, `F.rpad`, `F.ltrim`, `F.rtrim`, `F.trim`, `F.btrim` |
| `unbase64.py` | `F.unbase64` |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Batch a lands runnable local examples for the twenty-seven roster names the live oracle confirms, in eight files under `docs/examples/functions/`, every asserted value measured against live PySpark 4.1.2 + Iceberg 1.11.0 before it was written and every `COVERS` entry exercised by an assertion on that measured value; those twenty-seven leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves 654 → 627 as merged (844 → 817 at dispatch); the seven names whose repark value diverges or refuses stay backlog rows with both values recorded (`F.base64` BL-17, `F.initcap` FN-INITCAP-1, `F.chr`/`F.char` FN-CHR-1, encode/decode/format_number); no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (the 34 are uncovered at `d7e2c4a`), the oracle table, the green counts line, the recorded gate exit codes, and `pins: ex-4-functions-strings-a/C-001` in `scripts/map.md` plus BL-17 / FN-INITCAP-1 / FN-CHR-1 / FN-TRIM-CHARS-1. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first

At dispatch base `d7e2c4a`, `docs/examples/backlog.txt` holds all 34 roster names
and `BACKLOG_BASELINE = 844`. None of the eight files exist. Removing the 34
rows without examples would red the coverage gate one finding per name.

## Oracle table (live PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC`, 2026-09-03)

Engine: `_live_parity.build_spark_iceberg_engine` at
`/tmp/oc-ex4/.venv/bin/python`. Spark session version printed `4.1.2`. Throwaway
script `/tmp/oc-ex4-oracle/measure_strings.py` (not in the repo).

| Name | Input | Spark | repark | Verdict | File |
|---|---|---|---|---|---|
| `F.ascii` | `['Spark','Apache','aPACHE','',None]` | `[83, 65, 97, 0, None]` | same | kept | `length.py` |
| `F.char_length` | same strings | `[5, 6, 6, 0, None]` | same | kept | `length.py` |
| `F.character_length` | same strings | `[5, 6, 6, 0, None]` | same | kept | `length.py` |
| `F.length` | same strings | `[5, 6, 6, 0, None]` | same | kept | `length.py` |
| `F.chr` | `[256, 300, 321, 65601, -1]` | `['\\x00', ',', 'A', 'A', '']` (`n % 256`; `''` if `n < 0`) | `chr(300)` → `'Ĭ'`; `chr(-1)` raises `invalid Unicode scalar value: -1` | **dropped** — FN-CHR-1 | backlog |
| `F.char` | same ints | same as `F.chr` | same as `F.chr` | **dropped** — FN-CHR-1 | backlog |
| `F.lcase` / `F.lower` | `['Spark','Apache','aPACHE','',None]` | `['spark','apache','apache','',None]` | same | kept | `case.py` |
| `F.ucase` / `F.upper` | same | `['SPARK','APACHE','APACHE','',None]` | same | kept | `case.py` |
| `F.initcap` | `['hello world','a-b','foo.bar','1abc',"o'neil",'ab_cd','x\\ty']`; `'a-b c.d'` | `['Hello World','A-b','Foo.bar','1abc',"O'neil",'Ab_cd','X\\ty']`; `'A-b C.d'` | `['Hello World','A-B','Foo.Bar','1abc',"O'Neil",'Ab_Cd','X\\tY']`; `'A-B C.D'` | **dropped** — FN-INITCAP-1 | backlog |
| `F.concat` | `(s,t)` pairs plus `s+'!'` | `['Spark!','Apache!','aPACHE!','!',None]` and `['Sparkx','Apacheabc','aPACHE xx ','',None]` | same | kept | `concat.py` |
| `F.concat_ws` | `'-'` join of `(s,t)` | `['Spark-x','Apache-abc','aPACHE- xx ','-','']` | same | kept | `concat.py` |
| `F.left` | width 3 / −3 | `['Spa','Apa','aPA','',None]` / `['','','','',None]` | same | kept | `edges.py` |
| `F.right` | width 3 / −3 | `['ark','che','CHE','',None]` / `['','','','',None]` | same | kept | `edges.py` |
| `F.format_string` | `'%s=%d'` on `(s,n)` | `['Spark=4','Apache=-3','aPACHE=0','=6','null=null']` | same | kept | `format.py` |
| `F.printf` | `'%d apples'` on `n` | `['4 apples','-3 apples','0 apples','6 apples','null apples']` | same | kept | `format.py` |
| `F.contains` | `'par'` in `s` | `[True, False, False, False, None]` | same | kept | `matching.py` |
| `F.startswith` | `'Sp'` | `[True, False, False, False, None]` | same | kept | `matching.py` |
| `F.endswith` | `'rk'` | `[True, False, False, False, None]` | same | kept | `matching.py` |
| `F.instr` | `'par'` in `s` | `[2, 0, 0, 0, None]` | same | kept | `matching.py` |
| `F.locate` | 2-arg `'x'` in `t`; 3-arg `locate('x', 'xxSparkxx', 3)` | 2-arg `[1, 0, 2, 0, None]`; 3-arg `8` | 2-arg same; 3-arg `UnsupportedOperationException` (`R-FN-BATCH2`) | kept 2-arg only | `matching.py` |
| `F.levenshtein` | `(s,t)` and `kitten`/`sitting` | `[5, 5, 6, 0, None]` and `[3]*5` | same | kept | `matching.py` |
| `F.lpad` / `F.rpad` | width 8 and 3, pad `'-'` | Spark padded/truncated forms as in `padding.py` | same | kept | `padding.py` |
| `F.ltrim` / `F.rtrim` / `F.trim` / `F.btrim` | one-arg space-strip; `btrim('xxSparkxx','x')`; two-arg `trim('xxSparkxx','x')` | one-arg and `btrim` as in `padding.py`; two-arg trim → `'Spark'` | one-arg and `btrim` same; two-arg `TypeError` (1 positional) | kept one-arg + `btrim`; two-arg is FN-TRIM-CHARS-1 | `padding.py` |
| `F.unbase64` | `['U3Bhcms=','QXBhY2hl','QQ==',None]` | `[b'Spark', b'Apache', b'A', None]` | same | kept | `unbase64.py` |
| `F.base64` | `['Spark','Apache','A','',None]` (string and binary) | `['U3Bhcms=','QXBhY2hl','QQ==','',None]` | `['U3Bhcms','QXBhY2hl','QQ','',None]` | **dropped** — silent padding miss; BL-17 | backlog |
| `F.encode` | charset `'UTF-8'` on those strings | `[b'Spark', b'Apache', b'A', b'', None]` | planning error: encodings are `base64`, `base64pad`, `hex` | **dropped** — repark refuses Spark's charset | backlog |
| `F.decode` | charset `'UTF-8'` of the encoded bytes | `['Spark','Apache','A','',None]` | same planning error as encode | **dropped** | backlog |
| `F.format_number` | `[1234.567, -3.1, 0.0, None]`, `d=2` | `['1,234.57','-3.10','0.00',None]` | `UnsupportedOperationException` (`R-FN-BATCH3`) | **dropped** — loud refuse | backlog |
| Unicode row (kept names) | `['héllo','日本語','𝄞ab','straße','İstanbul']` | length `[5,3,3,6,8]`; upper `['HÉLLO','日本語','𝄞AB','STRASSE','İSTANBUL']`; ascii `[104,26085,119070,115,304]`; lpad `'ab'` to 8 `['abahéllo','ababa日本語','ababa𝄞ab','abstraße','İstanbul']`; btrim `'abİ'` `['héllo','日本語','𝄞','straße','stanbul']` | same | kept | `length.py` / `case.py` / `padding.py` |

`F.encode(..., 'base64')` is the inverse: repark returns unpadded `'U3Bhcms'` /
`'QQ'` and Spark analysis-refuses (`INVALID_PARAMETER_VALUE.CHARSET`, expects
utf-8 and siblings). Not a Spark-honest example.

## Gates

Measured 2026-09-03 on the merged tree (HEAD after critic round 2). Counts line
(both coverage legs identical):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 284 covered; 627 backlog; 2 exceptions; 71 examples`

At dispatch (`d7e2c4a`): `BACKLOG_BASELINE = 844`, this batch's first landing moved
844 → 814 (30 names). After critic round 2 the three silent misses return, so
the dispatch-equivalent is 844 → 817 (27 names). As merged on `887695c`: main
is 654, this branch after the drop is 627 (654 − 27).

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py` | **0** |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `uv run --no-sync ruff check docs/examples python/repark/tests` | **0** |
| `uv run --no-sync ruff format --check docs/examples python/repark/tests` | **0** |
| `docs/examples/functions/{case,concat,edges,format,length,matching,padding,unbase64}.py` each | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_fn_initcap*.py python/repark/tests/test_fn_chr*.py python/repark/tests/test_fn_trim_chars.py python/repark/tests/test_bl17_base64_padding.py` | **0** |

## Cost

| | |
|---|---|
| Wall-clock start | 2026-09-03T11:40:17Z |
| Wall-clock end | 2026-09-03T12:20:00Z |
| Disk at pickup | 569 G free of 1.8 T |
| Cleanup | throwaway `/tmp/oc-ex4-oracle/` Ivy download; no `target/` rebuild; no `make develop` |

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) BL-17, FN-INITCAP-1, FN-CHR-1, FN-TRIM-CHARS-1

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-4-functions-strings-a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every kept name's asserted cells equal the live Spark 4.1.2 dump on the example's own inputs including the Unicode row; the seven dropped names keep both Spark and repark values in this ledger; C-001 is cited from scripts/map.md and the BL-17 / FN-INITCAP-1 / FN-CHR-1 / FN-TRIM-CHARS-1 pins.
      artifacts: [docs/examples/functions/case.py, docs/examples/functions/concat.py, docs/examples/functions/edges.py, docs/examples/functions/format.py, docs/examples/functions/length.py, docs/examples/functions/matching.py, docs/examples/functions/padding.py, docs/examples/functions/unbase64.py, scripts/map.md, python/repark/tests/test_bl17_base64_padding.py, python/repark/tests/test_fn_initcap_divergence.py, python/repark/tests/test_fn_chr_divergence.py, python/repark/tests/test_fn_trim_chars.py]
    - id: AT-2
      status: ATTACKED
      evidence: Empty string, NULL, negative left/right width, pad truncation at width 3, concat NULL vs concat_ws skip, and the Unicode row ['héllo','日本語','𝄞ab','straße','İstanbul'] (length [5,3,3,6,8], upper, ascii, lpad ab-to-8, btrim abİ) were measured equal on Spark 4.1.2 and asserted. initcap and chr/char were measured unequal and dropped.
      artifacts: [docs/examples/functions/edges.py, docs/examples/functions/concat.py, docs/examples/functions/padding.py, docs/examples/functions/length.py, docs/examples/functions/case.py]
    - id: AT-3
      status: ATTACKED
      evidence: F.format_number raises UnsupportedOperationException; F.encode/F.decode with UTF-8 raise a planning error; F.base64 / F.initcap / F.chr silently diverge and are dropped plus pinned; F.trim two-arg is TypeError; F.locate 3-arg is UnsupportedOperationException.
      artifacts: [task/ledgers/staging/ex-4-functions-strings-a-ledger.md, python/repark/tests/test_bl17_base64_padding.py, python/repark/tests/test_fn_initcap_divergence.py, python/repark/tests/test_fn_chr_divergence.py, python/repark/tests/test_fn_trim_chars.py]
    - id: AT-4
      status: N/A
      justification: Scalar string examples on a local frame; no shared mutable engine state and no ordering-dependent aggregate.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, or secret handling. Examples are local-only. Oracle script lives under /tmp/oc-ex4-oracle/ and is not committed.
      artifacts: [docs/examples/functions/case.py]
    - id: AT-6
      status: N/A
      justification: No product kernel change; the BL-17 pin records today's unpadded base64 so a later fix is loud.
    - id: AT-7
      status: N/A
      justification: Eight tiny local frames; no resource surface.
    - id: AT-8
      status: ATTACKED
      evidence: Spark charset contract for encode/decode is utf-8 and siblings; repark's base64/hex charset is the inverse domain and is not taught as Spark encode. format_number stays the disclosed R-FN-BATCH3 refusal.
      artifacts: [task/ledgers/staging/ex-4-functions-strings-a-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: BL-17, FN-INITCAP-1, FN-CHR-1 and FN-TRIM-CHARS-1 are the operability record; the seven dropped names stay on docs/examples/backlog.txt with both values here.
      artifacts: [docs/spark-sql-iceberg-parity.md, docs/examples/backlog.txt]
    - id: AT-10
      status: ATTACKED
      evidence: Each COVERS name is selected and compared to the Spark cell; BL-17 / FN-INITCAP-1 / FN-CHR-1 / FN-TRIM-CHARS-1 assert repark's current answers; the coverage gate execute leg runs every example.
      artifacts: [python/repark/tests/test_bl17_base64_padding.py, python/repark/tests/test_fn_initcap_divergence.py, python/repark/tests/test_fn_chr_divergence.py, python/repark/tests/test_fn_trim_chars.py, scripts/check_example_coverage.py]
  reattested: []
  complete: true
```
