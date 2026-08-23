# Unit ledger — FNP-3 · de-stub what the engine already shipped

**Unit:** FNP-3 · **Date:** 2026-08-20 · **Executor:** Claude (Opus 5) ·
**Branch:** `feat/spark-function-parity` · **Base:** `2adda76` (FNP-2) ·
**Charter:** [fnp-0-charter-ledger.md](../../staging/fnp-0-charter-ledger.md) clauses **C-009**, **C-012** ·
**Design:** [../docs/design/spark-function-parity.md §7](../../../../docs/design/spark-function-parity.md).
**SEPMO:** STANDARD. Floor S1.

**Writable:** `crates/repark-functions/src/expr_fn.rs`,
`crates/repark-python/src/column/function_dispatch.rs`,
`python/repark/src/repark/spark/{functions.py,functions_expr.py}`, the facade tests, this ledger,
`task/map.md`, the touched `map.md` files. Registry / STATUS / lockfiles / `.github` closed —
the two deferred rows below are handed to FNP-Z.

## The class

Eleven names raised `UnsupportedOperationException` from the facade while
`spark.sql("SELECT <name>(...)")` evaluated them correctly. `register_all` installs the
`datafusion-spark` kernel by name; the facade's dispatch table simply had no arm. **The capability
was present the whole time — only one of the two doors could reach it.**

This is FNP-1's defect class pointing the other way. FNP-1 had both doors reachable and resolving
*different* kernels; here one door refused a kernel the other door used. One stub said so in its
own docstring:

> `map_from_arrays` — *"Unsupported as Column builder (SQL `map_from_arrays` may work; R-FN-BATCH2)"*

so the asymmetry was observed and disclosed rather than closed.

## Shipped — 11 names

`sha1` · `sha` (new; Spark's older spelling) · `crc32` · `xxhash64` · `soundex` ·
`format_string` · `printf` (free — it already delegated to `format_string`) · `datediff` ·
`from_utc_timestamp` · `to_utc_timestamp` · `map_from_arrays`.

`__all__` 338 → 339. Each name gets an `expr_fn` builder embedding the **same singleton** the
registry installs (`make_udf_function!` hands out one instance), so C-012 holds by construction
rather than by inspection.

### The `datediff` ruling, overridden with reasons

`functions_datetime.py`'s module docstring said **"`datediff` stays the R-FN-BATCH1 DISPOSED-STUB
— do not alias it onto `date_diff`"**, and `date_diff`'s own docstring repeated it. That reads as
a semantic ruling. It is not one: the FN-D ledger records the actual reason —

> `date_diff` | SEMANTIC-HAZARD | **DEFERRED** — cannot pin 2-arg DATE form equal to `datediff`
> without replacing the `functions_expr` UOE (would red out-of-fence `test_fn_batch1.py`)

— a **scope fence**, not an objection. FN-D was not allowed to touch the test asserting the
refusal. PySpark 4.1.2 declares `datediff(end, start)` and `date_diff(end, start)` with the same
signature over the same Catalyst expression, so they share one dispatch arm here, and the pin
asserts the two spellings agree on value and type. The stale notes are removed.

## Deferred, with the reason — 2 names

Both were on the "kernel already ships" list and both turn out to diverge from Spark. Neither is
shipped; both keep their loud refusal and are handed to **FNP-Z** for a divergence-registry
section (this unit does not hold the registry).

| Name | Kernel | Divergence measured |
|---|---|---|
| `arrays_zip` | `datafusion-functions-nested` | Names the result struct's fields **`1`, `2`**; Spark names them **`0`, `1`**. Measured: `list<item: struct<1: int64, 2: int64>>`. Closing it needs a field-renaming wrapper, not an arm. |
| `json_tuple` | `datafusion-spark` | Returns **one struct** — measured `struct<c0: string, c1: string>` — where Spark's `json_tuple` is generator-shaped and yields **n separate columns**. Needs the facade's `_generator` machinery, not a scalar arm. |

Shipping either as a plain arm would have looked like parity and behaved differently. The census
listed both as ready; that estimate is corrected here.

## Findings

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| F-FNP3-1 | S1 | Eleven names refused on the facade while the SQL door evaluated them | `REMEDIATED` — each pinned on the Arrow path AND cross-checked against the SQL door for value and type |
| F-FNP3-2 | S2 | `arrays_zip` struct field naming diverges from Spark (`1`/`2` vs `0`/`1`) | `ACCEPTED_FLAGGED` — not shipped; registry row handed to FNP-Z |
| F-FNP3-3 | S2 | `json_tuple` returns one struct where Spark returns n columns | `ACCEPTED_FLAGGED` — not shipped; registry row handed to FNP-Z |
| F-FNP3-4 | S3 | A "do not alias" note read as a semantic ruling but was a scope fence | `REMEDIATED` — overridden with the ledger citation; the stale notes are removed so the next reader is not misled the same way |

## Oracles

`crc32` and `sha1` are fixed algorithms, so the pins check against **`zlib.crc32` and
`hashlib.sha1`** rather than against RePark's own output — an engine that agrees with itself is
not evidence. `xxhash64` has no stdlib oracle available here; its pin asserts determinism,
distinctness and the `int64` return type, and says so rather than implying more.

## Gates

| Gate | Result |
|---|---|
| `cargo test --workspace --no-fail-fast` | **45 binaries, 1,987 passed, 0 failed**, cargo exit 0 |
| `make ci` | exit **0**. Three reds on the way, all mechanical: a clippy `empty line after doc comment` (a `///` section banner with no item under it — converted to `//`), an unused `# noqa: S324`, and one over-long parametrize row. |
| facade pytest (full) | first run **1 failed, 3,466 passed, 70 skipped** — `test_datediff_stub_untouched`; after converting it, **3,467 passed, 70 skipped, 0 failed** |
| C-012 door-parity guard | extended from 7 to **18** scalar spellings; all agree |

## The second fence, found by the suite

FN-GT2 had turned the `datediff` fence into a *test* — `test_datediff_stub_untouched`, asserting
the refusal — and its module docstring repeated the claim. So the same scope fence existed in
three places: a module docstring, a function docstring, and a passing test.

That is worth naming. A fence recorded as a passing assertion is indistinguishable, from the
outside, from a semantic decision: the test says the behaviour is intended and the docstring says
"do not". Only the FN-D **ledger** carried the actual reason — *"cannot pin ... without replacing
the `functions_expr` UOE (would red out-of-fence `test_fn_batch1.py`)"*. Without that sentence
this unit would have had to either respect a ruling that never existed or override it blind.

All three sites are now updated together, and the test asserts what is true — that the two
spellings are one function — rather than that one of them refuses.
