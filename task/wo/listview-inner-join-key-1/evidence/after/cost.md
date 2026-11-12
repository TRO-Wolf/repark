# Cost (ruling 8): base `21b339cb` against head `5aaeb19b`

**Gate:** the plain List key-name join, the unchanged path, at most 1.05x, median of three, interleaved.
**Result: it passes on both cases.** The cast path is reported beside it with no bar.

| Case | What | Base median (s) | Head median (s) | Head / base |
|---|---|---|---|---|
| execute, `List` key | `left.join(right, "k").agg(count, sum)` collected five times after two warm-ups; 200,000 × 200,000 rows, key `array(id % 50000, id % 3)` | 0.1690 | 0.1609 | **0.952** |
| build, `List` key | 2,000 `left.join(right, "k").columns` builds after two warm-up rounds (the helper's own cost: one schema lookup and a `can_hash` per key) | 0.4465 | 0.4472 | **1.0016** |

**The cast path, head only** (on base the same frames are a 200,000 × 200,000 cross product):

| Case | `ListView` key, head (s) | `List` key, head (s) | ListView / List |
|---|---|---|---|
| execute | 0.2502 | 0.1609 | 1.555 |
| build | 0.4539 | 0.4472 | 1.015 |

Every execute record answers the same row, `[300000, 29999850000]`, on base and head and for both key kinds.

The runs, in seconds, in the order they ran (per run: execute base, head; build base, head; then the cast path):

| Case | Base run 1, 2, 3 | Head run 1, 2, 3 |
|---|---|---|
| execute, `List` | 0.1605, 0.1750, 0.1690 | 0.1682, 0.1609, 0.1552 |
| build, `List` | 0.4380, 0.4573, 0.4465 | 0.4294, 0.4472, 0.4530 |
| execute, `ListView` | — | 0.2502, 0.2422, 0.2660 |
| build, `ListView` | — | 0.4539, 0.4520, 0.4565 |

## How it was measured

- **Builds.** Both release wheels, built by `maturin build --profile release` and installed into scratch sites: base
  from `HEAD~1`'s `crates/` in a copy of the lane's own target directory, head from `5aaeb19b`. `_native.abi3.so`
  md5: base `3eac2ead…`, head `adf3cd71…`; `__debug_assertions__` false on both.
- **Runs.** [cost_probe.py](cost_probe.py), one case per fresh process, the clone's CPython 3.12.3 with the leg's site
  first on `PYTHONPATH`, each process pinned to cores 32–47, the whole run inside `flock -x` on the measure lock and
  the build lock. `cost-raw.jsonl` holds the 18 records; `cost_probe.py --report cost-raw.jsonl` prints the medians.
