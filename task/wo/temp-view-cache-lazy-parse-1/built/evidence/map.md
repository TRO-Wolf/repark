# task/wo/temp-view-cache-lazy-parse-1/built/evidence/ — A1's probes, recordings and verdicts

Scratch paths read `S` (the lane scratchpad) and `N` (the session scratchpad); none is in the repository.

- [probes/](probes/map.md) — the new probes (comment-free Python).
- [raw/](raw/map.md) — the recordings and verdicts.

## Provenance

- **RePark main:** `S/ddu-work/pkg-rp1d-52f468ce` (`BUILT_FROM` `52f468ce`, main's product tree). **Head:**
  `S/ddu-work/pkg-a1-a55e6a5f` (`BUILT_FROM` `a55e6a5f`, the first product commit), then the final head package
  named in [../map.md](../map.md). The `lane-nan-write-canonical-1` interpreter with the package on `PYTHONPATH`,
  never with `-I`; every log printed `repark._native.__file__` from its package.
- **Spark:** PySpark 4.1.2 (the c26 oracle), Zulu JDK 17, `local[1]`, UI off, shuffle partitions 1, Iceberg
  1.11.0's runtime jar, a hadoop catalog `lc`, `spark_slot.sh`.
- **Runs** (2026-11-20): one command at a time under the shared measure lock on cores 48-57, `prlimit --as=48 GiB`.
  Step 0's `cells.py` (groups `R`, `Q` under ANSI on; `C`, `N`, `U`, `T`, `L` under both modes), `missed.py`,
  Fable's `child.py` (`N/fable-cl/`) and [probes/f8.py](probes/f8.py) on the head; `f8.py` on Spark and main; the
  pin recorder (`python/repark/tests/session/_record_temp_view_cache_lazy_parse_1.py`) on Spark, main and the head;
  the reader sweep (`N/view2-delta3-work/p1probes/readers.py`, with an ORC fixture written by Spark) on main and the
  head. Step 0's Spark, main and base recordings are reused from [../../evidence/raw/](../../evidence/raw/map.md).
- **Analysis:** `python3 probes/bar_cmp.py <step 0 raw> <head bar dir> <Fable's dir> <F8 dir> <out>`.

Up: [../map.md](../map.md).
