# TEMP-VIEW-CACHE-LAZY-PARSE-1 A1 — cost (partial: 2 of 10 rounds)

`flock -x measure.lock` (+ link, opus-cargo, opus2-cargo), cores 48-57, one sample at a time, the clock inside the
lock; builds per round main (`pkg-rp1d-52f468ce`), head (`pkg-a1-860c57c3`), main again. The probe is
[evidence/probes/a1_cost.py](evidence/probes/a1_cost.py) on a 2M-row JSON file under Los Angeles. Lane A's shared
holders starved the exclusive samples, and the builder's turn budget ended. **Only two rounds of the rows and one
or two of C-012 ran.** These are medians of two samples, not the ten-round bar. The next session reruns the series.
Ratio = head over the mean of main and main again.

| row | main ms | head ms | main again ms | ratio | reading |
|---|---|---|---|---|---|
| cached read, `count` (it fills) | 234.7 | 301.7 | 286.2 | 1.158 | noisy at n=2 |
| cached read, then `collect` | 1,801.5 | 1,778.7 | 1,793.9 | 0.989 | level |
| cached read, `count` then `collect`, total | 2,042.5 | 2,092.9 | 2,086.4 | 1.014 | level |
| cached read, `collect` first, total | 2,076.8 | 2,101.6 | 2,093.3 | 1.008 | level |
| a view of the cached frame: registration | 263.8 | 9.3 | 239.6 | 0.037 | the fill left registration |
| the view's first `count` | 2.5 | 406.7 | 2.5 | — | the fill moved here |
| the view, total | 266.3 | 416.1 | 242.2 | **1.637** | **over 1.15x, not yet attributed** |
| **the selective child's first `count`** | 67.0 | 332.1 | 65.1 | **5.03** | **Spark's contract: the child fills the parent; the owner rules** |
| **a second child's `count`** | 60.7 | 3.1 | 56.8 | **0.053** | reads the cache, where main re-read the source |
| C-012 `c012p6` (s) | 40.47, 39.96 | 40.81, 40.82 | 40.56, 40.23 | 1.012 | 2 rounds |
| C-012 `c012rest` (s) | 44.00, 44.01 | 44.38 | 43.66 | 1.012 | 1 round |

- **C-012** replays no `cache`, `persist` or checkpoint, so seat A does not touch its plans. It reads about 1.01x
  over one or two rounds, under the hard 1.05x, but this is not the five-round measurement.
- **The selective child (about 5x)** is Spark's behaviour for `cache()`, as Fable's §14 and the sketch forecast. It
  goes to the owner. The PR waits unmerged on that row. No pushdown-aware partial cache was written.
- **The second child (about 0.05x)** is the paired win the owner should see beside it.
- **The view's total (1.64x)** was expected to be level: the fill moved from the registration to the first read.
  The fill through the view costs about 140 ms more than the direct `count` fill (407 ms against 302 ms) and more
  than main's registration fill (264 ms). This is unattributed at n=2. The next session measures it over ten rounds
  with instruction counts (`N/perf-trial/probe.py`'s method), before the verifier.

Up: [map.md](map.md).
