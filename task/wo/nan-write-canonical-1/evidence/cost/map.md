# task/wo/nan-write-canonical-1/evidence/cost/ — the write cost row (rule 6)

NAN-WRITE-CANONICAL-1, 2026-11-13, model `claude-opus-5-5`. The bar is 1.05x on the like-for-like median.

**Builds.** Base: main `1d9d2808` (fork `92242cb4`), `maturin build --profile release` in its own worktree and
target directory (module sha256 `9c420a37a8881dee1d2f6b3f570e3c3ca48860ffb704b1c44b26e6904fc15b94`, the step 0 build's).
Head: `e100717f` (fork `d6592644`), `maturin develop --profile release` (module sha256
`dd1009157ad8375a8e55ab15cae499f1d322e7de8d430a8faaac8289a4a4d2f4`).

**Method.** `cost_probe.py` writes 10 M rows `(id BIGINT, p DOUBLE, d DOUBLE)` from `range` through the Spark door
into a local Hadoop-catalog Iceberg table and times the write statement alone: `INSERT` into a plain table, `INSERT`
into a table `PARTITIONED BY (p)` (eight values of `p`), and a partitioned CTAS; each with no NaN and with 1 % of
`d` and `p` a sign-set NaN (`sqrt` of a negative column, `fff8…`), so the head rewrites them. One process per
sample runs all six cases after an untimed warm-up write, and checks 10 M rows per table. Six base/head pairs with
the order alternating (samples 1–12), then one base/base pair (101, 102) for noise. Each sample held, in order,
the exclusive measure lock, both Opus cargo locks and the JVM lock, on cores 48–57; nothing else of the lane ran.

**Result** (`cost_probe.py --render --out cost-raw.jsonl --noise 101,102`):

| case | base median s | head median s | head / base | base-base noise |
|---|---|---|---|---|
| u_insert_nonan | 0.197 | 0.198 | 1.005x | 1.084x |
| u_insert_nan1pct | 0.213 | 0.217 | 1.019x | 0.896x |
| p_insert_nonan | 0.756 | 0.783 | 1.036x | 0.989x |
| p_insert_nan1pct | 0.767 | 0.792 | 1.032x | 1.082x |
| p_ctas_nonan | 0.761 | 0.778 | 1.023x | 1.011x |
| p_ctas_nan1pct | 0.769 | 0.806 | 1.048x | 0.983x |
| sum of medians | 3.463 | 3.574 | 1.032x | |

Every case is under the bar; the largest, the partitioned CTAS with 1 % NaN, is 1.048x, inside the base/base
spread of 0.90x–1.08x per case.

- [cost_probe.py](cost_probe.py) — the sample (`--label --sample --warehouse --out [--rows]`) and the render (`--render --noise`).
- [cost-raw.jsonl](cost-raw.jsonl) — one line per case per sample: label, sample, case, rows, seconds.

Up: [../map.md](../map.md).
