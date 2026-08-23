# Unit ledger — T5 iterator-form rsi/sma (bit-exact)

**Unit:** T5 · conductor-15 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-rsix` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-rsix` · **Branch:** `grok/rsix-rsi-sma-iter` ·
**Base (FROZEN):** `8cbde88bb076cbf09976fa0bfbc702472f267fca` (conductor-14 closeout #137).

**Charter:** `planning/grok/BRIEF-conductor-15.md` T5 + A10.
Source: `planning/hardening/P3-PREP-2026-08-15.md` §2.2–§2.3 / §3 / §5
(iterator form is the win; `unsafe` is closed).

CLOSED: `udf/**` (T4), EMA, ADX, every other kernel, goldens, Cargo.lock,
`[patch.crates-io]`, STATUS, registry, `.github/`, primary checkout.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Rewrite **only** `pub fn rsi` (`momentum.rs`) and `pub fn sma` (`overlap.rs`). | PROVEN — diffs name those two loops |
| C-002 | Loop-form, not math: identical operation order (perf-note §7). No `mul_add`, no reassociation, no hoisting `as_f64(period)` out of the loop. | PROVEN — same add/snapshot/subtract/divide (SMA); same Wilder `*=`/`/=` pair + `is_zero` (RSI) |
| C-003 | P-3 `safe_iter` shape: `iter_mut().zip(...)`. No `unsafe`. | PROVEN — no `unsafe` token; zip over incoming + trailing (SMA) / incoming + out-slot (RSI) |
| C-004 | Full `repark-ta` golden suite byte-identical (`f64::to_bits`). Any bit-move ⇒ HALT. | PROVEN — 41 goldens ok, including `sma_matches_c_talib` + `rsi_matches_c_talib` |
| C-005 | Contract tests green. Lookback / seed / NaN prefix / `check_period` unchanged. | PROVEN — 11 contract + 106 lib unit (incl. `sma_*` / `rsi_*`) |
| C-006 | Criterion before/after from `ta_kernels` (`--quick`) in the PR body. Report deltas, not absolute ns (`schedutil` host). | PROVEN — §3 |
| C-007 | File-size ceilings: `momentum.rs` ≤ 2600, `overlap.rs` ≤ 1900. No EXCEPTIONS raise. | PROVEN — 2511 / 2600 and 1843 / 1900 |
| C-008 | Map lockstep: T5 one-line note on rsi/sma kernel rows only. Do not rewrite the `udf` paragraph. | PROVEN — `src/map.md` rsi/sma parentheticals only |
| C-009 | `make verify` + fresh `make develop` + `make py-test-facade` + `make preflight`. Do not merge. | PROVEN — §4 |

---

## 0. Charge

P-3 measured (host-noisy; deltas, not absolute ns):

| kernel | `idx` → `safe_iter` | vs `unchecked` |
|---|---:|---|
| RSI | **+5.15%** | identical 34-insn codegen |
| SMA | **+1.16%** | `unchecked` marginally slower |

`rsi_iter` and `rsi_unchecked` compiled to the same instruction sequence.
This unit applies that **safe** form to the shipped kernels. EMA is not
rewritten (P-3: idx/iter indistinguishable / slightly negative).

## 1. What changed

SMA hot loop (`overlap.rs`): zip `out[lookback..]`, `input[lookback..]`,
and the trailing window `input[..len - lookback]`. Statement order
unchanged: add incoming → snapshot `temp` → subtract trailing → divide
`temp / as_f64(period)` (the divide stays in-loop).

RSI hot loop (`momentum.rs`, from `period + 1`): zip `out[period + 1..]`
with `input[period + 1..]`. Seed / NaN prefix / `check_period` untouched.
In-loop `as_f64(period - 1)` / `as_f64(period)` / two divides / `is_zero`
guard stay in the same order.

## 2. Files

| Path | Role |
|---|---|
| `crates/repark-ta/src/overlap.rs` | `pub fn sma` hot loop only |
| `crates/repark-ta/src/momentum.rs` | `pub fn rsi` hot loop only |
| `crates/repark-ta/src/map.md` | one-line T5 note on the rsi/sma kernel rows |
| `task/rsix-rsi-sma-iter-ledger.md` | this ledger |
| `task/map.md` | new T5 row (Contents + Live + I-want-to) |

## 3. Criterion (`cargo bench -p repark-ta --bench ta_kernels -- --quick`)

Host: `schedutil`, noisy box. Trust **deltas**, not absolute ns.

| Kernel | Before (`ns/row`) | After (`ns/row`) | Δ |
|---|---:|---:|---|
| sma period=20 | 2.673 | 2.070 | −22.6% (host-noisy; P-1 baseline was 2.029) |
| rsi period=14 | 7.291 | 7.224 | −0.92% (inside noise; P-1 was 7.180) |

Criterion wall (same `--quick` run; criterion's own change detector said
**"No change in performance detected"** for both, p > 0.05):

| id | Before | After | criterion Δ |
|---|---|---|---|
| `sma_n1e6` | 2.0635 ms | 2.0463 ms | −1.03% (p = 0.07) |
| `rsi_n1e6` | 7.2769 ms | 7.5523 ms | +3.90% (p = 0.10) |

Do not chase absolute ns. The funding measurement is P-3's quiet-enough
round-robin (`idx`→`iter` RSI +5.15%, SMA +1.16%, identical codegen vs
`unchecked`). This `--quick` pair is recorded because A10 requires it.

Raw `TA_KERNEL` lines live in `/tmp/grok-rsix-bench/{before,after}.txt`.

## 4. Gates (real exit codes)

| Gate | Exit |
|---|---|
| `cargo test -p repark-ta` (goldens `to_bits` + contract + unit) | **0** (106 + 11 + 41 + 1) |
| `make verify` | **0** |
| `make develop` (fresh maturin) | **0** |
| `make py-test-facade` | **0** (3230 passed, 71 skipped) |
| `make preflight` | **0** (facade 3230 passed, 71 skipped; audit + zizmor) |
| pre-commit hook | (fires on commit) |

## 5. Identity / hygiene

Per-command:
`git -c user.name='TRO-Wolf' -c user.email='64240326+TRO-Wolf@users.noreply.github.com'`

Trailer: `Authored-By: Grok (grok-4.6) <noreply@x.ai>`

After every commit `%ae` == `64240326+TRO-Wolf@users.noreply.github.com`.
Two-pass hygiene. Hooks fire. xfail forbidden.

## 6. ACC

Loop-form only. Floor S1. `to_bits` is the judge. No `unsafe`.
**Label: `ACC-CONVERGED`.**
