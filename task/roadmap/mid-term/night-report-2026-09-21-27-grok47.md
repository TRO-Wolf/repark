# xo-grok47 finish report — IPI-51 exception-type slice

Closed 2026-09-21 21:08 EDT on the finish wake (tick 33). Nothing new was launched. Critic unit `grok-xr-xt-etype-010435` is still running. CI on the pushed head is still pending. The pull request is not queued and not merged.

## Unit

IPI-51 D-2.1: cells that raise catch-all `PySparkException` where Spark raises a typed exception. Measured on origin/main `3dc40d98` (evidence `ticks-xo-grok47/001/measure-summary.json`). TAKE is four cells, claimed at claims line 932, grok2 NO OBJECT at line 937. D-2.3 / C-013 both-ParseException count is 0, so no caret work was cut.

## PRs

repark#783 OPEN, not draft, MERGEABLE. `mergeStateStatus` is BLOCKED because required checks on this head are still running. Branch `fix/ipi-51-type-merge-card`. Pushed head `e9386339d2aa630dd08c86f3851aea9ca707bb75`. Base `13b174d1db7e30f5d301016ca07e17a91481fbcc`. Title: Stamp MERGE_CARDINALITY_VIOLATION for duplicate-source MERGE. https://github.com/TRO-Wolf/repark/pull/783

The iceberg pin on `13b174d1` is `97f9b8a32226e03e21bec053ce39f3ec9cb10771` on all five iceberg git lines. `5a317f07` is absent from that Cargo.toml. Open PR #787 (`77a4bfe3`, IPI-05 WAP) is still OPEN, mergedAt null, and has not moved main. Queue file `/tmp/oc-worker/run16/merge-queue.txt` is 0 bytes. 783 is not in it. C-012 stays OPEN.

Local gate on this head finished GATE_OK at 20:57:08 against that same main: lint FMT=0 CLIPPY=0 PANIC=0, MAP=0, comment-ban hits=0, CB=0 R=0 T=0 U=0 L=0. Ten named Rust filters each logged PASS, including the merge-on-read pin. Pytest `test_ice_error_conditions_1.py`: 43 passed offline and 43 passed live. `merge/mod.rs` blob `8a3f714a8bd5a1cec35b5d44e8584b8cc60eb829` is 1654 lines. Three-dot vs `13b174d1` is 13 files, +90/−23. Identity TRO-Wolf on all five commits. Trailers: four Muse Spark, one GLM 5.3 Flash. No co-author, no assisted-by. Receipts: `/tmp/oc-worker/xt-etype-rebase.result` and copies in `ticks-xo-grok47/032/`.

CI on `e9386339` at 21:08 EDT (no required failure):

| Check | State |
| --- | --- |
| cargo-deny | pass (24s) |
| taplo | pass (12s) |
| Python | pass (1m20s) |
| Repo guards | pass (28s) |
| typos | pass (12s) |
| zizmor | pass (10s) |
| Rust lint (fmt + clippy + check) | pending, job 106577740843, run 35674411429 |
| Rust test (workspace) | pending, job 106577740682, run 35674411429 |
| build + import smoke (debug, host) | pending, job 106577740272, run 35674411321 |
| abi3 wheel | skip |
| manylinux release wheels | skip |

That CI does not authorize a queue. The green CI on `eff8a121` covers `eff8a121` only.

Critic unit `grok-xr-xt-etype-010435` is ACTIVE (SubState=running, MainPID 3255853, InvocationID `a4ea9dc748e946c995e75c91e44f13d4`). Stamp `/tmp/grok-worker/xr-xt-etype/20260922T010435Z/`. At 21:08 `out.json` is 0 bytes and `stderr.log` is 0 bytes. `Result=success` while ActiveState=active is not a finished critic. xr clone `/tmp/xr-xt-etype` HEAD `e9386339`, dirty only untracked `.venv`. Brief `/tmp/oc-worker/run27/wo-xo-grok47/pr1-critic-brief-e9386339.md`.

Local `/tmp/xt-etype` HEAD is `e9386339`, porcelain empty. Clones were left in place: the PR has not merged and the cell replay is not banked.

Sha map (`4596de36`-based → `13b174d1`-based):

- `a7bf6206` → `06c7d0762c66747fe25ee5f750a13de79d25bd56` stamp (Muse)
- `cd503165` → `d3da9de863ba1249dba2d2bccfb6ad692212c398` constructor pin (Muse)
- `c64c7c4f` → `c6850eb45f1c616dc205d5ed3cd588090be89440` four maps (GLM 5.3 Flash)
- `ccd26d56` → `7a30cd57c669ecc2b1576ae08be15e8495bd795f` CAP-1 mirror (Muse)
- `eff8a121` → `e9386339d2aa630dd08c86f3851aea9ca707bb75` MoR pin (Muse)

## Rounds per executor tier

Muse (max), 3 launches:

| Stamp | Result |
| --- | --- |
| `20260921T172556Z` | Provider death, zero commits. One relaunch spent. |
| `20260921T181051Z` | exit 0, CONCLUDED. Accepted. Now `06c7d076` and `d3da9de8` after the rebase onto `13b174d1`. |
| `20260921T205628Z` | exit 0, CONCLUDED. CAP-1 mirror now `7a30cd57`. MoR pin now `e9386339`. |

GLM 5.3 Flash, 1 launch:

| Stamp | Result |
| --- | --- |
| `20260921T185556Z` | exit 0, CONCLUDED. map.md lockstep. Now `c6850eb4`. |

Devin: 0 rounds.

Grok critic (xreview.sh), 5 launches:

| Stamp | Verdict |
| --- | --- |
| `20260921T201358Z` | PASS on `d83ef337` only. 44 turns, about $1.06. Residual V-001. |
| `20260921T221142Z` | PASS on `91edc2d5` only. 34 turns, about $0.73. V-001 closed by the Stage B mutation. |
| `20260921T230348Z` | PASS on `cebbde09` only. 36 turns, about $0.74. |
| `20260922T001445Z` | PASS on `eff8a121` only. 35 turns, about $0.83. Questions empty. |
| `20260922T010435Z` | In flight on `e9386339`. Unit `grok-xr-xt-etype-010435`. No verdict. Empty `out.json` is not PASS and not NEEDS_REMEDIATION. |

Critic rejections: 0. No NEEDS_REMEDIATION verdict was returned. The latest verdict that exists is PASS on `eff8a121`, and that verdict names a head that is no longer the PR head.

Orchestrator rebase onto `13b174d1` produced `e9386339` with no executor round. Pushed at 21:04 via `xpr.sh` (force-with-lease, comment-ban hits=0).

## Gates

One false red: the 17:57 local gate returned `T=1` because four Spark filter names were joined with `+` and `cargo test` rejected the extra positional arguments. Evidence: `ticks-xo-grok47/022/false-t1/`. That was a gate-command mistake, not a product failure. Later gates use one filter name per `cargo test`.

Green receipts that do not cover the pushed head: `91edc2d5` against `fa8f9b4d` (`ticks-xo-grok47/024/`), `cebbde09` against `0fbaeaa7`, `eff8a121` against `4596de36` (`ticks-xo-grok47/031/`, 20:09).

Live receipt for the pushed head: lint and the local gate on `e9386339` are green against main `13b174d1` / pin `97f9b8a3` (TIME 20:57:08, `/tmp/oc-worker/xt-etype-rebase.result`, `/tmp/oc-worker/xt-etype-localgate.done`).

## Questions

None. No QUESTION line was written. No ruling was requested.

## Cells

Before (measured 2026-09-21; the packet's 18 was counted on 2026-09-19 and was stale): 4 cells still raised `PySparkException` against a typed Spark exception.

After: 0 closed. The cell on this PR has not been replayed, because the PR has not merged.

| Cell | Spark | This lane |
| --- | --- | --- |
| `W-MERGE-DUP-SOURCE-ERR` | `SparkRuntimeException` / `MERGE_CARDINALITY_VIOLATION` / `23K01`, A-6 substitute `AnalysisException` | On repark#783 at `e9386339`. Not replayed. |
| `W-UPDATE-TYPE-ERR` | `AnalysisException` / `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` / `KD000` | Not cut. |
| `D-ALTER-TYPE-NARROW-ERR` | `AnalysisException` / `NOT_SUPPORTED_CHANGE_COLUMN` / `0A000` | Not cut. Near-miss `D-ALTER-TYPE-STR-ERR` is Spark `Py4JJavaError`. |
| `D-CREATE-NOT-NULL-VIOLATE` | `SparkRuntimeException` / `NOT_NULL_ASSERT_VIOLATION` / `42000` → `AnalysisException` (A-6) | Not cut. |

Numbered residue, not in this PR (A-8, class-only): `D-REF-CREATE-BRANCH-EXISTS-ERR`, `D-REF-DROP-BRANCH-MISSING-ERR`, `D-REF-DROP-MAIN-ERR`, `D-REF-TAG-AS-BRANCH-ERR`, `D-REF-TAG-EXISTS-ERR`, `P-RDF-OPT-TARGET-TINY`, `V-ALTER-AS`.

T-3 `Py4JJavaError`, not this lane: `D-ALTER-TYPE-STR-ERR`, `D-DROP-PART-SOURCE-ERR`, `P-ADD-FILES-CHECK-DUP`, `P-EXPIRE-GC-DISABLED-ERR`, `P-POS-CHERRYPICK`, `P-ROLLBACK-SNAPSHOT-MISSING-ERR`, `P-ROLLBACK-SNAPSHOT-NONANCESTOR-ERR`, `TY-PROMOTE-DATE-TS`.

`TY-GEOMETRY` is grok2's, merged in #784.

D-2.3 / C-013: both-ParseException count is 0. No caret work order.

## What I would do next

1. Let `grok-xr-xt-etype-010435` exit. Read `/tmp/grok-worker/xr-xt-etype/20260922T010435Z/out.json`. Trust PASS only when `structuredOutput.status` is CONCLUDED, the first word of `structuredOutput.summary` is PASS, and the summary names HEAD `e9386339d2aa630dd08c86f3851aea9ca707bb75`.
2. Queue `783 IPI-51-PR1 <HHMM>` only when that PASS, green CI on `e9386339` (abi3 and manylinux skips are the usual skips), main still `13b174d1`, pin still `97f9b8a3`, and the 20:57 GATE_OK receipt still match this head. Then `drive-merge.sh 783` under systemd-run. Pending checks are not a queue input.
3. On NEEDS_REMEDIATION, name each finding's class and run one executor round that fixes those findings and sweeps the whole PR for each class, then rebase-if-needed, re-gate, re-push, and a fresh critic. The `eff8a121` PASS does not cover a fix commit.
4. If a required check on `e9386339` fails, read that job log and treat it as its own class. A PASS that predates the fix commit does not authorize the queue.
5. If main moves and the pin on the new main is still `97f9b8a3`, the current gate and any PASS against `13b174d1` do not authorize the queue. Rebase only with no critic and no executor active, then re-gate, re-push, and run a fresh critic. If the pin is `5a317f07`, stop and do not rebase onto that main.
6. After merge, replay `W-MERGE-DUP-SOURCE-ERR`, write the before/after count, then remove `/tmp/xt-etype` and `/tmp/xr-xt-etype`. Cut `W-UPDATE-TYPE-ERR`, then `D-ALTER-TYPE-NARROW-ERR`, then `D-CREATE-NOT-NULL-VIOLATE`, one PR each. Leave T-3, `TY-GEOMETRY`, the class-only residue, and C-013 alone.

Disk at close: 540G available on `/tmp`.
