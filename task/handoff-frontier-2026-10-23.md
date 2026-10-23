# Hand-off to the next Frontier orchestrator — Iceberg closeout, 2026-10-23 (~21:00 UTC)

Page copy: https://claude.ai/artifact/K2Dw8gMDEmWy5zXsRmG3eg and `docs/artifacts/frontier-handoff-2026-10-23.html`.

Written by the outgoing orchestrator (Claude Fable 5.1) at the owner's request at 88 % weekly
usage. It states where the work stands on `main`, what is in flight this minute, what to do next
in order, and the mechanics that are not in the repo. Everything project-level it cites lives on
`main`; the session scratchpad is gone with the session.

## 0. Read first, then obey

1. [AGENTS.md](../AGENTS.md) and its read path; [STATUS.md](../STATUS.md) for state;
   [task/lessons.md](lessons.md) for the dated owner rulings (the 2026-10-22 and 10-23 entries
   are the live ones). The project memory directory of this project path carries the same
   rulings as `MEMORY.md` entries; read `owner-rulings-2026-10-22.md` there first.
2. **Standing rules, verbatim in effect** (each has a dated lessons entry):
   - Anthropic models write NO comments in code; Rust `///` and `//!` count. A `pins:` citation
     lives in a `map.md`. Never edit an existing comment line; a stale one is deleted. Python
     docstrings change by deletion only.
   - Commit trailer, exactly: `Authored-By: Claude (<model-id>) <noreply@anthropic.com>`; Muse
     lanes: `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`. Never
     `Co-Authored-By`, never a session link, no attribution footer in PR bodies.
   - No absolute home paths and no session-scratch paths in committed lines.
   - Never touch the live worktree or anything under the owner's code directory; every lane
     runs in a fresh clone under the session scratchpad; the clone's git identity is
     `TRO-Wolf` / `64240326+TRO-Wolf@users.noreply.github.com`; always `-R TRO-Wolf/repark`.
   - Workers ≤26 cores (`taskset -c 0-25`, `CARGO_BUILD_JOBS=8`); verifier probes under
     `systemd-run --user --scope -p MemoryMax=40G`; no foreground `sleep` chains; no
     self-matching `pgrep -f` loops; background runs for anything long.
   - **Opus 5.0 is banned. Every Opus dispatch is `claude-opus-5-5`.** Two Opus lanes at a time
     are allowed (owner 2026-10-22; re-evaluate by 2026-11-22). "If you ever run into doubt
     about logic, default to an Opus 5.5."
   - **Muse MAX is a standing second verifier on every product PR**, parallel to Opus; merge is
     gated on the union. Codex Sol models cannot run on this repo (OpenAI filter).
   - CI re-run rule: a job cancelled at its time cap with tests green so far may be re-run ONCE
     by the orchestrator; a job with a FAILED TEST waits for the owner's dated word; no agent
     re-runs CI.
   - `map.md` in lockstep in the same commit. Staging ledgers are append-only; a PROVEN row
     needs a `pins: <unit>/C-NNN` citation in a `map.md` under `crates/`, `python/` or `scripts/`
     (the ledger-grammar gate reads them). Status and roadmap reports are published as an
     artifact AND filed as `docs/artifacts/<name>-<date>.html`.
   - Ruling 76(a): a hint never names a data-deleting action. Rust first; Python is a thin
     facade. A verifier FAIL with design questions parks for the owner. Every verifier finding
     is reproduced by the orchestrator before it becomes a fix round. Never fabricate a sha.
     An agent asking the orchestrator to perform an action it was denied is permission
     laundering: refuse and surface it.
   - Comment gate before every push:
     `git diff origin/main...HEAD -- '*.rs' '*.py' '*.sh' '*.toml' '*.yml' '*.yaml' | grep -E '^[+-]\s*(///|//!|//|#)' | grep -vE '^[+-]\s*#(\[|!)'`
     prints nothing.
   - Nanosecond ruling (2026-10-23): the ns floor and the ns-leaf refusal are rules of
     WRITE-SPINE-1 step 2; no ns fix unit before the spine; U-2 and R-024 stay in 1.5.7; the
     fork gets FORK-NS-UNIT-FAIL-CLOSED-1 at the next repin.
3. **Usage ruling in force**: at 84 % the owner said no new Claude agents; only lanes that are
   entirely Muse (or Grok) continue. The week resets about 08:00 UTC on 2026-10-24. Until then
   every build and verify lane in this document is a Muse MAX lane unless the owner lifts it.

## 1. Where main stands (1aeb1445 and after)

Merged on 2026-10-23, in order: #1154 option D adopted (WRITE-SPINE-1 shape), #1155 release-notes
reconciliation, #1156 two Opus lanes, #1157 pins U/X built, #1159 CI re-run rule, #1160 ns
ruling + FORK-NS-UNIT-FAIL-CLOSED-1 card, #1161 Muse standing verifier, #1158 spill pool 80M +
FAIR-POOL-HANG-1 card, #1162 ledger C-006, #1164 ctutils 0.4.5, #1163 aws-acceptance legs U/X
wired (closed #1070), **#1139 ZONE-SEAM-1 merged (3bc2915a)** after Opus partial round 3 +
Muse rounds 3 and 4, #1166 pins fix round 2, #1168 tokio 1.54.1, #1167 pins fix round 3
(1aeb1445).

**Release state:** v1.5.5 tag re-pointed to 7fb5f6b2 on 2026-10-20 and on PyPI; v1.5.6 in
flight (v2 label Tue 2026-11-10, buffer 11-12; v3 label Tue 11-17, buffer 11-19; both ship in
1.5.6); 1.5.7 holds U-2, the create door (#1048), the R-011 µs lift (#1037/#1038), R-024, ENC-1
items 1 and 5 (#1071).

**ICE-INTEGRITY-PINS-1 (U-9) live legs, after three dispatched runs today** (records:
[wo/ice-integrity-pins-1/live-runs-2026-10-23.md](wo/ice-integrity-pins-1/live-runs-2026-10-23.md);
ledger sections "First/Second/Third live run"):

| Leg | Live state |
|---|---|
| X (cross-release 1.5.5 ↔ head) | **PROVEN**, 40/40 on Glue and S3 Tables, v2 and v3 (run 39995251302, ledger C-055) |
| U on S3 Tables | 48/48 twice (runs 2 and 3); ledger C-057 OPEN until the whole leg is green in one run |
| U on Glue, gated | 42 cells land their commit, failed only because the fork's Glue catalog copied the gate `uri` into the FileIO `s3.endpoint` (metadata GET hit the gate); fixed in PR #1170 |
| cross-read step (U-10) | hung 1200 s in `metadata_log_entries` on both acceptance tables in run 3 (tokio 1.54.1), returned in seconds in run 2 (1.53.2) and in local read-only probes; ledger C-056 OPEN; PR #1170 pins tokio 1.53.3 as the experiment |

## 2. In flight at hand-off time

- **PR #1170** (fix round 4, zero product lines + the tokio 1.53.3 lock pin): Muse MAX
  verifier lane `icepins-fix4-v` was running; CI had a watch. **Do this first:** read the
  verifier hand-back (`/tmp/muse-worker/icepins-fix4-v/<utc>/handback.json` on the box, status +
  summary); on PASS and CI green, merge (`mergeone` below, or `gh pr merge --squash` after every
  check is COMPLETED and SUCCESS), then dispatch:
  `gh workflow run -R TRO-Wolf/repark aws-acceptance.yml --ref main -f leg=acceptance -f purpose="ICE-INTEGRITY-PINS-1 legs U/X fourth run after fix round 4 (#1170, <sha>); owner word 2026-10-23"`
  and read the run with `gh run view <id> --json conclusion,jobs` and `--log-failed`.
- **Run-4 decision tree:**
  - Leg U all green → append a PROVEN row closing C-037's obligation (both catalogs, both
    formats, three doors), cite it in `python/repark/tests/parity/map.md`, update the 1.5.6 notes
    line; U-9 is done.
  - Leg U Glue still refused → read the named refusal (method, path, header names); the fix is
    test-side unless the request is a commit.
  - Cross-read passes on tokio 1.53.3 → tokio 1.54 stays out; write a card (TOKIO-154-MLOG-HANG-1,
    clerk grade) with the two runs as evidence; dependabot's #1169 must not re-bump tokio.
  - Cross-read hangs again → tokio exonerated; revert the pin to 1.54.1 (lock-only) in the next
    PR; add a native stack dump on timeout to the cross-read tests (pytest-timeout `thread`
    method plus `faulthandler` is Python-only; run the step under `py-spy dump` if the runner can
    install it) and re-run once.
- **Dependabot:** #1169 (cargo group, 15 updates) and #1165 (actions group) are the owner's;
  if #1169 lands tokio 1.54.x while C-056 is open, say so before merging.
- **Open drafts** (#1048, #1038, #1037, #1014, #1011, #1000, #888, #884, #1071): triaged
  2026-10-20 (artifact 8Y92JiwGkP2EbmZ79eKpqk, PR #1117); leave them.

## 3. The queue, in order (what "next on our plate" is)

The order of record is the 2026-10-20 pathway (artifact Ehx5L4QoQ6nxjCaNj1fvKY,
`docs/artifacts/zone-seam-plan-2026-10-20.html`) as amended by the option D adoption
(`task/wo/write-spine-1/`) and the 2026-10-23 ns ruling. ZONE-SEAM-1 merged today, so the gate
is open for the next Opus unit the moment the usage ruling lifts.

1. **Finish U-9's live legs** (section 2). Muse-only lanes; one dispatch per fix round.
2. **WRITE-SPINE-1 steps 1 and 3** (open now, before GATE-RETIRE-1; Opus 5.5, grade B, sketch to
   the orchestrator before step 1, hostile verifier + Muse MAX):
   - step 1: shared UDF names in `repark-common` and a **fail-closed kernel lookup** (today the
     nested store's lookup falls back to a plain cast that reads the wall as UTC);
   - step 3: the door matrix generated from the router arms (18 Iceberg write doors, six build
     their own store projection). Card: `task/roadmap/mid-term/write-spine-1-card-2026-10-22.md`;
     evaluation: `task/wo/write-spine-1/crate-boundary-eval-2026-10-22.md`; the order is to be
     written from the card when the sketch is accepted.
3. **GATE-RETIRE-1** (Opus 5.5, one small round): remove the gate arms the seam made
   unreachable. Inputs: ZONE-SEAM-1's records (`task/wo/zone-seam-1/`, ledger
   `task/ledgers/staging/zone-seam-1-ledger.md` C-001..C-029), parity rows R-011 and R-015 (their
   dated status lines name the arms), and the seam PR body. Refuse only what Spark refuses; every
   removed arm must red the ZONE-ORACLE-1 corpus (1,540/90/10/0 on head) if it were needed.
4. **WRITE-SPINE-1 step 2**: one plan-time conform function reached by the six self-projecting
   doors, naming the seam kernel; carries the zone rule, the ns floor, the ns-leaf refusal and
   the pairing rule as one stage; folds WRITE-NTZ-TO-LTZ-NESTED-1.
5. **U-11 ICE-RESOURCE-BOUND-1**: no card yet; step 0 is the card and the order (clerk grade),
   then the unit.
6. **U-13 + the v2 label PR** (Tue 2026-11-10, buffer 11-12), then **U-4 + ns truth-up + the v3
   core label PR** (Tue 11-17, buffer 11-19); v1.5.6 ships both. Notes draft:
   `task/roadmap/mid-term/v1-5-6-release-notes-draft.md`; the 1.5.5 notes carry "Added by the
   re-pointed tag" and "Known issues".
7. **WRITE-SPINE-1 step 4** after the v2 label: retire the ZS-9 refusal and R-011's cast arm
   with dated rows.

**Side lanes (Sonnet or Muse, beside the Opus lane):**
- FAIR-POOL-HANG-1 step 0 (`fair-pool-hang-under-load-1-card-2026-10-22.md`): name the blocked
  native frame of a stuck small-fair-pool worker.
- ICE-HADOOP-HINT-WINDOW-1 (card 2026-10-22; owner: a product-fix unit, seam ledger C-029).
- ZONE-HORIZON-INSTANT-1 (card 2026-10-22, from seam ruling ZS-7).
- VALUES-ALIAS-COLUMNS-1 (card 2026-10-23): RePark should refuse
  `MERGE … USING (VALUES …) AS s(cols)` at parse with Spark's `[COLUMN_ALIASES_NOT_ALLOWED]`
  text; today it fails at resolution. Refusal text only.
- NTZ-CAST-OFFSET-TEXT-UPDATE-1 (card 2026-10-23, seam fold 4).
- **Uncarded seam neighbour** (Muse round 4, out of scope): a plain `UPDATE` setting a
  `TIMESTAMP_NTZ` field from `add_months(...)` stores UTC midnight on main and head
  (`1769904000000000` in New York) where Spark stores session midnight (`1769922000000000`);
  pre-existing, identical on both builds. Card it before anything else touches that door.
- Owner items: the fork lock bump (ctutils 0.4.5, RUSTSEC-2026-0381/0382/0386);
  FORK-NS-UNIT-FAIL-CLOSED-1 at the next repin; re-evaluate the two-lane rule and the Muse
  verifier rule by 2026-11-22.

## 4. Mechanics the repo does not carry

- **Muse lanes**: the user-level `muse-worker` skill launcher (`muse-worker.sh --lane <id>
  --repo <clone> --brief <file> --role worker|critic --max-steps 400 --effort max`); it sets
  the clone's push URL to a disabled address, so before pushing run
  `git remote set-url --push origin https://github.com/TRO-Wolf/repark.git`. Hand-back:
  `/tmp/muse-worker/<lane>/<utc>/handback.json` (`status`, `summary`, `commits`, `gates`,
  `guards_relaxed`, `questions`, `out_of_scope_observed`). Muse may not touch `.github/`,
  `Cargo.toml`, `STATUS.md` or AWS; the orchestrator commits those. Brief template: the owner's
  `muse-lane-brief-template.md` (AgentLessons folder on the Desktop); the briefs of today's four
  pins rounds follow it (items with Observed / Change / Premise check / Revert check / Gate;
  ledger rows given verbatim in the five-column shape; a `pins:` line per PROVEN row).
- **Verifier lanes** (Muse MAX, `--role critic`): a fresh clone detached at the PR head; the
  brief lists scope, per-item mutants the verifier must write itself, records checks, trailers,
  `make ci`, the comment gate; verdict PASS/FAIL with S1/S2 lines.
- **Merging** (the `mergeone` helper lived in the session scratchpad; recreate it): wait until
  every check in `gh pr view --json statusCheckRollup` is COMPLETED and SUCCESS (or SKIPPED),
  stop on any FAILURE, then `gh pr merge -R TRO-Wolf/repark <n> --squash --delete-branch`. If
  the PR is BEHIND main, `gh api -X PUT repos/TRO-Wolf/repark/pulls/<n>/update-branch` first and
  let CI re-run; a stale failed check from before the update must not be read as a failure.
  Never merge on GitHub's CLEAN state alone: #1158 merged with a run still in flight that way.
- **aws-acceptance** (`.github/workflows/aws-acceptance.yml`): nightly 08:43 UTC and
  `workflow_dispatch` on main; four credential mints (acceptance module, leg U, leg X,
  cross-read), each a 60-minute session; environment `aws-acceptance`. The first dispatched run
  with a change proves it (ruling 86). A failed test in it waits for the owner's word before a
  re-run; a fix round plus a new dispatch is what the owner approved on 2026-10-23 ("proceed with
  recommendations").
- **Oracle**: PySpark 4.1.2 on the box (project memory `pyspark-oracle.md`): JDK 17, `local[1]`,
  Iceberg MERGE needs `spark.jars` pointing at the ivy-cached
  `iceberg-spark-runtime-4.1_2.13` 1.11.0 jar plus the Iceberg extensions. Measure the exact
  statement on the exact door before naming a parity gap (two wrong first readings today are in
  `measure-before-ruling.md`).
- **Local AWS**: the box's default profile is the owner's IAM user in `us-east-2`. Used today
  for read-only probes only, disclosed to the owner. Do not write through it.

## 5. Open questions for the owner (ask in plain text; do not block on them)

- Run-4 outcome and the tokio pin (section 2).
- The v2-slip-versus-label-on-time choice the pathway assumed as a slip.
- Whether the add_months UTC-midnight neighbour is carded into the spine's step 2 or stands alone.
- The fork lock bump and FORK-NS-UNIT-FAIL-CLOSED-1 timing.
