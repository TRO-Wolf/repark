---
name: audit-repark-parity
version: "1.0"
description: >-
  Audit a repark surface (a function family, a type-inference path, an error
  contract) for parity against the pinned live PySpark oracle, and turn every
  measured divergence into a fix, a pin, or a reported registry finding —
  never prose. Reach for it to triage a parity-live nightly red (mandatory
  first step, before any repair), inside any PR that changes a Spark-visible
  default or error contract (the affected pins re-measure in the same PR), or
  as a periodic sweep over the pinned set before a release. Do not use it to
  author new features; it measures and classifies existing behavior.
---

# Audit repark parity

An audit procedure adapted from Apache DataFusion's
`audit-datafusion-spark-expression` skill — the process discipline ported, the
mechanics rebound to this repo. The testing contract it serves is
[docs/testing.md](../../../docs/testing.md) (check mode vs live tier, golden
drift vs oracle drift, disclosures); this skill never restates that contract,
it walks a session through honoring it.

## When it runs

1. **First step of any parity-live nightly-red triage.** The output is a
   classification — product bug / disposed divergence / stale pin — *before*
   any repair. The repair follows from the class, never precedes it.
2. **Inside any PR that changes a Spark-visible default or error contract.**
   The audit runs over the affected pin surface in the same PR, so pins move
   in lockstep with the product and the nightly never inherits a known red.
   (The FA-4/G15 demotions in #205 are the cost of skipping this step: the
   flipping PRs landed months before their pins were swept.)
3. **A periodic sweep at campaign close or pre-release** over the full pinned
   set, so a release does not ship on stale claims.

## Step 0 — identify the surface and its recorded claims

Name the surface, then collect every claim the repo already makes about it:
pins in `_PINNED_PASSING_APACHE_TESTS` and the known-FAIL meta pins
([python/repark-parity/compat/smoke_suite.py](../../../python/repark-parity/compat/smoke_suite.py)),
scenario-registry goldens and disclosures, and divergence-registry rows in
[docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

**An existing expectation is a claim, not evidence.** Every claim collected
here gets re-measured in Step 1 — including the incidental control assertions
beside the one under suspicion. An assertion that cannot fail is not a
control.

**A path that does not resolve is a stop condition, not a step to skip.** An
empty grep over a missing directory and a grep that genuinely found nothing
produce the same output. When a path this skill names does not exist, say so
in the report, re-find it by content, and mark every dependent conclusion
unestablished. An unread source is never an absence of findings.

## Step 1 — measure against the live oracle

The baseline is the pinned local PySpark oracle (the version is pinned in
`uv.lock` via the `record` extra — read it from the repo, do not assume). No
expected value and no pin status is recorded without being observed live.

- **The banner ritual.** The proof of which Spark you are talking to is a
  banner printed from a live `SparkSession` — version and session timezone —
  at the top of the measurement session. Package metadata proves what is
  installed, not what answered. Beware `SPARK_HOME`/`PYSPARK_PYTHON`/
  `JAVA_HOME` shadowing (an ambient Java 11 kills the gateway with
  `JAVA_GATEWAY_EXITED`; the Makefile's `PARITY_LIVE_JAVA_HOME` knob is the
  fix), and remember an `unset` does not persist between tool calls — put it
  in the same command as every JVM-starting invocation. The session timezone
  follows the box unless pinned — that is why the banner prints it: a
  tz-sensitive measurement without the banner is a measurement of an unknown
  zone.
- **Streams and exit codes.** Keep stdout and stderr separate — `2>&1` makes
  output unreadable exactly on the error cases you came for. Judge commands
  by `cmd > log; echo $?`, never through a pipe.
- **Harness artifacts are not Spark results.** A Python-side
  `ValueError`/`OverflowError` raised while *collecting* a result is the
  harness failing, not Spark answering. Re-run wrapped in
  `CAST(... AS STRING)` before recording anything.
- **Enumerate as a product, not parallel checklists**: input shape × null mask
  (all-NULL / some-NULL / no-NULL) × the governing conf (ANSI on/off, the
  flipped default under audit). The bug this catches is two paths disagreeing
  while each looks correct alone. Write `-0.0` into float cases deliberately;
  it never appears in hand-written values by accident.

## Step 2 — classify every divergence

Each measured divergence lands in exactly one bucket, in docs/testing.md's
triage vocabulary:

1. **Product bug** — repark disagrees with live Spark and no ruling disposed
   it. Fix in this PR, with fail-before evidence (Step 3).
2. **Disposed divergence** — an owner-ruled default or a refuse-loud contract
   makes the difference intentional. Demote the pin to a known-FAIL meta pin
   (Step 3); never "fix" the product to make the pin pass.
3. **Stale or undisposed claim** — the pin/golden asserts something the oracle
   no longer shows (golden drift), or live Spark moved (oracle drift), or a
   disclosure converged. Repair per docs/testing.md; an undisposed divergence
   is reported as a finding for a registry row — registry rows are recorded
   orchestrator-side, the audit reports and never edits that file.

## Step 3 — apply findings. Findings do not survive as prose

Every finding becomes a fix, a pin, or a reported registry finding.

- **Fixes prove they bite.** Run the new assertion against the unfixed code
  (targeted `git stash push` / `git checkout <sha> -- <file>` of the
  implementation only) and watch it fail, then pass after. Traps: a
  `No local changes to save` message means the check just re-tested the fixed
  code; a first run that passes means the test is not exercising the fix —
  investigate, never proceed.
- **Demotions use the meta-pin idiom** already in `smoke_suite.py` (the
  `test_field_accessor` precedent, extended by the #205 demotions): row
  exists + status-class assertion + **cause-string assertions**, so the pin
  re-reds if the failure *mode* changes, not only the status. The docstring
  names the disposing PR or decision. Bait-check every new meta pin: flip one
  asserted cause string, watch it fail, flip back.
- **Deferred divergences** are captured next to the code with the correct
  oracle-measured value and a real tracking reference — never a placeholder
  link. If no tracker entry exists, omit the link and say so in the report.
- Never make an audit pass by flipping a product default or weakening an
  owner-ruled contract.

## The report

1. Surface summary and the claims collected in Step 0.
2. Measured results, with the banner line quoted.
3. The classification table (one row per divergence, one bucket each).
4. What was fixed / demoted / reported, with the bite-proof for each.
5. Unestablished conclusions — every stop condition hit, stated as such.
6. Honest confidence: an expectation this audit did not re-measure is labeled
   an unverified claim, not a fact.

Verification of the audit itself: `make parity-live` green end to end
(including the in-subprocess smoke wrapper) after any pin change, and the
standard gates per AGENTS.md "Verify before done".
