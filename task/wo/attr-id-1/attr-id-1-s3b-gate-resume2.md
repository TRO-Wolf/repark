# ATTR-ID-1 S3b gate, continued (same session; lane /tmp/xattr, head 29eeb817)

You ran out of steps after committing 29eeb817. **Your detached t-head-2 replay died with the round.** A Muse round runs as a systemd unit, and every process in it is killed when the round ends. Its `run.log` stops after corpus r1j3. **Never detach. Run every replay in the foreground** with `timeout` and `ulimit -v 67108864`.

29eeb817 is accepted as a follow-up. Finish the gate, and do nothing else:
1. Re-run the t-head-2 replay on 29eeb817 into `s3b/t-head-2`, overwriting it, then `compare.py`:
   - **0 REGRESSION** against `main.json`;
   - 0 of the 203 S3a gains lost;
   - the four §9c cells FIXED;
   - the 14 nested-lambda cells back to EQUAL.
   Explain the `setup J3/SJ2/TT failed AnalysisException` lines: they must be the same on main, or else they are a regression.
2. Timing: replays 2 and 3, like for like at most 1.2x main, median of 3, with FIXED cells reported separately. Use post-reboot bases built from `/tmp/xbase` (main 02c2f1be, already built) if your old base venv is gone. Say which base you used.
3. The 44-file sweep with `-n 8`.
4. `bash /tmp/xattr/gate.sh` printing GATE GREEN. The one parity failure from a torn external snapshot is environmental. Re-run it once, and record it as environmental only if it involves a `/tmp/muse-worker` inventory stamp.
5. Commit the ledger numbers: `docs(attr-id-1): record the S3b gate (replay, timing, sweep)`. Same identity, and exactly one `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>` trailer.
6. `handback.json`, with status CONCLUDED or HALT.

The halt rules of `attr-id-1-s3b-fix.md` stand. Rules: work only in `/tmp/xattr` and `.../attr-id-1/s3b/`. Do not push.
