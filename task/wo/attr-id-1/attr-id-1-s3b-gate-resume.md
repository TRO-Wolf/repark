# ATTR-ID-1 S3b: resume the gate after a reboot (same session; lane /tmp/xattr, branch feat/attr-id-1, head 6be8301f)

The owner stopped the lanes for system updates and the box rebooted. `/tmp` was wiped.
- `/tmp/xattr` is a **fresh clone at 6be8301f**, your S3b fix commit. The tree is clean, and `.venv` holds the dependencies but **no native module yet**.
- `gate.sh` is restored. `handback.json` is gone; start a new provisional one.
- `/tmp/oc-worker/direct/wo/attr-id-1/s3b/` survived, including `t-head-2`, which was only just starting. Treat it as incomplete and overwrite it.
- Your earlier steps 1–5 (B1, B2-sort, B2-lambda, B3/B4 pins and the mutations) are done in 6be8301f. **Do not redo them.**

Do only step 6 of `attr-id-1-s3b-fix.md`, the full S3b gate:
1. `make develop`.
2. Replay into `s3b/t-head-2` and compare:
   - **0 REGRESSION** against `main.json`;
   - 0 of the 203 S3a gains lost;
   - the four §9c cells FIXED.
3. Replays 2 and 3 with timing (§9d: like for like at most 1.2x main, median of 3, FIXED cells reported separately).
4. The 44-file sweep with `-n 8`.
5. `bash /tmp/xattr/gate.sh` printing GATE GREEN.
6. Every replay or probe child runs under `ulimit -v 67108864` with a time budget. Known-pathological shapes get at most 240 s.

If the gate needs a change, commit it as a follow-up with the same identity and exactly one `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>` trailer. No code comments.

Update `/tmp/xattr/handback.json` with status CONCLUDED or HALT. The halt rules of `attr-id-1-s3b-fix.md` stand.

Rules: work only in `/tmp/xattr` and `.../attr-id-1/s3b/`. Never touch `~/CodeRepos`. Never use AWS credentials. Do not push.
