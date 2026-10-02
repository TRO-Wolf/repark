# Continue S3b (same session; lane /tmp/xattr, head 15c1ea17)

S3b is committed. Finish its gate exactly as the brief says:
1. `make develop` in the slice, then the S0 replay into `/tmp/oc-worker/direct/wo/attr-id-1/s3b/`, then `compare.py` against `main.json` and against S3a's results:
   - 0 cells moved away from Spark;
   - S3a's gains kept;
   - the S3b gains listed;
   - the four §9c cells (`r2.{F,T}_oj_twinjoin_filt` and `r3.{F,T}_ob_agg_max`) equal Spark, or are named residues with the slice that owns them;
   - whether SORT-PARENT-COLUMN-1 (RC5-7) closes.
2. The like-for-like timing (§9d), median of 3, at most 1.2x, with the time on FIXED cells reported separately.
3. The 44-file neighbour sweep with `-n 8`.
4. `bash /tmp/xattr/gate.sh` prints GATE GREEN.
5. Ledger and `map.md` for whatever the gate finds, then a follow-up commit if anything changed.
6. Update `/tmp/xattr/handback.json` with status CONCLUDED or HALT.

Run every replay child under `ulimit -v 67108864`.
