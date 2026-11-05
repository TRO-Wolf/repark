# Card STAGED-PUBLISH-OPTIONS-PATH-1: version numbering and the begin-create file on the options path

**Date:** 2026-11-04. **Filed by:** the records clerk (`claude-haiku-5-5`), from the unit's builder question 4 (ledger clause C-008).

**Status:** open. Grade C. S3. Not dispatched; no build is owed until the owner charters it.

**Why:** On the options and session-write-conf path (`publish_applied`), a hadoop RTAS writes its metadata through `Transaction::apply_locally` in the fork, which takes `with_next_version` of the staged vN. The pointer can then land on v<N+2> with v<N+1> missing, and a stale-hint Java walk would stop short of it. A uuid create on the same path leaves the begin-create `00000` file as stray metadata on success. Muse confirmed the mechanism in the fork code; no verifier measured it.

**Fix:** Not designed. First measure both doors on the options path (a hadoop RTAS, and a uuid CTAS on success) with the Java reader, then pin the result or fix the numbering.

**Relation:** [STAGED-PUBLISH-CLEANUP-1](staged-publish-cleanup-1-card-2026-11-04.md) (its ledger lead L1, clause C-008 of [the ledger](../../ledgers/completed/staged-publish-cleanup-1-ledger.md)).
