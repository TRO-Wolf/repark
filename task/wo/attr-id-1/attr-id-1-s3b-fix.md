# ATTR-ID-1 S3b fix: rulings on halt H-1 (resume; lane /tmp/xattr, branch feat/attr-id-1, head 8458de7e)

Your halt H-1 was correct, and your triage is accepted. The rulings are in `/tmp/oc-worker/direct/wo/attr-id-1-design.md` **§9e**. Read it, then:

1. **B1:** add S3a's unique-engine-field guard to the filter walker, through one shared guard function. Pins:
   - `d.alias("a").crossJoin(d.alias("b")).filter(F.col("v") > 10)`, by string and by Column, must give AMBIGUOUS_REFERENCE;
   - one `j_cross` cell must refuse rather than return one side's rows.
2. **B2-sort:** a sort key with 0 output hits falls through to the engine, as on main. Pin `select("id").orderBy("v")`: it returns rows as Spark does. Also pin a key that hits two output ids: AMBIGUOUS_REFERENCE.
3. **B2-lambda:** the tokenizer skips lambda-bound variables.
   - Fetch `git fetch https://github.com/TRO-Wolf/repark.git pull/881/head:refs/remotes/pr881` and port the lambda scoping of RC4-1, RC4-6 and RC5-2 only.
   - Pins under caseSensitive=true: `filter("exists(arr, X -> X > 4)")` returns rows; `filter("transform(arr, (a, i) -> a + i)[0] > 1")` works; and a column named like the lambda variable outside the lambda still binds.
4. **B3/B4:** a dotted token is qualifier plus name only when its head matches a plan relation qualifier. Otherwise it takes main's path unchanged. Pins:
   - `filter("T.id > 3")` on a `(T struct<id>, id)` frame does struct access, with rows;
   - the `q_filt` shape raises AMBIGUOUS_REFERENCE as on main;
   - a real join-qualified `filter("L.id > 1")` binds.
   Measure every pin against Spark with `/tmp/oc-worker/_lib/jvm-lock.sh /tmp/sparkenv/bin/python` (JAVA_HOME=/usr/lib/jvm/zulu-17-amd64).
5. **Mutations**, each turning its pins red, then reverted with `git status` clean:
   - drop the uniqueness guard;
   - refuse on 0 sort hits;
   - make the tokenizer lambda-blind;
   - make dotted tokens always qualifier plus name.
6. **Full S3b gate:** `make develop`; replay into `s3b/t-head-2`, then compare.
   - **0 REGRESSION** against `main.json`, and 0 of the 203 S3a gains lost.
   - The four §9c cells stay FIXED.
   - Then replays 2 and 3 with timing (§9d: like for like at most 1.2x main, median of 3; report FIXED cells separately), the 44-file sweep with `-n 8`, and `bash /tmp/xattr/gate.sh` printing GATE GREEN.
   - Every replay or probe child runs under `ulimit -v 67108864` with a time budget. Known-pathological shapes (deep concat, 200+ chained filters) get at most 240 s.

## Commit (commit first, and keep `handback.json` provisional)
- Commit: `fix(attr-id-1): S3b binds only unique engine fields, sort misses fall through, lambda variables and struct access stay out of resolve (S3b H-1)`.
- Identity: `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
- Exactly one trailer: `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`.
- **No code comments** in any source file. Update `map.md` in lockstep. Extend the ledger. Never raise a ceiling.
- Hand-back: `/tmp/xattr/handback.json`.

## Halt when
- A cell EQUAL on main still moves away, after the four rulings, for a reason none of them covers.
- The like-for-like timing is above 1.2x.

## Rules
Work only in `/tmp/xattr` and `/tmp/oc-worker/direct/wo/attr-id-1/s3b/`. Never touch `~/CodeRepos`. Never use AWS credentials. Do not push.
