# ICE-INTEGRITY-PINS-1: the #1070 refresh for pins U and X (proposal, 2026-10-22)

**Status:** proposal. The workflow file is untouched (sketch H-6; owner ruling 86: "I merge the
.github change"). The owner's clerk PR applies the diff below in place of the draft's workflow
hunk, carrying U-9's step and U-10's AWS leg together as ruling 86 says.
**Against:** `.github/workflows/aws-acceptance.yml` on main `a2de82aa`; `git apply --check` is clean
there. **Unit:** [the sketch](sketch.md) §6, and [the unit ledger](../../ledgers/staging/ice-integrity-pins-1-ledger.md),
C-037 and C-038.

## What changed against the draft (#1070 head, 2026-10-15)

- **Paths.** FACADE-TESTS-LAYOUT-1 (#1113) moved the acceptance modules to
  `python/repark/tests/parity/`. The integrity legs, the requirements file and U-10's cross-read
  module now point there. The draft's `python/repark/tests/<file>` paths no longer exist.
- **The previous release is 1.5.5.** The draft pins 1.5.4. Under the orchestrator's ruling of
  2026-10-22, v1.5.5 (tagged `7fb5f6b2`, on PyPI since 2026-10-21) is the newest previous release
  at the 1.5.6 label, by ruling 81's "one release-checklist line moves the pin at each tag". The
  requirements file is `python/repark/tests/parity/previous_release_requirements.txt`, and the
  import check asserts `1.5.5`.
- **Pin K's live leg is not in it.** `test_aws_acceptance_integrity_kill.py`, which the draft
  lists, was never built: pin K is local only (`kill_mid_write*.rs`), and its 12 live cells await a
  round of their own. Only leg U (`test_aws_acceptance_commit_unknown.py`, 96 statements) and leg X
  (`test_aws_acceptance_cross_release.py`, 40 cells) are listed.
- **Today's context lines.** On main the credentials step is
  `aws-actions/configure-aws-credentials@e1253824…  # v6.3.0`. The draft's context still shows
  `e6de0542…  # v6.2.3`, so its hunk would not apply as written.
- **Unchanged from the draft:**
  - `timeout-minutes: 120` (sketch §6.4: about 25 minutes added; leg U grows to 96 statements);
  - U-10's Temurin 17 step and `--extra record`;
  - the step order of sketch §6.3: the integrity legs after dbt gold and before the cross-read
    step, so Spark also reads the integrity tables.

## The diff

```diff
--- a/.github/workflows/aws-acceptance.yml
+++ b/.github/workflows/aws-acceptance.yml
@@ -46,7 +46,7 @@
     name: tier-2 live AWS acceptance (Glue + S3 Tables, scratch-only)
     if: github.event_name == 'schedule' || inputs.leg == 'acceptance'
     runs-on: ubuntu-latest
-    timeout-minutes: 90
+    timeout-minutes: 120
     environment: aws-acceptance
     permissions:
       id-token: write   # OIDC exchange — scoped to this job only
@@ -73,14 +73,23 @@
           version: "0.9.5"  # Makefile UV_VERSION
           python-version: "3.12"
           enable-cache: true
+      - uses: actions/setup-java@dd06d9cba3e5552c54d9f8ea23572deb30010f7c  # v6.0.0
+        with:
+          distribution: temurin
+          java-version: "17"
       # Build BEFORE credentials exist — a supply-chain payload in this step has no AWS session.
       - name: sync + build native module
         run: |
           # --locked: uv.lock is an INPUT, never an output — a bare sync may rewrite the
           # checked-in lockfile as a side effect of a CI run (same rule as parity-live.yml).
-          uv sync --locked
+          uv sync --locked --extra record
           (cd python/repark && uvx maturin@1.14.1 develop)
           VIRTUAL_ENV="$PWD/.venv" uv pip install --quiet dbt-core==1.9.11 dbt-spark==1.9.3
+          uv venv --python 3.12 .venv-previous
+          VIRTUAL_ENV="$PWD/.venv-previous" uv pip install --only-binary :all: --require-hashes \
+            -r python/repark/tests/parity/previous_release_requirements.txt
+          .venv-previous/bin/python -c \
+            "import repark; assert repark.__version__ == '1.5.5', repark.__version__"
       # Credentials mint HERE, last, immediately before the only step that needs them.
       - uses: aws-actions/configure-aws-credentials@e1253824e5c10ff9df46874f81ed3ec929e19cfd  # v6.3.0
         with:
@@ -115,6 +124,34 @@
         run: >
           VIRTUAL_ENV="$PWD/.venv"
           uv run --no-project python -m pytest python/dbt-repark/tests/test_aws_acceptance_gold.py -q -rA
+      - name: integrity legs (REPARK_AWS_ACCEPTANCE=1, ICE-INTEGRITY-PINS-1)
+        if: ${{ !cancelled() }}
+        env:
+          REPARK_AWS_ACCEPTANCE: "1"
+          REPARK_ACCEPT_BRONZE_BUCKET: ${{ vars.REPARK_ACCEPT_BRONZE_BUCKET }}
+          REPARK_ACCEPT_WAREHOUSE: ${{ vars.REPARK_ACCEPT_WAREHOUSE }}
+          TABLE_BUCKET_ARN: ${{ secrets.TABLE_BUCKET_ARN }}
+          REPARK_PREVIOUS_RELEASE_PYTHON: ${{ github.workspace }}/.venv-previous/bin/python
+        run: >
+          VIRTUAL_ENV="$PWD/.venv"
+          uv run --no-project python -m pytest
+          python/repark/tests/parity/test_aws_acceptance_commit_unknown.py
+          python/repark/tests/parity/test_aws_acceptance_cross_release.py
+          -q -rA
+      - name: cross-read module (REPARK_AWS_ACCEPTANCE=1)
+        if: ${{ !cancelled() }}
+        env:
+          REPARK_AWS_ACCEPTANCE: "1"
+          REPARK_ACCEPT_DS: ${{ vars.REPARK_ACCEPT_DS }}
+          REPARK_ACCEPT_ENTITY: ${{ vars.REPARK_ACCEPT_ENTITY }}
+          REPARK_ACCEPT_ID_COL: ${{ vars.REPARK_ACCEPT_ID_COL }}
+          REPARK_ACCEPT_BRONZE_BUCKET: ${{ vars.REPARK_ACCEPT_BRONZE_BUCKET }}
+          REPARK_ACCEPT_WAREHOUSE: ${{ vars.REPARK_ACCEPT_WAREHOUSE }}
+          TABLE_BUCKET_ARN: ${{ secrets.TABLE_BUCKET_ARN }}
+          SPARK_LOCAL_IP: "127.0.0.1"
+        run: >
+          VIRTUAL_ENV="$PWD/.venv"
+          uv run --no-project python -m pytest python/repark/tests/parity/test_aws_acceptance_cross_read.py -q -rA
 
   ice-read-perf-bench:
     name: ICE-READ-PERF bench (S3 Tables + Glue, dispatch-only, scratch-only)
```

## Notes for the clerk PR

- The `.github/workflows/map.md` row for `aws-acceptance.yml` moves with it. In the draft's row,
  read `1.5.5` for `1.5.4`, list only the two integrity modules at their `parity/` paths, and
  `parity/test_aws_acceptance_cross_read.py` for the cross-read module.
- The legs need no new secret or variable. The gate signs again with the credentials the
  credentials step exports (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) and
  takes its region from `AWS_REGION`.
- No IAM change is expected (sketch §6.5), but nothing has measured it yet: the first run is the
  proof, and a denial surfaces in the leg's failure text.
- The first run is the legs' proof (ruling 86). A red cell is a fold PR. H-U5 (a double append in
  U-5 or U-5r) and H-X1 (a cross-release cell not equal) go to the owner first.
