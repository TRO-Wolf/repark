# ICE-SPARK-CROSS-READ-1 (U-10) — Spark reads what RePark writes. Tests only.

Built 2026-10-14 on branch `test/ice-spark-cross-read-1`, base `018d0efd`.
The local leg is green against live Spark 4.1.2 with Iceberg 1.11; the AWS
leg is written and collects cleanly but has not run. Evidence and the
per-table outcome table live in the unit ledger:
[ice-spark-cross-read-1-ledger.md](../../ledgers/staging/ice-spark-cross-read-1-ledger.md).

## What was built

- D1 — [the helper](../../../python/repark/tests/_acceptance_cross_read.py):
  given a RePark session and catalog, a PySpark session whose catalog points
  at the same warehouse, and table names, it adopts each table's newest
  metadata file into Spark (`register_table`: a Hadoop catalog cannot see
  memory-catalog tables directly, measured 2026-10-14) and compares schema
  (names, common-vocabulary types, nullability) and rows (order-insensitive,
  exact, floats bit-exact). One outcome per table: EQUAL, DIFFERENT with the
  first differences, or SPARK-CANNOT-READ with Spark's error class and first
  line. Nanosecond-column tables report `has_ns_columns` and are never
  skipped (registry row ICE-TSNS-SQL-1-R-002 in
  [the parity registry](../../../docs/spark-sql-iceberg-parity.md)). No
  expectation is typed in per table: the rule is Spark's rows equal
  RePark's rows.
- D2 — [the local leg](../../../python/repark/tests/test_acceptance_cross_read_local.py):
  three live cells run the MOR, create-or-replace-twice and v3 offline
  acceptance runners unchanged (reused by import), assert their own
  outcomes, then cross-read every table each wrote. Gated on the live flag
  plus a PySpark importorskip; the shared Spark session is never stopped
  and each test takes a private catalog.
- D3 — [the AWS leg](../../../python/repark/tests/test_aws_acceptance_cross_read.py):
  two cells discover every `testing_` table under the scratch namespace on
  Glue and on S3 Tables and cross-read each through the shared helper
  (direct catalog reads both sides, no adoption). Same module-level
  `REPARK_AWS_ACCEPTANCE` skip as the AWS legs plus a PySpark skip; never
  drops anything. Collects cleanly with the variable unset; not run.
- D4 — this order and the ledger.

## Local run

The leg needs one interpreter with both engines: the sparkenv interpreter
(PySpark 4.1.2, no RePark) with the release module on `PYTHONPATH`. From the
repo root, with `JAVA_HOME` on a Java 17 home:

```bash
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 REPARK_PARITY_LIVE=1 PYTHONPATH=/tmp/sb-main/python/repark/src:/tmp/sb-main/.venv/lib/python3.12/site-packages /tmp/sparkenv/bin/python -m pytest python/repark/tests/test_acceptance_cross_read_local.py -q -rA -s
```

Result 2026-10-14: 3 passed. Four tables EQUAL (`mw4mor` 20 rows,
`replace2mem` 5 rows, `v3acc` and `v3acc_adopted` 10 rows each), zero
DIFFERENT, zero SPARK-CANNOT-READ. The `-s` only shows the printed outcome
tables; without it the run passes identically. Without the live flag, or
without PySpark, the three cells skip cleanly. Spark banner for the run:
4.1.2 with `iceberg-spark-runtime-4.1_2.13:1.11.0`, UTC, ANSI on.

## What could not run locally

- The bronze publish path: it reads a real `s3a` bronze entity, so no
  offline runner exists for it. The AWS leg discovers its tables when the
  nightly has published them.
- The U12 loose-object writes: they are not Iceberg tables, so the
  cross-read does not apply.
- The SQL-harden S1–S9 shapes are already dual-engine: their own suite runs
  every program on RePark and on live Spark and compares verdicts, so the
  local leg does not repeat them. The AWS leg still discovers their tables.

## Workflow step the owner wires (proposal, markdown only)

The workflow file itself is untouched. Add to the `live-aws` job of
[.github/workflows/aws-acceptance.yml](../../../.github/workflows/aws-acceptance.yml),
after the dbt gold step so every acceptance table exists first:

```diff
--- a/.github/workflows/aws-acceptance.yml
+++ b/.github/workflows/aws-acceptance.yml
@@ -71,13 +71,18 @@
       - uses: astral-sh/setup-uv@20cfd1bf945f4377ade1205e4dbc17946fc9a30d  # v10.0.1 (node24)
         with:
           version: "0.9.5"  # Makefile UV_VERSION
           python-version: "3.12"
           enable-cache: true
+      # Java 17 (Temurin) — the cross-read leg starts live Spark 4.1.2 (same pin as parity-live.yml).
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
@@ -105,13 +105,27 @@
       - name: dbt gold acceptance (REPARK_AWS_ACCEPTANCE=1)
         if: ${{ !cancelled() }}
         env:
           REPARK_AWS_ACCEPTANCE: "1"
           REPARK_ACCEPT_DS: ${{ vars.REPARK_ACCEPT_DS }}
           REPARK_ACCEPT_ENTITY: ${{ vars.REPARK_ACCEPT_ENTITY }}
           REPARK_ACCEPT_ID_COL: ${{ vars.REPARK_ACCEPT_ID_COL }}
           REPARK_ACCEPT_BRONZE_BUCKET: ${{ vars.REPARK_ACCEPT_BRONZE_BUCKET }}
           REPARK_ACCEPT_WAREHOUSE: ${{ vars.REPARK_ACCEPT_WAREHOUSE }}
           TABLE_BUCKET_ARN: ${{ secrets.TABLE_BUCKET_ARN }}
         run: >
           VIRTUAL_ENV="$PWD/.venv"
           uv run --no-project python -m pytest python/dbt-repark/tests/test_aws_acceptance_gold.py -q -rA
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
+          uv run --no-project python -m pytest python/repark/tests/test_aws_acceptance_cross_read.py -q -rA
```

Notes for the wire-up, in the operator's own runbook terms
([tier2-aws.md](../../../docs/tier2-aws.md)):

- No new IAM action and no new variable: the leg reads tables the role can
  already read, writes nothing, and drops nothing. The Java and PySpark
  setup lands before credentials mint, with the rest of the build.
- `--extra record` is the same flag `parity-live.yml` uses: PySpark comes
  from the repo's pinned `record` extra (4.1.2), not a new version string.
  It only adds PySpark to the venv; the existing steps do not import it.
- The Iceberg Spark runtime and the AWS bundle jars resolve from Maven
  Central at Spark start through `spark.jars.packages` (pinned GAVs from
  `_oracle_pins`); the first run downloads them. The bundle coordinate
  `org.apache.iceberg:iceberg-aws-bundle:1.11.0` was taken from the Iceberg
  release layout and has not been resolved yet — if the first wire-up
  cannot resolve it, the fix is the coordinate, not the leg.
- The step runs after the dbt gold step so discovery sees every table the
  nightly wrote, including the gold models. Like the dbt step it runs
  unless the job was cancelled, so a red acceptance module still gets its
  tables cross-read.
