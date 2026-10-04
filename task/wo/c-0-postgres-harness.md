# C-0 — the disposable Postgres container and the live-database cell rules · grade C · clerk band · 1.6, first unit

## 0. Why, and what is out

Every 1.6 connector unit and the 1.7 capture harness run against a Postgres nobody else owns. C-0
delivers the container script, the Python live-cell fixture with unique names and cleanup, the five
capture failure scenarios as named, deliberately-failing pins, and the rules paragraph in
`docs/testing.md`. It is CC-6 of [../roadmap/epic-term/contracts-ahead-of-code-2026-10-01.md](../roadmap/epic-term/contracts-ahead-of-code-2026-10-01.md).
**No product code, no `repark-connect` directory** (the manifest reds if one appears before C-1).
Out: the SQL Server container (a separate measurement under CC-5), any Rust.

## 1. Rulings already made

- CC-6: a disposable local container, never an Airflow-managed database; publications and replication slots carry unique names and explicit cleanup; the five scenarios are cdc's S0 pins.
- Live-cell rules (2026-09): guard the shared session, single-file seed, private catalog, no environment pop, prove co-collected.
- Rust-first does not apply to a test fixture; the Spark live cells are Python and this tier matches them.
- No code comments; docstrings are required by the docstring-presence gate and are not comments.
- The image is `postgres:16-alpine`; logical replication is on from the first run (`wal_level=logical`, 8 slots, 8 senders) so the 1.7 harness changes nothing here.
- Docker (owner, 2026-10-02): the container is declared once in a Compose file and started through the stock `docker compose` client, so any daemon works (Desktop, Engine, rootless); the daemon is a machine-local choice named by `DOCKER_HOST`. On the owner's box that is rootless Docker under `repark.slice`, never the system daemon and never the docker group. The image is pre-pulled (`pull_policy: missing`). Every container is bounded (two cores, 2 GB, 256 pids), labelled `repark.disposable=1`, at most four run at once, and `reap` removes any older than two hours. No Dockerfile: nothing needs baking into the image.
- Reboot recovery (owner, 2026-10-03): the state file lives in `$XDG_RUNTIME_DIR`, falling back to `/tmp` when that is not writable, and both are wiped at boot. **Recovery after a reboot is the label-based `reap`, never the state file.** The compose file sets no restart policy, so a container that was running at shutdown comes back stopped. `reap` therefore lists every labelled container (`docker ps -a`) and removes any that is not running, whatever its age, with its anonymous volume and its project network. A running one is removed only when it is older than two hours. Run `reap` once after any reboot, before the first `up`.

## 2. Files

| action | path |
|---|---|
| new | `scripts/dev/pg/compose.yaml` — the one Postgres service: image, server flags, limits, label, health check, random localhost port |
| new | `scripts/dev/pg_disposable.sh` — `up` / `down` / `url` / `reap`, each one `docker compose` call under the project name `repark-pg-<user>-<pid>` |
| edited | `Makefile` — `pg-up`, `pg-down`, `pg-url` targets that call the script |
| edited | `DEVELOPMENT.md` — a "Docker for the live database tier" paragraph in Prerequisites (ceiling: `scripts/check_docs_compaction.py` CEILINGS if listed) |
| new | `python/repark-parity/tests/live_db/__init__.py` (empty) |
| new | `python/repark-parity/tests/live_db/conftest.py` — the `pg_live` fixture |
| new | `python/repark-parity/tests/live_db/test_c0_cdc_scenarios.py` — the five scenario pins |
| edited | `docs/testing.md` — a "Live database tier" subsection after "The live oracle tier (drift detector)" |
| map.md | `scripts/map.md` (a `dev/` line), `scripts/dev/map.md` (new), `scripts/dev/pg/map.md` (new), `python/repark-parity/tests/map.md` (a `live_db/` line), `python/repark-parity/tests/live_db/map.md` (new) |

## 3. Skeletons

`scripts/dev/pg/compose.yaml`, in full:

```yaml
services:
  postgres:
    image: postgres:16-alpine
    pull_policy: missing
    command:
      - -c
      - wal_level=logical
      - -c
      - max_replication_slots=8
      - -c
      - max_wal_senders=8
      - -c
      - shared_buffers=256MB
      - -c
      - max_connections=50
    environment:
      POSTGRES_PASSWORD: repark
    ports:
      - "127.0.0.1::5432"
    shm_size: 256m
    pids_limit: 256
    deploy:
      resources:
        limits:
          cpus: "2"
          memory: 2G
    labels:
      repark.disposable: "1"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 1s
      retries: 30
```

`scripts/dev/pg_disposable.sh`, in full:

```sh
#!/usr/bin/env bash
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
COMPOSE=(docker compose -f "$HERE/pg/compose.yaml")
STATE="${XDG_RUNTIME_DIR:-/tmp}/repark-pg.env"
case "${1:-}" in
  up)
    [ "$(docker ps -q --filter label=repark.disposable=1 | wc -l)" -lt 4 ] || { echo "four disposable containers already running" >&2; exit 3; }
    P="repark-pg-${USER:-u}-$$"
    "${COMPOSE[@]}" -p "$P" up -d --wait >/dev/null
    PORT=$("${COMPOSE[@]}" -p "$P" port postgres 5432 | cut -d: -f2)
    printf 'REPARK_PG_PROJECT=%s\nREPARK_PG_URL=postgresql://postgres:repark@127.0.0.1:%s/postgres\n' "$P" "$PORT" > "$STATE"
    cat "$STATE" ;;
  down) . "$STATE"; "${COMPOSE[@]}" -p "$REPARK_PG_PROJECT" down -v >/dev/null; rm -f "$STATE" ;;
  url) . "$STATE"; echo "$REPARK_PG_URL" ;;
  reap)
    cutoff=$(date -u -d '-2 hours' +%s)
    for id in $(docker ps -aq --filter label=repark.disposable=1); do
      if [ "$(docker inspect --format '{{.State.Running}}' "$id")" = true ]; then
        started=$(date -u -d "$(docker inspect --format '{{.State.StartedAt}}' "$id")" +%s)
        [ "$started" -ge "$cutoff" ] && continue
      fi
      project=$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' "$id")
      docker rm -f -v "$id" >/dev/null
      [ -n "$project" ] && docker network rm "${project}_default" >/dev/null 2>&1 || true
    done ;;
  *) echo "usage: $0 up|down|url|reap" >&2; exit 2 ;;
esac
```

`Makefile`, three targets after `preflight`, in full:

```make
.PHONY: pg-up
pg-up: ## Start one disposable Postgres (logical replication on); prints REPARK_PG_URL
	scripts/dev/pg_disposable.sh up

.PHONY: pg-down
pg-down: ## Stop the disposable Postgres this shell started
	scripts/dev/pg_disposable.sh down

.PHONY: pg-url
pg-url: ## Print the running disposable Postgres URL
	scripts/dev/pg_disposable.sh url
```

`DEVELOPMENT.md`, one bullet appended to the Prerequisites list, in full:

> - **Docker, only for the live database tier** — `make pg-up` starts one disposable Postgres
>   from `scripts/dev/pg/compose.yaml` (linked once the file exists) through the stock
>   `docker compose` client, bounded to two cores and two gigabytes, on a random localhost port;
>   `make pg-down` removes it with its volume. Any daemon works — Desktop, Engine or rootless; set
>   `DOCKER_HOST` when yours is not the default. A box that also runs agents should use rootless
>   Docker under a cgroup slice so test containers share the agents' CPU and memory cap. Nothing in
>   `verify` or `preflight` needs Docker; the live-database cells skip when `REPARK_PG_URL` is unset.

`conftest.py`:

```python
"""Live Postgres cells: one disposable container, unique names per test, explicit cleanup."""

import os
import uuid

import psycopg
import pytest


@pytest.fixture
def pg_live():
    """Yield (connection, names) for one test; drop everything the test created on exit."""
    url = os.environ.get("REPARK_PG_URL")
    if not url:
        pytest.skip("REPARK_PG_URL unset: make pg-up")
    tag = uuid.uuid4().hex[:12]
    names = {"schema": f"t_{tag}", "publication": f"pub_{tag}", "slot": f"slot_{tag}"}
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{names["schema"]}"')
        try:
            yield conn, names
        finally:
            conn.execute(f'DROP PUBLICATION IF EXISTS "{names["publication"]}"')
            conn.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
                (names["slot"],),
            )
            conn.execute(f'DROP SCHEMA "{names["schema"]}" CASCADE')
```

`test_c0_cdc_scenarios.py` — five functions, each `@pytest.mark.xfail(strict=True, reason="cdc S0 pin: the producer arrives at 1.7")`, each creating its table, publication and slot through `names` and then asserting the behaviour the producer must have; the assertion fails today because there is no producer, and `strict=True` makes the pin red on purpose the day one lands:

```python
def test_crash_after_commit_before_checkpoint(pg_live): ...
def test_snapshot_to_wal_handover_under_concurrent_writes(pg_live): ...
def test_replay_and_duplicate_delivery(pg_live): ...
def test_schema_change_and_partial_update_image(pg_live): ...
def test_lost_slot_or_unavailable_wal(pg_live): ...
```

Plus one green test, `test_fixture_isolates_names`, that opens two `pg_live` instances in one
session and proves their schema, publication and slot names differ and that nothing of the first
survives its teardown (`pg_replication_slots` and `pg_publication` empty of its tag).

`docs/testing.md` subsection, in full:

> ### The live database tier
>
> A live database cell runs only against the disposable container `make pg-up` starts from
> `scripts/dev/pg/compose.yaml` (one per invocation, logical replication on, bounded to two cores
> and two gigabytes, at most four at once), never against a database another system owns. The `pg_live` fixture (`python/repark-parity/tests/live_db/conftest.py`) gives each test a
> schema, a publication name and a slot name that carry one random tag, and drops all three on
> exit; a test that creates anything else drops it itself. Cells skip, not fail, when
> `REPARK_PG_URL` is unset. The Spark live-cell rules (guard the shared session, single-file seed,
> private catalog, no environment pop, prove co-collected) apply unchanged. Ruled 2026-10-01, CC-6.

## 4. Steps

1. Write the compose file, the script, the three make targets, the DEVELOPMENT.md bullet and the three Python files from §3; `chmod +x scripts/dev/pg_disposable.sh`.
2. Add `psycopg[binary]` to `python/repark-parity/pyproject.toml` test dependencies; `uv sync` or the repo's documented install.
3. `make pg-up && export REPARK_PG_URL=$(make -s pg-url)`.
4. `.venv/bin/python -m pytest python/repark-parity/tests/live_db -q` — expect `1 passed, 5 xfailed`.
5. `unset REPARK_PG_URL && .venv/bin/python -m pytest python/repark-parity/tests/live_db -q` — expect `6 skipped`.
6. `make pg-down && docker ps -a --filter label=repark.disposable=1 --format '{{.Names}}' && docker volume ls -q --filter label=repark.disposable=1` — expect empty output from both.
7. `docker compose -f scripts/dev/pg/compose.yaml config -q` — expect no output, exit 0.
8. The `docs/testing.md` subsection; the five map.md edits.
9. `ruff check python/repark-parity/tests/live_db && ruff format --check python/repark-parity/tests/live_db && scripts/check_map_md.sh --base origin/main && scripts/check_docstring_presence.sh && scripts/check_lib_py.sh && python3 scripts/check_docs_links.py && python3 scripts/check_docs_compaction.py`. No comment gate exists: the comment ban is held by review, because the `check-comment-density` ratchet was dropped before #247 merged.
10. Commit: `chore(c-0): the disposable Postgres via compose, the pg_live fixture and the five cdc S0 pins (CC-6)` with the `Authored-By:` trailer. Push, open the PR against `main`.

## 5. Gates and the line that means green

| command | green |
|---|---|
| step 4 | `1 passed, 5 xfailed` |
| step 5 | `6 skipped` |
| step 6 | no container or volume labelled `repark.disposable=1` remains |
| step 7 | `docker compose … config -q` exits 0 silently |
| `python3 scripts/check_docs_compaction.py` | `docs-compaction: clean` |
| `ruff check` / `ruff format --check` | no output, exit 0 |
| `scripts/check_map_md.sh --base origin/main` | `map-md: … clean` |
| `scripts/check_docstring_presence.sh` | `docstring-presence: … clean` |
| `python3 scripts/check_docs_links.py` | `docs-links: … clean` |

## 6. Halt rules

- **H-1** `docker` or `docker compose` is absent, the daemon does not answer, or the image cannot be pulled: hand back the error text; do not substitute a local Postgres and do not add a Dockerfile.
- **H-2** step 4 shows any `failed` or an `xpassed`: hand back the test name and output; do not loosen an assertion.
- **H-3** a gate in step 9 needs a change outside §2's file list: hand back the gate's output; do not edit the gate.
- **H-4** sixty minutes without green step 4: commit as `wip(c-0): …`, hand back.
- **H-5** `up` exits 3 (four disposable containers already running): hand back; do not remove another lane's container and do not run `reap` to make room.

## 7. Hand-back

```json
{"unit":"C-0","step4":"","step5":"","containers_left":0,"commit":"<sha>","halt":null}
```
