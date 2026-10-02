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

## 2. Files

| action | path |
|---|---|
| new | `scripts/dev/pg_disposable.sh` — `up` / `down` / `url`; one container per invocation, named `repark-pg-<user>-<pid>` |
| new | `python/repark-parity/tests/live_db/__init__.py` (empty) |
| new | `python/repark-parity/tests/live_db/conftest.py` — the `pg_live` fixture |
| new | `python/repark-parity/tests/live_db/test_c0_cdc_scenarios.py` — the five scenario pins |
| edited | `docs/testing.md` — a "Live database tier" subsection after "The live oracle tier (drift detector)" |
| map.md | `scripts/map.md` (a `dev/` line), `scripts/dev/map.md` (new), `python/repark-parity/tests/map.md` (a `live_db/` line), `python/repark-parity/tests/live_db/map.md` (new) |

## 3. Skeletons

`scripts/dev/pg_disposable.sh`:

```sh
#!/usr/bin/env bash
set -euo pipefail
NAME="repark-pg-${USER:-u}-$$"
IMAGE="postgres:16-alpine"
STATE="${XDG_RUNTIME_DIR:-/tmp}/repark-pg.env"
case "${1:-}" in
  up)
    PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
    docker run -d --rm --name "$NAME" -e POSTGRES_PASSWORD=repark -p "127.0.0.1:${PORT}:5432" "$IMAGE" \
      -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8 >/dev/null
    until docker exec "$NAME" pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
    printf 'REPARK_PG_NAME=%s\nREPARK_PG_URL=postgresql://postgres:repark@127.0.0.1:%s/postgres\n' "$NAME" "$PORT" > "$STATE"
    cat "$STATE" ;;
  down) . "$STATE"; docker rm -f "$REPARK_PG_NAME" >/dev/null; rm -f "$STATE" ;;
  url) . "$STATE"; echo "$REPARK_PG_URL" ;;
  *) echo "usage: $0 up|down|url" >&2; exit 2 ;;
esac
```

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
        pytest.skip("REPARK_PG_URL unset: scripts/dev/pg_disposable.sh up")
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
> A live database cell runs only against the disposable container `scripts/dev/pg_disposable.sh up`
> starts (one per invocation, logical replication on), never against a database another system
> owns. The `pg_live` fixture (`python/repark-parity/tests/live_db/conftest.py`) gives each test a
> schema, a publication name and a slot name that carry one random tag, and drops all three on
> exit; a test that creates anything else drops it itself. Cells skip, not fail, when
> `REPARK_PG_URL` is unset. The Spark live-cell rules (guard the shared session, single-file seed,
> private catalog, no environment pop, prove co-collected) apply unchanged. Ruled 2026-10-01, CC-6.

## 4. Steps

1. Write the four new files from §3; `chmod +x scripts/dev/pg_disposable.sh`.
2. Add `psycopg[binary]` to `python/repark-parity/pyproject.toml` test dependencies; `uv sync` or the repo's documented install.
3. `scripts/dev/pg_disposable.sh up && export $(scripts/dev/pg_disposable.sh url | sed 's/^/REPARK_PG_URL=/')`.
4. `.venv/bin/python -m pytest python/repark-parity/tests/live_db -q` — expect `1 passed, 5 xfailed`.
5. `unset REPARK_PG_URL && .venv/bin/python -m pytest python/repark-parity/tests/live_db -q` — expect `6 skipped`.
6. `scripts/dev/pg_disposable.sh down && docker ps -a --filter name=repark-pg --format '{{.Names}}'` — expect empty output.
7. The `docs/testing.md` subsection; the four map.md edits.
8. `ruff check python/repark-parity/tests/live_db && ruff format --check python/repark-parity/tests/live_db && scripts/check_map_md.sh --base origin/main && scripts/check_docstring_presence.sh && scripts/check_lib_py.sh && python3 scripts/check_docs_links.py && make check-comment-density`.
9. Commit: `chore(c-0): the disposable Postgres container, the pg_live fixture and the five cdc S0 pins (CC-6)` with the `Authored-By:` trailer. Push, open the PR against `main`.

## 5. Gates and the line that means green

| command | green |
|---|---|
| step 4 | `1 passed, 5 xfailed` |
| step 5 | `6 skipped` |
| step 6 | no container named `repark-pg-*` remains |
| `ruff check` / `ruff format --check` | no output, exit 0 |
| `scripts/check_map_md.sh --base origin/main` | `map-md: … clean` |
| `scripts/check_docstring_presence.sh` | `docstring-presence: … clean` |
| `python3 scripts/check_docs_links.py` | `docs-links: … clean` |

## 6. Halt rules

- **H-1** `docker` is absent or the image cannot be pulled: hand back the error text; do not substitute a local Postgres.
- **H-2** step 4 shows any `failed` or an `xpassed`: hand back the test name and output; do not loosen an assertion.
- **H-3** a gate in step 8 needs a change outside §2's file list: hand back the gate's output; do not edit the gate.
- **H-4** sixty minutes without green step 4: commit as `wip(c-0): …`, hand back.

## 7. Hand-back

```json
{"unit":"C-0","step4":"","step5":"","containers_left":0,"commit":"<sha>","halt":null}
```
