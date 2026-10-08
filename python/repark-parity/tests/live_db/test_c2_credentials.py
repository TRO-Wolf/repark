"""C-2d live cell: no Postgres credential reaches any user-visible surface (sketch §5.7)."""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

SURFACES = r"""
import logging, os, sys, threading, time
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
import repark
from repark import ReparkSession

schema = os.environ["C2_SCHEMA"]
session = ReparkSession.builder.configFile(os.environ["REPARK_CONFIG"]).getOrCreate()


def report(surface, action):
    try:
        value = action()
        print(f"SURFACE {surface} ok {value!r}")
    except Exception as error:
        print(f"SURFACE {surface} {type(error).__name__} {error!s} {error!r}")


rows = session.sources()
print(f"SURFACE sources ok {rows!r} {rows!s}")
for name in ("keyed", "inurl", "wrong", "closed"):
    report(f"ping-{name}", lambda name=name: session.source(name).ping())
query = f"SELECT id, note FROM keyed.{schema}.vals WHERE id > 1 AND lower(note) = 'x'"
report("explain", lambda: session.sql(query)._explain_text("extended"))
report("explain-sql", lambda: session.sql("EXPLAIN " + query).collect())
report("explain-verbose", lambda: repark.sql("EXPLAIN VERBOSE " + query).collect())
report("read-keyed", lambda: session.sql(f"SELECT id FROM keyed.{schema}.vals").collect())
report("read-inurl", lambda: session.sql(f"SELECT id FROM inurl.{schema}.vals").collect())
report("auth", lambda: session.sql(f"SELECT id FROM wrong.{schema}.vals").collect())
report("unreachable", lambda: session.sql(f"SELECT id FROM closed.{schema}.vals").collect())
report("missing", lambda: session.sql(f"SELECT * FROM keyed.{schema}.nope").collect())
report("refused-value", lambda: session.sql(f"SELECT n FROM keyed.{schema}.vals").collect())
report("lock", lambda: session.sql(f"SELECT id FROM keyed.{schema}.locked").collect())
holder = threading.Thread(
    target=report,
    args=("pool-holder", lambda: session.sql(f"SELECT id FROM pooled.{schema}.locked").collect()),
)
holder.start()
time.sleep(0.5)
report("pool", lambda: session.sql(f"SELECT id FROM pooled.{schema}.vals").collect())
holder.join()
report(
    "jdbc",
    lambda: session.read.jdbc(
        os.environ["C2_JDBC_URL"], f"{schema}.nope", properties={"sslmode": "disable"}
    ).collect(),
)
session.stop()
"""


def _source(name: str, lines: list[str]) -> str:
    """Render one `[default.database.postgres.<name>]` table."""
    body = "\n".join([*lines, 'sslmode = "disable"'])
    return f"[default.database.postgres.{name}]\n{body}\n"


def _config(path: Path, role: str, password: str, wrong: str, host: str, port: int) -> None:
    """Mount the cell role five ways: the key, the URL, a wrong password, a closed port, a pool."""
    keyed = [f'host = "{host}"', f'port = "{port}"', f'user = "{role}"', 'database = "postgres"']
    path.write_text(
        _source("keyed", [*keyed, f'password = "{password}"', 'lock_timeout_ms = "300"'])
        + _source("inurl", [f'url = "postgresql://{role}:{password}@{host}:{port}/postgres"'])
        + _source("wrong", [f'url = "postgresql://{role}:{wrong}@{host}:{port}/postgres"'])
        + _source(
            "closed",
            [
                f'url = "postgresql://{role}:{password}@{host}:1/postgres"',
                'connect_timeout_ms = "1000"',
            ],
        )
        + _source(
            "pooled",
            [
                *keyed,
                f'password = "{password}"',
                'pool_max_size = "1"',
                'pool_checkout_timeout_ms = "200"',
                'lock_timeout_ms = "3000"',
            ],
        ),
        encoding="utf-8",
    )


def _seed(conn: Any, schema: str, role: str, password: str) -> None:
    """Create the cell role with SELECT on two tables, one of them holding a `NaN`."""
    conn.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
    conn.execute(f'CREATE TABLE "{schema}".vals (id int4, n numeric(10,2), note text)')
    conn.execute(f"INSERT INTO \"{schema}\".vals VALUES (1, 1.5, 'x'), (2, 'NaN', 'x')")
    conn.execute(f'CREATE TABLE "{schema}".locked (id int4)')
    conn.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
    conn.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"')


def test_no_credential_reaches_any_surface(
    pg_live: tuple[Any, dict[str, str]], tmp_path: Path
) -> None:
    import psycopg

    conn, names = pg_live
    schema = names["schema"]
    role = f"r_{schema}"
    password = secrets.token_hex(16)
    wrong = secrets.token_hex(16)
    server = urlsplit(os.environ["REPARK_PG_URL"])
    host, port = server.hostname or "127.0.0.1", server.port or 5432
    _seed(conn, schema, role, password)
    config = tmp_path / "repark.toml"
    _config(config, role, password, wrong, host, port)
    jdbc_url = f"jdbc:postgresql://{host}:{port}/postgres?user={role}&password={password}"
    env = {
        **os.environ,
        "RUST_LOG": "trace",
        "REPARK_CONFIG": str(config),
        "C2_SCHEMA": schema,
        "C2_JDBC_URL": jdbc_url,
    }
    try:
        with psycopg.connect(os.environ["REPARK_PG_URL"]) as locker:
            locker.execute(f'LOCK TABLE "{schema}".locked IN ACCESS EXCLUSIVE MODE')
            done = subprocess.run(
                [sys.executable, "-c", SURFACES],
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            locker.rollback()
    finally:
        conn.execute(f'DROP OWNED BY "{role}"')
        conn.execute(f'DROP ROLE "{role}"')
    out, err = done.stdout, done.stderr
    assert done.returncode == 0, err[-4000:]
    surfaces = {
        line.split(" ", 2)[1]: line.split(" ", 2)[2]
        for line in out.splitlines()
        if line.startswith("SURFACE ")
    }
    assert surfaces["ping-keyed"].startswith("ok"), surfaces["ping-keyed"]
    assert surfaces["ping-inurl"].startswith("ok"), surfaces["ping-inurl"]
    assert surfaces["read-keyed"].startswith("ok"), surfaces["read-keyed"]
    assert surfaces["read-inurl"].startswith("ok"), surfaces["read-inurl"]
    assert "authentication failed" in surfaces["ping-wrong"]
    assert "authentication failed" in surfaces["auth"]
    assert "unreachable" in surfaces["ping-closed"]
    assert "unreachable" in surfaces["unreachable"]
    assert "AnalysisException" in surfaces["missing"] and "not found" in surfaces["missing"]
    assert "CONNECT-DECL-pg-numeric-special" in surfaces["refused-value"]
    assert "lock_timeout_ms" in surfaces["lock"]
    assert "pool_checkout_timeout_ms" in surfaces["pool"]
    assert "lock_timeout_ms" in surfaces["pool-holder"]
    assert "remote_sql=" in surfaces["explain-verbose"]
    assert "PostgresScanExec: source=keyed" in surfaces["explain"]
    assert "jdbc" in surfaces["jdbc"] and not surfaces["jdbc"].startswith("ok")
    assert "py.entry" in err and "time.busy" in err
    for text, label in ((out, "stdout"), (err, "stderr")):
        for secret in (password, wrong, jdbc_url, f"{role}:{password}@", f"{role}:{wrong}@"):
            assert secret not in text, f"{label} carries a credential"
