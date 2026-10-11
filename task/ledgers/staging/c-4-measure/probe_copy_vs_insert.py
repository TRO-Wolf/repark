from __future__ import annotations

import os
import struct
import time
import urllib.parse
from collections.abc import Callable

import psycopg


def encode_int4(value: int | None) -> bytes:
    if value is None:
        return struct.pack(">i", -1)
    size = struct.pack(">i", 4)
    body = struct.pack(">i", value)
    return size + body


def encode_text(value: str | None) -> bytes:
    if value is None:
        return struct.pack(">i", -1)
    raw = value.encode("utf-8")
    size = struct.pack(">i", len(raw))
    return size + raw


def copy_payload(rows: list[list[bytes]]) -> bytes:
    out = bytearray(b"PGCOPY\n\xff\r\n\x00")
    out += struct.pack(">i", 0)
    out += struct.pack(">i", 0)
    for row in rows:
        out += struct.pack(">h", len(row))
        for field in row:
            out += field
    out += b"\xff\xff"
    return bytes(out)


def three_row_payload() -> bytes:
    rows = [
        [encode_int4(1), encode_text("a")],
        [encode_int4(2), encode_text("b")],
        [encode_int4(3), encode_text("c")],
    ]
    return copy_payload(rows)


def first_line(text: str) -> str:
    return text.split("\n")[0]


def attempt_statement(cursor: psycopg.Cursor, label: str, sql: str) -> None:
    try:
        cursor.execute(sql)
        print(f"{label}: ok")
    except psycopg.Error as error:
        print(f"{label}: ERROR sqlstate={error.sqlstate} text={first_line(str(error))}")


def attempt_copy(cursor: psycopg.Cursor, label: str, sql: str, payload: bytes) -> None:
    try:
        with cursor.copy(sql) as stream:
            stream.write(payload)
        print(f"{label}: ok")
    except psycopg.Error as error:
        print(f"{label}: ERROR sqlstate={error.sqlstate} text={first_line(str(error))}")


def dump(cursor: psycopg.Cursor, label: str, sql: str) -> None:
    cursor.execute(sql)
    rows = cursor.fetchall()
    print(f"{label}: {len(rows)} rows")
    for row in rows:
        print(f"  {row!r}")


def setup(cursor: psycopg.Cursor, label: str, statements: list[str]) -> None:
    for sql in statements:
        cursor.execute(sql)
    print(f"{label}: ready")


def loopback_parts(url: str) -> tuple[str, str, str]:
    parsed = urllib.parse.urlparse(url)
    user = urllib.parse.unquote(parsed.username or "")
    password = urllib.parse.unquote(parsed.password or "")
    database = parsed.path.lstrip("/")
    return database, user, password


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def measure_statement_triggers(cursor: psycopg.Cursor, schema: str, url: str) -> None:
    print("== cell stmt-before-after ==")
    statements = [
        f"CREATE TABLE {schema}.t_stmt (id int4, v text)",
        f"CREATE TABLE {schema}.audit_stmt (seq bigserial PRIMARY KEY, fired text)",
        f"CREATE FUNCTION {schema}.note_before() RETURNS trigger LANGUAGE plpgsql AS "
        f"$$ BEGIN INSERT INTO {schema}.audit_stmt (fired) VALUES ('before'); "
        "RETURN NULL; END $$",
        f"CREATE FUNCTION {schema}.note_after() RETURNS trigger LANGUAGE plpgsql AS "
        f"$$ BEGIN INSERT INTO {schema}.audit_stmt (fired) VALUES ('after'); "
        "RETURN NULL; END $$",
        f"CREATE TRIGGER trg_before BEFORE INSERT ON {schema}.t_stmt "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {schema}.note_before()",
        f"CREATE TRIGGER trg_after AFTER INSERT ON {schema}.t_stmt "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {schema}.note_after()",
    ]
    setup(cursor, "stmt-before-after", statements)
    attempt_statement(
        cursor, "insert", f"INSERT INTO {schema}.t_stmt VALUES (1,'a'),(2,'b'),(3,'c')"
    )
    dump(cursor, "insert stored", f"SELECT id, v FROM {schema}.t_stmt ORDER BY id")
    dump(cursor, "insert audit", f"SELECT fired FROM {schema}.audit_stmt ORDER BY seq")
    cursor.execute(f"TRUNCATE {schema}.t_stmt, {schema}.audit_stmt")
    attempt_copy(
        cursor,
        "copy",
        f"COPY {schema}.t_stmt (id, v) FROM STDIN (FORMAT BINARY)",
        three_row_payload(),
    )
    dump(cursor, "copy stored", f"SELECT id, v FROM {schema}.t_stmt ORDER BY id")
    dump(cursor, "copy audit", f"SELECT fired FROM {schema}.audit_stmt ORDER BY seq")


def measure_transition_table(cursor: psycopg.Cursor, schema: str, url: str) -> None:
    print("== cell stmt-transition ==")
    statements = [
        f"CREATE TABLE {schema}.t_trans (id int4, v text)",
        f"CREATE TABLE {schema}.audit_trans (n bigint, ids text)",
        f"CREATE FUNCTION {schema}.note_trans() RETURNS trigger LANGUAGE plpgsql AS "
        f"$$ BEGIN INSERT INTO {schema}.audit_trans SELECT count(*), "
        "string_agg(n.id::text, ',' ORDER BY n.id) FROM n; RETURN NULL; END $$",
        f"CREATE TRIGGER trg_trans AFTER INSERT ON {schema}.t_trans "
        "REFERENCING NEW TABLE AS n FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {schema}.note_trans()",
    ]
    setup(cursor, "stmt-transition", statements)
    attempt_statement(
        cursor, "insert", f"INSERT INTO {schema}.t_trans VALUES (1,'a'),(2,'b'),(3,'c')"
    )
    dump(cursor, "insert stored", f"SELECT id, v FROM {schema}.t_trans ORDER BY id")
    dump(cursor, "insert audit", f"SELECT n, ids FROM {schema}.audit_trans")
    cursor.execute(f"TRUNCATE {schema}.t_trans, {schema}.audit_trans")
    attempt_copy(
        cursor,
        "copy",
        f"COPY {schema}.t_trans (id, v) FROM STDIN (FORMAT BINARY)",
        three_row_payload(),
    )
    dump(cursor, "copy stored", f"SELECT id, v FROM {schema}.t_trans ORDER BY id")
    dump(cursor, "copy audit", f"SELECT n, ids FROM {schema}.audit_trans")


def measure_postgres_fdw(cursor: psycopg.Cursor, schema: str, url: str) -> None:
    print("== cell fdw-postgres ==")
    database, user, password = loopback_parts(url)
    server = f"{schema}_far"
    statements = [
        "CREATE EXTENSION IF NOT EXISTS postgres_fdw",
        f"CREATE SERVER {server} FOREIGN DATA WRAPPER postgres_fdw OPTIONS "
        f"(host '127.0.0.1', port '5432', dbname {quote_literal(database)})",
        f"CREATE USER MAPPING FOR CURRENT_USER SERVER {server} OPTIONS "
        f"(user {quote_literal(user)}, password {quote_literal(password)})",
        f"CREATE TABLE {schema}.t_far (id int4, v text DEFAULT 'rem', "
        "n int4 DEFAULT 7 CHECK (n > 0))",
        f"CREATE TABLE {schema}.audit_far (seq bigserial PRIMARY KEY, fired text, id int4)",
        f"CREATE FUNCTION {schema}.far_row() RETURNS trigger LANGUAGE plpgsql AS "
        f"$$ BEGIN INSERT INTO {schema}.audit_far (fired, id) VALUES ('row', NEW.id); "
        "RETURN NEW; END $$",
        f"CREATE FUNCTION {schema}.far_stmt() RETURNS trigger LANGUAGE plpgsql AS "
        f"$$ BEGIN INSERT INTO {schema}.audit_far (fired, id) VALUES ('stmt', NULL); "
        "RETURN NULL; END $$",
        f"CREATE TRIGGER far_row_trg AFTER INSERT ON {schema}.t_far "
        f"FOR EACH ROW EXECUTE FUNCTION {schema}.far_row()",
        f"CREATE TRIGGER far_stmt_trg AFTER INSERT ON {schema}.t_far "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {schema}.far_stmt()",
        f"CREATE FOREIGN TABLE {schema}.t_fdw (id int4, v text, n int4) SERVER {server} "
        f"OPTIONS (schema_name {quote_literal(schema)}, table_name 't_far')",
    ]
    setup(cursor, "fdw-postgres", statements)
    attempt_statement(
        cursor, "insert", f"INSERT INTO {schema}.t_fdw (id, v) VALUES (1,'a'),(2,'b'),(3,'c')"
    )
    dump(cursor, "insert stored", f"SELECT id, v, n FROM {schema}.t_far ORDER BY id")
    dump(cursor, "insert audit", f"SELECT fired, id FROM {schema}.audit_far ORDER BY seq")
    cursor.execute(f"TRUNCATE {schema}.t_far, {schema}.audit_far")
    attempt_copy(
        cursor,
        "copy",
        f"COPY {schema}.t_fdw (id, v) FROM STDIN (FORMAT BINARY)",
        three_row_payload(),
    )
    dump(cursor, "copy stored", f"SELECT id, v, n FROM {schema}.t_far ORDER BY id")
    dump(cursor, "copy audit", f"SELECT fired, id FROM {schema}.audit_far ORDER BY seq")
    print("-- fdw-postgres constraint violation --")
    cursor.execute(f"TRUNCATE {schema}.t_far, {schema}.audit_far")
    attempt_statement(
        cursor, "insert bad", f"INSERT INTO {schema}.t_fdw (id, v, n) VALUES (9,'z',-1)"
    )
    dump(cursor, "insert bad stored", f"SELECT id, v, n FROM {schema}.t_far ORDER BY id")
    cursor.execute(f"TRUNCATE {schema}.t_far, {schema}.audit_far")
    bad_payload = copy_payload([[encode_int4(9), encode_text("z"), encode_int4(-1)]])
    attempt_copy(
        cursor,
        "copy bad",
        f"COPY {schema}.t_fdw (id, v, n) FROM STDIN (FORMAT BINARY)",
        bad_payload,
    )
    dump(cursor, "copy bad stored", f"SELECT id, v, n FROM {schema}.t_far ORDER BY id")


def measure_file_fdw(cursor: psycopg.Cursor, schema: str, url: str) -> None:
    print("== cell fdw-file ==")
    cursor.execute(
        "SELECT count(*) FROM pg_catalog.pg_available_extensions WHERE name = 'file_fdw'"
    )
    found = cursor.fetchone()
    available = found is not None and found[0] > 0
    print(f"file_fdw available: {available}")
    if not available:
        return
    server = f"{schema}_fil"
    statements = [
        "CREATE EXTENSION IF NOT EXISTS file_fdw",
        f"CREATE SERVER {server} FOREIGN DATA WRAPPER file_fdw",
        f"CREATE FOREIGN TABLE {schema}.t_file (id int4, v text) SERVER {server} OPTIONS "
        "(filename '/tmp/c4m_probe.csv', format 'csv')",
    ]
    try:
        setup(cursor, "fdw-file", statements)
    except psycopg.Error as error:
        print(f"setup: ERROR sqlstate={error.sqlstate} text={first_line(str(error))}")
        return
    attempt_statement(
        cursor, "insert", f"INSERT INTO {schema}.t_file VALUES (1,'a'),(2,'b'),(3,'c')"
    )
    attempt_copy(
        cursor,
        "copy",
        f"COPY {schema}.t_file (id, v) FROM STDIN (FORMAT BINARY)",
        three_row_payload(),
    )


def main() -> int:
    url = os.environ.get("REPARK_PG_URL", "")
    if not url:
        print("REPARK_PG_URL is empty")
        return 2
    schema = f"c4m_{os.getpid()}{int(time.time()) % 100000}"
    print(f"schema: {schema}")
    groups: list[tuple[str, Callable[[psycopg.Cursor, str, str], None]]] = [
        ("stmt-before-after", measure_statement_triggers),
        ("stmt-transition", measure_transition_table),
        ("fdw-postgres", measure_postgres_fdw),
        ("fdw-file", measure_file_fdw),
    ]
    connection = psycopg.connect(url, autocommit=True)
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        print(f"server: {version[0] if version else 'unknown'}")
        cursor.execute(f"CREATE SCHEMA {schema}")
        for name, group in groups:
            try:
                group(cursor, schema, url)
            except psycopg.Error as error:
                print(f"{name}: ERROR sqlstate={error.sqlstate} text={first_line(str(error))}")
    finally:
        cleanup = [
            f"DROP SCHEMA {schema} CASCADE",
            f"DROP SERVER {schema}_far CASCADE",
            f"DROP SERVER {schema}_fil CASCADE",
        ]
        for sql in cleanup:
            try:
                cursor.execute(sql)
            except psycopg.Error as error:
                print(f"cleanup: {first_line(str(error))}")
        cursor.close()
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
