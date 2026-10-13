"""Record whether a written TIMESTAMP cast stays nanoseconds in the logical plan.

The 64 positions are the R-017 verifier's position list. Each position is planned
with ``CAST``, ``::timestamp``, ``to_timestamp`` and a ``TIMESTAMP`` literal.
The logical plan is the public ``EXPLAIN`` plan. A plan is unresolved when it
still contains ``Timestamp(ns)`` or a nanosecond timestamp literal.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import re
import tempfile
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
from _ice_tsns_coercion_1_spellings import SOURCES, SRC

from repark import functions as spark_functions

TSL = "TIMESTAMP '2001-02-03 04:05:06.654321'"
CALLS = {
    "cast": "CAST(@ AS TIMESTAMP)",
    "colon": "(@)::timestamp",
    "tots": "to_timestamp(@)",
    "lit": TSL,
}
SEL = "INSERT INTO {t} (id, v, k) SELECT id, %s, 0 FROM {src}"
POS = {
    "root": SEL % "#",
    "coalesce_ts_first": SEL % "coalesce(ts, #)",
    "coalesce_three": SEL % "coalesce(NULL, #, ts)",
    "case_noelse": SEL % "CASE WHEN id > 0 THEN # END",
    "case_three": SEL % f"CASE WHEN id < 0 THEN ts WHEN id > 0 THEN # ELSE {TSL} END",
    "if_null": SEL % "if(id > 0, #, NULL)",
    "nvl2": SEL % "nvl2(id, #, ts)",
    "nullif": SEL % "nullif(#, ts)",
    "least_null": SEL % "least(#, NULL)",
    "greatest_lit": SEL % f"greatest(#, {TSL})",
    "element_at": SEL % "element_at(array(ts, #), 2)",
    "array_max": SEL % "array_max(array(#, ts))",
    "struct": SEL % "named_struct('f', #, 'g', ts).f",
    "struct_coalesce": SEL % "coalesce(named_struct('f', #).f, ts)",
    "map_value": SEL % "map('a', #, 'b', ts)['a']",
    "lambda_out": SEL % "transform(array(#), x -> coalesce(x, ts))[0]",
    "lambda_in": SEL % "transform(array({c}), x -> $x$)[0]",
    "lambda_in_coalesce": SEL % "transform(array({c}), x -> coalesce($x$, ts))[0]",
    "filter": SEL % "filter(array(#, ts), x -> x IS NOT NULL)[0]",
    "twice": SEL % "coalesce(CAST(# AS TIMESTAMP), ts)",
    "plus": SEL % "coalesce(# + INTERVAL '1' SECOND, ts)",
    "agg": "INSERT INTO {t} (id, v, k) SELECT id, max(#), 0 FROM {src} GROUP BY id",
    "agg_coalesce_in": (
        "INSERT INTO {t} (id, v, k) SELECT id, max(coalesce(#, ts)), 0 FROM {src} GROUP BY id"
    ),
    "agg_coalesce_out": (
        "INSERT INTO {t} (id, v, k) SELECT id, coalesce(max(#), max(ts)), 0 FROM {src} GROUP BY id"
    ),
    "agg_first": (
        "INSERT INTO {t} (id, v, k) SELECT id, coalesce(first(#), any_value(ts)), 0 "
        "FROM {src} GROUP BY id"
    ),
    "agg_rollup": (
        "INSERT INTO {t} (id, v, k) SELECT id, coalesce(min(#), min(ts)), 0 "
        "FROM {src} GROUP BY ROLLUP (id) HAVING id IS NOT NULL"
    ),
    "window_max": SEL % "coalesce(max(#) OVER (PARTITION BY id), ts)",
    "window_lag": SEL % "lag(#, 0, ts) OVER (ORDER BY id)",
    "window_first": SEL % "first_value(coalesce(#, ts)) OVER (PARTITION BY id ORDER BY id)",
    "subq": (
        "INSERT INTO {t} (id, v, k) SELECT id, coalesce(w, ts), 0 "
        "FROM (SELECT id, # AS w, ts FROM {src}) q"
    ),
    "subq_two": (
        "INSERT INTO {t} (id, v, k) SELECT id, CASE WHEN id > 0 THEN w ELSE ts END, 0 "
        "FROM (SELECT id, w, ts FROM (SELECT id, # AS w, ts FROM {src}) a) b"
    ),
    "cte": (
        "INSERT INTO {t} (id, v, k) WITH q AS (SELECT id, # AS w, ts FROM {src}) "
        "SELECT id, greatest(w, ts), 0 FROM q"
    ),
    "cte_join": (
        "INSERT INTO {t} (id, v, k) WITH q AS (SELECT id, # AS w FROM {src}) "
        "SELECT a.id, coalesce(q.w, a.ts), 0 FROM {src} a JOIN q ON a.id = q.id"
    ),
    "view": (
        "CREATE OR REPLACE TEMPORARY VIEW rv_{tn} AS SELECT id, # AS w, ts FROM {src} ;; "
        "INSERT INTO {t} (id, v, k) SELECT id, coalesce(w, ts), 0 FROM rv_{tn}"
    ),
    "union_first": (
        "INSERT INTO {t} (id, v, k) SELECT id, #, 0 FROM {src} "
        "UNION ALL SELECT id + 100, ts, 0 FROM {src}"
    ),
    "union_second": (
        "INSERT INTO {t} (id, v, k) SELECT id + 100, ts, 0 FROM {src} "
        "UNION ALL SELECT id, #, 0 FROM {src}"
    ),
    "union_null": (
        "INSERT INTO {t} (id, v, k) SELECT id, #, 0 FROM {src} UNION ALL SELECT 100, NULL, 0"
    ),
    "union_distinct_sub": (
        "INSERT INTO {t} (id, v, k) SELECT id, w, 0 FROM "
        "(SELECT id, # AS w FROM {src} UNION SELECT id + 100, ts FROM {src}) q"
    ),
    "except": (
        "INSERT INTO {t} (id, v, k) SELECT id, #, 0 FROM {src} "
        "EXCEPT SELECT id + 100, ts, 0 FROM {src}"
    ),
    "scalar": (
        "INSERT INTO {t} (id, v, k) SELECT id, "
        "coalesce((SELECT max($r.{c}$) FROM {src} r WHERE r.id = {src}.id), ts), 0 FROM {src}"
    ),
    "join_using": (
        "INSERT INTO {t} (id, v, k) SELECT id, coalesce(b.w, a.ts), 0 "
        "FROM {src} a JOIN (SELECT id, # AS w FROM {src}) b USING (id)"
    ),
    "lateral": (
        "INSERT INTO {t} (id, v, k) SELECT o.id, coalesce(l.w, o.ts), 0 "
        "FROM {src} o, LATERAL (SELECT $o.{c}$ AS w) l"
    ),
    "values": (
        f"INSERT INTO {{t}} (id, v, k) VALUES (1, ${{lit}}$, 0), (2, coalesce(${{lit}}$, {TSL}), 0)"
    ),
    "where_plain": (
        "INSERT INTO {t} (id, v, k) SELECT id, {c}, 0 FROM {src} "
        "WHERE # > TIMESTAMP '1500-01-01 00:00:00'"
    ),
    "order_plain": ("INSERT INTO {t} (id, v, k) SELECT id, {c}, 0 FROM {src} ORDER BY # LIMIT 100"),
    "sibling_narrow_where": (
        "INSERT INTO {t} (id, v, k) SELECT id, coalesce(#, ts), 0 FROM {src} "
        "WHERE coalesce({c}, ts) IS NOT NULL"
    ),
    "two_cols": "INSERT INTO {t} (id, v, k, c) SELECT id, coalesce(#, ts), 0, {c} FROM {src}",
    "two_cols_swapped": (
        "INSERT INTO {t} (id, v, k, c) SELECT id, {c}, 0, coalesce(#, ts) FROM {src}"
    ),
    "by_name": "INSERT INTO {t} BY NAME SELECT 0 AS k, coalesce(#, ts) AS v, id FROM {src}",
    "overwrite": (
        "INSERT OVERWRITE {t} SELECT id, coalesce(#, ts), 0, CAST(NULL AS {n}), ts, ntz FROM {src}"
    ),
    "S:update": "UPDATE {t} SET v = coalesce($c$, ts) WHERE id >= 0",
    "S:update_alias": "UPDATE {t} AS u SET u.v = if(u.id > 0, $u.c$, u.ts) WHERE u.id >= 0",
    "S:update_nowhere": "UPDATE {t} SET v = coalesce($c$, ts)",
    "M:update": "UPDATE {t} SET v = greatest($c$, ts) WHERE id >= 0",
    "S:update_scalar": (
        "UPDATE {t} SET v = (SELECT max($r.{c}$) FROM {src} r WHERE r.id = {tn}.id) WHERE id >= 0"
    ),
    "S:merge_update": (
        "MERGE INTO {t} t USING {src} s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = coalesce($s.{c}$, s.ts)"
    ),
    "M:merge_update": (
        "MERGE INTO {t} t USING {src} s ON t.id = s.id WHEN MATCHED THEN "
        "UPDATE SET t.v = CASE WHEN s.id > 0 THEN $s.{c}$ ELSE s.ts END"
    ),
    "merge_insert": (
        "MERGE INTO {t} t USING (SELECT id, # AS w, ts FROM {src}) s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, coalesce(s.w, s.ts), 0)"
    ),
    "S:merge_by_source": (
        "MERGE INTO {t} t USING (SELECT id + 1000 AS id FROM {src}) s ON t.id = s.id "
        "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = coalesce($t.c$, t.ts)"
    ),
    "X:ctas": (
        "CREATE TABLE {t} USING iceberg TBLPROPERTIES ('format-version' = '3') AS "
        "SELECT id, coalesce(#, ts) AS v, {c} AS c FROM {src}"
    ),
    "df_cast": (
        "py:spark.table('{src}').select('id', "
        'F.coalesce(F.expr("""#"""), F.col(\'ts\')).alias(\'v\'), '
        "F.lit(0).alias('k')).writeTo('{t}').append()"
    ),
    "df_selectexpr": (
        'py:spark.table(\'{src}\').selectExpr(\'id\', """if(id > 0, #, ts) AS v""", '
        "'0 AS k').write.insertInto('{t}')"
    ),
    "df_union": (
        "py:spark.table('{src}').selectExpr('id', \"\"\"# AS v\"\"\", '0 AS k')"
        ".union(spark.table('{src}').selectExpr('id + 100', 'ts', '0'))"
        ".writeTo('{t}').append()"
    ),
    "df_view": (
        "py:spark.table('{src}').selectExpr('id', \"\"\"# AS w\"\"\", 'ts')"
        ".createOrReplaceTempView('fv_{tn}'); "
        "spark.sql('INSERT INTO {t} (id, v, k) SELECT id, coalesce(w, ts), 0 FROM fv_{tn}')"
    ),
}
MOR = (
    ", 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read'"
)
_DOLLAR = re.compile(r"\$([^$]*)\$")
_HEX = re.compile(r"[0-9a-f]{8,}")


def build(template: str, call: str) -> str:
    """Substitute one written call into ``template``."""
    text = template.replace("#", call.replace("@", "{c}"))
    return _DOLLAR.sub(lambda match: call.replace("@", match.group(1)), text)


def classify(plan: str) -> str:
    """Return unresolved when a nanosecond cast or literal is still in ``plan``."""
    markers = ("Timestamp(ns)", "Timestamp(Nanosecond", "TimestampNanosecond")
    if any(marker in plan for marker in markers):
        return "unresolved"
    return "resolved"


def scrub(text: str) -> str:
    """Drop generated hex ids and keep digit-only literals."""

    def replace(match: re.Match[str]) -> str:
        token = match.group(0)
        if any(char in "abcdef" for char in token):
            return "<id>"
        return token

    flat = " ".join(_HEX.sub(replace, text).split())
    if len(flat) <= 220:
        return flat
    cut = flat[:220].rsplit(" ", 1)[0]
    return cut


def plan_line(plan: str) -> str:
    """Return the projection line, or the first line that still names nanoseconds."""
    chosen = ""
    for line in plan.splitlines():
        if "Timestamp(ns)" in line or "TimestampNanosecond" in line:
            return scrub(line)
        if line.strip().startswith("Projection:") and not chosen:
            chosen = line
    if chosen:
        return scrub(chosen)
    return scrub(plan)


def logical_plan(spark: Any, statement: str) -> str:
    """Return the logical plan text of ``statement``."""
    rows = spark.sql(f"EXPLAIN {statement}").collect()
    parts = [row["plan"] for row in rows if row["plan_type"] == "logical_plan"]
    return "\n".join(parts)


def frame_plan(frame: Any) -> str:
    """Return the logical section of a DataFrame ``explain``."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        frame.explain(True)
    text = buffer.getvalue()
    start = text.find("== Optimized Logical Plan ==")
    end = text.find("== Physical Plan ==")
    if start >= 0:
        body = text[start : end if end > start else None]
        return body
    return text


def names_for(table: str, short: str) -> dict[str, str]:
    """Return the format names one position uses."""
    return {
        "t": table,
        "tn": short,
        "c": "ns",
        "n": SOURCES["ns"],
        "src": SRC,
        "lit": "CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns)",
    }


def fill(text: str, names: dict[str, str]) -> str:
    """Format ``text`` with ``names``."""
    return text.format(**names)


def explain_sql(spark: Any, body: str, names: dict[str, str]) -> str:
    """Execute setup statements and return the logical plan of the last one."""
    parts = [part.strip() for part in body.split(";;") if part.strip()]
    for part in parts[:-1]:
        spark.sql(fill(part, names)).toArrow()
    last = fill(parts[-1], names)
    folded = last.lstrip().upper()
    marker = " AS SELECT "
    index = folded.find(marker)
    if folded.startswith("CREATE TABLE") and index > 0:
        return logical_plan(spark, last[index + 4 :])
    return logical_plan(spark, last)


def explain_python(spark: Any, body: str, names: dict[str, str]) -> str:
    """Return the plan of a DataFrame position without writing it."""
    text = body[3:]
    for word, value in names.items():
        text = text.replace("{" + word + "}", value)
    scope: dict[str, Any] = {"spark": spark, "F": spark_functions}
    if "createOrReplaceTempView" in text and "spark.sql(" in text:
        view_sql, _, rest = text.partition(";")
        exec(view_sql, scope)
        match = re.search(r"spark\.sql\('(.*)'\)", rest)
        if match is None:
            raise ValueError("position has no spark.sql literal")
        return logical_plan(spark, match.group(1))
    expression = text
    for suffix in (".writeTo(", ".write.insertInto("):
        if suffix in expression:
            expression = expression.split(suffix)[0]
            break
    frame = eval(expression, scope)
    return frame_plan(frame)


def one_position(
    spark: Any,
    number: int,
    position: str,
    call_name: str,
    body: str,
) -> dict[str, str]:
    """Plan one position and drop the table it created."""
    flags = position[:2] if position[:2] in {"S:", "M:", "X:"} else ""
    name = position[2:] if flags else position
    short = f"c{number}"
    table = f"ice.ns.{short}"
    mode = MOR if flags == "M:" else ""
    label = f"{name}~{call_name}"
    try:
        if flags != "X:":
            spark.sql(
                f"CREATE TABLE {table} ("
                "id INT, v TIMESTAMP, k INT, c timestamp_ns, ts TIMESTAMP, ntz TIMESTAMP_NTZ"
                ") USING iceberg "
                f"TBLPROPERTIES ('format-version' = '3'{mode})"
            ).toArrow()
        if flags in {"S:", "M:"}:
            spark.sql(
                f"INSERT INTO {table} (id, k, c, ts, ntz) SELECT id, 0, ns, ts, ntz FROM {SRC}"
            ).toArrow()
        filled = names_for(table, short)
        if body.startswith("py:"):
            plan = explain_python(spark, body, filled)
        else:
            plan = explain_sql(spark, body, filled)
        return {
            "position": label,
            "family": name,
            "call": call_name,
            "status": classify(plan),
            "plan_line": plan_line(plan),
        }
    except Exception as error:
        text = doors.error_text(error)
        status = "unexplained" if "Unsupported SQL statement: MERGE" in text else "error"
        return {
            "position": label,
            "family": name,
            "call": call_name,
            "status": status,
            "plan_line": text,
        }
    finally:
        for statement in (
            f"DROP TABLE IF EXISTS {table}",
            f"DROP VIEW IF EXISTS rv_{short}",
            f"DROP VIEW IF EXISTS fv_{short}",
        ):
            with contextlib.suppress(Exception):
                spark.sql(statement).toArrow()


def cases() -> list[tuple[str, str, str]]:
    """Return ``(position, call, body)`` for the 64 positions and four calls."""
    found: list[tuple[str, str, str]] = []
    for position, template in POS.items():
        for call_name, call in CALLS.items():
            found.append((position, call_name, build(template, call)))
    return found


def markdown(rows: list[dict[str, str]]) -> str:
    """Return the census table."""
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    lines = [
        "# Written TIMESTAMP casts — ICE-TSNS-COERCION-1 slice 0",
        "",
        "Generated by `_record_ice_tsns_coercion_1_casts.py`. Do not edit by hand.",
        "",
        "Each row is one of the 64 R-017 positions planned with one written form:",
        "`CAST(x AS TIMESTAMP)`, `x::timestamp`, `to_timestamp(x)`, or `TIMESTAMP '…'`.",
        "`x` is the `timestamp_ns` column, except the literal form, which is the literal.",
        "The plan is `EXPLAIN`'s logical plan. Unresolved means that plan still contains",
        "`Timestamp(ns)` or a nanosecond timestamp literal.",
        "",
        f"- Rows: {len(rows)}",
        f"- Resolved: {counts.get('resolved', 0)}",
        f"- Unresolved: {counts.get('unresolved', 0)}",
        f"- Unexplained: {counts.get('unexplained', 0)}",
        f"- Error: {counts.get('error', 0)}",
        "",
        "`unexplained` is a MERGE. `EXPLAIN` on this build rejects MERGE, so the",
        "analyzed plan of that statement is not on the public surface.",
        "",
        "| position | status | plan line |",
        "|---|---|---|",
    ]
    for row in rows:
        line = row["plan_line"].replace("|", "/")
        lines.append(f"| {row['position']} | {row['status']} | `{line}` |")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    """Write the census JSON and, with ``--markdown``, the table."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--markdown", type=Path)
    parser.add_argument("--zone", default="America/New_York")
    args = parser.parse_args()
    rows: list[dict[str, str]] = []
    with tempfile.TemporaryDirectory(prefix="tsns-coercion-casts-") as scratch:
        spark = doors.open_session(args.zone, Path(scratch))
        try:
            for number, (position, call_name, body) in enumerate(cases()):
                rows.append(one_position(spark, number, position, call_name, body))
                if number % 32 == 0:
                    print(f"casts {number}/{len(POS) * len(CALLS)}", flush=True)
        finally:
            spark.stop()
    payload = {"positions": len(POS), "calls": list(CALLS), "rows": rows}
    args.output.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8")
    if args.markdown is not None:
        args.markdown.write_text(markdown(rows), encoding="utf-8")
    print(
        f"done {len(rows)} "
        + " ".join(
            f"{name}={sum(row['status'] == name for row in rows)}"
            for name in ("resolved", "unresolved", "unexplained", "error")
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
