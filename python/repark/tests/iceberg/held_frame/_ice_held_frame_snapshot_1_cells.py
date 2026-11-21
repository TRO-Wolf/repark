from __future__ import annotations

import contextlib
import io
import re
from collections.abc import Callable
from typing import Any

CONDITION = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")


def condition(error: BaseException) -> str:
    text = " ".join(str(error).split())
    found = CONDITION.search(text)
    return "ERROR " + (found.group(1) if found else text[:120])


def rows(frame: Any) -> list[str]:
    return sorted(repr(tuple(row)) for row in frame.collect())


def named(text: str) -> str | None:
    found = CONDITION.search(" ".join(text.split()))
    return found.group(1) if found else None


def explained(frame: Any, *mode: Any) -> list[Any]:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        frame.explain(*mode)
    return ["PRINTED", named(buffer.getvalue())]


class Cells:
    def __init__(self, session: Any) -> None:
        self.s = session
        self.counter = 0

    def run_sql(self, statement: str) -> None:
        self.s.sql(statement).collect()

    def make(self, columns: str = "n INT, c STRING", seed: str = "(1, 'a'), (2, 'b')") -> str:
        self.counter += 1
        name = "lc.db.h" + str(self.counter)
        self.run_sql("CREATE TABLE " + name + " (" + columns + ") USING iceberg")
        if seed:
            self.run_sql("INSERT INTO " + name + " VALUES " + seed)
        return name

    def insert(self, t: str) -> None:
        self.run_sql("INSERT INTO " + t + " VALUES (9, 'z')")

    def recreate(self, t: str) -> None:
        self.run_sql("DROP TABLE " + t)
        self.run_sql("CREATE TABLE " + t + " (n INT, c STRING) USING iceberg")
        self.run_sql("INSERT INTO " + t + " VALUES (5, 'r')")

    def drop_column(self, t: str) -> None:
        self.run_sql("ALTER TABLE " + t + " DROP COLUMN c")
        self.run_sql("INSERT INTO " + t + " VALUES (9)")

    def add_column(self, t: str) -> None:
        self.run_sql("ALTER TABLE " + t + " ADD COLUMN d INT")
        self.run_sql("INSERT INTO " + t + " VALUES (9, 'z', 5)")

    def readd_column(self, t: str) -> None:
        self.run_sql("ALTER TABLE " + t + " DROP COLUMN c")
        self.run_sql("ALTER TABLE " + t + " ADD COLUMN c STRING")
        self.insert(t)

    def insert_collect(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.insert(t)
        return rows(held)

    def delete_count(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.run_sql("DELETE FROM " + t + " WHERE n = 1")
        return held.count()

    def delete_copy_stored(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.run_sql("DELETE FROM " + t + " WHERE n = 1")
        held.writeTo(t + "_c").create()
        return rows(self.s.table(t + "_c"))

    def delete_insert_into_stored(self) -> Any:
        t = self.make()
        held = self.s.table(t).filter("n > 0")
        other = self.make(seed="")
        self.run_sql("DELETE FROM " + t + " WHERE n = 1")
        held.write.insertInto(other)
        return rows(self.s.table(other))

    def self_append(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.insert(t)
        held.writeTo(t).append()
        return rows(self.s.table(t))

    def own_collect_twice(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        first = rows(held)
        self.insert(t)
        return [first, rows(held)]

    def own_explain_then_collect(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        with contextlib.redirect_stdout(io.StringIO()):
            held.explain()
        self.insert(t)
        return rows(held)

    def own_collect_then_count(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        first = rows(held)
        self.insert(t)
        return [first, held.count()]

    def checkpoint_after_collect(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        first = rows(held)
        self.insert(t)
        point = held.localCheckpoint()
        return [first, point.count()]

    def recreate_count(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.recreate(t)
        return held.count()

    def dropped_table_count(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.run_sql("DROP TABLE " + t)
        return held.count()

    def drop_column_count(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.drop_column(t)
        return held.count()

    def add_column_collect(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.add_column(t)
        return [list(held.columns), rows(held)]

    def add_column_derived_copy(self) -> Any:
        t = self.make()
        held = self.s.table(t).filter("n > 0")
        self.add_column(t)
        held.writeTo(t + "_c").create()
        return rows(self.s.table(t + "_c"))

    def dfview_recreate(self) -> Any:
        t = self.make()
        self.s.table(t).createOrReplaceTempView("hv" + str(self.counter))
        view = "hv" + str(self.counter)
        self.recreate(t)
        return rows(self.s.sql("SELECT * FROM " + view))

    def dfview_drop_column(self) -> Any:
        t = self.make()
        view = "hv" + str(self.counter)
        self.s.table(t).createOrReplaceTempView(view)
        self.drop_column(t)
        return rows(self.s.sql("SELECT * FROM " + view))

    def readd_by_name(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.readd_column(t)
        return rows(held)

    def widen_count(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.run_sql("ALTER TABLE " + t + " ALTER COLUMN n TYPE BIGINT")
        self.insert(t)
        return held.count()

    def reorder_collect(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.run_sql("ALTER TABLE " + t + " ALTER COLUMN c FIRST")
        self.run_sql("INSERT INTO " + t + " VALUES ('z', 9)")
        return rows(held)

    def call_rollback(self) -> Any:
        t = self.make()
        seed = self.s.sql("SELECT snapshot_id FROM " + t + ".snapshots").collect()[0][0]
        self.insert(t)
        held = self.s.table(t)
        short = t.split(".", 1)[1]
        self.run_sql("CALL lc.system.rollback_to_snapshot('" + short + "', " + str(seed) + ")")
        return held.count()

    def version_as_of_pinned(self) -> Any:
        t = self.make()
        seed = self.s.sql("SELECT snapshot_id FROM " + t + ".snapshots").collect()[0][0]
        held = self.s.sql("SELECT * FROM " + t + " VERSION AS OF " + str(seed))
        self.insert(t)
        return rows(held)

    def join_with_fresh(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.insert(t)
        return rows(held.join(self.s.table(t), ["n", "c"]))

    def truncate_count(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.run_sql("TRUNCATE TABLE " + t)
        return held.count()

    def insert_explain(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.insert(t)
        return explained(held)

    def drop_column_explain(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.drop_column(t)
        return explained(held)

    def drop_column_filter_explain(self) -> Any:
        t = self.make()
        held = self.s.table(t).filter("n > 0")
        self.drop_column(t)
        return explained(held)

    def drop_column_explain_extended(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.drop_column(t)
        return explained(held, True)

    def recreate_explain(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.recreate(t)
        return explained(held)

    def dropped_table_explain(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.run_sql("DROP TABLE " + t)
        return explained(held)

    def dfview_drop_column_sql_explain(self) -> Any:
        t = self.make()
        view = "hv" + str(self.counter)
        self.s.table(t).createOrReplaceTempView(view)
        self.drop_column(t)
        found = self.s.sql("EXPLAIN SELECT * FROM " + view).collect()
        return ["ROWS", named(" ".join(str(value) for row in found for value in row))]

    def dfview_frame(self, t: str, keep: str = "") -> Any:
        view = "hv" + str(self.counter)
        held = self.s.table(t)
        (held.filter(keep) if keep else held).createOrReplaceTempView(view)
        return view

    def dfview_frame_recreate_count(self) -> Any:
        t = self.make()
        frame = self.s.sql("SELECT * FROM " + self.dfview_frame(t))
        self.recreate(t)
        return frame.count()

    def dfview_frame_recreate_collect(self) -> Any:
        t = self.make()
        frame = self.s.sql("SELECT * FROM " + self.dfview_frame(t))
        self.recreate(t)
        return rows(frame)

    def dfview_frame_recreate_explain(self) -> Any:
        t = self.make()
        frame = self.s.sql("SELECT * FROM " + self.dfview_frame(t))
        self.recreate(t)
        return explained(frame)

    def dfview_frame_drop_column_count(self) -> Any:
        t = self.make()
        frame = self.s.sql("SELECT * FROM " + self.dfview_frame(t))
        self.drop_column(t)
        return frame.count()

    def dfview_frame_drop_column_explain(self) -> Any:
        t = self.make()
        frame = self.s.sql("SELECT * FROM " + self.dfview_frame(t))
        self.drop_column(t)
        return explained(frame)

    def dfview_frame_dropped_table_count(self) -> Any:
        t = self.make()
        frame = self.s.sql("SELECT * FROM " + self.dfview_frame(t))
        self.run_sql("DROP TABLE " + t)
        return frame.count()

    def dfview_frame_dropped_table_explain(self) -> Any:
        t = self.make()
        frame = self.s.sql("SELECT * FROM " + self.dfview_frame(t))
        self.run_sql("DROP TABLE " + t)
        return explained(frame)

    def dfview_frame_insert_count(self) -> Any:
        t = self.make()
        frame = self.s.sql("SELECT * FROM " + self.dfview_frame(t))
        self.insert(t)
        return frame.count()

    def dfview_frame_insert_explain(self) -> Any:
        t = self.make()
        frame = self.s.sql("SELECT * FROM " + self.dfview_frame(t))
        self.insert(t)
        return explained(frame)

    def dfview_written_frame_recreate_count(self) -> Any:
        t = self.make()
        view = self.dfview_frame(t)
        self.insert(t)
        frame = self.s.sql("SELECT * FROM " + view)
        self.recreate(t)
        return frame.count()

    def dfview_filter_frame_recreate_count(self) -> Any:
        t = self.make()
        frame = self.s.sql("SELECT * FROM " + self.dfview_frame(t, "n > 0"))
        self.recreate(t)
        return frame.count()

    def run(self) -> dict[str, Any]:
        cells: dict[str, Callable[[], Any]] = {
            "insert_collect": self.insert_collect,
            "delete_count": self.delete_count,
            "delete_copy_stored": self.delete_copy_stored,
            "delete_insert_into_stored": self.delete_insert_into_stored,
            "self_append": self.self_append,
            "own_collect_twice": self.own_collect_twice,
            "own_explain_then_collect": self.own_explain_then_collect,
            "own_collect_then_count": self.own_collect_then_count,
            "checkpoint_after_collect": self.checkpoint_after_collect,
            "recreate_count": self.recreate_count,
            "dropped_table_count": self.dropped_table_count,
            "drop_column_count": self.drop_column_count,
            "add_column_collect": self.add_column_collect,
            "add_column_derived_copy": self.add_column_derived_copy,
            "dfview_recreate": self.dfview_recreate,
            "dfview_drop_column": self.dfview_drop_column,
            "readd_by_name": self.readd_by_name,
            "widen_count": self.widen_count,
            "reorder_collect": self.reorder_collect,
            "call_rollback": self.call_rollback,
            "version_as_of_pinned": self.version_as_of_pinned,
            "join_with_fresh": self.join_with_fresh,
            "truncate_count": self.truncate_count,
            "insert_explain": self.insert_explain,
            "drop_column_explain": self.drop_column_explain,
            "drop_column_filter_explain": self.drop_column_filter_explain,
            "drop_column_explain_extended": self.drop_column_explain_extended,
            "recreate_explain": self.recreate_explain,
            "dropped_table_explain": self.dropped_table_explain,
            "dfview_drop_column_sql_explain": self.dfview_drop_column_sql_explain,
            "dfview_frame_recreate_count": self.dfview_frame_recreate_count,
            "dfview_frame_recreate_collect": self.dfview_frame_recreate_collect,
            "dfview_frame_recreate_explain": self.dfview_frame_recreate_explain,
            "dfview_frame_drop_column_count": self.dfview_frame_drop_column_count,
            "dfview_frame_drop_column_explain": self.dfview_frame_drop_column_explain,
            "dfview_frame_dropped_table_count": self.dfview_frame_dropped_table_count,
            "dfview_frame_dropped_table_explain": self.dfview_frame_dropped_table_explain,
            "dfview_frame_insert_count": self.dfview_frame_insert_count,
            "dfview_frame_insert_explain": self.dfview_frame_insert_explain,
            "dfview_written_frame_recreate_count": self.dfview_written_frame_recreate_count,
            "dfview_filter_frame_recreate_count": self.dfview_filter_frame_recreate_count,
        }
        found: dict[str, Any] = {}
        for key, body in cells.items():
            try:
                found[key] = body()
            except Exception as error:
                found[key] = condition(error)
        return found
