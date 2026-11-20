from __future__ import annotations

from collections.abc import Callable
from typing import Any

KEY = "review-test-key"


def rows(frame: Any) -> list[str]:
    return sorted(repr(tuple(row)) for row in frame.collect())


def answer(body: Callable[[], Any]) -> Any:
    try:
        found = body()
    except Exception as error:
        lines = str(error).strip().splitlines()
        return "ERROR " + type(error).__name__ + ": " + (lines[0] if lines else "")
    return "OK" if found is None else found


class EncCells:
    def __init__(self, session: Any) -> None:
        self.s = session
        self.counter = 0

    def run_sql(self, statement: str) -> None:
        self.s.sql(statement).collect()

    def make(self, keyed: bool) -> str:
        self.counter += 1
        name = "lc.db.e" + str(self.counter)
        self.run_sql("CREATE TABLE " + name + " (n INT, c STRING) USING iceberg")
        self.run_sql("INSERT INTO " + name + " VALUES (1, 'a'), (2, 'b')")
        if keyed:
            self.add_key(name)
        return name

    def add_key(self, t: str) -> None:
        self.run_sql(
            "ALTER TABLE " + t + " SET TBLPROPERTIES ('encryption.key-id' = '" + KEY + "')"
        )

    def drop_key(self, t: str) -> None:
        self.run_sql("ALTER TABLE " + t + " UNSET TBLPROPERTIES ('encryption.key-id')")

    def touch(self, t: str) -> None:
        self.run_sql("ALTER TABLE " + t + " SET TBLPROPERTIES ('comment' = 'held')")

    def refused_keeps_held(self, write: Callable[[str, Any], Any]) -> dict[str, Any]:
        t = self.make(keyed=True)
        held = self.s.table(t)
        before = rows(held)
        refusal = answer(lambda: write(t, held))
        return {
            "before": before,
            "refusal": refusal,
            "held_after": rows(held),
            "fresh_after": rows(self.s.table(t)),
        }

    def refused_sql_insert(self) -> Any:
        return self.refused_keeps_held(
            lambda t, _: self.run_sql("INSERT INTO " + t + " VALUES (9, 'z')")
        )

    def refused_sql_delete(self) -> Any:
        return self.refused_keeps_held(
            lambda t, _: self.run_sql("DELETE FROM " + t + " WHERE n = 1")
        )

    def refused_held_append(self) -> Any:
        return self.refused_keeps_held(lambda t, held: held.writeTo(t).append())

    def refused_frame_insert_into(self) -> Any:
        frame = self.s.createDataFrame([(9, "z")], "n INT, c STRING")
        return self.refused_keeps_held(lambda t, _: frame.write.insertInto(t))

    def pair(self, t: str, map_served: Callable[[], Any], fresh: Callable[[], Any]) -> list[Any]:
        return [answer(map_served), answer(fresh)]

    def keyed_read(self) -> Any:
        t = self.make(keyed=True)
        held = self.s.table(t)
        self.touch(t)
        return self.pair(t, lambda: rows(held), lambda: rows(self.s.table(t)))

    def keyed_view_read(self) -> Any:
        t = self.make(keyed=True)
        view = "ev" + str(self.counter)
        self.s.table(t).createOrReplaceTempView(view)
        self.touch(t)
        return self.pair(
            t, lambda: rows(self.s.sql("SELECT * FROM " + view)), lambda: rows(self.s.table(t))
        )

    def keyed_append(self) -> Any:
        t = self.make(keyed=True)
        held = self.s.table(t)
        self.touch(t)
        return self.pair(
            t, lambda: held.writeTo(t).append(), lambda: self.s.table(t).writeTo(t).append()
        )

    def keyed_insert_into(self) -> Any:
        t = self.make(keyed=True)
        held = self.s.table(t).filter("n > 0")
        self.touch(t)
        return self.pair(
            t,
            lambda: held.write.insertInto(t),
            lambda: self.s.table(t).filter("n > 0").write.insertInto(t),
        )

    def keyed_sql_from_view(self) -> Any:
        t = self.make(keyed=True)
        view = "ev" + str(self.counter)
        self.s.table(t).createOrReplaceTempView(view)
        self.touch(t)
        return self.pair(
            t,
            lambda: self.run_sql("INSERT INTO " + t + " SELECT * FROM " + view),
            lambda: self.run_sql("INSERT INTO " + t + " SELECT * FROM " + t),
        )

    def key_added_read(self) -> Any:
        t = self.make(keyed=False)
        held = self.s.table(t)
        self.run_sql("INSERT INTO " + t + " VALUES (9, 'z')")
        self.add_key(t)
        return self.pair(t, lambda: rows(held), lambda: rows(self.s.table(t)))

    def key_added_append(self) -> Any:
        t = self.make(keyed=False)
        held = self.s.table(t)
        self.add_key(t)
        return self.pair(
            t, lambda: held.writeTo(t).append(), lambda: self.s.table(t).writeTo(t).append()
        )

    def key_removed_read(self) -> Any:
        t = self.make(keyed=True)
        held = self.s.table(t)
        self.drop_key(t)
        self.run_sql("INSERT INTO " + t + " VALUES (9, 'z')")
        return self.pair(t, lambda: rows(held), lambda: rows(self.s.table(t)))

    def key_removed_append(self) -> Any:
        found = []
        for map_served in (True, False):
            t = self.make(keyed=True)
            held = self.s.table(t)
            self.drop_key(t)
            source = held if map_served else self.s.table(t)
            outcome = answer(lambda source=source, t=t: source.writeTo(t).append())
            found.append([outcome, rows(self.s.table(t))])
        return found

    def run(self) -> dict[str, Any]:
        cells: dict[str, Callable[[], Any]] = {
            "refused_sql_insert": self.refused_sql_insert,
            "refused_sql_delete": self.refused_sql_delete,
            "refused_held_append": self.refused_held_append,
            "refused_frame_insert_into": self.refused_frame_insert_into,
            "keyed_read": self.keyed_read,
            "keyed_view_read": self.keyed_view_read,
            "keyed_append": self.keyed_append,
            "keyed_insert_into": self.keyed_insert_into,
            "keyed_sql_from_view": self.keyed_sql_from_view,
            "key_added_read": self.key_added_read,
            "key_added_append": self.key_added_append,
            "key_removed_read": self.key_removed_read,
            "key_removed_append": self.key_removed_append,
        }
        return {key: answer(body) for key, body in cells.items()}
