from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from _ice_held_frame_snapshot_1_cells import condition, rows

MERGE = (
    "MERGE INTO {t} AS d USING (SELECT 2 AS n, 'y' AS c UNION ALL SELECT 9 AS n, 'z' AS c) AS u"
    " ON d.n = u.n WHEN MATCHED THEN UPDATE SET c = u.c WHEN NOT MATCHED THEN INSERT *"
)


def attempt(body: Callable[[], Any]) -> Any:
    try:
        return body()
    except Exception as error:
        return condition(error)


class CachedCells:
    def __init__(self, session: Any, functions: Any) -> None:
        self.s = session
        self.F = functions
        self.counter = 0

    def run_sql(self, statement: str) -> None:
        self.s.sql(statement).collect()

    def make(self) -> str:
        self.counter += 1
        name = "lc.db.k" + str(self.counter)
        self.run_sql("CREATE TABLE " + name + " (n INT, c STRING) USING iceberg")
        self.run_sql("INSERT INTO " + name + " VALUES (1, 'a'), (2, 'b')")
        return name

    def insert(self, t: str) -> None:
        self.run_sql("INSERT INTO " + t + " VALUES (9, 'z')")

    def one(self, n: int, c: str) -> Any:
        return self.s.createDataFrame([(n, c)], "n INT, c STRING")

    def held_cached(self) -> tuple[str, Any]:
        t = self.make()
        held = self.s.table(t)
        held.cache()
        return t, held

    def cache_count(self) -> Any:
        t, held = self.held_cached()
        first = held.count()
        self.insert(t)
        return [first, held.count(), self.s.table(t).count()]

    def cache_lazy(self) -> Any:
        t, held = self.held_cached()
        self.insert(t)
        return [held.count(), self.s.table(t).count()]

    def cache_table(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        self.run_sql("CACHE TABLE " + t)
        self.insert(t)
        return [held.count(), self.s.table(t).count()]

    def cache_append(self) -> Any:
        t, held = self.held_cached()
        first = held.count()
        self.one(9, "z").writeTo(t).append()
        return [first, held.count(), self.s.table(t).count()]

    def cache_held(self) -> Any:
        t, held = self.held_cached()
        first = held.count()
        self.insert(t)
        return [first, held.count()]

    def write_then_read(self, write: Callable[[str], None]) -> Any:
        t, held = self.held_cached()
        first = held.count()
        write(t)
        return [first, held.count(), rows(held.select("n", "c"))]

    def delete_refills(self) -> Any:
        return self.write_then_read(lambda t: self.run_sql("DELETE FROM " + t + " WHERE n = 1"))

    def merge_refills(self) -> Any:
        return self.write_then_read(lambda t: self.run_sql(MERGE.format(t=t)))

    def overwrite_refills(self) -> Any:
        return self.write_then_read(
            lambda t: self.one(7, "o").writeTo(t).overwrite(self.F.expr("n > 0"))
        )

    def insert_into_refills(self) -> Any:
        return self.write_then_read(lambda t: self.one(9, "z").write.insertInto(t))

    def two_inserts(self) -> Any:
        t, held = self.held_cached()
        found = [held.count()]
        self.insert(t)
        found.append(held.count())
        self.run_sql("INSERT INTO " + t + " VALUES (8, 'q')")
        found.append(held.count())
        return found

    def child_of_cached(self) -> Any:
        t, held = self.held_cached()
        found = [held.count()]
        child = held.filter("n > 0")
        found.append(child.count())
        self.insert(t)
        found.append(child.count())
        return found

    def view_of_cached(self) -> Any:
        t, held = self.held_cached()
        found = [held.count()]
        held.createOrReplaceTempView("kv" + str(self.counter))
        self.insert(t)
        found.append(self.s.sql("SELECT count(*) FROM kv" + str(self.counter)).collect()[0][0])
        return found

    def cached_sql_view_frame(self) -> Any:
        t = self.make()
        view = "ks" + str(self.counter)
        self.run_sql("CREATE TEMPORARY VIEW " + view + " AS SELECT * FROM " + t)
        framed = self.s.sql("SELECT * FROM " + view)
        framed.cache()
        found = [framed.count()]
        self.insert(t)
        found.append(framed.count())
        return found

    def nested_cache(self) -> Any:
        t, held = self.held_cached()
        found = [held.count()]
        child = held.filter("n > 0")
        child.cache()
        found.append(child.count())
        self.insert(t)
        found.extend([child.count(), held.count()])
        return found

    def join_other_table(self) -> Any:
        t = self.make()
        u = self.make()
        joined = self.s.table(t).join(self.s.table(u).withColumnRenamed("c", "d"), "n")
        joined.cache()
        found = [joined.count()]
        self.run_sql("INSERT INTO " + u + " VALUES (1, 'q')")
        found.append(joined.count())
        return found

    def own_count_collect(self) -> Any:
        t, held = self.held_cached()
        found: list[Any] = [held.count()]
        self.insert(t)
        found.append(rows(held))
        return found

    def own_collect_twice(self) -> Any:
        t, held = self.held_cached()
        found = [rows(held)]
        self.insert(t)
        found.extend([rows(held), held.count()])
        return found

    def own_insert_collect(self) -> Any:
        t, held = self.held_cached()
        self.insert(t)
        return [rows(held), held.count()]

    def child_collect_twice(self) -> Any:
        t, held = self.held_cached()
        found: list[Any] = [held.count()]
        child = held.filter("n > 0")
        found.append(rows(child))
        self.insert(t)
        found.extend([rows(child), held.count()])
        return found

    def child_first_collect(self) -> Any:
        t, held = self.held_cached()
        child = held.filter("n > 0")
        found: list[Any] = [child.count()]
        self.insert(t)
        found.append(rows(child))
        return found

    def collect_then_cache(self) -> Any:
        t = self.make()
        held = self.s.table(t)
        found: list[Any] = [rows(held)]
        held.cache()
        found.append(held.count())
        self.insert(t)
        found.extend([held.count(), rows(held)])
        return found

    def schema_change(self, statements: list[str]) -> Any:
        t, held = self.held_cached()
        found: list[Any] = [held.count()]
        for statement in statements:
            self.run_sql(statement.format(t=t))
        found.append(attempt(held.count))
        found.append(attempt(lambda: rows(held.select("n"))))
        found.append(held.is_cached)
        return found

    def dropped_column_keeps_the_fill(self) -> Any:
        return self.schema_change(["ALTER TABLE {t} DROP COLUMN c"])

    def renamed_column_keeps_the_fill(self) -> Any:
        return self.schema_change(["ALTER TABLE {t} RENAME COLUMN c TO d"])

    def added_column_then_insert(self) -> Any:
        return self.schema_change(
            ["ALTER TABLE {t} ADD COLUMN x INT", "INSERT INTO {t} VALUES (9, 'z', 1)"]
        )

    def dropped_column_then_insert(self) -> Any:
        return self.schema_change(["ALTER TABLE {t} DROP COLUMN c", "INSERT INTO {t} VALUES (9)"])

    def recreated_table(self) -> Any:
        return self.schema_change(
            [
                "DROP TABLE {t}",
                "CREATE TABLE {t} (n INT, c STRING) USING iceberg",
                "INSERT INTO {t} VALUES (5, 'r')",
            ]
        )

    def dropped_table(self) -> Any:
        return self.schema_change(["DROP TABLE {t}"])

    def stamped(self, event: Callable[[str, str], Any]) -> Any:
        t = self.make()
        other = self.make()
        held = self.s.table(t).withColumn("ts", self.F.current_timestamp())
        held.cache()
        first = held.agg(self.F.max("ts")).collect()[0][0]
        time.sleep(0.05)
        note = attempt(lambda: event(t, other))
        second = held.agg(self.F.max("ts")).collect()[0][0]
        return [first == second, held.count(), note]

    def kept_after_another_table_write(self) -> Any:
        return self.stamped(lambda t, other: self.insert(other))

    def kept_after_a_failed_write(self) -> Any:
        return self.stamped(
            lambda t, other: self.run_sql("INSERT INTO " + t + " (n, zz) VALUES (1, 'q')")
        )

    def kept_after_a_property_change(self) -> Any:
        return self.stamped(
            lambda t, other: self.run_sql("ALTER TABLE " + t + " SET TBLPROPERTIES ('k1' = 'v1')")
        )

    def kept_after_a_fresh_read(self) -> Any:
        return self.stamped(lambda t, other: self.s.table(t).count())

    def refilled_after_an_insert(self) -> Any:
        return self.stamped(lambda t, other: self.insert(t))

    def refilled_after_refresh_table(self) -> Any:
        return self.stamped(lambda t, other: self.run_sql("REFRESH TABLE " + t))

    def refilled_after_refresh_api(self) -> Any:
        return self.stamped(lambda t, other: self.s.catalog.refreshTable(t))

    def run(self) -> dict[str, Any]:
        cells: dict[str, Callable[[], Any]] = {
            "cache_count": self.cache_count,
            "cache_lazy": self.cache_lazy,
            "cache_table": self.cache_table,
            "cache_append": self.cache_append,
            "cache_held": self.cache_held,
            "delete_refills": self.delete_refills,
            "merge_refills": self.merge_refills,
            "overwrite_refills": self.overwrite_refills,
            "insert_into_refills": self.insert_into_refills,
            "two_inserts": self.two_inserts,
            "child_of_cached": self.child_of_cached,
            "view_of_cached": self.view_of_cached,
            "cached_sql_view_frame": self.cached_sql_view_frame,
            "nested_cache": self.nested_cache,
            "join_other_table": self.join_other_table,
            "own_count_collect": self.own_count_collect,
            "own_collect_twice": self.own_collect_twice,
            "own_insert_collect": self.own_insert_collect,
            "child_collect_twice": self.child_collect_twice,
            "child_first_collect": self.child_first_collect,
            "collect_then_cache": self.collect_then_cache,
            "dropped_column_keeps_the_fill": self.dropped_column_keeps_the_fill,
            "renamed_column_keeps_the_fill": self.renamed_column_keeps_the_fill,
            "added_column_then_insert": self.added_column_then_insert,
            "dropped_column_then_insert": self.dropped_column_then_insert,
            "recreated_table": self.recreated_table,
            "dropped_table": self.dropped_table,
            "kept_after_another_table_write": self.kept_after_another_table_write,
            "kept_after_a_failed_write": self.kept_after_a_failed_write,
            "kept_after_a_property_change": self.kept_after_a_property_change,
            "kept_after_a_fresh_read": self.kept_after_a_fresh_read,
            "refilled_after_an_insert": self.refilled_after_an_insert,
            "refilled_after_refresh_table": self.refilled_after_refresh_table,
            "refilled_after_refresh_api": self.refilled_after_refresh_api,
        }
        found: dict[str, Any] = {}
        for key, body in cells.items():
            found[key] = attempt(body)
            self.s.catalog.clearCache()
        return found
