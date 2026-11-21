from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

EXPECTED_PATH = Path(__file__).resolve().parent / "data" / "flat_matrix_expected.json"

PREDICATES: tuple[tuple[str, str], ...] = (
    ("p01", "i = 1"),
    ("p02", "i <> 1"),
    ("p03", "i IN (1, NULL)"),
    ("p04", "i NOT IN (1, NULL)"),
    ("p05", "i IS NULL"),
    ("p06", "i BETWEEN 1 AND 2"),
    ("p07", "i <=> NULL"),
    ("p08", "i + 1 > 0"),
    ("p09", "CAST(i AS STRING) = '1'"),
    ("p10", "l = 9223372036854775807"),
    ("p11", "dec = 1.10"),
    ("p12", "dec > 1.1"),
    ("p13", "d = CAST('NaN' AS DOUBLE)"),
    ("p14", "isnan(d)"),
    ("p15", "d = 0.0"),
    ("p16", "d > 0"),
    ("p17", "s = ''"),
    ("p18", "s = 'a '"),
    ("p19", "s LIKE 'a%'"),
    ("p20", "upper(s) = 'A'"),
    ("p21", "length(s) = 0"),
    ("p22", "b"),
    ("p23", "b IS NOT TRUE"),
    ("p24", "dt = DATE '1970-01-01'"),
    ("p25", "ts > TIMESTAMP '2026-01-01 00:00:00'"),
    ("p26", "ntz = TIMESTAMP_NTZ '2026-03-08 02:30:00'"),
    ("p35", "coalesce(i, 0) = 0"),
    ("p36", "CASE WHEN i = 1 THEN true END"),
)
PREDICATE_TEXT: dict[str, str] = dict(PREDICATES)

TYPED_BUILDERS: dict[str, Any] = {
    "p01": lambda f: f.col("i") == f.lit(1),
    "p02": lambda f: f.col("i") != f.lit(1),
    "p03": lambda f: f.col("i").isin(f.lit(1), f.lit(None)),
    "p04": lambda f: ~f.col("i").isin(f.lit(1), f.lit(None)),
    "p05": lambda f: f.col("i").isNull(),
    "p06": lambda f: f.col("i").between(f.lit(1), f.lit(2)),
    "p07": lambda f: f.col("i").eqNullSafe(f.lit(None)),
    "p08": lambda f: f.col("i") + f.lit(1) > f.lit(0),
    "p09": lambda f: f.col("i").cast("string") == f.lit("1"),
    "p10": lambda f: f.col("l") == f.lit(9223372036854775807),
    "p11": lambda f: f.col("dec") == f.lit(Decimal("1.10")),
    "p12": lambda f: f.col("dec") > f.lit(Decimal("1.1")),
    "p13": lambda f: f.col("d") == f.lit(float("nan")),
    "p14": lambda f: f.isnan(f.col("d")),
    "p15": lambda f: f.col("d") == f.lit(0.0),
    "p16": lambda f: f.col("d") > f.lit(0),
    "p17": lambda f: f.col("s") == f.lit(""),
    "p18": lambda f: f.col("s") == f.lit("a "),
    "p19": lambda f: f.col("s").like("a%"),
    "p20": lambda f: f.upper(f.col("s")) == f.lit("A"),
    "p21": lambda f: f.length(f.col("s")) == f.lit(0),
    "p22": lambda f: f.col("b"),
    "p23": lambda f: ~f.col("b").eqNullSafe(f.lit(True)),
    "p24": lambda f: f.col("dt") == f.to_date(f.lit("1970-01-01")),
    "p25": lambda f: f.col("ts") > f.to_timestamp(f.lit("2026-01-01 00:00:00")),
    "p26": lambda f: f.col("ntz") == f.to_timestamp_ntz(f.lit("2026-03-08 02:30:00")),
    "p35": lambda f: f.coalesce(f.col("i"), f.lit(0)) == f.lit(0),
    "p36": lambda f: f.when(f.col("i") == f.lit(1), f.lit(True)),
}


def typed_predicate(fns: Any, pid: str) -> Any:
    builder = TYPED_BUILDERS.get(pid)
    if builder is None:
        raise ValueError(f"no typed Column builder for predicate {pid!r}")
    return builder(fns)


READ_SEAT = "select"

SEED_ROWS: tuple[str, ...] = (
    "(1, 1, 1, CAST('1.10' AS DECIMAL(10,2)), CAST('1.5' AS DOUBLE), 'a', true, "
    "DATE '2026-01-01', TIMESTAMP '2026-01-01 00:00:00', "
    "TIMESTAMP_NTZ '2026-01-01 00:00:00', 'f1')",
    "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'f2')",
    "(3, NULL, NULL, NULL, CAST('NaN' AS DOUBLE), '', NULL, NULL, NULL, NULL, 'f3')",
    "(4, NULL, NULL, NULL, CAST('-0.0' AS DOUBLE), 'a ', false, NULL, NULL, NULL, 'f4')",
    "(5, 2147483647, 9223372036854775807, CAST('99999999.99' AS DECIMAL(10,2)), "
    "CAST('Infinity' AS DOUBLE), NULL, NULL, NULL, NULL, NULL, 'f5')",
    "(6, -1, NULL, NULL, CAST('0.0' AS DOUBLE), 'A', NULL, NULL, NULL, NULL, 'f6')",
    "(7, NULL, NULL, NULL, NULL, 'é', NULL, DATE '1970-01-01', TIMESTAMP '2026-03-08 02:30:00', "
    "TIMESTAMP_NTZ '2026-03-08 02:30:00', 'f7')",
    "(8, 1, 1, CAST('1.10' AS DECIMAL(10,2)), CAST('1.5' AS DOUBLE), 'a', true, "
    "DATE '2026-01-01', TIMESTAMP '2026-01-01 00:00:00', "
    "TIMESTAMP_NTZ '2026-01-01 00:00:00', 'f8')",
)
SEED_IDS: tuple[int, ...] = tuple(range(1, 9))


@dataclass(frozen=True)
class Combo:
    number: int
    door: str
    seat: str
    layout: str
    group: str
    klass: str
    committed: str | None

    @property
    def key(self) -> str:
        return f"{self.door}|{self.seat}|{self.layout}"

    @property
    def is_read(self) -> bool:
        return self.seat == READ_SEAT


COMBINATIONS: tuple[Combo, ...] = (
    Combo(1, "spark-sql", "select", "v2-cow", "J3a", "A", "spark-sql|select|v2-cow"),
    Combo(2, "spark-sql", "delete", "v2-cow", "J1", "A", "spark-sql|delete|v2-cow"),
    Combo(3, "spark-sql", "delete", "v2-mor", "J1", "A", "spark-sql|delete|v2-mor"),
    Combo(4, "spark-sql", "update", "v2-cow", "J1", "A", "spark-sql|update|v2-cow"),
    Combo(5, "spark-sql", "update", "v2-mor", "J1", "A", "spark-sql|update|v2-mor"),
    Combo(6, "spark-sql", "update-set", "v2-cow", "J1", "B", None),
    Combo(7, "spark-sql", "update-set", "v2-mor", "J1", "B", None),
    Combo(8, "spark-sql", "insert", "v2-cow", "J3a", "A", "spark-sql|insert|v2-cow"),
    Combo(9, "spark-sql", "insert", "v2-mor", "J3a", "A", "spark-sql|insert|v2-mor"),
    Combo(10, "spark-sql", "ctas", "v2-cow", "J3a", "A", "spark-sql|ctas|v2-cow"),
    Combo(11, "spark-sql", "ctas", "v2-mor", "J3a", "A", "spark-sql|ctas|v2-mor"),
    Combo(12, "spark-sql", "merge-when", "v2-cow", "J2", "A", "spark-sql|merge|v2-cow"),
    Combo(13, "spark-sql", "merge-when", "v2-mor", "J2", "A", "spark-sql|merge|v2-mor"),
    Combo(14, "spark-sql", "merge-on", "v2-cow", "J2", "B", None),
    Combo(15, "spark-sql", "merge-on", "v2-mor", "J2", "B", None),
    Combo(16, "filter-str", "select", "v2-cow", "J4a", "A", "filter-str|select|v2-cow"),
    Combo(17, "f-expr", "select", "v2-cow", "J4a", "A", "f-expr|select|v2-cow"),
    Combo(18, "select-expr", "select", "v2-cow", "J4b", "B", None),
    Combo(19, "column-api", "select", "v2-cow", "J4b", "B", None),
    Combo(20, "filter-str", "insert", "v2-cow", "J4a", "A", "filter-str|insert|v2-cow"),
    Combo(21, "f-expr", "insert", "v2-cow", "J4a", "A", "f-expr|insert|v2-cow"),
    Combo(22, "column-api", "insert", "v2-cow", "J4b", "B", None),
    Combo(23, "column-api", "append", "v2-cow", "J4b", "B", None),
    Combo(24, "f-expr", "overwrite", "v2-cow", "J3b", "B", None),
    Combo(25, "f-expr", "overwrite", "v2-mor", "J3b", "B", None),
    Combo(26, "f-expr", "merge-into-on", "v2-cow", "J5a", "B", None),
    Combo(27, "f-expr", "merge-into-on", "v2-mor", "J5a", "B", None),
    Combo(28, "f-expr", "merge-into-matched", "v2-cow", "J5a", "B", None),
    Combo(29, "f-expr", "merge-into-matched", "v2-mor", "J5a", "B", None),
    Combo(30, "column-api", "create-or-replace", "v2-cow", "J5b", "B", None),
)
COMBO_BY_NUMBER: dict[int, Combo] = {combo.number: combo for combo in COMBINATIONS}

FLOAT_SHAPES: tuple[str, ...] = (
    "K1-group-by",
    "K2-distinct",
    "K3-count-distinct",
    "K4-inner-join",
    "K5-left-semi",
    "K6-left-anti",
    "K7-full-outer",
    "K8-window-partition",
    "K9-intersect",
    "K10-except",
    "K11-union-distinct",
    "K12-in-subquery",
    "W1-merge-on",
    "W2-delete-in",
)
FLOAT_TYPES: tuple[str, ...] = ("DOUBLE", "FLOAT")
FLOAT_WRITE_SHAPES: frozenset[str] = frozenset({"W1-merge-on", "W2-delete-in"})

FLOAT_SQL: dict[str, str] = {
    "K1-group-by": "SELECT d, count(*) AS n FROM {t} GROUP BY d",
    "K2-distinct": "SELECT DISTINCT d FROM {t}",
    "K3-count-distinct": "SELECT count(DISTINCT d) AS n FROM {t}",
    "K4-inner-join": "SELECT t.id, u.id FROM {t} AS t JOIN {u} AS u ON t.d = u.d",
    "K5-left-semi": "SELECT t.id FROM {t} AS t LEFT SEMI JOIN {u} AS u ON t.d = u.d",
    "K6-left-anti": "SELECT t.id FROM {t} AS t LEFT ANTI JOIN {u} AS u ON t.d = u.d",
    "K7-full-outer": "SELECT t.id, u.id FROM {t} AS t FULL OUTER JOIN {u} AS u ON t.d = u.d",
    "K8-window-partition": ("SELECT id, rank() OVER (PARTITION BY d ORDER BY id) AS r FROM {t}"),
    "K9-intersect": "SELECT d FROM {t} INTERSECT SELECT d FROM {u}",
    "K10-except": "SELECT d FROM {t} EXCEPT SELECT d FROM {u}",
    "K11-union-distinct": "SELECT d FROM {t} UNION SELECT d FROM {u}",
    "K12-in-subquery": "SELECT id FROM {t} WHERE d IN (SELECT d FROM {u})",
    "W1-merge-on": (
        "MERGE INTO {t} AS t USING {u} AS s ON t.d = s.d WHEN MATCHED THEN UPDATE SET flag = 'z'"
    ),
    "W2-delete-in": "DELETE FROM {t} WHERE d IN (SELECT d FROM {u})",
}

NEGATIVE_ZERO = "CAST('-0.0' AS DOUBLE)"
FLOAT_SEED_T: tuple[str, ...] = (
    "(1, CAST('0.0' AS DOUBLE), NULL)",
    f"(2, {NEGATIVE_ZERO}, NULL)",
    "(3, CAST('1.5' AS DOUBLE), NULL)",
    "(4, CAST('0.0' AS DOUBLE), NULL)",
)
FLOAT_SEED_U: tuple[str, ...] = (f"(10, {NEGATIVE_ZERO}, NULL)",)

GROUP_NAMES: tuple[str, ...] = (
    "pilot",
    "floatkeys",
    "p15",
    "J1",
    "J2",
    "J3a",
    "J3b",
    "J4a",
    "J4b",
    "J5a",
    "J5b",
)
ALL_GROUPS = "all"

DECLARED_STAGE_CREATE = "DECLARED-STAGE-CREATE"
PASSING_LABELS: frozenset[str] = frozenset(
    {
        "EQUAL",
        "BOTH-REFUSED",
        "REFUSED-WHERE-SPARK-ACCEPTED",
        "KNOWN-EMPTY-DML",
        DECLARED_STAGE_CREATE,
    }
)
DML_SEATS: frozenset[str] = frozenset(
    {
        "delete",
        "update",
        "update-set",
        "insert",
        "ctas",
        "merge-when",
        "merge-on",
        "overwrite",
        "append",
        "merge-into-on",
        "merge-into-matched",
        "create-or-replace",
    }
)


@dataclass(frozen=True)
class Cell:
    cell_id: str
    group_tags: tuple[str, ...]
    combo: Combo | None
    pid: str | None
    shape: str | None
    float_type: str | None

    @property
    def is_float(self) -> bool:
        return self.shape is not None


def matrix_cells() -> tuple[Cell, ...]:
    cells: list[Cell] = []
    for combo in COMBINATIONS:
        for pid, _ in PREDICATES:
            tags = [combo.group]
            if pid == "p01":
                tags.append("pilot")
            if pid == "p15":
                tags.append("p15")
            cells.append(Cell(f"{combo.key}|{pid}", tuple(tags), combo, pid, None, None))
    return tuple(cells)


def float_cells() -> tuple[Cell, ...]:
    cells: list[Cell] = []
    for shape in FLOAT_SHAPES:
        for float_type in FLOAT_TYPES:
            cell_id = f"float|{shape}|{float_type.lower()}"
            cells.append(Cell(cell_id, ("floatkeys",), None, None, shape, float_type))
    return tuple(cells)


def universe() -> tuple[Cell, ...]:
    return matrix_cells() + float_cells()


def cells_for_group(group: str) -> tuple[Cell, ...]:
    if group == ALL_GROUPS:
        return universe()
    if group not in GROUP_NAMES:
        raise ValueError(f"unknown REPARK_FLAT_GROUP {group!r}")
    return tuple(cell for cell in universe() if group in cell.group_tags)


def known_open_unit(cell: Cell) -> str | None:
    return None


KNOWN_OPEN: dict[str, str] = {
    cell.cell_id: unit for cell in universe() if (unit := known_open_unit(cell)) is not None
}


def class_a_key(cell: Cell) -> str | None:
    if cell.combo is None or cell.combo.committed is None or cell.pid is None:
        return None
    return f"{cell.combo.committed}|{cell.pid}"


def load_expected(path: Path = EXPECTED_PATH) -> dict[str, dict[str, Any]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    return dict(document["cells"])


def expected_for(cell: Cell, table: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    key = class_a_key(cell)
    if key is None:
        return None
    return table[key]


def target_for(combo: Combo) -> str:
    if combo.seat in {"insert", "append"}:
        return "t2"
    if combo.seat in {"ctas", "create-or-replace"}:
        return "t3"
    return "t"


def committed_ids(combo: Combo, entry: dict[str, Any]) -> list[int]:
    if combo.is_read or target_for(combo) != "t":
        return [int(value) for value in entry["ids"] or []]
    return [int(value) for value in entry["after_ids"] or []]


def committed_commits(combo: Combo, entry: dict[str, Any]) -> int | None:
    if target_for(combo) != "t":
        return None
    return entry["commits"]


def answer_rows(ids: list[int] | None) -> list[tuple[int, ...]] | None:
    if ids is None:
        return None
    return sorted((int(value),) for value in ids)


TRUTH_PREDICATES: frozenset[str] = frozenset({"p15"})
PRUNED_WRONG = "PRUNED-WRONG"
ORACLE_AGREES_WITH_TRUTH = "EQUAL"
SEED_DOUBLE = re.compile(r"CAST\('([^']*)' AS DOUBLE\)")
COPY_SEATS: frozenset[str] = frozenset({"select", "insert", "append", "ctas", "create-or-replace"})
ROW_UPDATE_SEATS: frozenset[str] = frozenset(
    {"update", "update-set", "merge-when", "merge-on", "merge-into-on", "merge-into-matched"}
)


def seed_doubles() -> dict[int, float | None]:
    values: dict[int, float | None] = {}
    for row_id, row in zip(SEED_IDS, SEED_ROWS, strict=True):
        found = SEED_DOUBLE.findall(row)
        if len(found) > 1:
            raise ValueError(f"seed row {row_id} carries more than one DOUBLE: {row!r}")
        values[row_id] = float(found[0]) if found else None
    return values


def truth_match_ids(pid: str) -> list[int]:
    if pid not in TRUTH_PREDICATES:
        raise ValueError(f"no truth column for predicate {pid!r}")
    return [row_id for row_id, d in seed_doubles().items() if d is not None and d == 0.0]


def truth_ids(cell: Cell) -> list[int] | None:
    if cell.combo is None or cell.pid not in TRUTH_PREDICATES:
        return None
    seat = cell.combo.seat
    matched = truth_match_ids(cell.pid)
    kept = [row_id for row_id in SEED_IDS if row_id not in matched]
    if seat in COPY_SEATS:
        return matched
    if seat == "delete":
        return kept
    if seat in ROW_UPDATE_SEATS:
        return list(SEED_IDS)
    if seat == "overwrite":
        return sorted(kept + list(SEED_IDS))
    raise ValueError(f"no truth rule for seat {seat!r}")


def oracle_truth_label(oracle_ids: list[int] | None, truth: list[int]) -> str:
    if oracle_ids is not None and sorted(oracle_ids) == sorted(truth):
        return ORACLE_AGREES_WITH_TRUTH
    return PRUNED_WRONG


def grade_against_truth(
    cell: Cell, oracle_outcome: str, oracle_ids: list[int] | None
) -> tuple[list[int] | None, str | None]:
    truth = truth_ids(cell)
    if truth is None or oracle_outcome != "ANSWERED":
        return None, None
    return truth, oracle_truth_label(oracle_ids, truth)


KNOWN_EMPTY_DML = "KNOWN-EMPTY-DML"
FOLDED_PREDICATES: frozenset[str] = frozenset({"p04"})
FINDING_LABELS: frozenset[str] = frozenset(
    {"WRONG-WRITE", "EXTRA-COMMIT", "FEWER-COMMIT", "LINEAGE-DIFFER"}
)
UNANSWERED_ORACLE_LABEL = "ORACLE-MISSING"


def _answer_labels(
    *,
    seat: str,
    repark_answer: list[Any] | None,
    spark_answer: list[Any] | None,
    repark_damaged: list[Any] | None,
    spark_damaged: list[Any] | None,
    repark_lost: list[Any] | None,
    spark_lost: list[Any] | None,
    repark_own: list[Any] | None,
) -> list[str]:
    if seat == READ_SEAT or seat.startswith("K"):
        return ["WRONG-READ"] if repark_answer != spark_answer else []
    differs = (
        repark_answer != spark_answer
        or repark_damaged != spark_damaged
        or repark_lost != spark_lost
    )
    own_differs = repark_own is not None and repark_own != repark_answer
    return ["WRONG-WRITE"] if differs or own_differs else []


def _commit_labels(
    *,
    seat: str,
    repark_commits: int | None,
    spark_commits: int | None,
    repark_target_commits: int | None,
    spark_target_commits: int | None,
) -> list[str]:
    pairs = [
        (mine, theirs)
        for mine, theirs in (
            (repark_commits, spark_commits),
            (repark_target_commits, spark_target_commits),
        )
        if mine is not None and theirs is not None
    ]
    labels: list[str] = []
    if any(mine > theirs for mine, theirs in pairs) or (
        seat == READ_SEAT and (repark_commits or 0) != 0
    ):
        labels.append("EXTRA-COMMIT")
    if any(mine < theirs for mine, theirs in pairs):
        labels.append("FEWER-COMMIT")
    return labels


def classify(
    *,
    seat: str,
    pid: str | None,
    repark_outcome: str,
    spark_outcome: str,
    repark_answer: list[Any] | None,
    spark_answer: list[Any] | None,
    repark_commits: int | None = None,
    spark_commits: int | None = None,
    repark_target_commits: int | None = None,
    spark_target_commits: int | None = None,
    repark_damaged: list[Any] | None = None,
    spark_damaged: list[Any] | None = None,
    repark_lost: list[Any] | None = None,
    spark_lost: list[Any] | None = None,
    repark_own: list[Any] | None = None,
    repark_lineage: list[Any] | None = None,
    spark_lineage: list[Any] | None = None,
    folded: bool = False,
) -> str:
    if repark_outcome == "PANIC":
        return "PANIC"
    if repark_outcome in ("SETUP-REFUSED", "NOT-EXPRESSIBLE"):
        return repark_outcome
    if spark_outcome not in ("ANSWERED", "REFUSED"):
        return UNANSWERED_ORACLE_LABEL
    if repark_outcome == "REFUSED" and spark_outcome == "REFUSED":
        return "BOTH-REFUSED"
    if repark_outcome == "REFUSED":
        return "REFUSED-WHERE-SPARK-ACCEPTED"
    if spark_outcome == "REFUSED":
        return "ACCEPTED-WHERE-SPARK-REFUSED"
    labels = _answer_labels(
        seat=seat,
        repark_answer=repark_answer,
        spark_answer=spark_answer,
        repark_damaged=repark_damaged,
        spark_damaged=spark_damaged,
        repark_lost=repark_lost,
        spark_lost=spark_lost,
        repark_own=repark_own,
    )
    if not labels and repark_lineage != spark_lineage:
        labels.append("LINEAGE-DIFFER")
    labels.extend(
        _commit_labels(
            seat=seat,
            repark_commits=repark_commits,
            spark_commits=spark_commits,
            repark_target_commits=repark_target_commits,
            spark_target_commits=spark_target_commits,
        )
    )
    if folded and seat in DML_SEATS and FINDING_LABELS & set(labels):
        labels.insert(0, KNOWN_EMPTY_DML)
    return ", ".join(labels) if labels else "EQUAL"


def passes(label: str) -> bool:
    parts = set(label.split(", "))
    if parts <= PASSING_LABELS:
        return True
    return KNOWN_EMPTY_DML in parts and parts <= PASSING_LABELS | {"FEWER-COMMIT"}


def record_line(**fields: Any) -> str:
    return json.dumps(fields, sort_keys=True, default=str)


COLUMNS: tuple[str, ...] = ("id", "i", "l", "dec", "d", "s", "b", "dt", "ts", "ntz", "flag")
CANONICAL_EXPRESSIONS: tuple[str, ...] = tuple(
    f"CAST({name} AS STRING) AS {name}" for name in COLUMNS
)
NULL_TEXT = "\\N"
SEED_CANONICAL_TEXT: tuple[str, ...] = (
    "1|1|1|1.10|1.5|a|true|2026-01-01|2026-01-01 00:00:00|2026-01-01 00:00:00|f1",
    "2|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|f2",
    "3|\\N|\\N|\\N|NaN||\\N|\\N|\\N|\\N|f3",
    "4|\\N|\\N|\\N|-0.0|a |false|\\N|\\N|\\N|f4",
    "5|2147483647|9223372036854775807|99999999.99|Infinity|\\N|\\N|\\N|\\N|\\N|f5",
    "6|-1|\\N|\\N|0.0|A|\\N|\\N|\\N|\\N|f6",
    "7|\\N|\\N|\\N|\\N|é|\\N|1970-01-01|2026-03-08 02:30:00|2026-03-08 02:30:00|f7",
    "8|1|1|1.10|1.5|a|true|2026-01-01|2026-01-01 00:00:00|2026-01-01 00:00:00|f8",
)
SEED_CANONICAL: tuple[tuple[str | None, ...], ...] = tuple(
    tuple(None if value == NULL_TEXT else value for value in line.split("|"))
    for line in SEED_CANONICAL_TEXT
)
DDL_TYPE_TEXT = "bigint int bigint decimal(10,2) double string boolean date timestamp"
DDL_TYPES: tuple[str, ...] = (*DDL_TYPE_TEXT.split(), "timestamp_ntz", "string")
DDL_SCHEMA: tuple[tuple[str, str, bool], ...] = tuple(
    (name, kind, name != "id") for name, kind in zip(COLUMNS, DDL_TYPES, strict=True)
)
CTAS_SCHEMA: tuple[tuple[str, str, bool], ...] = tuple(
    (name, kind, True) for name, kind in zip(COLUMNS, DDL_TYPES, strict=True)
)
UPDATED_FLAG = "z"
ABSENT = "ABSENT"
OBSERVATION_MISSING = "OBSERVATION-MISSING"
STAGE_CREATE_ANSWERED = "STAGE-CREATE-ANSWERED"
STAGE_CREATE_DOOR = "column-api"
STAGE_CREATE_SEAT = "create-or-replace"
STAGE_CREATE_CATALOG = "s3tables"
STAGE_CREATE_MESSAGE = "Stage-create is currently not supported"
STAGE_CREATE_ERROR_CLASS = "BadRequestException"
STAGE_CREATE_STATUS_RULE = "class and message required; HTTP status recorded, not required"
HTTP_STATUS = re.compile(
    r"\b(?:HTTP|status(?: code)?|code)[\s:=\"']*([1-5][0-9][0-9])\b", re.IGNORECASE
)


@dataclass(frozen=True)
class Observation:
    outcome: str
    answer: list[tuple[Any, ...]] | None = None
    state: Any = None
    schema: tuple[tuple[str, str, bool], ...] | None = None
    commits: int | None = None
    error: str | None = None
    error_class: str | None = None
    status: int | None = None
    read_error: str | None = None
    probe_error: str | None = None


@dataclass(frozen=True)
class Grade:
    label: str
    expected: Observation
    truth: list[int] | None
    oracle_truth: str | None
    commits_graded: bool
    missing: tuple[str, ...]
    stage_create: dict[str, Any] | None = field(default=None)


def sorted_rows(rows: Any) -> list[tuple[Any, ...]]:
    return sorted((tuple(row) for row in rows), key=repr)


def canonical_rows(rows: Any) -> list[tuple[str | None, ...]]:
    return sorted_rows(
        tuple(None if value is None else str(value) for value in row) for row in rows
    )


def http_status(text: str | None) -> int | None:
    found = HTTP_STATUS.search(text or "")
    return int(found.group(1)) if found else None


def commits_graded(catalog: str) -> bool:
    return catalog == "glue"


def stage_create_status_holds(status: int | None) -> bool:
    return True


def is_stage_create_refusal(observation: Observation) -> bool:
    return (
        observation.outcome == "REFUSED"
        and STAGE_CREATE_ERROR_CLASS in (observation.error_class or "")
        and STAGE_CREATE_MESSAGE in (observation.error or "")
        and stage_create_status_holds(observation.status)
    )


def is_stage_create_cell(cell: Cell) -> bool:
    combo = cell.combo
    return combo is not None and (combo.door, combo.seat) == (STAGE_CREATE_DOOR, STAGE_CREATE_SEAT)


def state_for(seat: str, matched: list[int]) -> list[tuple[str | None, ...]]:
    chosen = {int(value) for value in matched}
    seed = list(SEED_CANONICAL)
    if seat == READ_SEAT:
        return sorted_rows(seed)
    if seat in COPY_SEATS:
        return sorted_rows(row for row in seed if int(str(row[0])) in chosen)
    if seat == "delete":
        return sorted_rows(row for row in seed if int(str(row[0])) not in chosen)
    if seat in ROW_UPDATE_SEATS:
        return sorted_rows(
            (*row[:-1], UPDATED_FLAG) if int(str(row[0])) in chosen else row for row in seed
        )
    if seat == "overwrite":
        return sorted_rows([row for row in seed if int(str(row[0])) not in chosen] + seed)
    raise ValueError(f"no state rule for seat {seat!r}")


def unchanged_state(combo: Combo) -> Any:
    target = target_for(combo)
    if target == "t":
        return sorted_rows(SEED_CANONICAL)
    return [] if target == "t2" else ABSENT


def committed_schema(combo: Combo) -> tuple[tuple[str, str, bool], ...]:
    return CTAS_SCHEMA if target_for(combo) == "t3" else DDL_SCHEMA


def committed_observation(cell: Cell, entry: dict[str, Any]) -> Observation:
    combo = cell.combo
    if combo is None:
        raise ValueError(f"cell {cell.cell_id!r} has no combination")
    answered = entry["outcome"] == "ANSWERED"
    error = entry.get("error") or {}
    return Observation(
        outcome=entry["outcome"],
        answer=answer_rows(committed_ids(combo, entry)) if answered else None,
        state=state_for(combo.seat, entry["ids"] or []) if answered else unchanged_state(combo),
        schema=committed_schema(combo) if answered else None,
        commits=committed_commits(combo, entry),
        error=error.get("message"),
        error_class=error.get("class"),
    )


def _with_truth(
    cell: Cell, oracle: Observation
) -> tuple[Observation, list[int] | None, str | None]:
    ids = None if oracle.answer is None else [int(row[0]) for row in oracle.answer]
    truth, oracle_truth = grade_against_truth(cell, oracle.outcome, ids)
    if truth is None or cell.combo is None or cell.pid is None:
        return oracle, None, None
    state = state_for(cell.combo.seat, truth_match_ids(cell.pid))
    graded = replace(oracle, answer=sorted_rows((value,) for value in truth), state=state)
    return graded, truth, oracle_truth


def _missing(
    repark: Observation, oracle: Observation, *, live_oracle: bool, graded: bool
) -> tuple[str, ...]:
    missing: list[str] = []
    acted = repark.outcome in ("ANSWERED", "REFUSED")
    if acted and repark.state is None:
        missing.append("repark-state")
    if live_oracle and oracle.outcome == "ANSWERED" and oracle.state is None:
        missing.append("oracle-state")
    if graded and acted and repark.commits is None:
        missing.append("repark-commits")
    if graded and live_oracle and oracle.outcome == "ANSWERED" and oracle.commits is None:
        missing.append("oracle-commits")
    return tuple(missing)


def _state_differs(repark: Observation, expected: Any, schema: Any) -> bool:
    if repark.state is None or expected is None:
        return False
    if repark.state != expected:
        return True
    return (
        schema is not None and repark.schema is not None and tuple(repark.schema) != tuple(schema)
    )


def _stage_create(
    cell: Cell, catalog: str, repark: Observation, oracle: Observation, reference: Any
) -> dict[str, Any] | None:
    if not is_stage_create_cell(cell) or catalog != STAGE_CREATE_CATALOG:
        return None
    if cell.pid in TRUTH_PREDICATES and cell.pid is not None:
        reference = state_for(STAGE_CREATE_SEAT, truth_match_ids(cell.pid))
    exact = is_stage_create_refusal(oracle)
    readback = repark.state is not None and reference is not None and repark.state == reference
    return {
        "exact_refusal": exact,
        "status": oracle.status,
        "status_rule": STAGE_CREATE_STATUS_RULE,
        "reference_state": reference,
        "readback_equal": readback,
        "declared": exact and readback and repark.outcome == "ANSWERED",
    }


def grade_cell(
    cell: Cell,
    catalog: str,
    repark: Observation,
    oracle: Observation,
    *,
    live_oracle: bool,
    reference_state: Any = None,
) -> Grade:
    combo = cell.combo
    if combo is None:
        raise ValueError(f"cell {cell.cell_id!r} has no combination")
    graded_oracle, truth, oracle_truth = _with_truth(cell, oracle)
    graded = commits_graded(catalog)
    label = classify(
        seat=combo.seat,
        pid=cell.pid,
        repark_outcome=repark.outcome,
        spark_outcome=graded_oracle.outcome,
        repark_answer=repark.answer,
        spark_answer=graded_oracle.answer,
        repark_commits=repark.commits if graded else None,
        spark_commits=graded_oracle.commits if graded else None,
        folded=cell.pid in FOLDED_PREDICATES,
    )
    labels = [] if label == "EQUAL" else label.split(", ")
    missing = _missing(repark, graded_oracle, live_oracle=live_oracle, graded=graded)
    stage = _stage_create(cell, catalog, repark, graded_oracle, reference_state)
    expected_state: Any = None
    expected_schema: Any = None
    if repark.outcome == "REFUSED":
        expected_state = unchanged_state(combo)
        if graded and repark.commits:
            labels.append("EXTRA-COMMIT")
    elif graded_oracle.outcome == "ANSWERED":
        expected_state, expected_schema = graded_oracle.state, graded_oracle.schema
    elif stage is not None:
        expected_state = stage["reference_state"]
        expected_schema = committed_schema(combo)
    if repark.outcome in ("ANSWERED", "REFUSED") and _state_differs(
        repark, expected_state, expected_schema
    ):
        labels.append("WRONG-WRITE")
    if missing:
        labels.append(OBSERVATION_MISSING)
    if stage is not None and graded_oracle.outcome == "ANSWERED":
        labels.append(STAGE_CREATE_ANSWERED)
    if stage is not None and stage["declared"] and labels == ["ACCEPTED-WHERE-SPARK-REFUSED"]:
        labels = [DECLARED_STAGE_CREATE]
    unique = list(dict.fromkeys(labels))
    expected = replace(graded_oracle, state=expected_state, schema=expected_schema)
    return Grade(
        label=", ".join(unique) if unique else "EQUAL",
        expected=expected,
        truth=truth,
        oracle_truth=oracle_truth,
        commits_graded=graded,
        missing=missing,
        stage_create=stage,
    )
