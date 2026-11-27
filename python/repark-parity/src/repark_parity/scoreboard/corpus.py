from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from repark_parity.scoreboard.classify import check_answers
from repark_parity.scoreboard.encoding import read_records, write_lines

DATA_DIR = Path(__file__).resolve().parent / "data"
CELLS = "cells"
CASES_SUFFIX = ".cases.jsonl.gz"
SPARK_SUFFIX = ".spark.jsonl.gz"
EXCLUSIONS = "exclusions.jsonl.gz"
SPARK_VERSION = "4.1.2"
PROVIDER_LANE = "local-statements"


class CorpusError(ValueError):
    """The stored corpus or a lane offered to it breaks a corpus rule."""


def lane_names(data_dir: Path = DATA_DIR) -> list[str]:
    """Return the stored lanes in name order."""
    return sorted(
        path.name.removesuffix(CASES_SUFFIX) for path in (data_dir / CELLS).glob("*" + CASES_SUFFIX)
    )


def load_lane(lane: str, data_dir: Path = DATA_DIR) -> tuple[dict, dict]:
    """Return one lane's cells and recorded Spark answers, keyed by id."""
    folder = data_dir / CELLS
    return read_records(folder / (lane + CASES_SUFFIX)), read_records(
        folder / (lane + SPARK_SUFFIX)
    )


def load_exclusions(data_dir: Path = DATA_DIR) -> dict[str, str]:
    """Return the frozen adjudicated exclusions as id to reason."""
    return {key: row["reason"] for key, row in read_records(data_dir / EXCLUSIONS).items()}


def is_provider_probe(lane: str, key: str) -> bool:
    """Tell whether a cell is a table probe bound to different default catalog providers."""
    return lane == PROVIDER_LANE and not key.startswith("plain.temp.")


def check_lane(lane: str, cases: dict, spark: dict) -> None:
    """Fail unless a lane's answers match its cells by id, digest, engine and Spark version."""
    if cases.keys() != spark.keys():
        raise CorpusError(f"{lane}: cell and Spark answer id sets differ")
    check_answers(cases, spark, "spark")
    versions = {answer["version"] for answer in spark.values()}
    if versions - {SPARK_VERSION}:
        raise CorpusError(f"{lane}: Spark answers from {sorted(versions)}, not {SPARK_VERSION}")


def check_corpus(data_dir: Path = DATA_DIR) -> dict[str, int]:
    """Check every stored lane and the exclusions; return the cell count per lane."""
    counts: dict[str, int] = {}
    seen: dict[str, str] = {}
    for lane in lane_names(data_dir):
        cases, spark = load_lane(lane, data_dir)
        check_lane(lane, cases, spark)
        for key in cases:
            if key in seen:
                raise CorpusError(f"{key} is stored in both {seen[key]} and {lane}")
            seen[key] = lane
        counts[lane] = len(cases)
    unknown = sorted(set(load_exclusions(data_dir)) - set(seen))
    if unknown:
        raise CorpusError(f"exclusions name cells the corpus does not hold: {unknown[:5]}")
    return counts


def select_cells(
    lanes: Iterable[str] | None = None,
    cells: Iterable[str] | None = None,
    data_dir: Path = DATA_DIR,
) -> dict[str, list[tuple[dict, dict]]]:
    """Choose cells: every comparable cell by default, whole lanes, or listed ids."""
    exclusions = load_exclusions(data_dir)
    wanted_lanes = set(lanes) if lanes else None
    wanted_cells = set(cells) if cells else None
    chosen: dict[str, list[tuple[dict, dict]]] = {}
    for lane in lane_names(data_dir):
        if wanted_lanes is not None and lane not in wanted_lanes:
            continue
        cases, spark = load_lane(lane, data_dir)
        for key, cell in cases.items():
            if wanted_cells is not None:
                keep = key in wanted_cells
            elif wanted_lanes is not None:
                keep = not is_provider_probe(lane, key)
            else:
                keep = key not in exclusions
            if keep:
                chosen.setdefault(lane, []).append((cell, spark[key]))
    missing = sorted((wanted_lanes or set()) - set(lane_names(data_dir)))
    if missing:
        raise CorpusError(f"no stored lane named {missing}")
    return chosen


def stage(destination: Path, chosen: dict[str, list[tuple[dict, dict]]]) -> list[Path]:
    """Write each chosen lane's cells and Spark answers into a new replay directory."""
    if destination.exists():
        raise FileExistsError(f"choose a new replay destination: {destination}")
    destination.mkdir(parents=True)
    folders = []
    for lane, pairs in sorted(chosen.items()):
        folder = destination / lane
        folder.mkdir()
        write_lines(folder / "cases.jsonl", (cell for cell, _ in pairs))
        write_lines(folder / "spark.jsonl", (answer for _, answer in pairs))
        folders.append(folder)
    return folders


def ingest(
    lane: str, cases_path: Path, spark_path: Path, replace: bool, data_dir: Path = DATA_DIR
) -> int:
    """Store a lane recorded live; refuse to overwrite a stored lane unless replace is set."""
    cases, spark = read_records(cases_path), read_records(spark_path)
    check_lane(lane, cases, spark)
    folder = data_dir / CELLS
    target = folder / (lane + CASES_SUFFIX)
    if target.exists() and not replace:
        raise FileExistsError(f"lane {lane} is stored; re-record it whole and pass --replace")
    for other in lane_names(data_dir):
        if other == lane:
            continue
        clash = sorted(set(cases) & set(load_lane(other, data_dir)[0]))
        if clash:
            raise CorpusError(f"{lane} reuses ids stored in {other}: {clash[:5]}")
    write_lines(target, cases.values())
    write_lines(folder / (lane + SPARK_SUFFIX), spark.values())
    return len(cases)
