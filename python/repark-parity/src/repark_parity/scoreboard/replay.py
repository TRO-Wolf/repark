from __future__ import annotations

import json
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from repark_parity.scoreboard.classify import diff_files
from repark_parity.scoreboard.corpus import select_cells, stage
from repark_parity.scoreboard.launch import (
    BATCH_SIZE,
    check_batch,
    require_engine_settings,
    run_engine,
    run_half,
    split_batches,
)
from repark_parity.scoreboard.settings import Settings

LOGGER = logging.getLogger("repark_parity.scoreboard")
REPLAY_RECORD = "replay.json"


def check_destination(destination: Path, settings: Settings) -> Path:
    """Return the resolved destination: a new path inside the output root."""
    root = Path(settings.require("output_root")).resolve()
    target = destination.resolve()
    if root not in target.parents:
        raise ValueError(f"destination must be inside the output root {root}")
    if target.exists():
        raise FileExistsError(f"choose a new destination: {target}")
    return target


def check_package(settings: Settings) -> Path:
    """Return the package directory after checking it holds a RePark extension."""
    package = Path(settings.require("package")).resolve()
    if not list((package / "repark").glob("_native*.so")):
        raise FileNotFoundError(f"missing RePark extension under {package}")
    return package


def _run_lane_batch(task: tuple[Path, Path, list, Settings]) -> str:
    path, folder, ids, settings = task
    result = path.with_name(path.name.replace(".cases.", ".answers."))
    with (folder / "repark.log").open("a") as log:
        run_engine("repark", path, result, settings, log)
    return check_batch(result, ids)


def replay_halves(folders: list[Path], settings: Settings, jobs: int, batch_size: int) -> None:
    """Run the RePark half of every staged lane, batch by batch on a pool of processes."""
    tasks = []
    for folder in folders:
        work = folder / "batches"
        work.mkdir()
        for path, ids in split_batches(folder / "cases.jsonl", work, batch_size):
            tasks.append((path, folder, ids, settings))
    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        texts = list(pool.map(_run_lane_batch, tasks))
    by_folder: dict[Path, list[str]] = {}
    for task, text in zip(tasks, texts, strict=True):
        by_folder.setdefault(task[1], []).append(text)
    for folder in folders:
        answers = folder / "repark.jsonl.incomplete"
        answers.write_text("".join(by_folder.get(folder, [])))
        answers.replace(folder / "repark.jsonl")
        shutil.rmtree(folder / "batches")
        diff_files(
            folder / "cases.jsonl",
            folder / "spark.jsonl",
            folder / "repark.jsonl",
            folder / "diff.jsonl",
        )
        LOGGER.info("replayed %s", folder.name)


def replay(
    settings: Settings,
    destination: Path,
    lanes: list[str] | None = None,
    cells: list[str] | None = None,
    jobs: int = 1,
    batch_size: int = BATCH_SIZE,
) -> Path:
    """Replay the RePark half against the recorded Spark answers in a new destination."""
    require_engine_settings("repark", settings)
    package = check_package(settings)
    target = check_destination(destination, settings)
    folders = stage(target, select_cells(lanes, cells))
    built_from = package / "BUILT_FROM"
    record = {
        "selection": "cells" if cells is not None else "lanes" if lanes else "comparable",
        "lanes": [folder.name for folder in folders],
        "built_from": built_from.read_text().strip() if built_from.exists() else None,
    }
    (target / REPLAY_RECORD).write_text(json.dumps(record, indent=1, sort_keys=True) + "\n")
    replay_halves(folders, settings, jobs, batch_size)
    return target


def record_spark(settings: Settings, cases: Path, destination: Path, with_repark: bool) -> Path:
    """Record fresh Spark answers for a case file in a new destination, and optionally pair them."""
    require_engine_settings("spark", settings)
    if with_repark:
        require_engine_settings("repark", settings)
    target = check_destination(destination, settings)
    if with_repark:
        check_package(settings)
    target.mkdir(parents=True)
    shutil.copyfile(cases, target / "cases.jsonl")
    run_half("spark", target / "cases.jsonl", target / "spark.jsonl", settings)
    if with_repark:
        run_half("repark", target / "cases.jsonl", target / "repark.jsonl", settings)
        diff_files(
            target / "cases.jsonl",
            target / "spark.jsonl",
            target / "repark.jsonl",
            target / "diff.jsonl",
        )
    return target
