from __future__ import annotations

import contextlib
import fcntl
import json
import os
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import IO

from repark_parity.scoreboard.encoding import read_records
from repark_parity.scoreboard.settings import Settings

HERE = Path(__file__).resolve().parent
PARITY_SOURCE = HERE.parents[1]
CELL_PATH = HERE / "cellpath"
DRIVER_MEMORY = "2g"
NICE_LEVEL = "19"
BATCH_SIZE = 192
MEMINFO = Path(os.sep, "proc", "meminfo")
THREAD_LIMITS = {
    "OPENBLAS_NUM_THREADS": "1",
    "OMP_NUM_THREADS": "1",
    "RAYON_NUM_THREADS": "2",
    "TOKIO_WORKER_THREADS": "2",
}


class EngineRunError(RuntimeError):
    """An engine process failed, or its answer file does not match its case file."""


def available_mib() -> int:
    """Return the kernel's MemAvailable in MiB."""
    for raw in MEMINFO.read_text().splitlines():
        if raw.startswith("MemAvailable:"):
            return int(raw.split()[1]) // 1024
    raise EngineRunError("MemAvailable is missing from the kernel memory report")


def check_memory(settings: Settings) -> None:
    """Refuse to start a batch while available memory is below the floor."""
    floor = int(settings.require("memory_floor_mib"))
    available = available_mib()
    if available < floor:
        raise EngineRunError(f"insufficient available memory: {available} MiB < {floor} MiB")


@contextlib.contextmanager
def spark_slot(settings: Settings) -> Iterator[None]:
    """Hold one of the Spark driver slot locks under the output root for one Spark process."""
    drivers = int(settings.require("spark_drivers"))
    if drivers < 1:
        raise ValueError("spark drivers must be at least 1")
    locks = Path(settings.require("output_root")) / "locks"
    locks.mkdir(parents=True, exist_ok=True)
    handle = None
    for slot in range(drivers):
        candidate = (locks / f"spark-{slot}").open("w")
        try:
            fcntl.flock(candidate, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            candidate.close()
            continue
        handle = candidate
        break
    if handle is None:
        handle = (locks / "spark-0").open("w")
        fcntl.flock(handle, fcntl.LOCK_EX)
    try:
        yield
    finally:
        handle.close()


def engine_python(engine: str, settings: Settings) -> Path:
    """Return the interpreter of one engine half."""
    if engine == "spark":
        return Path(settings.require("spark_python"))
    chosen = settings.repark_python or settings.spark_python
    return Path(chosen) if chosen else Path(settings.require("repark_python"))


ENGINE_SETTINGS = {
    "repark": ("cpus", "memory_floor_mib", "output_root", "package"),
    "spark": (
        "cpus",
        "memory_floor_mib",
        "output_root",
        "spark_python",
        "java_home",
        "spark_drivers",
    ),
}


def require_engine_settings(engine: str, settings: Settings) -> None:
    """Fail before any work unless every setting one engine's runs read is given."""
    for name in ENGINE_SETTINGS[engine]:
        settings.require(name)
    engine_python(engine, settings)


def import_path(engine: str, settings: Settings) -> str:
    """Return the engine's PYTHONPATH: package and dependencies first for RePark."""
    entries = []
    if engine == "repark":
        entries.append(str(Path(settings.require("package")).resolve()))
        if settings.dependency_path:
            entries.extend(settings.dependency_path.split(os.pathsep))
    entries.extend([str(PARITY_SOURCE), str(CELL_PATH)])
    return os.pathsep.join(entries)


def engine_env(engine: str, settings: Settings, run: Path) -> dict[str, str]:
    """Build the engine environment: private temp, home and Spark dirs, UTC, few threads."""
    env = {key: value for key, value in os.environ.items() if key not in {"SPARK_HOME"}}
    tmp = run / "tmp"
    home = run / "home"
    java_options = [
        "-XX:-UsePerfData",
        f"-Djava.io.tmpdir={tmp}",
        f"-Duser.home={home}",
        "-Duser.timezone=UTC",
        f"-XX:ErrorFile={run}/hs_err_pid%p.log",
    ]
    submit = [
        f"--driver-memory {DRIVER_MEMORY}",
        "--conf spark.driver.host=127.0.0.1",
        f"--conf spark.jars.ivy={home}/.ivy2",
        "pyspark-shell",
    ]
    env.update(
        TMPDIR=str(tmp),
        SPARK_LOCAL_DIRS=str(run / "spark-local"),
        SPARK_LOCAL_IP="127.0.0.1",
        JAVA_TOOL_OPTIONS=" ".join(java_options),
        XDG_CACHE_HOME=str(home / ".cache"),
        XDG_CONFIG_HOME=str(home / ".config"),
        XDG_DATA_HOME=str(home / ".local" / "share"),
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONNOUSERSITE="1",
        TZ="UTC",
        HADOOP_USER_NAME="audit",
        PYSPARK_PYTHON=str(engine_python(engine, settings)),
        PYSPARK_SUBMIT_ARGS=" ".join(submit),
        PYTHONPATH=import_path(engine, settings),
        HOME=str(home),
        **THREAD_LIMITS,
    )
    if engine == "spark":
        env["JAVA_HOME"] = str(settings.require("java_home"))
    elif settings.java_home:
        env["JAVA_HOME"] = str(settings.java_home)
    if engine == "repark":
        env["SCOREBOARD_PACKAGE"] = str(Path(settings.require("package")).resolve())
    return env


def run_engine(
    engine: str, cases: Path, output: Path, settings: Settings, log: IO[str] | None = None
) -> None:
    """Run one engine process over one case file, pinned and niced, in a fresh run directory."""
    cpus = str(settings.require("cpus"))
    root = Path(settings.require("output_root"))
    check_memory(settings)
    (root / "runs").mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix=f"{engine}.", dir=root / "runs"))
    for name in ("tmp", "home", "spark-local"):
        (run / name).mkdir()
    command = [
        "taskset",
        "-c",
        cpus,
        "nice",
        "-n",
        NICE_LEVEL,
        str(engine_python(engine, settings)),
        "-B",
        "-m",
        "repark_parity.scoreboard.engine",
        engine,
        str(cases.resolve()),
        str(output.resolve()),
    ]
    env = engine_env(engine, settings, run)
    slot = spark_slot(settings) if engine == "spark" else contextlib.nullcontext()
    with slot:
        done = subprocess.run(command, cwd=run, env=env, stdout=log, stderr=log, check=False)
    if done.returncode != 0:
        raise EngineRunError(f"{engine} engine exited {done.returncode} on {cases}")


def split_batches(cases: Path, work: Path, batch_size: int = BATCH_SIZE) -> list[tuple[Path, list]]:
    """Write a case file as batches of whole cells and return each batch path and its ids."""
    if batch_size < 1:
        raise ValueError("batch size must be positive")
    rows = list(read_records(cases).values())
    batches = []
    for offset in range(0, len(rows), batch_size):
        subset = rows[offset : offset + batch_size]
        path = work / f"{offset}.cases.jsonl"
        path.write_text("".join(json.dumps(cell) + "\n" for cell in subset))
        batches.append((path, [cell["id"] for cell in subset]))
    return batches


def check_batch(answers: Path, ids: list) -> str:
    """Return a batch's answer text after checking it answers exactly its ids in order."""
    if list(read_records(answers)) != ids:
        raise EngineRunError(f"incomplete answer file {answers}")
    return answers.read_text()


def run_half(
    engine: str, cases: Path, output: Path, settings: Settings, batch_size: int = BATCH_SIZE
) -> None:
    """Answer a large case file in bounded sessions and publish only a complete answer set."""
    root = Path(settings.require("output_root"))
    (root / "batches").mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=f"batch-{engine}-", dir=root / "batches"))
    answers = output.with_name(output.name + ".incomplete")
    log_path = output.with_name(output.stem + ".log")
    with answers.open("w") as combined, log_path.open("a") as log:
        for path, ids in split_batches(cases, work, batch_size):
            result = path.with_name(path.name.replace(".cases.", ".answers."))
            log.write(f"{engine} {cases.parent.name}: cells {ids[0]} .. {ids[-1]}\n")
            log.flush()
            run_engine(engine, path, result, settings, log)
            combined.write(check_batch(result, ids))
            combined.flush()
    answers.replace(output)
