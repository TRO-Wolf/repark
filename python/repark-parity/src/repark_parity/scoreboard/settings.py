from __future__ import annotations

import argparse
from collections.abc import Mapping
from pathlib import Path

from pydantic import BaseModel, ConfigDict

SOURCES: dict[str, tuple[str, tuple[str, ...], str]] = {
    "spark_python": ("--spark-python", ("SCOREBOARD_SPARK_PYTHON",), "the Spark 4.1.2 interpreter"),
    "repark_python": (
        "--repark-python",
        ("SCOREBOARD_REPARK_PYTHON", "SCOREBOARD_SPARK_PYTHON"),
        "the interpreter for the RePark half (defaults to the Spark interpreter)",
    ),
    "dependency_path": (
        "--dependency-path",
        ("SCOREBOARD_DEPENDENCY_PATH",),
        "extra import path after the package, for the RePark half",
    ),
    "java_home": ("--java-home", ("SCOREBOARD_JAVA_HOME", "JAVA_HOME"), "the Java 17 home"),
    "cpus": ("--cpus", ("SCOREBOARD_CPUS",), "the CPU list every engine process is pinned to"),
    "spark_drivers": (
        "--spark-drivers",
        ("SCOREBOARD_SPARK_DRIVERS",),
        "the most Spark drivers this run starts at once",
    ),
    "memory_floor_mib": (
        "--memory-floor-mib",
        ("SCOREBOARD_MEMORY_FLOOR_MIB",),
        "the available memory, in MiB, below which a batch refuses to start",
    ),
    "output_root": (
        "--output-root",
        ("SCOREBOARD_OUTPUT_ROOT",),
        "the directory that holds run directories, locks and new destinations",
    ),
    "package": (
        "--package",
        ("SCOREBOARD_PACKAGE",),
        "the directory that contains repark/_native*.so and its facade",
    ),
}


class MissingSettingError(RuntimeError):
    """A command needs a setting that neither its flag nor its environment variable gives."""


class Settings(BaseModel):
    """Machine settings for engine runs; each comes from a flag or an environment variable."""

    model_config = ConfigDict(frozen=True)

    spark_python: Path | None = None
    repark_python: Path | None = None
    dependency_path: str | None = None
    java_home: Path | None = None
    cpus: str | None = None
    spark_drivers: int | None = None
    memory_floor_mib: int | None = None
    output_root: Path | None = None
    package: Path | None = None

    def require(self, name: str) -> object:
        """Return one setting, or raise an error that names its flag and variables."""
        value = getattr(self, name)
        if value is None:
            flag, variables, meaning = SOURCES[name]
            names = " or ".join(variables)
            raise MissingSettingError(f"missing setting {flag} (or {names}): {meaning}")
        return value


def add_setting_flags(parser: argparse.ArgumentParser, names: tuple[str, ...]) -> None:
    """Add the flags of the named settings to one subcommand parser."""
    for name in names:
        flag, variables, meaning = SOURCES[name]
        parser.add_argument(flag, dest=name, default=None, help=f"{meaning}; env {variables[0]}")


def resolve_settings(args: argparse.Namespace, environ: Mapping[str, str]) -> Settings:
    """Take each setting from its flag first and then from its environment variables."""
    values: dict[str, str] = {}
    for name, (_, variables, _) in SOURCES.items():
        given = getattr(args, name, None)
        if given is None:
            given = next((environ[key] for key in variables if environ.get(key)), None)
        if given is not None:
            values[name] = given
    return Settings.model_validate(values)
