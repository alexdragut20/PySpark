from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession


def ensure_directories(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def path_exists(path: Path) -> bool:
    return Path(path).exists()


def read_parquet_if_exists(spark: SparkSession, path: Path) -> DataFrame | None:
    if not path_exists(path):
        return None
    return spark.read.parquet(str(path))


def write_parquet(df: DataFrame, path: Path, mode: str = "overwrite", partition_by: str | None = None) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.parquet(str(path))


def write_csv(df: DataFrame, path: Path, mode: str = "overwrite") -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.coalesce(1).write.mode(mode).option("header", True).csv(str(path))


def load_state(state_path: Path) -> dict:
    if not Path(state_path).exists():
        return {}

    with Path(state_path).open("r", encoding="utf-8") as state_file:
        return json.load(state_file)


def save_state(state_path: Path, state: dict) -> None:
    state_path = Path(state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    with state_path.open("w", encoding="utf-8") as state_file:
        json.dump(state, state_file, indent=2, sort_keys=True)
