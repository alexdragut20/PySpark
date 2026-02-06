from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import requests


LOGGER = logging.getLogger(__name__)
DEFAULT_DATASET_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"


def download_dataset(url: str, destination: Path, timeout_seconds: int = 60) -> Path:
    """Downloads the Online Retail Excel file."""

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Downloading dataset from %s", url)
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    destination.write_bytes(response.content)

    LOGGER.info("Dataset downloaded: %s", destination)
    return destination


def convert_excel_to_csv(excel_path: Path, csv_path: Path) -> Path:
    """Converts Excel source data to CSV using pandas."""

    excel_path = Path(excel_path)
    csv_path = Path(csv_path)

    if not excel_path.exists():
        raise FileNotFoundError(f"Excel source file not found: {excel_path}")

    csv_path.parent.mkdir(parents=True, exist_ok=True)

    dataframe = pd.read_excel(excel_path)
    dataframe.to_csv(csv_path, index=False)

    LOGGER.info("Converted %s to %s", excel_path, csv_path)
    return csv_path
