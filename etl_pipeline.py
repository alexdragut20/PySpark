"""Backward-compatible entrypoint for running the ETL pipeline."""

from __future__ import annotations

import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parent / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from online_retail_etl.cli import main


if __name__ == "__main__":
    args = sys.argv[1:] or ["run"]
    raise SystemExit(main(args))
