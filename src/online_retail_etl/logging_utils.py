from __future__ import annotations

import logging


def configure_logging(level: str = "INFO") -> None:
    """Configures root logger once and updates level on repeated calls."""

    numeric_level = logging.getLevelName(level.upper())
    root = logging.getLogger()

    if not root.handlers:
        logging.basicConfig(
            level=numeric_level,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )
    else:
        root.setLevel(numeric_level)
