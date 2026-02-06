from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_ROOT = PROJECT_ROOT / "data"
DEFAULT_RAW_DIR = DEFAULT_DATA_ROOT / "raw"
DEFAULT_PROCESSED_DIR = DEFAULT_DATA_ROOT / "processed"


@dataclass
class PipelineConfig:
    """Configuration object for ETL execution."""

    input_csv: Path
    output_root: Path
    state_path: Path
    export_csv: bool = True
    app_name: str = "OnlineRetailETL"
    spark_master: str | None = None

    def __post_init__(self) -> None:
        self.input_csv = Path(self.input_csv)
        self.output_root = Path(self.output_root)
        self.state_path = Path(self.state_path)

    @property
    def silver_transactions_path(self) -> Path:
        return self.output_root / "silver" / "transactions"

    @property
    def gold_root(self) -> Path:
        return self.output_root / "gold"

    @property
    def csv_exports_root(self) -> Path:
        return self.output_root / "exports_csv"

    @property
    def reports_root(self) -> Path:
        return self.output_root / "reports"

    @property
    def quality_report_path(self) -> Path:
        return self.reports_root / "quality_report.json"


def default_pipeline_config() -> PipelineConfig:
    """Builds default configuration rooted in the repository layout."""

    return PipelineConfig(
        input_csv=DEFAULT_PROCESSED_DIR / "online_retail.csv",
        output_root=DEFAULT_PROCESSED_DIR / "warehouse",
        state_path=DEFAULT_PROCESSED_DIR / "state" / "pipeline_state.json",
        export_csv=True,
    )


def default_excel_path() -> Path:
    return DEFAULT_RAW_DIR / "online_retail.xlsx"


def default_csv_path() -> Path:
    return DEFAULT_PROCESSED_DIR / "online_retail.csv"
