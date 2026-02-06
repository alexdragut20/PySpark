from __future__ import annotations

import json
import logging
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from online_retail_etl import analytics
from online_retail_etl.cleaning import QualityReport, clean_transactions, read_raw_csv
from online_retail_etl.config import PipelineConfig
from online_retail_etl.incremental import compute_incremental_changes, invoice_watermark, upsert_transactions
from online_retail_etl.storage import (
    ensure_directories,
    load_state,
    read_parquet_if_exists,
    save_state,
    write_csv,
    write_parquet,
)


LOGGER = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    full_refresh: bool
    source_rows: int
    cleaned_rows: int
    delta_rows: int
    curated_rows: int
    last_processed_invoice_ts: str | None


def _write_quality_report(report: QualityReport, quality_report_path) -> None:
    quality_report_path.parent.mkdir(parents=True, exist_ok=True)
    with quality_report_path.open("w", encoding="utf-8") as quality_file:
        json.dump(report.to_dict(), quality_file, indent=2, sort_keys=True)


def _write_output_dataset(df: DataFrame, name: str, config: PipelineConfig) -> None:
    parquet_path = config.gold_root / name
    write_parquet(df, parquet_path)

    if config.export_csv:
        csv_path = config.csv_exports_root / name
        write_csv(df, csv_path)


def _write_gold_outputs(curated_df: DataFrame, config: PipelineConfig) -> None:
    outputs = {
        "total_revenue_per_customer": analytics.total_revenue_per_customer(curated_df),
        "top_5_customers_by_revenue": analytics.top_customers_by_total_revenue(curated_df, limit=5),
        "most_popular_product_per_country": analytics.most_popular_product_per_country(curated_df),
        "monthly_sales_trend": analytics.monthly_sales_trend(curated_df),
        "monthly_order_total": analytics.monthly_order_totals(curated_df),
        "customers_consecutive_orders": analytics.customers_with_consecutive_orders(curated_df),
        "customer_summary": analytics.customer_summary(curated_df),
    }

    for name, dataframe in outputs.items():
        LOGGER.info("Writing output dataset: %s", name)
        _write_output_dataset(dataframe, name, config)


def run_pipeline(spark: SparkSession, config: PipelineConfig, full_refresh: bool = False) -> PipelineResult:
    """Executes the ETL pipeline in full-refresh or incremental mode."""

    if not config.input_csv.exists():
        raise FileNotFoundError(f"Input CSV does not exist: {config.input_csv}")

    ensure_directories([config.output_root, config.reports_root, config.state_path.parent])

    if full_refresh:
        LOGGER.info("Full refresh requested; deleting previous output and state.")
        if config.output_root.exists():
            shutil.rmtree(config.output_root)
        if config.state_path.exists():
            config.state_path.unlink()
        ensure_directories([config.output_root, config.reports_root, config.state_path.parent])

    LOGGER.info("Reading raw CSV: %s", config.input_csv)
    raw_df = read_raw_csv(spark, str(config.input_csv))

    cleaned_df, quality_report = clean_transactions(raw_df)
    transformed_df = analytics.add_derived_columns(cleaned_df).persist()

    existing_transactions_df = None
    if not full_refresh:
        existing_transactions_df = read_parquet_if_exists(spark, config.silver_transactions_path)

    delta_df = compute_incremental_changes(transformed_df, existing_transactions_df).persist()
    curated_df = upsert_transactions(existing_transactions_df, delta_df).persist()

    source_rows = quality_report.input_rows
    cleaned_rows = quality_report.output_rows
    delta_rows = delta_df.count()
    curated_rows = curated_df.count()
    watermark = invoice_watermark(curated_df)

    LOGGER.info(
        "Pipeline stats | source_rows=%s cleaned_rows=%s delta_rows=%s curated_rows=%s",
        source_rows,
        cleaned_rows,
        delta_rows,
        curated_rows,
    )

    write_parquet(curated_df, config.silver_transactions_path, partition_by="order_month")
    _write_gold_outputs(curated_df, config)

    _write_quality_report(quality_report, config.quality_report_path)

    previous_state = load_state(config.state_path)
    previous_state.update(
        {
            "last_run_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "last_processed_invoice_ts": watermark,
            "last_run_mode": "full_refresh" if full_refresh else "incremental",
            "source_rows": source_rows,
            "cleaned_rows": cleaned_rows,
            "delta_rows": delta_rows,
            "curated_rows": curated_rows,
            "quality_report": asdict(quality_report),
        }
    )
    save_state(config.state_path, previous_state)

    transformed_df.unpersist()
    delta_df.unpersist()
    curated_df.unpersist()

    return PipelineResult(
        full_refresh=full_refresh,
        source_rows=source_rows,
        cleaned_rows=cleaned_rows,
        delta_rows=delta_rows,
        curated_rows=curated_rows,
        last_processed_invoice_ts=watermark,
    )
