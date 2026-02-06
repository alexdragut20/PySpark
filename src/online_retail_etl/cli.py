from __future__ import annotations

import argparse
import logging
from pathlib import Path

from online_retail_etl.acquisition import DEFAULT_DATASET_URL, convert_excel_to_csv, download_dataset
from online_retail_etl.config import PipelineConfig, default_csv_path, default_excel_path, default_pipeline_config
from online_retail_etl.logging_utils import configure_logging
from online_retail_etl.pipeline import run_pipeline
from online_retail_etl.spark import create_spark_session


LOGGER = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    defaults = default_pipeline_config()

    parser = argparse.ArgumentParser(description="Online Retail ETL pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    acquire_parser = subparsers.add_parser("acquire", help="Download dataset and convert it to CSV")
    acquire_parser.add_argument("--dataset-url", default=DEFAULT_DATASET_URL)
    acquire_parser.add_argument("--excel-path", type=Path, default=default_excel_path())
    acquire_parser.add_argument("--csv-path", type=Path, default=default_csv_path())
    acquire_parser.add_argument("--log-level", default="INFO")

    run_parser = subparsers.add_parser("run", help="Execute ETL pipeline")
    run_parser.add_argument("--input-csv", type=Path, default=defaults.input_csv)
    run_parser.add_argument("--output-root", type=Path, default=defaults.output_root)
    run_parser.add_argument("--state-path", type=Path, default=defaults.state_path)
    run_parser.add_argument("--spark-master", default=None)
    run_parser.add_argument("--full-refresh", action="store_true")
    run_parser.add_argument("--no-csv-export", action="store_true")
    run_parser.add_argument("--log-level", default="INFO")

    return parser


def _run_acquisition(args: argparse.Namespace) -> int:
    configure_logging(args.log_level)

    excel_path = download_dataset(args.dataset_url, args.excel_path)
    convert_excel_to_csv(excel_path, args.csv_path)

    LOGGER.info("Source data prepared at %s", args.csv_path)
    return 0


def _run_pipeline_command(args: argparse.Namespace) -> int:
    configure_logging(args.log_level)

    config = PipelineConfig(
        input_csv=args.input_csv,
        output_root=args.output_root,
        state_path=args.state_path,
        export_csv=not args.no_csv_export,
        spark_master=args.spark_master,
    )

    spark = create_spark_session(config.app_name, master=config.spark_master)

    try:
        result = run_pipeline(spark, config, full_refresh=args.full_refresh)
        LOGGER.info(
            "Pipeline finished | mode=%s source_rows=%s cleaned_rows=%s delta_rows=%s curated_rows=%s watermark=%s",
            "full_refresh" if result.full_refresh else "incremental",
            result.source_rows,
            result.cleaned_rows,
            result.delta_rows,
            result.curated_rows,
            result.last_processed_invoice_ts,
        )
    except Exception:
        LOGGER.exception("Pipeline execution failed")
        return 1
    finally:
        spark.stop()

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "acquire":
        return _run_acquisition(args)

    if args.command == "run":
        return _run_pipeline_command(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
