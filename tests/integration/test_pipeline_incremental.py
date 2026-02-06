from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

from online_retail_etl.config import PipelineConfig
from online_retail_etl.pipeline import run_pipeline


def _write_source_csv(path: Path, rows: list[dict]) -> None:
    dataframe = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path, index=False)


def test_incremental_pipeline_upserts_changed_and_new_records(spark, tmp_path):
    input_csv = tmp_path / "online_retail.csv"
    output_root = tmp_path / "warehouse"
    state_path = tmp_path / "state" / "pipeline_state.json"

    initial_rows = [
        {
            "InvoiceNo": "INV1",
            "StockCode": "A",
            "Description": "Item A",
            "Quantity": "2",
            "InvoiceDate": "01/01/2011 08:00",
            "UnitPrice": "5.0",
            "CustomerID": "10001",
            "Country": "United Kingdom",
        },
        {
            "InvoiceNo": "INV2",
            "StockCode": "B",
            "Description": "Item B",
            "Quantity": "1",
            "InvoiceDate": "01/02/2011 08:00",
            "UnitPrice": "10.0",
            "CustomerID": "10002",
            "Country": "France",
        },
    ]

    _write_source_csv(input_csv, initial_rows)

    config = PipelineConfig(
        input_csv=input_csv,
        output_root=output_root,
        state_path=state_path,
        export_csv=False,
    )

    first_run = run_pipeline(spark, config, full_refresh=True)
    assert first_run.full_refresh is True
    assert first_run.curated_rows == 2

    curated_after_first_run = spark.read.parquet(str(config.silver_transactions_path))
    assert curated_after_first_run.count() == 2

    updated_rows = [
        {
            "InvoiceNo": "INV1",
            "StockCode": "A",
            "Description": "Item A",
            "Quantity": "4",
            "InvoiceDate": "01/01/2011 08:00",
            "UnitPrice": "5.0",
            "CustomerID": "10001",
            "Country": "United Kingdom",
        },
        {
            "InvoiceNo": "INV2",
            "StockCode": "B",
            "Description": "Item B",
            "Quantity": "1",
            "InvoiceDate": "01/02/2011 08:00",
            "UnitPrice": "10.0",
            "CustomerID": "10002",
            "Country": "France",
        },
        {
            "InvoiceNo": "INV3",
            "StockCode": "C",
            "Description": "Item C",
            "Quantity": "3",
            "InvoiceDate": "01/03/2011 08:00",
            "UnitPrice": "7.0",
            "CustomerID": "10003",
            "Country": "USA",
        },
    ]

    _write_source_csv(input_csv, updated_rows)

    second_run = run_pipeline(spark, config, full_refresh=False)
    assert second_run.full_refresh is False
    assert second_run.delta_rows == 2
    assert second_run.curated_rows == 3

    curated_after_second_run = spark.read.parquet(str(config.silver_transactions_path))
    assert curated_after_second_run.count() == 3

    updated_inv1 = (
        curated_after_second_run.filter(F.col("invoice_no") == "INV1")
        .select("quantity", "total_amount")
        .collect()[0]
    )
    assert updated_inv1["quantity"] == 4
    assert updated_inv1["total_amount"] == 20.0

    revenue_output = spark.read.parquet(str(config.gold_root / "total_revenue_per_customer"))
    customer_10001_revenue = (
        revenue_output.filter(F.col("customer_id") == "10001").select("total_revenue").collect()[0]["total_revenue"]
    )
    assert customer_10001_revenue == 20.0

    with state_path.open("r", encoding="utf-8") as state_file:
        state = json.load(state_file)

    assert state["last_run_mode"] == "incremental"
    assert state["curated_rows"] == 3
    assert state["delta_rows"] == 2
    assert state["last_processed_invoice_ts"] == "2011-01-03T08:00:00"
