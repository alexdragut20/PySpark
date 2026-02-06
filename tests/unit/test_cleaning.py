from __future__ import annotations

import pytest

from online_retail_etl.cleaning import clean_transactions, standardize_raw_columns


RAW_SCHEMA_COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]


def test_clean_transactions_handles_nulls_types_duplicates_and_invalid_rows(spark):
    rows = [
        ("10001", "A", "Desk Lamp", "2", "12/1/2010 8:26", "3.5", "17850.0", "United Kingdom"),
        ("10001", "A", "Desk Lamp", "2", "12/1/2010 8:26", "3.5", "17850.0", "United Kingdom"),
        ("10002", "B", None, None, "12/2/2010 9:00", None, None, None),
        ("", "C", "Invalid Row", "1", "", "2.0", "12345", "France"),
    ]

    raw_df = spark.createDataFrame(rows, RAW_SCHEMA_COLUMNS)
    cleaned_df, report = clean_transactions(raw_df)

    assert report.input_rows == 4
    assert report.output_rows == 2
    assert report.duplicates_removed == 1
    assert report.rows_dropped_missing_keys_or_dates == 1
    assert report.null_customer_ids_filled == 1
    assert report.null_quantities_filled == 1
    assert report.null_unit_prices_filled == 1
    assert report.null_descriptions_filled == 1
    assert report.null_countries_filled == 1
    assert report.invoice_timestamp_parse_failures == 1

    collected = {row["invoice_no"]: row.asDict() for row in cleaned_df.collect()}

    assert collected["10001"]["customer_id"] == "17850"
    assert collected["10001"]["quantity"] == 2
    assert collected["10001"]["unit_price"] == 3.5
    assert collected["10001"]["invoice_ts"] is not None

    assert collected["10002"]["customer_id"] == "UNKNOWN"
    assert collected["10002"]["quantity"] == 0
    assert collected["10002"]["unit_price"] == 0.0
    assert collected["10002"]["description"] == "UNKNOWN_PRODUCT"
    assert collected["10002"]["country"] == "UNKNOWN_COUNTRY"


def test_standardize_raw_columns_raises_for_missing_required_columns(spark):
    incomplete_df = spark.createDataFrame([(1,)], ["InvoiceNo"])

    with pytest.raises(ValueError, match="missing required columns"):
        standardize_raw_columns(incomplete_df)
