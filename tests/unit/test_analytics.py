from __future__ import annotations

from datetime import datetime

from online_retail_etl import analytics


def _build_base_dataframe(spark):
    rows = [
        ("INV1", "A", "Prod A", 2, datetime(2010, 1, 5, 10, 0), 5.0, "C1", "UK"),
        ("INV2", "A", "Prod A", 1, datetime(2010, 2, 5, 10, 0), 5.0, "C1", "UK"),
        ("INV3", "B", "Prod B", 4, datetime(2010, 1, 7, 10, 0), 2.0, "C2", "France"),
        ("INV4", "C", "Prod C", -1, datetime(2010, 1, 8, 10, 0), 10.0, "C2", "France"),
        ("INV5", "D", "Prod D", 3, datetime(2010, 3, 9, 10, 0), 3.0, "C3", "USA"),
        ("INV6", "E", "Prod E", 7, datetime(2010, 3, 10, 10, 0), 1.0, "UNKNOWN", "USA"),
    ]
    columns = [
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_ts",
        "unit_price",
        "customer_id",
        "country",
    ]
    return spark.createDataFrame(rows, columns)


def test_assignment_metrics_and_transformations(spark):
    base_df = _build_base_dataframe(spark)
    enriched_df = analytics.add_derived_columns(base_df)

    revenue_rows = analytics.total_revenue_per_customer(enriched_df).collect()
    revenue_map = {row["customer_id"]: row["total_revenue"] for row in revenue_rows}
    assert revenue_map == {"C1": 15.0, "C3": 9.0, "UNKNOWN": 7.0, "C2": -2.0}

    top_customer = analytics.top_customers_by_total_revenue(enriched_df, limit=1).collect()[0]
    assert top_customer["customer_id"] == "C1"
    assert top_customer["total_revenue"] == 15.0

    popular = analytics.most_popular_product_per_country(enriched_df).collect()
    popular_map = {row["country"]: row["stock_code"] for row in popular}
    assert popular_map == {"France": "B", "UK": "A", "USA": "E"}

    monthly_trend = analytics.monthly_sales_trend(enriched_df).collect()
    monthly_map = {row["order_month"]: row["monthly_revenue"] for row in monthly_trend}
    assert monthly_map == {"2010-01": 8.0, "2010-02": 5.0, "2010-03": 16.0}

    monthly_totals = analytics.monthly_order_totals(enriched_df).collect()
    assert any(row["customer_id"] == "C1" and row["order_month"] == "2010-01" and row["monthly_order_total"] == 10.0 for row in monthly_totals)

    consecutive = analytics.customers_with_consecutive_orders(enriched_df).collect()
    assert len(consecutive) == 1
    assert consecutive[0]["customer_id"] == "C1"
    assert consecutive[0]["previous_order_month"] == "2010-01"
    assert consecutive[0]["order_month"] == "2010-02"

    summary = analytics.customer_summary(enriched_df).collect()
    c1_records = [row for row in summary if row["customer_id"] == "C1"]
    assert len(c1_records) == 2
    assert all(record["has_consecutive_orders"] for record in c1_records)
