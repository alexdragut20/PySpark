from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Adds TotalAmount and OrderMonth fields required by the assignment."""

    return (
        df.withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("order_month", F.date_format(F.col("invoice_ts"), "yyyy-MM"))
    )


def total_revenue_per_customer(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("customer_id")
        .agg(F.round(F.sum("total_amount"), 2).alias("total_revenue"))
        .orderBy(F.col("total_revenue").desc(), F.col("customer_id").asc())
    )


def top_customers_by_total_revenue(df: DataFrame, limit: int = 5) -> DataFrame:
    return total_revenue_per_customer(df).limit(limit)


def most_popular_product_per_country(df: DataFrame) -> DataFrame:
    popularity = (
        df.filter(F.col("quantity") > 0)
        .groupBy("country", "stock_code", "description")
        .agg(F.sum("quantity").alias("total_quantity_sold"))
    )

    rank_window = Window.partitionBy("country").orderBy(
        F.col("total_quantity_sold").desc(), F.col("stock_code").asc()
    )

    return (
        popularity.withColumn("country_rank", F.row_number().over(rank_window))
        .filter(F.col("country_rank") == 1)
        .drop("country_rank")
        .orderBy("country")
    )


def monthly_sales_trend(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("order_month")
        .agg(F.round(F.sum("total_amount"), 2).alias("monthly_revenue"))
        .orderBy("order_month")
    )


def monthly_order_totals(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("customer_id", "order_month")
        .agg(F.round(F.sum("total_amount"), 2).alias("monthly_order_total"))
        .orderBy("customer_id", "order_month")
    )


def customers_with_consecutive_orders(df: DataFrame) -> DataFrame:
    customer_months = (
        df.select(
            "customer_id",
            "order_month",
            F.to_date(F.concat(F.col("order_month"), F.lit("-01"))).alias("order_month_date"),
        )
        .dropDuplicates(["customer_id", "order_month"])
        .filter(F.col("customer_id") != "UNKNOWN")
    )

    window = Window.partitionBy("customer_id").orderBy("order_month_date")

    with_previous = customer_months.withColumn("previous_order_month_date", F.lag("order_month_date", 1).over(window))

    consecutive = with_previous.filter(
        F.col("previous_order_month_date").isNotNull()
        & (F.add_months(F.col("previous_order_month_date"), 1) == F.col("order_month_date"))
    )

    return consecutive.select(
        "customer_id",
        F.date_format("order_month_date", "yyyy-MM").alias("order_month"),
        F.date_format("previous_order_month_date", "yyyy-MM").alias("previous_order_month"),
    ).orderBy("customer_id", "order_month")


def customer_consecutive_flags(df: DataFrame) -> DataFrame:
    consecutive = customers_with_consecutive_orders(df)
    flags = consecutive.select("customer_id").dropDuplicates().withColumn("has_consecutive_orders", F.lit(True))
    all_customers = df.select("customer_id").dropDuplicates()

    return (
        all_customers.join(flags, on="customer_id", how="left")
        .fillna(False, subset=["has_consecutive_orders"])
        .orderBy("customer_id")
    )


def customer_summary(df: DataFrame) -> DataFrame:
    revenue = total_revenue_per_customer(df)
    monthly = monthly_order_totals(df)
    flags = customer_consecutive_flags(df)

    return (
        monthly.join(revenue, on="customer_id", how="left")
        .join(flags, on="customer_id", how="left")
        .fillna(False, subset=["has_consecutive_orders"])
        .orderBy("customer_id", "order_month")
    )
