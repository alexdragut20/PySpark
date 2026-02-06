from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


RAW_TO_STANDARD_COLUMNS = {
    "InvoiceNo": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date_raw",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country",
}

CLEANED_COLUMNS = [
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_ts",
    "unit_price",
    "customer_id",
    "country",
]


@dataclass
class QualityReport:
    input_rows: int
    output_rows: int
    duplicates_removed: int
    rows_dropped_missing_keys_or_dates: int
    null_customer_ids_filled: int
    null_quantities_filled: int
    null_unit_prices_filled: int
    null_descriptions_filled: int
    null_countries_filled: int
    invoice_timestamp_parse_failures: int

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


def _null_or_blank(column_name: str) -> F.Column:
    column = F.col(column_name)
    return column.isNull() | (F.trim(column.cast("string")) == "")


def _count_null_like(df: DataFrame, column_names: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for column_name in column_names:
        counts[column_name] = df.filter(_null_or_blank(column_name)).count()
    return counts


def read_raw_csv(spark: SparkSession, csv_path: str) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(csv_path)
    )


def standardize_raw_columns(df: DataFrame) -> DataFrame:
    missing_columns = [column for column in RAW_TO_STANDARD_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Input data is missing required columns: {missing_columns}")

    return df.select([F.col(source).alias(target) for source, target in RAW_TO_STANDARD_COLUMNS.items()])


def clean_transactions(df: DataFrame) -> tuple[DataFrame, QualityReport]:
    standardized = standardize_raw_columns(df)
    input_rows = standardized.count()

    null_counts = _count_null_like(
        standardized,
        ["customer_id", "quantity", "unit_price", "description", "country"],
    )

    trimmed = standardized.select(
        F.when(_null_or_blank("invoice_no"), F.lit(None)).otherwise(F.trim(F.col("invoice_no"))).alias(
            "invoice_no"
        ),
        F.when(_null_or_blank("stock_code"), F.lit(None)).otherwise(F.trim(F.col("stock_code"))).alias(
            "stock_code"
        ),
        F.when(_null_or_blank("description"), F.lit(None)).otherwise(F.trim(F.col("description"))).alias(
            "description"
        ),
        F.when(_null_or_blank("quantity"), F.lit(None)).otherwise(F.trim(F.col("quantity"))).alias("quantity"),
        F.when(_null_or_blank("invoice_date_raw"), F.lit(None))
        .otherwise(F.trim(F.col("invoice_date_raw")))
        .alias("invoice_date_raw"),
        F.when(_null_or_blank("unit_price"), F.lit(None)).otherwise(F.trim(F.col("unit_price"))).alias(
            "unit_price"
        ),
        F.when(_null_or_blank("customer_id"), F.lit(None)).otherwise(F.trim(F.col("customer_id"))).alias(
            "customer_id"
        ),
        F.when(_null_or_blank("country"), F.lit(None)).otherwise(F.trim(F.col("country"))).alias("country"),
    )

    timestamp_column = F.coalesce(
        F.to_timestamp(F.col("invoice_date_raw"), "M/d/yyyy H:mm"),
        F.to_timestamp(F.col("invoice_date_raw"), "M/d/yyyy H:mm:ss"),
        F.to_timestamp(F.col("invoice_date_raw"), "MM/dd/yyyy HH:mm"),
        F.to_timestamp(F.col("invoice_date_raw"), "yyyy-MM-dd HH:mm:ss"),
    )

    typed = (
        trimmed.withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("invoice_ts", timestamp_column)
        .withColumn("customer_id", F.regexp_replace(F.col("customer_id").cast("string"), r"\.0$", ""))
        .drop("invoice_date_raw")
    )

    cleaned = (
        typed.fillna({"quantity": 0, "unit_price": 0.0})
        .withColumn(
            "customer_id",
            F.when(_null_or_blank("customer_id"), F.lit("UNKNOWN")).otherwise(F.col("customer_id")),
        )
        .withColumn(
            "description",
            F.when(_null_or_blank("description"), F.lit("UNKNOWN_PRODUCT")).otherwise(F.col("description")),
        )
        .withColumn(
            "country",
            F.when(_null_or_blank("country"), F.lit("UNKNOWN_COUNTRY")).otherwise(F.col("country")),
        )
    )

    parse_failures = cleaned.filter(F.col("invoice_ts").isNull()).count()

    deduplicated = cleaned.dropDuplicates()
    deduplicated_rows = deduplicated.count()
    duplicates_removed = cleaned.count() - deduplicated_rows

    valid_records = deduplicated.filter(
        F.col("invoice_no").isNotNull()
        & F.col("stock_code").isNotNull()
        & F.col("invoice_ts").isNotNull()
    )

    output_rows = valid_records.count()

    report = QualityReport(
        input_rows=input_rows,
        output_rows=output_rows,
        duplicates_removed=duplicates_removed,
        rows_dropped_missing_keys_or_dates=deduplicated_rows - output_rows,
        null_customer_ids_filled=null_counts["customer_id"],
        null_quantities_filled=null_counts["quantity"],
        null_unit_prices_filled=null_counts["unit_price"],
        null_descriptions_filled=null_counts["description"],
        null_countries_filled=null_counts["country"],
        invoice_timestamp_parse_failures=parse_failures,
    )

    return valid_records.select(*CLEANED_COLUMNS), report
