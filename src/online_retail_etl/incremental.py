from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


UPSERT_KEY_COLUMNS = ["invoice_no", "stock_code", "customer_id"]
HASH_COLUMNS = [
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_ts",
    "unit_price",
    "customer_id",
    "country",
    "total_amount",
    "order_month",
]


def with_record_hash(df: DataFrame) -> DataFrame:
    hash_expr = F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(column).cast("string"), F.lit("NULL")) for column in HASH_COLUMNS],
        ),
        256,
    )
    return df.withColumn("record_hash", hash_expr)


def deduplicate_latest_by_key(df: DataFrame) -> DataFrame:
    window = Window.partitionBy(*UPSERT_KEY_COLUMNS).orderBy(F.col("invoice_ts").desc(), F.col("record_hash").desc())
    return df.withColumn("row_number", F.row_number().over(window)).filter(F.col("row_number") == 1).drop("row_number")


def compute_incremental_changes(incoming_df: DataFrame, existing_df: DataFrame | None) -> DataFrame:
    incoming_hashed = deduplicate_latest_by_key(with_record_hash(incoming_df))

    if existing_df is None:
        return incoming_hashed

    existing_hashed = deduplicate_latest_by_key(with_record_hash(existing_df)).select(
        *UPSERT_KEY_COLUMNS,
        "record_hash",
    )

    joined = incoming_hashed.alias("incoming").join(existing_hashed.alias("existing"), on=UPSERT_KEY_COLUMNS, how="left")

    return joined.filter(
        F.col("existing.record_hash").isNull() | (F.col("incoming.record_hash") != F.col("existing.record_hash"))
    ).select("incoming.*")


def upsert_transactions(existing_df: DataFrame | None, delta_df: DataFrame) -> DataFrame:
    delta_without_hash = delta_df.drop("record_hash")

    if existing_df is None:
        return delta_without_hash

    delta_keys = delta_without_hash.select(*UPSERT_KEY_COLUMNS).dropDuplicates()
    untouched_existing = existing_df.join(delta_keys, on=UPSERT_KEY_COLUMNS, how="left_anti")

    merged = untouched_existing.unionByName(delta_without_hash)
    return deduplicate_latest_by_key(with_record_hash(merged)).drop("record_hash")


def invoice_watermark(df: DataFrame) -> str | None:
    record = df.select(F.date_format(F.max("invoice_ts"), "yyyy-MM-dd'T'HH:mm:ss").alias("max_invoice_ts")).collect()[0]
    return record["max_invoice_ts"]
