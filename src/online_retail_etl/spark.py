from __future__ import annotations

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str,
    master: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Creates a Spark session with sane local defaults."""

    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    builder = builder.config("spark.sql.session.timeZone", "UTC")
    builder = builder.config("spark.sql.shuffle.partitions", "8")

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
