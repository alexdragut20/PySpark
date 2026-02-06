# Spark Optimization Strategy (100GB Scale)

For significantly larger datasets, the following strategy should be applied.

## 1. Storage and File Layout

- Keep source and curated layers in Parquet.
- Partition by `order_month` to prune scans for monthly analytics.
- Target partition/file size around 128MB-512MB.
- Use compression (Snappy by default in Parquet).

## 2. Data Model and Incremental Design

- Keep a Silver transactional table with immutable business columns plus ETL metadata.
- Use row-level hash and deterministic business key for upserts.
- Store processing state and watermark in a reliable metadata store (e.g., object store + transactional DB).

## 3. Spark Execution Tuning

- Use adaptive query execution:
  - `spark.sql.adaptive.enabled=true`
  - `spark.sql.adaptive.coalescePartitions.enabled=true`
- Tune shuffle partitions based on cluster cores and data size.
- Broadcast small dimensions/reference data.
- Repartition intentionally before heavy joins and writes.

## 4. Cluster and Resource Sizing

- Size executors for workload profile (memory-heavy vs shuffle-heavy).
- Avoid oversized executors; prefer balanced parallelism.
- Enable dynamic allocation where available.
- Monitor spill, GC, and skew in Spark UI.

## 5. Skew and Hotspot Management

- Detect skewed keys (`country`, dominant customers, etc.).
- Use salting or skew hints for pathological joins.
- Split very large partitions before aggregation.

## 6. Data Quality at Scale

- Make quality checks incremental (check only changed partitions when possible).
- Emit observability metrics (null rates, duplicate rates, parse failures) to monitoring.
- Add threshold-based alerts to stop bad writes.

## 7. Operational Reliability

- Make writes atomic (write to temp path, then swap/commit).
- Add idempotent retry semantics for transient failures.
- Keep full-refresh as controlled maintenance operation.
