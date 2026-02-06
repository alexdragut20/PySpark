# Assignment Requirement Mapping

This document maps each requirement from `Data Engineer_Home Assignment.pdf` to the implemented code.

## Part 1 - Data Processing

### Task 1.1 - Data Acquisition and Preparation

1. Download dataset from UCI using Python
- Implemented in `src/online_retail_etl/acquisition.py::download_dataset`
- CLI command: `python -m online_retail_etl.cli acquire`

2. Convert Excel to CSV using pandas
- Implemented in `src/online_retail_etl/acquisition.py::convert_excel_to_csv`
- Uses `pandas.read_excel(...).to_csv(...)`

3. Read CSV with PySpark and perform cleaning/quality checks
- CSV read: `src/online_retail_etl/cleaning.py::read_raw_csv`
- Type conversion:
  - `quantity` -> `int`
  - `unit_price` -> `double`
  - `invoice_ts` -> `timestamp`
  - `customer_id` normalized to string
- Null handling:
  - `customer_id` -> `UNKNOWN`
  - `quantity` -> `0`
  - `unit_price` -> `0.0`
  - `description` -> `UNKNOWN_PRODUCT`
  - `country` -> `UNKNOWN_COUNTRY`
- Duplicate removal: `dropDuplicates()`
- Quality report: `QualityReport` dataclass persisted to
  `data/processed/warehouse/reports/quality_report.json`

### Task 1.2 - Data Processing and Analysis

1. Total revenue per customer + top 5
- `src/online_retail_etl/analytics.py::total_revenue_per_customer`
- `src/online_retail_etl/analytics.py::top_customers_by_total_revenue`

2. Most popular product per country
- `src/online_retail_etl/analytics.py::most_popular_product_per_country`
- Uses quantity aggregation and country-level ranking

3. Monthly sales trend
- `src/online_retail_etl/analytics.py::monthly_sales_trend`

## Part 2 - Data Transformation

### Task 2.1 - Data Transformation

1. `TotalAmount = Quantity * UnitPrice`
- `src/online_retail_etl/analytics.py::add_derived_columns`

2. `OrderMonth` from `InvoiceDate`
- `src/online_retail_etl/analytics.py::add_derived_columns` (`yyyy-MM`)

3. Monthly order total per customer
- `src/online_retail_etl/analytics.py::monthly_order_totals`

4. Customers with orders in consecutive months
- `src/online_retail_etl/analytics.py::customers_with_consecutive_orders`
- Uses window lag and `add_months` comparison

## Part 3 - Data Pipeline Implementation

### Task 3.1 - ETL Pipeline

1. Read CSV created in Task 1.1
- `src/online_retail_etl/pipeline.py::run_pipeline`

2. Apply transformations from Tasks 1.2 and 2.1
- Pipeline calls analytics module to materialize all outputs

3. Choose output format
- Primary format: Parquet (`silver` and `gold` layers)
- Optional CSV exports enabled by default
- Rationale: Parquet supports compression, column pruning, and efficient Spark scans

4. Error handling and logging
- CLI catches and logs pipeline failures (`src/online_retail_etl/cli.py`)
- Structured logging configured by `src/online_retail_etl/logging_utils.py`

### Task 3.2 - Incremental Processing

1. Process only new or updated records since previous run
- `src/online_retail_etl/incremental.py::compute_incremental_changes`
- Uses row-level SHA-256 hash comparison against existing curated data

2. Track last processed date/ID
- `last_processed_invoice_ts` stored in state file:
  `data/processed/state/pipeline_state.json`

3. Update existing output records if source changed
- `src/online_retail_etl/incremental.py::upsert_transactions`
- Replaces existing rows by business key when hash differs

4. Full refresh support
- `--full-refresh` in `python -m online_retail_etl.cli run --full-refresh`
- Clears prior outputs and rebuilds all datasets

## Part 3.3 - Optimization Challenge

Optimization strategy for 100GB-scale data is documented in:
- `docs/OPTIMIZATION_STRATEGY.md`

## Assumptions and Notes

- Returns/cancellations are kept (negative quantities/prices), because they affect net revenue.
- Product popularity uses positive quantity rows only.
- Incremental key is `invoice_no + stock_code + customer_id`.
- If `customer_id` is missing, it is represented as `UNKNOWN`.
