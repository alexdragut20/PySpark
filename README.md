# Online Retail PySpark ETL

Production-style data engineering solution for the UCI Online Retail dataset.

The repository implements the full technical assignment with:
- dataset download and Excel-to-CSV conversion,
- PySpark cleaning and quality checks,
- analytical transformations,
- ETL orchestration with logging and error handling,
- incremental processing with upsert semantics,
- unit and integration tests.

## Project Structure

```text
.
├── docs/
│   └── OPTIMIZATION_STRATEGY.md
├── src/
│   └── online_retail_etl/
│       ├── acquisition.py
│       ├── analytics.py
│       ├── cleaning.py
│       ├── cli.py
│       ├── config.py
│       ├── incremental.py
│       ├── logging_utils.py
│       ├── pipeline.py
│       ├── spark.py
│       └── storage.py
├── tests/
│   ├── integration/
│   └── unit/
├── etl_pipeline.py
├── pyproject.toml
├── REQUIREMENTS.md
└── requirements.txt
```

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## Usage

### 1) Download source data and convert to CSV

```bash
python -m online_retail_etl.cli acquire
```

### 2) Run ETL (full refresh)

```bash
python -m online_retail_etl.cli run --full-refresh
```

### 3) Run ETL incrementally

```bash
python -m online_retail_etl.cli run
```

Backwards-compatible entrypoint:

```bash
python etl_pipeline.py
```

## Outputs

Primary storage format is Parquet (columnar, compressed, Spark-friendly):

- `data/processed/warehouse/silver/transactions`
- `data/processed/warehouse/gold/total_revenue_per_customer`
- `data/processed/warehouse/gold/top_5_customers_by_revenue`
- `data/processed/warehouse/gold/most_popular_product_per_country`
- `data/processed/warehouse/gold/monthly_sales_trend`
- `data/processed/warehouse/gold/monthly_order_total`
- `data/processed/warehouse/gold/customers_consecutive_orders`
- `data/processed/warehouse/gold/customer_summary`

State and quality reports:

- `data/processed/state/pipeline_state.json`
- `data/processed/warehouse/reports/quality_report.json`

Optional CSV exports are generated under `data/processed/warehouse/exports_csv/`.

## Incremental Processing

Incremental mode computes row-level hashes and upserts by business key:
- key: `invoice_no`, `stock_code`, `customer_id`
- change detection: hash comparison of all relevant fields
- updates: existing rows are replaced when the source row changes
- full refresh: `--full-refresh` clears output and rebuilds from source

## Tests

```bash
pytest
```

The test suite covers:
- cleaning decisions and quality checks,
- analytical transformations,
- end-to-end incremental update behavior.

## Requirement Mapping

Detailed mapping from assignment tasks to implementation is documented in `REQUIREMENTS.md`.
