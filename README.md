# Data Processing and ETL Pipeline for Online Retail Dataset

## Overview
This project provides an end-to-end ETL pipeline to process, analyze, and transform the "Online Retail" dataset from the UCI Machine Learning Repository using PySpark. The pipeline includes data acquisition, cleaning, transformation, and analysis. Outputs are provided in various formats such as CSV and Parquet.

Key objectives include:
- Acquiring and preparing the raw dataset
- Performing data cleaning and quality checks
- Applying analytical transformations to calculate total revenue, sales trends, and product popularity
- Designing an efficient ETL pipeline for large-scale data processing

## Table of Contents
- [Overview](#overview)
- [Installation](#installation)
- [Usage](#usage)
- [Classes Overview](#classes-overview)
- [ETL Pipeline](#etl-pipeline)
- [Project Structure](#project-structure)
- [Outputs](#outputs)
- [Testing](#testing)
- [Requirements](#requirements)
- [License](#license)

## Installation
### Prerequisites
1. Install Python 3.7 or above.
2. Install PySpark and other necessary dependencies by running:
   ```bash
   pip install -r requirements.txt
   ```

### Setup Instructions
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Ensure you have Apache Spark installed on your system. You can download it from [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html).

3. Set up your Spark environment variables if needed:
   ```bash
   export SPARK_HOME=/path/to/spark
   export PATH=$SPARK_HOME/bin:$PATH
   ```

## Usage
To run the ETL pipeline and process the dataset:
1. Place the input CSV file (`online_retail.csv`) inside the `data/processed/` directory.
2. Run the pipeline using the following command:
   ```bash
   python etl_pipeline.py
   ```

The processed data will be saved in the `data/processed/final_output` folder.

## Classes Overview

### DataCleaner
- **Purpose**: Handles data acquisition and preparation tasks.
- **Key Methods**:
  - `read_csv()`: Reads the raw CSV file into a PySpark DataFrame.
  - `clean_data()`: Cleans the data by filling null values, converting types, and removing duplicates.

### DataProcessor
- **Purpose**: Performs data analysis and transformation.
- **Key Methods**:
  - `calculate_total_revenue_per_customer()`: Calculates total revenue per customer.
  - `most_popular_product_per_country()`: Identifies the most popular product by quantity sold for each country.
  - `calculate_monthly_sales_trend()`: Analyzes the monthly sales trend.
  - `create_total_amount_column()`: Creates a `TotalAmount` column.
  - `identify_customers_with_consecutive_orders()`: Identifies customers with consecutive orders.

## ETL Pipeline
The ETL pipeline performs the following steps:
1. **Data Acquisition and Cleaning**: The raw data is read from a CSV file, cleaned to handle null values, and checked for duplicates.
2. **Data Processing**:
   - Calculate the total revenue per customer.
   - Determine the most popular product by country.
   - Analyze monthly sales trends.
3. **Data Transformation**: 
   - Create new columns like `TotalAmount` and `OrderMonth`.
   - Calculate monthly order totals and identify customers with consecutive orders.
4. **Output**: The processed data is saved in CSV and Parquet formats in the `final_output` directory.

## Project Structure



## Outputs
The processed data includes:
- **total_revenue_per_customer.csv**: Total revenue per customer, ordered by revenue.
- **most_popular_product_per_country.csv**: The most popular product in each country, ordered alphabetically.
- **monthly_sales_trend.csv**: Total revenue per month.
- **monthly_order_total.csv**: The monthly order total for each customer.
- **customers_consecutive_orders.csv**: Customers who placed consecutive monthly orders.
- **combined_customer_data.csv**: Customer data including total revenue, monthly order totals, and consecutive order tags.

## Testing
To test the output data:
1. Run the provided `testing.py` script to convert the Parquet outputs into CSV and XLSX formats.
2. Ensure that the processed data matches the expected structure and content.

## Requirements
- Python 3.7 or higher
- Apache Spark
- PySpark
- Pandas
- Requests

