# PySpark
=======
# Project Overview

This project handles the ingestion, conversion, and initial cleaning of the "Online Retail" dataset from the UCI Machine Learning Repository.

### Requirements:
Ensure you have installed the required libraries by running:
```
pip install -r requirements.txt
```

## Step 1: Data Ingestion

The first step is to download the "Online Retail" dataset from the UCI Machine Learning Repository using a Python script.

### Usage:
1. Navigate to the `src/ingestion/` directory:
```
cd src/ingestion
```
2. Run the `download_data.py` script:
```
python download_data.py
```

This will download the dataset and save it as `data/raw/online_retail.xlsx`.

## Step 2: Convert Excel to CSV

The downloaded Excel file is then converted to CSV using the `DataConverter` class in the `src/ingestion/data_converter.py` script.

### Usage:
1. Navigate to the `src/ingestion/` directory:
```
cd src/ingestion
```
2. Run the `data_converter.py` script:
```
python data_converter.py
```

This will convert the Excel file located at `data/raw/online_retail.xlsx` to a CSV file and save it in `data/processed/online_retail.csv`.

### Notes:
1. **OOP Structure**: The `DataConverter` class is designed for flexibility and future enhancements. New methods can be added to handle different file formats or more complex conversions.
2. **Best Practices**: The code checks for the existence of files, provides error handling, and uses `pandas` for conversion.

## Step 3: Data Cleaning and Quality Checks

In this step, the CSV file is read using **PySpark** for initial data cleaning and quality checks:
- **Handling null values**: Missing values for `CustomerID` are filled with `Unknown`, while `Quantity` and `UnitPrice` are filled with zeros.
- **Date conversion**: The `InvoiceDate` column is converted to a proper date format for easier time-based analysis.
- **Duplicate removal**: Duplicate rows are removed to ensure the data is clean.

### Usage:
1. Navigate to the `src/cleaning/` directory:
```
cd src/cleaning
```
2. Run the `data_cleaner.py` script:
```
python data_cleaner.py
```

This script reads the CSV file, cleans the data, and outputs a summary, including the schema and the first few rows of the cleaned DataFrame.

### Notes:
1. The script **does not save** the cleaned data to a file. Instead, it prints a summary to verify the cleaning steps.
2. The focus is on ensuring data quality by handling missing values, converting data types, and removing duplicates.

### Data Cleaning Decisions:
- **Null Handling**: `CustomerID` is critical for customer analysis, so `Unknown` is used as a placeholder for missing values. Missing values in numeric fields (`Quantity`, `UnitPrice`) are replaced with zeros.
- **Date Conversion**: The `InvoiceDate` is converted to a proper date type to ensure it can be used in time-based analysis.
- **Duplicate Removal**: Duplicate rows can lead to inaccuracies in data analysis, so they are removed.

