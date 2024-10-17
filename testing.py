import os
import pandas as pd
from pyspark.sql import SparkSession

def convert_parquet_to_xlsx(parquet_folder, output_xlsx_folder):
    """
    Convert Parquet files to XLSX format.
    :param parquet_folder: The folder containing Parquet files.
    :param output_xlsx_folder: The folder to save the converted XLSX files.
    """
    # Initialize Spark session
    spark = SparkSession.builder.appName("ParquetToXLSX").getOrCreate()

    # Ensure the output directory exists
    os.makedirs(output_xlsx_folder, exist_ok=True)

    # List all OrderMonth folders in the parquet_folder
    for order_month_folder in os.listdir(parquet_folder):
        folder_path = os.path.join(parquet_folder, order_month_folder)
        if os.path.isdir(folder_path):
            print(f"Converting Parquet files in {folder_path} to XLSX...")

            # Read Parquet files
            parquet_df = spark.read.parquet(folder_path)

            # Convert to Pandas DataFrame and save as XLSX
            xlsx_file_path = os.path.join(output_xlsx_folder, f"{order_month_folder}.xlsx")
            parquet_df.toPandas().to_excel(xlsx_file_path, index=False)

            print(f"Saved XLSX file to {xlsx_file_path}")

    print("Conversion from Parquet to XLSX completed.")
    spark.stop()


def convert_xlsx_to_csv(xlsx_folder, output_csv_folder):
    """
    Convert XLSX files to CSV format.
    :param xlsx_folder: The folder containing XLSX files.
    :param output_csv_folder: The folder to save the converted CSV files.
    """
    # Ensure the output directory exists
    os.makedirs(output_csv_folder, exist_ok=True)

    # List all XLSX files in the xlsx_folder
    for xlsx_file in os.listdir(xlsx_folder):
        if xlsx_file.endswith(".xlsx"):
            xlsx_path = os.path.join(xlsx_folder, xlsx_file)
            csv_file_name = xlsx_file.replace(".xlsx", ".csv")
            csv_path = os.path.join(output_csv_folder, csv_file_name)

            print(f"Converting {xlsx_file} to CSV...")

            # Read the XLSX file and convert it to CSV
            df = pd.read_excel(xlsx_path)
            df.to_csv(csv_path, index=False)

            print(f"Saved CSV file to {csv_path}")

    print("Conversion from XLSX to CSV completed.")


if __name__ == "__main__":
    # Define the paths
    parquet_folder = "data/processed/final_output"
    output_xlsx_folder = "data/processed/final_output_xlsx"
    output_csv_folder = "data/processed/final_output_csv"

    # Step 1: Convert Parquet to XLSX
    convert_parquet_to_xlsx(parquet_folder, output_xlsx_folder)

    # Step 2: Convert XLSX to CSV
    convert_xlsx_to_csv(output_xlsx_folder, output_csv_folder)
