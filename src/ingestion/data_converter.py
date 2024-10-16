"""
This script defines the DataConverter class, which converts an Excel file to a CSV file.

Approach:
- The script reads an Excel file using pandas and converts it to a CSV file.
- File paths are passed during initialization.
- Basic error handling is included to manage missing files.

Assumptions:
- The input file is an Excel file, and pandas will handle the file reading.
- The output file is expected to be in CSV format.
"""
import os
import pandas as pd

class DataConverter:
    def __init__(self, input_filepath, output_filepath):
        self.input_filepath = input_filepath
        self.output_filepath = output_filepath

    def convert_excel_to_csv(self):
        try:
            # Check if input file exists
            if not os.path.exists(self.input_filepath):
                raise FileNotFoundError(f"Input file not found: {self.input_filepath}")

            # Read Excel file
            data = pd.read_excel(self.input_filepath)

            # Convert to CSV and save
            data.to_csv(self.output_filepath, index=False)
            print(f"File successfully converted to CSV and saved as {self.output_filepath}")
        except Exception as e:
            print(f"Error during conversion: {e}")

# Define file paths
input_file = "../../data/raw/online_retail.xlsx"
output_file = "../../data/processed/online_retail.csv"

if __name__ == "__main__":
    converter = DataConverter(input_file, output_file)
    converter.convert_excel_to_csv()
