"""
This script defines the DataCleaner class used to read, clean, and perform data quality checks with PySpark.

Approach:
- A Spark session is initialized to process the data.
- Data is read from a CSV file into a PySpark DataFrame.
- The cleaning process involves:
    1. Filling missing values for important columns like 'CustomerID', 'Quantity', and 'UnitPrice'.
    2. Converting 'InvoiceDate' to a proper date format for time-based analysis.
    3. Removing duplicate rows to ensure data quality.

Assumptions:
- Missing 'CustomerID' is filled with 'Unknown' as it is critical for customer-related analysis.
- Missing 'Quantity' and 'UnitPrice' are filled with zeros to avoid affecting sales calculations.
- The CSV files have headers and proper schema inference is applied during reading.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

class DataCleaner:
    def __init__(self, input_filepath):
        self.input_filepath = input_filepath

        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("DataCleaning") \
            .getOrCreate()

        # Set log level to ERROR to suppress unnecessary warnings
        self.spark.sparkContext.setLogLevel("ERROR")

    def read_csv(self):
        # Read the CSV file into a PySpark DataFrame
        return self.spark.read.csv(self.input_filepath, header=True, inferSchema=True)

    def clean_data(self, df):
        # Handle any null values
        df = df.fillna({
            'CustomerID': 'Unknown',
            'Quantity': 0,
            'UnitPrice': 0.0
        })
        # Convert 'InvoiceDate' to proper date format
        df = df.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "MM/dd/yyyy HH:mm"))
        # Remove any duplicate rows
        df = df.dropDuplicates()
        return df

    def show_summary(self, df):
        # Display the schema and a few rows for quality check
        df.printSchema()
        df.show(5)



def create_global_temporary_view(df):
    """
    Create a global temporary view that can be accessed across different Spark sessions.
    """
    df.createOrReplaceGlobalTempView("cleaned_data")

if __name__ == "__main__":
    input_file = "../../data/processed/online_retail.csv"
    cleaner = DataCleaner(input_file)
    raw_df = cleaner.read_csv()
    cleaned_df = cleaner.clean_data(raw_df)
    cleaner.show_summary(cleaned_df)

    # Create a global temporary view for the cleaned DataFrame
    create_global_temporary_view(cleaned_df)
