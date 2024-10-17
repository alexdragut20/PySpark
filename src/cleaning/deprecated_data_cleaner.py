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

    def read_csv(self, header=True, inferSchema=True):
        """
        Read the CSV file into a PySpark DataFrame.
        :param header: Boolean, whether the CSV file has headers
        :param inferSchema: Boolean, whether to infer the schema automatically
        :return: PySpark DataFrame
        """
        return self.spark.read.csv(self.input_filepath, header=header, inferSchema=inferSchema)

    def clean_data(self, df, fillna_dict=None, date_col=None, date_format="MM/dd/yyyy HH:mm"):
        """
        Clean the DataFrame by filling missing values, converting date columns, and removing duplicates.
        :param df: PySpark DataFrame to clean
        :param fillna_dict: Dictionary of column names and their corresponding fillna values
        :param date_col: Name of the date column to be converted
        :param date_format: Date format to use for conversion (default is "MM/dd/yyyy HH:mm")
        :return: Cleaned PySpark DataFrame
        """
        # Fill missing values based on the provided dictionary
        if fillna_dict:
            df = df.fillna(fillna_dict)

        # Convert date column if provided
        if date_col:
            df = df.withColumn(date_col, to_date(col(date_col), date_format))

        # Remove duplicates
        df = df.dropDuplicates()
        return df

    def show_summary(self, df):
        """
        Display the schema and a few rows for a quality check.
        :param df: PySpark DataFrame
        """
        df.printSchema()
        df.show(5)


def create_global_temporary_view(df, view_name="cleaned_data"):
    """
    Create a global temporary view that can be accessed across different Spark sessions.
    :param df: PySpark DataFrame to create a view from
    :param view_name: Name of the global temporary view (default is "cleaned_data")
    """
    df.createOrReplaceGlobalTempView(view_name)


if __name__ == "__main__":
    input_file = "../../data/processed/online_retail.csv"

    # Initialize the DataCleaner class
    cleaner = DataCleaner(input_file)

    # Read the raw CSV data
    raw_df = cleaner.read_csv()

    # Define a dictionary for filling missing values
    fillna_values = {
        'CustomerID': 'Unknown',
        'Quantity': 0,
        'UnitPrice': 0.0
    }

    # Clean the data by specifying the columns to clean and the date column format
    cleaned_df = cleaner.clean_data(raw_df, fillna_dict=fillna_values, date_col="InvoiceDate")

    # Show the summary of the cleaned data
    cleaner.show_summary(cleaned_df)

    # Create a global temporary view for the cleaned DataFrame
    create_global_temporary_view(cleaned_df)
