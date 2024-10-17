from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, to_date, round, month, year
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lag
from pyspark.sql import functions as F
import logging

# logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataCleaner:
    def __init__(self, input_filepath):
        """
        Initialize the DataCleaner with the input file path.
        :param input_filepath: Path to the CSV file containing the raw data.
        """
        self.input_filepath = input_filepath
        # Initializing spark
        # TBD config file
        self.spark = SparkSession.builder \
            .appName("DataCleaningAndProcessing") \
            .getOrCreate()
        # Set log level to ERROR to suppress unnecessary warnings - not doing what is supposed to
        self.spark.sparkContext.setLogLevel("ERROR")

    def read_csv(self):
        """
        Read the CSV file into a PySpark DataFrame.
        :return: PySpark DataFrame containing the raw data.
        """
        return self.spark.read.csv(self.input_filepath, header=True, inferSchema=True)

    def clean_data(self, df, customer_id_col, invoice_date_col):
        """
        Clean the data by removing duplicates, filling nulls, and casting CustomerID to the appropriate type.
        :param df: The input DataFrame
        :param customer_id_col: Name of the Customer ID column
        :param invoice_date_col: Name of the Invoice Date column
        :return: Cleaned DataFrame.
        """
        # Convert (Cast) CustomerID to IntegerType to remove .0 from DoubleType
        df = df.withColumn(customer_id_col, col(customer_id_col).cast(IntegerType()))

        # Convert (Cast) CustomerID to string to allow 'Unknown' value
        df = df.withColumn(customer_id_col, col(customer_id_col).cast("string"))

        # Fill null values
        df = df.fillna({
            customer_id_col: 'Unknown',
            'Quantity': 0,
            'UnitPrice': 0.0
        })

        # Convert InvoiceDate to proper date format
        df = df.withColumn(invoice_date_col, to_date(col(invoice_date_col), "MM/dd/yyyy HH:mm"))

        # Remove duplicates
        df = df.dropDuplicates()
        return df

    def show_summary(self, df):
        """
        Display the schema and a few rows for a quality check.
        :param df: DataFrame to show the summary of. (first 5)
        """
        df.printSchema()
        df.show(5)


class DataProcessor:
    def __init__(self, df):
        """
        Initialize the DataProcessor with the cleaned DataFrame.
        :param df: Cleaned DataFrame to process.
        """
        self.df = df

    def calculate_total_revenue_per_customer(self, customer_id_col, quantity_col, unit_price_col):
        """
        Calculate the total revenue per customer.
        :param customer_id_col: Name of the Customer ID column
        :param quantity_col: Name of the Quantity column
        :param unit_price_col: Name of the Unit Price column
        :return: DataFrame with total revenue per customer
        """
        # Calculate revenue per row (Quantity * UnitPrice)
        revenue_df = self.df.withColumn("Revenue", col(quantity_col) * col(unit_price_col))

        # Calculate total revenue per customer by grouping per customer_id and aggregating the Revenue values
        customer_revenue = revenue_df.groupBy(customer_id_col).agg(
            round(spark_sum("Revenue"), 2).alias("TotalRevenue")
        )

        return customer_revenue

    def most_popular_product_per_country(self, product_col, country_col, quantity_col):
        """
        Determine the most popular product by quantity sold for each country.
        :param product_col: Name of the Product column
        :param country_col: Name of the Country column
        :param quantity_col: Name of the Quantity column
        :return: DataFrame with the most popular product per country.
        """
        # Group by product and country, and calculate total quantity sold
        product_popularity = self.df.groupBy(product_col, country_col).agg(
            spark_sum(quantity_col).alias("TotalQuantitySold")
        )

        # Rank products by total quantity sold in each country
        window_spec = Window.partitionBy(country_col).orderBy(col("TotalQuantitySold").desc())
        ranked_products = product_popularity.withColumn("Rank", row_number().over(window_spec))

        # Filter for the most popular product in each country
        most_popular_products = ranked_products.filter(col("Rank") == 1).drop("Rank")

        return most_popular_products

    def calculate_monthly_sales_trend(self, invoice_date_col, quantity_col, unit_price_col):
        """
        Calculate the monthly sales trend.
        :param invoice_date_col: Name of the Invoice Date column
        :param quantity_col: Name of the Quantity column
        :param unit_price_col: Name of the Unit Price column
        :return: DataFrame with monthly sales trend.
        """
        # Calculate revenue per row (Quantity * UnitPrice)
        revenue_df = self.df.withColumn("Revenue", col(quantity_col) * col(unit_price_col))

        # Group by year and month of InvoiceDate and calculate total revenue
        monthly_sales = revenue_df.groupBy(year(col(invoice_date_col)).alias("Year"),
                                           month(col(invoice_date_col)).alias("Month")).agg(
            round(spark_sum("Revenue"), 2).alias("TotalRevenue")
        ).orderBy("Year", "Month")

        return monthly_sales

    def create_total_amount_column(self, quantity_col, unit_price_col):
        """
        Create a new column 'TotalAmount' by multiplying 'Quantity' and 'UnitPrice'.
        :param quantity_col: Name of the Quantity column
        :param unit_price_col: Name of the Unit Price column
        """
        self.df = self.df.withColumn("TotalAmount", col(quantity_col) * col(unit_price_col))

    def create_order_month_column(self, invoice_date_col):
        """
        Create a new column 'OrderMonth' extracted from the 'InvoiceDate'.
        :param invoice_date_col: Name of the Invoice Date column
        """
        self.df = self.df.withColumn("OrderMonth", F.date_format(col(invoice_date_col), "yyyy-MM"))

    def save_to_parquet(self, output_path, partition_by=None):
        """
        Save the processed DataFrame to Parquet format.
        :param output_path: Path to save the Parquet file.
        :param partition_by: Column(s) to partition the data by.
        """
        try:
            if partition_by:
                logging.info(f"Saving DataFrame to Parquet format at {output_path}, partitioned by {partition_by}")
                self.df.write.mode("overwrite").partitionBy(partition_by).parquet(output_path)
            else:
                logging.info(f"Saving DataFrame to Parquet format at {output_path}")
                self.df.write.mode("overwrite").parquet(output_path)
        except Exception as e:
            logging.error(f"Error saving DataFrame to Parquet: {e}")
            raise

    def calculate_monthly_order_total(self, customer_id_col):
        """
        Calculate the monthly order total for each customer.
        :param customer_id_col: Name of the Customer ID column
        :return: DataFrame with the monthly order total for each customer.
        """
        monthly_total = self.df.groupBy(customer_id_col, "OrderMonth").agg(
            round(spark_sum("TotalAmount"), 2).alias("MonthlyOrderTotal")
        ).orderBy(customer_id_col, "OrderMonth")

        return monthly_total

    def identify_customers_with_consecutive_orders(self, customer_id_col):
        """
        Identify customers who have placed orders in consecutive months.
        :param customer_id_col: Name of the Customer ID column
        :return: DataFrame with customers who placed consecutive monthly orders.
        """
        # Use windowing to compare each order with the previous one
        window_spec = Window.partitionBy(customer_id_col).orderBy("OrderMonth")

        # Lag the OrderMonth column and calculate the difference between consecutive months
        consecutive_orders_df = self.df.withColumn(
            "PreviousOrderMonth", F.lag("OrderMonth", 1).over(window_spec)
        )

        # Filter for consecutive months (PreviousOrderMonth + 1 month == OrderMonth)
        consecutive_orders = consecutive_orders_df.filter(
            (F.month(col("OrderMonth")) - F.month(col("PreviousOrderMonth")) == 1) |
            (F.year(col("OrderMonth")) > F.year(col("PreviousOrderMonth")))
        ).select(customer_id_col, "OrderMonth", "PreviousOrderMonth")

        return consecutive_orders




if __name__ == "__main__":
    input_file = "../../data/processed/online_retail.csv"

    # Initialize DataCleaner and clean the data
    print("Reading and cleaning data from the CSV file...")
    cleaner = DataCleaner(input_file)
    raw_df = cleaner.read_csv()

    # Apply data cleaning
    cleaned_df = cleaner.clean_data(raw_df, customer_id_col="CustomerID", invoice_date_col="InvoiceDate")
    print("Data has been cleaned successfully.")

    # Show summary of the cleaned data
    cleaner.show_summary(cleaned_df)

    # Initialize DataProcessor with the cleaned DataFrame
    processor = DataProcessor(cleaned_df)

    # Calculate total revenue per customer
    print("Calculating total revenue per customer...")
    customer_revenue_df = processor.calculate_total_revenue_per_customer(
        customer_id_col="CustomerID", quantity_col="Quantity", unit_price_col="UnitPrice"
    )

    # Display only the top 5 customers by total revenue
    print("Displaying top 5 customers by total revenue...")
    top_5_customers = customer_revenue_df.orderBy(col("TotalRevenue").desc()).limit(5)
    top_5_customers.show()

    # Determine the most popular product by quantity sold for each country
    print("Determining the most popular product by quantity sold for each country...")
    most_popular_product_df = processor.most_popular_product_per_country(
        product_col="StockCode", country_col="Country", quantity_col="Quantity"
    )
    most_popular_product_df.show()

    # Calculate the monthly sales trend
    print("Calculating the monthly sales trend...")
    monthly_sales_trend_df = processor.calculate_monthly_sales_trend(
        invoice_date_col="InvoiceDate", quantity_col="Quantity", unit_price_col="UnitPrice"
    )
    monthly_sales_trend_df.show()

    # Create a new column 'TotalAmount'
    print("Creating 'TotalAmount' column by multiplying 'Quantity' and 'UnitPrice'...")
    processor.create_total_amount_column(quantity_col="Quantity", unit_price_col="UnitPrice")

    # Show the first 5 rows of the DataFrame after adding 'TotalAmount'
    print("Displaying the first 5 rows after adding the 'TotalAmount' column...")
    processor.df.show(5)

    # Create a new column 'OrderMonth'
    print("Creating 'OrderMonth' column extracted from 'InvoiceDate'...")
    processor.create_order_month_column(invoice_date_col="InvoiceDate")

    # Show the first 5 rows of the DataFrame after adding 'OrderMonth'
    print("Displaying the first 5 rows after adding the 'OrderMonth' column...")
    processor.df.show(5)

    # Calculate the monthly order total for each customer
    print("Calculating the monthly order total for each customer...")
    monthly_order_total_df = processor.calculate_monthly_order_total(customer_id_col="CustomerID")
    monthly_order_total_df.show()

    # Identify customers who have placed orders in consecutive months
    print("Identifying customers who placed orders in consecutive months...")
    consecutive_orders_df = processor.identify_customers_with_consecutive_orders(customer_id_col="CustomerID")
    consecutive_orders_df.show()

    print("Stopping the Spark session...")
    cleaner.spark.stop()
