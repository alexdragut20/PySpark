from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, to_date, round, month, year
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


class DataCleaner:
    def __init__(self, input_filepath):
        self.input_filepath = input_filepath
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("DataCleaningAndProcessing") \
            .getOrCreate()
        # Set log level to ERROR to suppress unnecessary warnings
        self.spark.sparkContext.setLogLevel("ERROR")

    def read_csv(self):
        # Read the CSV file into a PySpark DataFrame
        return self.spark.read.csv(self.input_filepath, header=True, inferSchema=True)

    def clean_data(self, df):
        # First cast 'CustomerID' to IntegerType to remove .0 from double type
        df = df.withColumn("CustomerID", col("CustomerID").cast(IntegerType()))

        # Then cast 'CustomerID' to string (to handle 'Unknown')
        df = df.withColumn("CustomerID", col("CustomerID").cast("string"))

        # Fill null values in CustomerID with 'Unknown', Quantity with 0, and UnitPrice with 0.0
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
        # Display schema and a few rows for quality check
        df.printSchema()
        df.show(5)


class DataProcessor:
    def __init__(self, df):
        self.df = df

    def calculate_total_revenue_per_customer(self):
        """
        Calculate the total revenue per customer and return the top 5 customers by total revenue.
        Revenue is calculated as Quantity * UnitPrice and rounded to 2 decimal places.
        """
        try:
            # Calculate revenue per row (Quantity * UnitPrice)
            revenue_df = self.df.withColumn("Revenue", col("Quantity") * col("UnitPrice"))

            # Calculate total revenue per customer and round to 2 decimal places
            customer_revenue = revenue_df.groupBy("CustomerID").agg(
                round(spark_sum("Revenue"), 2).alias("TotalRevenue")
            )

            # Identify top 5 customers by total revenue
            top_5_customers = customer_revenue.orderBy(col("TotalRevenue").desc()).limit(5)
            top_5_customers.show()

        except Exception as e:
            print(f"Error in calculating total revenue per customer: {e}")

    def most_popular_product_per_country(self):
        """
        Determine the most popular product (by quantity sold) for each country.
        """
        try:
            # Group by StockCode (Product) and Country, and calculate the total quantity sold
            product_popularity = self.df.groupBy("StockCode", "Country").agg(
                spark_sum("Quantity").alias("TotalQuantitySold")
            )

            # Use a window to rank products by total quantity sold for each country
            window_spec = Window.partitionBy("Country").orderBy(col("TotalQuantitySold").desc())
            ranked_products = product_popularity.withColumn("Rank", row_number().over(window_spec))

            # Filter to get the most popular product for each country (Rank = 1)
            most_popular_products = ranked_products.filter(col("Rank") == 1).drop("Rank")
            most_popular_products.show()

        except Exception as e:
            print(f"Error in determining the most popular product: {e}")

    def calculate_monthly_sales_trend(self):
        """
        Calculate the monthly sales trend (total revenue per month).
        """
        try:
            # Calculate revenue per row (Quantity * UnitPrice)
            revenue_df = self.df.withColumn("Revenue", col("Quantity") * col("UnitPrice"))

            # Extract year and month from InvoiceDate and group by them
            monthly_sales = revenue_df.groupBy(year("InvoiceDate").alias("Year"), month("InvoiceDate").alias("Month")).agg(
                round(spark_sum("Revenue"), 2).alias("TotalRevenue")
            ).orderBy("Year", "Month")

            # Show the monthly sales trend
            monthly_sales.show()

        except Exception as e:
            print(f"Error in calculating monthly sales trend: {e}")


if __name__ == "__main__":
    # File paths
    input_file = "../../data/processed/online_retail.csv"

    # Initialize DataCleaner and clean the data
    cleaner = DataCleaner(input_file)
    raw_df = cleaner.read_csv()
    cleaned_df = cleaner.clean_data(raw_df)
    cleaner.show_summary(cleaned_df)

    # Initialize DataProcessor and process the cleaned data
    processor = DataProcessor(cleaned_df)

    # Task 1.2(a): Calculate total revenue per customer
    processor.calculate_total_revenue_per_customer()

    # Task 1.2(b): Determine the most popular product by quantity sold for each country
    processor.most_popular_product_per_country()

    # Task 1.2(c): Calculate the monthly sales trend
    processor.calculate_monthly_sales_trend()

    # Stop the Spark session
    cleaner.spark.stop()
