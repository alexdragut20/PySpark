from src.transformation.data_processing import DataCleaner, DataProcessor
import logging
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import os
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def save_as_single_csv(df, output_file):
    """
    Save the given DataFrame as a single CSV file with proper naming.
    :param df: PySpark DataFrame to be saved.
    :param output_file: Output file path (excluding ".csv").
    This approach was used because the basic use of saving to .csv was generating more useless files.
    """
    try:
        # Coalesce the DataFrame into a single partition
        logging.info(f"Saving DataFrame to {output_file}.csv as a single CSV file...")
        df.coalesce(1).write.mode("overwrite").csv(output_file, header=True)

        # Find the part file that was written
        dir_path = f"{output_file}"
        files = os.listdir(dir_path)

        # Find the part-00000 file and rename it
        for file_name in files:
            if file_name.startswith("part-") and file_name.endswith(".csv"):
                os.rename(f"{dir_path}/{file_name}", f"{output_file}.csv")

        # Remove unnecessary files and directories
        shutil.rmtree(dir_path)
        logging.info(f"Saved DataFrame as {output_file}.csv successfully.")

    except Exception as e:
        logging.error(f"Error saving DataFrame as a single CSV: {e}")
        raise


def etl_pipeline(input_file, output_path):
    """
    The ETL pipeline to read, clean, transform, and process data, then save the final output.
    :param input_file: Path to the input CSV file.
    :param output_path: Path to save the final output.
    """
    # Initialize DataCleaner and clean the data
    logging.info("Starting the ETL pipeline...")

    cleaner = DataCleaner(input_file)
    try:
        raw_df = cleaner.read_csv()
        cleaned_df = cleaner.clean_data(raw_df, customer_id_col="CustomerID", invoice_date_col="InvoiceDate")
        logging.info("Data cleaning completed successfully.")
    except Exception as e:
        logging.error(f"Error during data cleaning: {e}")
        raise

    # Initialize DataProcessor with the cleaned DataFrame
    processor = DataProcessor(cleaned_df)

    # Calculate total revenue per customer and save top 5 customers in a separate .csv file
    try:
        logging.info("Calculating total revenue per customer...")
        customer_revenue_df = processor.calculate_total_revenue_per_customer(
            customer_id_col="CustomerID", quantity_col="Quantity", unit_price_col="UnitPrice"
        )
        save_as_single_csv(customer_revenue_df.orderBy(col("TotalRevenue").desc()),
                           f"{output_path}/total_revenue_per_customer")
    except Exception as e:
        logging.error(f"Error calculating total revenue per customer: {e}")
        raise

    # Determine the most popular product by quantity sold for each country and save in a separate .csv file
    try:
        logging.info("Determining the most popular product by quantity sold for each country...")
        most_popular_product_df = processor.most_popular_product_per_country(product_col="StockCode",
                                                                             country_col="Country",
                                                                             quantity_col="Quantity")
        save_as_single_csv(most_popular_product_df.orderBy("Country"),
                           f"{output_path}/most_popular_product_per_country")
    except Exception as e:
        logging.error(f"Error determining the most popular product: {e}")
        raise

    # Calculate the monthly sales trend and save in a separate .csv file
    try:
        logging.info("Calculating the monthly sales trend...")
        monthly_sales_df = processor.calculate_monthly_sales_trend(invoice_date_col="InvoiceDate",
                                                                   quantity_col="Quantity",
                                                                   unit_price_col="UnitPrice")
        save_as_single_csv(monthly_sales_df, f"{output_path}/monthly_sales_trend")
    except Exception as e:
        logging.error(f"Error calculating monthly sales trend: {e}")
        raise

    # Create a new column 'TotalAmount' and keeping in the main df
    try:
        logging.info("Creating 'TotalAmount' column by multiplying 'Quantity' and 'UnitPrice'...")
        processor.create_total_amount_column(quantity_col="Quantity", unit_price_col="UnitPrice")
    except Exception as e:
        logging.error(f"Error creating 'TotalAmount' column: {e}")
        raise

    # Create a new column 'OrderMonth' and keeping in the main df
    try:
        logging.info("Creating 'OrderMonth' column extracted from 'InvoiceDate'...")
        processor.create_order_month_column(invoice_date_col="InvoiceDate")
    except Exception as e:
        logging.error(f"Error creating 'OrderMonth' column: {e}")
        raise

    # Calculate the monthly order total for each customer and save in a separate .csv file
    try:
        logging.info("Calculating monthly order total for each customer...")
        monthly_order_total_df = processor.calculate_monthly_order_total(customer_id_col="CustomerID")
        save_as_single_csv(monthly_order_total_df, f"{output_path}/monthly_order_total")
    except Exception as e:
        logging.error(f"Error calculating monthly order total: {e}")
        raise

    # Identify customers with consecutive orders and save in a separate .csv file
    try:
        logging.info("Identifying customers who placed orders in consecutive months...")
        consecutive_customers_df = processor.identify_customers_with_consecutive_orders(customer_id_col="CustomerID")
        save_as_single_csv(consecutive_customers_df, f"{output_path}/customers_consecutive_orders")
    except Exception as e:
        logging.error(f"Error identifying customers with consecutive orders: {e}")
        raise

    # Combine into one CSV (customerid, totalrevenue, monthly order total, consecutive order tag [true|false] )
    try:
        logging.info("Combining customer data into one CSV...")

        # Rename 'OrderMonth' in one of the DataFrames to avoid column name conflict during the join
        # to be reviewed
        monthly_order_total_df = monthly_order_total_df.withColumnRenamed("OrderMonth", "MonthlyOrder")

        combined_df = customer_revenue_df.alias('a') \
            .join(monthly_order_total_df.alias('b'), on="CustomerID", how="left") \
            .join(consecutive_customers_df.alias('c'), on="CustomerID", how="left") \
            .withColumn("ConsecutiveOrder",
                        F.when(col("PreviousOrderMonth").isNotNull(), F.lit(True)).otherwise(F.lit(False)))

        save_as_single_csv(combined_df, f"{output_path}/combined_customer_data")
    except Exception as e:
        logging.error(f"Error combining customer data: {e}")
        raise

    logging.info("ETL pipeline completed successfully.")


if __name__ == "__main__":
    input_file = "data/processed/online_retail.csv"
    output_path = "data/processed/final_output"

    etl_pipeline(input_file, output_path)
