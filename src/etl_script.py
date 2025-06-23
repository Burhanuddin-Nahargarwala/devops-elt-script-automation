import requests
import pandas as pd
from pyspark.sql import SparkSession

from transformations import transform_data

# --- 1. Extraction ---
def fetch_data_from_api(api_url: str) -> list:
    """Fetches raw user data from the specified API."""
    print(f"Fetching data from {api_url}...")
    response = requests.get(api_url)
    response.raise_for_status()  # This will raise an error for a bad response (e.g., 404, 500)
    print("Data fetched successfully.")
    return response.json()

# --- 3. Loading ---
def load_data_to_delta(spark: SparkSession, df: pd.DataFrame, table_name: str):
    """Converts the Pandas DataFrame to a Spark DataFrame and saves it as a Delta table."""
    print(f"Loading data into Delta table: {table_name}...")
    # Create a Spark DataFrame from the Pandas DataFrame
    spark_df = spark.createDataFrame(df)
    
    # Write the data to a Delta table, overwriting any existing data.
    # This makes the job idempotent (rerunnable).
    spark_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print("Data loaded successfully.")

# --- Main execution block ---
if __name__ == "__main__":
    # Configuration
    API_URL = "https://jsonplaceholder.typicode.com/users"
    TABLE_NAME = "default.users_prod" # The table will be created in the 'default' database

    # Initialize Spark Session (this is provided by the Databricks environment)
    spark = SparkSession.builder.appName("API_to_Delta_ETL").getOrCreate()

    # Run the ETL pipeline
    raw_json_data = fetch_data_from_api(API_URL)
    transformed_pandas_df = transform_data(raw_json_data)
    load_data_to_delta(spark, transformed_pandas_df, TABLE_NAME)

    print("ETL job completed successfully!")