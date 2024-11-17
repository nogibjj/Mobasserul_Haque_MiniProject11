from pyspark.sql import SparkSession 
import pandas as pd 
import re 
 
def extract(table_name, database="Airline_Safety"): 
    """ 
    Extract airline safety data from a predefined URL, clean, and save to Delta table. 
 
    Args: 
        table_name (str): Name of the Delta table to create. 
        database (str): Databricks database. 
 
    Returns: 
        None 
    """ 
    # Define the URL for the dataset 
    url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv" 
 
    # Initialize SparkSession (Databricks environment) 
    spark = SparkSession.builder.getOrCreate() 
 
    # Load CSV using pandas 
    df = pd.read_csv(url) 
     
    # Clean column names: replace invalid characters with underscores 
    df.columns = [ 
        re.sub(r"[^\w]", "_", col.strip()).replace("__", "_").lower()  
        for col in df.columns 
    ] 
 
    # Convert pandas DataFrame to Spark DataFrame 
    spark_df = spark.createDataFrame(df) 
 
    # Ensure the target database exists 
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}") 
 
    # Write Spark DataFrame to Delta table 
    spark_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{database}.{table_name}") 
     
    print(f"Airline safety data successfully extracted and saved to {database}.{table_name}")

def main():
    # Example usage 
    extract("airline_safety")

if __name__ == "__main__":
    main()