from mylib.extract import extractData
from mylib.transform import transformData, loadDataToDBFS, loadDataToDelta
from mylib.query import queryData
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

if __name__ == "__main__":
    # Initialize Spark session with Delta support
    spark = SparkSession.builder \
        .appName("Airline Safety ETL") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
        .getOrCreate()

    # Load environment variables
    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    headers = {'Authorization': f'Bearer {access_token}'}
    
    # Define paths
    dbfs_path = FILESTORE_PATH + "/airline-safety.csv" 
    local_file_path = "data/airline-safety.csv"
    dbfs_file_path = "dbfs:/FileStore/mh720_week11/airline-safety.csv"
    delta_table_path = "dbfs:/FileStore/mh720_week11/mh720_week11_airline_safety_delta_table"


    # Step 1: Extract data from the source
    print("Starting data extraction...")
    extractData(local_file_path)

    # Step 2: Load the data into Spark and transform it
    print("Transforming data...")
    transformed_data = transformData(spark, dbfs_file_path)

    # Step 3: Load data to DBFS
    print("Loading data to DBFS...")
    loadDataToDBFS(local_file_path, dbfs_file_path, headers)

    # Step 4: Load data into Delta Lake
    print("Loading data to Delta Lake...")
    loadDataToDelta(spark, dbfs_file_path, delta_table_path)

    # Step 5: Query the transformed data
    print("Querying the data...")
    queryData()
