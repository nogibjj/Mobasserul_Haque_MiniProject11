from pyspark.sql.functions import col, sum, when
from pyspark.sql import SparkSession
import os
import base64
import json
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://" + server_h + "/api/2.0"

# Transform and clean the data using Spark
def transformData(spark, file_path):
    try:
        # Load the CSV file into a Spark DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"File loaded successfully: {file_path}")
    except Exception as e:
        print(f"Error loading file: {e}")
        return None

    # Add new columns for total incidents and total fatalities
    df_transformed = df.withColumn("total_incidents", col("incidents_85_99") + col("incidents_00_14")) \
                      .withColumn("total_fatalities", col("fatalities_85_99") + col("fatalities_00_14"))

    # Filter airlines with more than 10 total incidents
    df_filtered = df_transformed.filter(col("total_incidents") > 10)

    print("Data transformed successfully.")
    return df_filtered

def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request('POST', url + path, 
                           data=json.dumps(data), 
                           verify=True, 
                           headers=headers)
    return resp.json()

def loadDataToDBFS(pathLocal, pathDBFS, headers):
    if not os.path.exists(pathLocal):
        print(f"Error: The file {pathLocal} does not exist.")
        return

    # Open and read the file content
    with open(pathLocal, 'rb') as file:
        content = file.read()

    # Create the file in DBFS
    create_data = {'path': pathDBFS, 'overwrite': True}
    handle = perform_query('/dbfs/create', headers, data=create_data)['handle']

    # Upload content in chunks
    for i in range(0, len(content), 2**20):
        chunk = base64.standard_b64encode(content[i:i+2**20]).decode()
        perform_query('/dbfs/add-block', headers, data={'handle': handle, 'data': chunk})

    # Close the file handle
    perform_query('/dbfs/close', headers, data={'handle': handle})
    print(f"File {pathLocal} uploaded to {pathDBFS} successfully.")

def loadDataToDelta(spark, file_path, delta_table_path):
    try:
        # Load the CSV file from DBFS
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"File loaded successfully from: {file_path}")

        # Data transformation
        df_transformed = df.withColumn("total_incidents", col("incidents_85_99") + col("incidents_00_14")) \
                           .withColumn("total_fatalities", col("fatalities_85_99") + col("fatalities_00_14"))

        df_filtered = df_transformed.filter(col("total_incidents") > 10)

        # Write the DataFrame to Delta Lake
        df_filtered.write.format("delta").mode("overwrite").save(delta_table_path)
        print(f"Data successfully written to Delta Lake at: {delta_table_path}")

    except Exception as e:
        print(f"Error while loading data to Delta Lake: {e}")


