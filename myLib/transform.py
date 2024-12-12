# transform
import os
from pyspark.sql import SparkSession

def transform_data(database, raw_table, transformed_table, output_dbfs_path):
    """
    Transform airline safety data with simple transformations.

    Args:
        database (str): Databricks database.
        raw_table (str): Name of the raw data table.
        transformed_table (str): Name of the transformed table.
        output_dbfs_path (str): DBFS path to save the transformed data as CSV.

    Returns:
        None
    """
    try:
        # Initialize SparkSession
        spark = SparkSession.builder.getOrCreate()

        # Check if the raw table exists
        if not spark.catalog.tableExists(f"{database}.{raw_table}"):
            print(
                f"Error: Table {database}.{raw_table} does not exist. "
                "Transformation aborted."
            )
            return

        # Generate simplified SQL query
        query = f"""
        CREATE OR REPLACE TABLE {database}.{transformed_table} AS
        SELECT 
            airline,
            avail_seat_km_per_week,
            
            -- Total incidents across both periods
            (incidents_85_99 + incidents_00_14) AS total_incidents,
            
            -- Total fatalities across both periods
            (fatalities_85_99 + fatalities_00_14) AS total_fatalities,
            
            -- Retain original period columns for reference
            incidents_85_99,
            fatal_accidents_85_99,
            fatalities_85_99,
            incidents_00_14,
            fatal_accidents_00_14,
            fatalities_00_14
        FROM {database}.{raw_table}
        """
        
        # Execute the query
        spark.sql(query)
        print(
            f"Transformation successful: Data saved to "
            f"{database}.{transformed_table}"
        )

        # Save the transformed data to DBFS in CSV format
        print(f"Loading transformed data from {database}.{transformed_table}...")
        transformed_df = spark.table(f"{database}.{transformed_table}")

        print(f"Saving transformed data to DBFS at {output_dbfs_path} (CSV format)...")
        transformed_df.write.csv(output_dbfs_path, mode="overwrite", header=True)
        print(f"Transformed data successfully saved to DBFS as CSV: {output_dbfs_path}")

    except Exception as e:
        print(f"Transformation failed: {e}")
        raise

if __name__ == "__main__":
    # Define database, table names, and output path
    database_name = "mh720_week11"
    raw_table = "airline_safety"
    transformed_table = "airline_safety_transformed"
    output_path = "/dbfs/mh720_week11/airline_safety_transformed"

    # Execute the transformation
    transform_data(database_name, raw_table, transformed_table, output_path)
