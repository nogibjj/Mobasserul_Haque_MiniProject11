from pyspark.sql import SparkSession

def filter_and_save_data(database, source_table, target_table):
    """
    Filter specific columns from the airline safety table and save to a new Delta table.

    Args:
        database (str): Databricks database name.
        source_table (str): Source table to filter data from.
        target_table (str): Target table name to save filtered data.

    Returns:
        None
    """
    # Initialize SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Check if the source table exists
    if not spark.catalog.tableExists(f"{database}.{source_table}"):
        print(f"Error: Table {database}.{source_table} does not exist.")
        return

    # Define the columns to select
    selected_columns = [
        "airline",
        "avail_seat_km_per_week",
        "total_incidents",
        "total_fatalities",
        "fatal_accidents_85_99",
        "fatal_accidents_00_14"
    ]

    # Generate SQL query to select only the specified columns
    query = (
        f"CREATE OR REPLACE TABLE {database}.{target_table} AS "
        f"SELECT {', '.join(selected_columns)} "
        f"FROM {database}.{source_table}"
    )

    # Execute the query
    try:
        print(
            f"Executing query to filter and save data to "
            f"{database}.{target_table}..."
        )
        spark.sql(query)
        print(
            f"Filtered data saved successfully to "
            f"{database}.{target_table}."
        )
    except Exception as e:
        print(f"Failed to filter and save data: {e}")
        raise

if __name__ == "__main__":
    # Define database and table names
    database_name = "mh720_week11"
    source_table = "airline_safety_transformed"
    target_table = "airline_safety_filtered"

    # Execute the filter and save operation
    filter_and_save_data(database_name, source_table, target_table)