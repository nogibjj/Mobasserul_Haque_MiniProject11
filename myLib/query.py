from pyspark.sql import SparkSession

# Initialize Spark session if it's not already initialized
spark = SparkSession.builder \
    .appName("Airline Safety Query App") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .getOrCreate()

# Markdown file to log the SQL functions and queries
def logQuery(query):
    with open("output_Log.md", "a") as file:
        file.write(f"```sql\n{query}\n```\n")

# Register the Delta table as a temporary view
delta_table_path = "dbfs:/FileStore/mh720_week11/mh720_week11_airline_safety_delta_table"
spark.read.format("delta").load(delta_table_path).createOrReplaceTempView("mh720_week11_airline_safety_delta_table")

# Define and run SQL queries on the registered table
def queryData():
    # Example query: Top airlines with the highest total fatalities
    query = """
        SELECT airline, total_fatalities
        FROM mh720_week11_airline_safety_delta_table
        ORDER BY total_fatalities DESC
        LIMIT 5
    """
    # Log the query
    logQuery(query)

    # Execute the query
    query_result = spark.sql(query)
    query_result.show()

# Additional example queries
def filterAirlinesWithIncidents():
    # Query to filter airlines with more than 10 total incidents
    query = """
        SELECT airline, total_incidents
        FROM mh720_week11_airline_safety_delta_table
        WHERE total_incidents > 10
        ORDER BY total_incidents DESC
    """
    # Log the query
    logQuery(query)

    # Execute the query
    query_result = spark.sql(query)
    query_result.show()

# Main execution
if __name__ == "__main__":
    print("Querying data from the airline safety dataset...\n")

    # Example query executions
    queryData()
    filterAirlinesWithIncidents()
