"""Query the database from a db connection to Azure Databricks"""
import os
from databricks import sql
from dotenv import load_dotenv


LOG_FILE = "query_log.md"


def add_to_markdown(query, result="none"):
    """adds to a query markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")
        file.write(f"```response from databricks\n{result}\n```\n\n")


def query(query):
    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")
    
    try:
        with sql.connect(
            server_hostname=server_h,
            http_path=http_path,
            access_token=access_token,
        ) as connection:
            c = connection.cursor()
            c.execute(query)
            result = c.fetchall()
        c.close()
        add_to_markdown(f"{query}", result)
        return result  # Return the result for further validation in tests
    except Exception as e:
        print(f"Error executing query: {e}")  # Print error message
        add_to_markdown(f"{query}", str(e))  # Log the error
        raise  # Re-raise the exception for visibility in tests
