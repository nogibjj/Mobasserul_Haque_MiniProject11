from extract import extract
from transform import transform_data
from load import load_data
from query import filter_and_save_data

def main():
    """ 
    Main function to orchestrate the ETL pipeline for 
    Airline Safety data on Databricks using DBFS. 
    """
    # Configurations
    database_name = "mh720_week11"  # Databricks database name
    source_table_name = "airline_safety"  # Source table name after extraction
    transformed_table_name = "airline_safety_transformed"  # Transformed table name
    final_table_name = "airline_safety_filtered"  # Final filtered table name
    dbfs_file_path = (
        "dbfs:/mnt/data/airline-safety.csv"  # DBFS path for raw data
    )

    try:
        # Step 1: Upload CSV to DBFS
        print("Uploading raw CSV to DBFS...")
        raw_data_url = (
            "https://raw.githubusercontent.com/fivethirtyeight/data/"
            "master/airline-safety/airline-safety.csv"
        )
        upload_file_to_dbfs(raw_data_url, dbfs_file_path)
        print("Raw CSV uploaded to DBFS.")

        # Step 2: Extract data from DBFS
        print("Starting data extraction...")
        extract(source_table_name, database=database_name)
        print("Data extraction completed.")

        # Step 3: Transform data
        print("Starting data transformation...")
        transform_data(
            database_name, 
            source_table_name, 
            transformed_table_name, 
            "/dbfs/mh720_week11/airline_safety_transformed"
        )
        print("Data transformation completed.")

        # Step 4: Query/Filter data
        print("Starting data filtering...")
        filter_and_save_data(
            database_name, 
            transformed_table_name, 
            final_table_name
        )
        print("Data filtering completed.")

        # Step 5: Load and display data
        print("Starting data load...")
        load_data(database_name, final_table_name)
        print("Data load completed.")

    except Exception as e:
        print(f"ETL pipeline failed: {e}")
        raise

def upload_file_to_dbfs(source_url, dbfs_path):
    """ 
    Uploads a file from a URL to DBFS.

    Args:
        source_url (str): URL of the file to upload.
        dbfs_path (str): DBFS destination path.

    Returns:
        None 
    """
    import requests
    with requests.get(source_url, stream=True) as r:
        if r.status_code == 200:
            with open("/dbfs" + dbfs_path[4:], "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        else:
            raise Exception(f"Failed to download file from {source_url}.")

if __name__ == "__main__":
    main()