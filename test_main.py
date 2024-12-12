import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SERVER_HOSTNAME = os.getenv("SERVER_HOSTNAME")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = os.getenv("FILESTORE_PATH", "dbfs:/FileStore/mini_project11")
BASE_URL = f"https://{SERVER_HOSTNAME}/api/2.0"

# Function to check if a DBFS path exists
def check_filestore_path(path):
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    try:
        response = requests.get(
            f"{BASE_URL}/dbfs/get-status?path={path}", headers=headers
        )
        response.raise_for_status()
        print(f"Path exists: {path}")
        return True  # Path exists if no exception is raised
    except requests.exceptions.RequestException as e:
        print(f"Error: Unable to access DBFS path '{path}'. Details: {e}")
        return False

# Test function
def test_databricks():
    print(f"Testing if DBFS path exists: {FILESTORE_PATH}")
    if check_filestore_path(FILESTORE_PATH):
        print(f"Test Passed: DBFS path '{FILESTORE_PATH}' exists.")
    else:
        print(f"Test Failed: DBFS path '{FILESTORE_PATH}' does not exist.")

if __name__ == "__main__":
    test_databricks()
