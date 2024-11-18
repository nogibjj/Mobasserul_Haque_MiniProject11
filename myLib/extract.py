import os
import requests
import pandas as pd


def extract(
    url="https://raw.githubusercontent.com/fivethirtyeight/data/master/college-majors/grad-students.csv",
    url_2="https://raw.githubusercontent.com/fivethirtyeight/data/master/college-majors/recent-grads.csv",
    file_path="data/grad-students.csv",
    file_path_2="data/recent-grads.csv"):
    
    if not os.path.exists("data"):
        os.makedirs("data")
    
    try:
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(r.content)
        else:
            print(f"Failed to download file from {url}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")
    
    try:
        r = requests.get(url_2, stream=True)
        if r.status_code == 200:
            with open(file_path_2, "wb") as f:
                f.write(r.content)
        else:
            print(f"Failed to download file from {url_2}")
    except Exception as e:
        print(f"Error downloading {url_2}: {e}")
    
    # Read the files and get the first 100 rows
    try:
        df = pd.read_csv(file_path)
        df_2 = pd.read_csv(file_path_2)
        
        df_subset = df.head(100)
        df_subset_2 = df_2.head(100)
        
        # Overwrite the files with the subset of data
        df_subset.to_csv(file_path, index=False)
        df_subset_2.to_csv(file_path_2, index=False)
        
        return file_path, file_path_2

    except Exception as e:
        print(f"Error processing the CSV files: {e}")
        return None, None


file_path, file_path_2 = extract()
