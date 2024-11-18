import os
from databricks import sql
import pandas as pd
from dotenv import load_dotenv

def load_data(recent_grads_path="data/recent-grads.csv", grad_students_path="data/grad-students.csv"):
    print("Loading data to Databricks...")  # Add this line

    # Load recent_grads.csv
    try:
        recent_grads = pd.read_csv(recent_grads_path, delimiter=",", skiprows=1)
        print("Successfully loaded recent_grads.csv")
    except Exception as e:
        print(f"Error loading recent_grads.csv: {e}")
        return "Error loading recent_grads.csv"

    # Load grad_students.csv
    try:
        grad_students = pd.read_csv(grad_students_path, delimiter=",", skiprows=1)
        print("Successfully loaded grad_students.csv")
    except Exception as e:
        print(f"Error loading grad_students.csv: {e}")
        return "Error loading grad_students.csv"

    # Fill NaN values
    recent_grads.fillna(0, inplace=True)  
    grad_students.fillna(0, inplace=True)  
    print("Filled NaN values with 0 in both DataFrames.")

    # Load environment variables
    load_dotenv(dotenv_path='.env')
    server_hostname = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")
    
    print("Environment variables loaded.")

    # Connect to Databricks
    try:
        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        ) as connection:
            c = connection.cursor()
            print("Connected to Databricks successfully.")

            # Check if the recent-grads table exists
            c.execute("SHOW TABLES FROM default LIKE 'recent_grads*'")
            result = c.fetchall()
            print(f"Recent grads table check result: {result}")

            # Create the table and insert data if it doesn't exist
            if not result:
                print("Creating RecentGradsDB table...")
                c.execute(
                    """
                    CREATE TABLE IF NOT EXISTS RecentGradsDB (
                        Rank int,
                        Major_code int,
                        Major string,
                        Total int,
                        Men int,
                        Women int,
                        Major_category string,
                        ShareWomen float,
                        Sample_size int,
                        Employed int,
                        Full_time int,
                        Part_time int,
                        Full_time_year_round int,
                        Unemployed int,
                        Unemployment_rate float,
                        Median int,
                        P25th int,
                        P75th int,
                        College_jobs int,
                        Non_college_jobs int,
                        Low_wage_jobs int
                    )
                    """
                )
                print("RecentGradsDB table created.")

                # Insert data into RecentGradsDB
                for _, row in recent_grads.iterrows():
                    values = tuple(row)
                    c.execute(f"INSERT INTO RecentGradsDB VALUES {values}")
                print(f"Inserted {len(recent_grads)} rows into RecentGradsDB.")

            # Check if the grad-students table exists
            c.execute("SHOW TABLES FROM default LIKE 'grad_students*'")
            result = c.fetchall()
            print(f"Grad students table check result: {result}")

            # Create the table and insert data if it doesn't exist
            if not result:
                print("Creating GradStudentsDB table...")
                c.execute(
                    """
                    CREATE TABLE IF NOT EXISTS GradStudentsDB (
                        Major_code int,
                        Major string,
                        Major_category string,
                        Grad_total int,
                        Grad_sample_size int,
                        Grad_employed int,
                        Grad_full_time_year_round int,
                        Grad_unemployed int,
                        Grad_unemployment_rate float,
                        Grad_median int,
                        Grad_P25 int,
                        Grad_P75 int,
                        Nongrad_total int,
                        Nongrad_employed int,
                        Nongrad_full_time_year_round int,
                        Nongrad_unemployed int,
                        Nongrad_unemployment_rate float,
                        Nongrad_median int,
                        Nongrad_P25 int,
                        Nongrad_P75 int,
                        Grad_share float,
                        Grad_premium float
                    )
                    """
                )
                print("GradStudentsDB table created.")

                # Insert data into GradStudentsDB
                for _, row in grad_students.iterrows():
                    values = tuple(row)
                    c.execute(f"INSERT INTO GradStudentsDB VALUES {values}")
                print(f"Inserted {len(grad_students)} rows into GradStudentsDB.")

            c.close()
            print("Databricks connection closed.")

    except Exception as e:
        print(f"Error connecting to Databricks or executing SQL: {e}")
        return "Error during Databricks connection"

    print("Data loading completed successfully.")
    return "success"

if __name__ == "__main__":
    load_data()
