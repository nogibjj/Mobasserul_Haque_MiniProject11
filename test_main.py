import subprocess


def test_extract():
    """Test extractData()"""
    result = subprocess.run(
        ["python", "main.py", "extract"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert (
        result.returncode == 0
    ), f"Extract failed with return code {result.returncode}"
    assert (
        "Extracting data..." in result.stdout
    ), "Expected 'Extracting data...' in output"
    print("Extract Test Passed!")


def test_load():
    """Test loadData()"""
    result = subprocess.run(
        ["python", "main.py", "load"],
        capture_output=True,
        text=True,
        check=True,
    )

    if result.returncode != 0:
        print(f"Load failed with return code {result.returncode}")
        print(f"Error output: {result.stderr}")  # Print the error output
        assert result.returncode == 0  # Reassert to ensure the test fails

    assert (
        "Loading data to Databricks..." in result.stdout
    ), "Expected 'Loading data to Databricks...' in output"
    print("Load Test Passed!")


def test_general_query():
    """Test general_query() with a complex SQL query"""
    query_string = """
        SELECT 
            rg.Major, 
            rg.Employed AS Undergrad_Employed, 
            gs.Grad_employed AS Grad_Employed,
            rg.Unemployment_rate AS Undergrad_Unemployment_Rate,
            gs.Grad_unemployment_rate AS Grad_Unemployment_Rate,
            (gs.Grad_median - rg.Median) AS Salary_Premium
        FROM RecentGradsDB rg
        JOIN GradStudentsDB gs ON rg.Major_code = gs.Major_code
        WHERE rg.Unemployment_rate < 0.05  
          AND gs.Grad_unemployment_rate < 0.05  
        ORDER BY Salary_Premium DESC
        LIMIT 5;
    """

    result = subprocess.run(
        ["python", "main.py", "query", query_string],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    print("General Query Test Passed!")


if __name__ == "__main__":
    test_extract()
    test_load()
    test_general_query()
