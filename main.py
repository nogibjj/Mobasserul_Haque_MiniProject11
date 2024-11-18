"""command-line interface (CLI) for the ETL (Extract, Transform, Load) process and querying."""
import sys
import argparse
from myLib.extract import extract
from myLib.transform_load import load_data
from myLib.query import query


def handle_arguments(args):
    parser = argparse.ArgumentParser(description="ETL-Query script")
    parser.add_argument(
        "action",
        choices=["extract", "load", "query"],
    )

    if "query" in args:
        # Add an argument for a raw SQL query passed as a string
        parser.add_argument(
            "query", help="The SQL query to execute, passed as a string."
        )

    return parser.parse_args(args)


def main():
    args = handle_arguments(sys.argv[1:])

    if args.action == "extract":
        print("Extracting data...")
        extract()
    elif args.action == "load":
        print("Transforming data...")
        load_data()
    elif args.action == "query":
        print("Executing query...")
        sql_query = args.query  # Capture the SQL query from the command line argument
        query(sql_query)
    else:
        print(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()
