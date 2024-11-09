"""
Main CLI or app entry point for the US Births data processing.
"""

from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


def main():
    # Step 1: Extract data from the URL
    file_path = extract()
    
    # Step 2: Start a Spark session
    spark = start_spark("USBirthsApp")
    
    # Step 3: Load data into a DataFrame
    df = load_data(spark, data=file_path)
    
    # Step 4: Display summary statistics
    describe(df)
    
    # Step 5: Run a sample query
    query_str = """
        SELECT year, month, SUM(births) AS total_births
        FROM USBirths
        GROUP BY year, month
        ORDER BY year, month
    """
    query(spark, df, query_str, "USBirths")
    
    # Step 6: Apply an example transformation
    example_transform(df)
    
    # Step 7: End the Spark session
    end_spark(spark)


if __name__ == "__main__":
    main()
