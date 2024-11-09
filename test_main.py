"""
Unit tests for the US Births data processing functions.
"""

import os
import pytest
from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


@pytest.fixture(scope="module")
def spark():
    """Set up and tear down the Spark session for testing."""
    spark_session = start_spark("TestApp")
    yield spark_session
    end_spark(spark_session)


def test_extract():
    """Test the extract function to ensure the file is downloaded correctly."""
    file_path = extract()
    assert os.path.exists(file_path), "The extracted file does not exist."


def test_load_data(spark):
    """Test the load_data function to check if the DataFrame loads correctly."""
    df = load_data(spark)
    assert df is not None, "DataFrame is None after loading."
    assert df.count() > 0, "DataFrame is empty."


def test_describe(spark):
    """Test the describe function to verify if summary statistics are generated."""
    df = load_data(spark)
    result = describe(df)
    assert result is None, "Describe function should return None."


def test_query(spark):
    """Test the query function with a sample SQL query."""
    df = load_data(spark)
    query_str = "SELECT year, month, SUM(births) AS total_births FROM US_Births GROUP BY year, month LIMIT 5"
    result = query(spark, df, query_str, "US_Births")
    assert result is None, "Query function should return None."


def test_example_transform(spark):
    """Test the example_transform function to check if transformation is applied correctly."""
    df = load_data(spark)
    result = example_transform(df)
    assert result is None, "Transform function should return None."


if __name__ == "__main__":
    # Run tests individually if the script is executed directly
    test_extract()
    spark_session = start_spark("TestApp")
    test_load_data(spark_session)
    test_describe(spark_session)
    test_query(spark_session)
    test_example_transform(spark_session)
    end_spark(spark_session)
