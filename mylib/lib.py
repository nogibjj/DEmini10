"""
Library functions for data extraction, loading, querying, and transformation.
"""

import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    # StringType
)

LOG_FILE = "pyspark_output.md"


def log_output(operation, output, query=None):
    """Adds to a markdown file."""
    with open(LOG_FILE, "a") as file:
        file.write(f"### Operation: {operation}\n\n")
        if query:
            file.write(f"**Query**: {query}\n\n")
        file.write("**Output**:\n\n")
        file.write(output)
        file.write("\n\n")


def start_spark(appName="USBirthsApp"):
    """Initialize Spark session."""
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark


def end_spark(spark):
    """Stop Spark session."""
    spark.stop()
    return "Spark session stopped."


def extract(
    url="https://raw.githubusercontent.com/fivethirtyeight/data/master/births/US_births_2000-2014_SSA.csv",
    file_path="data/US_births.csv",
    directory="data"
):
    """Download a CSV file from a URL to a specified path."""
    if not os.path.exists(directory):
        os.makedirs(directory)
    response = requests.get(url)
    with open(file_path, "wb") as file:
        file.write(response.content)

    return file_path


def load_data(spark, data="data/US_births.csv", name="USBirths"):
    """Load data into a Spark DataFrame with a defined schema."""
    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("date_of_month", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("births", IntegerType(), True)
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)
    log_output("Load data", df.limit(10).toPandas().to_markdown())

    return df


def query(spark, df, query_str, table_name="USBirths"):
    """Run a SQL query on the DataFrame using Spark SQL."""
    df.createOrReplaceTempView(table_name)
    result = spark.sql(query_str)
    log_output("Query data", result.limit(10).toPandas().to_markdown(), query_str)

    return result.show()


def describe(df):
    """Get summary statistics of the DataFrame."""
    summary_stats = df.describe().toPandas().to_markdown()
    log_output("Describe data", summary_stats)
    
    return df.describe().show()


def example_transform(df):
    """Apply an example transformation on the dataset."""
    # Adding a column to categorize births as high or low based on a threshold
    birth_threshold = 5000
    df = df.withColumn(
        "Birth_Category",
        when(col("births") > birth_threshold, "High")
        .otherwise("Low")
    )
    
    log_output("Transform data", df.limit(10).toPandas().to_markdown())

    return df.show()
