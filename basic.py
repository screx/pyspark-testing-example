from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from typing import Any


def create_sample_dataframe(spark: SparkSession) -> Any:
    """Create a sample dataframe with fake data for testing purposes.

    Args:
        spark: SparkSession instance for creating dataframes

    Returns:
        DataFrame: A dataframe containing sample data with id, name, age, and
        salary columns
    """
    # Define the schema for our dataframe
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True)
    ])

    # Sample data
    sample_data = [
        (1, "Alice Johnson", 28, 75000.50),
        (2, "Bob Smith", 34, 82000.75),
        (3, "Charlie Brown", 29, 68000.25),
        (4, "Diana Prince", 31, 91000.00),
        (5, "Eve Wilson", 26, 59000.80),
        (6, "Frank Miller", 35, 88000.40),
        (7, "Grace Lee", 30, 72000.60),
        (8, "Henry Davis", 33, 85000.90)
    ]

    # Create dataframe
    df = spark.createDataFrame(sample_data, schema)
    return df


def calculate_salary_sum(df: Any) -> float:
    """Calculate the sum of the salary column.

    Args:
        df: DataFrame containing salary data

    Returns:
        float: The total sum of all salaries
    """
    # Apply column sum on the salary field
    salary_sum_result = df.agg(spark_sum(col("salary"))).collect()[0][0]

    # Handle case where DataFrame is empty (sum returns None)
    if salary_sum_result is None:
        return 0.0

    return salary_sum_result


def main() -> None:
    """Main function to run the Spark job."""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("BasicSparkJob") \
        .master("local[*]") \
        .getOrCreate()

    try:
        # Create sample dataframe
        print("Creating sample dataframe...")
        df = create_sample_dataframe(spark)

        # Display the dataframe
        print("Sample dataframe:")
        df.show()

        # Display dataframe schema
        print("Dataframe schema:")
        df.printSchema()

        # Calculate and display salary sum
        print("Calculating salary sum...")
        total_salary = calculate_salary_sum(df)
        print(f"Total salary sum: ${total_salary:,.2f}")

        # Show some basic statistics
        print("\nBasic statistics:")
        df.describe().show()

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    main()
