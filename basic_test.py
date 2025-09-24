import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from pyspark.sql import DataFrame

from basic import create_sample_dataframe, calculate_salary_sum


class TestCreateSampleDataframe:
    """Test cases for the create_sample_dataframe function."""

    def test_create_sample_dataframe_returns_dataframe(
        self, spark_session: SparkSession
    ) -> None:
        """Test that create_sample_dataframe returns a DataFrame object.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Act
        result_df = create_sample_dataframe(spark_session)

        # Assert
        assert isinstance(result_df, DataFrame)
        assert result_df is not None

    def test_create_sample_dataframe_has_correct_schema(
        self, spark_session: SparkSession
    ) -> None:
        """Test that the returned DataFrame has the expected schema structure.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Act
        result_df = create_sample_dataframe(spark_session)

        # Assert
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
        ])

        assert result_df.schema == expected_schema

    def test_create_sample_dataframe_has_correct_row_count(
        self, spark_session: SparkSession
    ) -> None:
        """Test that the returned DataFrame contains the expected number of rows.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Act
        result_df = create_sample_dataframe(spark_session)

        # Assert
        assert result_df.count() == 8

    def test_create_sample_dataframe_has_correct_column_count(
        self, spark_session: SparkSession
    ) -> None:
        """Test that the returned DataFrame has the expected number of columns.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Act
        result_df = create_sample_dataframe(spark_session)

        # Assert
        assert len(result_df.columns) == 4
        assert "id" in result_df.columns
        assert "name" in result_df.columns
        assert "age" in result_df.columns
        assert "salary" in result_df.columns

    def test_create_sample_dataframe_data_types_are_correct(
        self, spark_session: SparkSession
    ) -> None:
        """Test that the data in the DataFrame has the correct types.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Act
        result_df = create_sample_dataframe(spark_session)

        # Assert
        # Check that id column contains integers
        id_values = [row.id for row in result_df.select("id").collect()]
        assert all(isinstance(val, int) for val in id_values)

        # Check that name column contains strings
        name_values = [row.name for row in result_df.select("name").collect()]
        assert all(isinstance(val, str) for val in name_values)

        # Check that age column contains integers
        age_values = [row.age for row in result_df.select("age").collect()]
        assert all(isinstance(val, int) for val in age_values)

        # Check that salary column contains floats
        salary_values = [
            row.salary for row in result_df.select("salary").collect()
        ]
        assert all(isinstance(val, float) for val in salary_values)

    def test_create_sample_dataframe_contains_expected_data(
        self, spark_session: SparkSession
    ) -> None:
        """Test that the DataFrame contains the expected sample data.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Act
        result_df = create_sample_dataframe(spark_session)

        # Assert
        # Collect all data and sort by id for consistent comparison
        collected_data = sorted(result_df.collect(), key=lambda x: x.id)

        expected_data = [
            (1, "Alice Johnson", 28, 75000.50),
            (2, "Bob Smith", 34, 82000.75),
            (3, "Charlie Brown", 29, 68000.25),
            (4, "Diana Prince", 31, 91000.00),
            (5, "Eve Wilson", 26, 59000.80),
            (6, "Frank Miller", 35, 88000.40),
            (7, "Grace Lee", 30, 72000.60),
            (8, "Henry Davis", 33, 85000.90),
        ]

        for i, expected_row in enumerate(expected_data):
            actual_row = collected_data[i]
            assert actual_row.id == expected_row[0]
            assert actual_row.name == expected_row[1]
            assert actual_row.age == expected_row[2]
            assert actual_row.salary == expected_row[3]

    def test_create_sample_dataframe_with_none_spark_session_raises_error(
        self,
    ) -> None:
        """Test that passing None as spark parameter raises appropriate error.

        This test verifies error handling for invalid input.
        """
        # Act & Assert
        with pytest.raises(AttributeError):
            create_sample_dataframe(None)


class TestCalculateSalarySum:
    """Test cases for the calculate_salary_sum function."""

    def test_calculate_salary_sum_returns_float(
        self, spark_session: SparkSession
    ) -> None:
        """Test that calculate_salary_sum returns a float value.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Arrange
        df = create_sample_dataframe(spark_session)

        # Act
        result = calculate_salary_sum(df)

        # Assert
        assert isinstance(result, float)

    def test_calculate_salary_sum_returns_correct_total(
        self, spark_session: SparkSession
    ) -> None:
        """Test that calculate_salary_sum returns the correct sum of all salaries.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Arrange
        df = create_sample_dataframe(spark_session)
        expected_total = (
            75000.50 + 82000.75 + 68000.25 + 91000.00 +
            59000.80 + 88000.40 + 72000.60 + 85000.90
        )

        # Act
        result = calculate_salary_sum(df)

        # Assert
        # Allow for small floating point differences
        assert abs(result - expected_total) < 0.01

    def test_calculate_salary_sum_with_empty_dataframe_returns_zero(
        self, spark_session: SparkSession
    ) -> None:
        """Test that calculate_salary_sum returns 0 for an empty DataFrame.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Arrange
        empty_schema = StructType([
            StructField("salary", DoubleType(), True)
        ])
        empty_df = spark_session.createDataFrame([], empty_schema)

        # Act
        result = calculate_salary_sum(empty_df)

        # Assert
        assert result == 0.0

    def test_calculate_salary_sum_with_single_record(
        self, spark_session: SparkSession
    ) -> None:
        """Test that calculate_salary_sum works correctly with a single record.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Arrange
        single_record_schema = StructType([
            StructField("salary", DoubleType(), True)
        ])
        single_record_data = [(50000.0,)]
        single_df = spark_session.createDataFrame(
            single_record_data, single_record_schema
        )

        # Act
        result = calculate_salary_sum(single_df)

        # Assert
        assert result == 50000.0

    def test_calculate_salary_sum_with_negative_salaries(
        self, spark_session: SparkSession
    ) -> None:
        """Test that calculate_salary_sum handles negative salary values correctly.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Arrange
        negative_salary_schema = StructType([
            StructField("salary", DoubleType(), True)
        ])
        negative_salary_data = [(1000.0,), (-500.0,), (2000.0,)]
        negative_df = spark_session.createDataFrame(
            negative_salary_data, negative_salary_schema
        )

        # Act
        result = calculate_salary_sum(negative_df)

        # Assert
        assert result == 2500.0

    def test_calculate_salary_sum_with_zero_salaries(
        self, spark_session: SparkSession
    ) -> None:
        """Test that calculate_salary_sum handles zero salary values correctly.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Arrange
        zero_salary_schema = StructType([
            StructField("salary", DoubleType(), True)
        ])
        zero_salary_data = [(1000.0,), (0.0,), (2000.0,)]
        zero_df = spark_session.createDataFrame(
            zero_salary_data, zero_salary_schema
        )

        # Act
        result = calculate_salary_sum(zero_df)

        # Assert
        assert result == 3000.0

    def test_calculate_salary_sum_with_none_dataframe_raises_error(
        self,
    ) -> None:
        """Test that passing None as DataFrame parameter raises appropriate error.

        This test verifies error handling for invalid input.
        """
        # Act & Assert
        with pytest.raises(AttributeError):
            calculate_salary_sum(None)

    def test_calculate_salary_sum_with_missing_salary_column_raises_error(
        self, spark_session: SparkSession
    ) -> None:
        """Test that calculate_salary_sum raises error when salary column is missing.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Arrange
        wrong_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        wrong_data = [(1, "Test")]
        wrong_df = spark_session.createDataFrame(wrong_data, wrong_schema)

        # Act & Assert
        # PySpark will raise an AnalysisException
        with pytest.raises(Exception):
            calculate_salary_sum(wrong_df)


@pytest.fixture
def spark_session() -> SparkSession:
    """Create a SparkSession for testing purposes.

    Returns:
        SparkSession: A configured SparkSession instance for testing
    """
    spark = (
        SparkSession.builder
        .appName("TestSparkJob")
        .master("local[1]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()


# Integration test that tests both functions together
class TestIntegration:
    """Integration tests for the basic module functions."""

    def test_create_and_calculate_salary_sum_integration(
        self, spark_session: SparkSession
    ) -> None:
        """Test the integration between create_sample_dataframe and
        calculate_salary_sum.

        Args:
            spark_session: Pytest fixture providing SparkSession instance
        """
        # Act
        df = create_sample_dataframe(spark_session)
        total_salary = calculate_salary_sum(df)

        # Assert
        assert isinstance(df, DataFrame)
        assert isinstance(total_salary, float)
        assert total_salary > 0
        assert df.count() == 8
        assert len(df.columns) == 4
