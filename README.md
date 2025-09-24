# PySpark Testing Guide

This project demonstrates how to write and run tests for PySpark applications locally.

## Dependencies

### Required Packages

The project uses the following key dependencies:

- **PySpark 4.0.1** - Apache Spark's Python API
- **pytest 8.4.2** - Testing framework
- **pytest-spark 0.8.0** - PySpark testing utilities
- **findspark 2.0.1** - Helps locate Spark installation

### Python Version

- Python 3.13.3

## Setup

### 1. Install Java (OpenJDK)

PySpark requires Java to run. Install OpenJDK 17:

**macOS (using Homebrew):**
```bash
brew install openjdk@17
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install openjdk-17-jdk
```

### 2. Set JAVA_HOME Environment Variable

**macOS/Linux:**
```bash
# Add to your ~/.zshrc or ~/.bashrc
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
```

### 3. Verify Java Installation

```bash
java -version
echo $JAVA_HOME  # Should show your Java installation path
```

### 4. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 5. Install Dependencies

```bash
pip install pyspark pytest pytest-spark findspark
```

### 6. Verify Installation

```bash
python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
pytest --version
```

## Project Structure

```
pyspark-testing/
├── basic.py              # Main PySpark application
├── basic_test.py         # Test file
├── README.md            # This file
└── venv/                # Virtual environment
```

## Writing Tests

### Basic Test Structure

Tests are written using pytest and follow this pattern:

```python
import pytest
from pyspark.sql import SparkSession
from your_module import your_function

class TestYourFunction:
    def test_your_function_behavior(self, spark_session: SparkSession) -> None:
        # Arrange
        test_data = create_test_dataframe(spark_session)

        # Act
        result = your_function(test_data)

        # Assert
        assert result == expected_value
```

### Key Testing Concepts

1. **Fixtures**: Use `spark_session` fixture for Spark context
2. **DataFrame Testing**: Test schema, row count, and data types
3. **Error Handling**: Test with invalid inputs using `pytest.raises()`
4. **Edge Cases**: Test empty DataFrames, single records, etc.

### Example Test Categories

- **Unit Tests**: Test individual functions
- **Integration Tests**: Test multiple functions together
- **Schema Tests**: Verify DataFrame structure
- **Data Tests**: Verify actual data content
- **Error Tests**: Test error conditions

## Running Tests

### Run All Tests

```bash
pytest
```

### Run Specific Test File

```bash
pytest basic_test.py
```

### Run Specific Test Class

```bash
pytest basic_test.py::TestCreateSampleDataframe
```

### Run Specific Test Method

```bash
pytest basic_test.py::TestCreateSampleDataframe::test_create_sample_dataframe_returns_dataframe
```

### Run with Verbose Output

```bash
pytest -v
```

### Run with Coverage

```bash
pytest --cov=basic
```


## Common Test Patterns

### Testing DataFrame Creation

```python
def test_dataframe_creation(self, spark_session: SparkSession) -> None:
    result_df = create_dataframe(spark_session)

    # Test it's a DataFrame
    assert isinstance(result_df, DataFrame)

    # Test schema
    expected_schema = StructType([...])
    assert result_df.schema == expected_schema

    # Test row count
    assert result_df.count() == expected_count
```

### Testing DataFrame Operations

```python
def test_dataframe_operation(self, spark_session: SparkSession) -> None:
    df = create_test_dataframe(spark_session)
    result = perform_operation(df)

    # Test result type
    assert isinstance(result, expected_type)

    # Test result value
    assert result == expected_value
```

### Testing Error Conditions

```python
def test_error_handling(self) -> None:
    with pytest.raises(AttributeError):
        your_function(None)
```

## Best Practices

1. **Use Descriptive Test Names**: Test names should explain what is being tested
2. **Follow AAA Pattern**: Arrange, Act, Assert
3. **Test Edge Cases**: Empty DataFrames, single records, invalid inputs
4. **Use Type Hints**: Include type annotations for better code clarity
5. **Clean Up Resources**: The `spark_session` fixture handles cleanup automatically
6. **Test Data Types**: Verify DataFrame column types match expectations
7. **Test Schema**: Ensure DataFrame structure is correct

## Troubleshooting

### Common Issues

1. **Java Not Found**: Ensure OpenJDK is installed and `JAVA_HOME` is set correctly
2. **Spark Not Found**: Ensure `findspark` is installed and Spark is in PATH
3. **Memory Issues**: Use `local[1]` for single-threaded testing
4. **Slow Tests**: Consider using `pytest-xdist` for parallel execution
5. **Import Errors**: Verify virtual environment is activated
6. **JAVA_HOME Not Set**: Check that `echo $JAVA_HOME` returns a valid path

### Performance Tips

- Use `local[1]` for testing to avoid overhead
- Disable adaptive query execution for consistent test results
- Use small test datasets
- Stop Spark sessions properly in fixtures

## Example Usage

See `basic_test.py` for comprehensive examples of:
- DataFrame creation testing
- Schema validation
- Data type checking
- Error handling
- Integration testing

The test file demonstrates testing patterns you can adapt for your own PySpark applications.
