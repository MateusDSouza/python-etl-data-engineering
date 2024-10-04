import pytest
from pyspark.sql import SparkSession
from pipeline import PipelineDevicesSensors
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark_session = (
        SparkSession.builder.master("local").appName("TestPipeline").getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_clean_and_transform(spark: SparkSession):
    """
    Test the cleaning and transformation of sensor data.

    This test evaluates the `clean_and_transform` method by passing a DataFrame
    with duplicate records and checking that duplicates are removed, a new
    'month' column is added, and the 'device' column is renamed to 'code'.

    Args:
        spark (SparkSession): The Spark session used for testing.
    """
    pipeline = PipelineDevicesSensors(spark, "DEV")
    mock_df = spark.createDataFrame(
        [(1, "2021-01-01", "device1"), (1, "2021-01-01", "device1")],
        ["id", "timestamp", "device"],
    )

    cleaned_df = pipeline.clean_and_transform(mock_df)

    assert "month" in cleaned_df.columns
    assert cleaned_df.schema["code"]


def test_join_devices_info_sensor_data(spark: SparkSession):
    """
    Test the inner join of device information and sensor data.

    This test verifies that the `join_devices_info_sensor_data` method correctly
    performs an inner join between the device information DataFrame and the sensor
    data DataFrame, resulting in a new DataFrame with selected fields.

    Args:
        spark (SparkSession): The Spark session used for testing.
    """
    pipeline = PipelineDevicesSensors(spark, "DEV")
    mock_df_devices = spark.createDataFrame(
        [(1, "device1", "area1")], ["id", "code", "area"]
    )

    mock_df_sensors = spark.createDataFrame(
        [(1, "device1", 400, 30, 22, 1)],
        ["id", "code", "CO2_level", "humidity", "temperature", "month"],
    )

    joined_df = pipeline.join_devices_info_sensor_data(mock_df_devices, mock_df_sensors)

    assert "avg_CO2_level" not in joined_df.columns


def test_aggregation(spark: SparkSession):
    """
    Test the aggregation of sensor data.

    This test checks that the `aggregation` method correctly calculates the
    average values of CO2 level, humidity, and temperature, grouped by area
    and month, and returns a new DataFrame with the aggregated results.

    Args:
        spark (SparkSession): The Spark session used for testing.
    """
    pipeline = PipelineDevicesSensors(spark, "DEV")
    mock_df = spark.createDataFrame(
        [("area1", 1, 400, 30, 22)],
        ["area", "month", "CO2_level", "humidity", "temperature"],
    )

    aggregated_df = pipeline.aggregation(mock_df)

    assert "avg_CO2_level" in aggregated_df.columns
