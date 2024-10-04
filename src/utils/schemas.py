from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

device_schema = StructType(
    [
        StructField("code", StringType(), nullable=False),
        StructField("type", StringType(), nullable=False),
        StructField("area", StringType(), nullable=False),
        StructField("customer", StringType(), nullable=False),
    ]
)

"""
device_schema (StructType):
    Schema for the CSV files located in 'sample_data\devices_info\devices.csv'.

    Fields:
    - code (StringType, nullable=False): ID of the device.
    - type (StringType, nullable=False): Device type.
    - area (StringType, nullable=False): Area in which the device was installed.
    - customer (StringType, nullable=False): Name of the customer using the device.
"""

sensor_schema = StructType(
    [
        StructField("device", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("CO2_level", FloatType(), nullable=False),
        StructField("humidity", FloatType(), nullable=False),
        StructField("temperature", FloatType(), nullable=False),
    ]
)

"""
sensor_schema (StructType):
    Schema for the JSON files located in 'sample_data\sensor_data\received=...'.

    Fields:
    - device (StringType, nullable=False): ID of the device.
    - timestamp (TimestampType, nullable=False): The timestamp of the sensor record.
    - CO2_level (FloatType, nullable=False): The CO2 concentration measured by the sensor.
    - humidity (FloatType, nullable=False): The humidity level measured by the sensor.
    - temperature (FloatType, nullable=False): The temperature measured by the sensor.
"""
