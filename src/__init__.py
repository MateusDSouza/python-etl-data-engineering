from pipeline import PipelineDevicesSensors
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    Entry-point of the ETL.

    This script initialize a spark session and pass it to the class PipelineDevicesSensors.
    The class contains different functions responsible for reading, cleaning, joining,
    aggregating data from 2 different sources.
    We are consuming .csv files representing the information of the sensor devices and
    .JSON files representing data collected from the sensors, joining this data,
    cleaning, aggregating and storing.

    As a goal, we want to represent the average values for `CO2_level`,
    `humidity` and `temperature` for each month and area.

    Steps of the process:
    1. Creates a local Spark session - single-node clusster in local environment for the simplicity.
    2. Instantiates the `PipelineDevicesSensors` object.
    3. Reads and displays raw device data from a CSV file.
    4. Reads and displays raw sensor data from JSON files.
    5. Clean sensor data - avoiding missing data and duplicated data.
    6. Create new column for month, based on timestamp of the sensor data - used for the
    aggregation.
    7. Execute join.
    8. The joined data frame contain the columns 'month' and 'area' - in case of a silver layer of
    data, those columns should be used as partitions.
    9. Execute the aggregation of average values for the columns  'CO2_level', 'humidity'
    and 'temperature' by the columns 'month' and 'area'.
    Note that the calculated values were already declared as FloatType in the input schema for
    the sensor_data to facilitate the aggregation.
    10. Stops the Spark session after processing.

    Note: Ensure the directory paths and schemas are correctly set in the configuration file
    'config.ini'.
    """

    local_spark = (
        SparkSession.builder.appName("session").master("local[*]").getOrCreate()
    )
    etl = PipelineDevicesSensors(spark=local_spark, env="DEV")
    etl.execute()
    local_spark.stop()
