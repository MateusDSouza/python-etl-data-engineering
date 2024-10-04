import configparser
import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import avg, format_number, month
from utils.schemas import device_schema, sensor_schema


class PipelineDevicesSensors:
    """
    This class represents an ETL that processes device and sensor data.
    It reads device information from a CSV file and sensor data from JSON files.
    It cleans the data, join, aggregate and store.

    Attributes:
        spark (SparkSession): The spark session object passed in the entry-point.
    """

    def __init__(self, spark: SparkSession, env: str) -> None:
        """
        Start the PipelineDevicesSensors with a spark session.

        Args:
            spark (SparkSession): The spark session object used for data processing.
            env (str): Environment to run the ETL. The env is connected to the right folder paths
            in the configuration file config.ini.
            devices_info_path (str): Path to the devices_info csv data.
            sensor_data_path(str): Path to the jsons of sensor_data.
            output_path_bronze(str): Path to the parquet files of the raw data with a schema
            applied.
            output_path_silver(str): Path to the parquet files of the joined process data.
            output_path_gold(str): Path to the parquet files of the aggregated process data.


        """
        self.spark = spark

        if env not in ["DEV", "STG", "PRD"]:
            self.env = "DEV"
        else:
            self.env = env

        self.config = configparser.ConfigParser()
        self.config.read("config.ini")

        self.devices_info_path = self.config[self.env]["devices_info_path"]
        self.sensor_data_path = self.config[self.env]["sensor_data_path"]
        self.output_path_bronze = self.config[self.env]["output_path_bronze"]
        self.output_path_silver = self.config[self.env]["output_path_silver"]
        self.output_path_gold = self.config[self.env]["output_path_gold"]

    def read_devices_data(self) -> DataFrame:
        """
        Reads device data from a CSV file and returns a DataFrame.

        The CSV file contains information about devices and is expected to have a header row.

        Returns:
            DataFrame: DataFrame containing device data.

        Raises:
            Exception: If an error occurs during reading of the CSV file, it prints the
            error in the console.
        """
        try:
            df = self.spark.read.schema(device_schema).csv(
                self.devices_info_path, header=True
            )
            return df

        except Exception as e:
            print(f"An error occurred in the reading of devices data: {e}")
            raise

    def read_sensor_data(self) -> DataFrame:
        """
        Reads sensor data from JSON files.

        Sensor data files are located in a directory structured by date ( 'received=2021-04-01/*.json').
        Because I cannot use hadoop to create the file system, I used glob to map the existing
        data and process the data by iterating a list.
        I simplify the situation of the problem in order to not spend extra time configuring
        a container with hdfs, spark and so on...
        For a matter of curiosity, I would recommend to implement a deletion of the files right after
        the reading to not read twice and generate duplicated data.
        This could also be avoided by using tools such as AutoLoader (lakehouse solutions).

        Returns:
            DataFrame: A Spark DataFrame containing sensor data from multiple JSON files.

        Raises:
            FileNotFoundError: If no JSON files are found in the specified directory.
            Exception: If an error occurs during reading of the JSON files.
        """

        try:
            files = glob.glob(
                os.path.join(self.sensor_data_path, "received=*", "*.json")
            )
            if not files:
                raise FileNotFoundError(
                    "No files were found. Probably the directory is wrong."
                )
            for file in files:
                df = self.spark.read.schema(sensor_schema).json(file)
                df = df.union(df)
            return df
        except Exception as e:
            print(f"An error occurred in the reading of sensor data: {e}")
            raise

    def clean_and_transform(self, df: DataFrame) -> DataFrame:
        """
        Clean and transform the input DataFrame by removing duplicates and
        extracting the month from the 'timestamp' column and renaming the join column
        from 'device' to 'code'.

        The checking of the schema in this point was avoided because of the incapsulation
        done in the execute orchestrator method.
        The drop duplicates is done taking into consideration all the columns because
        of the structure of the data.
        Duplicated records are the errors of the sensors in which they send same
        measures with the same timestamp from the same device.

        Args:
            df (DataFrame): The input DataFrame containing sensor data, which
                            must include 'timestamp' column and 'device' column.

        Returns:
            DataFrame: A new DataFrame that has duplicates removed and includes
                        an additional column 'month' containing the month
                        extracted from the 'timestamp' column.
        """
        df_clean = df.dropDuplicates()
        df_transformed = df_clean.withColumn("month", month(df["timestamp"]))
        df_renamed = df_transformed.withColumnRenamed("device", "code")
        return df_renamed

    def join_devices_info_sensor_data(
        self, df_devices: DataFrame, df_sensors: DataFrame
    ) -> DataFrame:
        """
        Perform an inner join on the sensor_data cleaned and the devices_info and select
        the fields that are important for the aggregation.
        The choice of the inner join is because maybe in tthe list of the devices there
        are more devices than the ones we have in the field.
        Also, it is possible that we have sensor data from a device that maybe was missed
        in the catalog of devices.
        Because we are interested in have aggregations based on the 'area', a field
        exclusive of the devices_info dataframe, and based
        on the 'month', a field exclusive of the sensor_data dataframe, in my opinion,
        the inner join is the most adequated join to be used.

        Args:
            df_devices (DataFrame): The dataframe containing device information.
            df_sensors (DataFrame): The dataframe containing sensor data.

        Returns:
            DataFrame: A new DataFrame resulting from the inner join of devices_info_raw and sensor_data_clean.
        """
        ##test the precommit
        result_df = df_devices.join(df_sensors, on="code", how="inner").select(
            df_devices.area,
            df_sensors.CO2_level,
            df_sensors.humidity,
            df_sensors.temperature,
            df_sensors.month,
        )
        return result_df

    def aggregation(self, df: DataFrame) -> DataFrame:
        """
        Execute the aggregation function in the joined dataframe to get the average
        CO2_level, humidity, and temperature for each month and area.
        Format the numbers to 3 decimals for better visualization without
        compromissing prrecision.

        Args:
            df (DataFrame): The input DataFrame containing the joined data of
            devices_info and sensor_data.

        Returns:
            DataFrame: A DataFrame containing the average values grouped by area and month.
        """
        df_agg = df.groupBy("area", "month").agg(
            format_number(avg("CO2_level"), 3).alias("avg_CO2_level"),
            format_number(avg("humidity"), 3).alias("avg_humidity"),
            format_number(avg("temperature"), 3).alias("avg_temperature"),
        )
        return df_agg

    def save(self, df: DataFrame, location: str) -> None:
        """
        Save the DataFrame to a local folder as a Parquet file, partitioned by area and month.

        A workaround was implemented using pandas dataframe.

        Args:
            df (DataFrame): The dataframe processed by the etl.
            location (str): Location where the dataframe will be saved.

        Raises:
        FileNotFoundError: If the output path does not exist or is not accessible.

        """
        try:
            # Problem to save from pyspark because of the installation of Hadoop in windows.
            ###############
            # df.write.format("delta").partitionBy("area", "month").save(
            #    self.output_path
            # )
            ###############
            # df.write.mode("overwrite").partitionBy("area", "month").parquet(
            #    self.output_path
            # )
            ###############
            pandas_df = df.toPandas()
            pandas_df.to_parquet(location, index=False)

        except Exception as e:
            print(
                f"An error occurred while saving the DataFrame to the output path specified in the configuration file: {e}"
            )
            raise

    def execute(self) -> None:
        """
        ETL Orchestrator method.

        Orchestrate the flow of data within the class methods

        Returns:
            DataFrame: A Spark DataFrame containing sensor data from multiple JSON files.
        """
        ##############
        # BRONZE LAYER#
        ##############
        devices_info_raw = self.read_devices_data()
        self.save(
            df=devices_info_raw,
            location=self.output_path_bronze + "df_devices_info.parquet",
        )
        sensor_data_raw = self.read_sensor_data()
        self.save(
            df=sensor_data_raw,
            location=self.output_path_bronze + "df_sensor_data.parquet",
        )
        ##############
        # SILVER LAYER#
        ##############
        sensor_data_clean = self.clean_and_transform(df=sensor_data_raw)
        joined_devices_sensors = self.join_devices_info_sensor_data(
            df_devices=devices_info_raw, df_sensors=sensor_data_clean
        )
        self.save(
            df=joined_devices_sensors,
            location=self.output_path_silver + "df_joined_devices_sensors.parquet",
        )
        ##############
        # GOLD LAYER ##
        ##############
        aggregated_data = self.aggregation(joined_devices_sensors)
        self.save(df=aggregated_data, location=self.output_path_gold + "df_agg.parquet")
        aggregated_data.show()
