# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2024-09-28
### Added
- Implemented the `PipelineDevicesSensors` class to process device and sensor data.
- Added method `read_devices_data()` to read device information from CSV files.
- Added method `read_sensor_data()` to read sensor data from JSON files.
- Implemented `clean_and_transform()` method to clean and prepare sensor data for processing.
- Created `join_devices_info_sensor_data()` method to perform inner joins between device info and sensor data.
- Added `aggregation()` method to calculate averages of CO2 levels, humidity, and temperature grouped by area and month.
- Created `save()` method to store data in Parquet format at different ETL layers (bronze, silver, and gold).
- Developed `execute()` orchestrator method to run the entire ETL pipeline.
- Configuration-based folder paths for environment variables (DEV, STG, PRD) managed via `config.ini`.

### Fixed
- Addressed Windows-specific limitations related to Hadoop by introducing `glob` for file listing.
- Replaced direct Spark saving methods with Pandas-based workaround due to Spark-Hadoop installation issues on Windows.

## [0.1.0] - 2024-09-20
### Added
- Initial project setup with dependencies for PySpark, configparser, and utility schemas.
- Basic structure for reading and processing device and sensor data.
