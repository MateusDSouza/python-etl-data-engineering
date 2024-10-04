# Python - ETL - Data Engineering
This repository contains the project used to improve my Data Engineering skills and python development.

* Need to containarize this application to spark with driver nodes and workers.
* Need hadoop and hive.
* Integration testing is not done. I need CONTAINERS!!!

## Final Result

| Area       | Month | Avg CO2 Level | Avg Humidity | Avg Temperature |
|------------|-------|---------------|--------------|-----------------|
| Residential| 4     | 1,238.911     | 64.439       | 22.724          |
| Commercial | 4     | 1,213.958     | 64.729       | 22.028          |
| Industrial | 4     | 1,184.906     | 64.732       | 23.583          |

## Project Description

The goal of this project was to create a data pipeline capable of processing two distinct data sources: `devices_info` and `sensors_data`. The pipeline was built using two different ingestion methods tailored to the nature of each data source. Due to limitations in using a distributed file system like Hadoop on my Windows computer, a work-around was developed.

### Ingestion

To ingest the data, I used the `glob` library to create a list of files that followed a specific pattern. These files were then iterated through, with a union operation applied to generate a single dataframe.

### Schema and Bronze Layer

After consuming the data sources and applying a defined schema (located in the `utils` folder), the data was saved to the bronze layer in Parquet format.

### Data Cleaning and Transformation

For the cleaning and transformation of the `sensors_data`, I performed the following operations:
- **Deduplication**: A `dropDuplicates` operation was applied across all fields.
- **Date Parsing**: Using Spark SQL's `month` function, a new `month` column was created from the timestamp field.
- **Field Renaming**: The join field between `sensors_data` and `devices_info` was renamed to align with the column naming conventions of the `devices_info` dataframe.

Once the data was cleaned, I executed an inner join between the two datasets. The join was done on the `area` field from the `devices_info` dataframe and the `month` field from the `sensors_data`. This decision was made to ensure that devices present in one dataset but missing in the other were handled appropriately. The resulting dataframe contained 611 records.

### Silver Layer

After the join, unused columns were filtered out to make the data slimmer and it was saved to the silver layer.

### Gold Layer Aggregation

In the gold layer, the data was grouped by `area` and `month`, and average values for `CO2_level`, `humidity`, and `temperature` were calculated. The results were formatted to 3 decimal for user-friendly readability and maintain accuracy.

### Performance Optimizations

Because of the limitations of using PySpark operations on Windows, I used `pyarrow` and `pandas` for writing the Parquet files, simulating a data warehouse in my local file system.
Caching was not necessary for this project due to the small data size, but it could be useful for larger datasets.

To improve read performance, I would recommend partitioning the Parquet files by `area` and `month`. In a production scenario that receives daily data, the usage of an autoloader would be good. However, in a framework-agnostic scenario, I would implement a read-delete strategy to manage data ingestion effectively.


## Considerations

### 1. Dependency Management and Packaging

Due to the complexity of managing PySpark's dependencies with Java, a package version was not implemented in this project. However, for deployment in a production environment, such as on a cluster or containerized setup, it would be ideal to create a Docker image containing all the necessary dependencies. This image could then be uploaded and executed in the target environment. Alternatively, for Databricks or similar platforms, building a `.whl` (wheel) package would allow easy installation and management of the project dependencies.

For managing dependencies locally, I used **Poetry**. It streamlined the process of dependency management, and it can also be utilized to automate testing and packaging for PyPI if necessary in the future.

### 2. Unit Testing

Given the time constraints and the focus on completing the core functionality of the assignment, unit tests were not implemented for the pipeline’s methods. While this decision was made to maintain a balance between time spent and project completeness, it’s important to note that adding unit tests in the future would significantly enhance the robustness of the pipeline. Automating these tests through a continuous integration (CI) system would further improve the maintainability of the project.

### 3. Documentation

All the code in this project includes **docstrings** that document the thought process, class structures, and method functionalities. These docstrings serve as inline documentation, offering insights into how each part of the pipeline operates and the reasoning behind certain decisions.

Additionally, in the `utils` folder, you can find the schemas for the data along with their respective documentation, outlining how the data structure is defined and validated.

### 4. Execution and Configuration

To run the pipeline, the entry point is the `__init__.py` file located inside the `main` folder. This file serves as the starting point for executing the entire pipeline.

To make the project more **environment-agnostic**, I included a configuration file named `config.ini`. This file contains configurations for three distinct environments: **DEV** (development), **STG** (staging), and **PRD** (production). This approach allows the pipeline to be easily adapted and deployed in different environments without requiring major code changes.

### 5. Commit Strategy

All development was done on the **main branch**, with small, frequent commits. This commit strategy ensures that there is a clear history of code implementation and progress over time. It also helps in tracking changes and reverting to previous versions if necessary.

### 6. Output and Parquet File Handling

The results of the pipeline are saved as **Parquet files** in different folders inside the `output_data` folder. These files were written without partitioning due to issues encountered when trying to install Hadoop on my Windows machine. For larger datasets or production environments, partitioning the Parquet files by relevant fields (e.g., `area` and `month`) would significantly improve read performance, especially when querying the data.

### 7. Balance Between Completeness and Time

Throughout the project, I sought to balance the completeness of the assignment with the time allocated. This led to certain trade-offs, such as avoiding unit test creation and not partitioning Parquet files due to system limitations. However, the pipeline successfully meets its core objectives and offers a scalable framework that can be expanded upon or modified for larger, more complex use cases in the future.
