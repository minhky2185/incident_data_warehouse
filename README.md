
# **Incident Data Warehouse**
The project aims to build a data warehouse for incidents that happened in San Francisco from scratch. There are 2 pipelines in this project:
- `full_load_pipeline`: Load all data to the data warehouse for the first load.
- `incremental_load_pipeline`: Run daily and load new data into the data warehouse as well as manage changes in data by implementing SCD Types 1 and 2.


## **Tech Stack**

**Staging Area:** Amazon S3

**Data warehouse:** Amazon Redshift

**Visualization:** Power BI

**Orchestration:** Apache Airflow with Docker

**Processing:** Python


## **Architecture**
The architecture of this project is presented as follows:

![Architecture_1](https://github.com/minhky2185/incident_data_warehouse/blob/main/images/architecture_1.png)

- Data is sourced from [Socrata API](https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783) and ingested into `raw zone` of Staging Area hosted on S3.
- Raw data is cleansed and standardized before moving to `cleansed zone`.
- Cleansed data is transformed into data model used in data warehouse and loaded into `stage zone`. Now the data is ready for moving to data warehouse.
- Reports are created in Power BI from the data in data warehouse.


## **Data Model**
- The data warehouse schema is designed follow `Star schema` model. 
- To manage the relation many to many between incidents and incident categories, the `bridge table` is used.
- To manage changes in data, `SCD Type 1` is applied to all table, `SCD Type 2` applied to `dim_category` and `dim_intersection` tables.

<p align="center">
  <img src="https://github.com/minhky2185/incident_data_warehouse/blob/main/images/data_model.png">
</p>

## **ETL Pipeline**
- To manage the repetitive jobs of ingest (from source to staging area) and load to data warehouse, I build 2 Airflow custom operators are `Socrata_to_S3` and `S3_to_Redshift`.
- Full load pipeline architecture

![full_load](https://github.com/minhky2185/incident_data_warehouse/blob/main/images/full_load.png)

- Full load pipeline architecture (zoom in)

![full_load_zoom_in](https://github.com/minhky2185/incident_data_warehouse/blob/main/images/full_load_zoom_in.png)

- Incremental load pipeline architecture

![incre_load](https://github.com/minhky2185/incident_data_warehouse/blob/main/images/incre_load.png)

- Incremental load pipeline architecture (zoom in)

![incre_load_zoom_in](https://github.com/minhky2185/incident_data_warehouse/blob/main/images/incre_load_zoom_in.png)

## **Visualization**
Some dashboards create from the data from data warehouse
- Total report for year 2022

![year_report](https://github.com/minhky2185/incident_data_warehouse/blob/main/images/year_report.png)

- Daily report for everyday 

![daily_report](https://github.com/minhky2185/incident_data_warehouse/blob/main/images/daily_report.png)

## **Achievement in learning**
### Python
- Use Python to process data, especially the date time datatype. Most used libraries to process data are Pandas and Numpy.
- Implement OOP in the project.
- Structure files in project.
### Apache Airflow
- Write custom operators for repetive jobs (code)
- Use runtime variable and template variable
- Group tasks that belong to each stage of the pipeline for more briefness when looking
- Connect to cloud services through Hook
- Secure connections and other important variables by using Variable and Connection features
- Implement Airflow using Docker
- Components of Airflow
### AWS
- Use S3 as the Staging Area
- Use Redshift as the data warehouse
- How Redshift works
### Visualization
- Connect to Redshift and creata dashboard
