# Logistics-Daily-Data-Movement

This project is built to showcase orchestrating the batch processing based on Airflow.

Tech Stacks that were utilized:
- Airflow
- GCP DataProc
- GCP Cloud Storage
- Hive


**Step 1: Creating the GCP Buckets**
>The project began with creating a bucket in GCP cloud storage where the daily input data will be placed. We need to make sure that the batch files have a field with 'date' having the current date, denoting the date of processing the data since we will be using it as a partition key (elaborate in further steps).

**Step 2: Building Hive Queries**
Before adding the hive query to the DAG, we need to analyse our requirements and build the hive query. In this scenario, the initial obvious process was to create a database and create an external table on top of the batch file being received. If we have any processed Spark file then we need to point it to the specific output director of the Spark job.
