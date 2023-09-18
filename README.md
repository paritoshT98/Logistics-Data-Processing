# Logistics-Daily-Data-Movement

This project is built to showcase orchestrating the batch processing based on Airflow.

Tech Stacks that were utilized:
- Airflow
- GCP DataProc
- GCP Cloud Storage
- Hive


### Step 1: Creating the GCP Buckets
The project began with creating a bucket in GCP cloud storage where the daily input data will be placed. We need to make sure that the batch files have a field with 'date' having the current date, denoting the date of processing the data since we will be using it as a partition key (elaborate in further steps).

### Step 2: Defining Hive Queries
Before adding the hive query to the DAG, we need to analyze our requirements and build the hive query. In this scenario, the initial obvious process was to create a database. Our goal is to load the data into a partitioned table, however, we cannot load the data into a partition table from a file that is dumped in any of the locations. Therefore, we need to create an external table on top of the batch file being received and load the data into the partitioned target table. If we have any processed Spark file then we need to point it to the specific output directory of the Spark job.

Create database:

    CREATE DATABASE IF NOT EXISTS logistics_db;

Create external table:

    CREATE EXTERNAL TABLE IF NOT EXISTS logistics_db.logistics_data (
        delivery_id INT,
        date STRING,
        origin_state STRING,
        destination_state STRING,
        delivery_status STRING,
        delivery_time STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'gs://logistic-bucket/input-delta-data/'
    tblproperties('skip.header.line.count'='1');
            

Create Partitioned Table:

    CREATE EXTERNAL TABLE IF NOT EXISTS logistics_db.logistics_data_partitioned (
        delivery_id INT,
        origin_state STRING,
        destination_state STRING,
        delivery_status STRING,
        delivery_time STRING
    )
    PARTITIONED BY (date STRING)
    STORED AS TEXTFILE;

Set HIVE properties for and loading data into the partition table:

    SET hive.exec.dynamic.partition = true;
    SET hive.exec.dynamic.partition.mode = nonstrict;
    
    INSERT INTO logistics_db.logistics_data_partitioned
    SELECT delivery_id, origin_state, destination_state, delivery_status, delivery_time, `date` FROM logistics_db.logistics_data;

### Step 3: Building Airflow DAG
While building the airflow script, we would need 3 operators
- _GCSObjectsWithPrefixExistenceSensor_: This will sense the daily data file in the GCP location being added in the configuration.
- _DataprocSubmitHiveJobOperator_: Primarily this is being used to execute the HIVE command there would we would need to define this operator 4 times respectively.
- _BashOperator_: Once we have executed the file for hive execution, we need to move the processed file into a different location because if the file is not moved and the new file comes to us the next day, we will end up getting the redundant/duplicate data in the hive tables. To move the file from the input location to the archival location, this operator will come into play.

Further, we need to mention the proper sequence of the tasks to state the dependencies while in the execution.

In the airflow script, I have declared the variable parameter which is being defined in the Airflow UI > Variables section. This would help us to pass the parameter for the cluster details, project id, and region. The format for declaring the variable should be a dictionary data type or JSON format. Please make sure you have a DataProc cluster.

      Key: 'cluster_details'
      Value: {'CLUSTER_NAME':'cluster-1a2b', 'REGION'='us-central1', 'PROJECT_ID'='spark-393408'}

### Step 4: Execution
Place the file in the DAG folder of the airflow environment. After a few minutes, the DAG will run automatically which could be monitored in the airflow UI. If there is no file in the GCS-defined location then the sensor task will run for 300 minutes (as we have mentioned in the DAG configuration). Once we place the file the workflow will proceed further.

### Step 5: Monitoring
You will notice that the DataProc will be starting a few of the jobs based on how many files you have added in the GCS storage. Further, you can notice in Hive, while having an SSH connection in the master node, that there would be a database, table, and partitioned table being created.
To view the database and table:

    show databases;
    show tables;
    
To view the partitions:

    hadoop fs -ls /user/hive/warehouse/logistics_db.db/logistics_data_partitioned/


Please refer to the airflow_load_hive.py script to be able to see the configuration in detail.
