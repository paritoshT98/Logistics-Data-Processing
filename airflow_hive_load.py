from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitHiveJobOperator
from airflow.operators.bash_operator import BashOperator


# Defining the varible being declared in the Airflow UI > Variables section.
config = Variable.get("cluster_details",deserialize_json=True)

default_args = {
    'owner':'airflow',
    'depend_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    "batch_hive_job",
    default_args=default_args,
    description='A DAG to run Hive Job on Dataproc',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023,9,1),
    tags=['dev_hive']
    
)
# Sensing the data from GCS location. The poke_interval is 30 seconds which means after ever 30 seconds the sensor will look for data in the input folder.
sense_logistics_file = GCSObjectsWithPrefixExistenceSensor(
    task_id = 'sense_logistics_file',
    bucket = 'logistic-bucket',
    prefix = 'input-delta-data/logistics_',
    mode = 'poke',
    timeout = 300,
    poke_interval = 30,
    dag=dag
)

create_hive_database = DataprocSubmitHiveJobOperator(
    task_id = 'create_hive_database',
    query=""""
        CREATE DATABASE IF NOT EXISTS logistics_db;
        """,
    cluster_name = config['CLUSTER_NAME'],
    region = config['REGION'],
    project_id = config['PROJECT_ID'],
    dag=dag
)

create_hive_table = DataprocSubmitHiveJobOperator(
    task_id = 'create_hive_table',
    query="""
        CREATE EXTERNAL TABLE IF NOT EXISTS logistics_db.logistics_data (
            delivery_id INT,
            `date` STRING,
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
        """,
    cluster_name = config['CLUSTER_NAME'],
    region = config['REGION'],
    project_id = config['PROJECT_ID'],
    dag=dag
)

create_partitioned_table = DataprocSubmitHiveJobOperator(
    task_id = 'create_partitioned_table',
    query="""
        CREATE EXTERNAL TABLE IF NOT EXISTS logistics_db.logistics_data_partitioned (
            delivery_id INT,
            origin_state STRING,
            destination_state STRING,
            delivery_status STRING,
            delivery_time STRING
        )
        PARTITIONED BY (`date` STRING)
        STORED AS TEXTFILE;
        """,
    cluster_name = config['CLUSTER_NAME'],
    region = config['REGION'],
    project_id = config['PROJECT_ID'],
    dag=dag
)

set_hive_properties_and_load_partitioned = DataprocSubmitHiveJobOperator(
    task_id = 'set_hive_properties_and_load_partitioned',
    query=f"""
        SET hive.exec.dynamic.partition = true;
        SET hive.exec.dynamic.partition.mode = nonstrict;
        
        INSERT INTO logistics_db.logistics_data_partitioned
        SELECT delivery_id, origin_state, destination_state, delivery_status, delivery_time, `date` FROM logistics_db.logistics_data;
    """,
    cluster_name = config['CLUSTER_NAME'],
    region = config['REGION'],
    project_id = config['PROJECT_ID'],
    dag=dag
)

# Archiving file
archive_processed_file = BashOperator(
    task_id='archive_processed_file',
    bash_command = f"gsutil -m mv gs://logistic-bucket/input-delta-data/logistics_*.csv gs://logistic-bucket/archive-delta-file/",
    dag=dag
)

# DAG Dependencies
sense_logistics_file>>create_hive_database>>create_hive_table>>create_partitioned_table>>set_hive_properties_and_load_partitioned>>archive_processed_file


