# -*- coding: utf-8 -*-

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator, PythonOperator

import os
import sys

args = {
    'owner': 'airflow'
}
hiveSQL_create_cards = '''
CREATE EXTERNAL TABLE IF NOT EXISTS cards(
  name: string, 
  multiverseid: decimal(4,0), 
  artist: string) COMMENT 'Magic Cards' ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
   LOCATION '/user/hadoop/mtg/raw/';
'''



dag = DAG('Magic', default_args=args, description='IMDb Import',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 11, 11), catchup=False, max_active_runs=1
          )

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='magic',
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/magic',
    pattern='*',
    dag=dag,

)
download_cards_method  = HttpDownloadOperator(
        task_id='download_cards',
    download_uri='https://api.magicthegathering.io/v1/cards/',
    save_to='/home/airflow/magic/cards_{{ ds }}.json',
    dag=dag,
)
hdfs_put_cards = HdfsPutFileOperator(
    task_id='upload_cards_to_hdfs',
    local_file='/home/airflow/magic/cards_{{ ds }}.json',
    remote_file='/user/hadoop/mtg/raw/cards_{{ ds }}.json',
    hdfs_conn_id='hdfs',
    dag=dag,
)
create_HiveTable_cards = BashOperator(
    task_id='spark_python',
    bash_command='python /home/airflow/airflow/dags/scripts/Spark.py',
    
    dag=dag
)

dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

create_local_import_dir >> clear_local_import_dir
create_local_import_dir >> clear_local_import_dir >> download_cards_method >> hdfs_put_cards >> create_HiveTable_cards


