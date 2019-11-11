# -*- coding: utf-8 -*-

"""
Title: Simple Example Dag 
Author: Marcel Mittelstaedt
Description: 
Just for educational purposes, not to be used in any productive mannor.
Downloads IMDb data, puts them into HDFS and creates HiveTable.
See Lecture Material: https://github.com/marcelmittelstaedt/BigData
"""

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

args = {
    'owner': 'airflow'
}

dag = DAG('IMDb', default_args=args, description='IMDb Import',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1
          )

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='imdb',
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/imdb',
    pattern='*',
    dag=dag,

)
download_cards_method  = HttpDownloadOperator(
        task_id='download_cards',
    download_uri='https://api.magicthegathering.io/v1/cards/4711',
    save_to='/home/airflow/imdb/cards_{{ ds }}.json',
    dag=dag,
)
dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

create_local_import_dir >> clear_local_import_dir
create_local_import_dir >> clear_local_import_dir >> download_cards_method  >> dummy_op


