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
hiveSQL_create_cards = '''
CREATE EXTERNAL TABLE IF NOT EXISTS cards(
  card struct<name: string, multiverseid: decimal(4,0), artist: string> ) COMMENT 'Magic Cards' ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
   LOCATION '/user/hadoop/mtg/raw/';
'''

#hiveQL_create_top_movies_external_table='''
#CREATE TABLE IF NOT EXISTS top_movies (
#    name STRING, 
#    multiverseid DECIMAL(4,0), 
#    artist STRING
#) STORED AS ORCFILE LOCATION '/user/hadoop/mtg/final/cards_';
#'''

hiveSQL_filter_cards = '''
SELECT  name STRING,
        multiverseid DECIMAL(4,0),
        artist STRING
FROM   cards
where 

'''

'''
CREATE EXTERNAL TABLE IF NOT EXISTS title_basics(
	tconst STRING,
	title_type STRING,
	primary_title STRING,
	original_title STRING,
	is_adult DECIMAL(1,0),
	start_year DECIMAL(4,0),
	end_year STRING,
	runtime_minutes INT,
	genres STRING
) COMMENT 'IMDb Movies' PARTITIONED BY (partition_year int, partition_month int, partition_day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb/cards'
TBLPROPERTIES ('skip.header.line.count'='1');
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
create_HiveTable_cards = HiveOperator(
    task_id='create_cards_table',
    hql=hiveSQL_create_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

create_local_import_dir >> clear_local_import_dir
create_local_import_dir >> clear_local_import_dir >> download_cards_method >> hdfs_put_cards >> create_HiveTable_cards


