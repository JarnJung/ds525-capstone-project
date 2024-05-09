import logging
import re
import os
import datetime
import pytz
import csv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.utils import timezone
import pendulum
from google.cloud import bigquery
import requests
import json

def _upload_stn_to_gcs():
    # Create a LocalFilesystemToGCSOperator to upload the file to GCS
    gcs_csv_object = "cleaned_station/cleaned_station.csv"  # Specify the desired object name here
    
    upload_task = LocalFilesystemToGCSOperator(
        task_id="upload_stn_to_gcs",
        src="/opt/airflow/dags/cleaned_station.csv",
        dst=gcs_csv_object,
        bucket= "ds525-capstone-test-49",
        gcp_conn_id="my_gcp_conn",
    )
    upload_task.execute(context=None)

with DAG (
    "load_station",
    start_date=timezone.datetime(2024, 5, 9, 10, 0, 0, tzinfo=pytz.UTC),
    schedule = None, #cron expression
    tags=["DS525 Capstone"],
):

    upload_stn_to_gcs = PythonOperator(
        task_id="upload_stn_to_gcs",
        python_callable=_upload_stn_to_gcs,
    )

    load_stn_to_bq = GCSToBigQueryOperator(
        task_id="load_stn_to_bq",
        source_objects=["cleaned_station/cleaned_station.csv"],
        bucket="ds525-capstone-test-49",
        destination_project_dataset_table="ds525-capstone.ds525_capstone_db.station_data",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "station_id", "type": "STRING"},
            {"name": "name_th", "type": "STRING"},
            {"name": "name_en", "type": "STRING"},
            {"name": "area_th", "type": "STRING"},
            {"name": "area_en", "type": "STRING"},
            {"name": "station_type", "type": "STRING"},
            {"name": "location", "type": "STRING"},
        ],
        autodetect=True,  # Set autodetect to False since the schema is predefined        
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="my_gcp_conn",
    )

    upload_stn_to_gcs >> load_stn_to_bq