import requests
import apikey
import json
import time
import os
import glob
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")


def access_api():
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': apikey.key,
    }

    params = {
        "start" : "1",
        "limit": "5000",
        "convert": "USD"
    }

    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

    json_response = requests.get(url, params=params, headers=headers).json()

    json_object = json.dumps(json_response["data"])
  
    with open(os.path.join(AIRFLOW_HOME, "data.json"), "w") as outfile:
        outfile.write(json_object)


def process_with_spark():
    spark = SparkSession.builder.master("local[*]").appName('test').getOrCreate()

    df = spark.read.json(os.path.join(AIRFLOW_HOME, "data.json"))

    df = df.select("name", "date_added", "id", "last_updated", "max_supply", "total_supply", "circulating_supply", "cmc_rank", "symbol", "quote.USD.price", "quote.USD.volume_24h", "quote.USD.volume_change_24h", "quote.USD.percent_change_1h", "quote.USD.percent_change_24h", "quote.USD.percent_change_30d")
    df = df.withColumn("date_added", to_timestamp("date_added"))
    df = df.withColumn("last_updated", to_timestamp("last_updated"))
    #df.printSchema()
    df.write.mode("overwrite").json(os.path.join(AIRFLOW_HOME, "output"))
    #df.show()


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    local_file = glob.glob(local_file)[0]

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

    #os.remove(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag_crypto_data",
    start_date=datetime(2022, 4, 1),
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    file_name = str(int(time.time())) + ".json"

    # download_dataset_task = PythonOperator(
    #     task_id="download_data_from_api",
    #     python_callable=access_api
    # )

    transform_data_with_spark = PythonOperator(
        task_id="transform_data_with_spark",
        python_callable=process_with_spark,
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"crypto_data_raw/" + file_name,
            "local_file": f"{AIRFLOW_HOME}/output/*.json",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "JSON",
                "sourceUris": [f"gs://{BUCKET}/crypto_data_raw/" + file_name],
            },
        },
    )

    download_dataset_task >> transform_data_with_spark >> local_to_gcs_task >> bigquery_external_table_task
