from datetime import datetime, timedelta
from airflow import DAG
from operators.socrata_to_s3 import SocrataToS3Operator
from airflow.operators.python_operator import PythonOperator
from scripts.transformation.clean_data import clean_data
from groups.group_prepare import prepare_task
from groups.group_copy import copy_task
from groups.group_incremental_insert import incremental_insert_task

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": "",
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "incident-incremental-pipeline",
    start_date="",
    schedule_interval="@daily",
    user_defined_macros={"redshift_conn_id": "redshift"},
) as dag:

    ingest_task = SocrataToS3Operator(
        task_id="ingest_Socrata_to_S3",
        dag=dag,
        http_conn_id="incident_api",
        endpoint="resource/wg3w-h783.json?",
        aws_conn_id="aws_credentials",
        s3_bucket="{{ var.json.STAGING_AREA.RAW_BUCKET }}",
        s3_key="incident-sf/"
        + "{{ var.json.STAGING_AREA.TIME_STORE_GRAIN }}"
        + "/raw_incident_{{ ds }}.json",
    )

    clean_task = PythonOperator(
        task_id="clean_data",
        dag=dag,
        python_callable=clean_data,
        provide_context=True,
        op_kwargs={
            "aws_conn_id": "aws_credentials",
            "src_s3_bucket": "{{ var.json.STAGING_AREA.RAW_BUCKET }}",
            "src_s3_key": "incident-sf/"
            + "{{ var.json.STAGING_AREA.TIME_STORE_GRAIN }}"
            + "/raw_incident_{{ ds }}.json",
            "trg_s3_bucket": "{{ var.json.STAGING_AREA.CLEANSED_BUCKET }}",
            "trg_s3_key": "incident-sf/"
            + "{{ var.json.STAGING_AREA.TIME_STORE_GRAIN }}"
            + "/cleansed_incident_{{ ds }}.parquet",
        },
    )

    prepare_tasks = prepare_task(
        src_bucket="{{ var.json.STAGING_AREA.CLEANSED_BUCKET }}",
        src_key="incident-sf/"
        + "{{ var.json.STAGING_AREA.TIME_STORE_GRAIN }}"
        + "/cleansed_incident_{{ ds }}.parquet",
        trg_bucket="{{ var.json.STAGING_AREA.STAGE_BUCKET }}",
        time_grain="{{ var.json.STAGING_AREA.TIME_STORE_GRAIN }}",
        aws_conn_id="aws_credentials",
    )

    copy_tasks = copy_task(
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        s3_bucket="{{ var.json.STAGING_AREA.STAGE_BUCKET }}",
        time_grain="{{ var.json.STAGING_AREA.TIME_STORE_GRAIN }}",
    )

    insert_tasks = incremental_insert_task(redshift_conn_id="redshift")

    (ingest_task >> clean_task >> prepare_tasks >> copy_tasks >> insert_tasks)
