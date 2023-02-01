from datetime import datetime, timedelta
from airflow import DAG
from operators.socrata_to_s3 import SocrataToS3Operator
from airflow.operators.python_operator import PythonOperator
from scripts.transformation.clean_data import clean_data
from groups.group_prepare import prepare_task
from groups.group_create_dwh_tables import create_dwh_tables_task
from groups.group_copy import copy_task
from groups.group_full_insert import full_insert_task

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": '',
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "incident-full-load-pipeline",
    schedule_interval=''
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
        full_load=True,
        from_date="2022-01-01",
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
            "valid_date": "2022-01-01",
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

    create_tables_tasks = create_dwh_tables_task(redshift_conn_id="redshift")

    copy_tasks = copy_task(
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        s3_bucket="{{ var.json.STAGING_AREA.STAGE_BUCKET }}",
        time_grain="{{ var.json.STAGING_AREA.TIME_STORE_GRAIN }}",
    )

    insert_tasks = full_insert_task(redshift_conn_id="redshift")

    (
        ingest_task
        >> clean_task
        >> prepare_tasks
        >> create_tables_tasks
        >> copy_tasks
        >> insert_tasks
    )
