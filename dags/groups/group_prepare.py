from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from scripts.transformation.prepare_data import PrepareData


def prepare_task(
    src_bucket: str = "",
    src_key: str = "",
    trg_bucket: str = "",
    time_grain: str = "",
    aws_conn_id: str = "",
):
    """
    Tasks perform preparing data before loading to staging table in data warehouse

    :param src_bucket: name of bucket where data is fetched (i.e raw bucket)
    :param src_key: relative path to source dataset
    :param trg_bucket: name of bucket where store stage data (i.e stage bucket)
    :param time_grain: time level for storing in bucket
    :param aws_conn_id: name of airflow connection containing key for accessing AWS
    """

    with TaskGroup("prepare_staging_data", tooltip="Prepare staging data") as group:

        prepare_dim_date_task = PythonOperator(
            task_id="prepare_stg_dim_date",
            python_callable=PrepareData.stg_dim_date,
            provide_context=True,
            op_kwargs={
                "src_bucket": src_bucket,
                "src_key": src_key,
                "trg_bucket": trg_bucket,
                "trg_key": "dim_date/" + time_grain + "/stage_date_{{ ds }}.parquet",
                "aws_conn_id": aws_conn_id,
            },
        )

        prepare_dim_time_task = PythonOperator(
            task_id="prepare_stg_dim_time",
            python_callable=PrepareData.stg_dim_time,
            provide_context=True,
            op_kwargs={
                "src_bucket": src_bucket,
                "src_key": src_key,
                "trg_bucket": trg_bucket,
                "trg_key": "dim_time/" + time_grain + "/stage_time_{{ ds }}.parquet",
                "aws_conn_id": aws_conn_id,
            },
        )

        prepare_dim_resolution_task = PythonOperator(
            task_id="prepare_stg_dim_resolution",
            python_callable=PrepareData.stg_dim_resolution,
            provide_context=True,
            op_kwargs={
                "src_bucket": src_bucket,
                "src_key": src_key,
                "trg_bucket": trg_bucket,
                "trg_key": "dim_resolution/"
                + time_grain
                + "/stage_resolution_{{ ds }}.parquet",
                "aws_conn_id": aws_conn_id,
            },
        )

        prepare_dim_police_district_task = PythonOperator(
            task_id="prepare_stg_dim_police_district",
            python_callable=PrepareData.stg_dim_police,
            provide_context=True,
            op_kwargs={
                "src_bucket": src_bucket,
                "src_key": src_key,
                "trg_bucket": trg_bucket,
                "trg_key": "dim_police_district/"
                + time_grain
                + "/stage_police_district_{{ ds }}.parquet",
                "aws_conn_id": aws_conn_id,
            },
        )

        prepare_dim_intersection_task = PythonOperator(
            task_id="prepare_stg_dim_intersection",
            python_callable=PrepareData.stg_dim_intersection,
            provide_context=True,
            op_kwargs={
                "src_bucket": src_bucket,
                "src_key": src_key,
                "trg_bucket": trg_bucket,
                "trg_key": "dim_intersection/"
                + time_grain
                + "/stage_intersection_{{ ds }}.parquet",
                "aws_conn_id": aws_conn_id,
            },
        )

        prepare_dim_category_task = PythonOperator(
            task_id="prepare_stg_dim_category",
            python_callable=PrepareData.stg_dim_category,
            provide_context=True,
            op_kwargs={
                "src_bucket": src_bucket,
                "src_key": src_key,
                "trg_bucket": trg_bucket,
                "trg_key": "dim_category/"
                + time_grain
                + "/stage_category_{{ ds }}.parquet",
                "aws_conn_id": aws_conn_id,
            },
        )

        prepare_fact_incident_task = PythonOperator(
            task_id="prepare_stg_fact_incident",
            python_callable=PrepareData.stg_fact_incident,
            provide_context=True,
            op_kwargs={
                "src_bucket": src_bucket,
                "src_key": src_key,
                "trg_bucket": trg_bucket,
                "trg_key": "fact_incident/"
                + time_grain
                + "/stage_incident_{{ ds }}.parquet",
                "aws_conn_id": aws_conn_id,
            },
        )

        return group
