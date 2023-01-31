from airflow.utils.task_group import TaskGroup
from operators.s3_to_redshift import S3ToRedshiftOperator


def copy_task(
    aws_conn_id: str = "",
    redshift_conn_id: str = "",
    s3_bucket: str = "",
    time_grain: str = "",
):
    """
    Copy stage data from S3 into stage tables in Redshift data warehouse

    :param aws_conn_id: name of airflow connection to AWS
    :param redshift_conn_id: name of airflow connection to Redshift
    :param s3_bucket: source bucket name
    :param time_grain: time level for storing in bucket
    """
    with TaskGroup(
        "copy_data_to_redshift_tasks", tooltip="Copy data to Redshift"
    ) as group_copy:

        copy_stg_date_task = S3ToRedshiftOperator(
            task_id="copy_stg_dim_date",
            aws_conn_id=aws_conn_id,
            redshift_conn_id=redshift_conn_id,
            table_name="stg.dim_date",
            s3_bucket=s3_bucket,
            s3_key="dim_date/" + time_grain + "/stage_date_{{ ds }}.parquet",
            load_type="truncate",
        )

        copy_stg_time_task = S3ToRedshiftOperator(
            task_id="copy_stg_dim_time",
            aws_conn_id=aws_conn_id,
            redshift_conn_id=redshift_conn_id,
            table_name="stg.dim_time",
            s3_bucket=s3_bucket,
            s3_key="dim_time/" + time_grain + "/stage_time_{{ ds }}.parquet",
            load_type="truncate",
        )

        copy_stg_resolution_task = S3ToRedshiftOperator(
            task_id="copy_stg_dim_resolution",
            aws_conn_id=aws_conn_id,
            redshift_conn_id=redshift_conn_id,
            table_name="stg.dim_resolution",
            s3_bucket=s3_bucket,
            s3_key="dim_resolution/"
            + time_grain
            + "/stage_resolution_{{ ds }}.parquet",
            load_type="truncate",
        )

        copy_stg_police_district_task = S3ToRedshiftOperator(
            task_id="copy_stg_dim_police_district",
            aws_conn_id=aws_conn_id,
            redshift_conn_id=redshift_conn_id,
            table_name="stg.dim_police_district",
            s3_bucket=s3_bucket,
            s3_key="dim_police_district/"
            + time_grain
            + "/stage_police_district_{{ ds }}.parquet",
            load_type="truncate",
        )

        copy_stg_intersection_task = S3ToRedshiftOperator(
            task_id="copy_stg_dim_intersection",
            aws_conn_id=aws_conn_id,
            redshift_conn_id=redshift_conn_id,
            table_name="stg.dim_intersection",
            s3_bucket=s3_bucket,
            s3_key="dim_intersection/"
            + time_grain
            + "/stage_intersection_{{ ds }}.parquet",
            load_type="truncate",
        )

        copy_stg_category_task = S3ToRedshiftOperator(
            task_id="copy_stg_dim_category",
            aws_conn_id=aws_conn_id,
            redshift_conn_id=redshift_conn_id,
            table_name="stg.dim_category",
            s3_bucket=s3_bucket,
            s3_key="dim_category/" + time_grain + "/stage_category_{{ ds }}.parquet",
            load_type="truncate",
        )

        copy_stg_incident_task = S3ToRedshiftOperator(
            task_id="copy_stg_fact_incident",
            aws_conn_id=aws_conn_id,
            redshift_conn_id=redshift_conn_id,
            table_name="stg.fact_incident",
            s3_bucket=s3_bucket,
            s3_key="fact_incident/" + time_grain + "/stage_incident_{{ ds }}.parquet",
            load_type="truncate",
        )

        return group_copy
