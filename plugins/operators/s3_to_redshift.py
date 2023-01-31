"""
Custom operator for loading data from parquet file in S3 to Redshift.
"""
from contextvars import Context
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class S3ToRedshiftOperator(BaseOperator):
    """
    Copy data from Parquet file in S3 to table in Redshift

    :attr truncate_query: query used to truncate data in Redshift
    :attr copy_query: query used to copy data from S3 to Redshift
    :attr s3_path: S3 path to source Parquet file
    :attr template_fields: fields that need to be templated
    """

    truncate_query = "TRUNCATE TABLE {};"
    copy_query = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS PARQUET;
    """
    s3_path = "s3://{}/{}"
    template_fields = ("s3_bucket", "s3_key")

    @apply_defaults
    def __init__(
        self,
        aws_conn_id: str,
        redshift_conn_id: str,
        table_name: str,
        s3_bucket: str,
        s3_key: str,
        col_list: list = [],
        load_type: str = "append",
        *args,
        **kwargs,
    ):
        """
        Constructor for S3ToRedshiftOperator.
        :param aws_conn_id: AWS connection defined in Airflow connection
        :param redshift_conn_id: name of Redshift connection defined in Airflow
        :param table_name: name of table in Redshift
        :param s3_bucket: source bucket name
        :param s3_key: relative path to source parquet file
        :col_list: specific columns to copy data into if needed
        :load_type: types of loading data into table, include truncate and append
        """

        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.col_list = col_list
        self.load_type = load_type

    def execute(self, context: Context):
        """
        Function automatically runs when calling operator.

        :param context: context variable of runtime environment
        """
        # Initiate connection to relavant services
        aws_hook = AwsHook(aws_conn_id=self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Rendered s3 key with runtime variables
        rendered_s3_key = self.s3_key.format(**context)
        rendered_s3_path = self.s3_path.format(self.s3_bucket, rendered_s3_key)
        # Rendered truncate query
        if self.load_type == "truncate":
            self.copy_query = (
                self.truncate_query.format(self.table_name) + self.copy_query
            )
        # Rendered table columns
        if self.col_list:
            col_list_str = ", ".join(self.col_list)
            self.table_name = self.table_name + "(" + col_list_str + ")"
        # Rendered copy query
        rendered_copy_query = self.copy_query.format(
            self.table_name,
            rendered_s3_path,
            credentials.access_key,
            credentials.secret_key,
        )
        logging.info(f"-------------------{rendered_copy_query}")
        # Run formatted copy query
        redshift_hook.run(rendered_copy_query)
