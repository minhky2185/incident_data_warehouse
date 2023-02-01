"""
Custom operator for ingesting data from Socrata open data API into AWS S3 bucket in JSON.
"""
from contextvars import Context
import logging
import sys
import requests

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.http_hook import HttpHook


class ObjectSizeError(Exception):
    """
    Class for raising error when file size exceed the boundary.
    """

    def __init__(self):
        """
        Constructor for ObjectSizeError.
        """
        self.message = "Size of object is too large"

    def __str__(self):
        """
        Message returning when error occurs.
        """
        return f"Size exceeded error. {self.message}"


class SocrataToS3Operator(BaseOperator):
    """
    Ingest data from Socrata opendata API into S3 bucket.

    :attr load_query: query used to load data from Socrate API
    :attr s3_log_key: key for storing logs in S3 bucket
    :attr template_field: fields that need to be templated
    """

    load_query = '$query=SELECT * WHERE :created_at >= "{ds}" or :updated_at >= "{ds}"'
    s3_log_key = "ingest_logs/{ds}.json"
    template_fields = ("s3_bucket", "s3_key")

    @apply_defaults
    def __init__(
        self,
        http_conn_id: str,
        endpoint: str,
        aws_conn_id: str,
        s3_bucket: str,
        s3_key: str,
        method: str = "GET",
        max_size: int = 5000000000,
        limit_row_return: int = 1000000,
        full_load: bool = False,
        from_date: str = None,
        *args,
        **kwargs,
    ):
        """
        Constructor for SocrataToS3Operator class.

        :param http_conn_id: name of http connection that is defined in Airflow connection
        :param endpoint: the endpoint to be called
        :param aws_conn_id: AWS connection defined in Airflow connection
        :param s3_bucket: destination bucket name
        :param s3_key: relative path in destination bucket
        :method: the API method to be called
        :max_size: threshold (in bytes) to check the size of ingested data
        :limit_row_return: limit number of row ingested from source
        :full_load: determine full load or incremental load
        :from_date: historical date for execute full load if needed
        """

        super().__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.max_size = max_size
        self.limit_row_return = limit_row_return
        self.full_load = full_load
        self.from_date = from_date

    def get_metadata(self, respond: requests.Response):
        """
        Parsing header and return necessary metadata as well as datetime object used for easily
        naming log data.

        :param respond: respond get after calling the API
        """
        try:
            headers = respond.headers
            # Parse metadata
            metadata = {
                "Date": headers["Date"],
                "Content-Type": headers["Content-Type"],
                "Content-Encoding": headers["Content-Encoding"],
                "Transfer-Encoding": headers["Transfer-Encoding"],
                "Region": headers["X-Socrata-Region"],
            }
        except LookupError:
            metadata = {"Metadata is missing. Please check error log."}

        return metadata

    def execute(self, context: Context):
        """
        Function automatically runs when calling operator.

        :param context: context variable of runtime environment
        """
        # Initiate connections to source and destination
        socrata = HttpHook(method=self.method, http_conn_id=self.http_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        # Render load query for requesting Socrata API
        if self.full_load:
            if self.from_date:
                load_query_rendered = self.load_query.format(ds=self.from_date)
                load_query_rendered = (
                    load_query_rendered + f" LIMIT {self.limit_row_return}"
                )
            else:
                load_query_rendered = f"$limit={self.limit_row_return}"
        else:
            load_query_rendered = self.load_query.format(**context)
            load_query_rendered = (
                load_query_rendered + f" LIMIT {self.limit_row_return}"
            )
        logging.info(load_query_rendered)

        # Get the response from the source
        response = socrata.run(endpoint=self.endpoint, data=load_query_rendered)

        # Check size of data ingested from source
        if sys.getsizeof(response) <= self.max_size:
            logging.info("Size of data conforms the size limit")
        else:
            raise ObjectSizeError

        # Render key with context variable
        s3_key_rendered = self.s3_key.format(**context)
        s3_log_key_rendered = self.s3_log_key.format(**context)
        # Content and key for log data
        metadata = self.get_metadata(response)
        # Ingest data into destination s3 bucket
        s3_hook.load_string(
            string_data=response.text, bucket_name=self.s3_bucket, key=s3_key_rendered
        )
        # Ingest log content into destination s3 bucket
        s3_hook.load_string(
            string_data=str(metadata),
            bucket_name=self.s3_bucket,
            key=s3_log_key_rendered,
        )
