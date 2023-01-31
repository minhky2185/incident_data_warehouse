"""
Class for interacting with S3 Buckets using S3 Hook
"""
from io import BytesIO
from io import StringIO
import pandas as pd
from airflow.hooks.S3_hook import S3Hook


class S3BucketConnector:
    """
    Class for interacting with S3 Buckets.
    """

    def __init__(self, aws_conn_id: str, s3_bucket: str):
        """
        Constructor for S3BucketConnector

        :param aws_credential: AWS connection defined in Airflow connection
        :param s3_bucket: S3 bucket name
        """
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        self.s3_bucket = s3_bucket

    def read_json_from_s3(self, s3_key: str, decoding: str = "utf-8"):
        """
        Return a Pandas dataframe read from Json object in S3 bucket.

        :param s3_key: relative path in source bucket
        :decoding: encoding system for object
        """
        # Get desired object
        s3_obj = self.s3_hook.get_key(key=s3_key, bucket_name=self.s3_bucket)
        # Parse object
        json_obj = s3_obj.get().get("Body").read().decode(decoding)
        # Get the dataframe
        data_frame = pd.read_json(json_obj)

        return data_frame

    def read_prq_from_s3(self, s3_key: str):
        """
        Return a Pandas dataframe read from Parquet object in S3 bucket.

        :param s3_key: relative path in source bucket
        """
        # Get desired object
        s3_obj = self.s3_hook.get_key(key=s3_key, bucket_name=self.s3_bucket)
        buffer = BytesIO()
        # Convert file object to binary mode
        s3_obj.download_fileobj(buffer)
        # Get the data frame
        data_frame = pd.read_parquet(buffer)

        return data_frame

    def write_df_to_s3_prq(self, s3_key: str, data_frame: pd.DataFrame):
        """
        Write a Pandas dataframe to S3 bucket in Parquet format.

        :param s3_key: key for storing in target bucket
        :parma data_frame: dataframe need to be written to target S3 bucket
        """
        out_buffer = BytesIO()
        data_frame.to_parquet(out_buffer, index=False)
        out_buffer.seek(0, 0)
        self.s3_hook.load_file_obj(out_buffer, key=s3_key, bucket_name=self.s3_bucket)

    def write_df_to_s3_json(self, s3_key: str, data_frame: pd.DataFrame):
        """
        Write a Pandas dataframe to S3 bucket in Json format.

        :param s3_key: key for storing in target bucket
        :parma data_frame: dataframe need to be written to target S3 bucket
        """
        self.s3_hook.load_string(
            data_frame.to_json(), key=s3_key, bucket_name=self.s3_bucket
        )
