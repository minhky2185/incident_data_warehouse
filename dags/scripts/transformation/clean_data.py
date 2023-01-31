"Do transformation on raw data and load transformed data to cleansed zone"
from scripts.common.s3 import S3BucketConnector


def clean_data(
    aws_conn_id: str,
    src_s3_bucket: str,
    src_s3_key: str,
    trg_s3_bucket: str,
    trg_s3_key: str,
    valid_date: str = None,
    *args,
    **kwargs
):
    """
    Transform raw data and load to cleansed zone.

    :param aws_conn_id: AWS connection defined in Airflow connection
    :param src_s3_bucket: name of bucket containing raw data
    :param src_s3_key: relative path for reading raw data file in source bucket
    :param trg_s3_bucket: name of bucket containing transformed data
    :param trg_s3_key: relative path for writing transformed data file in target bucket
    :valid_date: the incident date that you want to begin data warehouse
    """
    # Initiate bucket connection
    src_s3_conn = S3BucketConnector(aws_conn_id=aws_conn_id, s3_bucket=src_s3_bucket)
    trg_bucket_conn = S3BucketConnector(
        aws_conn_id=aws_conn_id, s3_bucket=trg_s3_bucket
    )
    # Read raw data as dataframe
    raw_df = src_s3_conn.read_json_from_s3(s3_key=src_s3_key.format(**kwargs))
    # Filter desired columns
    columns_to_filter = [
        "incident_datetime",
        "report_datetime",
        "incident_id",
        "report_type_description",
        "incident_code",
        "incident_category",
        "incident_subcategory",
        "incident_description",
        "resolution",
        "police_district",
        "intersection",
        "cnn",
        "latitude",
        "longitude",
    ]
    incident_df = raw_df[columns_to_filter]
    # Eliminate null values
    incident_df.dropna(inplace=True)
    # Eliminate unnecessary rows
    incident_df = incident_df[incident_df["police_district"] != "Out of SF"]
    incident_df = incident_df[
        incident_df["report_type_description"].str.contains("Supplement") == False
    ]
    # Drop unnecessary columns
    incident_df = incident_df.drop(columns=["report_type_description"])
    # Filter the valid date
    if valid_date:
        incident_df = incident_df[incident_df["incident_datetime"] >= valid_date]
    # Correct columns data types
    incident_df = incident_df.astype(
        {
            "incident_datetime": "datetime64[ns]",
            "report_datetime": "datetime64[ns]",
            "cnn": "int32",
        }
    )
    # Write transformed dataframe to cleansed zone
    trg_bucket_conn.write_df_to_s3_prq(
        s3_key=trg_s3_key.format(**kwargs), data_frame=incident_df
    )
