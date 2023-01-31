"""
Prepare data for feeding into staging dimensional model in data warehouse.
"""
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
import numpy as np
from scripts.common.s3 import S3BucketConnector


class PrepareData:
    """
    Prepare data before loading into data warehouse.
    """

    @staticmethod
    def init_connection(
        src_bucket: str,
        trg_bucket: str,
        aws_conn_id: str,
        rendered_src_key: str,
    ):
        """
        Return source dataframe and connection to target bucket

        :param src_bucket: name of bucket containing source data
        :param trg_bucket: name of bucket containing transformed data
        :param aws_conn_id: AWS connection defined in Airflow connection
        :param rendered_src_key: complete s3 key to source file
        """
        src_bucket_conn = S3BucketConnector(
            aws_conn_id=aws_conn_id, s3_bucket=src_bucket
        )
        src_df = src_bucket_conn.read_prq_from_s3(s3_key=rendered_src_key)
        trg_bucket_conn = S3BucketConnector(
            aws_conn_id=aws_conn_id, s3_bucket=trg_bucket
        )

        return src_df, trg_bucket_conn

    @staticmethod
    def stg_dim_date(
        src_bucket: str,
        src_key: str,
        trg_bucket: str,
        trg_key: str,
        aws_conn_id: str,
        *args,
        **kwargs,
    ):
        """
        Prepare data for loading to stg_dim_date table in data warehouse.

        :param src_bucket: name of bucket containing source data
        :param src_key: relative path to source dataset
        :param trg_bucket: name of bucket containing transformed data
        :param trg_key: relative path for storing transform data
        :param aws_conn_id: AWS connection defined in Airflow connection
        """
        src_df, trg_bucket_conn = PrepareData.init_connection(
            src_bucket=src_bucket,
            trg_bucket=trg_bucket,
            aws_conn_id=aws_conn_id,
            rendered_src_key=src_key.format(**kwargs),
        )
        # Get a centralize datetime column from source data
        df_incident_datetime = src_df[["incident_datetime"]].rename(
            columns={"incident_datetime": "dt_col"}
        )
        df_report_datetime = src_df[["report_datetime"]].rename(
            columns={"report_datetime": "dt_col"}
        )
        df_stg_dim_date = pd.concat(
            (df_incident_datetime, df_report_datetime), ignore_index=True
        )
        # Create columns conform date dimension table
        df_stg_dim_date["stg_date_id"] = df_stg_dim_date["dt_col"].dt.strftime("%Y%m%d")
        df_stg_dim_date = df_stg_dim_date.astype({"stg_date_id": "int64"})
        df_stg_dim_date["stg_full_date"] = df_stg_dim_date["dt_col"].dt.date
        df_stg_dim_date = df_stg_dim_date.astype({"stg_full_date": "str"})
        df_stg_dim_date["stg_year"] = df_stg_dim_date["dt_col"].dt.year
        df_stg_dim_date["stg_month"] = df_stg_dim_date["dt_col"].dt.month
        df_stg_dim_date["stg_month_name"] = df_stg_dim_date["dt_col"].dt.month_name()
        df_stg_dim_date["stg_day_of_month"] = df_stg_dim_date["dt_col"].dt.day
        df_stg_dim_date["stg_day_of_week"] = df_stg_dim_date["dt_col"].dt.dayofweek
        df_stg_dim_date["stg_day_name"] = df_stg_dim_date["dt_col"].dt.day_name()
        df_stg_dim_date["stg_quarter"] = df_stg_dim_date["dt_col"].dt.to_period("Q")
        # Column holiday indicator includes 2 values are 'Holiday' and 'Non-Holiday'
        cal = USFederalHolidayCalendar()
        holidays = cal.holidays(
            start=df_stg_dim_date["dt_col"].min(), end=df_stg_dim_date["dt_col"].max()
        )
        df_stg_dim_date["holiday_indicator"] = (
            df_stg_dim_date["dt_col"].dt.date.astype("datetime64").isin(holidays)
        )
        df_stg_dim_date["stg_holiday_indicator"] = np.where(
            df_stg_dim_date["holiday_indicator"], "Holiday", "Non-Holiday"
        )
        # Column weekday indicator includes 2 values are 'Weekday' and 'Weekend'
        df_stg_dim_date["stg_weekday_indicator"] = np.where(
            df_stg_dim_date["stg_day_of_week"] > 4, "Weekend", "Weekday"
        )
        # Remove duplicate date data and drop unnecessary columns
        df_stg_dim_date = df_stg_dim_date.drop_duplicates(subset=["stg_date_id"]).drop(
            columns=["dt_col", "holiday_indicator"]
        )
        # Change int64 columns to int32 columns to compatible with `int`` datatype in Redshift
        df_stg_dim_date = df_stg_dim_date.astype(
            {col: "int32" for col in df_stg_dim_date.select_dtypes("int64").columns}
        )
        df_stg_dim_date = df_stg_dim_date.astype({"stg_quarter": "str"})
        # Load processed dataframe to S3
        trg_bucket_conn.write_df_to_s3_prq(
            s3_key=trg_key.format(**kwargs), data_frame=df_stg_dim_date
        )

    @staticmethod
    def stg_dim_time(
        src_bucket: str,
        src_key: str,
        trg_bucket: str,
        trg_key: str,
        aws_conn_id: str,
        *args,
        **kwargs,
    ):
        """
        Prepare data for loading to stg_dim_time table in data warehouse.

        :param src_bucket: name of bucket containing source data
        :param src_key: relative path to source dataset
        :param trg_bucket: name of bucket containing transformed data
        :param trg_key: relative path for storing transform data
        :param aws_conn_id: AWS connection defined in Airflow connection
        """
        src_df, trg_bucket_conn = PrepareData.init_connection(
            src_bucket=src_bucket,
            trg_bucket=trg_bucket,
            aws_conn_id=aws_conn_id,
            rendered_src_key=src_key.format(**kwargs),
        )
        # Get a centralize datetime column from source data
        df_incident_datetime = src_df[["incident_datetime"]].rename(
            columns={"incident_datetime": "dt_col"}
        )
        df_report_datetime = src_df[["report_datetime"]].rename(
            columns={"report_datetime": "dt_col"}
        )
        df_stg_dim_time = pd.concat(
            (df_incident_datetime, df_report_datetime), ignore_index=True
        )
        # Create columns conform time dimension table
        df_stg_dim_time["stg_full_time"] = df_stg_dim_time["dt_col"].dt.strftime(
            "%H:%M"
        )
        df_stg_dim_time["stg_hour"] = df_stg_dim_time["dt_col"].dt.hour
        df_stg_dim_time["stg_minute"] = df_stg_dim_time["dt_col"].dt.minute
        df_stg_dim_time["stg_daytime_indicator"] = np.where(
            (df_stg_dim_time["stg_hour"] >= 6) & (df_stg_dim_time["stg_hour"] <= 18),
            "Day",
            "Night",
        )
        # Remove duplicate time data and drop unnecessary columns
        df_stg_dim_time = df_stg_dim_time.drop_duplicates(
            subset=["stg_hour", "stg_minute"]
        ).drop(columns="dt_col")
        # Change int64 columns to int32 columns
        df_stg_dim_time = df_stg_dim_time.astype(
            {col: "int32" for col in df_stg_dim_time.select_dtypes("int64").columns}
        )
        # Load processed dataframe to S3
        trg_bucket_conn.write_df_to_s3_prq(
            s3_key=trg_key.format(**kwargs), data_frame=df_stg_dim_time
        )

    @staticmethod
    def stg_dim_resolution(
        src_bucket: str,
        src_key: str,
        trg_bucket: str,
        trg_key: str,
        aws_conn_id: str,
        *args,
        **kwargs,
    ):
        """
        Prepare data for loading to stg_dim_resolution table in data warehouse.

        :param src_bucket: name of bucket containing source data
        :param src_key: relative path to source dataset
        :param trg_bucket: name of bucket containing transformed data
        :param trg_key: relative path for storing transform data
        :param aws_conn_id: AWS connection defined in Airflow connection
        """
        src_df, trg_bucket_conn = PrepareData.init_connection(
            src_bucket=src_bucket,
            trg_bucket=trg_bucket,
            aws_conn_id=aws_conn_id,
            rendered_src_key=src_key.format(**kwargs),
        )
        df_stg_dim_resolution = pd.DataFrame()
        df_stg_dim_resolution["stg_resolution"] = src_df[["resolution"]]
        df_stg_dim_resolution = df_stg_dim_resolution.drop_duplicates()
        # Load processed dataframe to S3
        trg_bucket_conn.write_df_to_s3_prq(
            s3_key=trg_key.format(**kwargs), data_frame=df_stg_dim_resolution
        )

    @staticmethod
    def stg_dim_police(
        src_bucket: str,
        src_key: str,
        trg_bucket: str,
        trg_key: str,
        aws_conn_id: str,
        *args,
        **kwargs,
    ):
        """
        Prepare data for loading to stg_dim_police_district table in data warehouse.

        :param src_bucket: name of bucket containing source data
        :param src_key: relative path to source dataset
        :param trg_bucket: name of bucket containing transformed data
        :param trg_key: relative path for storing transform data
        :param aws_conn_id: AWS connection defined in Airflow connection
        """
        src_df, trg_bucket_conn = PrepareData.init_connection(
            src_bucket=src_bucket,
            trg_bucket=trg_bucket,
            aws_conn_id=aws_conn_id,
            rendered_src_key=src_key.format(**kwargs),
        )
        df_stg_dim_police = pd.DataFrame()
        df_stg_dim_police["stg_police_district"] = src_df[["police_district"]]
        df_stg_dim_police = df_stg_dim_police.drop_duplicates()
        # Load processed dataframe to S3
        trg_bucket_conn.write_df_to_s3_prq(
            s3_key=trg_key.format(**kwargs), data_frame=df_stg_dim_police
        )

    @staticmethod
    def stg_dim_intersection(
        src_bucket: str,
        src_key: str,
        trg_bucket: str,
        trg_key: str,
        aws_conn_id: str,
        *args,
        **kwargs,
    ):
        """
        Prepare data for loading to stg_dim_intersection table in data warehouse.

        :param src_bucket: name of bucket containing source data
        :param src_key: relative path to source dataset
        :param trg_bucket: name of bucket containing transformed data
        :param trg_key: relative path for storing transform data
        :param aws_conn_id: AWS connection defined in Airflow connection
        """
        src_df, trg_bucket_conn = PrepareData.init_connection(
            src_bucket=src_bucket,
            trg_bucket=trg_bucket,
            aws_conn_id=aws_conn_id,
            rendered_src_key=src_key.format(**kwargs),
        )
        df_stg_dim_intersection = src_df[
            ["cnn", "intersection", "latitude", "longitude"]
        ]
        df_stg_dim_intersection = df_stg_dim_intersection.astype({"cnn": "int32"})
        df_stg_dim_intersection = df_stg_dim_intersection.rename(
            columns={
                "cnn": "stg_cnn",
                "intersection": "stg_intersection",
                "latitude": "stg_latitude",
                "longitude": "stg_longitude",
            }
        )
        df_stg_dim_intersection = df_stg_dim_intersection.drop_duplicates()
        # Load processed dataframe to S3
        trg_bucket_conn.write_df_to_s3_prq(
            s3_key=trg_key.format(**kwargs), data_frame=df_stg_dim_intersection
        )

    @staticmethod
    def stg_dim_category(
        src_bucket: str,
        src_key: str,
        trg_bucket: str,
        trg_key: str,
        aws_conn_id: str,
        *args,
        **kwargs,
    ):
        """
        Prepare data for loading to stg_dim_category table in data warehouse.

        :param src_bucket: name of bucket containing source data
        :param src_key: relative path to source dataset
        :param trg_bucket: name of bucket containing transformed data
        :param trg_key: relative path for storing transform data
        :param aws_conn_id: AWS connection defined in Airflow connection
        """
        src_df, trg_bucket_conn = PrepareData.init_connection(
            src_bucket=src_bucket,
            trg_bucket=trg_bucket,
            aws_conn_id=aws_conn_id,
            rendered_src_key=src_key.format(**kwargs),
        )
        category_cols = [
            "incident_code",
            "incident_category",
            "incident_subcategory",
            "incident_description",
        ]
        df_stg_dim_category = src_df[category_cols]
        df_stg_dim_category = df_stg_dim_category.rename(
            columns={
                "incident_code": "stg_category_code",
                "incident_category": "stg_category",
                "incident_subcategory": "stg_subcategory",
                "incident_description": "stg_description",
            }
        )
        df_stg_dim_category = df_stg_dim_category.drop_duplicates()
        # Change int64 columns to int32 columns
        df_stg_dim_category = df_stg_dim_category.astype(
            {col: "int32" for col in df_stg_dim_category.select_dtypes("int64").columns}
        )
        # Load processed dataframe to S3
        trg_bucket_conn.write_df_to_s3_prq(
            s3_key=trg_key.format(**kwargs), data_frame=df_stg_dim_category
        )

    @staticmethod
    def stg_fact_incident(
        src_bucket: str,
        src_key: str,
        trg_bucket: str,
        trg_key: str,
        aws_conn_id: str,
        *args,
        **kwargs,
    ):
        """
        Prepare data for loading to stg_fact_incident table in data warehouse.

        :param src_bucket: name of bucket containing source data
        :param src_key: relative path to source dataset
        :param trg_bucket: name of bucket containing transformed data
        :param trg_key: relative path for storing transform data
        :param aws_conn_id: AWS connection defined in Airflow connection
        """
        src_df, trg_bucket_conn = PrepareData.init_connection(
            src_bucket=src_bucket,
            trg_bucket=trg_bucket,
            aws_conn_id=aws_conn_id,
            rendered_src_key=src_key.format(**kwargs),
        )
        fact_incident_cols = [
            "incident_datetime",
            "report_datetime",
            "incident_id",
            "incident_code",
            "resolution",
            "police_district",
            "cnn",
        ]
        df_stg_fact_incident = src_df[fact_incident_cols]
        df_stg_fact_incident["stg_incident_date"] = df_stg_fact_incident[
            "incident_datetime"
        ].dt.date
        df_stg_fact_incident = df_stg_fact_incident.astype({"stg_incident_date": "str"})
        df_stg_fact_incident["stg_incident_time"] = df_stg_fact_incident[
            "incident_datetime"
        ].dt.strftime("%H:%M")
        df_stg_fact_incident["stg_report_date"] = df_stg_fact_incident[
            "report_datetime"
        ].dt.date
        df_stg_fact_incident = df_stg_fact_incident.astype({"stg_report_date": "str"})
        df_stg_fact_incident["stg_report_time"] = df_stg_fact_incident[
            "report_datetime"
        ].dt.strftime("%H:%M")
        df_stg_fact_incident = df_stg_fact_incident.drop(
            columns=["incident_datetime", "report_datetime"]
        )
        df_stg_fact_incident = df_stg_fact_incident.rename(
            columns={
                "incident_id": "stg_incident_id",
                "incident_code": "stg_category_code",
                "resolution": "stg_resolution",
                "police_district": "stg_police_district",
                "cnn": "stg_cnn",
            }
        )
        df_stg_fact_incident = df_stg_fact_incident.astype(
            {"stg_category_code": "int32", "stg_cnn": "int32"}
        )
        col_order = [
            "stg_incident_date",
            "stg_incident_time",
            "stg_report_date",
            "stg_report_time",
            "stg_incident_id",
            "stg_category_code",
            "stg_resolution",
            "stg_police_district",
            "stg_cnn",
        ]
        df_stg_fact_incident = df_stg_fact_incident[col_order]
        # Load processed dataframe to S3
        trg_bucket_conn.write_df_to_s3_prq(
            s3_key=trg_key.format(**kwargs), data_frame=df_stg_fact_incident
        )
