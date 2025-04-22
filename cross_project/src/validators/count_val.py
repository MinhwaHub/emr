from src.utils import (
    MySQLReader,
    get_date,
    S3Handler,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
from src.base import BaseConfig

start_date, end_date = get_date()


class CountValidator:
    def __init__(
        self,
        spark: SparkSession,
        config: dict,
        start_date: str = start_date,
        end_date: str = end_date,
    ):
        self.spark = spark
        self.config = config
        self.s3_client = S3Handler().s3_client
        self.start_date = start_date
        self.end_date = end_date
        # self.config["Table"] = self.config["Table"].split(" ")[0]

    def get_db_count(self):
        db_reader = MySQLReader(self.spark, self.config["db_config"])
        try:
            if "TimeField" in self.config:
                if self.config.get("Mode", "append") == "overwrite":
                    filtering_query = f"{self.config['TimeField']} < UNIX_TIMESTAMP('{self.end_date} 00:00:00')"
                else:
                    filtering_query = f"{self.config['TimeField']} >= UNIX_TIMESTAMP('{self.start_date} 00:00:00')  AND {self.config['TimeField']} < UNIX_TIMESTAMP('{self.end_date} 00:00:00')"
                df = db_reader.read_table(
                    table_name=self.config["Table"],
                    select_field="count(*) AS CNT",
                    query=filtering_query,
                )
            else:
                df = db_reader.read_table(
                    table_name=self.config["Table"],
                    select_field="count(*) AS CNT",
                )
            return df.select("CNT").collect()[0][0]
        except:
            return 0
        

    def get_catalog_count(self):
        try:
            temp_table = self.config["Table"].split(" ")[0]
            if self.config.get("Mode", "append") == "overwrite":
                if "TimeField" in self.config:
                    val = (
                        self.spark.table(self.config["Service"] + ".dl_" + temp_table)
                        .filter(col("dt_utc") < self.end_date)
                        .count()
                    )
                else:
                    val = self.spark.table(
                        self.config["Service"] + ".dl_" + temp_table
                    ).count()
            else:
                val = (
                    self.spark.table(self.config["Service"] + ".dl_" + temp_table)
                    .filter(
                        (col("dt_utc") >= self.start_date) & (col("dt_utc") < self.end_date)
                    )
                    .count()
                )
            return val
        except:
            return 0

    def get_s3_count(self):
        temp_table = self.config["Table"].split(" ")[0]

        def path_exists(date):
            prefix = (
                f"results/{self.config['Service']}/dl/dl_{temp_table}/dt_utc={date}/"
            )
            response = self.s3_client.list_objects_v2(
                Bucket=BaseConfig.S3_BUCKET, Prefix=prefix, MaxKeys=1
            )
            return "Contents" in response

        try:
            if (
                "TimeField" in self.config
                and self.config.get("Mode", "append") != "overwrite"
            ):
                dates = pd.date_range(
                    start=self.start_date, end=self.end_date, inclusive="left"
                ).strftime("%Y-%m-%d")

                valid_dates = [date for date in dates if path_exists(date)]
                s3_paths = [
                    f"{BaseConfig.S3_PATH}results/{self.config['Service']}/dl/dl_{temp_table}/dt_utc={date}/"
                    for date in valid_dates
                ]
                print(s3_paths, "\n")

                if s3_paths:
                    df = self.spark.read.parquet(*s3_paths)
            else:
                s3_path = f"{BaseConfig.S3_PATH}results/{self.config['Service']}/dl/dl_{temp_table}/"
                print(s3_path)
                df = self.spark.read.parquet(s3_path)

            return df.count()
        except Exception as e:
            print(e)
            return 0

    def validate_count(self):
        db_count = self.get_db_count()
        s3_count = self.get_s3_count()
        catalog_count = self.get_catalog_count()

        return {
            "service_name": self.config["Service"],
            "table_name": self.config["Table"],
            "db_count": db_count,
            "s3_count": s3_count,
            "catalog_count": catalog_count,
        }
