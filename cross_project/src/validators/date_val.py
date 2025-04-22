from src.utils import (
    MySQLReader,
    get_date,
    S3Handler,
)
from pyspark.sql import SparkSession

start_date, end_date = get_date()


class DateValidator:
    def __init__(
        self,
        spark: SparkSession,
        config: dict,
        start_date: str = start_date,
        end_date: str = end_date,
    ):
        self.spark = spark
        self.config = config
        self.start_date = start_date
        self.end_date = end_date

    def get_db_value(self):
        db_reader = MySQLReader(self.spark, self.config["db_config"])
        df = db_reader.read_table(
            table_name=self.config["Table"],
            select_field=f"MAX(DATE_FORMAT(FROM_UNIXTIME({self.config['TimeField']}), '%Y-%m-%d %H:%i:%s')) AS max_bdate",
            query=f"{self.config['TimeField']} >= UNIX_TIMESTAMP('{start_date} 00:00:00')  AND {self.config['TimeField']} < UNIX_TIMESTAMP('{end_date} 00:00:00')",
        )

        return df.select("max_bdate").collect()[0][0]

    def validate_count(self):
        db_value = self.get_db_value()

        return {
            "service_name": self.config["Service"],
            "table_name": self.config["Table"],
            "start_date": start_date,
            "end_date": end_date,
            "db_max_bdate": db_value,
        }
