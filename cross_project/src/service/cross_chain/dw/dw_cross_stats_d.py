from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta
import os

spark = SparkSession.builder.appName("dw_cross_stats_d").getOrCreate()
file_name = os.path.basename(__file__)

from pyspark.sql.window import Window
from pyspark.sql.functions import *
from src.utils import (
    filter_by_timezone,
    union_dataframe,
    get_job_info,
    write_to_s3,
)

### set parameters
job = get_job_info(file_name)
data_layer = job["data_layer"]
job["schema_name"] = "cross_chain"
table_name = job["table_name"]
start_date = job["start_date"]
end_date = job["end_date"]
batch_bdate = job["batch_bdate"]
###


def main(spark):

    def get_closest_to_midnight(df, date_col, ts_col):
        # Extract hour and minute from timestamp
        df_with_time = df.withColumn("hour", hour(ts_col)).withColumn(
            "minute", minute(ts_col)
        )

        # Adjust the date for records that are very close to midnight
        # For KST timezone, if the hour is 0 and minute is less than 5, consider it as part of the previous day
        # For UTC timezone, if the hour is 0 and minute is less than 5, consider it as part of the previous day
        df_with_adjusted_date = df_with_time.withColumn(
            "adjusted_date",
            when(
                (col("timezone_type") == "kst")
                & (hour(ts_col) == 0)
                & (minute(ts_col) < 5),
                date_sub(col(date_col), 1),
            )
            .when(
                (col("timezone_type") == "utc")
                & (hour(ts_col) == 0)
                & (minute(ts_col) < 5),
                date_sub(col(date_col), 1),
            )
            .otherwise(col(date_col)),
        )

        # Calculate time difference from midnight
        df_with_diff = df_with_adjusted_date.withColumn(
            "time_diff_from_midnight",
            when((col("hour") == 0) & (col("minute") == 0), 0)  # Exactly midnight
            .when(col("hour") == 0, col("minute"))  # After midnight
            .when(col("hour") == 23, 60 - col("minute"))  # Before midnight
            .when(col("hour") == 14, 60 - col("minute") + 600)  # 14:59
            .when(col("hour") == 15, col("minute") + 600)  # 15:00
            .otherwise(9999),  # Any other time
        )

        # Get the record with the smallest time difference for each adjusted date
        window = Window.partitionBy("adjusted_date").orderBy("time_diff_from_midnight")
        result = (
            df_with_diff.withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num", "hour", "minute", "time_diff_from_midnight")
        )

        # Update the original date column with the adjusted date
        return result.withColumn(date_col, col("adjusted_date")).drop("adjusted_date")

    global end_date
    date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    plus_one = date_obj + timedelta(days=1)
    end_date_plus_one = plus_one.strftime("%Y-%m-%d")

    sdf_utc = filter_by_timezone(
        spark.table("cross_chain.dl_cross_stats"), "utc", start_date, end_date_plus_one
    )
    sdf_kst = filter_by_timezone(
        spark.table("cross_chain.dl_cross_stats"), "kst", start_date, end_date_plus_one
    )

    sdf_utc_closest = get_closest_to_midnight(sdf_utc, "bdate", "ts_utc")
    sdf_kst_closest = get_closest_to_midnight(sdf_kst, "bdate", "ts_kst")

    sdf = union_dataframe(sdf_utc_closest, sdf_kst_closest).filter(
        (col("bdate") >= start_date) & (col("bdate") < end_date)
    )

    if isinstance(sdf, DataFrame) and sdf.count() > 0:
        final_sdf = (
            sdf.withColumn("total_blocks", col("total_blocks").cast("decimal(38,0)"))
            .withColumn(
                "total_transactions", col("total_transactions").cast("decimal(38,0)")
            )
            .withColumn("total_addresses", col("total_addresses").cast("decimal(38,0)"))
            .withColumn("total_gas_used", col("total_gas_used").cast("decimal(38,0)"))
            .withColumn(
                "transactions_today", col("transactions_today").cast("decimal(38,0)")
            )
            .withColumn("market_cap", col("market_cap").cast("decimal(38,0)"))
            .withColumn("gas_used_today", col("gas_used_today").cast("decimal(38,0)"))
        )

        write_to_s3(spark, final_sdf, job)


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tmmp").getOrCreate()

main(spark)
