from pyspark.sql import DataFrame
import os

file_name = os.path.basename(__file__)


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
job["schema_name"] = "cross_wallet"
table_name = job["table_name"]
start_date = job["start_date"]
end_date = job["end_date"]
batch_bdate = job["batch_bdate"]
###


def main(spark):
    sdf_token_price = (
        spark.table("cross_wallet.dl_token_price")
        .withColumn("utc_hour", hour(col("ts_utc")))
        .withColumn("kst_hour", hour(col("ts_kst")))
        .filter((col("utc_hour") == 0) | (col("kst_hour") == 0))
    )

    sdf_token = spark.table("cross_wallet.dim_token")

    sdf_utc = filter_by_timezone(sdf_token_price, "utc", start_date, end_date).filter(
        col("utc_hour") == 0
    )
    sdf_kst = filter_by_timezone(sdf_token_price, "kst", start_date, end_date).filter(
        col("kst_hour") == 0
    )

    sdf_union = (
        union_dataframe(sdf_utc, sdf_kst)
        .join(sdf_token, ["address", "chain_id"], "left")
        .withColumn("bdate", date_sub(col("bdate"), 1))
    )
    print(sdf_union.count())

    if isinstance(sdf_union, DataFrame) and sdf_union.count() > 0:
        final_sdf = sdf_union.select(
            "timezone_type",
            "bdate",
            "ts_utc",
            "ts_kst",
            "chain_id",
            "address",
            "symbol",
            "decimals",
            "price_usd",
        )

    if isinstance(final_sdf, DataFrame) and final_sdf.count() > 0:
        write_to_s3(spark, final_sdf, job)

    # spark.stop()
