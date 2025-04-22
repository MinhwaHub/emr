from pyspark.sql import DataFrame
import os

file_name = os.path.basename(__file__)


from pyspark.sql.functions import *
from src.utils import (
    union_dataframe,
    filter_by_timezone_end,
    get_job_info,
    write_to_s3,
)

### set parameters
job = get_job_info(file_name)
data_layer = job["data_layer"]
job["schema_name"] = "cross_dex"
table_name = job["table_name"]
start_date = job["start_date"]
end_date = job["end_date"]
batch_bdate = job["batch_bdate"]
###


def main(spark):
    sdf_utc = filter_by_timezone_end(
        spark.table("cross_dex.dw_order_details"), "utc", end_date
    )
    sdf_kst = filter_by_timezone_end(
        spark.table("cross_dex.dw_order_details"), "kst", end_date
    )
    sdf = union_dataframe(sdf_utc, sdf_kst).withColumn(
        "is_cancelled", when(col("closed_type") == 0, lit(0)).otherwise(lit(1))
    )

    if isinstance(sdf, DataFrame) and sdf.count() > 0:
        final_sdf = sdf.groupBy(
            [
                "timezone_type",
                "bdate",
                "pair_name",
                "pair_address",
                "quote_address",
                "quote_name",
                "quote_symbol",
                "base_address",
                "base_name",
                "base_symbol",
                "owner",
                "order_side",
                "is_cancelled",
            ]
        ).agg(
            avg("price").alias("avg_price"),
            avg("price_usd").alias("avg_price_usd"),
            sum("amount").alias("amount"),
            sum("amount_usd").alias("amount_usd"),
            sum("filled").alias("filled"),
            sum("filled_usd").alias("filled_usd"),
            sum("volume").alias("volume"),
            sum("volume_usd").alias("volume_usd"),
            sum("filled_volume").alias("filled_volume"),
            sum("filled_volume_usd").alias("filled_volume_usd"),
        )

        final_sdf.limit(3).show()

        write_to_s3(spark, final_sdf, job)

    # spark.stop()
