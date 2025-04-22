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
job["schema_name"] = "cross_dex"
table_name = job["table_name"]
start_date = job["start_date"]
end_date = job["end_date"]
batch_bdate = job["batch_bdate"]
###


def main(spark):
    sdf = spark.table("cross_dex.dl_fee_collectors")
    sdf_pair = spark.table("cross_dex.dl_pair_infos")
    sdf_token = spark.table("cross_wallet.dim_token").select(
        "address", "symbol", "name"
    )

    sdf_utc = filter_by_timezone(sdf, "utc", start_date, end_date)
    sdf_kst = filter_by_timezone(sdf, "kst", start_date, end_date)

    sdf1 = union_dataframe(sdf_utc, sdf_kst)
    if isinstance(sdf1, DataFrame) and sdf1.count() > 0:
        sdf2 = (
            sdf1.alias("a")
            .join(sdf_pair.alias("b"), ["pair_address"], "left")
            .join(sdf_token.alias("c"), col("base_address") == col("c.address"), "left")
            .join(
                sdf_token.alias("d"), col("quote_address") == col("d.address"), "left"
            )
            .select(
                "a.*",
                "b.pair_name",
                col("c.name").alias("base_name"),
                col("c.symbol").alias("base_symbol"),
                col("c.address").alias("base_address"),
                col("d.name").alias("quote_name"),
                col("d.address").alias("quote_address"),
                col("d.symbol").alias("quote_symbol"),
            )
        )

        final_sdf = sdf2.groupBy(
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
                "recipient",
            ]
        ).agg(
            sum("gross_amount").cast("decimal(38,10)").alias("gross_amount"),
            sum("net_amount").cast("decimal(38,10)").alias("net_amount"),
            sum("fee").cast("decimal(38,10)").alias("fee"),
        )

        write_to_s3(spark, final_sdf, job)

    # spark.stop()
