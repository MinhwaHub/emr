from pyspark.sql import DataFrame
import os

file_name = os.path.basename(__file__)


from pyspark.sql.functions import *
from src.utils import filter_by_time, get_job_info, write_to_s3

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
    sdf = filter_by_time(
        spark.table("cross_dex.dl_match_histories"), start_date, end_date
    )

    sdf_pair = spark.table("cross_dex.dl_pair_infos")
    sdf_token = spark.table("cross_wallet.dim_token").select(
        "address", "symbol", "name"
    )

    if isinstance(sdf, DataFrame) and sdf.count() > 0:
        final_sdf = (
            sdf.alias("a")
            .join(sdf_pair.alias("b"), ["pair_address"], "left")
            .join(sdf_token.alias("c"), col("base_address") == col("c.address"), "left")
            .join(
                sdf_token.alias("d"), col("quote_address") == col("d.address"), "left"
            )
            .select(
                "dt_utc",
                "dt_kst",
                "ts_utc",
                "ts_kst",
                "block",
                col("time_utc").alias("block_time"),
                col("hash").alias("txhash"),
                "idx",
                "pair_name",
                "pair_address",
                "quote_address",
                col("d.name").alias("quote_name"),
                col("d.symbol").alias("quote_symbol"),
                "base_address",
                col("c.name").alias("base_name"),
                col("c.symbol").alias("base_symbol"),
                "owner",
                "order_side",
                "is_taker",
                "price",
                col("price_usd_at").alias("price_usd"),
                "amount",
                "volume",  # amount * price
                col("volume_usd_at").alias("volume_usd"),  # amount * price_usd
                "fee",
                col("fee_usd_at").alias("fee_usd"),
            )
        )

        final_sdf.limit(3).show()

        write_to_s3(spark, final_sdf, job)

    # spark.stop()
