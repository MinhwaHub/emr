from pyspark.sql import DataFrame
import os

file_name = os.path.basename(__file__)


from pyspark.sql.functions import *
from src.utils import (
    union_dataframe,
    filter_by_time_end,
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
    sdf1 = filter_by_time_end(
        spark.table("cross_dex.dl_close_orders").withColumn("is_completed", lit(1)),
        end_date,
    )
    sdf2 = filter_by_time_end(
        spark.table("cross_dex.dl_user_orders").withColumn("is_completed", lit(0)),
        end_date,
    )
    sdf2_unique = sdf2.join(
        sdf1.select("order_id", "pair_address"),
        ["order_id", "pair_address"],
        "leftanti",
    )
    sdf_pair = spark.table("cross_dex.dl_pair_infos")
    sdf_token = spark.table("cross_wallet.dim_token").select(
        "address", "symbol", "name"
    )

    sdf = union_dataframe(sdf1, sdf2_unique)
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
                "order_id",
                "created_block",
                "created_hash",
                "is_completed",
                "closed_type",
                "closed_at",
                "closed_block",
                "closed_hash",
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
                "price",
                col("price_usd_at").alias("price_usd"),
                "amount",
                (col("price_usd_at") * col("amount")).alias("amount_usd"),
                "filled",
                col("filled_usd_at").alias("filled_usd"),
                "volume",
                col("volume_usd_at").alias("volume_usd"),
                "filled_volume",
                col("filled_usd_at").alias("filled_volume_usd"),
            )
        )

        final_sdf.limit(3).show()

        write_to_s3(spark, final_sdf, job)

    # spark.stop()
