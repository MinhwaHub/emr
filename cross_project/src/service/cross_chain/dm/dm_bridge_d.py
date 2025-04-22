from pyspark.sql import DataFrame
import os
from datetime import datetime, timedelta

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
job["schema_name"] = "cross_chain"
table_name = job["table_name"]
start_date = job["start_date"]
end_date = job["end_date"]
batch_bdate = job["batch_bdate"]
###


def main(spark):
    global start_date
    date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    two_days_ago = date_obj - timedelta(days=1)
    start_date = two_days_ago.strftime("%Y-%m-%d")

    sdf = spark.table("cross_chain.dw_bridge_details")

    sdf_token_price = spark.table("cross_wallet.dm_token_price_d")
    sdf_token = spark.table("cross_wallet.dim_token")

    sdf_utc = filter_by_timezone(sdf, "utc", start_date, end_date)
    sdf_kst = filter_by_timezone(sdf, "kst", start_date, end_date)

    sdf_union = union_dataframe(sdf_utc, sdf_kst)

    if isinstance(sdf_union, DataFrame) and sdf_union.count() > 0:
        sdf_joined = (
            sdf_union.alias("a")
            .join(
                sdf_token.select("chain_id", "address", "decimals").alias("c"),
                (
                    (col("from_token") == col("c.address"))
                    & (col("from_chain_id") == col("c.chain_id"))
                ),
                "left",
            )
            .join(
                sdf_token_price.alias("b"),
                (
                    (col("from_token_symbol") == col("b.symbol"))
                    & (col("from_chain_id") == col("b.chain_id"))
                    & (col("a.timezone_type") == col("b.timezone_type"))
                    & (col("a.bdate") == col("b.bdate"))
                ),
                how="left",
            )
            .select("a.*", "b.price_usd", "c.decimals")
            .withColumn(
                "value_eth",
                (col("value") / pow(10, col("decimals"))).cast("decimal(38,10)"),
            )
            .withColumn(
                "volume_usd",
                (col("value") * col("price_usd") / pow(10, col("decimals"))).cast(
                    "decimal(38,10)"
                ),
            )
            .withColumn(
                "exchange_fee_usd",
                (
                    col("exchange_fee") * col("price_usd") / pow(10, col("decimals"))
                ).cast("decimal(38,10)"),
            )
            .withColumn(
                "network_fee_usd",
                (col("network_fee") * col("price_usd") / pow(10, col("decimals"))).cast(
                    "decimal(38,10)"
                ),
            )
        )

        final_sdf = sdf_joined.groupBy(
            [
                "timezone_type",
                "bdate",
                "from_chain_name",
                "to_chain_name",
                "from_token_symbol",
                "to_token_symbol",
            ]
        ).agg(
            count(col("initiated_tx_hash")).alias("initiated_cnt"),
            count(col("finalized_tx_hash")).alias("finalized_cnt"),
            count(when(col("status") == 1, col("_id"))).alias("success_cnt"),
            count(when(col("status") != 1, col("_id"))).alias("failed_cnt"),
            countDistinct(col("initiated_tx_hash")).alias("tx_cnt"),
            sum(when(col("status") == 1, col("value_eth"))).alias("amount"),
            sum(when(col("status") == 1, col("volume_usd"))).alias("volume_usd"),
            sum(col("exchange_fee").alias("exchange_fee")).alias("exchange_fee"),
            sum(col("exchange_fee_usd")).alias("exchange_fee_usd"),
            sum(col("network_fee").alias("network_fee")).alias("network_fee"),
            sum(col("network_fee_usd")).alias("network_fee_usd"),
        )

        write_to_s3(spark, final_sdf, job)
