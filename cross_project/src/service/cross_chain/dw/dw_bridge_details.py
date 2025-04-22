from pyspark.sql import DataFrame
import os
from datetime import datetime, timedelta

file_name = os.path.basename(__file__)

from pyspark.sql.functions import *
from src.utils import (
    filter_by_time,
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

    sdf_chain = spark.table("cross_chain.dim_chain")
    sdf_token = spark.table("cross_wallet.dim_token")

    sdf = filter_by_time(
        spark.table("cross_chain.dl_bridge_history"), start_date, end_date
    )

    sdf_joined = (
        sdf.alias("a")
        .join(sdf_chain.alias("b"), col("from_chain_id") == col("chain_id"), how="left")
        .drop(col("b.chain_id"))
        .join(sdf_chain.alias("c"), col("to_chain_id") == col("chain_id"), how="left")
        .drop(col("c.chain_id"))
        .join(
            sdf_token.alias("d"),
            (
                (col("from_token") == col("address"))
                & (col("from_chain_id") == col("d.chain_id"))
            ),
            how="left",
        )
        .drop(col("d.address"))
        .join(
            sdf_token.alias("e"),
            (
                (col("to_token") == col("address"))
                & (col("to_chain_id") == col("e.chain_id"))
            ),
            how="left",
        )
        .drop(col("e.address"))
    )
    final_sdf = sdf_joined.select(
        "dt_utc",
        "dt_kst",
        "ts_utc",
        "ts_kst",
        "initiated_time",
        "finalized_time",
        "a._id",
        "from_chain_id",
        col("b.network").alias("from_chain_name"),
        "to_chain_id",
        col("c.network").alias("to_chain_name"),
        "index",
        "from_token",
        col("d.symbol").alias("from_token_symbol"),
        "to_token",
        col("e.symbol").alias("to_token_symbol"),
        "from",
        "to",
        "value",
        "exchange_fee",
        "network_fee",
        "extra_data",
        "initiated_tx_hash",
        "finalized_tx_hash",
        "status",
    )

    final_sdf.limit(3).show()

    write_to_s3(spark, final_sdf, job)


# spark.stop()
