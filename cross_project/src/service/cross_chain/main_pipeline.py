from pyspark.sql import SparkSession
from src.service.cross_wallet.dl import dl_cross_wallet
from src.service.cross_wallet.dm import dm_token_price_d

from src.service.cross_chain.dl import dl_cross_chain
from src.service.cross_chain.dw import dw_bridge_details, dw_cross_stats_d
from src.service.cross_chain.dm import dm_bridge_d
from src.utils.spark_utils import run_pipeline

from src.base import BaseConfig


spark = SparkSession.builder.appName("Daily_Wallet_Batch").getOrCreate()


S3_BUCKET = BaseConfig.S3_BUCKET
S3_PATH = "cross_project/src/service/cross_chain/pipeline_status.log"


STEPS = [
    #
    ("dl_cross_chain", dl_cross_chain.main),
    ("dl_cross_wallet", dl_cross_wallet.main),
    ("dm_token_price_d", dm_token_price_d.main),
    # wallet batch
    ("dw_cross_stats_d", dw_cross_stats_d.main),
    ("dw_bridge_details", dw_bridge_details.main),
    ("dm_bridge_d", dm_bridge_d.main),
]


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Daily_Chain_Batch").getOrCreate()
    run_pipeline(spark, STEPS, S3_BUCKET, S3_PATH)
