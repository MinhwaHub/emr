from pyspark.sql import SparkSession
import logging
from src.service.cross_dex.dl import dl_cross_dex
from src.service.cross_wallet.dl import dl_cross_wallet
from src.service.cross_dex.dw import (
    dw_order_details,
    dw_trade_details,
    dw_order_d,
    dw_fee_d,
)
from src.service.cross_dex.dm import dm_user_summary_d, dm_trade_d
from src.validators.rules import dl_count_check, dl_date_check
from src.utils.spark_utils import run_pipeline

from src.base import BaseConfig


spark = SparkSession.builder.appName("Daily_Dex_Batch").getOrCreate()


S3_BUCKET = BaseConfig.S3_BUCKET
S3_PATH = "cross_project/src/service/cross_dex/pipeline_status.log"

# 실행할 함수들을 순서대로 리스트로 정의
STEPS = [
    ("dl_cross_wallet", dl_cross_wallet.main),
    ("dl_cross_dex", dl_cross_dex.main),
    ("dl_date_check", dl_date_check.main),
    ("dl_count_check", dl_count_check.main),
    ("dw_order_details", dw_order_details.main),
    ("dw_trade_details", dw_trade_details.main),
    ("dw_order_d", dw_order_d.main),
    ("dw_fee_d", dw_fee_d.main),
    ("dm_user_summary_d", dm_user_summary_d.main),
    ("dm_trade_d", dm_trade_d.main),
]


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Daily_Dex_Batch").getOrCreate()
    run_pipeline(spark, STEPS, S3_BUCKET, S3_PATH)
