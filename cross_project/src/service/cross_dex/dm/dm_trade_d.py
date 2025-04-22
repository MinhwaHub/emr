from pyspark.sql import DataFrame
import os

file_name = os.path.basename(__file__)


from pyspark.sql.functions import *
from src.utils import (
    filter_by_timezone,
    filter_by_timezone_end,
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
    sdf_close_utc = filter_by_timezone(
        spark.table("cross_dex.dw_trade_details"), "utc", start_date, end_date
    )
    sdf_close_kst = filter_by_timezone(
        spark.table("cross_dex.dw_trade_details"), "kst", start_date, end_date
    )

    sdf_close_tmp = union_dataframe(sdf_close_utc, sdf_close_kst).filter(
        col("is_taker") == 1
    )

    if isinstance(sdf_close_tmp, DataFrame) and sdf_close_tmp.count() > 0:
        sdf_close = sdf_close_tmp.groupBy(
            [
                "timezone_type",
                "bdate",
                "pair_name",
                "base_symbol",
                "quote_symbol",
            ]
        ).agg(
            countDistinct("txhash").alias("tx_cnt"),
            min("price").alias("low_price"),
            max("price").alias("high_price"),
            avg("price").alias("avg_price"),
            sum("amount").cast("decimal(38,10)").alias("amount"),
            sum("volume").cast("decimal(38,10)").alias("volume"),
            sum("volume_usd").cast("decimal(38,10)").alias("volume_usd"),
            sum("fee").cast("decimal(38,10)").alias("fee"),
            sum("fee_usd").cast("decimal(38,10)").alias("fee_usd"),
        )

    # sdf_open_utc = filter_by_timezone_end(
    #     spark.table("cross_dex.dw_order_details").filter(
    #         col("is_completed") == 0
    #     ),  # user_orders
    #     "utc",
    #     end_date,
    # )
    # sdf_open_kst = filter_by_timezone_end(
    #     spark.table("cross_dex.dw_order_details").filter(
    #         col("is_completed") == 0
    #     ),  # user_orders
    #     "kst",
    #     end_date,
    # )
    # sdf_open_tmp = union_dataframe(sdf_open_utc, sdf_open_kst)
    # sdf_open_tmp = (
    #     sdf_open_tmp.withColumn(
    #         "tvl_base", (col("amount") - col("filled")).cast("decimal(38,10)")
    #     )
    #     .withColumn(
    #         "tvl_base_usd",
    #         ((col("amount") - col("filled")) * col("price_usd")).cast("decimal(38,10)"),
    #     )
    #     .withColumn(
    #         "tvl_quote", (col("volume") - col("filled_volume")).cast("decimal(38,10)")
    #     )
    #     .withColumn(
    #         "tvl_quote_usd",
    #         (col("volume_usd") - col("filled_volume_usd")).cast("decimal(38,10)"),
    #     )
    # )
    # if isinstance(sdf_open_tmp, DataFrame) and sdf_open_tmp.count() > 0:

    #     sdf_open = (
    #         sdf_open_tmp.groupBy(
    #             [
    #                 "timezone_type",
    #                 "bdate",
    #                 "pair_name",
    #                 "base_symbol",
    #                 "quote_symbol",
    #             ]
    #         )
    #         .pivot("order_side")
    #         .agg(
    #             sum(col("tvl_base")).alias("tvl_base"),
    #             sum(col("tvl_base_usd")).alias("tvl_base_usd"),
    #             sum(col("tvl_quote")).alias("tvl_quote"),
    #             sum(col("tvl_quote_usd")).alias("tvl_quote_usd"),
    #         )
    #         .withColumnRenamed("0_tvl_base", "tvl_base")
    #         .withColumnRenamed("0_tvl_base_usd", "tvl_base_usd")
    #         .withColumnRenamed("1_tvl_quote", "tvl_quote")
    #         .withColumnRenamed("1_tvl_quote_usd", "tvl_quote_usd")
    #         .na.fill(0)
    #         .withColumn("tvl_base", col("tvl_base").cast("decimal(38,10)"))
    #         .withColumn("tvl_base_usd", col("tvl_base_usd").cast("decimal(38,10)"))
    #         .withColumn("tvl_quote", col("tvl_quote").cast("decimal(38,10)"))
    #         .withColumn("tvl_quote_usd", col("tvl_quote_usd").cast("decimal(38,10)"))
    #         .withColumn(
    #             "tvl_usd",
    #             (col("tvl_base_usd") + col("tvl_quote_usd")).cast("decimal(38,10)"),
    #         )
    #         .drop("1_tvl_base", "1_tvl_base_usd", "0_tvl_quote", "0_tvl_quote_usd")
    #     )

    has_close_data = isinstance(sdf_close_tmp, DataFrame) and sdf_close_tmp.count() > 0
    # has_open_data = isinstance(sdf_open_tmp, DataFrame) and sdf_open_tmp.count() > 0

    # 조건에 따라 final_sdf 생성
    # if has_close_data and has_open_data:
    # final_sdf = sdf_close.join(
    #     sdf_open,
    #     ["timezone_type", "bdate", "pair_name", "base_symbol", "quote_symbol"],
    #     "left",
    # ).na.fill(0)
    if has_close_data:
        final_sdf = sdf_close
        # sdf_close만 있는 경우, open 관련 컬럼들을 0으로 추가
        # final_sdf = (
        #     sdf_close.withColumn("tvl_base", lit(0).cast("decimal(38,10)"))
        #     .withColumn("tvl_base_usd", lit(0).cast("decimal(38,10)"))
        #     .withColumn("tvl_quote", lit(0).cast("decimal(38,10)"))
        #     .withColumn("tvl_quote_usd", lit(0).cast("decimal(38,10)"))
        #     .withColumn("tvl_usd", lit(0).cast("decimal(38,10)"))
        # )
    # elif has_open_data:
    #     final_sdf = sdf_open
    #     # sdf_open만 있는 경우, close 관련 컬럼들을 0으로 추가
    #     final_sdf = (
    #         sdf_open.withColumn("tx_cnt", lit(0))
    #         .withColumn("low_price", lit(0).cast("decimal(38,10)"))
    #         .withColumn("high_price", lit(0).cast("decimal(38,10)"))
    #         .withColumn("avg_price", lit(0).cast("decimal(38,10)"))
    #         .withColumn("volume", lit(0).cast("decimal(38,10)"))
    #         .withColumn("volume_usd", lit(0).cast("decimal(38,10)"))
    #         .withColumn("fee", lit(0).cast("decimal(38,10)"))
    #         .withColumn("fee_usd", lit(0).cast("decimal(38,10)"))
    #     )
    else:
        # 둘 다 없는 경우
        print("No data available for both close and open orders")
        final_sdf = None

    if isinstance(final_sdf, DataFrame) and final_sdf.count() > 0:
        write_to_s3(spark, final_sdf, job)

    # spark.stop()
