from pyspark.sql import DataFrame
import os

file_name = os.path.basename(__file__)

from pyspark.sql import Window
from pyspark.sql.functions import *
from src.utils import (
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

    # 필요한 데이터만 로드
    sdf_total_utc = filter_by_timezone_end(
        spark.table("cross_dex.dw_order_details"), "utc", end_date
    )
    sdf_total_kst = filter_by_timezone_end(
        spark.table("cross_dex.dw_order_details"), "kst", end_date
    )

    sdf_total_union = union_dataframe(sdf_total_utc, sdf_total_kst)
    sdf_distinct = sdf_total_union.select("timezone_type", "bdate", "owner").distinct()

    # DAU
    sdf_d = sdf_distinct.groupBy(["timezone_type", "bdate"]).agg(
        countDistinct("owner").alias("dau")
    )

    # WAU
    sdf_w_tmp = sdf_distinct.alias("a").join(
        sdf_distinct.alias("b"),
        (col("a.timezone_type") == col("b.timezone_type"))
        & (
            col("b.bdate").between(
                col("a.bdate"),
                date_add(col("a.bdate").cast("date"), 6),
            )
        ),
        "inner",
    )

    sdf_w = sdf_w_tmp.groupBy(["a.timezone_type", "a.bdate"]).agg(
        countDistinct("b.owner").alias("wau")
    )

    # MAU
    sdf_m_tmp = sdf_distinct.alias("a").join(
        sdf_distinct.alias("b"),
        (col("a.timezone_type") == col("b.timezone_type"))
        & (
            col("b.bdate").between(
                date_trunc("month", col("a.bdate").cast("date")),  # 해당 월의 첫날
                last_day(col("a.bdate").cast("date")),  # 해당 월의 마지막날
            )
        ),
        "inner",
    )

    sdf_m = sdf_m_tmp.groupBy(["a.timezone_type", "a.bdate"]).agg(
        countDistinct("b.owner").alias("mau")
    )

    # RU
    window = (
        Window.partitionBy("timezone_type")
        .orderBy("bdate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    sdf_total = (
        sdf_total_union.withColumn("ru", size(collect_set("owner").over(window)))
        .groupBy(["timezone_type", "bdate"])
        .agg(max("ru").alias("ru"))
    )

    # NRU
    sdf_first_visit = sdf_distinct.groupBy("owner", "timezone_type").agg(
        min("bdate").alias("first_visit")
    )  # 각 사용자의 첫 방문일

    sdf_nru = (
        sdf_first_visit.groupBy("timezone_type", "first_visit")
        .agg(countDistinct("owner").alias("nru"))  # 첫 방문일 기준으로 집계
        .withColumnRenamed("first_visit", "bdate")
    )

    # 최종 결과 합치기
    final_sdf = (
        sdf_d.join(sdf_w, ["timezone_type", "bdate"], "outer")
        .join(sdf_m, ["timezone_type", "bdate"], "outer")
        .join(sdf_total, ["timezone_type", "bdate"], "outer")
        .join(sdf_nru, ["timezone_type", "bdate"], "outer")
        .orderBy("timezone_type", "bdate")
    ).na.fill(0)

    if isinstance(final_sdf, DataFrame) and final_sdf.count() > 0:
        write_to_s3(spark, final_sdf, job)

    # spark.stop()
