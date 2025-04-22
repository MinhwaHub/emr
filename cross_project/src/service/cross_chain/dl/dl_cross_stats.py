import os

file_name = os.path.basename(__file__)

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os
import http.client
import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)

from src.utils import SlackMessenger

spark = SparkSession.builder.appName("dl_cross_stats").getOrCreate()
file_name = os.path.basename(__file__)

from pyspark.sql.functions import *
from src.utils import (
    get_job_info,
    S3Handler,
    repair_glue_partitions,
)

### set parameters
job = get_job_info(file_name)
data_layer = job["data_layer"]
schema_name = "cross_chain"
start_date = job["start_date"]
end_date = job["end_date"]
batch_bdate = job["batch_bdate"]
###


# def main(spark, start_date, end_date):
conn = http.client.HTTPSConnection("testnet.crossscan.io")
payload = ""
headers = {}
conn.request("GET", "/api/v2/stats", payload, headers)
res = conn.getresponse()
data = res.read().decode("utf-8")
data1 = json.loads(data)

schema = StructType(
    [
        StructField("average_block_time", DoubleType(), True),
        StructField("coin_price", DoubleType(), True),
        StructField("coin_price_change_percentage", DoubleType(), True),
        StructField("gas_price_updated_at", StringType(), True),
        StructField("gas_prices_slow", DoubleType(), True),
        StructField("gas_prices_average", DoubleType(), True),
        StructField("gas_prices_fast", DoubleType(), True),
        StructField("gas_prices_update_in", LongType(), True),
        StructField("gas_used_today", LongType(), True),
        StructField("market_cap", LongType(), True),
        StructField("network_utilization_percentage", DoubleType(), True),
        StructField("secondary_coin_price", DoubleType(), True),
        StructField("static_gas_price", DoubleType(), True),
        StructField("total_addresses", IntegerType(), True),
        StructField("total_blocks", LongType(), True),
        StructField("total_gas_used", LongType(), True),
        StructField("total_transactions", LongType(), True),
        StructField("transactions_today", LongType(), True),
        StructField("tvl", LongType(), True),
    ]
)

stats = {}
for k, v in data1.items():
    if k == "gas_prices":
        stats["gas_prices_slow"] = data1["gas_prices"]["slow"]
        stats["gas_prices_average"] = data1["gas_prices"]["average"]
        stats["gas_prices_fast"] = data1["gas_prices"]["fast"]
    else:
        if k not in ("coin_image", "secondary_coin_image"):
            stats[k] = v

df = (
    spark.createDataFrame([stats], schema)
    .withColumn("ts_utc", lit(datetime.now()))
    .withColumn("dt_utc", col("ts_utc").cast("date"))
    .withColumn("ts_kst", lit(datetime.now() + timedelta(hours=9)))
    .withColumn("dt_kst", col("ts_kst").cast("date"))
    .select(
        *[
            "dt_utc",
            "ts_utc",
            "dt_kst",
            "ts_kst",
            "average_block_time",
            "coin_price",
            "coin_price_change_percentage",
            "gas_price_updated_at",
            "gas_prices_slow",
            "gas_prices_average",
            "gas_prices_fast",
            "gas_prices_update_in",
            "gas_used_today",
            "market_cap",
            "network_utilization_percentage",
            "secondary_coin_price",
            "static_gas_price",
            "total_addresses",
            "total_blocks",
            "total_gas_used",
            "total_transactions",
            "transactions_today",
            "tvl",
        ]
    )
)

s3_writer = S3Handler()

try:
    s3_writer.append_table(
        df=df,
        data_layer=data_layer,
        schema_name=schema_name,
        table_name="cross_stats",
        partition_cols=None,
        format="parquet",
        # mode="overwrite",
    )
    print(f"S3 write success \n")
    repair_glue_partitions(spark, schema_name, data_layer, "cross_stats")
    s3_writer.table_manager(
        spark,
        schema_name,
        "cross_stats",
        batch_bdate,
        start_date,
        end_date,
        status=1,
    )
    print(f"table_manager success \n")

except Exception as e:
    print(f"S3 write failed: {e}")

    s3_writer.table_manager(
        spark,
        schema_name,
        "cross_stats",
        batch_bdate,
        start_date,
        end_date,
        status=0,
    )
    print(f"table_manager success \n")

messenger = SlackMessenger()
messenger.send_slack(text=" *FINISH CRAWLING CHAIN STATS*")

spark.stop()
