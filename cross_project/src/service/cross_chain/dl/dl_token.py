from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os
import http.client
import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    IntegerType,
)

from src.utils import SlackMessenger

spark = SparkSession.builder.appName("dl_token").getOrCreate()
file_name = os.path.basename(__file__)

from pyspark.sql.functions import *
from src.utils import (
    get_date,
    S3Handler,
    repair_glue_partitions,
)

### set parameters
data_layer = file_name.split("_")[0]
schema_name = "cross_chain"
###

# if exists sys.args then start_date, end_date = sys.args[0], sys.args[1]
# else start_date, end_date = get_date()
start_date, end_date = get_date()
batch_bdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("start_date, end_date: ", start_date, end_date, "\n")


conn = http.client.HTTPSConnection("testnet.crossscan.io")
payload = ""
headers = {}
conn.request("GET", "/api/v2/tokens?type=ERC-20", payload, headers)
res = conn.getresponse()
data = res.read().decode("utf-8")
data1 = json.loads(data)["items"]


schema = StructType(
    [
        StructField("address", StringType()),
        StructField("circulating_market_cap", StringType(), True),
        StructField("decimals", StringType()),
        StructField("exchange_rate", StringType(), True),
        StructField("holders", StringType(), True),
        StructField("icon_url", StringType(), True),
        StructField("name", StringType()),
        StructField("symbol", StringType()),
        StructField("total_supply", StringType(), True),
        StructField("type", StringType()),
        StructField("volume_24h", StringType(), True),
    ]
)


sdf = spark.createDataFrame(data1, schema)
final_sdf = (
    sdf.drop("icon_url")
    .withColumn(
        "circulating_market_cap", sdf["circulating_market_cap"].cast(DecimalType(38, 2))
    )
    .withColumn("decimals", sdf["decimals"].cast(IntegerType()))
    .withColumn("exchange_rate", sdf["exchange_rate"].cast(DecimalType(38, 2)))
    .withColumn("holders", sdf["holders"].cast(IntegerType()))
    .withColumn("volume_24h", sdf["volume_24h"].cast(DecimalType(38, 2)))
    .withColumn("total_supply", sdf["total_supply"].cast(DecimalType(38, 0)))
)

s3_writer = S3Handler()

try:
    s3_writer.write_table(
        df=final_sdf,
        data_layer=data_layer,
        schema_name=schema_name,
        table_name="token",
        partition_cols=[],
        format="parquet",
        mode="overwrite",
    )
    print(f"S3 write success \n")
    repair_glue_partitions(spark, schema_name, data_layer, "token")
    s3_writer.table_manager(
        spark,
        schema_name,
        "token",
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
        "token",
        batch_bdate,
        start_date,
        end_date,
        status=0,
    )
    print(f"table_manager success \n")

messenger = SlackMessenger()
messenger.send_slack(text=" *FINISH CRAWLING TOKEN*")
spark.stop()
