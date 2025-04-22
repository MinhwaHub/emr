from pyspark.sql import SparkSession
from datetime import datetime
import os

spark = SparkSession.builder.appName("dw_order_matches_d").getOrCreate()
file_name = os.path.basename(__file__)


from pyspark.sql.functions import *
from src.utils import get_date, timezone_filtering, union_dataframe, S3Handler

data_layer = file_name.split("_")[0]
schema_name = file_name.split("dw_")[1].split(".py")[0]
# if exists sys.args then start_date, end_date = sys.args[0], sys.args[1]
# else start_date, end_date = get_date()
start_date, end_date = get_date()
batch_bdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("start_date, end_date: ", start_date, end_date, "\n")

sdf = spark.table("cross_dex.dl_order_matches")
sdf_pair = spark.table("cross_dex.dl_pair_infos")
sdf_token = spark.table("cross_wallet.dl_token").select("address", "symbol", "name")

sdf_utc = timezone_filtering(sdf, "utc", start_date, end_date)
sdf_kst = timezone_filtering(sdf, "kst", start_date, end_date)

sdf1 = union_dataframe(sdf_utc, sdf_kst)
sdf2 = sdf1.groupBy(["timezone_type", "bdate", "pair_address", "order_side"]).agg(
    avg("price").alias("avg_price"),
    sum("amount").alias("total_amount"),
    sum("fee").alias("total_fee"),
    max("block").alias("last_block"),
)

final_sdf = (
    (sdf2.join(sdf_pair, ["pair_address"], "left"))
    .alias("a")
    .join(sdf_token.alias("b"), col("base_address") == col("b.address"), "left")
    .join(sdf_token.alias("c"), col("quote_address") == col("c.address"), "left")
    .select(
        "timezone_type",
        "bdate",
        "a.pair_name",
        "a.pair_address",
        col("c.name").alias("quote_name"),
        col("c.symbol").alias("quote_symbol"),
        "quote_address",
        col("b.name").alias("base_name"),
        col("b.symbol").alias("base_symbol"),
        "base_address",
        "order_side",
        "avg_price",
        "total_amount",
        "total_fee",
        "last_block",
    )
    .orderBy("bdate")
)

schema_name = "cross_dex"
data_layer = "dw"
table_name = "order_matches_d"
partition_cols = ["bdate"]

s3_writer = S3Handler()
try:
    s3_writer.write_table(
        df=final_sdf,
        data_layer=data_layer,
        schema_name=schema_name,
        table_name=table_name,
        partition_cols=partition_cols,
    )
    print(f"S3 write success \n")
    s3_writer.table_manager(
        spark,
        schema_name,
        table_name,
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
        table_name,
        batch_bdate,
        start_date,
        end_date,
        status=0,
    )
    print(f"table_manager success \n")


spark.stop()
