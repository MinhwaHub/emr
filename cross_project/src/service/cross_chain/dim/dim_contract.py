from pyspark.sql import SparkSession
from datetime import datetime
import os
import http.client
import json

spark = SparkSession.builder.appName("DIM_CONTRACT").getOrCreate()
file_name = os.path.basename(__file__)

from pyspark.sql.functions import *
from src.utils import (
    get_date,
    S3Handler,
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
conn.request("GET", "/api/v2/smart-contracts", payload, headers)
res = conn.getresponse()
data = res.read().decode("utf-8")
data1 = json.loads(data)

contracts = []
for contract in data1["items"]:
    row = {
        "hash": contract["address"]["hash"],
        "address": contract["address"]["hash"].lower(),
        "name": contract["address"]["name"],
        "verified_at": contract["verified_at"],
    }
    contracts.append(row)

df = spark.createDataFrame(contracts).select("name", "address", "hash", "verified_at")

s3_writer = S3Handler()

try:
    s3_writer.write_table(
        df=df,
        data_layer=data_layer,
        schema_name=schema_name,
        table_name="contract",
        partition_cols=None,
        format="parquet",
        mode="overwrite",
    )
    print(f"S3 write success \n")
    s3_writer.table_manager(
        spark,
        schema_name,
        "contract",
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
        "contract",
        batch_bdate,
        start_date,
        end_date,
        status=0,
    )
    print(f"table_manager success \n")


spark.stop()
