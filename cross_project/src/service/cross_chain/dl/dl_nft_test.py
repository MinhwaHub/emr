import http.client
import json
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

# --------- 1. API 요청 ---------
conn = http.client.HTTPSConnection("testnet.crossscan.io")
conn.request("GET", "/api/v2/tokens?type=ERC-721", "", {})
res = conn.getresponse()
data = res.read().decode("utf-8")
data1 = json.loads(data)["items"]

# --------- 2. Pandas DataFrame 생성 ---------
df = pd.DataFrame(data1)

# --------- 3. 컬럼 정리 및 타입 캐스팅 ---------
df = df.drop(columns=["icon_url"], errors="ignore")

# 캐스팅이 실패하면 NaN으로 들어감. Pandas는 그렇게 쿨해.
df["circulating_market_cap"] = pd.to_numeric(
    df["circulating_market_cap"], errors="coerce"
)
df["decimals"] = pd.to_numeric(df["decimals"], downcast="integer", errors="coerce")
df["exchange_rate"] = pd.to_numeric(df["exchange_rate"], errors="coerce")
df["holders"] = pd.to_numeric(df["holders"], downcast="integer", errors="coerce")
df["volume_24h"] = pd.to_numeric(df["volume_24h"], errors="coerce")
df["total_supply"] = pd.to_numeric(df["total_supply"], errors="coerce")

# --------- 4. S3 저장 ---------
fs = s3fs.S3FileSystem()

table = pa.Table.from_pandas(df)

pq.write_table(
    table,
    "s3://emr-data-pipeline-test/results/cross_chain/dl/dl_nft/tokens.parquet",
    filesystem=fs,
)
