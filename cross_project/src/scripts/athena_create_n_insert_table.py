import boto3

# AWS 세션을 사용하여 자격 증명 가져오기
session = boto3.Session(profile_name="s3")
credentials = session.get_credentials()

# 액세스 키, 비밀 키, 세션 토큰 가져오기
aws_access_key = credentials.access_key
aws_secret_key = credentials.secret_key
aws_token = credentials.token

# S3 리소스를 생성하여 S3 객체에 접근
s3_resource = session.resource(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    aws_session_token=aws_token,  # 임시 크레덴셜인 경우 필요
)

from pyspark.sql import SparkSession
import warnings

warnings.filterwarnings("ignore")

jars_path = "/Users/mina/nx-mina/test/cross_project_local"  # JAR 파일들이 있는 디렉토리
jars = [
    f"/Users/mina/nx-mina/test/cross_project_local/hadoop-aws-3.3.1.jar",
    f"/Users/mina/nx-mina/test/cross_project_local/aws-java-sdk-bundle-1.12.781.jar",
]
jars_str = ",".join(jars)

spark = (
    SparkSession.builder.appName("test123")
    .config("spark.jars", jars_str)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    .config("spark.hadoop.fs.s3a.session.token", aws_token)
    .getOrCreate()
)


date_list = [
    (612088, "cross_devnet"),
    (612044, "cross_testnet"),
    (612055, "cross_mainnet"),
    (11155111, "eth_testnet"),
    (56, "bnb_mainnet"),
    (97, "bnb_testnet"),
]

# DataFrame 생성
df = spark.createDataFrame(date_list, ["chain_id", "network"])

# s3_client = boto3.client("s3")

writer = df.write.format("parquet").mode("overwrite")
writer.save("s3a://emr-data-pipeline-test/results/cross_chain/dim/dim_chain")
